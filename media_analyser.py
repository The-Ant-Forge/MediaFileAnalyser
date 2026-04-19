#!/usr/bin/env python3
"""
Interactive web-based media stats viewer.

Usage:
    python media_analyser.py [--db media.db] [--port 8080]
"""

import argparse
import json
import math
import os
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs

DB_PATH = "tv.db"
CONFIG_PATH = "media_analyser_config.json"
CONFIG_DIR = "."  # resolved in main()

DEFAULT_CONFIG = {
    "active_library": "Default",
    "libraries": [
        {
            "name": "Default",
            "db": "media.db",
            "scan_folders": [],
            "ignore_patterns": [],
        }
    ],
    "ffprobe_path": "",
    "workers": 8,
}


def load_config():
    """Load config from JSON file, with migration from old flat format."""
    config = dict(DEFAULT_CONFIG)
    config["libraries"] = [dict(lib) for lib in DEFAULT_CONFIG["libraries"]]
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r") as f:
                saved = json.load(f)
            # Migrate old flat format → library format
            if "libraries" not in saved and "scan_folders" in saved:
                saved["libraries"] = [{
                    "name": "Default",
                    "db": "media.db",
                    "scan_folders": saved.pop("scan_folders", []),
                    "ignore_patterns": saved.pop("ignore_patterns", []),
                }]
                saved["active_library"] = "Default"
            config.update(saved)
        except (json.JSONDecodeError, OSError):
            pass
    return config


def save_config(config):
    """Save config to JSON file."""
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)


def get_active_library(config=None):
    """Return the active library dict from config."""
    if config is None:
        config = load_config()
    name = config.get("active_library", "Default")
    for lib in config.get("libraries", []):
        if lib["name"] == name:
            return lib
    # Fallback to first library
    libs = config.get("libraries", [])
    return libs[0] if libs else {"name": "Default", "db": "media.db", "scan_folders": [], "ignore_patterns": []}


def switch_library(name):
    """Switch active library and update DB_PATH."""
    global DB_PATH
    config = load_config()
    for lib in config.get("libraries", []):
        if lib["name"] == name:
            config["active_library"] = name
            save_config(config)
            db = lib.get("db", "media.db")
            DB_PATH = os.path.join(CONFIG_DIR, db) if not os.path.isabs(db) else db
            refresh_views()
            invalidate_all_caches()
            return True
    return False


def invalidate_all_caches():
    """Invalidate all server-side caches."""
    invalidate_pbps_tiles_cache()
    invalidate_distributions_cache()
    invalidate_quality_cache()
    invalidate_upgrade_cache()
    invalidate_violin_cache()


# ---------------------------------------------------------------------------
# Scan state (shared between scan thread and API handler)
# ---------------------------------------------------------------------------
scan_state = {
    "running": False,
    "phase": "idle",
    "total": 0,
    "done": 0,
    "errors": 0,
    "message": "",
}
scan_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Scan logic (reuses index_media internals)
# ---------------------------------------------------------------------------
from index_media import (
    VIDEO_EXTENSIONS, init_db, probe_file,
    insert_file, insert_streams,
)


def find_video_files_filtered(paths, ignore_patterns):
    """Walk directories finding video files, skipping ignored folder names."""
    files = []
    lower_patterns = [p.lower() for p in ignore_patterns if p]
    for base in paths:
        if not os.path.exists(base):
            continue
        if os.path.isfile(base):
            if Path(base).suffix.lower() in VIDEO_EXTENSIONS:
                files.append(base)
            continue
        for root, dirs, filenames in os.walk(base):
            # Prune ignored directories in-place
            if lower_patterns:
                dirs[:] = [d for d in dirs
                           if not any(pat in d.lower() for pat in lower_patterns)]
            for fname in sorted(filenames):
                if Path(fname).suffix.lower() in VIDEO_EXTENSIONS:
                    files.append(os.path.join(root, fname))
    return files


def _load_existing_index():
    """Load file_path → (file_mtime, file_size_bytes) from the live DB.
    Returns (index_dict, has_timestamps_bool)."""
    index = {}
    has_timestamps = False
    if not os.path.exists(DB_PATH):
        return index, False
    try:
        conn = get_db()
        for row in conn.execute("SELECT file_path, file_mtime, file_size_bytes FROM files"):
            index[row["file_path"]] = (row["file_mtime"], row["file_size_bytes"])
            if row["file_mtime"] is not None:
                has_timestamps = True
        conn.close()
    except Exception:
        pass
    return index, has_timestamps


def _copy_file_rows(src_conn, dst_conn, filepath):
    """Copy a file and its streams from src DB to dst DB."""
    src_row = src_conn.execute("SELECT * FROM files WHERE file_path = ?", (filepath,)).fetchone()
    if not src_row:
        return False
    src_dict = dict(src_row)
    old_id = src_dict.pop("id")
    cols = list(src_dict.keys())
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(f"[{c}]" for c in cols)
    cur = dst_conn.execute(
        f"INSERT INTO files ({col_names}) VALUES ({placeholders})",
        [src_dict[c] for c in cols],
    )
    new_id = cur.lastrowid

    # Copy streams
    for srow in src_conn.execute("SELECT * FROM streams WHERE file_id = ?", (old_id,)):
        sd = dict(srow)
        sd.pop("id")
        sd["file_id"] = new_id
        scols = list(sd.keys())
        dst_conn.execute(
            f"INSERT OR IGNORE INTO streams ({', '.join(f'[{c}]' for c in scols)}) VALUES ({', '.join(['?'] * len(scols))})",
            [sd[c] for c in scols],
        )
    return True


def run_scan():
    """Background scan: discover, probe new/changed files, copy unchanged, then swap."""
    try:
        config = load_config()
        lib = get_active_library(config)
        folders = lib.get("scan_folders", [])
        if not folders:
            with scan_lock:
                scan_state.update(running=False, phase="error",
                                  message="No scan folders configured for library '" + lib["name"] + "'")
            return

        ignore = lib.get("ignore_patterns", [])
        ffprobe_cmd = config.get("ffprobe_path", "").strip() or "ffprobe"
        workers = config.get("workers", 8)

        # Phase: discovering files
        with scan_lock:
            scan_state["phase"] = "discovering"
            scan_state["message"] = "Scanning directories..."
        video_files = find_video_files_filtered(folders, ignore)

        if not video_files:
            with scan_lock:
                scan_state.update(running=False, phase="error",
                                  message="No video files found in configured folders")
            return

        # Phase: checking which files need probing
        existing_index, has_timestamps = _load_existing_index()
        to_probe = []
        to_copy = []

        if existing_index and has_timestamps:
            with scan_lock:
                scan_state["phase"] = "checking"
                scan_state["message"] = "Checking file timestamps..."
            for fp in video_files:
                try:
                    st = os.stat(fp)
                    mtime = st.st_mtime
                    size = st.st_size
                except OSError:
                    to_probe.append(fp)
                    continue
                prev = existing_index.get(fp)
                if prev and prev[0] is not None and prev[0] == mtime and prev[1] == size:
                    to_copy.append(fp)
                else:
                    to_probe.append(fp)
        else:
            # No timestamps in DB — skip stat checks, probe everything
            to_probe = video_files

        with scan_lock:
            scan_state["total"] = len(video_files)
            scan_state["phase"] = "probing"
            msg = f"{len(to_copy)} unchanged, {len(to_probe)} to probe" if to_copy else f"{len(to_probe)} files to probe"
            if not has_timestamps and existing_index:
                msg += " (no cached timestamps — full re-probe)"
            scan_state["message"] = msg

        # Create temp database
        db_dir = os.path.dirname(os.path.abspath(DB_PATH))
        temp_db = os.path.join(db_dir, "media_scan_temp.db")
        if os.path.exists(temp_db):
            os.remove(temp_db)
        conn = init_db(temp_db)

        done = 0
        errors = 0
        skipped = len(to_copy)

        # Copy unchanged files from existing DB
        if to_copy and os.path.exists(DB_PATH):
            src_conn = get_db()
            batch_count = 0
            for fp in to_copy:
                if not _copy_file_rows(src_conn, conn, fp):
                    to_probe.append(fp)  # fallback: re-probe if copy fails
                batch_count += 1
                if batch_count >= 100:
                    conn.commit()
                    batch_count = 0
                done += 1
                with scan_lock:
                    scan_state["done"] = done
                    scan_state["message"] = f"Copied {done}/{len(to_copy)} unchanged, {len(to_probe)} to probe"
            conn.commit()
            src_conn.close()

        # Probe new/changed files in parallel
        if to_probe:
            with scan_lock:
                scan_state["message"] = f"Probing {len(to_probe)} new/changed files..."

            def probe_and_insert(filepath):
                return filepath, probe_file(filepath, ffprobe_cmd=ffprobe_cmd)

            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(probe_and_insert, f): f for f in to_probe}
                batch_count = 0
                for future in as_completed(futures):
                    filepath, probe_data = future.result()
                    if probe_data is None:
                        errors += 1
                    else:
                        try:
                            mtime = os.stat(filepath).st_mtime
                        except OSError:
                            mtime = None
                        file_id = insert_file(conn, filepath, probe_data, file_mtime=mtime)
                        if file_id:
                            insert_streams(conn, file_id, probe_data)
                        batch_count += 1
                        if batch_count >= 100:
                            conn.commit()
                            batch_count = 0

                    done += 1
                    with scan_lock:
                        scan_state["done"] = done
                        scan_state["errors"] = errors
                        scan_state["message"] = f"{done}/{len(video_files)} files ({skipped} cached, {errors} errors)"

                conn.commit()

        # Phase: swapping databases
        with scan_lock:
            scan_state["phase"] = "swapping"
            scan_state["message"] = "Finalizing database..."

        # Preserve scan_snapshots from the live DB into the temp DB
        if os.path.exists(DB_PATH):
            try:
                live_conn = get_db()
                tables = {r[0] for r in live_conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
                if "scan_snapshots" in tables:
                    conn.execute("""CREATE TABLE IF NOT EXISTS scan_snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        scanned_at TEXT DEFAULT (datetime('now')),
                        total_files INTEGER, total_size_gb REAL,
                        h264_count INTEGER, hevc_count INTEGER, av1_count INTEGER,
                        res_4k INTEGER, res_1080p INTEGER, res_720p INTEGER,
                        res_480p INTEGER, res_other INTEGER,
                        rva_up_count INTEGER, rva_down_count INTEGER,
                        mean_bpp_sec REAL, median_bpp_sec REAL
                    )""")
                    for row in live_conn.execute("SELECT * FROM scan_snapshots ORDER BY id"):
                        d = dict(row)
                        d.pop("id")
                        cols = list(d.keys())
                        conn.execute(
                            f"INSERT INTO scan_snapshots ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})",
                            [d[c] for c in cols])
                    conn.commit()
                live_conn.close()
            except Exception:
                pass

        # Checkpoint and close temp DB
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.close()

        # Swap: rename temp → live
        backup_db = os.path.join(db_dir, "media_old.db")
        live_db = os.path.abspath(DB_PATH)

        # Remove old backup if present
        for f in [backup_db, backup_db + "-wal", backup_db + "-shm"]:
            if os.path.exists(f):
                os.remove(f)

        # Move current live → backup
        if os.path.exists(live_db):
            os.rename(live_db, backup_db)
            for suffix in ["-wal", "-shm"]:
                src = live_db + suffix
                if os.path.exists(src):
                    os.rename(src, backup_db + suffix)

        # Move temp → live
        os.rename(temp_db, live_db)
        for suffix in ["-wal", "-shm"]:
            src = temp_db + suffix
            if os.path.exists(src):
                os.rename(src, live_db + suffix)

        # Clean up backup
        for f in [backup_db, backup_db + "-wal", backup_db + "-shm"]:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except OSError:
                    pass

        invalidate_all_caches()
        capture_snapshot()
        with scan_lock:
            scan_state.update(running=False, phase="done",
                              message=f"Scan complete: {done} files ({skipped} cached, {len(to_probe)} probed, {errors} errors)")

    except Exception as e:
        with scan_lock:
            scan_state.update(running=False, phase="error",
                              message=f"Scan failed: {e}")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def refresh_views():
    """Recreate views on the live DB to pick up threshold changes."""
    if not os.path.exists(DB_PATH):
        return
    try:
        conn = get_db()
        for view in ["v_video_summary", "v_audio_summary", "v_subtitle_summary"]:
            conn.execute(f"DROP VIEW IF EXISTS [{view}]")
        from index_media import DB_SCHEMA
        conn.executescript(DB_SCHEMA)
        conn.close()
    except Exception:
        pass


def dict_rows(cursor):
    return [dict(row) for row in cursor.fetchall()]


# ---------------------------------------------------------------------------
# PBPS tiles cache — only recomputed after a scan or on first request
# ---------------------------------------------------------------------------
_pbps_tiles_cache = {"data": None}

PBPS_TILES_SQL = """
WITH bpp AS (
    SELECT *,
           file_size_bytes * 1.0 / (width * height * duration_seconds) AS bpp_sec
    FROM v_video_summary
    WHERE width > 0 AND height > 0 AND duration_seconds > 60
),
avg_bpp AS (
    SELECT resolution_class,
           AVG(bpp_sec) AS avg_bpp_sec
    FROM bpp
    GROUP BY resolution_class
),
joined AS (
    SELECT b.bpp_sec,
           b.bpp_sec / a.avg_bpp_sec AS ratio_vs_avg,
           b.video_codec,
           b.resolution_class,
           b.duration_seconds
    FROM bpp b
    JOIN avg_bpp a ON b.resolution_class = a.resolution_class
    WHERE b.duration_seconds / 60.0 > 10
)
SELECT
    SUM(CASE WHEN ratio_vs_avg > 1.5 THEN 1 ELSE 0 END) AS rva_up_count,
    SUM(CASE WHEN ratio_vs_avg < 0.5 THEN 1 ELSE 0 END) AS rva_down_count,
    ROUND(AVG(bpp_sec), 6) AS mean_bpp_sec,
    SUM(CASE WHEN video_codec = 'h264' THEN 1 ELSE 0 END) AS h264_count,
    SUM(CASE WHEN resolution_class IN ('720p', '480p', 'other') THEN 1 ELSE 0 END) AS low_res_count,
    SUM(CASE WHEN video_codec = 'av1' THEN 1 ELSE 0 END) AS av1_count,
    SUM(CASE WHEN resolution_class IN ('4K', '1080p') THEN 1 ELSE 0 END) AS high_res_count,
    COUNT(*) AS total_files
FROM joined
"""

PBPS_MEDIAN_SQL = """
WITH bpp AS (
    SELECT file_size_bytes * 1.0 / (width * height * duration_seconds) AS bpp_sec,
           duration_seconds
    FROM v_video_summary
    WHERE width > 0 AND height > 0 AND duration_seconds > 60
)
SELECT ROUND(bpp_sec, 6) AS median_bpp_sec
FROM bpp
WHERE duration_seconds / 60.0 > 10
ORDER BY bpp_sec
LIMIT 1 OFFSET (SELECT COUNT(*) / 2 FROM bpp WHERE duration_seconds / 60.0 > 10)
"""


PBPS_CODEC_SQL = """
WITH bpp AS (
    SELECT video_codec, duration_seconds
    FROM v_video_summary
    WHERE width > 0 AND height > 0 AND duration_seconds > 60
      AND duration_seconds / 60.0 > 10
)
SELECT video_codec, COUNT(*) AS cnt
FROM bpp
GROUP BY video_codec
ORDER BY cnt DESC
"""


def compute_pbps_tiles():
    conn = get_db()
    try:
        row = dict(conn.execute(PBPS_TILES_SQL).fetchone())
        median_row = conn.execute(PBPS_MEDIAN_SQL).fetchone()
        row["median_bpp_sec"] = median_row["median_bpp_sec"] if median_row else None

        # Top 3 codecs + other
        codec_rows = conn.execute(PBPS_CODEC_SQL).fetchall()
        codecs = []
        other_count = 0
        for i, cr in enumerate(codec_rows):
            if i < 3:
                codecs.append({"name": cr["video_codec"], "count": cr["cnt"]})
            else:
                other_count += cr["cnt"]
        if other_count > 0:
            codecs.append({"name": "other", "count": other_count})
        row["codecs"] = codecs

        # Ratio distribution histogram (0.1-wide buckets, 0.4 to 1.6)
        hist_rows = conn.execute("""
            WITH bpp AS (
                SELECT *,
                       file_size_bytes * 1.0 / (width * height * duration_seconds) AS bpp_sec
                FROM v_video_summary
                WHERE width > 0 AND height > 0 AND duration_seconds > 60
            ),
            avg_bpp AS (
                SELECT resolution_class, AVG(bpp_sec) AS avg_bpp_sec
                FROM bpp GROUP BY resolution_class
            ),
            joined AS (
                SELECT b.bpp_sec / a.avg_bpp_sec AS ratio
                FROM bpp b JOIN avg_bpp a ON b.resolution_class = a.resolution_class
                WHERE b.duration_seconds / 60.0 > 10
                  AND b.bpp_sec / a.avg_bpp_sec >= 0.4
                  AND b.bpp_sec / a.avg_bpp_sec < 1.6
            ),
            buckets AS (
                SELECT CAST(ratio / 0.1 AS INTEGER) AS bucket,
                       COUNT(*) AS cnt
                FROM joined GROUP BY bucket
            )
            SELECT bucket, cnt FROM buckets ORDER BY bucket
        """).fetchall()
        row["ratio_hist"] = [{"bucket": r["bucket"], "count": r["cnt"]} for r in hist_rows]

        # Resolution distribution
        res_rows = conn.execute("""
            SELECT resolution_class, COUNT(*) AS cnt
            FROM v_video_summary
            WHERE width > 0 AND height > 0 AND duration_seconds > 60
              AND duration_seconds / 60.0 > 10
            GROUP BY resolution_class
        """).fetchall()
        res_map = {r["resolution_class"]: r["cnt"] for r in res_rows}
        row["resolution"] = [
            {"name": "4K", "count": res_map.get("4K", 0)},
            {"name": "1080p", "count": res_map.get("1080p", 0)},
            {"name": "720p", "count": res_map.get("720p", 0)},
            {"name": "480p", "count": res_map.get("480p", 0)},
            {"name": "other", "count": res_map.get("other", 0)},
        ]

        return row
    finally:
        conn.close()


def get_pbps_tiles():
    if _pbps_tiles_cache["data"] is None:
        _pbps_tiles_cache["data"] = compute_pbps_tiles()
    return _pbps_tiles_cache["data"]


def invalidate_pbps_tiles_cache():
    _pbps_tiles_cache["data"] = None


# ---------------------------------------------------------------------------
# Distributions cache — ratio buckets broken down by codec and resolution
# ---------------------------------------------------------------------------
_distributions_cache = {"data": None}

DISTRIBUTIONS_SQL = """
WITH bpp AS (
    SELECT *,
           file_size_bytes * 1.0 / (width * height * duration_seconds) AS bpp_sec
    FROM v_video_summary
    WHERE width > 0 AND height > 0 AND duration_seconds > 60
),
avg_bpp AS (
    SELECT resolution_class, AVG(bpp_sec) AS avg_bpp_sec
    FROM bpp GROUP BY resolution_class
),
joined AS (
    SELECT b.video_codec, b.resolution_class,
           b.bpp_sec / a.avg_bpp_sec AS ratio
    FROM bpp b JOIN avg_bpp a ON b.resolution_class = a.resolution_class
    WHERE b.duration_seconds / 60.0 > 10
      AND b.bpp_sec / a.avg_bpp_sec >= 0.4
      AND b.bpp_sec / a.avg_bpp_sec < 1.6
)
SELECT CAST(ratio / 0.1 AS INTEGER) AS bucket,
       video_codec, resolution_class,
       COUNT(*) AS cnt
FROM joined
GROUP BY bucket, video_codec, resolution_class
ORDER BY bucket
"""


def compute_distributions():
    conn = get_db()
    try:
        rows = conn.execute(DISTRIBUTIONS_SQL).fetchall()
        # Build two breakdowns: by codec and by resolution
        codec_data = {}   # bucket -> {codec: count}
        res_data = {}     # bucket -> {resolution: count}
        all_codecs = set()
        all_res = set()

        for r in rows:
            b = r["bucket"]
            codec = r["video_codec"]
            res = r["resolution_class"]
            cnt = r["cnt"]

            all_codecs.add(codec)
            all_res.add(res)

            if b not in codec_data:
                codec_data[b] = {}
            codec_data[b][codec] = codec_data[b].get(codec, 0) + cnt

            if b not in res_data:
                res_data[b] = {}
            res_data[b][res] = res_data[b].get(res, 0) + cnt

        # Top codecs (keep top 5, rest as "other")
        codec_totals = {}
        for bd in codec_data.values():
            for c, n in bd.items():
                codec_totals[c] = codec_totals.get(c, 0) + n
        top_codecs = sorted(codec_totals, key=codec_totals.get, reverse=True)[:5]

        # Collapse non-top codecs into "other"
        codec_clean = {}
        for b, bd in codec_data.items():
            codec_clean[b] = {}
            for c, n in bd.items():
                key = c if c in top_codecs else "other"
                codec_clean[b][key] = codec_clean[b].get(key, 0) + n
        final_codecs = top_codecs + (["other"] if any("other" in v for v in codec_clean.values()) else [])

        # Fixed resolution order
        res_order = ["4K", "1080p", "720p", "480p", "other"]

        # Build bucket range 4..15
        buckets = list(range(4, 16))
        bucket_labels = [f"{b * 0.1:.1f}" for b in buckets]

        return {
            "buckets": bucket_labels,
            "codec_series": [
                {"name": c, "values": [codec_clean.get(b, {}).get(c, 0) for b in buckets]}
                for c in final_codecs
            ],
            "res_series": [
                {"name": r, "values": [res_data.get(b, {}).get(r, 0) for b in buckets]}
                for r in res_order
            ],
        }
    finally:
        conn.close()


def get_distributions():
    if _distributions_cache["data"] is None:
        _distributions_cache["data"] = compute_distributions()
    return _distributions_cache["data"]


def invalidate_distributions_cache():
    _distributions_cache["data"] = None


# ---------------------------------------------------------------------------
# Quality Heatmap + Sankey cache
# ---------------------------------------------------------------------------
_quality_cache = {"data": None}


def compute_quality_data():
    conn = get_db()
    try:
        # Heatmap: avg ratio_vs_avg per codec x resolution
        heatmap_rows = conn.execute("""
            WITH bpp AS (
                SELECT *,
                       file_size_bytes * 1.0 / (width * height * duration_seconds) AS bpp_sec
                FROM v_video_summary
                WHERE width > 0 AND height > 0 AND duration_seconds > 60
            ),
            avg_bpp AS (
                SELECT resolution_class, AVG(bpp_sec) AS avg_bpp_sec
                FROM bpp GROUP BY resolution_class
            )
            SELECT b.video_codec, b.resolution_class,
                   ROUND(AVG(b.bpp_sec / a.avg_bpp_sec), 3) AS avg_ratio,
                   COUNT(*) AS cnt
            FROM bpp b JOIN avg_bpp a ON b.resolution_class = a.resolution_class
            WHERE b.duration_seconds / 60.0 > 10
            GROUP BY b.video_codec, b.resolution_class
        """).fetchall()

        # Build heatmap matrix — codec order: oldest → newest
        codec_order = ["msmpeg4v3", "mpeg4", "mpeg2video", "vc1", "h264", "hevc", "av1", "vvc"]
        res_order = ["other", "480p", "720p", "1080p", "4K"]

        matrix = {}
        counts = {}
        seen_codecs = set()
        for r in heatmap_rows:
            codec = r["video_codec"]
            if codec not in codec_order:
                codec = "other"
            seen_codecs.add(codec)
            res = r["resolution_class"]
            key = (codec, res)
            if key not in matrix:
                matrix[key] = []
                counts[key] = 0
            matrix[key].append(r["avg_ratio"] * r["cnt"])
            counts[key] += r["cnt"]

        # Only include codecs that have data, in defined order
        codecs_list = [c for c in codec_order if c in seen_codecs]
        if "other" in seen_codecs:
            codecs_list.insert(0, "other")

        heatmap = {
            "codecs": codecs_list,
            "resolutions": res_order,
            "values": [[round(sum(matrix.get((c, r), [0])) / max(counts.get((c, r), 1), 1), 3)
                         if counts.get((c, r), 0) > 0 else None
                         for r in res_order] for c in codecs_list],
            "counts": [[counts.get((c, r), 0) for r in res_order] for c in codecs_list],
        }

        # Sankey: resolution → codec flow
        sankey_rows = conn.execute("""
            SELECT resolution_class, video_codec, COUNT(*) AS cnt
            FROM v_video_summary
            WHERE width > 0 AND height > 0 AND duration_seconds > 60
              AND duration_seconds / 60.0 > 10
            GROUP BY resolution_class, video_codec
            ORDER BY cnt DESC
        """).fetchall()

        # Build sankey nodes and links
        # Plotly renders first item at top, so reverse the desired bottom→top order
        # Left (res): bottom→top = other,480p,720p,1080p,4K → list = [4K,1080p,720p,480p,other]
        sankey_res_order = ["4K", "1080p", "720p", "480p", "other"]
        # Right (codec): bottom→top = other,msmpeg4v3,mpeg4,h264,hevc,av1,vvc → list top→bottom:
        sankey_codec_order = ["vvc", "av1", "hevc", "h264", "mpeg4", "msmpeg4v3", "other"]

        codec_totals = {}
        for r in sankey_rows:
            codec_totals[r["video_codec"]] = codec_totals.get(r["video_codec"], 0) + r["cnt"]

        # Only include codecs that exist in the data, in defined order
        seen_sankey_codecs = set()
        for r in sankey_rows:
            if r["video_codec"] in sankey_codec_order:
                seen_sankey_codecs.add(r["video_codec"])
            else:
                seen_sankey_codecs.add("other")
        codec_nodes = [c for c in sankey_codec_order if c in seen_sankey_codecs]
        res_nodes = sankey_res_order[:]

        all_nodes = res_nodes + codec_nodes
        node_idx = {n: i for i, n in enumerate(all_nodes)}

        links_src, links_tgt, links_val = [], [], []
        for r in sankey_rows:
            res = r["resolution_class"]
            codec = r["video_codec"] if r["video_codec"] in codec_nodes else "other"
            if res in node_idx and codec in node_idx:
                found = False
                for i in range(len(links_src)):
                    if links_src[i] == node_idx[res] and links_tgt[i] == node_idx[codec]:
                        links_val[i] += r["cnt"]
                        found = True
                        break
                if not found:
                    links_src.append(node_idx[res])
                    links_tgt.append(node_idx[codec])
                    links_val.append(r["cnt"])

        res_colors = {"4K": "#4ecca3", "1080p": "#5b8def", "720p": "#e0a030", "480p": "#e94560", "other": "#888"}
        codec_colors = {"vvc": "#00e5ff", "av1": "#5b8def", "hevc": "#4ecca3", "h264": "#e94560",
                        "vc1": "#533483", "mpeg2video": "#e0a030", "mpeg4": "#c97030", "msmpeg4v3": "#885030",
                        "other": "#888"}
        node_colors = [res_colors.get(n, "#888") for n in res_nodes] + \
                      [codec_colors.get(n, "#888") for n in codec_nodes]

        sankey = {
            "nodes": all_nodes,
            "node_colors": node_colors,
            "links_src": links_src,
            "links_tgt": links_tgt,
            "links_val": links_val,
        }

        # Scatter comparison data for evaluating alternative x-axes
        scatter_rows = conn.execute("""
            WITH bpp AS (
                SELECT *,
                       file_size_bytes * 1.0 / (width * height * duration_seconds) AS bpp_sec
                FROM v_video_summary
                WHERE width > 0 AND height > 0 AND duration_seconds > 60
            ),
            avg_bpp AS (
                SELECT resolution_class, AVG(bpp_sec) AS avg_bpp_sec
                FROM bpp GROUP BY resolution_class
            )
            SELECT b.file_name, b.video_codec, b.resolution_class,
                   b.width * b.height AS pixels,
                   ROUND(b.duration_seconds / 60.0, 2) AS duration_min,
                   ROUND(b.file_size_bytes / 1048576.0, 2) AS size_mb,
                   ROUND(a.avg_bpp_sec * b.width * b.height * b.duration_seconds / 1048576.0, 2) AS expected_size_mb,
                   ROUND(b.bpp_sec, 6) AS bpp_sec,
                   ROUND(a.avg_bpp_sec, 6) AS avg_bpp_sec,
                   ROUND(b.bpp_sec / a.avg_bpp_sec, 4) AS ratio
            FROM bpp b JOIN avg_bpp a ON b.resolution_class = a.resolution_class
            WHERE b.duration_seconds / 60.0 > 10
              AND b.bpp_sec / a.avg_bpp_sec < 5.0
        """).fetchall()

        # Group by codec for separate traces
        scatter = {}
        for r in scatter_rows:
            codec = r["video_codec"]
            if codec not in scatter:
                scatter[codec] = {"points": []}
            scatter[codec]["points"].append({
                "file_name": r["file_name"],
                "resolution_class": r["resolution_class"],
                "pixels": r["pixels"],
                "duration_min": r["duration_min"],
                "size_mb": r["size_mb"],
                "expected_size_mb": r["expected_size_mb"],
                "bpp_sec": r["bpp_sec"],
                "avg_bpp_sec": r["avg_bpp_sec"],
                "ratio": r["ratio"],
            })

        return {"heatmap": heatmap, "sankey": sankey, "scatter": scatter}
    finally:
        conn.close()


def get_quality_data():
    if _quality_cache["data"] is None:
        _quality_cache["data"] = compute_quality_data()
    return _quality_cache["data"]


def invalidate_quality_cache():
    _quality_cache["data"] = None


# ---------------------------------------------------------------------------
# Upgrade Priority List
# ---------------------------------------------------------------------------
_upgrade_cache = {"data": None}

UPGRADE_SQL = """
WITH bpp AS (
    SELECT *,
           file_size_bytes * 1.0 / (width * height * duration_seconds) AS bpp_sec
    FROM v_video_summary
    WHERE width > 0 AND height > 0 AND duration_seconds > 60
      AND duration_seconds / 60.0 > 10
),
avg_bpp AS (
    SELECT resolution_class, AVG(bpp_sec) AS avg_bpp_sec
    FROM bpp GROUP BY resolution_class
),
scored AS (
    SELECT b.file_name, b.file_path, b.video_codec, b.resolution_class,
           b.width, b.height,
           ROUND(b.bpp_sec / a.avg_bpp_sec, 3) AS ratio_vs_avg,
           ROUND(b.file_size_bytes / 1048576.0, 1) AS size_mb,
           ROUND(b.duration_seconds / 60.0, 1) AS dur_min,
           b.file_size_bytes,
           -- Score components (higher = more urgent upgrade)
           CASE WHEN b.video_codec = 'h264' THEN 3
                WHEN b.video_codec = 'mpeg2video' THEN 5
                WHEN b.video_codec = 'vc1' THEN 4
                WHEN b.video_codec = 'msmpeg4v3' THEN 5
                ELSE 0 END AS codec_penalty,
           CASE WHEN b.resolution_class = '480p' THEN 4
                WHEN b.resolution_class = 'other' THEN 5
                WHEN b.resolution_class = '720p' THEN 2
                ELSE 0 END AS res_penalty,
           CASE WHEN b.bpp_sec / a.avg_bpp_sec > 2.0 THEN 3
                WHEN b.bpp_sec / a.avg_bpp_sec > 1.5 THEN 2
                WHEN b.bpp_sec / a.avg_bpp_sec < 0.3 THEN 3
                WHEN b.bpp_sec / a.avg_bpp_sec < 0.5 THEN 1
                ELSE 0 END AS ratio_penalty
    FROM bpp b JOIN avg_bpp a ON b.resolution_class = a.resolution_class
)
SELECT file_name, file_path, video_codec, resolution_class,
       width || 'x' || height AS resolution,
       ratio_vs_avg, size_mb, dur_min,
       codec_penalty + res_penalty + ratio_penalty AS upgrade_score,
       CASE WHEN codec_penalty > 0 THEN 'old codec' ELSE '' END ||
       CASE WHEN res_penalty > 0 THEN ' low res' ELSE '' END ||
       CASE WHEN ratio_penalty > 0 THEN ' bad ratio' ELSE '' END AS reasons
FROM scored
WHERE codec_penalty + res_penalty + ratio_penalty > 0
ORDER BY codec_penalty + res_penalty + ratio_penalty DESC,
         file_size_bytes DESC
"""


def compute_upgrade_list():
    conn = get_db()
    try:
        cursor = conn.execute(UPGRADE_SQL)
        columns = [d[0] for d in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchmany(5000)]
        return {"columns": columns, "rows": rows, "total": len(rows)}
    finally:
        conn.close()


def get_upgrade_list():
    if _upgrade_cache["data"] is None:
        _upgrade_cache["data"] = compute_upgrade_list()
    return _upgrade_cache["data"]


def invalidate_upgrade_cache():
    _upgrade_cache["data"] = None


# ---------------------------------------------------------------------------
# Violin plot data (ratio_vs_avg values per resolution class)
# ---------------------------------------------------------------------------
_violin_cache = {"data": None}


def compute_violin_data():
    conn = get_db()
    try:
        rows = conn.execute("""
            WITH bpp AS (
                SELECT *,
                       file_size_bytes * 1.0 / (width * height * duration_seconds) AS bpp_sec
                FROM v_video_summary
                WHERE width > 0 AND height > 0 AND duration_seconds > 60
            ),
            avg_bpp AS (
                SELECT resolution_class, AVG(bpp_sec) AS avg_bpp_sec
                FROM bpp GROUP BY resolution_class
            )
            SELECT b.resolution_class,
                   ROUND(b.bpp_sec / a.avg_bpp_sec, 4) AS ratio
            FROM bpp b JOIN avg_bpp a ON b.resolution_class = a.resolution_class
            WHERE b.duration_seconds / 60.0 > 10
              AND b.bpp_sec / a.avg_bpp_sec < 5.0
        """).fetchall()

        # Group by resolution
        data = {}
        for r in rows:
            res = r["resolution_class"]
            if res not in data:
                data[res] = []
            data[res].append(r["ratio"])

        res_order = ["4K", "1080p", "720p", "480p", "other"]
        return [{"name": r, "values": data.get(r, [])} for r in res_order if r in data]
    finally:
        conn.close()


def get_violin_data():
    if _violin_cache["data"] is None:
        _violin_cache["data"] = compute_violin_data()
    return _violin_cache["data"]


def invalidate_violin_cache():
    _violin_cache["data"] = None


# ---------------------------------------------------------------------------
# Scan snapshots — track library health over time
# ---------------------------------------------------------------------------
SNAPSHOT_SQL = """
WITH bpp AS (
    SELECT *,
           file_size_bytes * 1.0 / (width * height * duration_seconds) AS bpp_sec
    FROM v_video_summary
    WHERE width > 0 AND height > 0 AND duration_seconds > 60
),
avg_bpp AS (
    SELECT resolution_class, AVG(bpp_sec) AS avg_bpp_sec
    FROM bpp GROUP BY resolution_class
),
joined AS (
    SELECT b.bpp_sec, b.bpp_sec / a.avg_bpp_sec AS ratio_vs_avg,
           b.video_codec, b.resolution_class,
           b.file_size_bytes, b.duration_seconds
    FROM bpp b JOIN avg_bpp a ON b.resolution_class = a.resolution_class
    WHERE b.duration_seconds / 60.0 > 10
)
SELECT
    COUNT(*) AS total_files,
    ROUND(SUM(file_size_bytes) / 1073741824.0, 2) AS total_size_gb,
    SUM(CASE WHEN video_codec = 'h264' THEN 1 ELSE 0 END) AS h264_count,
    SUM(CASE WHEN video_codec = 'hevc' THEN 1 ELSE 0 END) AS hevc_count,
    SUM(CASE WHEN video_codec = 'av1' THEN 1 ELSE 0 END) AS av1_count,
    SUM(CASE WHEN resolution_class = '4K' THEN 1 ELSE 0 END) AS res_4k,
    SUM(CASE WHEN resolution_class = '1080p' THEN 1 ELSE 0 END) AS res_1080p,
    SUM(CASE WHEN resolution_class = '720p' THEN 1 ELSE 0 END) AS res_720p,
    SUM(CASE WHEN resolution_class = '480p' THEN 1 ELSE 0 END) AS res_480p,
    SUM(CASE WHEN resolution_class = 'other' THEN 1 ELSE 0 END) AS res_other,
    SUM(CASE WHEN ratio_vs_avg > 1.5 THEN 1 ELSE 0 END) AS rva_up_count,
    SUM(CASE WHEN ratio_vs_avg < 0.5 THEN 1 ELSE 0 END) AS rva_down_count,
    ROUND(AVG(bpp_sec), 6) AS mean_bpp_sec
FROM joined
"""

SNAPSHOT_MEDIAN_SQL = """
WITH bpp AS (
    SELECT file_size_bytes * 1.0 / (width * height * duration_seconds) AS bpp_sec,
           duration_seconds
    FROM v_video_summary
    WHERE width > 0 AND height > 0 AND duration_seconds > 60
)
SELECT ROUND(bpp_sec, 6) AS median_bpp_sec
FROM bpp WHERE duration_seconds / 60.0 > 10
ORDER BY bpp_sec
LIMIT 1 OFFSET (SELECT COUNT(*) / 2 FROM bpp WHERE duration_seconds / 60.0 > 10)
"""


def capture_snapshot():
    """Capture current library metrics into scan_snapshots table."""
    if not os.path.exists(DB_PATH):
        return None
    conn = get_db()
    try:
        # Ensure table exists (for pre-existing DBs)
        conn.execute("""CREATE TABLE IF NOT EXISTS scan_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scanned_at TEXT DEFAULT (datetime('now')),
            total_files INTEGER, total_size_gb REAL,
            h264_count INTEGER, hevc_count INTEGER, av1_count INTEGER,
            res_4k INTEGER, res_1080p INTEGER, res_720p INTEGER, res_480p INTEGER, res_other INTEGER,
            rva_up_count INTEGER, rva_down_count INTEGER,
            mean_bpp_sec REAL, median_bpp_sec REAL
        )""")
        row = dict(conn.execute(SNAPSHOT_SQL).fetchone())
        median_row = conn.execute(SNAPSHOT_MEDIAN_SQL).fetchone()
        row["median_bpp_sec"] = median_row["median_bpp_sec"] if median_row else None

        conn.execute("""INSERT INTO scan_snapshots
            (total_files, total_size_gb, h264_count, hevc_count, av1_count,
             res_4k, res_1080p, res_720p, res_480p, res_other,
             rva_up_count, rva_down_count, mean_bpp_sec, median_bpp_sec)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (row["total_files"], row["total_size_gb"],
             row["h264_count"], row["hevc_count"], row["av1_count"],
             row["res_4k"], row["res_1080p"], row["res_720p"], row["res_480p"], row["res_other"],
             row["rva_up_count"], row["rva_down_count"],
             row["mean_bpp_sec"], row["median_bpp_sec"]))
        conn.commit()
        return row
    except Exception:
        return None
    finally:
        conn.close()


def get_snapshot_history():
    """Return all snapshots for the current library."""
    if not os.path.exists(DB_PATH):
        return []
    conn = get_db()
    try:
        # Check if table exists
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "scan_snapshots" not in tables:
            return []
        rows = conn.execute("SELECT * FROM scan_snapshots ORDER BY scanned_at ASC").fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
    finally:
        conn.close()


class APIHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # Suppress default access logs

    def send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, html):
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        if path == "/":
            self.send_html(INDEX_HTML)
        elif path == "/api/views":
            self.handle_views()
        elif path == "/api/data":
            self.handle_data(params)
        elif path == "/api/stats":
            self.handle_stats(params)
        elif path == "/api/query":
            self.handle_query(params)
        elif path == "/api/config":
            self.handle_get_config()
        elif path == "/api/libraries":
            self.handle_get_libraries()
        elif path == "/api/scan/status":
            self.handle_scan_status()
        elif path == "/api/pbps/tiles":
            self.handle_pbps_tiles()
        elif path == "/api/distributions":
            self.handle_distributions()
        elif path == "/api/quality":
            self.handle_quality()
        elif path == "/api/upgrades":
            self.handle_upgrades()
        elif path == "/api/violins":
            self.handle_violins()
        elif path == "/api/snapshots":
            self.handle_snapshots()
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/config":
            self.handle_post_config()
        elif path == "/api/libraries/switch":
            self.handle_switch_library()
        elif path == "/api/snapshots/capture":
            self.handle_capture_snapshot()
        elif path == "/api/snapshots/delete":
            self.handle_delete_snapshot()
        elif path == "/api/scan/start":
            self.handle_scan_start()
        else:
            self.send_response(404)
            self.end_headers()

    def handle_get_config(self):
        self.send_json(load_config())

    def handle_get_libraries(self):
        config = load_config()
        self.send_json({
            "active": config.get("active_library", "Default"),
            "libraries": [lib["name"] for lib in config.get("libraries", [])],
        })

    def handle_switch_library(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, ValueError):
            self.send_json({"error": "Invalid JSON"}, 400)
            return
        name = body.get("name", "")
        if switch_library(name):
            self.send_json({"ok": True, "active": name, "db": DB_PATH})
        else:
            self.send_json({"error": f"Library '{name}' not found"}, 404)

    def handle_post_config(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, ValueError):
            self.send_json({"error": "Invalid JSON"}, 400)
            return
        config = load_config()
        # Global settings
        if "ffprobe_path" in body:
            config["ffprobe_path"] = str(body["ffprobe_path"])
        if "workers" in body:
            config["workers"] = max(1, min(32, int(body["workers"])))
        # Libraries array (full replacement)
        if "libraries" in body and isinstance(body["libraries"], list):
            config["libraries"] = []
            for lib in body["libraries"]:
                config["libraries"].append({
                    "name": str(lib.get("name", "Unnamed")),
                    "db": str(lib.get("db", "media.db")),
                    "scan_folders": [str(p) for p in lib.get("scan_folders", [])],
                    "ignore_patterns": [str(p) for p in lib.get("ignore_patterns", [])],
                })
            # Ensure active library still exists
            names = [l["name"] for l in config["libraries"]]
            if config.get("active_library") not in names and names:
                config["active_library"] = names[0]
        if "active_library" in body:
            config["active_library"] = str(body["active_library"])
        save_config(config)
        # Re-apply active library DB path
        switch_library(config.get("active_library", "Default"))
        self.send_json({"ok": True})

    def handle_scan_status(self):
        with scan_lock:
            self.send_json(dict(scan_state))

    def handle_pbps_tiles(self):
        try:
            self.send_json(get_pbps_tiles())
        except Exception as e:
            self.send_json({"error": str(e)}, 500)

    def handle_distributions(self):
        try:
            self.send_json(get_distributions())
        except Exception as e:
            self.send_json({"error": str(e)}, 500)

    def handle_quality(self):
        try:
            self.send_json(get_quality_data())
        except Exception as e:
            self.send_json({"error": str(e)}, 500)

    def handle_upgrades(self):
        try:
            self.send_json(get_upgrade_list())
        except Exception as e:
            self.send_json({"error": str(e)}, 500)

    def handle_violins(self):
        try:
            self.send_json(get_violin_data())
        except Exception as e:
            self.send_json({"error": str(e)}, 500)

    def handle_snapshots(self):
        self.send_json(get_snapshot_history())

    def handle_capture_snapshot(self):
        result = capture_snapshot()
        if result:
            self.send_json({"ok": True})
        else:
            self.send_json({"error": "Failed to capture snapshot"}, 500)

    def handle_delete_snapshot(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, ValueError):
            self.send_json({"error": "Invalid JSON"}, 400)
            return
        snap_id = body.get("id")
        if not snap_id:
            self.send_json({"error": "No snapshot ID"}, 400)
            return
        try:
            conn = get_db()
            conn.execute("DELETE FROM scan_snapshots WHERE id = ?", (snap_id,))
            conn.commit()
            conn.close()
            self.send_json({"ok": True})
        except Exception as e:
            self.send_json({"error": str(e)}, 500)

    def handle_scan_start(self):
        with scan_lock:
            if scan_state["running"]:
                self.send_json({"error": "Scan already running"}, 409)
                return
            scan_state["running"] = True
            scan_state["phase"] = "starting"
            scan_state["total"] = 0
            scan_state["done"] = 0
            scan_state["errors"] = 0
            scan_state["message"] = ""
        t = threading.Thread(target=run_scan, daemon=True)
        t.start()
        self.send_json({"started": True})

    def handle_views(self):
        """List available views/tables."""
        self.send_json({
            "views": [
                {"name": "v_video_summary", "label": "Video Summary"},
                {"name": "v_audio_summary", "label": "Audio Summary"},
                {"name": "v_subtitle_summary", "label": "Subtitle Summary"},
                {"name": "files", "label": "All Files"},
                {"name": "streams", "label": "All Streams"},
            ]
        })

    def handle_data(self, params):
        """Paginated, sortable, filterable data from a view."""
        view = params.get("view", ["v_video_summary"])[0]
        allowed = {"v_video_summary", "v_audio_summary", "v_subtitle_summary",
                    "files", "streams"}
        if view not in allowed:
            self.send_json({"error": "Invalid view"}, 400)
            return

        conn = get_db()

        # Get columns
        cursor = conn.execute(f"SELECT * FROM [{view}] LIMIT 0")
        columns = [d[0] for d in cursor.description]

        # Build WHERE clause from filters
        where_parts = []
        where_params = []
        search = params.get("search", [""])[0].strip()
        if search:
            text_cols = []
            for col in columns:
                text_cols.append(f"CAST([{col}] AS TEXT) LIKE ?")
                where_params.append(f"%{search}%")
            where_parts.append(f"({' OR '.join(text_cols)})")

        # Column-specific filters
        for key, vals in params.items():
            if key.startswith("filter_") and vals[0]:
                col = key[7:]
                if col in columns:
                    where_parts.append(f"[{col}] = ?")
                    where_params.append(vals[0])

        where_sql = ""
        if where_parts:
            where_sql = "WHERE " + " AND ".join(where_parts)

        # Count
        count = conn.execute(
            f"SELECT COUNT(*) FROM [{view}] {where_sql}", where_params
        ).fetchone()[0]

        # Sort
        sort_col = params.get("sort", [""])[0]
        sort_dir = params.get("dir", ["asc"])[0]
        order_sql = ""
        if sort_col and sort_col in columns:
            direction = "DESC" if sort_dir.lower() == "desc" else "ASC"
            order_sql = f"ORDER BY [{sort_col}] {direction}"

        # Pagination
        limit = min(int(params.get("limit", ["200"])[0]), 5000)
        offset = int(params.get("offset", ["0"])[0])

        rows = dict_rows(conn.execute(
            f"SELECT * FROM [{view}] {where_sql} {order_sql} LIMIT ? OFFSET ?",
            where_params + [limit, offset]
        ))

        conn.close()
        self.send_json({
            "columns": columns,
            "rows": rows,
            "total": count,
            "limit": limit,
            "offset": offset,
        })

    def handle_stats(self, params):
        """Compute mean/std for numerical columns, optionally grouped."""
        view = params.get("view", ["v_video_summary"])[0]
        allowed = {"v_video_summary", "v_audio_summary", "v_subtitle_summary",
                    "files", "streams"}
        if view not in allowed:
            self.send_json({"error": "Invalid view"}, 400)
            return

        conn = get_db()
        cursor = conn.execute(f"SELECT * FROM [{view}] LIMIT 0")
        columns = [d[0] for d in cursor.description]

        # Find numeric columns by sampling
        sample = dict_rows(conn.execute(f"SELECT * FROM [{view}] LIMIT 100"))
        numeric_cols = []
        for col in columns:
            nums = [r[col] for r in sample if isinstance(r[col], (int, float)) and r[col] is not None]
            if len(nums) > 5:
                numeric_cols.append(col)

        group_by = params.get("group_by", [""])[0]
        if group_by and group_by not in columns:
            group_by = ""

        agg_parts = []
        for col in numeric_cols:
            agg_parts.append(f"COUNT([{col}]) AS [{col}_count]")
            agg_parts.append(f"ROUND(AVG([{col}]), 3) AS [{col}_mean]")
            agg_parts.append(f"ROUND(MIN([{col}]), 3) AS [{col}_min]")
            agg_parts.append(f"ROUND(MAX([{col}]), 3) AS [{col}_max]")
            # SQLite doesn't have STDDEV, compute variance manually
            agg_parts.append(
                f"ROUND(SQRT(AVG([{col}]*[{col}]) - AVG([{col}])*AVG([{col}])), 3) AS [{col}_std]"
            )

        if not agg_parts:
            self.send_json({"numeric_columns": [], "stats": []})
            conn.close()
            return

        agg_sql = ", ".join(agg_parts)
        if group_by:
            sql = f"SELECT [{group_by}], {agg_sql} FROM [{view}] GROUP BY [{group_by}] ORDER BY [{group_by}]"
        else:
            sql = f"SELECT {agg_sql} FROM [{view}]"

        rows = dict_rows(conn.execute(sql))
        conn.close()

        self.send_json({
            "numeric_columns": numeric_cols,
            "group_by": group_by,
            "stats": rows,
        })

    def handle_query(self, params):
        """Run a read-only SQL query."""
        sql = params.get("sql", [""])[0].strip()
        if not sql:
            self.send_json({"error": "No SQL provided"}, 400)
            return

        # Block writes
        first_word = sql.split()[0].upper() if sql.split() else ""
        if first_word not in ("SELECT", "WITH", "EXPLAIN"):
            self.send_json({"error": "Only SELECT queries allowed"}, 400)
            return

        conn = get_db()
        try:
            cursor = conn.execute(sql)
            columns = [d[0] for d in cursor.description] if cursor.description else []
            rows = [dict(zip(columns, row)) for row in cursor.fetchmany(5000)]
            self.send_json({"columns": columns, "rows": rows, "total": len(rows)})
        except sqlite3.Error as e:
            self.send_json({"error": str(e)}, 400)
        finally:
            conn.close()


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Media Stats Viewer</title>
<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
<style>
:root {
    --bg: #1a1a2e; --surface: #16213e; --border: #0f3460;
    --text: #e0e0e0; --text2: #a0a0b0; --accent: #e94560; --accent2: #533483;
    --green: #4ecca3; --input-bg: #0f3460;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: 'Segoe UI', system-ui, sans-serif; background: var(--bg); color: var(--text); }
.container { max-width: 1800px; margin: 0 auto; padding: 16px; }
h1 { color: var(--accent); margin-bottom: 8px; font-size: 1.5em; }
h2 { color: var(--green); margin: 16px 0 8px; font-size: 1.1em; }

/* Tabs */
.tabs { display: flex; gap: 4px; margin-bottom: 16px; flex-wrap: wrap; }
.tab { padding: 8px 16px; background: var(--surface); border: 1px solid var(--border);
       border-radius: 6px 6px 0 0; cursor: pointer; color: var(--text2); font-size: 0.9em; }
.tab.active { background: var(--border); color: var(--text); border-bottom-color: var(--border); }
.tab:hover { background: var(--border); }

/* Controls */
.controls { display: flex; gap: 8px; margin-bottom: 12px; flex-wrap: wrap; align-items: center; }
input, select, button, textarea {
    background: var(--input-bg); color: var(--text); border: 1px solid var(--border);
    padding: 6px 10px; border-radius: 4px; font-size: 0.85em;
}
input:focus, select:focus, textarea:focus { outline: none; border-color: var(--accent); }
button { cursor: pointer; background: var(--accent2); border-color: var(--accent2); }
button:hover { background: var(--accent); border-color: var(--accent); }
.btn-sm { padding: 4px 8px; font-size: 0.8em; }

/* Table */
.table-wrap { overflow-x: auto; max-height: 60vh; overflow-y: auto; }
table { border-collapse: collapse; width: 100%; font-size: 0.82em; }
thead { position: sticky; top: 0; z-index: 1; }
th { background: var(--border); padding: 6px 8px; text-align: left; cursor: pointer;
     white-space: nowrap; user-select: none; }
th:hover { background: var(--accent2); }
th .sort-arrow { margin-left: 4px; font-size: 0.7em; }
td { padding: 4px 8px; border-bottom: 1px solid #1a1a3e; white-space: nowrap;
     max-width: 400px; overflow: hidden; text-overflow: ellipsis; }
tr:hover td { background: rgba(233,69,96,0.18); }

/* Stats */
.stats-table { font-size: 0.82em; }
.stats-table th { background: var(--accent2); }
.stats-table td { text-align: right; }
.stats-table td:first-child { text-align: left; font-weight: 600; }

/* Panels */
.panel { background: var(--surface); border: 1px solid var(--border); border-radius: 6px;
         padding: 16px; margin-bottom: 16px; }
.section { display: none; }
.section.active { display: block; }

/* Pagination */
.paging { display: flex; gap: 8px; align-items: center; margin-top: 8px; font-size: 0.85em; color: var(--text2); }

/* Chart area */
#chart { min-height: 400px; background: var(--surface); border-radius: 6px;
         border: 1px solid var(--border); }
.chart-controls { display: flex; gap: 8px; flex-wrap: wrap; align-items: end; margin-bottom: 12px; }
.chart-controls label { font-size: 0.8em; color: var(--text2); display: flex; flex-direction: column; gap: 2px; }

/* SQL */
textarea { width: 100%; min-height: 80px; font-family: 'Fira Code', monospace; resize: vertical; }
.info { color: var(--text2); font-size: 0.8em; margin-bottom: 8px; }

/* Header row */
.header-row { display: flex; align-items: center; gap: 12px; margin-bottom: 8px; }
.header-row h1 { margin-bottom: 0; }
.header-actions { margin-left: auto; display: flex; gap: 8px; align-items: center; }
.icon-btn { background: var(--surface); border: 1px solid var(--border); color: var(--text2);
            width: 36px; height: 36px; border-radius: 6px; cursor: pointer; font-size: 1.2em;
            display: flex; align-items: center; justify-content: center; padding: 0; }
.icon-btn:hover { background: var(--border); color: var(--text); }
.icon-btn.scanning { animation: pulse 1.5s ease-in-out infinite; }
@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.5; } }

/* Settings overlay */
.settings-overlay { position: fixed; inset: 0; background: rgba(0,0,0,0.6); z-index: 100;
                    display: none; align-items: center; justify-content: center; }
.settings-overlay.open { display: flex; }
.settings-panel { background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
                  padding: 24px; width: 560px; max-width: 95vw; max-height: 85vh; overflow-y: auto; }
.settings-panel h2 { margin-top: 0; }
.settings-panel h3 { color: var(--green); font-size: 0.95em; margin: 16px 0 6px; }
.settings-panel .list-items { margin: 4px 0 8px; }
.settings-panel .list-item { display: flex; align-items: center; gap: 6px; padding: 4px 0; font-size: 0.85em; }
.settings-panel .list-item span { flex: 1; word-break: break-all; color: var(--text); }
.settings-panel .remove-btn { background: none; border: none; color: var(--accent); cursor: pointer;
                               font-size: 1em; padding: 2px 6px; }
.settings-panel .remove-btn:hover { color: #ff6b81; }
.settings-panel .add-row { display: flex; gap: 6px; }
.settings-panel .add-row input { flex: 1; }
.settings-panel .actions { display: flex; gap: 8px; margin-top: 20px; justify-content: flex-end; }
.settings-panel .actions button { padding: 8px 20px; }

/* Score tiles */
.score-tiles { display: flex; gap: 10px; margin-bottom: 14px; flex-wrap: wrap; }
.score-tile { background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
              padding: 12px 18px; min-width: 130px; flex: 1; text-align: center;
              display: flex; flex-direction: column; }
.score-tile .tile-label { font-size: 0.75em; color: var(--text2); margin-bottom: 4px; }
.score-tile .tile-value { font-size: 1.6em; font-weight: 700; color: var(--green); line-height: 1.2; flex: 1;
              display: flex; align-items: center; justify-content: center; }
.score-tile.warn .tile-value { color: var(--accent); }
.score-tile.neutral .tile-value { color: var(--text); }
.tile-pct { font-size: 0.5em; color: var(--text2); font-weight: 400; }

/* Mini bar chart in tiles */
.mini-bars { display: flex; align-items: flex-end; gap: 6px; height: 48px; }
.mini-bar { display: flex; flex-direction: column; align-items: center; flex: 1; height: 100%; justify-content: flex-end; }
.mini-bar-fill { width: 100%; min-width: 24px; border-radius: 3px 3px 0 0; transition: height 0.3s ease; }
.mini-bar-label { font-size: 0.6em; color: var(--text2); margin-top: 2px; white-space: nowrap; }
.mini-bar-count { font-size: 0.65em; color: var(--text); font-weight: 600; margin-bottom: 1px; }

/* Ratio distribution histogram */
.ratio-hist { display: flex; align-items: flex-end; gap: 1px; height: 44px; position: relative; }
.ratio-hist .rh-bar { flex: 1; min-width: 0; border-radius: 2px 2px 0 0; position: relative; }
.ratio-hist .rh-bar:hover::after { content: attr(data-tip); position: absolute; bottom: 100%; left: 50%;
    transform: translateX(-50%); background: var(--bg); border: 1px solid var(--border); color: var(--text);
    padding: 2px 6px; border-radius: 3px; font-size: 0.65em; white-space: nowrap; z-index: 5; }
.ratio-hist .rh-line { position: absolute; bottom: 0; top: 0; width: 1px; background: var(--text); opacity: 0.6; pointer-events: none; }
.ratio-hist-labels { display: flex; justify-content: space-between; font-size: 0.55em; color: var(--text2); margin-top: 1px; }

/* Clickable cells */
td.clickable { cursor: pointer; color: var(--green); }
td.clickable:hover { text-decoration: underline; }
td.clickable-path { cursor: pointer; }
td.clickable-path:hover { color: var(--green); }
.copied-flash { animation: flash-green 0.6s ease; }
@keyframes flash-green { 0% { background: var(--green); color: #000; } 100% {} }

/* Scan toast */
.scan-toast { position: fixed; bottom: 0; left: 0; right: 0; background: var(--surface);
              border-top: 2px solid var(--border); padding: 10px 20px; z-index: 90;
              display: none; align-items: center; gap: 12px; font-size: 0.85em; }
.scan-toast.visible { display: flex; }
.scan-toast .progress-wrap { flex: 1; background: var(--bg); border-radius: 4px; height: 18px; overflow: hidden; }
.scan-toast .progress-bar { height: 100%; background: var(--green); border-radius: 4px;
                            transition: width 0.3s ease; min-width: 0; }
.scan-toast .progress-bar.error { background: var(--accent); }
.scan-toast .scan-info { white-space: nowrap; color: var(--text2); }
.scan-toast .dismiss-btn { background: none; border: none; color: var(--text2); cursor: pointer;
                           font-size: 1.1em; padding: 2px 6px; }
.scan-toast .dismiss-btn:hover { color: var(--text); }
</style>
</head>
<body>
<div class="container">
<div class="header-row">
    <h1>Media Stats Viewer</h1>
    <div class="header-actions">
        <select id="librarySwitcher" onchange="switchLibrary(this.value)" title="Switch library" style="font-size:0.85em"></select>
        <button class="icon-btn" id="scanBtn" onclick="startScan()" title="Start scan">&#9654;</button>
        <button class="icon-btn" id="settingsBtn" onclick="openSettings()" title="Settings">&#9881;</button>
    </div>
</div>

<div class="tabs" id="mainTabs">
    <div class="tab active" data-section="data-section">Data Browser</div>
    <div class="tab" data-section="stats-section">Statistics</div>
    <div class="tab" data-section="chart-section">Charts</div>
    <div class="tab" data-section="pbps-section">Normalized PBPS</div>
    <div class="tab" data-section="dist-section">Distributions</div>
    <div class="tab" data-section="quality-section">Quality Map</div>
    <div class="tab" data-section="upgrade-section">Upgrades</div>
    <div class="tab" data-section="progress-section">Progress</div>
    <div class="tab" data-section="sql-section">SQL Console</div>
</div>

<div class="score-tiles" id="scoreTiles">
    <div class="score-tile warn" id="tileRVA"><div class="tile-label">RVA</div><div class="tile-value" style="font-size:1.3em;display:block"><span style="color:var(--text2);font-size:0.7em">&gt;1.5x </span><span id="tileRVAUp">--</span><br><span style="color:var(--text2);font-size:0.7em">&lt;0.5x </span><span id="tileRVADown">--</span></div></div>
    <div class="score-tile neutral" id="tileBPP"><div class="tile-label">BPP/sec</div><div class="tile-value" style="font-size:1.3em;display:block"><span style="color:var(--text2);font-size:0.7em">med </span><span id="tileBPPMedian">--</span><br><span style="color:var(--text2);font-size:0.7em">avg </span><span id="tileBPPMean">--</span></div></div>
    <div class="score-tile" id="tileCodecs" style="min-width:180px"><div class="tile-label">Codecs</div><div class="mini-bars" id="codecBars">--</div></div>
    <div class="score-tile" id="tileResDist" style="min-width:180px"><div class="tile-label">Resolution</div><div class="mini-bars" id="resBars">--</div></div>
    <div class="score-tile" id="tileRatioDist" style="min-width:240px"><div class="tile-label">Ratio Distribution</div><div class="ratio-hist" id="ratioHist">--</div></div>
    <div class="score-tile warn" id="tileLegacy"><div class="tile-label">Legacy</div><div class="tile-value" style="font-size:1.3em;display:block"><span style="color:var(--text2);font-size:0.7em">h264 </span><span id="tileLegH264">--</span><br><span style="color:var(--text2);font-size:0.7em">&le;720p </span><span id="tileLeg720p">--</span></div></div>
    <div class="score-tile" id="tileModern"><div class="tile-label">Modern</div><div class="tile-value" style="font-size:1.3em;display:block;color:var(--green)"><span style="color:var(--text2);font-size:0.7em">av1 </span><span id="tileModAV1">--</span><br><span style="color:var(--text2);font-size:0.7em">&ge;FHD </span><span id="tileMod1080p">--</span></div></div>
</div>

<!-- DATA BROWSER -->
<div class="section active" id="data-section">
<div class="panel">
    <div class="controls">
        <select id="viewSelect"></select>
        <input id="searchBox" type="text" placeholder="Search all columns..." style="width:250px">
        <button onclick="loadData()">Search</button>
        <span id="rowCount" style="color:var(--text2);font-size:0.85em"></span>
    </div>
    <div class="table-wrap" id="dataTableWrap">
        <table id="dataTable"><thead></thead><tbody></tbody></table>
    </div>
    <div class="paging">
        <button class="btn-sm" onclick="pagePrev()">← Prev</button>
        <span id="pageInfo"></span>
        <button class="btn-sm" onclick="pageNext()">Next →</button>
        <select id="pageSizeSelect" onchange="loadData()">
            <option value="100">100</option>
            <option value="200" selected>200</option>
            <option value="500">500</option>
            <option value="1000">1000</option>
        </select>
        <span>per page</span>
    </div>
</div>
</div>

<!-- STATISTICS -->
<div class="section" id="stats-section">
<div class="panel">
    <div class="controls">
        <select id="statsViewSelect"></select>
        <label style="color:var(--text2);font-size:0.85em">Group by:</label>
        <select id="statsGroupSelect"><option value="">(none)</option></select>
        <button onclick="loadStats()">Compute</button>
    </div>
    <div class="table-wrap" id="statsTableWrap"></div>
</div>
</div>

<!-- CHARTS -->
<div class="section" id="chart-section">
<div class="panel">
    <div class="chart-controls">
        <label>Data source <select id="chartViewSelect"></select></label>
        <label>Chart type
            <select id="chartType">
                <option value="bar">Bar</option>
                <option value="scatter">Scatter</option>
                <option value="box">Box</option>
                <option value="histogram">Histogram</option>
                <option value="pie">Pie</option>
            </select>
        </label>
        <label>X axis <select id="chartX"></select></label>
        <label>Y axis <select id="chartY"></select></label>
        <label>Color/Group <select id="chartColor"><option value="">(none)</option></select></label>
        <label>Aggregation
            <select id="chartAgg">
                <option value="">None (raw)</option>
                <option value="avg">Average</option>
                <option value="sum">Sum</option>
                <option value="count">Count</option>
                <option value="min">Min</option>
                <option value="max">Max</option>
            </select>
        </label>
        <button onclick="renderChart()">Draw Chart</button>
    </div>
    <div id="chart"></div>
</div>
</div>

<!-- NORMALIZED PBPS -->
<div class="section" id="pbps-section">
<div class="panel">
    <p class="info">Per-pixel Bytes Per Second normalized against resolution class average. Shows files with duration &gt; 10 minutes, ordered by ratio vs average (highest first).</p>
    <button onclick="loadPBPS()">Run Analysis</button>
    <span id="pbpsInfo" style="color:var(--text2);font-size:0.85em;margin-left:8px"></span>
    <div class="table-wrap" id="pbpsTableWrap" style="margin-top:12px"></div>
</div>
</div>

<!-- DISTRIBUTIONS -->
<div class="section" id="dist-section">
<div class="panel">
    <p class="info">Ratio vs Average distribution broken down by codec and resolution. Bars show file counts in 0.1-wide ratio buckets (0.4–1.6). Vertical line marks 1.0 (average).</p>
    <button onclick="loadDistributions()">Load Charts</button>
    <span id="distInfo" style="color:var(--text2);font-size:0.85em;margin-left:8px"></span>
    <h2>Bitrate Distribution (Violin Plots)</h2>
    <div id="distViolinChart" style="min-height:400px"></div>
    <h2>By Video Codec</h2>
    <div id="distCodecChart" style="min-height:400px"></div>
    <h2>By Resolution</h2>
    <div id="distResChart" style="min-height:400px"></div>
</div>
</div>

<!-- QUALITY MAP -->
<div class="section" id="quality-section">
<div class="panel">
    <p class="info">Quality heatmap shows average ratio vs average per codec/resolution combination.</p>
    <button onclick="loadQualityMap()">Load Charts</button>
    <span id="qualityInfo" style="color:var(--text2);font-size:0.85em;margin-left:8px"></span>
    <h2>Quality Heatmap (Codec x Resolution)</h2>
    <div id="qualityHeatmap" style="min-height:450px"></div>
    <h2>Resolution &rarr; Codec Flow</h2>
    <div id="qualitySankey" style="min-height:500px"></div>
    <h2>Scatter Comparison</h2>
    <div id="qualityScatterExpected" style="min-height:500px"></div>
    <div id="qualityScatterDuration" style="min-height:500px"></div>
    <div id="qualityScatterSize" style="min-height:500px"></div>
</div>
</div>

<!-- UPGRADES -->
<div class="section" id="upgrade-section">
<div class="panel">
    <p class="info">Files scored by upgrade urgency: old codecs (h264, mpeg2, vc1), low resolution (&le;720p), and abnormal bitrate ratios. Higher score = more urgent.</p>
    <button onclick="loadUpgrades()">Load List</button>
    <span id="upgradeInfo" style="color:var(--text2);font-size:0.85em;margin-left:8px"></span>
    <div class="table-wrap" id="upgradeTableWrap" style="margin-top:12px"></div>
</div>
</div>

<!-- PROGRESS -->
<div class="section" id="progress-section">
<div class="panel">
    <p class="info">Library health tracked over time. A snapshot is captured automatically after each scan.</p>
    <div class="controls">
        <button onclick="loadProgress()">Refresh</button>
        <button onclick="captureSnapshot()">Take Snapshot Now</button>
        <span id="progressInfo" style="color:var(--text2);font-size:0.85em;margin-left:8px"></span>
    </div>
    <h2>Codec Migration</h2>
    <div id="progressCodecChart" style="min-height:300px"></div>
    <h2>Resolution Upgrades</h2>
    <div id="progressResChart" style="min-height:300px"></div>
    <h2>Quality Metrics</h2>
    <div id="progressQualityChart" style="min-height:300px"></div>
    <h2>Manage Snapshots</h2>
    <div class="table-wrap" id="snapshotTableWrap" style="max-height:300px"></div>
</div>
</div>

<!-- SQL CONSOLE -->
<div class="section" id="sql-section">
<div class="panel">
    <p class="info">Tables: files, streams &nbsp;|&nbsp; Views: v_video_summary, v_audio_summary, v_subtitle_summary</p>
    <textarea id="sqlInput" placeholder="SELECT video_codec, resolution_class, ROUND(AVG(mb_per_minute),2) as avg_mb_min&#10;FROM v_video_summary&#10;GROUP BY video_codec, resolution_class&#10;ORDER BY avg_mb_min DESC"></textarea>
    <div class="controls" style="margin-top:8px">
        <button onclick="runSQL()">Run Query</button>
        <span id="sqlInfo" style="color:var(--text2);font-size:0.85em"></span>
    </div>
    <div class="table-wrap" id="sqlTableWrap" style="margin-top:12px"></div>
</div>
</div>

</div>

<!-- SETTINGS MODAL -->
<div class="settings-overlay" id="settingsOverlay" onclick="if(event.target===this)closeSettings()">
<div class="settings-panel" style="width:640px">
    <h2 style="color:var(--green);margin-top:0">Settings</h2>

    <h3>Libraries</h3>
    <div class="controls" style="margin-bottom:8px">
        <select id="settingsLibSelect" onchange="switchSettingsLib()" style="flex:1"></select>
        <button class="btn-sm" onclick="addLibrary()">+ New</button>
        <button class="btn-sm" onclick="removeLibrary()" style="background:var(--accent)">Remove</button>
    </div>

    <div style="background:var(--bg);border:1px solid var(--border);border-radius:6px;padding:12px;margin-bottom:12px">
        <label style="font-size:0.8em;color:var(--text2)">Library Name</label>
        <input type="text" id="libName" style="width:100%;margin-bottom:8px">

        <label style="font-size:0.8em;color:var(--text2)">Database File</label>
        <input type="text" id="libDB" placeholder="media.db" style="width:100%;margin-bottom:8px">

        <label style="font-size:0.8em;color:var(--text2)">Target Folders</label>
        <div class="list-items" id="folderList"></div>
        <div class="add-row" style="margin-bottom:8px">
            <input type="text" id="newFolder" placeholder="/path/to/media/folder">
            <button onclick="addListItem('folder')">Add</button>
        </div>

        <label style="font-size:0.8em;color:var(--text2)">Ignore Patterns</label>
        <div class="list-items" id="patternList"></div>
        <div class="add-row">
            <input type="text" id="newPattern" placeholder="e.g. sample, mature, .recycle">
            <button onclick="addListItem('pattern')">Add</button>
        </div>
    </div>

    <h3>Global Settings</h3>
    <label style="font-size:0.8em;color:var(--text2)">ffprobe Path</label>
    <input type="text" id="ffprobePath" placeholder="(uses system PATH)" style="width:100%;margin-bottom:8px">

    <label style="font-size:0.8em;color:var(--text2)">Parallel Workers</label>
    <input type="number" id="workerCount" min="1" max="32" value="8" style="width:80px">

    <div class="actions">
        <button onclick="closeSettings()" style="background:var(--surface)">Cancel</button>
        <button onclick="saveSettings()">Save</button>
    </div>
</div>
</div>

<!-- SCAN TOAST -->
<div class="scan-toast" id="scanToast">
    <span class="scan-info" id="scanPhase">Idle</span>
    <div class="progress-wrap">
        <div class="progress-bar" id="scanProgressBar" style="width:0%"></div>
    </div>
    <span class="scan-info" id="scanDetail"></span>
    <button class="dismiss-btn" onclick="dismissScanToast()" title="Dismiss">&times;</button>
</div>

<script>
let currentOffset = 0;
let currentTotal = 0;
let currentSort = '';
let currentDir = 'asc';
let cachedColumns = {};

// Tab switching
document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.section').forEach(s => s.classList.remove('active'));
        tab.classList.add('active');
        document.getElementById(tab.dataset.section).classList.add('active');
    });
});

// Init
async function init() {
    const resp = await fetch('/api/views');
    const data = await resp.json();
    const views = data.views;
    for (const sel of [document.getElementById('viewSelect'),
                       document.getElementById('statsViewSelect'),
                       document.getElementById('chartViewSelect')]) {
        sel.innerHTML = views.map(v => `<option value="${v.name}">${v.label}</option>`).join('');
    }
    document.getElementById('viewSelect').onchange = () => { currentOffset = 0; loadData(); };
    document.getElementById('statsViewSelect').onchange = loadStatsColumns;
    document.getElementById('chartViewSelect').onchange = loadChartColumns;
    document.getElementById('searchBox').addEventListener('keydown', e => { if (e.key === 'Enter') loadData(); });
    loadData();
    loadStatsColumns();
    loadChartColumns();
    loadPBPSTiles();
    loadLibraries();
}

// Data browser
async function loadData() {
    const view = document.getElementById('viewSelect').value;
    const search = document.getElementById('searchBox').value;
    const limit = document.getElementById('pageSizeSelect').value;
    let url = `/api/data?view=${encodeURIComponent(view)}&limit=${limit}&offset=${currentOffset}&search=${encodeURIComponent(search)}`;
    if (currentSort) url += `&sort=${encodeURIComponent(currentSort)}&dir=${currentDir}`;

    const resp = await fetch(url);
    const data = await resp.json();
    currentTotal = data.total;
    cachedColumns[view] = data.columns;

    const thead = document.querySelector('#dataTable thead');
    thead.innerHTML = '<tr>' + data.columns.map(c => {
        const arrow = currentSort === c ? (currentDir === 'asc' ? '▲' : '▼') : '';
        return `<th onclick="sortBy('${c}')">${c}<span class="sort-arrow">${arrow}</span></th>`;
    }).join('') + '</tr>';

    const tbody = document.querySelector('#dataTable tbody');
    tbody.innerHTML = data.rows.map(row =>
        '<tr>' + data.columns.map(c => `<td title="${escHtml(String(row[c] ?? ''))}">${escHtml(fmt(row[c]))}</td>`).join('') + '</tr>'
    ).join('');

    document.getElementById('rowCount').textContent = `${data.total.toLocaleString()} rows`;
    updatePageInfo();
}

function fmt(v) {
    if (v === null || v === undefined) return '';
    if (typeof v === 'number') {
        if (Number.isInteger(v) && Math.abs(v) > 1000000) return v.toLocaleString();
        if (!Number.isInteger(v)) return parseFloat(v.toFixed(3)).toString();
    }
    return String(v);
}

function escHtml(s) { return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

function geekseekUrl(fileName, filePath) {
    const isTV = /\\TV\\/i.test(filePath) || /\/TV\//i.test(filePath);
    const category = isTV ? '5000' : '2000';
    let name = fileName || '';
    name = name.replace(/\.[^.]+$/, '');
    name = name.replace(/\{[^}]*\}/g, '');
    name = name.replace(/[._]/g, ' ');
    name = name.replace(/\s*S\d{1,2}E\d{1,2}.*/i, '');
    name = name.replace(/\s*(1080p|720p|480p|2160p|4k|x264|x265|h264|h265|hevc|aac|bluray|bdrip|webrip|web-dl|hdtv|remux|dts|atmos)\b.*/i, '');
    name = name.replace(/\s*[\(\[]\d{4}[\)\]]\s*$/, '');
    name = name.trim();
    return `https://nzbgeek.info/geekseek.php?moviesgeekseek=1&c=${category}&browseincludewords=${encodeURIComponent(name)}`;
}

// Reusable client-side sortable table
function renderSortableTable(wrapId, columns, rows) {
    const state = renderSortableTable._state = renderSortableTable._state || {};
    if (!state[wrapId]) state[wrapId] = { col: '', dir: 'asc', columns: [], rows: [] };
    if (columns) { state[wrapId].columns = columns; state[wrapId].rows = rows; state[wrapId].col = ''; state[wrapId].dir = 'asc'; }
    const s = state[wrapId];
    let sorted = [...s.rows];
    if (s.col) {
        sorted.sort((a, b) => {
            let va = a[s.col], vb = b[s.col];
            if (va == null && vb == null) return 0;
            if (va == null) return 1;
            if (vb == null) return -1;
            if (typeof va === 'number' && typeof vb === 'number') return s.dir === 'asc' ? va - vb : vb - va;
            va = String(va).toLowerCase(); vb = String(vb).toLowerCase();
            return s.dir === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
        });
    }
    const arrow = c => s.col === c ? (s.dir === 'asc' ? ' ▲' : ' ▼') : '';
    let html = '<table><thead><tr>' + s.columns.map(c =>
        `<th onclick="sortTableCol('${wrapId}','${c.replace(/'/g,"\\'")}')">${escHtml(c)}<span class="sort-arrow">${arrow(c)}</span></th>`
    ).join('') + '</tr></thead><tbody>';
    html += sorted.map(row => '<tr>' + s.columns.map(c => {
        let cls = '';
        if (c === 'file_name') cls = ' class="clickable"';
        else if (c === 'file_path') cls = ' class="clickable-path"';
        const val = escHtml(fmt(row[c]));
        const title = escHtml(String(row[c] ?? ''));
        if (c === 'file_name' && row['file_path'] != null) {
            const href = escHtml(geekseekUrl(String(row[c] ?? ''), String(row['file_path'] ?? '')));
            return `<td${cls} data-col="${escHtml(c)}" title="${title}"><a href="${href}" target="_blank" rel="noopener" style="color:var(--green);text-decoration:none">${val}</a></td>`;
        }
        return `<td${cls} data-col="${escHtml(c)}" title="${title}">${val}</td>`;
    }).join('') + '</tr>').join('');
    html += '</tbody></table>';
    document.getElementById(wrapId).innerHTML = html;
}

function sortTableCol(wrapId, col) {
    const s = renderSortableTable._state[wrapId];
    if (s.col === col) s.dir = s.dir === 'asc' ? 'desc' : 'asc';
    else { s.col = col; s.dir = 'asc'; }
    renderSortableTable(wrapId);
}

// Click handlers for file_name (geekseek) and file_path (copy to clipboard)
function handleTableClick(e) {
    const td = e.target.closest('td');
    if (!td) return;
    const col = td.dataset.col;
    const tr = td.closest('tr');
    if (!tr) return;

    if (col === 'file_path') {
        const path = td.title || td.textContent;
        navigator.clipboard.writeText(path).then(() => {
            td.classList.add('copied-flash');
            setTimeout(() => td.classList.remove('copied-flash'), 600);
        });
    }
}
document.getElementById('pbpsTableWrap').addEventListener('click', handleTableClick);
document.getElementById('upgradeTableWrap').addEventListener('click', handleTableClick);

function sortBy(col) {
    if (currentSort === col) currentDir = currentDir === 'asc' ? 'desc' : 'asc';
    else { currentSort = col; currentDir = 'asc'; }
    currentOffset = 0;
    loadData();
}

function pageNext() {
    const limit = parseInt(document.getElementById('pageSizeSelect').value);
    if (currentOffset + limit < currentTotal) { currentOffset += limit; loadData(); }
}
function pagePrev() {
    const limit = parseInt(document.getElementById('pageSizeSelect').value);
    currentOffset = Math.max(0, currentOffset - limit); loadData();
}
function updatePageInfo() {
    const limit = parseInt(document.getElementById('pageSizeSelect').value);
    const page = Math.floor(currentOffset / limit) + 1;
    const pages = Math.ceil(currentTotal / limit);
    document.getElementById('pageInfo').textContent = `Page ${page} of ${pages}`;
}

// Statistics
async function loadStatsColumns() {
    const view = document.getElementById('statsViewSelect').value;
    const resp = await fetch(`/api/data?view=${encodeURIComponent(view)}&limit=1`);
    const data = await resp.json();
    const sel = document.getElementById('statsGroupSelect');
    sel.innerHTML = '<option value="">(none / overall)</option>' +
        data.columns.map(c => `<option value="${c}">${c}</option>`).join('');
}

async function loadStats() {
    const view = document.getElementById('statsViewSelect').value;
    const group = document.getElementById('statsGroupSelect').value;
    const resp = await fetch(`/api/stats?view=${encodeURIComponent(view)}&group_by=${encodeURIComponent(group)}`);
    const data = await resp.json();

    if (!data.numeric_columns.length) {
        document.getElementById('statsTableWrap').innerHTML = '<p style="color:var(--text2)">No numeric columns found.</p>';
        return;
    }

    let html = '<table class="stats-table"><thead><tr>';
    if (data.group_by) html += `<th>${escHtml(data.group_by)}</th>`;
    html += '<th>Column</th><th>Count</th><th>Mean</th><th>Std Dev</th><th>Min</th><th>Max</th></tr></thead><tbody>';

    for (const row of data.stats) {
        const groupVal = data.group_by ? row[data.group_by] : null;
        for (let i = 0; i < data.numeric_columns.length; i++) {
            const col = data.numeric_columns[i];
            html += '<tr>';
            if (data.group_by && i === 0) html += `<td rowspan="${data.numeric_columns.length}">${escHtml(String(groupVal ?? ''))}</td>`;
            html += `<td>${escHtml(col)}</td>`;
            html += `<td>${fmt(row[col+'_count'])}</td>`;
            html += `<td>${fmt(row[col+'_mean'])}</td>`;
            html += `<td>${fmt(row[col+'_std'])}</td>`;
            html += `<td>${fmt(row[col+'_min'])}</td>`;
            html += `<td>${fmt(row[col+'_max'])}</td>`;
            html += '</tr>';
        }
    }
    html += '</tbody></table>';
    document.getElementById('statsTableWrap').innerHTML = html;
}

// Charts
async function loadChartColumns() {
    const view = document.getElementById('chartViewSelect').value;
    const resp = await fetch(`/api/data?view=${encodeURIComponent(view)}&limit=1`);
    const data = await resp.json();
    for (const selId of ['chartX', 'chartY']) {
        document.getElementById(selId).innerHTML = data.columns.map(c => `<option value="${c}">${c}</option>`).join('');
    }
    document.getElementById('chartColor').innerHTML = '<option value="">(none)</option>' +
        data.columns.map(c => `<option value="${c}">${c}</option>`).join('');
}

async function renderChart() {
    const view = document.getElementById('chartViewSelect').value;
    const chartType = document.getElementById('chartType').value;
    const xCol = document.getElementById('chartX').value;
    const yCol = document.getElementById('chartY').value;
    const colorCol = document.getElementById('chartColor').value;
    const agg = document.getElementById('chartAgg').value;

    let sql;
    if (agg && colorCol) {
        sql = `SELECT [${xCol}], [${colorCol}], ${agg}([${yCol}]) as [${yCol}] FROM [${view}] WHERE [${yCol}] IS NOT NULL GROUP BY [${xCol}], [${colorCol}] ORDER BY [${xCol}]`;
    } else if (agg) {
        sql = `SELECT [${xCol}], ${agg}([${yCol}]) as [${yCol}] FROM [${view}] WHERE [${yCol}] IS NOT NULL GROUP BY [${xCol}] ORDER BY [${xCol}]`;
    } else {
        sql = `SELECT [${xCol}], [${yCol}]${colorCol ? ', ['+colorCol+']' : ''} FROM [${view}] WHERE [${yCol}] IS NOT NULL LIMIT 5000`;
    }

    const resp = await fetch(`/api/query?sql=${encodeURIComponent(sql)}`);
    const data = await resp.json();
    if (data.error) { alert('Query error: ' + data.error); return; }

    const layout = {
        paper_bgcolor: '#1a1a2e', plot_bgcolor: '#16213e',
        font: { color: '#e0e0e0' },
        xaxis: { title: xCol + (agg ? '' : ''), gridcolor: '#0f3460' },
        yaxis: { title: (agg ? agg + '(' + yCol + ')' : yCol), gridcolor: '#0f3460' },
        margin: { t: 40, r: 20 },
    };

    let traces = [];

    if (colorCol && chartType !== 'histogram' && chartType !== 'pie') {
        const groups = {};
        for (const row of data.rows) {
            const g = String(row[colorCol] ?? 'null');
            if (!groups[g]) groups[g] = { x: [], y: [] };
            groups[g].x.push(row[xCol]);
            groups[g].y.push(row[yCol]);
        }
        for (const [name, vals] of Object.entries(groups)) {
            traces.push({ x: vals.x, y: vals.y, name, type: chartType === 'scatter' ? 'scatter' : chartType, mode: chartType === 'scatter' ? 'markers' : undefined });
        }
    } else if (chartType === 'histogram') {
        traces = [{ x: data.rows.map(r => r[yCol]), type: 'histogram', marker: { color: '#e94560' } }];
        layout.xaxis.title = yCol;
        layout.yaxis.title = 'frequency';
    } else if (chartType === 'pie') {
        traces = [{ labels: data.rows.map(r => r[xCol]), values: data.rows.map(r => r[yCol]), type: 'pie', textfont: { color: '#fff' } }];
    } else if (chartType === 'box') {
        const groups = {};
        for (const row of data.rows) {
            const g = String(row[xCol] ?? 'null');
            if (!groups[g]) groups[g] = [];
            groups[g].push(row[yCol]);
        }
        for (const [name, vals] of Object.entries(groups)) {
            traces.push({ y: vals, name, type: 'box' });
        }
    } else {
        traces = [{ x: data.rows.map(r => r[xCol]), y: data.rows.map(r => r[yCol]),
                     type: chartType === 'scatter' ? 'scatter' : 'bar',
                     mode: chartType === 'scatter' ? 'markers' : undefined,
                     marker: { color: '#e94560' } }];
    }

    Plotly.newPlot('chart', traces, layout, { responsive: true });
}

// SQL Console
async function runSQL() {
    const sql = document.getElementById('sqlInput').value.trim();
    if (!sql) return;
    const resp = await fetch(`/api/query?sql=${encodeURIComponent(sql)}`);
    const data = await resp.json();
    if (data.error) {
        document.getElementById('sqlInfo').textContent = 'Error: ' + data.error;
        document.getElementById('sqlTableWrap').innerHTML = '';
        return;
    }
    document.getElementById('sqlInfo').textContent = `${data.total} rows returned`;
    renderSortableTable('sqlTableWrap', data.columns, data.rows);
}

document.getElementById('sqlInput').addEventListener('keydown', e => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') runSQL();
});

// Normalized PBPS — tiles
let pbpsTilesLoaded = false;

async function loadPBPSTiles(force) {
    if (pbpsTilesLoaded && !force) return;
    try {
        const resp = await fetch('/api/pbps/tiles');
        const t = await resp.json();
        if (t.error) return;

        // RVA combined tile
        document.getElementById('tileRVAUp').textContent = (t.rva_up_count ?? 0).toLocaleString();
        document.getElementById('tileRVADown').textContent = (t.rva_down_count ?? 0).toLocaleString();

        // BPP
        document.getElementById('tileBPPMedian').textContent = t.median_bpp_sec != null ? Number(t.median_bpp_sec).toPrecision(3) : '--';
        document.getElementById('tileBPPMean').textContent = t.mean_bpp_sec != null ? Number(t.mean_bpp_sec).toPrecision(3) : '--';

        // Legacy (red) and Modern (green) tiles
        const total = t.total_files || 1;
        const h264 = t.h264_count ?? 0;
        const lowRes = t.low_res_count ?? 0;
        const av1 = t.av1_count ?? 0;
        const highRes = t.high_res_count ?? 0;
        document.getElementById('tileLegH264').innerHTML = `${h264.toLocaleString()} <span class="tile-pct">(${Math.round(h264/total*100)}%)</span>`;
        document.getElementById('tileLeg720p').innerHTML = `${lowRes.toLocaleString()} <span class="tile-pct">(${Math.round(lowRes/total*100)}%)</span>`;
        document.getElementById('tileModAV1').innerHTML = `${av1.toLocaleString()} <span class="tile-pct">(${Math.round(av1/total*100)}%)</span>`;
        document.getElementById('tileMod1080p').innerHTML = `${highRes.toLocaleString()} <span class="tile-pct">(${Math.round(highRes/total*100)}%)</span>`;

        // Codec mini histogram
        const codecs = t.codecs || [];
        const maxCount = Math.max(...codecs.map(c => c.count), 1);
        const codecColors = ['var(--green)', 'var(--accent2)', '#e0a030', 'var(--border)'];
        document.getElementById('codecBars').innerHTML = codecs.map((c, i) => {
            const pct = Math.round(c.count / maxCount * 100);
            return `<div class="mini-bar"><span class="mini-bar-count">${c.count.toLocaleString()}</span><div class="mini-bar-fill" style="height:${pct}%;background:${codecColors[i] || codecColors[3]}"></div><span class="mini-bar-label">${escHtml(c.name)}</span></div>`;
        }).join('');

        // Resolution mini histogram
        const resData = t.resolution || [];
        const maxRes = Math.max(...resData.map(r => r.count), 1);
        const resColors = ['var(--green)', 'var(--green)', '#e0a030', 'var(--accent)', 'var(--border)'];
        document.getElementById('resBars').innerHTML = resData.map((r, i) => {
            const pct = Math.round(r.count / maxRes * 100);
            return `<div class="mini-bar"><span class="mini-bar-count">${r.count.toLocaleString()}</span><div class="mini-bar-fill" style="height:${pct}%;background:${resColors[i] || resColors[4]}"></div><span class="mini-bar-label">${escHtml(r.name)}</span></div>`;
        }).join('');

        // Ratio distribution histogram (0.1 buckets, 0.4–1.6)
        const hist = t.ratio_hist || [];
        const NUM_BARS = 12, START_BUCKET = 4;
        const buckets = new Array(NUM_BARS).fill(0);
        for (const h of hist) {
            const idx = h.bucket - START_BUCKET;
            if (idx >= 0 && idx < NUM_BARS) buckets[idx] = h.count;
        }
        const maxH = Math.max(...buckets, 1);
        const linePos = (6.5 / NUM_BARS) * 100;
        let histHtml = `<div class="rh-line" style="left:${linePos}%"></div>`;
        // Color gradient: red at edges → green at center (index 6)
        histHtml += buckets.map((cnt, i) => {
            const lo = ((START_BUCKET + i) * 0.1).toFixed(1);
            const hi = ((START_BUCKET + i + 1) * 0.1).toFixed(1);
            const pct = Math.round(cnt / maxH * 100);
            const dist = Math.abs(i - 6);
            const color = dist === 0 ? 'var(--green)' : dist <= 2 ? '#4e8' : dist <= 4 ? '#e0a030' : 'var(--accent)';
            return `<div class="rh-bar" style="height:${Math.max(pct, 2)}%;background:${color}" data-tip="${lo}–${hi}: ${cnt.toLocaleString()}"></div>`;
        }).join('');
        document.getElementById('ratioHist').innerHTML = histHtml +
            '<div class="ratio-hist-labels" style="position:absolute;bottom:-10px;left:0;right:0"><span>0.4</span><span>1.0</span><span>1.6</span></div>';

        pbpsTilesLoaded = true;
    } catch (e) { /* network error, leave as -- */ }
}

async function loadPBPS() {
    const baseSql = `WITH bpp AS (
    SELECT *,
           file_size_bytes * 1.0 / (width * height * duration_seconds) AS bpp_sec
    FROM v_video_summary
    WHERE width > 0 AND height > 0 AND duration_seconds > 60
),
avg_bpp AS (
    SELECT resolution_class,
           AVG(bpp_sec) AS avg_bpp_sec
    FROM bpp
    GROUP BY resolution_class
)
SELECT b.file_name,
       b.file_path,
       b.video_codec,
       b.resolution_class,
       ROUND(b.bpp_sec, 6) AS bpp_sec,
       ROUND(a.avg_bpp_sec, 6) AS avg_for_res,
       ROUND(b.bpp_sec / a.avg_bpp_sec, 4) AS ratio_vs_avg,
       ROUND(b.file_size_bytes / 1048576.0, 1) AS size_mb,
       ROUND(b.duration_seconds / 60.0, 1) AS dur_min
FROM bpp b
JOIN avg_bpp a ON b.resolution_class = a.resolution_class
WHERE dur_min > 10`;

    // Fetch worst (DESC) and best (ASC) in parallel
    const [descResp, ascResp] = await Promise.all([
        fetch(`/api/query?sql=${encodeURIComponent(baseSql + ' ORDER BY ratio_vs_avg DESC')}`),
        fetch(`/api/query?sql=${encodeURIComponent(baseSql + ' ORDER BY ratio_vs_avg ASC')}`),
    ]);
    const descData = await descResp.json();
    const ascData = await ascResp.json();
    if (descData.error) {
        document.getElementById('pbpsInfo').textContent = 'Error: ' + descData.error;
        document.getElementById('pbpsTableWrap').innerHTML = '';
        return;
    }

    // Combine: DESC rows first, then ASC rows not already included
    const seen = new Set(descData.rows.map(r => r.file_path));
    const ascExtra = ascData.rows.filter(r => !seen.has(r.file_path));
    const combined = [...descData.rows, ...ascExtra];

    document.getElementById('pbpsInfo').textContent = `${combined.length} rows (worst → best)`;
    renderSortableTable('pbpsTableWrap', descData.columns, combined);
}

// Distributions
let distLoaded = false;

async function loadDistributions(force) {
    if (distLoaded && !force) { document.getElementById('distInfo').textContent = '(cached)'; return; }
    document.getElementById('distInfo').textContent = 'Loading...';
    const resp = await fetch('/api/distributions');
    const d = await resp.json();
    if (d.error) { document.getElementById('distInfo').textContent = 'Error: ' + d.error; return; }

    const darkLayout = {
        paper_bgcolor: '#1a1a2e', plot_bgcolor: '#16213e',
        font: { color: '#e0e0e0' },
        xaxis: { title: 'Ratio vs Average', gridcolor: '#0f3460' },
        yaxis: { title: 'File Count', gridcolor: '#0f3460' },
        barmode: 'stack',
        margin: { t: 30, r: 20 },
        shapes: [{
            type: 'line', x0: '1.0', x1: '1.0', y0: 0, y1: 1, yref: 'paper',
            line: { color: '#e0e0e0', width: 1.5 }
        }],
        legend: { orientation: 'h', y: -0.15 },
    };

    // Codec chart
    const codecColors = ['#4ecca3', '#533483', '#e0a030', '#e94560', '#5b8def', '#888'];
    const codecTraces = d.codec_series.map((s, i) => ({
        x: d.buckets, y: s.values, name: s.name, type: 'bar',
        marker: { color: codecColors[i % codecColors.length] },
    }));
    Plotly.newPlot('distCodecChart', codecTraces, {...darkLayout, xaxis: {...darkLayout.xaxis, title: 'Ratio vs Average (by Codec)'}}, { responsive: true });

    // Resolution chart
    const resColors = { '4K': '#4ecca3', '1080p': '#5b8def', '720p': '#e0a030', '480p': '#e94560', 'other': '#888' };
    const resTraces = d.res_series.map(s => ({
        x: d.buckets, y: s.values, name: s.name, type: 'bar',
        marker: { color: resColors[s.name] || '#888' },
    }));
    Plotly.newPlot('distResChart', resTraces, {...darkLayout, xaxis: {...darkLayout.xaxis, title: 'Ratio vs Average (by Resolution)'}}, { responsive: true });

    // Violin plots
    const vResp = await fetch('/api/violins');
    const vData = await vResp.json();
    const violinColors = { '4K': '#4ecca3', '1080p': '#5b8def', '720p': '#e0a030', '480p': '#e94560', 'other': '#888' };
    const violinTraces = vData.map(s => ({
        type: 'violin', y: s.values, name: s.name,
        box: { visible: true }, meanline: { visible: true },
        marker: { color: violinColors[s.name] || '#888' },
        line: { color: violinColors[s.name] || '#888' },
    }));
    Plotly.newPlot('distViolinChart', violinTraces, {
        ...darkLayout,
        xaxis: { ...darkLayout.xaxis, title: 'Resolution Class' },
        yaxis: { ...darkLayout.yaxis, title: 'Ratio vs Average' },
        shapes: [{ type: 'line', x0: -0.5, x1: vData.length - 0.5, y0: 1, y1: 1,
                   line: { color: '#e0e0e0', width: 1, dash: 'dash' } }],
    }, { responsive: true });

    document.getElementById('distInfo').textContent = '';
    distLoaded = true;
}

// Quality Map (Heatmap + Sankey)
let qualityLoaded = false;

async function loadQualityMap(force) {
    if (qualityLoaded && !force) { document.getElementById('qualityInfo').textContent = '(cached)'; return; }
    document.getElementById('qualityInfo').textContent = 'Loading...';
    const resp = await fetch('/api/quality');
    const q = await resp.json();
    if (q.error) { document.getElementById('qualityInfo').textContent = 'Error: ' + q.error; return; }

    const darkLayout = {
        paper_bgcolor: '#1a1a2e', plot_bgcolor: '#16213e',
        font: { color: '#e0e0e0' }, margin: { t: 30, r: 20 },
    };

    // Heatmap — green at 1.0, yellow/orange/red toward extremes, blank for zero files
    const hm = q.heatmap;
    const hoverText = hm.codecs.map((c, ci) =>
        hm.resolutions.map((r, ri) => hm.values[ci][ri] != null
            ? `${c} / ${r}<br>Avg ratio: ${hm.values[ci][ri]}<br>Files: ${hm.counts[ci][ri]}`
            : `${c} / ${r}<br>No files`)
    );
    Plotly.newPlot('qualityHeatmap', [{
        type: 'heatmap',
        z: hm.values, x: hm.resolutions, y: hm.codecs,
        text: hoverText, hoverinfo: 'text',
        colorscale: [
            [0, '#e94560'],     // far below avg — red
            [0.25, '#e0a030'],  // below avg — orange
            [0.5, '#4ecca3'],   // at 1.0 — green (ideal)
            [0.75, '#e0a030'],  // above avg — orange
            [1, '#e94560'],     // far above avg — red
        ],
        zmid: 1.0,
        zmin: 0.3,
        zmax: 1.7,
        connectgaps: false,
        colorbar: { title: 'Ratio', tickvals: [0.5, 0.75, 1.0, 1.25, 1.5] },
    }], {
        ...darkLayout,
        xaxis: { title: 'Resolution', color: '#e0e0e0' },
        yaxis: { title: 'Codec (oldest → newest)', color: '#e0e0e0' },
        margin: { t: 30, r: 100, l: 120 },
    }, { responsive: true });

    // Sankey
    const sk = q.sankey;
    Plotly.newPlot('qualitySankey', [{
        type: 'sankey',
        orientation: 'h',
        node: {
            label: sk.nodes,
            color: sk.node_colors,
            pad: 15, thickness: 20,
            line: { color: '#0f3460', width: 1 },
        },
        link: {
            source: sk.links_src,
            target: sk.links_tgt,
            value: sk.links_val,
            color: 'rgba(78, 204, 163, 0.15)',
        },
    }], {
        ...darkLayout,
        margin: { t: 20, r: 20, b: 20, l: 20 },
    }, { responsive: true });

    // Scatter comparison charts: same points, different x-axes
    const sc = q.scatter;
    const codecAge = {msmpeg4v3:0, mpeg4:1, mpeg2video:2, vc1:3, h264:4, hevc:5, av1:6, vvc:7};
    const codecColor = {msmpeg4v3:'#e94560', mpeg4:'#d35530', mpeg2video:'#c97030', vc1:'#b08030',
                        h264:'#e0a030', hevc:'#4ecca3', av1:'#5b8def', vvc:'#00e5ff'};
    const codecs = Object.keys(sc).sort((a,b) => (codecAge[a]??99) - (codecAge[b]??99));
    const scatterConfigs = [
        {
            elementId: 'qualityScatterExpected',
            title: 'X = Expected Size At Average Density',
            xKey: 'expected_size_mb',
            xLabel: 'Expected size at average density (MB)',
            xType: 'log',
            xValue: point => Number(point.expected_size_mb).toLocaleString(undefined, { maximumFractionDigits: 2 }),
            xHoverLabel: 'Expected size',
        },
        {
            elementId: 'qualityScatterDuration',
            title: 'X = Duration',
            xKey: 'duration_min',
            xLabel: 'Duration (minutes)',
            xType: 'log',
            xValue: point => Number(point.duration_min).toLocaleString(undefined, { maximumFractionDigits: 2 }),
            xHoverLabel: 'Duration',
        },
        {
            elementId: 'qualityScatterSize',
            title: 'X = Actual Size',
            xKey: 'size_mb',
            xLabel: 'Actual file size (MB)',
            xType: 'log',
            xValue: point => Number(point.size_mb).toLocaleString(undefined, { maximumFractionDigits: 2 }),
            xHoverLabel: 'Actual size',
        },
    ];

    function pointHoverText(point, xHoverLabel, xValue) {
        return [
            escHtml(point.file_name),
            `Resolution class: ${escHtml(point.resolution_class)}`,
            `${xHoverLabel}: ${xValue(point)}`,
            `Ratio: ${Number(point.ratio).toFixed(3)}`,
            `Actual size: ${Number(point.size_mb).toLocaleString(undefined, { maximumFractionDigits: 2 })} MB`,
            `Expected size: ${Number(point.expected_size_mb).toLocaleString(undefined, { maximumFractionDigits: 2 })} MB`,
            `Duration: ${Number(point.duration_min).toLocaleString(undefined, { maximumFractionDigits: 2 })} min`,
            `Pixels: ${Number(point.pixels).toLocaleString()}`,
            `BPP/sec: ${Number(point.bpp_sec).toLocaleString(undefined, { maximumFractionDigits: 6 })}`,
            `Avg BPP/sec: ${Number(point.avg_bpp_sec).toLocaleString(undefined, { maximumFractionDigits: 6 })}`,
        ].join('<br>');
    }

    function renderQualityScatter(config) {
        const traces = codecs.map(codec => {
            const points = (sc[codec] && sc[codec].points ? sc[codec].points : []).filter(point => Number(point[config.xKey]) > 0);
            return {
                x: points.map(point => point[config.xKey]),
                y: points.map(point => point.ratio),
                text: points.map(point => pointHoverText(point, config.xHoverLabel, config.xValue)),
                name: codec,
                type: 'scattergl',
                mode: 'markers',
                marker: { color: codecColor[codec] || '#888', size: 4, opacity: 0.6 },
                hovertemplate: '%{text}<extra>' + codec + '</extra>',
            };
        }).filter(trace => trace.x.length > 0);

        Plotly.newPlot(config.elementId, traces, {
            ...darkLayout,
            title: { text: config.title, font: { color: '#e0e0e0', size: 16 } },
            xaxis: { title: config.xLabel, type: config.xType, gridcolor: '#0f3460', color: '#e0e0e0' },
            yaxis: { title: 'Ratio vs Average', gridcolor: '#0f3460', color: '#e0e0e0' },
            shapes: [{ type: 'line', x0: 0, x1: 1, xref: 'paper', y0: 1, y1: 1,
                       line: { color: '#e0e0e0', width: 1, dash: 'dash' } }],
            margin: { t: 50, r: 20 },
            legend: { orientation: 'h', y: -0.15 },
        }, { responsive: true });
    }

    scatterConfigs.forEach(renderQualityScatter);

    document.getElementById('qualityInfo').textContent = '';
    qualityLoaded = true;
}

// Upgrade Priority List
let upgradeLoaded = false;

async function loadUpgrades(force) {
    if (upgradeLoaded && !force) { document.getElementById('upgradeInfo').textContent = '(cached)'; return; }
    document.getElementById('upgradeInfo').textContent = 'Loading...';
    const resp = await fetch('/api/upgrades');
    const data = await resp.json();
    if (data.error) { document.getElementById('upgradeInfo').textContent = 'Error: ' + data.error; return; }
    document.getElementById('upgradeInfo').textContent = `${data.total} files need attention`;
    renderSortableTable('upgradeTableWrap', data.columns, data.rows);
    upgradeLoaded = true;
}

// Progress tracking
async function loadProgress() {
    const resp = await fetch('/api/snapshots');
    const snaps = await resp.json();
    if (!snaps.length) {
        document.getElementById('progressInfo').textContent = 'No snapshots yet. Run a scan or click "Take Snapshot Now".';
        return;
    }
    document.getElementById('progressInfo').textContent = `${snaps.length} snapshot(s)`;
    const dates = snaps.map(s => s.scanned_at);

    const darkLayout = {
        paper_bgcolor: '#1a1a2e', plot_bgcolor: '#16213e',
        font: { color: '#e0e0e0' },
        xaxis: { gridcolor: '#0f3460' },
        yaxis: { gridcolor: '#0f3460', title: 'Files' },
        margin: { t: 30, r: 20 },
        legend: { orientation: 'h', y: -0.2 },
    };

    // Codec migration: h264, hevc, av1 over time
    Plotly.newPlot('progressCodecChart', [
        { x: dates, y: snaps.map(s => s.h264_count), name: 'h264', line: { color: '#e94560' } },
        { x: dates, y: snaps.map(s => s.hevc_count), name: 'hevc', line: { color: '#4ecca3' } },
        { x: dates, y: snaps.map(s => s.av1_count), name: 'av1', line: { color: '#5b8def' } },
    ], { ...darkLayout, yaxis: { ...darkLayout.yaxis, title: 'File Count' } }, { responsive: true });

    // Resolution upgrades
    Plotly.newPlot('progressResChart', [
        { x: dates, y: snaps.map(s => s.res_4k), name: '4K', line: { color: '#4ecca3' } },
        { x: dates, y: snaps.map(s => s.res_1080p), name: '1080p', line: { color: '#5b8def' } },
        { x: dates, y: snaps.map(s => s.res_720p), name: '720p', line: { color: '#e0a030' } },
        { x: dates, y: snaps.map(s => s.res_480p), name: '480p', line: { color: '#e94560' } },
        { x: dates, y: snaps.map(s => s.res_other), name: 'other', line: { color: '#888' } },
    ], { ...darkLayout, yaxis: { ...darkLayout.yaxis, title: 'File Count' } }, { responsive: true });

    // Quality: RVA counts and mean BPP
    Plotly.newPlot('progressQualityChart', [
        { x: dates, y: snaps.map(s => s.rva_up_count), name: 'RVA >1.5x', line: { color: '#e94560' } },
        { x: dates, y: snaps.map(s => s.rva_down_count), name: 'RVA <0.5x', line: { color: '#e0a030' } },
        { x: dates, y: snaps.map(s => s.total_files), name: 'Total Files', line: { color: '#4ecca3', dash: 'dot' }, yaxis: 'y2' },
    ], {
        ...darkLayout,
        yaxis: { ...darkLayout.yaxis, title: 'RVA Count' },
        yaxis2: { title: 'Total Files', overlaying: 'y', side: 'right', gridcolor: 'transparent', color: '#4ecca3' },
    }, { responsive: true });

    // Snapshot management table
    let thtml = '<table><thead><tr><th>#</th><th>Date</th><th>Files</th><th>Size (GB)</th><th>h264</th><th>hevc</th><th>av1</th><th>1080p</th><th>720p</th><th>RVA&gt;1.5</th><th>RVA&lt;0.5</th><th></th></tr></thead><tbody>';
    thtml += snaps.map(s =>
        `<tr><td>${s.id}</td><td>${s.scanned_at}</td><td>${s.total_files}</td><td>${s.total_size_gb}</td>` +
        `<td>${s.h264_count}</td><td>${s.hevc_count}</td><td>${s.av1_count}</td>` +
        `<td>${s.res_1080p}</td><td>${s.res_720p}</td>` +
        `<td>${s.rva_up_count}</td><td>${s.rva_down_count}</td>` +
        `<td><button class="remove-btn" onclick="deleteSnapshot(${s.id})" title="Delete">&times;</button></td></tr>`
    ).join('');
    thtml += '</tbody></table>';
    document.getElementById('snapshotTableWrap').innerHTML = thtml;
}

async function captureSnapshot() {
    const resp = await fetch('/api/snapshots/capture', { method: 'POST' });
    const result = await resp.json();
    if (result.ok) {
        loadProgress();
    } else {
        alert(result.error || 'Failed to capture snapshot');
    }
}

async function deleteSnapshot(id) {
    if (!confirm('Delete snapshot #' + id + '?')) return;
    const resp = await fetch('/api/snapshots/delete', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ id }),
    });
    const result = await resp.json();
    if (result.ok) loadProgress();
    else alert(result.error || 'Delete failed');
}

// Settings panel — multi-library
let settingsConfig = null;
let settingsPrevLibIdx = 0;

function switchSettingsLib() {
    // Save fields for the previous library before switching
    const prevIdx = settingsPrevLibIdx;
    const sel = document.getElementById('settingsLibSelect');
    const newIdx = parseInt(sel.value);
    if (settingsConfig.libraries[prevIdx]) {
        settingsConfig.libraries[prevIdx].name = document.getElementById('libName').value.trim() || 'Unnamed';
        settingsConfig.libraries[prevIdx].db = document.getElementById('libDB').value.trim() || 'media.db';
        settingsConfig.libraries[prevIdx].scan_folders = JSON.parse(document.getElementById('folderList').dataset.items || '[]');
        settingsConfig.libraries[prevIdx].ignore_patterns = JSON.parse(document.getElementById('patternList').dataset.items || '[]');
    }
    settingsPrevLibIdx = newIdx;
    loadLibraryFields();
}

async function openSettings() {
    await loadSettings();
    document.getElementById('settingsOverlay').classList.add('open');
}

function closeSettings() {
    document.getElementById('settingsOverlay').classList.remove('open');
}

async function loadSettings() {
    const resp = await fetch('/api/config');
    settingsConfig = await resp.json();
    const sel = document.getElementById('settingsLibSelect');
    const libs = settingsConfig.libraries || [];
    sel.innerHTML = libs.map((l, i) => `<option value="${i}">${escHtml(l.name)}</option>`).join('');
    // Select the active library
    const activeIdx = libs.findIndex(l => l.name === settingsConfig.active_library);
    if (activeIdx >= 0) sel.value = activeIdx;
    loadLibraryFields();
    document.getElementById('ffprobePath').value = settingsConfig.ffprobe_path || '';
    document.getElementById('workerCount').value = settingsConfig.workers || 8;
}

function loadLibraryFields() {
    const idx = parseInt(document.getElementById('settingsLibSelect').value);
    settingsPrevLibIdx = idx;
    const libs = settingsConfig.libraries || [];
    const lib = libs[idx] || { name: '', db: '', scan_folders: [], ignore_patterns: [] };
    document.getElementById('libName').value = lib.name || '';
    document.getElementById('libDB').value = lib.db || '';
    renderList('folder', lib.scan_folders || []);
    renderList('pattern', lib.ignore_patterns || []);
}

function saveLibraryFields() {
    const idx = parseInt(document.getElementById('settingsLibSelect').value);
    if (!settingsConfig.libraries[idx]) return;
    settingsConfig.libraries[idx].name = document.getElementById('libName').value.trim() || 'Unnamed';
    settingsConfig.libraries[idx].db = document.getElementById('libDB').value.trim() || 'media.db';
    settingsConfig.libraries[idx].scan_folders = JSON.parse(document.getElementById('folderList').dataset.items || '[]');
    settingsConfig.libraries[idx].ignore_patterns = JSON.parse(document.getElementById('patternList').dataset.items || '[]');
}

function addLibrary() {
    saveLibraryFields();
    const name = prompt('Library name:');
    if (!name) return;
    const db = prompt('Database filename:', name.toLowerCase().replace(/\s+/g, '_') + '.db');
    if (!db) return;
    settingsConfig.libraries.push({ name, db, scan_folders: [], ignore_patterns: [] });
    const sel = document.getElementById('settingsLibSelect');
    sel.innerHTML = settingsConfig.libraries.map((l, i) => `<option value="${i}">${escHtml(l.name)}</option>`).join('');
    sel.value = settingsConfig.libraries.length - 1;
    loadLibraryFields();
}

function removeLibrary() {
    const idx = parseInt(document.getElementById('settingsLibSelect').value);
    if (settingsConfig.libraries.length <= 1) { alert('Cannot remove the last library'); return; }
    if (!confirm(`Remove library "${settingsConfig.libraries[idx].name}"?`)) return;
    settingsConfig.libraries.splice(idx, 1);
    const sel = document.getElementById('settingsLibSelect');
    sel.innerHTML = settingsConfig.libraries.map((l, i) => `<option value="${i}">${escHtml(l.name)}</option>`).join('');
    sel.value = 0;
    loadLibraryFields();
}

function renderList(type, items) {
    const container = document.getElementById(type === 'folder' ? 'folderList' : 'patternList');
    container.innerHTML = items.map((item, i) =>
        `<div class="list-item"><span>${escHtml(item)}</span><button class="remove-btn" onclick="removeListItem('${type}',${i})">&times;</button></div>`
    ).join('');
    container.dataset.items = JSON.stringify(items);
}

function addListItem(type) {
    const inputId = type === 'folder' ? 'newFolder' : 'newPattern';
    const input = document.getElementById(inputId);
    const val = input.value.trim();
    if (!val) return;
    const container = document.getElementById(type === 'folder' ? 'folderList' : 'patternList');
    const items = JSON.parse(container.dataset.items || '[]');
    items.push(val);
    renderList(type, items);
    input.value = '';
}

function removeListItem(type, idx) {
    const container = document.getElementById(type === 'folder' ? 'folderList' : 'patternList');
    const items = JSON.parse(container.dataset.items || '[]');
    items.splice(idx, 1);
    renderList(type, items);
}

async function saveSettings() {
    saveLibraryFields();
    const config = {
        libraries: settingsConfig.libraries,
        active_library: settingsConfig.active_library,
        ffprobe_path: document.getElementById('ffprobePath').value.trim(),
        workers: parseInt(document.getElementById('workerCount').value) || 8,
    };
    const resp = await fetch('/api/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(config),
    });
    const result = await resp.json();
    if (result.ok) {
        closeSettings();
        loadLibraries();
    } else {
        alert('Error saving: ' + (result.error || 'unknown'));
    }
}

// Library switcher
async function loadLibraries() {
    const resp = await fetch('/api/libraries');
    const data = await resp.json();
    const sel = document.getElementById('librarySwitcher');
    sel.innerHTML = data.libraries.map(name =>
        `<option value="${escHtml(name)}"${name === data.active ? ' selected' : ''}>${escHtml(name)}</option>`
    ).join('');
}

async function switchLibrary(name) {
    const resp = await fetch('/api/libraries/switch', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ name }),
    });
    const result = await resp.json();
    if (result.ok) {
        // Refresh everything
        pbpsTilesLoaded = false;
        distLoaded = false;
        qualityLoaded = false;
        upgradeLoaded = false;
        loadPBPSTiles(true);
        loadData();
    } else {
        alert(result.error || 'Switch failed');
    }
}

// Scan
let scanPollTimer = null;

async function startScan() {
    if (!confirm('Scanning may take a long time depending on the number of files and network speed.\n\nMake sure you have configured scan folders in Settings.\n\nContinue?')) return;
    const resp = await fetch('/api/scan/start', { method: 'POST' });
    const data = await resp.json();
    if (data.error) { alert(data.error); return; }
    document.getElementById('scanBtn').classList.add('scanning');
    showScanToast();
    scanPollTimer = setInterval(pollScanStatus, 2000);
}

function showScanToast() {
    document.getElementById('scanToast').classList.add('visible');
}

function dismissScanToast() {
    document.getElementById('scanToast').classList.remove('visible');
    if (scanPollTimer) { clearInterval(scanPollTimer); scanPollTimer = null; }
    document.getElementById('scanBtn').classList.remove('scanning');
}

async function pollScanStatus() {
    try {
        const resp = await fetch('/api/scan/status');
        const s = await resp.json();
        const phase = document.getElementById('scanPhase');
        const bar = document.getElementById('scanProgressBar');
        const detail = document.getElementById('scanDetail');

        const labels = { discovering: 'Discovering files...', checking: 'Checking timestamps...', probing: 'Probing files...',
                         swapping: 'Finalizing...', done: 'Complete', error: 'Error', starting: 'Starting...' };
        phase.textContent = labels[s.phase] || s.phase;

        if (s.total > 0) {
            const pct = Math.round((s.done / s.total) * 100);
            bar.style.width = pct + '%';
            detail.textContent = `${s.done.toLocaleString()} / ${s.total.toLocaleString()} files` + (s.errors ? ` (${s.errors} errors)` : '');
        } else {
            bar.style.width = '0%';
            detail.textContent = s.message || '';
        }

        if (s.phase === 'error') {
            bar.classList.add('error');
            detail.textContent = s.message;
        } else {
            bar.classList.remove('error');
        }

        if (!s.running) {
            clearInterval(scanPollTimer);
            scanPollTimer = null;
            document.getElementById('scanBtn').classList.remove('scanning');
            if (s.phase === 'done') {
                bar.style.width = '100%';
                loadData();  // refresh current view
                loadPBPSTiles(true);  // refresh tiles (force, cache invalidated server-side)
                distLoaded = false;  // invalidate client caches
                qualityLoaded = false;
                upgradeLoaded = false;
            }
        }
    } catch (e) {
        // network error, keep polling
    }
}

init();
</script>
</body>
</html>
"""


def main():
    parser = argparse.ArgumentParser(description="Interactive media stats web viewer.")
    parser.add_argument("--db", default="media.db", help="SQLite database path")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on")
    args = parser.parse_args()

    global DB_PATH, CONFIG_PATH, CONFIG_DIR
    CONFIG_DIR = os.path.dirname(os.path.abspath(args.db))
    CONFIG_PATH = os.path.join(CONFIG_DIR, "media_analyser_config.json")

    # Load config and set active library's DB (or fall back to CLI arg)
    config = load_config()
    lib = get_active_library(config)
    db = lib.get("db", args.db)
    DB_PATH = os.path.join(CONFIG_DIR, db) if not os.path.isabs(db) else db
    refresh_views()

    # Retrofit: capture initial snapshot if DB exists but has no snapshots
    if os.path.exists(DB_PATH) and not get_snapshot_history():
        print("Capturing initial snapshot...")
        capture_snapshot()

    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH} (library: {lib['name']})")
        print("Start a scan from the web UI to create it.")
        # Don't exit — allow the server to start so the user can configure and scan

    # Set console window title on Windows
    if os.name == "nt":
        os.system("title Media File Analyser")

    server = ThreadingHTTPServer(("0.0.0.0", args.port), APIHandler)
    print(f"Media File Analyser serving at http://localhost:{args.port}")
    print(f"Database: {DB_PATH}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()


if __name__ == "__main__":
    main()
