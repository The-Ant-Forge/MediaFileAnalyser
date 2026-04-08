#!/usr/bin/env python3
"""
Interactive web-based media stats viewer.

Usage:
    python web_viewer.py [--db media.db] [--port 8080]
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

DB_PATH = "media.db"
CONFIG_PATH = "media_analyser_config.json"

DEFAULT_CONFIG = {
    "scan_folders": [],
    "ignore_patterns": [],
    "ffprobe_path": "",
    "workers": 8,
}


def load_config():
    """Load config from JSON file, merging with defaults."""
    config = dict(DEFAULT_CONFIG)
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r") as f:
                saved = json.load(f)
            config.update(saved)
        except (json.JSONDecodeError, OSError):
            pass
    return config


def save_config(config):
    """Save config to JSON file."""
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)


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
    """Load file_path → (file_mtime, file_size_bytes) from the live DB."""
    index = {}
    if not os.path.exists(DB_PATH):
        return index
    try:
        conn = get_db()
        for row in conn.execute("SELECT file_path, file_mtime, file_size_bytes FROM files"):
            index[row["file_path"]] = (row["file_mtime"], row["file_size_bytes"])
        conn.close()
    except Exception:
        pass
    return index


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
        folders = config.get("scan_folders", [])
        if not folders:
            with scan_lock:
                scan_state.update(running=False, phase="error",
                                  message="No scan folders configured")
            return

        ignore = config.get("ignore_patterns", [])
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
        with scan_lock:
            scan_state["phase"] = "checking"
            scan_state["message"] = "Checking file timestamps..."

        existing_index = _load_existing_index()
        to_probe = []
        to_copy = []
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

        with scan_lock:
            scan_state["total"] = len(video_files)
            scan_state["phase"] = "probing"
            scan_state["message"] = (f"{len(to_copy)} unchanged, {len(to_probe)} to probe...")

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

        invalidate_pbps_tiles_cache()
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
    SUM(CASE WHEN ratio_vs_avg > 2 THEN 1 ELSE 0 END) AS rva_up_count,
    SUM(CASE WHEN ratio_vs_avg < 0.5 THEN 1 ELSE 0 END) AS rva_down_count,
    ROUND(AVG(bpp_sec), 6) AS mean_bpp_sec,
    SUM(CASE WHEN video_codec = 'h264' THEN 1 ELSE 0 END) AS h264_count,
    SUM(CASE WHEN resolution_class IN ('720p', '480p', 'other') THEN 1 ELSE 0 END) AS low_res_count,
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


def compute_pbps_tiles():
    conn = get_db()
    try:
        row = dict(conn.execute(PBPS_TILES_SQL).fetchone())
        median_row = conn.execute(PBPS_MEDIAN_SQL).fetchone()
        row["median_bpp_sec"] = median_row["median_bpp_sec"] if median_row else None
        return row
    finally:
        conn.close()


def get_pbps_tiles():
    if _pbps_tiles_cache["data"] is None:
        _pbps_tiles_cache["data"] = compute_pbps_tiles()
    return _pbps_tiles_cache["data"]


def invalidate_pbps_tiles_cache():
    _pbps_tiles_cache["data"] = None


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
        elif path == "/api/scan/status":
            self.handle_scan_status()
        elif path == "/api/pbps/tiles":
            self.handle_pbps_tiles()
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/config":
            self.handle_post_config()
        elif path == "/api/scan/start":
            self.handle_scan_start()
        else:
            self.send_response(404)
            self.end_headers()

    def handle_get_config(self):
        self.send_json(load_config())

    def handle_post_config(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, ValueError):
            self.send_json({"error": "Invalid JSON"}, 400)
            return
        config = load_config()
        if "scan_folders" in body and isinstance(body["scan_folders"], list):
            config["scan_folders"] = [str(p) for p in body["scan_folders"]]
        if "ignore_patterns" in body and isinstance(body["ignore_patterns"], list):
            config["ignore_patterns"] = [str(p) for p in body["ignore_patterns"]]
        if "ffprobe_path" in body:
            config["ffprobe_path"] = str(body["ffprobe_path"])
        if "workers" in body:
            config["workers"] = max(1, min(32, int(body["workers"])))
        save_config(config)
        self.send_json({"ok": True})

    def handle_scan_status(self):
        with scan_lock:
            self.send_json(dict(scan_state))

    def handle_pbps_tiles(self):
        try:
            self.send_json(get_pbps_tiles())
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
tr:hover td { background: rgba(233,69,96,0.08); }

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
              padding: 12px 18px; min-width: 130px; flex: 1; text-align: center; }
.score-tile .tile-value { font-size: 1.6em; font-weight: 700; color: var(--green); line-height: 1.2; }
.score-tile .tile-label { font-size: 0.75em; color: var(--text2); margin-top: 2px; }
.score-tile.warn .tile-value { color: var(--accent); }
.score-tile.neutral .tile-value { color: var(--text); }
.tile-pct { font-size: 0.5em; color: var(--text2); font-weight: 400; }

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
        <button class="icon-btn" id="scanBtn" onclick="startScan()" title="Start scan">&#9654;</button>
        <button class="icon-btn" id="settingsBtn" onclick="openSettings()" title="Settings">&#9881;</button>
    </div>
</div>

<div class="tabs" id="mainTabs">
    <div class="tab active" data-section="data-section">Data Browser</div>
    <div class="tab" data-section="stats-section">Statistics</div>
    <div class="tab" data-section="chart-section">Charts</div>
    <div class="tab" data-section="pbps-section">Normalized PBPS</div>
    <div class="tab" data-section="sql-section">SQL Console</div>
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
<div class="score-tiles" id="pbpsTiles">
    <div class="score-tile warn" id="tileRVAUp"><div class="tile-value">--</div><div class="tile-label">R&#9650;A (&gt;2x avg)</div></div>
    <div class="score-tile warn" id="tileRVADown"><div class="tile-value">--</div><div class="tile-label">R&#9660;A (&lt;0.5x avg)</div></div>
    <div class="score-tile neutral" id="tileMedianBPP"><div class="tile-value">--</div><div class="tile-label">Median BPP/sec</div></div>
    <div class="score-tile neutral" id="tileMeanBPP"><div class="tile-value">--</div><div class="tile-label">Mean BPP/sec</div></div>
    <div class="score-tile" id="tileH264"><div class="tile-value">--</div><div class="tile-label">h264 Files</div></div>
    <div class="score-tile" id="tile720p"><div class="tile-value">--</div><div class="tile-label">&le;720p Files</div></div>
</div>
<div class="panel">
    <p class="info">Per-pixel Bytes Per Second normalized against resolution class average. Shows files with duration &gt; 10 minutes, ordered by ratio vs average (highest first).</p>
    <button onclick="loadPBPS()">Run Analysis</button>
    <span id="pbpsInfo" style="color:var(--text2);font-size:0.85em;margin-left:8px"></span>
    <div class="table-wrap" id="pbpsTableWrap" style="margin-top:12px"></div>
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
<div class="settings-panel">
    <h2 style="color:var(--green);margin-top:0">Settings</h2>

    <h3>Target Folders</h3>
    <div class="list-items" id="folderList"></div>
    <div class="add-row">
        <input type="text" id="newFolder" placeholder="/path/to/media/folder">
        <button onclick="addListItem('folder')">Add</button>
    </div>

    <h3>Ignore Patterns</h3>
    <p style="color:var(--text2);font-size:0.78em;margin-bottom:4px">Directory names containing these strings will be skipped during scanning</p>
    <div class="list-items" id="patternList"></div>
    <div class="add-row">
        <input type="text" id="newPattern" placeholder="e.g. sample, mature, .recycle">
        <button onclick="addListItem('pattern')">Add</button>
    </div>

    <h3>ffprobe Path</h3>
    <input type="text" id="ffprobePath" placeholder="(uses system PATH)" style="width:100%">

    <h3>Parallel Workers</h3>
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
        if (tab.dataset.section === 'pbps-section') loadPBPSTiles();
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
    html += sorted.map(row => '<tr>' + s.columns.map(c => `<td title="${escHtml(String(row[c] ?? ''))}">${escHtml(fmt(row[c]))}</td>`).join('') + '</tr>').join('');
    html += '</tbody></table>';
    document.getElementById(wrapId).innerHTML = html;
}

function sortTableCol(wrapId, col) {
    const s = renderSortableTable._state[wrapId];
    if (s.col === col) s.dir = s.dir === 'asc' ? 'desc' : 'asc';
    else { s.col = col; s.dir = 'asc'; }
    renderSortableTable(wrapId);
}

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
        document.querySelector('#tileRVAUp .tile-value').textContent = (t.rva_up_count ?? 0).toLocaleString();
        document.querySelector('#tileRVADown .tile-value').textContent = (t.rva_down_count ?? 0).toLocaleString();
        document.querySelector('#tileMedianBPP .tile-value').textContent = t.median_bpp_sec != null ? t.median_bpp_sec : '--';
        document.querySelector('#tileMeanBPP .tile-value').textContent = t.mean_bpp_sec != null ? t.mean_bpp_sec : '--';
        const total = t.total_files || 1;
        const h264 = t.h264_count ?? 0;
        const lowRes = t.low_res_count ?? 0;
        document.querySelector('#tileH264 .tile-value').innerHTML = `${h264.toLocaleString()} <span class="tile-pct">(${Math.round(h264/total*100)}%)</span>`;
        document.querySelector('#tile720p .tile-value').innerHTML = `${lowRes.toLocaleString()} <span class="tile-pct">(${Math.round(lowRes/total*100)}%)</span>`;
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

// Settings panel
async function openSettings() {
    await loadSettings();
    document.getElementById('settingsOverlay').classList.add('open');
}

function closeSettings() {
    document.getElementById('settingsOverlay').classList.remove('open');
}

async function loadSettings() {
    const resp = await fetch('/api/config');
    const cfg = await resp.json();
    renderList('folder', cfg.scan_folders || []);
    renderList('pattern', cfg.ignore_patterns || []);
    document.getElementById('ffprobePath').value = cfg.ffprobe_path || '';
    document.getElementById('workerCount').value = cfg.workers || 8;
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
    const folders = JSON.parse(document.getElementById('folderList').dataset.items || '[]');
    const patterns = JSON.parse(document.getElementById('patternList').dataset.items || '[]');
    const config = {
        scan_folders: folders,
        ignore_patterns: patterns,
        ffprobe_path: document.getElementById('ffprobePath').value.trim(),
        workers: parseInt(document.getElementById('workerCount').value) || 8,
    };
    const resp = await fetch('/api/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(config),
    });
    const result = await resp.json();
    if (result.ok) closeSettings();
    else alert('Error saving: ' + (result.error || 'unknown'));
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

    global DB_PATH, CONFIG_PATH
    DB_PATH = args.db
    CONFIG_PATH = os.path.join(
        os.path.dirname(os.path.abspath(DB_PATH)),
        "media_analyser_config.json",
    )

    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        return

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
