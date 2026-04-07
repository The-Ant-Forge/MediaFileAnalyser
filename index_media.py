#!/usr/bin/env python3
"""
Index video media metadata from a NAS into a SQLite database.

Usage:
    python index_media.py [--db media.db] [--resume] <path> [<path> ...]

Examples:
    python index_media.py "/run/user/1000/gvfs/smb-share:server=storage4.local,share=video/Movies"
    python index_media.py --resume /mnt/nas/Movies /mnt/nas/TV
"""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm

VIDEO_EXTENSIONS = {
    ".mkv", ".mp4", ".avi", ".wmv", ".mov", ".flv", ".webm",
    ".m4v", ".mpg", ".mpeg", ".ts", ".vob", ".ogv", ".3gp",
}

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path TEXT UNIQUE NOT NULL,
    file_name TEXT NOT NULL,
    file_size_bytes INTEGER,
    format_name TEXT,
    format_long_name TEXT,
    duration_seconds REAL,
    bit_rate INTEGER,
    nb_streams INTEGER,
    probe_score INTEGER,
    format_tags_json TEXT,
    raw_probe_json TEXT,
    indexed_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS streams (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    stream_index INTEGER NOT NULL,
    codec_name TEXT,
    codec_long_name TEXT,
    codec_type TEXT,
    profile TEXT,
    codec_tag_string TEXT,
    width INTEGER,
    height INTEGER,
    coded_width INTEGER,
    coded_height INTEGER,
    display_aspect_ratio TEXT,
    pix_fmt TEXT,
    level INTEGER,
    color_range TEXT,
    color_space TEXT,
    color_transfer TEXT,
    color_primaries TEXT,
    field_order TEXT,
    r_frame_rate TEXT,
    avg_frame_rate TEXT,
    bit_rate INTEGER,
    max_bit_rate INTEGER,
    nb_frames INTEGER,
    duration_seconds REAL,
    sample_fmt TEXT,
    sample_rate INTEGER,
    channels INTEGER,
    channel_layout TEXT,
    bits_per_raw_sample INTEGER,
    has_b_frames INTEGER,
    language TEXT,
    title TEXT,
    tags_json TEXT,
    disposition_json TEXT,
    raw_stream_json TEXT,
    UNIQUE(file_id, stream_index)
);

CREATE INDEX IF NOT EXISTS idx_files_path ON files(file_path);
CREATE INDEX IF NOT EXISTS idx_streams_file ON streams(file_id);
CREATE INDEX IF NOT EXISTS idx_streams_codec ON streams(codec_name, codec_type);
CREATE INDEX IF NOT EXISTS idx_streams_language ON streams(language);
CREATE INDEX IF NOT EXISTS idx_streams_type ON streams(codec_type);

-- Convenience view: one row per video file with primary video stream info
CREATE VIEW IF NOT EXISTS v_video_summary AS
SELECT
    f.id AS file_id,
    f.file_path,
    f.file_name,
    f.file_size_bytes,
    f.duration_seconds,
    f.bit_rate AS file_bit_rate,
    s.codec_name AS video_codec,
    s.profile AS video_profile,
    s.width,
    s.height,
    s.pix_fmt,
    s.r_frame_rate,
    s.bit_rate AS video_bit_rate,
    s.color_range,
    s.color_space,
    CASE
        WHEN f.duration_seconds > 0
        THEN ROUND(f.file_size_bytes / 1048576.0 / (f.duration_seconds / 60.0), 3)
        ELSE NULL
    END AS mb_per_minute,
    CASE
        WHEN s.width >= 3840 OR s.height >= 2160 THEN '4K'
        WHEN s.width >= 1920 OR s.height >= 1080 THEN '1080p'
        WHEN s.width >= 1280 OR s.height >= 720 THEN '720p'
        WHEN s.width >= 720 OR s.height >= 480 THEN '480p'
        ELSE 'other'
    END AS resolution_class
FROM files f
JOIN streams s ON s.file_id = f.id
    AND s.codec_type = 'video'
    AND s.stream_index = (
        SELECT MIN(s2.stream_index) FROM streams s2
        WHERE s2.file_id = f.id AND s2.codec_type = 'video'
    );

-- Audio streams per file
CREATE VIEW IF NOT EXISTS v_audio_summary AS
SELECT
    f.id AS file_id,
    f.file_path,
    f.file_name,
    s.stream_index,
    s.codec_name AS audio_codec,
    s.profile AS audio_profile,
    s.channels,
    s.channel_layout,
    s.sample_rate,
    s.bit_rate AS audio_bit_rate,
    s.language,
    s.title AS stream_title,
    CASE
        WHEN s.bit_rate IS NOT NULL AND f.duration_seconds > 0
        THEN ROUND(s.bit_rate * f.duration_seconds / 8.0 / 1048576.0, 2)
        ELSE NULL
    END AS estimated_size_mb
FROM files f
JOIN streams s ON s.file_id = f.id AND s.codec_type = 'audio';

-- Subtitle streams per file
CREATE VIEW IF NOT EXISTS v_subtitle_summary AS
SELECT
    f.id AS file_id,
    f.file_path,
    f.file_name,
    s.stream_index,
    s.codec_name AS subtitle_codec,
    s.language,
    s.title AS stream_title,
    json_extract(s.disposition_json, '$.forced') AS is_forced,
    json_extract(s.disposition_json, '$.default') AS is_default,
    json_extract(s.disposition_json, '$.hearing_impaired') AS is_sdh
FROM files f
JOIN streams s ON s.file_id = f.id AND s.codec_type = 'subtitle';
"""


def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(DB_SCHEMA)
    conn.commit()
    return conn


def find_video_files(paths: list[str]) -> list[str]:
    """Recursively find video files under the given paths."""
    files = []
    for base in paths:
        if os.path.isfile(base):
            if Path(base).suffix.lower() in VIDEO_EXTENSIONS:
                files.append(base)
        elif os.path.isdir(base):
            dir_count = 0
            for root, _dirs, filenames in os.walk(base):
                dir_count += 1
                if dir_count % 10 == 0:
                    print(f"  Scanning dirs... ({dir_count} dirs, {len(files)} videos found so far)",
                          flush=True)
                for fname in sorted(filenames):
                    if Path(fname).suffix.lower() in VIDEO_EXTENSIONS:
                        files.append(os.path.join(root, fname))
            print(f"  Done scanning {base}: {dir_count} dirs, {len(files)} videos", flush=True)
    return files


def probe_file(filepath: str, timeout: int = 120) -> dict | None:
    """Run ffprobe on a file and return parsed JSON."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_format", "-show_streams",
                filepath,
            ],
            capture_output=True, text=True, timeout=timeout,
        )
        if result.returncode != 0:
            return None
        return json.loads(result.stdout)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError) as e:
        print(f"  ERROR probing: {e}", file=sys.stderr)
        return None


def safe_int(val) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def insert_file(conn: sqlite3.Connection, filepath: str, probe: dict) -> int | None:
    fmt = probe.get("format", {})
    tags = fmt.get("tags", {})

    try:
        cur = conn.execute(
            """INSERT INTO files
               (file_path, file_name, file_size_bytes, format_name, format_long_name,
                duration_seconds, bit_rate, nb_streams, probe_score,
                format_tags_json, raw_probe_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                filepath,
                os.path.basename(filepath),
                safe_int(fmt.get("size")),
                fmt.get("format_name"),
                fmt.get("format_long_name"),
                safe_float(fmt.get("duration")),
                safe_int(fmt.get("bit_rate")),
                safe_int(fmt.get("nb_streams")),
                safe_int(fmt.get("probe_score")),
                json.dumps(tags) if tags else None,
                json.dumps(probe),
            ),
        )
        return cur.lastrowid
    except sqlite3.IntegrityError:
        # Already exists
        return None


def insert_streams(conn: sqlite3.Connection, file_id: int, probe: dict):
    for stream in probe.get("streams", []):
        tags = stream.get("tags", {})
        disposition = stream.get("disposition", {})

        conn.execute(
            """INSERT OR IGNORE INTO streams
               (file_id, stream_index, codec_name, codec_long_name, codec_type,
                profile, codec_tag_string, width, height, coded_width, coded_height,
                display_aspect_ratio, pix_fmt, level, color_range, color_space,
                color_transfer, color_primaries, field_order,
                r_frame_rate, avg_frame_rate, bit_rate, max_bit_rate, nb_frames,
                duration_seconds, sample_fmt, sample_rate, channels, channel_layout,
                bits_per_raw_sample, has_b_frames, language, title,
                tags_json, disposition_json,
                raw_stream_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                file_id,
                safe_int(stream.get("index")),
                stream.get("codec_name"),
                stream.get("codec_long_name"),
                stream.get("codec_type"),
                stream.get("profile"),
                stream.get("codec_tag_string"),
                safe_int(stream.get("width")),
                safe_int(stream.get("height")),
                safe_int(stream.get("coded_width")),
                safe_int(stream.get("coded_height")),
                stream.get("display_aspect_ratio"),
                stream.get("pix_fmt"),
                safe_int(stream.get("level")),
                stream.get("color_range"),
                stream.get("color_space"),
                stream.get("color_transfer"),
                stream.get("color_primaries"),
                stream.get("field_order"),
                stream.get("r_frame_rate"),
                stream.get("avg_frame_rate"),
                safe_int(stream.get("bit_rate")),
                safe_int(stream.get("max_bit_rate")),
                safe_int(stream.get("nb_frames")),
                safe_float(stream.get("duration")),
                stream.get("sample_fmt"),
                safe_int(stream.get("sample_rate")),
                safe_int(stream.get("channels")),
                stream.get("channel_layout"),
                safe_int(stream.get("bits_per_raw_sample")),
                safe_int(stream.get("has_b_frames")),
                tags.get("language") or tags.get("LANGUAGE"),
                tags.get("title") or tags.get("TITLE"),
                json.dumps(tags) if tags else None,
                json.dumps(disposition) if disposition else None,
                json.dumps(stream),
            ),
        )


def get_indexed_paths(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT file_path FROM files").fetchall()
    return {r[0] for r in rows}


def index_files(conn: sqlite3.Connection, video_files: list[str],
                resume: bool = False, workers: int = 8):
    already_indexed = get_indexed_paths(conn) if resume else set()
    total = len(video_files)

    # Filter out already-indexed files up front so tqdm count is accurate
    to_probe = []
    skipped = 0
    for fp in video_files:
        if resume and fp in already_indexed:
            skipped += 1
        else:
            to_probe.append(fp)

    if skipped:
        print(f"Skipping {skipped} already-indexed files", flush=True)

    errors = 0
    indexed = 0
    batch_size = 100  # commit every N inserts

    pbar = tqdm(total=len(to_probe), desc="Indexing", unit="file",
                dynamic_ncols=True, miniters=1)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit all probe jobs
        future_to_path = {
            executor.submit(probe_file, fp): fp for fp in to_probe
        }

        for future in as_completed(future_to_path):
            filepath = future_to_path[future]
            fname = os.path.basename(filepath)
            probe = future.result()

            if probe is None:
                tqdm.write(f"  SKIP (probe failed): {fname}", file=sys.stderr)
                errors += 1
                pbar.update(1)
                pbar.set_postfix(idx=indexed, err=errors, refresh=False)
                continue

            file_id = insert_file(conn, filepath, probe)
            if file_id is None:
                skipped += 1
                pbar.update(1)
                pbar.set_postfix(idx=indexed, err=errors, refresh=False)
                continue

            insert_streams(conn, file_id, probe)
            indexed += 1

            if indexed % batch_size == 0:
                conn.commit()

            pbar.update(1)
            pbar.set_postfix(idx=indexed, err=errors, refresh=False)

    conn.commit()
    pbar.close()
    print(f"\nDone: {indexed} indexed, {skipped} skipped, {errors} errors (of {total} found)")


def main():
    parser = argparse.ArgumentParser(
        description="Index video media metadata into a SQLite database."
    )
    parser.add_argument("paths", nargs="+", help="Directories or files to scan")
    parser.add_argument("--db", default="media.db", help="SQLite database path (default: media.db)")
    parser.add_argument("--resume", action="store_true", help="Skip files already in the database")
    parser.add_argument("--workers", type=int, default=8,
                        help="Number of parallel ffprobe workers (default: 8)")
    args = parser.parse_args()

    # Validate paths
    for p in args.paths:
        if not os.path.exists(p):
            print(f"Error: path does not exist: {p}", file=sys.stderr)
            sys.exit(1)

    print(f"Database: {args.db}", flush=True)
    conn = init_db(args.db)

    print("Scanning for video files...", flush=True)
    video_files = find_video_files(args.paths)
    print(f"Found {len(video_files)} video files", flush=True)

    if not video_files:
        print("Nothing to index.")
        return

    start = time.time()
    index_files(conn, video_files, resume=args.resume, workers=args.workers)
    elapsed = time.time() - start
    print(f"Elapsed: {elapsed:.1f}s")

    conn.close()


if __name__ == "__main__":
    main()
