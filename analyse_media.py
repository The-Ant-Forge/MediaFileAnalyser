#!/usr/bin/env python3
"""
Analyse video metadata stored in the SQLite database.

Usage:
    python analyse_media.py [--db media.db] [command]

Commands:
    summary       Overall summary statistics
    codec-stats   MB/min stats (mean, std, median, count) by codec and resolution
    resolution    File counts and sizes by resolution class
    codecs        List all codecs and their usage counts
    largest       Top 20 largest files
    smallest      Top 20 smallest MB/min files
    sql           Run a custom SQL query interactively
"""

import argparse
import sqlite3
import sys
import math


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def print_table(headers: list[str], rows: list[list], alignments: list[str] | None = None):
    """Simple formatted table printer."""
    if not rows:
        print("  (no data)")
        return

    str_rows = [[str(v) if v is not None else "" for v in row] for row in rows]
    widths = [max(len(h), max((len(r[i]) for r in str_rows), default=0)) for i, h in enumerate(headers)]

    if alignments is None:
        alignments = ["<"] * len(headers)

    header_line = "  ".join(f"{h:{a}{w}}" for h, w, a in zip(headers, widths, alignments))
    print(header_line)
    print("  ".join("-" * w for w in widths))
    for row in str_rows:
        print("  ".join(f"{v:{a}{w}}" for v, w, a in zip(row, widths, alignments)))


def cmd_summary(conn: sqlite3.Connection):
    """Overall summary."""
    row = conn.execute("""
        SELECT
            COUNT(*) as total_files,
            ROUND(SUM(file_size_bytes) / 1073741824.0, 2) as total_gb,
            ROUND(SUM(duration_seconds) / 3600.0, 1) as total_hours,
            ROUND(AVG(file_size_bytes) / 1048576.0, 1) as avg_mb,
            ROUND(AVG(duration_seconds) / 60.0, 1) as avg_min
        FROM files
    """).fetchone()

    print("=== Overall Summary ===")
    print(f"  Total files:     {row['total_files']}")
    print(f"  Total size:      {row['total_gb']} GB")
    print(f"  Total duration:  {row['total_hours']} hours")
    print(f"  Avg file size:   {row['avg_mb']} MB")
    print(f"  Avg duration:    {row['avg_min']} min")


def cmd_codec_stats(conn: sqlite3.Connection):
    """MB per minute statistics by codec and resolution."""
    rows = conn.execute("""
        SELECT
            video_codec,
            resolution_class,
            COUNT(*) AS n,
            ROUND(AVG(mb_per_minute), 3) AS mean_mb_min,
            ROUND(SUM((mb_per_minute - sub.grp_avg) * (mb_per_minute - sub.grp_avg)) / COUNT(*), 6) AS variance,
            ROUND(MIN(mb_per_minute), 3) AS min_mb_min,
            ROUND(MAX(mb_per_minute), 3) AS max_mb_min
        FROM v_video_summary
        JOIN (
            SELECT video_codec AS gc, resolution_class AS rc, AVG(mb_per_minute) AS grp_avg
            FROM v_video_summary
            WHERE mb_per_minute IS NOT NULL
            GROUP BY video_codec, resolution_class
        ) sub ON sub.gc = video_codec AND sub.rc = resolution_class
        WHERE mb_per_minute IS NOT NULL
        GROUP BY video_codec, resolution_class
        ORDER BY video_codec, resolution_class
    """).fetchall()

    print("=== MB per Minute by Codec & Resolution ===")
    table_rows = []
    for r in rows:
        std = math.sqrt(r["variance"]) if r["variance"] else 0
        table_rows.append([
            r["video_codec"],
            r["resolution_class"],
            r["n"],
            f"{r['mean_mb_min']:.3f}",
            f"{std:.3f}",
            f"{r['min_mb_min']:.3f}",
            f"{r['max_mb_min']:.3f}",
        ])

    print_table(
        ["Codec", "Resolution", "Count", "Mean MB/min", "Std MB/min", "Min MB/min", "Max MB/min"],
        table_rows,
        ["<", "<", ">", ">", ">", ">", ">"],
    )


def cmd_resolution(conn: sqlite3.Connection):
    """File counts by resolution."""
    rows = conn.execute("""
        SELECT
            resolution_class,
            COUNT(*) AS n,
            ROUND(SUM(file_size_bytes) / 1073741824.0, 2) AS total_gb,
            ROUND(AVG(file_size_bytes) / 1048576.0, 1) AS avg_mb
        FROM v_video_summary
        GROUP BY resolution_class
        ORDER BY n DESC
    """).fetchall()

    print("=== Files by Resolution ===")
    print_table(
        ["Resolution", "Count", "Total GB", "Avg MB"],
        [[r["resolution_class"], r["n"], r["total_gb"], r["avg_mb"]] for r in rows],
        ["<", ">", ">", ">"],
    )


def cmd_codecs(conn: sqlite3.Connection):
    """Codec usage counts."""
    rows = conn.execute("""
        SELECT codec_name, codec_type, COUNT(*) AS n
        FROM streams
        GROUP BY codec_name, codec_type
        ORDER BY n DESC
    """).fetchall()

    print("=== Codec Usage ===")
    print_table(
        ["Codec", "Type", "Count"],
        [[r["codec_name"], r["codec_type"], r["n"]] for r in rows],
        ["<", "<", ">"],
    )


def cmd_largest(conn: sqlite3.Connection):
    """Top 20 largest files."""
    rows = conn.execute("""
        SELECT file_name, 
               ROUND(file_size_bytes / 1073741824.0, 2) AS size_gb,
               video_codec, resolution_class,
               ROUND(mb_per_minute, 2) AS mb_min
        FROM v_video_summary
        ORDER BY file_size_bytes DESC
        LIMIT 20
    """).fetchall()

    print("=== Top 20 Largest Files ===")
    print_table(
        ["File", "Size GB", "Codec", "Resolution", "MB/min"],
        [[r["file_name"], r["size_gb"], r["video_codec"], r["resolution_class"], r["mb_min"]] for r in rows],
        ["<", ">", "<", "<", ">"],
    )


def cmd_smallest_mbmin(conn: sqlite3.Connection):
    """Top 20 most efficient (smallest MB/min) files."""
    rows = conn.execute("""
        SELECT file_name,
               ROUND(file_size_bytes / 1048576.0, 1) AS size_mb,
               video_codec, resolution_class,
               ROUND(mb_per_minute, 3) AS mb_min
        FROM v_video_summary
        WHERE mb_per_minute IS NOT NULL
        ORDER BY mb_per_minute ASC
        LIMIT 20
    """).fetchall()

    print("=== Top 20 Most Efficient (Lowest MB/min) ===")
    print_table(
        ["File", "Size MB", "Codec", "Resolution", "MB/min"],
        [[r["file_name"], r["size_mb"], r["video_codec"], r["resolution_class"], r["mb_min"]] for r in rows],
        ["<", ">", "<", "<", ">"],
    )


def cmd_audio(conn: sqlite3.Connection):
    """Audio codec and language statistics."""
    print("=== Audio Codecs ===")
    rows = conn.execute("""
        SELECT audio_codec, channels, channel_layout,
               COUNT(*) AS n,
               ROUND(AVG(audio_bit_rate) / 1000.0, 1) AS avg_kbps,
               ROUND(AVG(estimated_size_mb), 1) AS avg_mb
        FROM v_audio_summary
        GROUP BY audio_codec, channels, channel_layout
        ORDER BY n DESC
    """).fetchall()
    print_table(
        ["Codec", "Channels", "Layout", "Count", "Avg kbps", "Avg MB"],
        [[r["audio_codec"], r["channels"], r["channel_layout"], r["n"],
          r["avg_kbps"], r["avg_mb"]] for r in rows],
        ["<", ">", "<", ">", ">", ">"],
    )

    print("\n=== Audio Languages ===")
    rows = conn.execute("""
        SELECT COALESCE(language, '(unknown)') AS lang, COUNT(*) AS n
        FROM v_audio_summary
        GROUP BY language
        ORDER BY n DESC
        LIMIT 30
    """).fetchall()
    print_table(
        ["Language", "Count"],
        [[r["lang"], r["n"]] for r in rows],
        ["<", ">"],
    )


def cmd_subtitles(conn: sqlite3.Connection):
    """Subtitle language and format statistics."""
    print("=== Subtitle Formats ===")
    rows = conn.execute("""
        SELECT subtitle_codec, COUNT(*) AS n
        FROM v_subtitle_summary
        GROUP BY subtitle_codec
        ORDER BY n DESC
    """).fetchall()
    print_table(
        ["Format", "Count"],
        [[r["subtitle_codec"], r["n"]] for r in rows],
        ["<", ">"],
    )

    print("\n=== Subtitle Languages ===")
    rows = conn.execute("""
        SELECT COALESCE(language, '(unknown)') AS lang,
               COUNT(*) AS n,
               SUM(is_forced) AS forced,
               SUM(is_sdh) AS sdh
        FROM v_subtitle_summary
        GROUP BY language
        ORDER BY n DESC
        LIMIT 30
    """).fetchall()
    print_table(
        ["Language", "Count", "Forced", "SDH"],
        [[r["lang"], r["n"], r["forced"], r["sdh"]] for r in rows],
        ["<", ">", ">", ">"],
    )


def cmd_sql(conn: sqlite3.Connection):
    """Interactive SQL."""
    print("Enter SQL queries (empty line or Ctrl-D to exit):")
    print("Available tables: files, streams")
    print("Available views:  v_video_summary, v_audio_summary, v_subtitle_summary")
    print()
    while True:
        try:
            query = input("sql> ").strip()
        except EOFError:
            break
        if not query:
            break
        try:
            cur = conn.execute(query)
            rows = cur.fetchall()
            if rows:
                headers = [d[0] for d in cur.description]
                print_table(headers, [list(r) for r in rows])
            else:
                print("  (no results)")
        except sqlite3.Error as e:
            print(f"  Error: {e}", file=sys.stderr)
        print()


COMMANDS = {
    "summary": cmd_summary,
    "codec-stats": cmd_codec_stats,
    "resolution": cmd_resolution,
    "codecs": cmd_codecs,
    "audio": cmd_audio,
    "subtitles": cmd_subtitles,
    "largest": cmd_largest,
    "smallest": cmd_smallest_mbmin,
    "sql": cmd_sql,
}


def main():
    parser = argparse.ArgumentParser(description="Analyse indexed video metadata.")
    parser.add_argument("command", nargs="?", default="summary",
                        choices=list(COMMANDS.keys()),
                        help="Analysis command (default: summary)")
    parser.add_argument("--db", default="media.db", help="SQLite database path")
    parser.add_argument("--all", action="store_true", help="Run all reports (except sql)")
    args = parser.parse_args()

    if not __import__("os").path.exists(args.db):
        print(f"Database not found: {args.db}", file=sys.stderr)
        print("Run index_media.py first.", file=sys.stderr)
        sys.exit(1)

    conn = connect(args.db)

    if args.all:
        for name, func in COMMANDS.items():
            if name == "sql":
                continue
            func(conn)
            print()
    else:
        COMMANDS[args.command](conn)

    conn.close()


if __name__ == "__main__":
    main()
