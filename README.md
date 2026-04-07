# Media Size Analyser

Index and analyse video metadata from a NAS into a SQLite database.

## Setup

Requires Python 3.10+ and `ffprobe` (from FFmpeg):

```bash
sudo apt install ffmpeg   # Ubuntu/Pop!_OS
```

## Usage

### 1. Index metadata

```bash
# Mount your NAS share first (GVFS, cifs, nfs, etc.)

# Index Movies and TV folders
python3 index_media.py --db media.db \
  "/run/user/1000/gvfs/smb-share:server=storage4.local,share=video/Movies" \
  "/run/user/1000/gvfs/smb-share:server=storage4.local,share=video/TV"

# Resume after interruption (skips already-indexed files)
python3 index_media.py --resume --db media.db <paths...>
```

### 2. Analyse

```bash
# Overall summary
python3 analyse_media.py --db media.db summary

# MB per minute by codec & resolution (mean, std, min, max)
python3 analyse_media.py --db media.db codec-stats

# All reports
python3 analyse_media.py --db media.db --all

# Interactive SQL
python3 analyse_media.py --db media.db sql
```

Available commands: `summary`, `codec-stats`, `resolution`, `codecs`, `largest`, `smallest`, `sql`

## Database Schema

- **files** — One row per video file (path, size, duration, format, raw probe JSON)
- **streams** — One row per stream (video/audio/subtitle codec details)
- **v_video_summary** — Convenience view joining files with their primary video stream,
  including computed `mb_per_minute` and `resolution_class` columns

### Custom Queries

```sql
-- Files encoded in h264 larger than 5GB
SELECT file_name, file_size_bytes / 1e9 AS gb
FROM v_video_summary
WHERE video_codec = 'h264' AND file_size_bytes > 5e9
ORDER BY file_size_bytes DESC;

-- Average bitrate by codec
SELECT video_codec, ROUND(AVG(file_bit_rate) / 1e6, 2) AS avg_mbps
FROM v_video_summary
GROUP BY video_codec;
```


#### Choice Query

Take the avg bytes per pixel per second of a resolutions class and compares that to the avg bpps of the resolution class.

```sql

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
WHERE dur_min > 10
ORDER BY ratio_vs_avg DESC

```