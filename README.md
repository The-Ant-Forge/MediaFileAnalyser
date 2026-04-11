# Media File Analyser

Index and analyse video metadata from a NAS or local storage into a SQLite database, then explore it through an interactive web dashboard.

## Features

- **Media indexing** with ffprobe — walks directories, extracts codec/resolution/bitrate metadata
- **Smart scanning** — skips unchanged files using mtime+size checks, copies cached data for fast re-scans
- **Web dashboard** with dark theme:
  - **Scoreboard tiles** — RVA ratios, BPP/sec, codec/resolution histograms, Legacy/Modern counts
  - **Data Browser** — sortable, filterable, paginated views of all indexed media
  - **Statistics** — mean/std/min/max for numeric columns with optional grouping
  - **Charts** — bar, scatter, box, histogram, pie charts via Plotly.js
  - **Normalized PBPS** — per-pixel bytes-per-second analysis with Geekseek integration
  - **Distributions** — stacked bar charts of ratio distribution by codec and resolution
  - **SQL Console** — run custom SELECT queries against the database
- **Settings panel** — configure scan folders, ignore patterns, ffprobe path, worker count
- **Web-triggered scanning** — start scans from the browser with live progress updates
- **System tray app** — start/stop server and open browser from the Windows tray

## Setup

Requires Python 3.10+ and `ffprobe` (from FFmpeg):

```bash
# Linux
sudo apt install ffmpeg

# Windows — install FFmpeg and ensure ffprobe is on PATH
```

## Usage

### CLI Indexing

```bash
python index_media.py --db media.db --resume /path/to/videos
python analyse_media.py --db media.db --all
```

### Web Dashboard

```bash
python media_analyser.py --db media.db --port 8081
```

Or use the system tray launcher (Windows):

```
Double-click MediaFileAnalyser-Tray.vbs
```

Right-click the tray icon to Start/Stop the server or open it in the browser.

### Scanning from the Web UI

1. Click the **gear icon** to configure scan folders and ignore patterns
2. Click the **play button** to start a scan — progress shows in a toast bar
3. The scan writes to a temporary database and swaps on completion, so browsing is uninterrupted

## Database Schema

- **files** — one row per video file (path, size, duration, format, mtime, raw probe JSON)
- **streams** — one row per stream (video/audio/subtitle codec details)
- **v_video_summary** — convenience view with computed `mb_per_minute` and `resolution_class`
- **v_audio_summary** — audio streams with estimated sizes
- **v_subtitle_summary** — subtitle streams with disposition flags

## Tech Stack

- Python 3.10+ (stdlib only — no web framework)
- ffprobe (FFmpeg) for media probing
- SQLite3 with WAL mode
- Plotly.js (CDN) for charts
- PowerShell + VBS for Windows tray integration
