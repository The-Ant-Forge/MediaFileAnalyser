# Media File Analyser

Index and analyse video metadata from a NAS or local storage into a SQLite database, then explore it through an interactive web dashboard.

## Features

- **Media indexing** with ffprobe — walks directories, extracts codec/resolution/bitrate metadata
- **Smart scanning** — skips unchanged files using mtime+size checks, copies cached data for fast re-scans. Detects when no timestamps exist and skips unnecessary stat checks.
- **Multiple libraries** — manage separate databases for different media collections (e.g. Movies, TV, Music Videos), each with their own scan folders and ignore patterns. Switch between libraries from the header dropdown.
- **Web dashboard** with dark theme:
  - **Scoreboard tiles** — RVA ratios, BPP/sec, codec/resolution histograms, ratio distribution, Legacy/Modern counts (visible on all tabs)
  - **Data Browser** — sortable, filterable, paginated views of all indexed media
  - **Statistics** — mean/std/min/max for numeric columns with optional grouping
  - **Charts** — bar, scatter, box, histogram, pie charts via Plotly.js
  - **Normalized PBPS** — per-pixel bytes-per-second analysis with Geekseek integration
  - **Distributions** — violin plots, stacked bar charts by codec and resolution
  - **Quality Map** — heatmap (codec x resolution) and Sankey flow diagram
  - **Upgrades** — scored priority list of files needing quality replacement
  - **SQL Console** — run custom SELECT queries against the database
- **Settings panel** — per-library scan folders and ignore patterns, global ffprobe path and worker count
- **Web-triggered scanning** — start scans from the browser with live progress updates
- **Geekseek integration** — click file names in PBPS/Upgrades to search nzbgeek (auto-detects Movie vs TV)
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

### Managing Libraries

1. Click the **gear icon** to open Settings
2. Use **+ New** to create a library (e.g. "Movies", "TV Shows")
3. Set a database filename, scan folders, and ignore patterns for each
4. Switch between libraries using the **dropdown in the header**
5. Each library maintains its own database and scan configuration

### Scanning from the Web UI

1. Configure scan folders for the active library in Settings
2. Click the **play button** to start a scan — progress shows in a toast bar
3. The scan writes to a temporary database and swaps on completion, so browsing is uninterrupted
4. Subsequent scans use mtime+size caching to skip unchanged files

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
