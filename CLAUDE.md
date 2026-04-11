# MediaFileAnalyser

## Project Overview
A Python-based media file analyser that indexes video metadata from NAS/local storage into a SQLite database using ffprobe, then provides analysis and an interactive web dashboard with gamified quality metrics.

## Architecture
- **Single-file scripts** — no framework, no external web dependencies beyond Plotly CDN
- **`index_media.py`** — CLI tool that walks directories, runs `ffprobe` per file via `subprocess.run()`, stores results in SQLite. Uses `ThreadPoolExecutor` (8 workers default) with `tqdm` progress. Supports `--resume` to skip already-indexed files.
- **`analyse_media.py`** — CLI analysis tool with commands: summary, codec-stats, resolution, codecs, largest, smallest, sql, audio, subtitles
- **`media_analyser.py`** — Self-contained web app: Python `ThreadingHTTPServer` backend + embedded HTML/CSS/JS frontend. Dark-themed tab UI with scoreboard tiles, multiple analysis pages, and settings panel. All API communication via `fetch()` to `/api/*` endpoints.
- **`MediaFileAnalyser-Tray.ps1` / `.vbs`** — Windows system tray launcher for start/stop/open browser.

## Multi-Library Support
- Config stores an array of `libraries`, each with name, db filename, scan_folders, ignore_patterns
- `active_library` tracks which is currently loaded
- Switching libraries changes `DB_PATH` and invalidates all caches
- Old flat configs (pre-library) auto-migrate to a single "Default" library
- Library DB files are relative to CONFIG_DIR (same directory as config JSON)

## Database
- **SQLite with WAL mode** for concurrent read access
- **Tables**: `files` (one row per video, includes `file_mtime`), `streams` (one row per stream per file)
- **Views**: `v_video_summary`, `v_audio_summary`, `v_subtitle_summary`
- DB can be ~1GB+ with tens of thousands of files indexed

## Caching Strategy
- **Server-side caches** for tiles, distributions, quality heatmap, upgrades, violin data — computed once, invalidated on scan completion or library switch
- **Client-side flags** (`pbpsTilesLoaded`, `distLoaded`, `qualityLoaded`, `upgradeLoaded`) prevent re-fetching on tab switches — reset after scan or library switch
- **Smart scan caching** — file mtime+size stored in DB, compared on re-scan to skip unchanged files. Skips stat-check phase entirely when DB has no timestamps.

## Tech Stack
- Python 3.10+ (stdlib only — no web framework)
- ffprobe (FFmpeg) for media probing — `encoding="utf-8", errors="replace"` for Windows compat
- SQLite3
- Plotly.js (CDN) for charts (stacked bars, heatmaps, Sankey, violin plots)
- tqdm for CLI progress bars
- PowerShell + VBS for Windows tray integration

## Key Patterns
- The web frontend is a single `INDEX_HTML` raw string embedded in `media_analyser.py`
- API endpoints are routed in `do_GET()` / `do_POST()` via path matching
- All SQL queries use parameterized values or view whitelisting to prevent injection
- `get_db()` creates a fresh connection per request (enables seamless DB switching)
- Tab system: `<div class="tab" data-section="...">` paired with `<div class="section" id="...">`
- Sortable tables use `renderSortableTable()` with client-side sorting and `data-col` attributes
- Clickable `file_name` cells open Geekseek search (Movie vs TV auto-detected from path)
- Clickable `file_path` cells copy to clipboard with flash animation

## Conventions
- Keep the single-file-per-concern approach (indexer, analyser, web app)
- Use CSS variables for theming (defined in `:root`)
- Follow existing patterns for adding tabs/sections/API endpoints
- Add new caches alongside existing ones, invalidate in `invalidate_all_caches()`
- Generated output files (*.db, *.log, stats.txt, config JSON) are gitignored

## Running
```bash
python index_media.py --db media.db --resume /path/to/videos
python analyse_media.py --db media.db --all
python media_analyser.py --db media.db --port 8081
```
