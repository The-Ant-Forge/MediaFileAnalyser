# MediaFileAnalyser

## Project Overview
A Python-based media file analyser that indexes video metadata from NAS/local storage into a SQLite database using ffprobe, then provides analysis and an interactive web viewer.

## Architecture
- **Single-file scripts** — no framework, no external web dependencies beyond Plotly CDN
- **`index_media.py`** — CLI tool that walks directories, runs `ffprobe` per file via `subprocess.run()`, stores results in SQLite. Uses `ThreadPoolExecutor` (8 workers default) with `tqdm` progress. Supports `--resume` to skip already-indexed files.
- **`analyse_media.py`** — CLI analysis tool with commands: summary, codec-stats, resolution, codecs, largest, smallest, sql, audio, subtitles
- **`media_analyser.py`** — Self-contained web app: Python `HTTPServer` backend + embedded HTML/CSS/JS frontend. Dark-themed tab UI (Data Browser, Statistics, Charts, Normalized PBPS, SQL Console). All API communication via `fetch()` GET requests to `/api/*` endpoints.

## Database
- **SQLite with WAL mode** for concurrent read access
- **Tables**: `files` (one row per video), `streams` (one row per stream per file)
- **Views**: `v_video_summary`, `v_audio_summary`, `v_subtitle_summary`
- DB can be ~1GB+ with tens of thousands of files indexed

## Tech Stack
- Python 3.10+ (stdlib only — no web framework)
- ffprobe (FFmpeg) for media probing
- SQLite3
- Plotly.js (CDN) for charts
- tqdm for CLI progress bars

## Key Patterns
- The web frontend is a single `INDEX_HTML` raw string embedded in `media_analyser.py`
- API endpoints are routed in `do_GET()` via path matching
- All SQL queries use parameterized values or view whitelisting to prevent injection
- `get_db()` creates a fresh connection per request (no connection pooling)
- Tab system: each tab is a `<div class="tab" data-section="...">` paired with a `<div class="section" id="...">`, toggled via `.active` CSS class

## Conventions
- Keep the single-file-per-concern approach (indexer, analyser, viewer)
- Use CSS variables for theming (defined in `:root`)
- Follow existing patterns for adding tabs/sections/API endpoints
- Generated output files (*.db, *.log, stats.txt) are gitignored

## Running
```bash
python index_media.py --db media.db --resume /path/to/videos
python analyse_media.py --db media.db --all
python media_analyser.py --db media.db --port 8081
```
