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
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

DB_PATH = "media.db"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def dict_rows(cursor):
    return [dict(row) for row in cursor.fetchall()]


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
        else:
            self.send_response(404)
            self.end_headers()

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
</style>
</head>
<body>
<div class="container">
<h1>Media Stats Viewer</h1>

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
    let html = '<table><thead><tr>' + data.columns.map(c => `<th>${escHtml(c)}</th>`).join('') + '</tr></thead><tbody>';
    html += data.rows.map(row => '<tr>' + data.columns.map(c => `<td>${escHtml(fmt(row[c]))}</td>`).join('') + '</tr>').join('');
    html += '</tbody></table>';
    document.getElementById('sqlTableWrap').innerHTML = html;
}

document.getElementById('sqlInput').addEventListener('keydown', e => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') runSQL();
});

// Normalized PBPS
async function loadPBPS() {
    const sql = `WITH bpp AS (
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
ORDER BY ratio_vs_avg DESC`;
    const resp = await fetch(`/api/query?sql=${encodeURIComponent(sql)}`);
    const data = await resp.json();
    if (data.error) {
        document.getElementById('pbpsInfo').textContent = 'Error: ' + data.error;
        document.getElementById('pbpsTableWrap').innerHTML = '';
        return;
    }
    document.getElementById('pbpsInfo').textContent = `${data.total} rows returned`;
    let html = '<table><thead><tr>' + data.columns.map(c => `<th>${escHtml(c)}</th>`).join('') + '</tr></thead><tbody>';
    html += data.rows.map(row => '<tr>' + data.columns.map(c => `<td>${escHtml(fmt(row[c]))}</td>`).join('') + '</tr>').join('');
    html += '</tbody></table>';
    document.getElementById('pbpsTableWrap').innerHTML = html;
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

    global DB_PATH
    DB_PATH = args.db

    if not os.path.exists(DB_PATH):
        print(f"Database not found: {DB_PATH}")
        return

    server = HTTPServer(("0.0.0.0", args.port), APIHandler)
    print(f"Serving at http://localhost:{args.port}")
    print(f"Database: {DB_PATH}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()


if __name__ == "__main__":
    main()
