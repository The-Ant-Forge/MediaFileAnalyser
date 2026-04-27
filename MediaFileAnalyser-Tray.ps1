param(
    [int]$Port = 8081,
    [string]$DB = "media.db"
)

# ---------------------------------------------------------------------------
# MediaFileAnalyser System Tray
# Provides Start / Stop / Open browser controls via a Windows tray icon.
# Launch via MediaFileAnalyser-Tray.vbs for a hidden console window.
# ---------------------------------------------------------------------------

Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing

$script:ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$script:Port = $Port
$script:DB = $DB
$script:BaseUrl = "http://localhost:$Port"
$script:ServerProcess = $null

# --- Icon loading ---
function New-AppIcon {
    param([bool]$Running = $false)

    $icoPath = Join-Path $script:ScriptDir "MediaAnalyser.ico"
    if (Test-Path $icoPath) {
        $icon = New-Object System.Drawing.Icon($icoPath, 16, 16)
        if (-not $Running) {
            # Convert to grayscale for stopped state
            $bmp = $icon.ToBitmap()
            for ($x = 0; $x -lt $bmp.Width; $x++) {
                for ($y = 0; $y -lt $bmp.Height; $y++) {
                    $px = $bmp.GetPixel($x, $y)
                    $grey = [int](0.3 * $px.R + 0.59 * $px.G + 0.11 * $px.B)
                    $bmp.SetPixel($x, $y, [System.Drawing.Color]::FromArgb($px.A, $grey, $grey, $grey))
                }
            }
            $icon = [System.Drawing.Icon]::FromHandle($bmp.GetHicon())
        }
        return $icon
    }

    # Fallback: generate simple icon
    $bmp = New-Object System.Drawing.Bitmap(16, 16)
    $g = [System.Drawing.Graphics]::FromImage($bmp)
    $g.SmoothingMode = 'AntiAlias'
    $color = if ($Running) { [System.Drawing.Color]::FromArgb(78, 204, 163) } else { [System.Drawing.Color]::FromArgb(160, 160, 176) }
    $g.FillEllipse((New-Object System.Drawing.SolidBrush($color)), 1, 1, 14, 14)
    $font = New-Object System.Drawing.Font("Segoe UI", 8, [System.Drawing.FontStyle]::Bold)
    $sf = New-Object System.Drawing.StringFormat
    $sf.Alignment = 'Center'; $sf.LineAlignment = 'Center'
    $g.DrawString("M", $font, (New-Object System.Drawing.SolidBrush([System.Drawing.Color]::White)), (New-Object System.Drawing.RectangleF(0,0,16,16)), $sf)
    $g.Dispose()
    return [System.Drawing.Icon]::FromHandle($bmp.GetHicon())
}

# --- Server control ---
function Start-Server {
    if ($script:ServerProcess -and -not $script:ServerProcess.HasExited) {
        return
    }

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "python"
    $psi.Arguments = "media_analyser.py --db `"$script:DB`" --port $script:Port"
    $psi.WorkingDirectory = $script:ScriptDir
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true

    try {
        $script:ServerProcess = [System.Diagnostics.Process]::Start($psi)
        $script:TrayIcon.Icon = New-AppIcon -Running $true
        $script:TrayIcon.Text = "Media File Analyser - Running on port $script:Port"
        $script:StartItem.Enabled = $false
        $script:StopItem.Enabled = $true
        $script:TrayIcon.ShowBalloonTip(2000, "Media File Analyser", "Server started on port $script:Port", [System.Windows.Forms.ToolTipIcon]::Info)
    }
    catch {
        [System.Windows.Forms.MessageBox]::Show("Failed to start server: $_", "Error", 'OK', 'Error')
    }
}

function Stop-Server {
    if ($script:ServerProcess -and -not $script:ServerProcess.HasExited) {
        try {
            $script:ServerProcess.Kill()
            $script:ServerProcess.WaitForExit(5000)
        } catch {}
        $script:ServerProcess.Dispose()
        $script:ServerProcess = $null
    }

    # Also kill any orphaned python media_analyser processes on our port
    $procs = Get-WmiObject Win32_Process -Filter "Name='python.exe'" 2>$null |
        Where-Object { $_.CommandLine -like "*media_analyser*--port $script:Port*" }
    foreach ($p in $procs) {
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    }

    $script:TrayIcon.Icon = New-AppIcon -Running $false
    $script:TrayIcon.Text = "Media File Analyser - Stopped"
    $script:StartItem.Enabled = $true
    $script:StopItem.Enabled = $false
    $script:TrayIcon.ShowBalloonTip(2000, "Media File Analyser", "Server stopped", [System.Windows.Forms.ToolTipIcon]::Info)
}

function Open-Browser {
    Start-Process $script:BaseUrl
}

function Find-ExistingServer {
    # Look for any python.exe running media_analyser on our port
    $procs = Get-WmiObject Win32_Process -Filter "Name='python.exe'" 2>$null |
        Where-Object { $_.CommandLine -like "*media_analyser*--port $script:Port*" }
    if ($procs) {
        $first = $procs | Select-Object -First 1
        try {
            return [System.Diagnostics.Process]::GetProcessById($first.ProcessId)
        } catch {
            return $null
        }
    }
    return $null
}

function Sync-ServerState {
    # Reflect actual server state in the tray UI
    $existing = Find-ExistingServer
    if ($existing) {
        $script:ServerProcess = $existing
        $script:TrayIcon.Icon = New-AppIcon -Running $true
        $script:TrayIcon.Text = "Media File Analyser - Running on port $script:Port"
        $script:StartItem.Enabled = $false
        $script:StopItem.Enabled = $true
    } else {
        $script:ServerProcess = $null
        $script:TrayIcon.Icon = New-AppIcon -Running $false
        $script:TrayIcon.Text = "Media File Analyser - Stopped"
        $script:StartItem.Enabled = $true
        $script:StopItem.Enabled = $false
    }
}

# --- Build tray icon and context menu ---
$script:TrayIcon = New-Object System.Windows.Forms.NotifyIcon
$script:TrayIcon.Icon = New-AppIcon -Running $false
$script:TrayIcon.Text = "Media File Analyser - Stopped"
$script:TrayIcon.Visible = $true

$contextMenu = New-Object System.Windows.Forms.ContextMenuStrip

# Open browser
$openItem = New-Object System.Windows.Forms.ToolStripMenuItem("Open in Browser")
$openItem.Font = New-Object System.Drawing.Font($openItem.Font, [System.Drawing.FontStyle]::Bold)
$openItem.Add_Click({ Open-Browser })
$contextMenu.Items.Add($openItem) | Out-Null

$contextMenu.Items.Add((New-Object System.Windows.Forms.ToolStripSeparator)) | Out-Null

# Start
$script:StartItem = New-Object System.Windows.Forms.ToolStripMenuItem("Start Server")
$script:StartItem.Add_Click({ Start-Server })
$contextMenu.Items.Add($script:StartItem) | Out-Null

# Stop
$script:StopItem = New-Object System.Windows.Forms.ToolStripMenuItem("Stop Server")
$script:StopItem.Enabled = $false
$script:StopItem.Add_Click({ Stop-Server })
$contextMenu.Items.Add($script:StopItem) | Out-Null

$contextMenu.Items.Add((New-Object System.Windows.Forms.ToolStripSeparator)) | Out-Null

# Exit
$exitItem = New-Object System.Windows.Forms.ToolStripMenuItem("Exit")
$exitItem.Add_Click({
    Stop-Server
    $script:TrayIcon.Visible = $false
    $script:TrayIcon.Dispose()
    [System.Windows.Forms.Application]::Exit()
})
$contextMenu.Items.Add($exitItem) | Out-Null

# Also stop the server if PowerShell exits unexpectedly (e.g. user kills host)
Register-EngineEvent -SourceIdentifier ([System.Management.Automation.PsEngineEvent]::Exiting) -Action {
    $procs = Get-WmiObject Win32_Process -Filter "Name='python.exe'" 2>$null |
        Where-Object { $_.CommandLine -like "*media_analyser*--port $using:Port*" }
    foreach ($p in $procs) {
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    }
} | Out-Null

$script:TrayIcon.ContextMenuStrip = $contextMenu

# Double-click opens browser
$script:TrayIcon.Add_DoubleClick({ Open-Browser })

# Detect any pre-existing server and reflect it in the menu
Sync-ServerState

# --- Run message loop ---
[System.Windows.Forms.Application]::Run()
