Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "powershell -ExecutionPolicy Bypass -WindowStyle Hidden -File """ & Replace(WScript.ScriptFullName, "MediaFileAnalyser-Tray.vbs", "MediaFileAnalyser-Tray.ps1") & """", 0, False
