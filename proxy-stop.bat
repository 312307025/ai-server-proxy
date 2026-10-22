@echo off
rem ============================================================
rem  AI API Proxy Stopper
rem  Stops both the daemon loop and the node process.
rem ============================================================
chcp 65001 >nul
echo Stopping AI proxy ...

powershell -NoProfile -Command ^
  "Get-CimInstance Win32_Process | Where-Object { ($_.Name -eq 'node.exe' -and $_.CommandLine -like '*proxy-server.js*') -or ($_.Name -eq 'cmd.exe' -and $_.CommandLine -like '*proxy-daemon.bat*') } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"

echo Proxy stopped.
rem Use ping instead of timeout: timeout fails in non-interactive sessions
ping -n 3 127.0.0.1 >nul
