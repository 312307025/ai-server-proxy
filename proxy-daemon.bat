@echo off
rem ============================================================
rem  AI API Proxy Daemon
rem  Keeps proxy-server.js running: auto-restart 5s after exit.
rem  Started by proxy-start.vbs (hidden window). Do not run directly.
rem ============================================================
chcp 65001 >nul
cd /d "%~dp0"

rem Prefer the absolute node path (stable symlink managed by nvm4w),
rem fall back to PATH lookup if it does not exist.
set "NODE_EXE=C:\nvm4w\nodejs\node.exe"
if not exist "%NODE_EXE%" set "NODE_EXE=node"

rem --- single instance guard: exit if port 13000 is already listening ---
netstat -ano | findstr /R /C:":13000 .*LISTENING" >nul
if not errorlevel 1 (
    echo Port 13000 is already in use, another proxy instance is running. Exit.
    echo Run proxy-stop.bat first if you want a clean restart.
    ping -n 4 127.0.0.1 >nul
    exit /b 0
)

:loop
echo [%date% %time%] starting proxy-server.js ...
"%NODE_EXE%" proxy-server.js
echo [%date% %time%] proxy exited with code %errorlevel%, restart in 5s ...
rem Use ping instead of timeout: timeout fails in non-interactive/hidden sessions
ping -n 6 127.0.0.1 >nul
goto loop
