@echo off
rem Launch the PowerShell status window (nicer UI than cmd).
start "" powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0proxy-status.ps1"
