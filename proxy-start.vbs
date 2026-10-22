' ============================================================
'  AI API Proxy Launcher
'  Starts proxy-daemon.bat in a hidden window (no console popup),
'  so closing the editor / terminal will NOT kill the proxy.
'  Double-click to start. Use proxy-stop.bat to stop.
' ============================================================
Option Explicit

Dim shell, fso, baseDir, cmdLine
Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

baseDir = fso.GetParentFolderName(WScript.ScriptFullName)
shell.CurrentDirectory = baseDir

cmdLine = "cmd /c """ & baseDir & "\proxy-daemon.bat"""
shell.Run cmdLine, 0, False
