# ============================================================
#  AI API 代理服务状态查看（PowerShell 版）
#  由 proxy-status.bat 启动，也可直接运行：
#    powershell -NoProfile -ExecutionPolicy Bypass -File proxy-status.ps1
# ============================================================
$ErrorActionPreference = 'SilentlyContinue'

# 统一 UTF-8 输出，保证中文正常显示
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$Host.UI.RawUI.WindowTitle = 'AI API 代理服务状态'

Write-Host ''
Write-Host '============================================================' -ForegroundColor DarkCyan
Write-Host '  AI API 代理服务状态' -ForegroundColor Cyan
Write-Host '============================================================' -ForegroundColor DarkCyan
Write-Host ''

# 通过端口监听状态判断服务是否在运行
$conn = Get-NetTCPConnection -LocalPort 13000 -State Listen -ErrorAction SilentlyContinue

if ($conn) {
    Write-Host '  状态    ' -NoNewline
    Write-Host '● 运行中' -ForegroundColor Green
    Write-Host '  端口    13000'
    Write-Host ('  PID     ' + ($conn.OwningProcess -join ', '))
} else {
    Write-Host '  状态    ' -NoNewline
    Write-Host '● 未启动' -ForegroundColor Red
    Write-Host '  端口    13000（无监听）'
}

# 读取最新一份日志并展示末尾内容（日志统一存放在 logs 子目录）
$logDir = Join-Path $PSScriptRoot 'logs'
$log = Get-ChildItem (Join-Path $logDir 'proxy-server-*.log') -ErrorAction SilentlyContinue |
       Sort-Object LastWriteTime -Descending | Select-Object -First 1

if ($log) {
    Write-Host ('  日志    ' + $log.Name)
    Write-Host ''
    Write-Host ('-' * 60) -ForegroundColor DarkGray
    Write-Host '  最近 10 行日志（错误红色 / 警告黄色）' -ForegroundColor DarkYellow
    Write-Host ('-' * 60) -ForegroundColor DarkGray

    Get-Content $log.FullName -Tail 10 -Encoding UTF8 | ForEach-Object {
        if ($_ -match '\[ERROR\]|\[FATAL\]') {
            Write-Host $_ -ForegroundColor Red
        } elseif ($_ -match '\[WARN\]') {
            Write-Host $_ -ForegroundColor Yellow
        } else {
            Write-Host $_
        }
    }
} else {
    Write-Host '  日志    暂无日志文件'
}

Write-Host ''
Read-Host '按回车键关闭窗口'
