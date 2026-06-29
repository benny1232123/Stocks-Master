@echo off
chcp 65001 >nul 2>&1
title Stocks-Master Daemon

echo ========================================
echo   Stocks-Master 后台守护进程
echo ========================================
echo.
echo 功能：
echo   - 工作日 21:30 自动选股 + 推送
echo   - 盘中每 5 分钟刷新行情快照
echo   - 盘中每 10 分钟检查预警（止损/止盈/买点）
echo.
echo 日志：stock_data\auto_logs\daemon-YYYYMMDD.log
echo 退出：Ctrl+C 或关闭此窗口
echo.

cd /d "%~dp0"
E:\Anaconda\python.exe run_daemon.py

pause
