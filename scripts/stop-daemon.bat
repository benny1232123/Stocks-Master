@echo off
chcp 65001 >nul 2>&1
echo 正在停止 Stocks-Master Daemon...

REM 查找并终止 run_daemon.py 进程
for /f "tokens=2" %%i in ('tasklist /fi "imagename eq python.exe" /fo list 2^>nul ^| findstr "PID"') do (
    for /f "tokens=1" %%w in ('wmic process where processid=%%i get commandline 2^>nul ^| findstr "run_daemon"') do (
        taskkill /pid %%i /f 2>nul
        echo 已终止进程 %%i
    )
)

REM 也检查 pythonw.exe
for /f "tokens=2" %%i in ('tasklist /fi "imagename eq pythonw.exe" /fo list 2^>nul ^| findstr "PID"') do (
    for /f "tokens=1" %%w in ('wmic process where processid=%%i get commandline 2^>nul ^| findstr "run_daemon"') do (
        taskkill /pid %%i /f 2>nul
        echo 已终止进程 %%i
    )
)

echo 完成。
timeout /t 2 >nul
