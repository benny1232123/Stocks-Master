@echo off
setlocal

set "TASK_NAME_EVENING=StocksMaster-Boll-Daily"
set "TASK_NAME_NOON=StocksMaster-Boll-Noon"

echo [Stocks-Master] Start task now: %TASK_NAME_EVENING%
schtasks /Run /TN "%TASK_NAME_EVENING%"

if errorlevel 1 (
    echo [Stocks-Master] Failed to start evening task. Ensure task exists first.
    pause
    exit /b 1
)

echo [Stocks-Master] Start task now: %TASK_NAME_NOON%
schtasks /Run /TN "%TASK_NAME_NOON%"

if errorlevel 1 (
    echo [Stocks-Master] Failed to start noon task. Ensure task exists first.
    pause
    exit /b 1
)

echo [Stocks-Master] Both tasks triggered. Check status with check-boll-daily-task.bat
pause
