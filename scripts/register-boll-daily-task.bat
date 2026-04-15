@echo off
setlocal

cd /d "%~dp0"

set "TASK_NAME_EVENING=StocksMaster-Boll-Daily"
set "OLD_TASK_STARTUP=StocksMaster-Boll-Startup"
set "OLD_TASK_NOON=StocksMaster-Boll-Noon"
set "RUN_TIME_EVENING=21:30"

set "VENV_PY=%~dp0.venv\Scripts\python.exe"
if exist "%VENV_PY%" (
    set "PYTHON_EXE=%VENV_PY%"
) else (
    set "PYTHON_EXE=python"
)

set "PROJECT_ROOT=%~dp0.."
set "SCRIPT_PATH=%PROJECT_ROOT%\Frequently-Used-Program\auto_notify_boll.py"
set "TASK_CMD=\"%PYTHON_EXE%\" \"%SCRIPT_PATH%\" --fast-mode"

echo [Stocks-Master] Create/Update daily task
echo [Stocks-Master] Evening: %TASK_NAME_EVENING% at %RUN_TIME_EVENING%
echo [Stocks-Master] Command: %TASK_CMD%

schtasks /Delete /TN "%OLD_TASK_STARTUP%" /F >nul 2>&1
schtasks /Delete /TN "%OLD_TASK_NOON%" /F >nul 2>&1

schtasks /Create /SC DAILY /TN "%TASK_NAME_EVENING%" /TR "%TASK_CMD%" /ST %RUN_TIME_EVENING% /F

if errorlevel 1 (
    echo [Stocks-Master] Failed to create daily task. Try run as Administrator.
    pause
    exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable; Set-ScheduledTask -TaskName '%TASK_NAME_EVENING%' -Settings $settings | Out-Null; Write-Host '[Stocks-Master] Enabled StartWhenAvailable (missed time will run once after boot).' } catch { Write-Host '[Stocks-Master] Could not set StartWhenAvailable automatically. You can enable it manually in Task Scheduler.'; Write-Host $_.Exception.Message }"

echo [Stocks-Master] Task created. You can check with:
echo schtasks /Query /TN "%TASK_NAME_EVENING%" /V /FO LIST

pause
