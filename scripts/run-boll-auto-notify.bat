@echo off
setlocal

cd /d "%~dp0"

set "VENV_PY=..\.venv\Scripts\python.exe"
if exist "%VENV_PY%" (
    set "PYTHON_EXE=%VENV_PY%"
) else (
    set "PYTHON_EXE=python"
)

echo [Stocks-Master] Using Python: %PYTHON_EXE%

if "%CCTV_STATS_DAYS%"=="" set "CCTV_STATS_DAYS=3"
echo [Stocks-Master] CCTV stats window: %CCTV_STATS_DAYS%d

if "%WECOM_WEBHOOK_URL%"=="" (
    echo [Stocks-Master] WECOM_WEBHOOK_URL is empty. Push will be skipped unless SMTP is configured.
)

"%PYTHON_EXE%" "..\Frequently-Used-Program\auto_notify_boll.py" --fast-mode

if errorlevel 1 (
    echo [Stocks-Master] Daily run finished with errors.
) else (
    echo [Stocks-Master] Daily run finished successfully.
)

pause
