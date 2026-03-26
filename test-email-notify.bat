@echo off
setlocal

cd /d "%~dp0"

set "VENV_PY=.venv\Scripts\python.exe"
if exist "%VENV_PY%" (
    set "PYTHON_EXE=%VENV_PY%"
) else (
    set "PYTHON_EXE=python"
)

echo [Stocks-Master] Testing email only...
"%PYTHON_EXE%" "Frequently-Used-Program\auto_notify_boll.py" --test-email-only

if errorlevel 1 (
    echo [Stocks-Master] Test email failed. Check stock_data\auto_logs\ latest log.
) else (
    echo [Stocks-Master] Test email sent successfully.
)

pause
