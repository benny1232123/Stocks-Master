@echo off
setlocal

cd /d "%~dp0"

set "VENV_PY=.venv\Scripts\python.exe"
if exist "%VENV_PY%" (
    set "PYTHON_EXE=%VENV_PY%"
) else (
    set "PYTHON_EXE=python"
)

set "KEEP_DAYS=30"
if not "%~1"=="" set "KEEP_DAYS=%~1"

echo [Stocks-Master] Using Python: %PYTHON_EXE%
echo [Stocks-Master] Cleanup keep-days: %KEEP_DAYS%

"%PYTHON_EXE%" "Frequently-Used-Program\cleanup_stock_data.py" --keep-days %KEEP_DAYS% --log-keep-days %KEEP_DAYS% --plots-keep-days %KEEP_DAYS%

if errorlevel 1 (
    echo [Stocks-Master] Cleanup finished with errors.
) else (
    echo [Stocks-Master] Cleanup finished successfully.
)

pause
