@echo off
setlocal

cd /d "%~dp0"

set "VENV_PY=.venv\Scripts\python.exe"
if exist "%VENV_PY%" (
    set "PYTHON_EXE=%VENV_PY%"
) else (
    set "PYTHON_EXE=python"
)

echo [Stocks-Master] Using Python: %PYTHON_EXE%
"%PYTHON_EXE%" "Frequently-Used-Program\index_stock_data.py"
if errorlevel 1 (
    echo [Stocks-Master] Failed to build stock_data index.
    pause
    exit /b 1
)

if exist "stock_data\INDEX.md" (
    echo [Stocks-Master] Open stock_data\INDEX.md
    start "" "stock_data\INDEX.md"
)

endlocal
