@echo off
setlocal

cd /d "%~dp0"

set "VENV_PY=.venv\Scripts\python.exe"
if exist "%VENV_PY%" (
    set "PYTHON_EXE=%VENV_PY%"
) else (
    set "PYTHON_EXE=python"
)

set "KEEP_ROOT_DAYS=7"
set "ARCHIVE_KEEP_DAYS=365"
set "ARCHIVE_MODE=recent"
if not "%~1"=="" set "KEEP_ROOT_DAYS=%~1"
if not "%~2"=="" set "ARCHIVE_KEEP_DAYS=%~2"
if /I "%~3"=="all" set "ARCHIVE_MODE=all"

echo [Stocks-Master] Using Python: %PYTHON_EXE%
echo [Stocks-Master] Archive keep-root-days: %KEEP_ROOT_DAYS%
echo [Stocks-Master] Archive keep-days: %ARCHIVE_KEEP_DAYS%
echo [Stocks-Master] Archive mode: %ARCHIVE_MODE%
echo [Stocks-Master] Archive layout: archive\YYYYMM\category\file (secondary-level)

set "ARCHIVE_ARGS=--keep-root-days %KEEP_ROOT_DAYS% --archive-keep-days %ARCHIVE_KEEP_DAYS% --secondary-level"
if /I "%ARCHIVE_MODE%"=="all" set "ARCHIVE_ARGS=%ARCHIVE_ARGS% --archive-all-root-dated"

"%PYTHON_EXE%" "Frequently-Used-Program\archive_stock_data.py" %ARCHIVE_ARGS%

if errorlevel 1 (
    echo [Stocks-Master] Archive finished with errors.
) else (
    echo [Stocks-Master] Archive finished successfully.
)

pause
