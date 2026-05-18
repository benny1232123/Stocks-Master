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
echo [Stocks-Master] Compress cold data: auto_logs, plots, ui_uploads, checkpoints

set "AUTO_LOGS_KEEP_DAYS=30"
set "PLOTS_KEEP_DAYS=30"
set "UI_UPLOADS_KEEP_DAYS=30"
set "CHECKPOINTS_KEEP_DAYS=180"

if not "%~1"=="" set "AUTO_LOGS_KEEP_DAYS=%~1"
if not "%~2"=="" set "PLOTS_KEEP_DAYS=%~2"
if not "%~3"=="" set "UI_UPLOADS_KEEP_DAYS=%~3"
if not "%~4"=="" set "CHECKPOINTS_KEEP_DAYS=%~4"

echo [Stocks-Master] Keep days => auto_logs=%AUTO_LOGS_KEEP_DAYS% plots=%PLOTS_KEEP_DAYS% ui_uploads=%UI_UPLOADS_KEEP_DAYS% checkpoints=%CHECKPOINTS_KEEP_DAYS%

"%PYTHON_EXE%" "Frequently-Used-Program\compress_stock_data.py" ^
  --auto-logs-keep-days %AUTO_LOGS_KEEP_DAYS% ^
  --plots-keep-days %PLOTS_KEEP_DAYS% ^
  --ui-uploads-keep-days %UI_UPLOADS_KEEP_DAYS% ^
  --checkpoints-keep-days %CHECKPOINTS_KEEP_DAYS% ^
  %*

if errorlevel 1 (
    echo [Stocks-Master] Compression finished with errors.
) else (
    echo [Stocks-Master] Compression finished successfully.
)

pause