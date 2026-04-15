@echo off
setlocal

cd /d "%~dp0"

if /I "%~1"=="run" goto run
if /I "%~1"=="register" goto register
if /I "%~1"=="trigger" goto trigger
if /I "%~1"=="check" goto check
if /I "%~1"=="clean" goto clean
if /I "%~1"=="visual" goto visual
if /I "%~1"=="config-email" goto config_email
if /I "%~1"=="test-email" goto test_email
if /I "%~1"=="cctv" goto cctv
if /I "%~1"=="index" goto index
if /I "%~1"=="archive" goto archive
if /I "%~1"=="signal-backtest" goto signal_backtest
if /I "%~1"=="trade-backtest" goto trade_backtest
if /I "%~1"=="backtest-ui" goto backtest_ui

:menu
echo ========================================
echo Stocks-Master Unified Launcher
echo ========================================
echo [1] Run BOLL notify now
echo [2] Register daily task (21:30)
echo [3] Trigger daily task now
echo [4] Check task status
echo [5] Cleanup stock_data (default 30 days)
echo [6] Start BOLL visualizer
echo [7] Configure SMTP email
echo [8] Test email notify
echo [9] Run CCTV sectors strategy
echo [10] Build stock_data quick index
echo [11] Organize + secondary archive stock_data
echo [12] Run signal backtest
echo [13] Run tradebook backtest
echo [14] Open backtest software UI
echo [0] Exit
echo.
set /p "CHOICE=Select an option (0-14): "

if "%CHOICE%"=="1" goto run
if "%CHOICE%"=="2" goto register
if "%CHOICE%"=="3" goto trigger
if "%CHOICE%"=="4" goto check
if "%CHOICE%"=="5" goto clean
if "%CHOICE%"=="6" goto visual
if "%CHOICE%"=="7" goto config_email
if "%CHOICE%"=="8" goto test_email
if "%CHOICE%"=="9" goto cctv
if "%CHOICE%"=="10" goto index
if "%CHOICE%"=="11" goto archive
if "%CHOICE%"=="12" goto signal_backtest
if "%CHOICE%"=="13" goto trade_backtest
if "%CHOICE%"=="14" goto backtest_ui
if "%CHOICE%"=="0" goto end
echo Invalid option.
echo.
goto menu

:run
call run-boll-auto-notify.bat
goto end

:register
call register-boll-daily-task.bat
goto end

:trigger
call run-boll-daily-task-now.bat
goto end

:check
call check-boll-daily-task.bat
goto end

:clean
if not "%~2"=="" (
    call clean-stock-data.bat %~2
) else (
    set /p "KEEP_DAYS=Keep days (default 30): "
    if "%KEEP_DAYS%"=="" (
        call clean-stock-data.bat
    ) else (
        call clean-stock-data.bat %KEEP_DAYS%
    )
)
goto end

:visual
call start-boll-visualizer.bat
goto end

:config_email
call configure-email-smtp.bat
goto end

:test_email
call test-email-notify.bat
goto end

:cctv
set "VENV_PY=.venv\Scripts\python.exe"
if exist "%VENV_PY%" (
    set "PYTHON_EXE=%VENV_PY%"
) else (
    set "PYTHON_EXE=python"
)
echo [Stocks-Master] Using Python: %PYTHON_EXE%
"%PYTHON_EXE%" "Frequently-Used-Program\Stock-Selection-CCTV-Sectors.py"
pause
goto end

:index
call index-stock-data.bat
goto end

:archive
set /p "KEEP_ROOT_DAYS=Keep recent days in stock_data root (default 7): "
if "%KEEP_ROOT_DAYS%"=="" set "KEEP_ROOT_DAYS=7"
set /p "ARCHIVE_KEEP_DAYS=Delete archive older than days (default 365): "
if "%ARCHIVE_KEEP_DAYS%"=="" set "ARCHIVE_KEEP_DAYS=365"
set /p "ARCHIVE_ALL=Archive all dated root files now? (y/N): "
echo [Stocks-Master] Step1: organize existing archive layout
echo [Stocks-Master] Step2: move old root files to secondary archive
if /I "%ARCHIVE_ALL%"=="y" (
    call auto-archive-stock-data.bat %KEEP_ROOT_DAYS% %ARCHIVE_KEEP_DAYS% all
) else (
    call auto-archive-stock-data.bat %KEEP_ROOT_DAYS% %ARCHIVE_KEEP_DAYS%
)
goto end

:signal_backtest
call run-backtest-signal-picks.bat
goto end

:trade_backtest
call run-backtest-tradebook.bat
goto end

:backtest_ui
call start-backtest-center.bat
goto end

:end
endlocal
