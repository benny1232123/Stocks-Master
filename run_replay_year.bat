@echo off
REM ============================================================
REM  Replay History - one-click runner (Windows)
REM  Extends fuse + backtest back over the past year (weekly, ~52 days)
REM
REM  RESUMABLE: if it stops or fails, just run this file again.
REM             Finished dates are skipped automatically.
REM  FAIL LOGS: .workbuddy\replay_results\logs\  (one file per date+strategy)
REM
REM  Optional: pass your own args, e.g.
REM     run_replay_year.bat --phase replay --dry-run
REM     run_replay_year.bat --phase all --cadence weekly --limit 3
REM  With no args it uses: --phase all --cadence weekly --retries 3
REM  NOTE: keep this file pure ASCII (cmd.exe misparses UTF-8 Chinese).
REM ============================================================
cd /d "%~dp0"
setlocal
chcp 65001 >nul 2>nul
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

REM --- pick python ---
REM  Anaconda FIRST on purpose: this project needs Anaconda's env for
REM  akshare compatibility, and PATH "python" on Win10/11 is often the
REM  Microsoft Store alias stub (opens the Store instead of running).
REM  Override anytime:  set REPLAY_PY=C:\path\to\python.exe
set "PY="
if defined REPLAY_PY set "PY=%REPLAY_PY%"
if not defined PY if exist "E:\Anaconda\python.exe" set "PY=E:\Anaconda\python.exe"
if not defined PY if exist "%USERPROFILE%\anaconda3\python.exe" set "PY=%USERPROFILE%\anaconda3\python.exe"
if not defined PY if exist "C:\Anaconda\python.exe" set "PY=C:\Anaconda\python.exe"
if not defined PY if exist "C:\ProgramData\Anaconda3\python.exe" set "PY=C:\ProgramData\Anaconda3\python.exe"
if not defined PY where python >nul 2>nul && set "PY=python"
if not defined PY goto :nopy

REM --- args: use caller args if given, else defaults ---
set "ARGS=%*"
if not defined ARGS set "ARGS=--phase all --cadence weekly --retries 3"

echo [Replay] Python : %PY%
echo [Replay] Args   : %ARGS%
echo [Replay] Resumable - re-run this file to continue after any interruption.
echo.

"%PY%" -u scripts\replay_history.py %ARGS%
set "RC=%errorlevel%"

echo.
echo [Replay] exit code = %RC%
if not "%RC%"=="0" echo [Replay] Some dates failed. Re-run this file to resume. Details: .workbuddy\replay_results\logs
echo.
pause
exit /b %RC%

:nopy
echo [Replay] ERROR: no usable python found.
echo [Replay] Set one and retry, for example:
echo     set REPLAY_PY=E:\Anaconda\python.exe
echo     run_replay_year.bat
echo.
pause
exit /b 9009
