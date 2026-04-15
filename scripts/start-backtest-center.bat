@echo off
setlocal

cd /d "%~dp0"

set "VENV_PY=..\.venv\Scripts\python.exe"
if exist "%VENV_PY%" (
    set "PYTHON_EXE=%VENV_PY%"
) else (
    set "PYTHON_EXE=python"
)

echo [Backtest Center] Using Python: %PYTHON_EXE%
"%PYTHON_EXE%" -c "import streamlit" >nul 2>&1
if errorlevel 1 (
    echo [Backtest Center] Installing dependencies...
    "%PYTHON_EXE%" -m pip install -r ..\requirements.txt
)

set "PORT=8530"
set "MAX_PORT=8599"

:find_free_port
netstat -ano | findstr /r /c:":%PORT% " >nul
if not errorlevel 1 (
    set /a PORT+=1
    if %PORT% LEQ %MAX_PORT% goto find_free_port
    echo [Backtest Center] No free port found between 8530 and 8599.
    pause
    exit /b 1
)

set "LOCAL_URL=http://localhost:%PORT%"
echo [Backtest Center] Local URL: %LOCAL_URL%
start "" "%LOCAL_URL%"

"%PYTHON_EXE%" -m streamlit run ..\Frequently-Used-Program\backtest_center_app.py --server.port %PORT% --server.address 0.0.0.0

pause
