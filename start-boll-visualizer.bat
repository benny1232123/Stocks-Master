@echo off
setlocal

cd /d "%~dp0Frequently-Used-Program\boll-visualizer"

set "VENV_PY=..\..\.venv\Scripts\python.exe"
if exist "%VENV_PY%" (
    set "PYTHON_EXE=%VENV_PY%"
) else (
    set "PYTHON_EXE=python"
)

echo [Boll Visualizer] Using Python: %PYTHON_EXE%
"%PYTHON_EXE%" -c "import streamlit" >nul 2>&1
if errorlevel 1 (
    echo [Boll Visualizer] Installing dependencies...
    "%PYTHON_EXE%" -m pip install -r requirements.txt
)

set "PORT=8520"
set "MAX_PORT=8599"

:find_free_port
netstat -ano | findstr /r /c:":%PORT% " >nul
if not errorlevel 1 (
    set /a PORT+=1
    if %PORT% LEQ %MAX_PORT% goto find_free_port
    echo [Boll Visualizer] No free port found between 8520 and 8599.
    pause
    exit /b 1
)

if not "%PORT%"=="8520" (
    echo [Boll Visualizer] Port 8520 is busy, switched to %PORT%.
)

echo [Boll Visualizer] Starting at http://localhost:%PORT%
start "" "http://localhost:%PORT%"
"%PYTHON_EXE%" -m streamlit run src\app.py --server.port %PORT%

pause
