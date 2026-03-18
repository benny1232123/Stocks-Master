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

set "LOCAL_URL=http://localhost:%PORT%"
set "LAN_IP="
for /f "usebackq delims=" %%i in (`powershell -NoProfile -Command "$ErrorActionPreference='SilentlyContinue';$ip=(Get-NetIPAddress -AddressFamily IPv4 ^| Where-Object { $_.IPAddress -ne '127.0.0.1' -and $_.IPAddress -notlike '169.254*' -and $_.InterfaceAlias -notmatch 'Loopback^|vEthernet^|Hyper-V^|Virtual' } ^| Select-Object -First 1 -ExpandProperty IPAddress);if($ip){$ip}"`) do set "LAN_IP=%%i"

echo [Boll Visualizer] Local URL: %LOCAL_URL%
if defined LAN_IP (
    echo [Boll Visualizer] LAN URL: http://%LAN_IP%:%PORT%
    echo [Boll Visualizer] Same-network devices can open the LAN URL.
) else (
    echo [Boll Visualizer] LAN URL: unavailable
)

set "PUBLIC_TUNNEL="
where cloudflared >nul 2>&1
if not errorlevel 1 (
    set /p "PUBLIC_TUNNEL=[Boll Visualizer] cloudflared found. Create shareable public URL? (y/N): "
) else (
    echo [Boll Visualizer] cloudflared not found, skip public URL.
    echo [Boll Visualizer] Install guide: https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/install-and-setup/installation/
)

if /I "%PUBLIC_TUNNEL%"=="y" (
    echo [Boll Visualizer] Starting public tunnel in another window...
    start "Boll Visualizer Tunnel" cmd /k "timeout /t 5 >nul ^& cloudflared tunnel --url http://localhost:%PORT%"
)

start "" "%LOCAL_URL%"
"%PYTHON_EXE%" -m streamlit run src\app.py --server.port %PORT% --server.address 0.0.0.0

pause
