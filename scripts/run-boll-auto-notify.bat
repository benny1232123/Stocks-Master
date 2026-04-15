@echo off
setlocal

cd /d "%~dp0"

set "VENV_PY=..\.venv\Scripts\python.exe"
if exist "%VENV_PY%" (
    set "PYTHON_EXE=%VENV_PY%"
) else (
    set "PYTHON_EXE=python"
)

echo [Stocks-Master] Using Python: %PYTHON_EXE%

if "%MIN_STOCK_PRICE%"=="" set "MIN_STOCK_PRICE=5"
if "%MAX_STOCK_PRICE%"=="" set "MAX_STOCK_PRICE=30"
if "%THEME_MIN_LATEST_PRICE%"=="" set "THEME_MIN_LATEST_PRICE=%MIN_STOCK_PRICE%"
if "%THEME_MAX_LATEST_PRICE%"=="" set "THEME_MAX_LATEST_PRICE=%MAX_STOCK_PRICE%"
if "%RELATIVITY_MIN_PRICE%"=="" set "RELATIVITY_MIN_PRICE=%MIN_STOCK_PRICE%"
if "%RELATIVITY_MAX_PRICE%"=="" set "RELATIVITY_MAX_PRICE=%MAX_STOCK_PRICE%"
echo [Stocks-Master] Stock price range: [%MIN_STOCK_PRICE%, %MAX_STOCK_PRICE%] (theme=%THEME_MIN_LATEST_PRICE%~%THEME_MAX_LATEST_PRICE%, relativity=%RELATIVITY_MIN_PRICE%~%RELATIVITY_MAX_PRICE%)

if "%CCTV_STATS_DAYS%"=="" set "CCTV_STATS_DAYS=3"
echo [Stocks-Master] CCTV stats window: %CCTV_STATS_DAYS%d

if "%CCTV_AUTO_ACCEPT_KEYWORDS%"=="" set "CCTV_AUTO_ACCEPT_KEYWORDS=1"
if "%CCTV_AUTO_ACCEPT_MIN_COUNT%"=="" set "CCTV_AUTO_ACCEPT_MIN_COUNT=4"
if "%CCTV_AUTO_ACCEPT_MIN_CONF%"=="" set "CCTV_AUTO_ACCEPT_MIN_CONF=medium"
echo [Stocks-Master] CCTV auto keyword update: %CCTV_AUTO_ACCEPT_KEYWORDS% (min_count=%CCTV_AUTO_ACCEPT_MIN_COUNT%, min_conf=%CCTV_AUTO_ACCEPT_MIN_CONF%)

if "%CCTV_DISABLE_EXTRA_NEWS%"=="" set "CCTV_DISABLE_EXTRA_NEWS=0"
if "%CCTV_EXTRA_NEWS_SOURCES%"=="" set "CCTV_EXTRA_NEWS_SOURCES=cls,sina"
if "%CCTV_EXTRA_NEWS_LIMIT%"=="" set "CCTV_EXTRA_NEWS_LIMIT=120"
echo [Stocks-Master] CCTV extra news: disable=%CCTV_DISABLE_EXTRA_NEWS% sources=%CCTV_EXTRA_NEWS_SOURCES% limit=%CCTV_EXTRA_NEWS_LIMIT%

if "%ENABLE_RELATIVITY_STRATEGY%"=="" set "ENABLE_RELATIVITY_STRATEGY=1"
if "%RELATIVITY_MAX_WORKERS%"=="" set "RELATIVITY_MAX_WORKERS=1"
if "%RELATIVITY_RESUME%"=="" set "RELATIVITY_RESUME=1"
if "%RELATIVITY_SLEEP_SECONDS%"=="" set "RELATIVITY_SLEEP_SECONDS=2"
if "%RELATIVITY_DISABLE_RS%"=="" set "RELATIVITY_DISABLE_RS=0"
if "%RELATIVITY_USE_SEED%"=="" set "RELATIVITY_USE_SEED=0"
echo [Stocks-Master] Relativity strategy enabled: %ENABLE_RELATIVITY_STRATEGY%
echo [Stocks-Master] Relativity params: workers=%RELATIVITY_MAX_WORKERS% resume=%RELATIVITY_RESUME% sleep=%RELATIVITY_SLEEP_SECONDS% disable_rs=%RELATIVITY_DISABLE_RS% use_seed=%RELATIVITY_USE_SEED%
echo [Stocks-Master] Optional allocation env: ALLOC_UP_* / ALLOC_DOWN_* / ALLOC_SIDE_* (unit=percent)
echo [Stocks-Master] Example: set ALLOC_SIDE_BOLL=35 ^& set ALLOC_SIDE_THEME=30

if "%WECOM_WEBHOOK_URL%"=="" (
    echo [Stocks-Master] WECOM_WEBHOOK_URL is empty. Push will be skipped unless SMTP is configured.
)

"%PYTHON_EXE%" "..\Frequently-Used-Program\auto_notify_boll.py" --fast-mode

if errorlevel 1 (
    echo [Stocks-Master] Daily run finished with errors.
) else (
    echo [Stocks-Master] Daily run finished successfully.
)

pause
