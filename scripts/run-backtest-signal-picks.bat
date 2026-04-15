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

if "%SIGNALS_GLOB%"=="" set "SIGNALS_GLOB=stock_data/Stock-Selection-Boll-*.csv"
if "%TOP_N%"=="" set "TOP_N=10"
if "%HOLD_DAYS%"=="" set "HOLD_DAYS=5"

if "%BUY_SLIP_BPS%"=="" set "BUY_SLIP_BPS=5"
if "%SELL_SLIP_BPS%"=="" set "SELL_SLIP_BPS=5"
if "%BUY_FEE_RATE%"=="" set "BUY_FEE_RATE=0.0003"
if "%SELL_FEE_RATE%"=="" set "SELL_FEE_RATE=0.0003"
if "%SELL_STAMP_TAX_RATE%"=="" set "SELL_STAMP_TAX_RATE=0.001"

echo [Stocks-Master] signals_glob=%SIGNALS_GLOB%
echo [Stocks-Master] top_n=%TOP_N% hold_days=%HOLD_DAYS%
echo [Stocks-Master] costs: buy_slip=%BUY_SLIP_BPS%bps sell_slip=%SELL_SLIP_BPS%bps buy_fee=%BUY_FEE_RATE% sell_fee=%SELL_FEE_RATE% stamp_tax=%SELL_STAMP_TAX_RATE%

set "START_DATE_ARG="
if not "%START_DATE%"=="" set "START_DATE_ARG=--start-date %START_DATE%"
set "END_DATE_ARG="
if not "%END_DATE%"=="" set "END_DATE_ARG=--end-date %END_DATE%"
set "OUTPUT_PREFIX_ARG="
if not "%OUTPUT_PREFIX%"=="" set "OUTPUT_PREFIX_ARG=--output-prefix %OUTPUT_PREFIX%"

"%PYTHON_EXE%" "..\Frequently-Used-Program\backtest_signal_picks.py" ^
  --signals-glob "%SIGNALS_GLOB%" ^
  --top-n %TOP_N% ^
  --hold-days %HOLD_DAYS% ^
  --buy-slip-bps %BUY_SLIP_BPS% ^
  --sell-slip-bps %SELL_SLIP_BPS% ^
  --buy-fee-rate %BUY_FEE_RATE% ^
  --sell-fee-rate %SELL_FEE_RATE% ^
  --sell-stamp-tax-rate %SELL_STAMP_TAX_RATE% ^
  %START_DATE_ARG% ^
  %END_DATE_ARG% ^
  %OUTPUT_PREFIX_ARG% ^
  %*

if errorlevel 1 (
    echo [Stocks-Master] Signal backtest finished with errors.
) else (
    echo [Stocks-Master] Signal backtest finished successfully.
)

pause
