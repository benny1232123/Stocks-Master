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

if "%TRADES_CSV%"=="" set "TRADES_CSV=stock_data/my_trades.csv"

echo [Stocks-Master] trades_csv=%TRADES_CSV%
if not "%BUY_CSV%"=="" echo [Stocks-Master] buy_csv=%BUY_CSV%
if not "%SELL_CSV%"=="" echo [Stocks-Master] sell_csv=%SELL_CSV%
if not "%OUTPUT_PREFIX%"=="" echo [Stocks-Master] output_prefix=%OUTPUT_PREFIX%

set "OUT_ARG="
if not "%OUTPUT_PREFIX%"=="" set "OUT_ARG=--output-prefix %OUTPUT_PREFIX%"

if not "%BUY_CSV%"=="" if not "%SELL_CSV%"=="" goto run_two_files

if exist "%TRADES_CSV%" goto run_single_file
if exist "..\%TRADES_CSV%" goto run_single_file
(
  echo [Stocks-Master] Trade file not found: %TRADES_CSV%
  echo [Stocks-Master] Please set TRADES_CSV to your exported trade file.
  echo [Stocks-Master] Example: set TRADES_CSV=stock_data/my_trades.template.csv
  if exist "..\stock_data\my_trades.template.csv" (
    echo [Stocks-Master] Template available: ..\stock_data\my_trades.template.csv
  )
  goto done
)

:run_single_file
"%PYTHON_EXE%" "..\Frequently-Used-Program\backtest_tradebook.py" ^
  --trades-csv "%TRADES_CSV%" ^
  %OUT_ARG% ^
  %*

goto done

:run_two_files
if not exist "%BUY_CSV%" if not exist "..\%BUY_CSV%" (
  echo [Stocks-Master] Buy file not found: %BUY_CSV%
  goto done
)
if not exist "%SELL_CSV%" if not exist "..\%SELL_CSV%" (
  echo [Stocks-Master] Sell file not found: %SELL_CSV%
  goto done
)
"%PYTHON_EXE%" "..\Frequently-Used-Program\backtest_tradebook.py" ^
  --buy-csv "%BUY_CSV%" ^
  --sell-csv "%SELL_CSV%" ^
  %OUT_ARG% ^
  %*

:done
if errorlevel 1 (
    echo [Stocks-Master] Tradebook backtest finished with errors.
) else (
    echo [Stocks-Master] Tradebook backtest finished successfully.
)

pause
