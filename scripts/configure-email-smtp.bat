@echo off
setlocal

echo [Stocks-Master] Configure SMTP variables (User scope)
echo.

set /p SMTP_HOST=SMTP_HOST (e.g. smtp.qq.com): 
set /p SMTP_PORT=SMTP_PORT (default 465): 
if "%SMTP_PORT%"=="" set "SMTP_PORT=465"
set /p SMTP_USER=SMTP_USER (sender email): 
set /p SMTP_PASS=SMTP_PASS (auth code, not login password): 
set /p SMTP_TO=SMTP_TO (receiver email): 

if "%SMTP_HOST%"=="" goto :missing
if "%SMTP_USER%"=="" goto :missing
if "%SMTP_PASS%"=="" goto :missing
if "%SMTP_TO%"=="" goto :missing

setx SMTP_HOST "%SMTP_HOST%" >nul
setx SMTP_PORT "%SMTP_PORT%" >nul
setx SMTP_USER "%SMTP_USER%" >nul
setx SMTP_PASS "%SMTP_PASS%" >nul
setx SMTP_TO "%SMTP_TO%" >nul

echo.
echo [Stocks-Master] Saved. Please close and reopen terminal/VS Code.
echo [Stocks-Master] Then run test-email-notify.bat
pause
exit /b 0

:missing
echo.
echo [Stocks-Master] Required field is empty. Nothing was saved.
pause
exit /b 1
