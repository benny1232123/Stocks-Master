@echo off
setlocal

set ROOT=%~dp0

start "Stocks-Master Backend" cmd /k "E:\Anaconda\python.exe %ROOT%app.py"
start "Stocks-Master Frontend" cmd /k "cd /d %ROOT%frontend && npm run dev"

echo Backend and frontend are starting in separate windows.
echo Backend: http://localhost:8000
echo Frontend: http://localhost:5173