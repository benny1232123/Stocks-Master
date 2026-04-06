@echo off
setlocal

set "TASK_NAME=StocksMaster-Boll-Daily"
set "SCRIPT_KEYWORD=auto_notify_boll.py"

echo [Stocks-Master] Pre-cleanup: stop previous running instance (if any)
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='SilentlyContinue'; Stop-ScheduledTask -TaskName '%TASK_NAME%'; Start-Sleep -Seconds 1; $stopped=0; Get-CimInstance Win32_Process | ForEach-Object { $cmd=$_.CommandLine; if($cmd -and $cmd -like '*%SCRIPT_KEYWORD%*' -and ($_.Name -ieq 'python.exe' -or $_.Name -ieq 'pythonw.exe')) { try { Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop; $stopped++ } catch {} } }; Write-Host ('[Stocks-Master] Pre-cleanup finished. Killed python processes: ' + $stopped)"

echo [Stocks-Master] Start task now: %TASK_NAME%
schtasks /Run /TN "%TASK_NAME%"

if errorlevel 1 (
        echo [Stocks-Master] Failed to start task. Ensure task exists first.
    pause
    exit /b 1
)

echo [Stocks-Master] Task triggered. Showing live progress...
echo.
powershell -NoProfile -ExecutionPolicy Bypass -Command "$taskName='%TASK_NAME%'; $deadline=(Get-Date).AddMinutes(20); $runningSeen=$false; while((Get-Date) -lt $deadline){ $task=Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; if(-not $task){ Write-Host '[Stocks-Master] Task missing. Please run register-boll-daily-task.bat first.'; exit 1 }; $info=Get-ScheduledTaskInfo -TaskName $taskName; $state=[string]$task.State; $lastRun=[string]$info.LastRunTime; $nextRun=[string]$info.NextRunTime; $lastResult=[int]$info.LastTaskResult; Write-Host ((Get-Date -Format 'HH:mm:ss') + '  State=' + $state + '  LastResult=' + $lastResult + '  LastRun=' + $lastRun + '  NextRun=' + $nextRun); if($state -eq 'Running'){ $runningSeen=$true }; if($runningSeen -and $state -ne 'Running'){ if($lastResult -eq 0){ Write-Host '[Stocks-Master] Task finished successfully.'; exit 0 } else { Write-Host ('[Stocks-Master] Task finished with code ' + $lastResult); exit $lastResult } }; Start-Sleep -Seconds 2 }; Write-Host '[Stocks-Master] Progress wait timeout (20min). Use check-boll-daily-task.bat for latest status.'; exit 2"

if errorlevel 1 (
        echo [Stocks-Master] Task ended with non-zero status.
) else (
        echo [Stocks-Master] Task run completed.
)

pause
