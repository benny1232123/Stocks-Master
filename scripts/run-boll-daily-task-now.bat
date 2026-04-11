@echo off
setlocal

set "TASK_NAME=StocksMaster-Boll-Daily"
set "ROOT_DIR=%~dp0.."
set "LOG_DIR=%ROOT_DIR%\stock_data\auto_logs"

echo [Stocks-Master] Start task now: %TASK_NAME%
schtasks /Run /TN "%TASK_NAME%"

if errorlevel 1 (
        echo [Stocks-Master] Failed to start task. Ensure task exists first.
    pause
    exit /b 1
)

echo [Stocks-Master] Task triggered. Tailing live log output...
echo.
powershell -NoProfile -ExecutionPolicy Bypass -Command "$taskName='%TASK_NAME%'; $logDir='%LOG_DIR%'; $deadline=(Get-Date).AddMinutes(20); $start=(Get-Date); if(-not (Test-Path $logDir)){ Write-Host '[Stocks-Master] Log dir not found: ' + $logDir; exit 1 }; $log=$null; while(-not $log -and (Get-Date) -lt $deadline){ $log=Get-ChildItem -Path $logDir -Filter 'boll_auto_*.log' | Where-Object { $_.LastWriteTime -ge $start.AddSeconds(-5) } | Sort-Object LastWriteTime -Descending | Select-Object -First 1; if(-not $log){ Start-Sleep -Seconds 1 } }; if(-not $log){ Write-Host '[Stocks-Master] No new log found yet. Waiting for the latest log...'; $log=Get-ChildItem -Path $logDir -Filter 'boll_auto_*.log' | Sort-Object LastWriteTime -Descending | Select-Object -First 1 }; if(-not $log){ Write-Host '[Stocks-Master] No log file found.'; exit 1 }; Write-Host ('[Stocks-Master] Live log: ' + $log.FullName); $job = Start-Job -ScriptBlock { param($path) Get-Content -Path $path -Wait } -ArgumentList $log.FullName; while((Get-Date) -lt $deadline){ $task=Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue; if(-not $task){ Write-Host '[Stocks-Master] Task missing. Please run register-boll-daily-task.bat first.'; break }; $info=Get-ScheduledTaskInfo -TaskName $taskName; $state=[string]$task.State; $lastResult=[int]$info.LastTaskResult; if($state -ne 'Running'){ if($lastResult -eq 0){ Write-Host '[Stocks-Master] Task finished successfully.' } else { Write-Host ('[Stocks-Master] Task finished with code ' + $lastResult) }; break }; Start-Sleep -Seconds 2 }; if($job){ Stop-Job $job | Out-Null; Remove-Job $job | Out-Null }; if((Get-Date) -ge $deadline){ Write-Host '[Stocks-Master] Progress wait timeout (20min). Use check-boll-daily-task.bat for latest status.'; exit 2 }"

if errorlevel 1 (
        echo [Stocks-Master] Task ended with non-zero status.
) else (
        echo [Stocks-Master] Task run completed.
)

pause
