@echo off
setlocal

set "TASK_NAME=StocksMaster-Boll-Daily"

echo [Stocks-Master] Task summary
powershell -NoProfile -ExecutionPolicy Bypass -Command "$n='%TASK_NAME%'; $t=Get-ScheduledTask -TaskName $n -ErrorAction SilentlyContinue; if($t){ $i=Get-ScheduledTaskInfo -TaskName $n; [PSCustomObject]@{TaskName=$n; Enabled=$t.Settings.Enabled; State=$t.State; NextRunTime=$i.NextRunTime; LastRunTime=$i.LastRunTime; LastTaskResult=$i.LastTaskResult} | Format-Table -AutoSize } else { [PSCustomObject]@{TaskName=$n; Enabled='(missing)'; State='(missing)'; NextRunTime='(missing)'; LastRunTime=''; LastTaskResult=''} | Format-Table -AutoSize }"

echo.
echo [Stocks-Master] If any task is missing, run register-boll-daily-task.bat

pause
