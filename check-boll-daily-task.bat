@echo off
setlocal

set "TASK_NAME_EVENING=StocksMaster-Boll-Daily"
set "TASK_NAME_NOON=StocksMaster-Boll-Noon"

echo [Stocks-Master] Task summary
powershell -NoProfile -ExecutionPolicy Bypass -Command "$rows=@(); $names=@('%TASK_NAME_EVENING%','%TASK_NAME_NOON%'); foreach($n in $names){ $t=Get-ScheduledTask -TaskName $n -ErrorAction SilentlyContinue; if($t){ $i=Get-ScheduledTaskInfo -TaskName $n; $rows += [PSCustomObject]@{TaskName=$n; Enabled=$t.Settings.Enabled; NextRunTime=$i.NextRunTime; LastRunTime=$i.LastRunTime; LastTaskResult=$i.LastTaskResult} } else { $rows += [PSCustomObject]@{TaskName=$n; Enabled='(missing)'; NextRunTime='(missing)'; LastRunTime=''; LastTaskResult=''} } }; $rows | Format-Table -AutoSize"

echo.
echo [Stocks-Master] If any task is missing, run register-boll-daily-task.bat

pause
