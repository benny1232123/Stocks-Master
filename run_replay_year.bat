@echo off
REM ============================================================
REM  历史回放全量驱动 - Windows 一键运行
REM  把融合+回测口径延伸回过去一整年(默认周频 52 日)
REM  断点续跑: 中断/失败后直接重跑本文件即可, 已完成的日自动跳过
REM  单日失败详情: .workbuddy\replay_results\logs\<日期>_<策略>.log
REM ============================================================
cd /d %~dp0
setlocal
set PYTHONIOENCODING=utf-8

REM 选 python: 优先 PATH, 否则回退常见 Anaconda 路径
set PY=
where python >nul 2>nul && set PY=python
if not defined PY if exist "E:\Anaconda\python.exe" set PY=E:\Anaconda\python.exe
if not defined PY if exist "C:\Users\%USERNAME%\anaconda3\python.exe" set PY=C:\Users\%USERNAME%\anaconda3\python.exe
if not defined PY set PY=python

echo [Replay] Using Python: %PY%
echo [Replay] phase=all cadence=weekly retries=3  (断点续跑, 失败自动重试)
echo.
%PY% scripts\replay_history.py --phase all --cadence weekly --retries 3
echo.
echo [Replay] 退出码=%errorlevel%
if not "%errorlevel%"=="0" (
  echo 有日失败(已重试仍失败). 直接重跑本文件续跑; 详情看 .workbuddy\replay_results\logs\
)
pause
