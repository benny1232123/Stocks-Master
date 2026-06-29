"""轻量任务调度器 —— 纯标准库实现，零依赖。

不依赖 APScheduler/schedule，用 threading + time + datetime 实现。
专为 24h 守护进程设计：单进程多线程，异常隔离，日志落盘。

支持的调度类型：
- daily(hour, minute, job)       每天 定时
- weekday(hour, minute, job)     工作日 定时（周一到周五，节假日需 job 内部判断）
- interval(minutes, job, trading_hours_only)  每 N 分钟（可限制盘中）

用法：
    sched = Scheduler()
    sched.weekday("21:30", daily_pick_job)
    sched.interval(5, refresh_quote_job, trading_hours_only=True)
    sched.run_forever()
"""
from __future__ import annotations

import datetime as dt
import logging
import signal
import threading
import time
from typing import Callable

logger = logging.getLogger("smcore.scheduler")

# A 股交易时段（用于 trading_hours_only）
TRADING_MORNING_START = dt.time(9, 25)
TRADING_MORNING_END = dt.time(11, 35)
TRADING_AFTERNOON_START = dt.time(12, 55)
TRADING_AFTERNOON_END = dt.time(15, 5)


def is_trading_time(now: dt.datetime | None = None) -> bool:
    """判断当前是否在 A 股交易时段内（含集合竞价）。"""
    now = now or dt.datetime.now()
    # 周末不交易
    if now.weekday() >= 5:
        return False
    t = now.time()
    return (TRADING_MORNING_START <= t <= TRADING_MORNING_END) or (
        TRADING_AFTERNOON_START <= t <= TRADING_AFTERNOON_END
    )


def is_weekday(now: dt.datetime | None = None) -> bool:
    """是否工作日（周一到周五，不含节假日判断）。"""
    now = now or dt.datetime.now()
    return now.weekday() < 5


class _Job:
    """一个定时任务。"""

    def __init__(
        self,
        name: str,
        func: Callable,
        *,
        kind: str,  # "daily" | "weekday" | "interval"
        hour_minute: str | None = None,
        interval_minutes: int | None = None,
        trading_hours_only: bool = False,
    ):
        self.name = name
        self.func = func
        self.kind = kind
        self.hour_minute = hour_minute
        self.interval_minutes = interval_minutes
        self.trading_hours_only = trading_hours_only
        self.last_run: dt.datetime | None = None
        self.last_status: str = "pending"  # pending | running | success | failed
        self.last_error: str | None = None
        self.run_count = 0

    def should_run(self, now: dt.datetime) -> bool:
        """判断此刻是否应执行。"""
        if self.kind in ("daily", "weekday"):
            h, m = self.hour_minute.split(":")
            target_time = now.replace(hour=int(h), minute=int(m), second=0, microsecond=0)
            # 当天到点且今天没跑过
            if now >= target_time:
                if self.last_run is None or self.last_run.date() < now.date():
                    if self.kind == "weekday" and not is_weekday(now):
                        return False
                    return True
            return False

        if self.kind == "interval":
            if self.trading_hours_only and not is_trading_time(now):
                return False
            if self.last_run is None:
                return True
            elapsed = (now - self.last_run).total_seconds() / 60
            return elapsed >= self.interval_minutes

        return False

    def execute(self) -> None:
        """在独立线程中执行任务，异常隔离。"""
        self.last_status = "running"
        thread = threading.Thread(target=self._run, name=f"job-{self.name}", daemon=True)
        thread.start()

    def _run(self) -> None:
        self.last_run = dt.datetime.now()
        self.run_count += 1
        start = time.time()
        try:
            logger.info("[%s] 开始执行 (第%d次)", self.name, self.run_count)
            self.func()
            elapsed = time.time() - start
            self.last_status = "success"
            logger.info("[%s] 执行成功 (%.1fs)", self.name, elapsed)
        except Exception as e:
            elapsed = time.time() - start
            self.last_status = "failed"
            self.last_error = str(e)
            logger.exception("[%s] 执行失败 (%.1fs): %s", self.name, elapsed, e)


class Scheduler:
    """任务调度器。

    主循环每 20 秒检查一次所有 job，到点的在独立线程执行。
    单个 job 失败不影响其他 job 和主循环。
    """

    def __init__(self, check_interval_seconds: int = 20):
        self._jobs: list[_Job] = []
        self._check_interval = check_interval_seconds
        self._stop = threading.Event()

    def daily(self, hour_minute: str, job: Callable, name: str | None = None) -> None:
        """每天 定时执行。"""
        self._jobs.append(
            _Job(name or job.__name__, job, kind="daily", hour_minute=hour_minute)
        )

    def weekday(self, hour_minute: str, job: Callable, name: str | None = None) -> None:
        """工作日 定时执行（周一到周五）。"""
        self._jobs.append(
            _Job(name or job.__name__, job, kind="weekday", hour_minute=hour_minute)
        )

    def interval(
        self,
        minutes: int,
        job: Callable,
        *,
        name: str | None = None,
        trading_hours_only: bool = False,
    ) -> None:
        """每 N 分钟执行一次（可限制仅盘中）。"""
        self._jobs.append(
            _Job(
                name or job.__name__,
                job,
                kind="interval",
                interval_minutes=minutes,
                trading_hours_only=trading_hours_only,
            )
        )

    def run_forever(self) -> None:
        """主循环，阻塞直到收到 SIGINT/SIGTERM。"""
        logger.info("调度器启动，注册了 %d 个任务", len(self._jobs))
        for j in self._jobs:
            logger.info("  - %s: %s %s", j.name, j.kind, j.hour_minute or f"{j.interval_minutes}min")

        # 优雅退出
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        while not self._stop.is_set():
            now = dt.datetime.now()
            for job in self._jobs:
                try:
                    if job.should_run(now) and job.last_status != "running":
                        job.execute()
                except Exception as e:
                    logger.exception("检查任务 %s 时出错: %s", job.name, e)
            self._stop.wait(self._check_interval)

        logger.info("调度器已停止")

    def _handle_signal(self, signum, frame) -> None:
        logger.info("收到信号 %d，准备退出...", signum)
        self._stop.set()

    def status(self) -> list[dict]:
        """返回所有任务状态（供 streamlit 展示）。"""
        return [
            {
                "name": j.name,
                "kind": j.kind,
                "schedule": j.hour_minute or f"{j.interval_minutes}min",
                "trading_hours_only": j.trading_hours_only,
                "last_run": j.last_run.strftime("%Y-%m-%d %H:%M:%S") if j.last_run else "-",
                "last_status": j.last_status,
                "last_error": j.last_error,
                "run_count": j.run_count,
            }
            for j in self._jobs
        ]
