"""Stocks-Master 后台守护进程入口。

24h 常驻运行，独立于 streamlit，负责：
- 工作日 21:30 自动选股 + 推送（替代 Windows 任务计划程序）
- 盘中每 5 分钟刷新行情快照缓存
- 盘中每 10 分钟检查预警（触止损/止盈/买点 → 发邮件）

用法：
    python run_daemon.py                # 前台运行
    python run_daemon.py --once daily   # 只跑一次指定任务（调试用）
    python run_daemon.py --status       # 打印任务状态

启动后 Ctrl+C 优雅退出。streamlit 关闭不影响本进程。
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from smcore.scheduler import Scheduler
from smcore.scheduler.jobs import job_daily_pick, job_intraday_alert, job_refresh_quotes


def _setup_logging() -> None:
    """配置日志：控制台 + 文件。"""
    log_dir = REPO_ROOT / "stock_data" / "auto_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"daemon-{time.strftime('%Y%m%d')}.log"

    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )


def build_scheduler() -> Scheduler:
    """构建调度器，注册所有定时任务。"""
    sched = Scheduler(check_interval_seconds=20)

    # 1. 每日选股推送（工作日 21:30，新闻联播后）
    sched.weekday("21:30", job_daily_pick, name="每日选股推送")

    # 2. 行情快照刷新（盘中每 5 分钟）
    sched.interval(5, job_refresh_quotes, name="行情快照刷新", trading_hours_only=True)

    # 3. 盘中预警（盘中每 10 分钟）
    sched.interval(10, job_intraday_alert, name="盘中预警", trading_hours_only=True)

    return sched


def run_once(job_name: str) -> None:
    """只跑一次指定任务（调试用）。"""
    jobs = {
        "daily": ("每日选股推送", job_daily_pick),
        "quotes": ("行情快照刷新", job_refresh_quotes),
        "alert": ("盘中预警", job_intraday_alert),
    }
    if job_name not in jobs:
        print(f"未知任务: {job_name}，可选: {list(jobs.keys())}")
        sys.exit(1)

    label, func = jobs[job_name]
    print(f"单次执行: {label}")
    func()
    print("完成")


def print_status() -> None:
    """打印调度器状态（供外部查询）。"""
    import json

    sched = build_scheduler()
    status = sched.status()
    print(json.dumps(status, ensure_ascii=False, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Stocks-Master 后台守护进程")
    parser.add_argument("--once", help="只跑一次指定任务 (daily/quotes/alert)", default=None)
    parser.add_argument("--status", action="store_true", help="打印任务状态后退出")
    args = parser.parse_args()

    _setup_logging()
    logger = logging.getLogger("daemon")

    if args.status:
        print_status()
        return

    if args.once:
        run_once(args.once)
        return

    logger.info("=" * 50)
    logger.info("Stocks-Master 守护进程启动")
    logger.info("工作目录: %s", REPO_ROOT)
    logger.info("Python: %s", sys.executable)
    logger.info("=" * 50)

    sched = build_scheduler()
    sched.run_forever()

    logger.info("守护进程已退出")


if __name__ == "__main__":
    main()
