"""daemon 定时任务定义。

每个任务是一个无参函数，由 Scheduler 调度执行。
任务内部异常会被 Scheduler 捕获隔离，不影响其他任务。
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

logger = logging.getLogger("smcore.daemon")

REPO_ROOT = Path(__file__).resolve().parent
ANB_SCRIPT = REPO_ROOT / "Frequently-Used-Program" / "auto_notify_boll.py"


def job_daily_pick() -> None:
    """每日选股 + 推送 + 上传操作清单到 COS（供 SCF 预警用）。

    子进程方式好处：巨石崩了不影响 daemon；stdout 实时可见。
    """
    logger.info("启动每日选股子进程: %s", ANB_SCRIPT)
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    result = subprocess.run(
        [sys.executable, "-u", str(ANB_SCRIPT)],
        cwd=str(REPO_ROOT),
        env=env,
        timeout=3600,  # 1 小时上限
        capture_output=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"选股子进程退出码 {result.returncode}")

    # 选股完成后，生成操作清单（含 Boll 止损止盈水位）并上传 COS
    try:
        from smcore.strategy import fuse_signals, save_action_list

        today = date.today().strftime("%Y%m%d")
        df, _ = fuse_signals(today, total_capital=100000, max_picks=15, fetch_levels=True)
        if not df.empty:
            path = save_action_list(df, today)
            if path:
                logger.info("操作清单已生成: %s", path)
                # 上传 COS（未配置 COS 则跳过，不影响本地流程）
                from smcore.storage import upload_file

                remote_key = f"Daily-Action-List-{today}.csv"
                if upload_file(path, remote_key):
                    logger.info("操作清单已上传 COS: %s", remote_key)
    except Exception as e:
        logger.exception("生成/上传操作清单失败: %s", e)


