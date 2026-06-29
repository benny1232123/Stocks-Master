"""daemon 定时任务定义。

每个任务是一个无参函数，由 Scheduler 调度执行。
任务内部异常会被 Scheduler 捕获隔离，不影响其他任务。
"""
from __future__ import annotations

import csv
import logging
import os
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

from smcore.config.defaults import STOCK_DATA_DIR
from smcore.data.quote import _load_full_snapshot, clear_quote_cache, fetch_realtime_quotes
from smcore.notify import send_wecom_markdown
from smcore.utils.code import format_stock_code

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


def job_refresh_quotes() -> None:
    """刷新实时行情快照缓存（盘中每 5 分钟）。

    清内存+磁盘缓存后重新拉全量，保证后续预警用最新价。
    """
    clear_quote_cache()
    df = _load_full_snapshot()
    logger.info("行情快照已刷新: %d 只股票", len(df))


def job_intraday_alert() -> None:
    """盘中预警：监控操作清单候选股，触止损/止盈推企微。

    读取最新的 Daily-Action-List-*.csv，对比实时价与止损/止盈位。
    需要 WECOM_WEBHOOK_URL 环境变量，未配置则只记日志不推送。
    """
    webhook = os.getenv("WECOM_WEBHOOK_URL", "").strip()
    if not webhook:
        logger.info("未配置 WECOM_WEBHOOK_URL，跳过推送（仅记日志）")

    # 找最新操作清单
    today = date.today().strftime("%Y%m%d")
    candidates = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"), reverse=True)
    if not candidates:
        logger.info("无操作清单，跳过预警")
        return

    csv_path = candidates[0]
    alerts = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = format_stock_code(row.get("股票代码", ""))
            name = row.get("股票名称", "")
            stop_loss = _to_float(row.get("止损价(下轨)"))
            take_profit = _to_float(row.get("止盈价(上轨)"))
            buy_price = _to_float(row.get("建议买入价"))

            if not code:
                continue
            # 没有水位就跳过（fusion fetch_levels=False 时）
            if stop_loss is None and take_profit is None:
                continue

            alerts.append({
                "code": code,
                "name": name,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "buy_price": buy_price,
            })

    if not alerts:
        logger.info("操作清单无水位数据，跳过预警")
        return

    # 拉实时价（从缓存，5分钟刷新一次）
    codes = [a["code"] for a in alerts]
    quotes = fetch_realtime_quotes(codes)
    quote_map = {row["code"]: float(row["price"]) for _, row in quotes.iterrows()}

    triggered = []
    for a in alerts:
        price = quote_map.get(a["code"])
        if price is None:
            continue

        sl = a["stop_loss"]
        tp = a["take_profit"]
        buy = a["buy_price"]

        if sl and price <= sl:
            triggered.append(f"⚠️ 止损: {a['name']}({a['code']}) 现价{price:.2f} ≤ 下轨{sl:.2f}")
        elif sl and price <= sl * 1.02:
            triggered.append(f"🔔 接近止损: {a['name']}({a['code']}) 现价{price:.2f} 接近下轨{sl:.2f}")
        elif tp and price >= tp:
            triggered.append(f"✅ 止盈: {a['name']}({a['code']}) 现价{price:.2f} ≥ 上轨{tp:.2f}")
        elif tp and price >= tp * 0.98:
            triggered.append(f"🎯 接近止盈: {a['name']}({a['code']}) 现价{price:.2f} 接近上轨{tp:.2f}")
        elif buy and price <= buy * 1.01:
            triggered.append(f"📍 接近买点: {a['name']}({a['code']}) 现价{price:.2f} ≈ 建议买入{buy:.2f}")

    if not triggered:
        logger.info("盘中预警检查完成: %d 只监控，无触发", len(alerts))
        return

    logger.info("盘中预警触发 %d 条", len(triggered))

    # 推送企微
    if webhook:
        content = "## 盘中预警\n" + "\n".join(triggered)
        logs = []
        ok = send_wecom_markdown(webhook, content, log_lines=logs)
        if ok:
            logger.info("预警已推送企微")
        else:
            logger.warning("预警推送失败: %s", logs)


def _to_float(val):
    try:
        v = float(val)
        return v if v == v else None  # NaN 检查
    except (TypeError, ValueError):
        return None
