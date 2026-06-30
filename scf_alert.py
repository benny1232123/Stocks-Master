"""SCF 盘中预警入口 —— 腾讯云函数。

部署后在 SCF 定时触发器运行（盘中每 10 分钟），不依赖本地电脑。
从 COS 读操作清单 → 拉新浪行情 → 对比止损止盈 → 发邮件。

本地测试：python scf_alert.py
SCF 入口：main_handler(event, context)
"""
from __future__ import annotations

import csv
import logging
import sys
import tempfile
from datetime import date
from pathlib import Path

# SCF 入口需要能 import smcore
# 部署时 smcore 会和本文件一起打包
_REPO = Path(__file__).resolve().parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from smcore.data.quote_sina import fetch_sina_quotes
from smcore.notify import send_email
from smcore.storage import download_latest
from smcore.utils.code import format_stock_code

logger = logging.getLogger("scf_alert")


def _to_float(val):
    try:
        v = float(val)
        return v if v == v else None
    except (TypeError, ValueError):
        return None


def check_alerts(action_list_path: Path) -> list[str]:
    """读取操作清单，对比实时价，返回触发的预警消息列表。"""
    alerts_meta = []
    with action_list_path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            code = format_stock_code(row.get("股票代码", ""))
            if not code:
                continue
            alerts_meta.append({
                "code": code,
                "name": row.get("股票名称", ""),
                "stop_loss": _to_float(row.get("止损价(下轨)")),
                "take_profit": _to_float(row.get("止盈价(上轨)")),
                "buy_price": _to_float(row.get("建议买入价")),
                "latest": _to_float(row.get("最新价")),
            })

    if not alerts_meta:
        return []

    # 批量拉新浪行情
    codes = [a["code"] for a in alerts_meta]
    quotes = fetch_sina_quotes(codes)

    triggered = []
    for a in alerts_meta:
        info = quotes.get(a["code"])
        if not info or info.get("price") is None:
            continue
        price = info["price"]
        name = a["name"] or info.get("name", "")
        sl = a["stop_loss"]
        tp = a["take_profit"]
        buy = a["buy_price"]

        if sl and price <= sl:
            triggered.append(f"⚠️ 止损: {name}({a['code']}) 现价{price:.2f} ≤ 下轨{sl:.2f}")
        elif sl and price <= sl * 1.02:
            triggered.append(f"🔔 接近止损: {name}({a['code']}) 现价{price:.2f} 接近下轨{sl:.2f}")
        elif tp and price >= tp:
            triggered.append(f"✅ 止盈: {name}({a['code']}) 现价{price:.2f} ≥ 上轨{tp:.2f}")
        elif tp and price >= tp * 0.98:
            triggered.append(f"🎯 接近止盈: {name}({a['code']}) 现价{price:.2f} 接近上轨{tp:.2f}")
        elif buy and price <= buy * 1.01:
            triggered.append(f"📍 接近买点: {name}({a['code']}) 现价{price:.2f} ≈ 建议买入{buy:.2f}")

    return triggered


def main_handler(event: dict, context: object) -> dict:
    """SCF 入口：从 COS 读操作清单，检查预警，触发推送。"""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger.info("SCF 预警任务启动")

    # 1. 从 COS 下载最新操作清单
    tmp_dir = Path(tempfile.gettempdir()) / "scf_alert"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    action_list = download_latest("Daily-Action-List-", tmp_dir)
    if action_list is None or not action_list.exists():
        logger.warning("COS 无操作清单，跳过预警")
        return {"code": 0, "message": "无操作清单", "triggered": 0}

    logger.info("已下载操作清单: %s", action_list.name)

    # 2. 检查预警
    triggered = check_alerts(action_list)

    if not triggered:
        logger.info("无预警触发")
        return {"code": 0, "message": "无触发", "triggered": 0}

    logger.info("触发 %d 条预警", len(triggered))

    # 3. 发邮件推送
    today_str = date.today().strftime("%Y-%m-%d")
    content = f"## 盘中预警（{today_str}）\n\n" + "\n".join(triggered)
    logs: list[str] = []
    ok = send_email(
        subject=f"盘中预警 {today_str}",
        content=content,
        csv_path=str(action_list),
        log_lines=logs,
    )
    if ok:
        logger.info("已推送邮件")
    else:
        logger.warning("推送失败: %s", logs)

    return {"code": 0, "message": "完成", "triggered": len(triggered)}


if __name__ == "__main__":
    # 本地测试：用本地操作清单（不走 COS）
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    from smcore.config.defaults import STOCK_DATA_DIR

    candidates = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"), reverse=True)
    if not candidates:
        print("无操作清单，先跑 fusion 生成")
        sys.exit(1)

    print(f"用本地清单测试: {candidates[0].name}")
    triggered = check_alerts(candidates[0])
    if triggered:
        print(f"触发 {len(triggered)} 条预警:")
        for t in triggered:
            print(" ", t)
    else:
        print("无预警触发")
