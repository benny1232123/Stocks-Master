"""持仓快照工具：只知道「现在持有多少股」也能补录交易。

背景
----
真实用户常常只记得当前持仓数量，忘了买入日期/成本价。本模块提供：

- :func:`last_close_from_kdata` —— 从本地 ``stock_data/k_data/{code}_qfq_full.csv``
  读最新收盘价（**离线可用**，沙箱/无网络时最稳）。
- :func:`resolve_placeholder_price` —— 先本地 K 线，再回退实时行情。
- :func:`build_snapshot_trades` —— 把「代码 + 数量」补全成标准 trade 记录。

为什么可以占位
--------------
每日持仓个股分析报告（`scripts/notify_holdings_analysis.py`）只按**代码**
出技术面（Boll/MA/RSI/MACD/KDJ）与基本面，**不含成本价与盈亏**。
因此成本价/日期占位**不影响该日报内容**；它们只影响「持仓盈亏」展示页。
"""
from __future__ import annotations

import csv
from datetime import date as _date
from pathlib import Path
from typing import Any

from smcore.config.defaults import STOCK_DATA_DIR
from smcore.utils.code import format_stock_code

KDATA_DIR = STOCK_DATA_DIR / "k_data"

#: 写入 notes 的占位标记，便于日后识别与校正
PLACEHOLDER_NOTE = "【快照导入·成本价/日期占位】"


def last_close_from_kdata(code: str) -> tuple[float | None, str | None]:
    """从本地 K 线 CSV 读最新收盘价。

    返回 ``(收盘价, 该收盘价对应日期)``；读不到则 ``(None, None)``。
    """
    path = KDATA_DIR / f"{code}_qfq_full.csv"
    if not path.exists():
        return None, None
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            rows = [r for r in csv.reader(fh) if r and any(c.strip() for c in r)]
        if len(rows) < 2:
            return None, None
        header = [c.strip().lstrip("\ufeff") for c in rows[0]]
        if "close" not in header:
            return None, None
        ci = header.index("close")
        # 从末尾往回找第一条能解析出价格的记录（跳过可能的空行/坏行）
        for row in reversed(rows[1:]):
            if len(row) <= ci:
                continue
            try:
                return float(row[ci]), str(row[0]).strip()
            except (TypeError, ValueError):
                continue
    except Exception:
        return None, None
    return None, None


def resolve_placeholder_price(code: str) -> tuple[float | None, str]:
    """解析占位成本价，返回 ``(价格, 来源说明)``。

    优先本地 K 线（离线最稳），失败再回退 :func:`smcore.data.quote.fetch_realtime_price`。
    两者都拿不到时返回 ``(None, "")``，调用方应要求用户手填。
    """
    price, kdate = last_close_from_kdata(code)
    if price is not None:
        return price, f"本地k_data {kdate or ''}收盘".strip()
    try:
        from smcore.data.quote import fetch_realtime_price

        rt = fetch_realtime_price(code)
        if rt:
            return float(rt), "实时行情"
    except Exception:
        pass
    return None, ""


def build_snapshot_trades(
    items: list[dict[str, Any]],
    default_date: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """把「代码 + 数量」补成标准 trade 记录。

    参数
    ----
    items
        每项至少含 ``code`` 与 ``qty``；可选 ``name`` / ``date`` / ``price`` / ``notes``。
    default_date
        ``date`` 缺省时使用（默认今天，格式 ``YYYY-MM-DD``）。

    返回
    ----
    ``(trades, skipped)`` —— trades 可直接写入 DB；skipped 记录被跳过的行及原因。
    """
    today = (default_date or "").strip() or _date.today().isoformat()
    trades: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for idx, item in enumerate(items, start=1):
        raw_code = str(item.get("code", "")).strip()
        code = format_stock_code(raw_code)
        if not code:
            skipped.append({"row": idx, "code": raw_code, "reason": "股票代码无效"})
            continue

        try:
            qty = float(str(item.get("qty", "")).strip())
        except (TypeError, ValueError):
            skipped.append({"row": idx, "code": code, "reason": f"数量无效: {item.get('qty')!r}"})
            continue
        if qty <= 0:
            skipped.append({"row": idx, "code": code, "reason": f"数量必须 > 0: {qty}"})
            continue

        trade_date = str(item.get("date", "")).strip() or today
        notes = str(item.get("notes", "")).strip()
        name = str(item.get("name", "")).strip() or code

        raw_price = str(item.get("price", "")).strip()
        if raw_price:
            try:
                price = float(raw_price)
            except (TypeError, ValueError):
                skipped.append({"row": idx, "code": code, "reason": f"价格无效: {raw_price!r}"})
                continue
            if price <= 0:
                skipped.append({"row": idx, "code": code, "reason": f"价格必须 > 0: {price}"})
                continue
        else:
            price, src = resolve_placeholder_price(code)
            if price is None:
                skipped.append(
                    {
                        "row": idx,
                        "code": code,
                        "reason": "无法自动获取价格（本地无 K 线且实时行情不可用），请手填成本价",
                    }
                )
                continue
            notes = f"{notes} {PLACEHOLDER_NOTE}成本价按{src}占位".strip()

        trades.append(
            {
                "trade_date": trade_date,
                "code": code,
                "name": name,
                "side": "BUY",
                "price": price,
                "quantity": qty,
                "fee": 0.0,
                "notes": notes,
            }
        )

    return trades, skipped
