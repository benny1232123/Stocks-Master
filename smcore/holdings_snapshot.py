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


def resolve_identifier(raw: str) -> tuple[str, str, str]:
    """把用户输入解析为 ``(code, name, error)``。

    ``raw`` 可以是 6 位代码（``600519``），也可以是股票名称（``贵州茅台`` / ``茅台``）。
    解析失败时 ``code`` 为空，``error`` 说明原因（歧义时会列出候选，方便用户改用代码）。
    """
    from smcore.stock_names import code_to_name, resolve as _resolve

    raw = str(raw or "").strip()
    if not raw:
        return "", "", "缺少股票代码或名称"

    has_cjk = any("\u4e00" <= ch <= "\u9fff" for ch in raw)
    has_alpha = any(ch.isalpha() for ch in raw)
    digits = "".join(ch for ch in raw if ch.isdigit())

    # 纯数字（含 600519.SH 这类带后缀）→ 直接当代码，不查索引
    if not has_cjk and not has_alpha and digits:
        code = format_stock_code(digits)
        if code:
            return code, (code_to_name(code) or code), ""
        return "", "", f"股票代码无效: {raw}"

    result = _resolve(raw)
    if result.get("ok"):
        return str(result["code"]), (str(result.get("name") or "") or str(result["code"])), ""

    cands = result.get("candidates") or []
    if cands:
        hint = "；".join(f"{c['code']} {c['name']}" for c in cands[:5])
        return "", "", f"名称有多个匹配，请改用代码（{hint}）"
    return "", "", f"无法识别的股票名称或代码: {raw}"


def build_snapshot_trades(
    items: list[dict[str, Any]],
    default_date: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """把「名称/代码 + 成本价 + 数量」补成标准 trade 记录。

    参数
    ----
    items
        每项需含 ``code``（**可以是 6 位代码，也可以是股票名称**）与 ``qty``；
        可选 ``price``（成本价，缺省则自动占位）、``name``、``date``、``notes``。
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
        raw_id = str(item.get("code") or item.get("name") or "").strip()
        code, resolved_name, err = resolve_identifier(raw_id)
        if not code:
            skipped.append({"row": idx, "code": raw_id, "reason": err or "股票代码或名称无效"})
            continue

        try:
            qty = float(str(item.get("qty", "")).strip())
        except (TypeError, ValueError):
            skipped.append({"row": idx, "code": raw_id, "reason": f"数量无效: {item.get('qty')!r}"})
            continue
        if qty <= 0:
            skipped.append({"row": idx, "code": raw_id, "reason": f"数量必须 > 0: {qty}"})
            continue

        trade_date = str(item.get("date", "")).strip() or today
        notes = str(item.get("notes", "")).strip()
        # 用户填的 name 优先 → 索引解析出的名称 → 代码
        name = str(item.get("name", "")).strip() or resolved_name or code

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
