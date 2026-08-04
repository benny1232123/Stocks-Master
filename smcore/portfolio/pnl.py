"""持仓盈亏计算 —— 联动实时行情。

此前持仓页只展示录入的买卖记录，不会自动算浮盈亏。本模块读取持仓数据、
拉实时报价、计算每只票的浮盈亏/胜率/总市值。

配合 visualizer 的 position_view 使用，也可独立调用。
"""
from __future__ import annotations

from typing import Optional

import pandas as pd

from smcore.data.quote import fetch_realtime_quotes
from smcore.utils.code import format_stock_code


def compute_position_pnl(positions: list[dict]) -> pd.DataFrame:
    """计算持仓浮盈亏。

    Args:
        positions: 持仓列表，每个 dict 需含:
            - code: 股票代码
            - name: 股票名称（可选）
            - quantity: 持仓数量
            - cost_price: 成本价（买入均价）

    Returns:
        DataFrame[code, name, quantity, cost_price, current_price, market_value,
                  cost_value, pnl, pnl_pct, pct(今日涨跌幅)]
    """
    if not positions:
        return pd.DataFrame(columns=[
            "code", "name", "quantity", "cost_price", "current_price",
            "market_value", "cost_value", "pnl", "pnl_pct", "pct"
        ])

    codes = [p.get("code", "") for p in positions]
    quotes = fetch_realtime_quotes(codes)
    quote_map = {row["code"]: row for row in quotes.to_dict("records")}

    rows = []
    for pos in positions:
        code = format_stock_code(pos.get("code", ""))
        name = pos.get("name", "")
        quantity = float(pos.get("quantity", 0) or 0)
        cost_price = float(pos.get("cost_price", 0) or 0)

        q = quote_map.get(code, {})
        current_price = q.get("price")
        today_pct = q.get("pct")
        if not name and q.get("name"):
            name = q["name"]

        if current_price is None or quantity <= 0:
            rows.append({
                "code": code, "name": name, "quantity": quantity,
                "cost_price": cost_price, "current_price": None,
                "market_value": None, "cost_value": cost_price * quantity if cost_price else None,
                "pnl": None, "pnl_pct": None, "pct": today_pct,
            })
            continue

        market_value = current_price * quantity
        cost_value = cost_price * quantity if cost_price else None
        pnl = (current_price - cost_price) * quantity if cost_price else None
        pnl_pct = ((current_price - cost_price) / cost_price * 100) if cost_price else None

        rows.append({
            "code": code, "name": name, "quantity": quantity,
            "cost_price": cost_price, "current_price": current_price,
            "market_value": market_value, "cost_value": cost_value,
            "pnl": pnl, "pnl_pct": pnl_pct, "pct": today_pct,
        })

    return pd.DataFrame(rows)


def summarize_portfolio(pnl_df: pd.DataFrame) -> dict:
    """汇总持仓组合统计。

    Returns:
        {total_market_value, total_cost, total_pnl, total_pnl_pct,
         winners, losers, win_rate}
    """
    if pnl_df.empty:
        return {
            "total_market_value": 0, "total_cost": 0,
            "total_pnl": 0, "total_pnl_pct": 0,
            "winners": 0, "losers": 0, "win_rate": 0,
        }

    valid = pnl_df.dropna(subset=["pnl"])
    total_mv = valid["market_value"].sum() if "market_value" in valid else 0
    total_cost = valid["cost_value"].sum() if "cost_value" in valid else 0
    total_pnl = valid["pnl"].sum() if "pnl" in valid else 0
    total_pnl_pct = (total_pnl / total_cost * 100) if total_cost else 0

    winners = int((valid["pnl"] > 0).sum()) if "pnl" in valid else 0
    losers = int((valid["pnl"] < 0).sum()) if "pnl" in valid else 0
    total_positions = winners + losers
    win_rate = (winners / total_positions * 100) if total_positions else 0

    return {
        "total_market_value": float(total_mv),
        "total_cost": float(total_cost),
        "total_pnl": float(total_pnl),
        "total_pnl_pct": float(total_pnl_pct),
        "winners": winners,
        "losers": losers,
        "win_rate": float(win_rate),
    }
