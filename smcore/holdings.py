"""Shared helpers for trade history, FIFO holdings, and portfolio summaries."""
from __future__ import annotations

from typing import Any

import pandas as pd

from smcore.storage.trades_repo import get_trade_repository
from smcore.utils.code import format_stock_code


def trades_backend_name() -> str:
    """Return active trade storage backend name (json | supabase)."""
    return get_trade_repository().backend_name


def load_trades() -> list[dict[str, Any]]:
    """Load persisted trades from configured backend."""
    return get_trade_repository().load_all()


def save_trades(trades: list[dict[str, Any]]) -> None:
    """Replace all trades in configured backend."""
    get_trade_repository().replace_all(trades)


def validate_trade(trade: dict[str, Any], trades: list[dict[str, Any]]) -> str | None:
    """Validate a trade before persisting. Returns error message or None."""
    side = str(trade.get("side", "buy")).lower()
    if side != "sell":
        return None

    try:
        sell_qty = int(trade.get("qty", 0))
    except (TypeError, ValueError):
        return "卖出数量格式无效"
    if sell_qty <= 0:
        return "卖出数量必须大于 0"

    code = format_stock_code(str(trade.get("code", "")))
    if not code:
        return "股票代码无效"

    pos_df, _ = compute_fifo_positions(trades)
    if pos_df.empty:
        return f"无 {code} 持仓，无法卖出"

    held_rows = pos_df[pos_df["代码"].astype(str).map(format_stock_code) == code]
    if held_rows.empty:
        return f"无 {code} 持仓，无法卖出"

    held_qty = float(held_rows["数量"].sum())
    if sell_qty > held_qty:
        return f"卖出数量({sell_qty})超过持仓({int(held_qty)})"
    return None


def add_trade(trade: dict[str, Any]) -> list[dict[str, Any]]:
    """Append a trade and persist it."""
    trade = dict(trade)
    trade["code"] = format_stock_code(str(trade.get("code", ""))) or str(trade.get("code", "")).strip()
    trades = load_trades()
    error = validate_trade(trade, trades)
    if error:
        raise ValueError(error)
    get_trade_repository().append(trade)
    return load_trades()


def clear_trades() -> None:
    """Remove all persisted trades."""
    save_trades([])


def trades_to_df(trades: list[dict[str, Any]]) -> pd.DataFrame:
    """Normalize trades into a display-friendly DataFrame."""
    if not trades:
        return pd.DataFrame(columns=["日期", "代码", "名称", "方向", "价格", "数量", "手续费", "备注"])

    df = pd.DataFrame(trades)
    col_map = {
        "date": "日期",
        "code": "代码",
        "name": "名称",
        "side": "方向",
        "price": "价格",
        "qty": "数量",
        "fee": "手续费",
        "notes": "备注",
    }
    df = df.rename(columns=col_map)
    for column in ["日期", "代码", "名称", "方向", "价格", "数量", "手续费", "备注"]:
        if column not in df.columns:
            df[column] = ""
    return df[["日期", "代码", "名称", "方向", "价格", "数量", "手续费", "备注"]]


def compute_fifo_positions(trades: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute open positions and closed trades using FIFO matching."""
    if not trades:
        return pd.DataFrame(), pd.DataFrame()

    df = pd.DataFrame(trades)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    df["fee"] = pd.to_numeric(df["fee"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["price", "qty", "code"])

    positions: list[dict[str, Any]] = []
    closed_trades: list[dict[str, Any]] = []

    for code, group in df.groupby("code"):
        group = group.sort_values("date")
        buy_queue: list[dict[str, Any]] = []

        for _, row in group.iterrows():
            if row["side"] == "buy":
                buy_queue.append(
                    {
                        "date": row["date"],
                        "price": float(row["price"]),
                        "qty": float(row["qty"]),
                        "fee": float(row["fee"] or 0),
                    }
                )
                continue

            if row["side"] != "sell":
                continue

            sell_qty = float(row["qty"])
            sell_price = float(row["price"])
            sell_fee = float(row["fee"] or 0)

            while sell_qty > 0 and buy_queue:
                oldest = buy_queue[0]
                matched_qty = min(sell_qty, oldest["qty"])
                profit = (
                    (sell_price - oldest["price"]) * matched_qty
                    - sell_fee * (matched_qty / float(row["qty"]))
                    - oldest["fee"] * (matched_qty / oldest["qty"])
                )
                closed_trades.append(
                    {
                        "代码": code,
                        "买入日期": oldest["date"].strftime("%Y-%m-%d"),
                        "卖出日期": row["date"].strftime("%Y-%m-%d"),
                        "数量": matched_qty,
                        "买入价": oldest["price"],
                        "卖出价": sell_price,
                        "盈亏": round(profit, 2),
                        "收益率": round(profit / (oldest["price"] * matched_qty) * 100, 2),
                    }
                )
                sell_qty -= matched_qty
                oldest["qty"] -= matched_qty
                if oldest["qty"] <= 0:
                    buy_queue.pop(0)

        for remaining in buy_queue:
            if remaining["qty"] > 0:
                positions.append(
                    {
                        "代码": code,
                        "买入日期": remaining["date"].strftime("%Y-%m-%d"),
                        "数量": remaining["qty"],
                        "成本价": remaining["price"],
                        "成本金额": round(remaining["price"] * remaining["qty"], 2),
                    }
                )

    pos_df = pd.DataFrame(positions) if positions else pd.DataFrame()
    closed_df = pd.DataFrame(closed_trades) if closed_trades else pd.DataFrame()

    if not pos_df.empty:
        pos_df = pos_df.sort_values("买入日期", ascending=False)

    return pos_df, closed_df


def portfolio_snapshot() -> dict[str, Any]:
    """Return a JSON-friendly portfolio snapshot for the frontend."""
    repo = get_trade_repository()
    trades = load_trades()
    trades_df = trades_to_df(trades)
    pos_df, closed_df = compute_fifo_positions(trades)
    realtime_rows: list[dict[str, Any]] = []
    pnl_summary: dict[str, Any] = {
        "holding_cost": 0.0,
        "holding_value": 0.0,
        "total_pnl": 0.0,
    }

    if not pos_df.empty:
        try:
            from smcore.data.quote import fetch_realtime_quotes

            codes = pos_df["代码"].astype(str).tolist()
            quotes = fetch_realtime_quotes(codes)
            price_map = {
                str(row["code"]): float(row["price"])
                for _, row in quotes.iterrows()
                if pd.notna(row.get("price"))
            }
        except Exception:
            price_map = {}

        for _, row in pos_df.iterrows():
            code = str(row["代码"])
            cost = float(row["成本价"])
            qty = float(row["数量"])
            current_price = price_map.get(code)
            pnl = None
            pnl_pct = None
            if current_price is not None:
                pnl = (current_price - cost) * qty
                pnl_pct = (current_price / cost - 1) * 100 if cost else None
            realtime_rows.append(
                {
                    "代码": code,
                    "买入日期": row["买入日期"],
                    "数量": qty,
                    "成本价": cost,
                    "现价": current_price,
                    "浮动盈亏": round(pnl, 2) if pnl is not None else None,
                    "收益率%": round(pnl_pct, 2) if pnl_pct is not None else None,
                }
            )

        pnl_summary["holding_cost"] = float(pos_df["成本金额"].sum())
        pnl_summary["holding_value"] = float(
            sum((price_map.get(str(row["代码"]), float(row["成本价"]))) * float(row["数量"]) for _, row in pos_df.iterrows())
        )
        pnl_summary["total_pnl"] = pnl_summary["holding_value"] - pnl_summary["holding_cost"]

    snapshot: dict[str, Any] = {
        "storage_backend": repo.backend_name,
        "trades_count": len(trades),
        "trades_preview": trades_df.head(25).to_dict(orient="records"),
        "open_positions": [],
        "closed_trades": [],
        "realtime_positions": realtime_rows,
        "pnl_summary": pnl_summary,
    }

    if not pos_df.empty:
        snapshot["open_positions"] = pos_df.to_dict(orient="records")
    if not closed_df.empty:
        snapshot["closed_trades"] = closed_df.head(50).to_dict(orient="records")

    return snapshot
