"""FIFO 持仓计算器 — 从交易记录计算当前持仓和已实现盈亏。

复用 backtest_tradebook.py 的 FIFO 匹配逻辑，增加持仓汇总功能。
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field

import pandas as pd


@dataclass
class OpenLot:
    """一笔尚未完全平仓的买入批次。"""
    code: str
    buy_date: str
    buy_price: float
    quantity: float
    fee: float


@dataclass
class PositionSummary:
    """单只股票的持仓汇总。"""
    code: str
    total_qty: float
    avg_cost: float
    total_cost: float
    total_fee: float
    lots: list[dict]          # 各批次明细
    closed_pnl: float = 0.0   # 已实现盈亏


@dataclass
class ClosedTrade:
    """一笔已平仓交易记录。"""
    code: str
    buy_date: str
    sell_date: str
    buy_price: float
    sell_price: float
    quantity: float
    hold_days: int
    pnl: float
    ret_pct: float
    buy_fee: float
    sell_fee: float


def compute_positions(trades_df: pd.DataFrame) -> tuple[dict[str, PositionSummary], list[ClosedTrade]]:
    """
    对交易流水执行 FIFO 匹配，返回：
    - open_positions: {code: PositionSummary} 当前持仓
    - closed_trades: 已平仓交易列表
    """
    if trades_df.empty:
        return {}, []

    lots: dict[str, deque[OpenLot]] = defaultdict(deque)
    closed: list[ClosedTrade] = []
    realized_pnl: dict[str, float] = defaultdict(float)

    # 确保按日期排序
    df = trades_df.sort_values(["date", "code"]).reset_index(drop=True)

    for row in df.itertuples(index=False):
        trade_date = str(row.date)
        code = str(row.code)
        side = str(row.side)
        price = float(row.price)
        qty = float(row.quantity)
        fee = float(row.fee)

        if side == "BUY":
            lots[code].append(
                OpenLot(code=code, buy_date=trade_date, buy_price=price, quantity=qty, fee=fee)
            )
            continue

        # SELL — FIFO 匹配
        remaining = qty
        while remaining > 1e-9 and lots[code]:
            lot = lots[code][0]
            matched_qty = min(remaining, lot.quantity)

            buy_dt = pd.to_datetime(lot.buy_date, errors="coerce")
            sell_dt = pd.to_datetime(trade_date, errors="coerce")
            hold_days = (sell_dt - buy_dt).days if (pd.notna(buy_dt) and pd.notna(sell_dt)) else 0

            buy_amount = lot.buy_price * matched_qty
            sell_amount = price * matched_qty
            buy_fee_alloc = lot.fee * (matched_qty / lot.quantity) if lot.quantity > 0 else 0.0
            sell_fee_alloc = fee * (matched_qty / qty) if qty > 0 else 0.0
            pnl = (sell_amount - buy_amount) - buy_fee_alloc - sell_fee_alloc
            ret_pct = (pnl / buy_amount * 100.0) if buy_amount > 0 else 0.0

            closed.append(ClosedTrade(
                code=code,
                buy_date=lot.buy_date,
                sell_date=trade_date,
                buy_price=round(lot.buy_price, 4),
                sell_price=round(price, 4),
                quantity=round(matched_qty, 4),
                hold_days=max(hold_days, 0),
                pnl=round(pnl, 2),
                ret_pct=round(ret_pct, 3),
                buy_fee=round(buy_fee_alloc, 2),
                sell_fee=round(sell_fee_alloc, 2),
            ))
            realized_pnl[code] += pnl

            remaining -= matched_qty
            lot.quantity -= matched_qty
            if lot.quantity <= 1e-9:
                lots[code].popleft()

    # 构建持仓汇总
    open_positions: dict[str, PositionSummary] = {}
    for code, lot_deque in lots.items():
        if not lot_deque:
            continue
        total_qty = sum(lot.quantity for lot in lot_deque)
        total_cost = sum(lot.buy_price * lot.quantity for lot in lot_deque)
        total_fee = sum(lot.fee for lot in lot_deque)
        avg_cost = total_cost / total_qty if total_qty > 0 else 0.0
        lot_details = [
            {
                "buy_date": lot.buy_date,
                "buy_price": lot.buy_price,
                "quantity": lot.quantity,
                "fee": lot.fee,
                "cost": round(lot.buy_price * lot.quantity, 2),
            }
            for lot in lot_deque
        ]
        open_positions[code] = PositionSummary(
            code=code,
            total_qty=round(total_qty, 4),
            avg_cost=round(avg_cost, 4),
            total_cost=round(total_cost, 2),
            total_fee=round(total_fee, 2),
            lots=lot_details,
            closed_pnl=round(realized_pnl.get(code, 0.0), 2),
        )

    return open_positions, closed


def closed_trades_to_dataframe(closed: list[ClosedTrade]) -> pd.DataFrame:
    """将已平仓交易列表转为 DataFrame（兼容 backtest_tradebook 输出格式）。"""
    if not closed:
        return pd.DataFrame(columns=[
            "股票代码", "买入日期", "卖出日期", "买入价", "卖出价",
            "数量", "持有天数", "单笔收益率(%)", "单笔收益(元)",
            "买入手续费", "卖出手续费",
        ])
    rows = [
        {
            "股票代码": t.code,
            "买入日期": t.buy_date,
            "卖出日期": t.sell_date,
            "买入价": t.buy_price,
            "卖出价": t.sell_price,
            "数量": t.quantity,
            "持有天数": t.hold_days,
            "单笔收益率(%)": t.ret_pct,
            "单笔收益(元)": t.pnl,
            "买入手续费": t.buy_fee,
            "卖出手续费": t.sell_fee,
        }
        for t in closed
    ]
    return pd.DataFrame(rows)
