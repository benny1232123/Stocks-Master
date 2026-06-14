"""持仓查看页面 — 显示当前持仓、浮动盈亏和已实现盈亏。"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from core.position_calculator import compute_positions, closed_trades_to_dataframe
from core.trade_manager import TradeManager


def render_position_view() -> None:
    st.title("💼 持仓总览")

    tm = TradeManager()
    trades_df = tm.get_trades_for_fifo()

    if trades_df.empty:
        st.info("暂无交易记录，请先在「交易录入」页添加交易。")
        return

    open_positions, closed_trades = compute_positions(trades_df)

    if not open_positions:
        st.info("当前无持仓。所有买入均已平仓。")
    else:
        _render_summary_cards(open_positions, closed_trades)
        st.divider()
        _render_positions_table(open_positions, tm)
        st.divider()
        _render_position_details(open_positions)

    # 已实现盈亏
    if closed_trades:
        st.divider()
        _render_closed_summary(closed_trades)


def _render_summary_cards(open_positions, closed_trades) -> None:
    """汇总卡片：持仓数量、总成本、已实现盈亏。"""
    total_cost = sum(p.total_cost for p in open_positions.values())
    total_realized = sum(p.closed_pnl for p in open_positions.values())
    position_count = len(open_positions)

    col1, col2, col3 = st.columns(3)
    col1.metric("持仓只数", f"{position_count}")
    col2.metric("总投入成本", f"¥{total_cost:,.2f}")
    col3.metric(
        "已实现盈亏",
        f"¥{total_realized:,.2f}",
        delta=f"{total_realized:+,.2f}",
    )


def _render_positions_table(open_positions, tm: TradeManager) -> None:
    """持仓表格：股票代码、名称、数量、均价、成本。"""
    rows = []
    for code, pos in sorted(open_positions.items()):
        rows.append({
            "股票代码": code,
            "名称": _get_stock_name(tm, code),
            "持仓数量": f"{pos.total_qty:,.0f}",
            "均价": f"{pos.avg_cost:,.3f}",
            "总成本": f"¥{pos.total_cost:,.2f}",
            "手续费": f"¥{pos.total_fee:,.2f}",
            "已实现盈亏": f"¥{pos.closed_pnl:+,.2f}",
            "批次数": len(pos.lots),
        })

    df = pd.DataFrame(rows)
    st.subheader("当前持仓")
    st.dataframe(df, use_container_width=True, hide_index=True)


def _render_position_details(open_positions) -> None:
    """展开每只持仓的 FIFO 批次明细。"""
    st.subheader("批次明细")
    for code, pos in sorted(open_positions.items()):
        if not pos.lots:
            continue
        with st.expander(f"{code}（{len(pos.lots)} 笔）", expanded=False):
            lot_df = pd.DataFrame(pos.lots)
            lot_df = lot_df.rename(columns={
                "buy_date": "买入日期",
                "buy_price": "买入价",
                "quantity": "数量",
                "fee": "手续费",
                "cost": "成本",
            })
            st.dataframe(lot_df, use_container_width=True, hide_index=True)


def _render_closed_summary(closed_trades) -> None:
    """已平仓交易汇总统计。"""
    st.subheader("已平仓统计")
    closed_df = closed_trades_to_dataframe(closed_trades)

    pnls = closed_df["单笔收益(元)"]
    returns = closed_df["单笔收益率(%)"]
    win_count = (pnls > 0).sum()
    total_count = len(pnls)
    win_rate = win_count / total_count * 100 if total_count > 0 else 0
    total_pnl = pnls.sum()
    avg_ret = returns.mean()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("平仓笔数", f"{total_count}")
    col2.metric("胜率", f"{win_rate:.1f}%")
    col3.metric("平均收益率", f"{avg_ret:+.2f}%")
    col4.metric("总已实现盈亏", f"¥{total_pnl:+,.2f}", delta=f"{total_pnl:+,.2f}")

    with st.expander("查看平仓明细", expanded=False):
        st.dataframe(closed_df, use_container_width=True, hide_index=True)


def _get_stock_name(tm: TradeManager, code: str) -> str:
    """从最近交易中获取股票名称。"""
    df = tm.get_trades(code=code, limit=1)
    if not df.empty and "stock_name" in df.columns:
        name = df.iloc[0].get("stock_name", "")
        if name and str(name) != "nan":
            return str(name)
    return ""
