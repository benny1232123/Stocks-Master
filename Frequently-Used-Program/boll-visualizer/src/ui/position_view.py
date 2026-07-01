"""持仓查看页面 — 显示当前持仓、浮动盈亏和已实现盈亏。

依赖 core.position_calculator 和 core.trade_manager，
这两个模块只读写本地 CSV，不依赖任何云端服务。
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

# 使用本地的 TradeManager（不依赖 supabase）
# TradeManager 在没有 supabase 时用本地 CSV 存储
from core.trade_manager import TradeManager
from core.position_calculator import compute_positions, closed_trades_to_dataframe


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
