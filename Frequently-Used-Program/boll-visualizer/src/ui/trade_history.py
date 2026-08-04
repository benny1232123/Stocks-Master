"""交易历史页面 — 查看、筛选、导出和删除交易记录。"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import streamlit as st

from core.position_calculator import compute_positions, closed_trades_to_dataframe
from core.trade_manager import TradeManager


def render_trade_history() -> None:
    st.title("📋 交易历史")

    tm = TradeManager()

    # ── 筛选条件 ──────────────────────────────────────────────

    col1, col2, col3 = st.columns(3)
    with col1:
        start_date = st.date_input(
            "开始日期",
            value=date.today() - timedelta(days=90),
            key="th_start",
        )
    with col2:
        end_date = st.date_input(
            "结束日期",
            value=date.today(),
            key="th_end",
        )
    with col3:
        all_codes = tm.get_all_codes()
        code_filter = st.selectbox(
            "股票代码",
            options=["全部", *all_codes],
            key="th_code",
        )

    # ── 查询 ──────────────────────────────────────────────────

    filter_code = code_filter if code_filter != "全部" else None
    trades_df = tm.get_trades(
        code=filter_code,
        start_date=start_date,
        end_date=end_date,
        limit=2000,
    )

    if trades_df.empty:
        st.info("该时间范围内无交易记录。")
        return

    # ── 统计摘要 ──────────────────────────────────────────────

    buy_count = (trades_df["side"] == "BUY").sum()
    sell_count = (trades_df["side"] == "SELL").sum()
    total_amount = (trades_df["price"] * trades_df["quantity"]).sum()
    total_fee = trades_df["fee"].sum()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("交易笔数", f"{len(trades_df)}")
    col2.metric("买入 / 卖出", f"{buy_count} / {sell_count}")
    col3.metric("总成交金额", f"¥{total_amount:,.2f}")
    col4.metric("总手续费", f"¥{total_fee:,.2f}")

    st.divider()

    # ── 交易明细表 ────────────────────────────────────────────

    display_df = trades_df[["date", "code", "stock_name", "side", "price", "quantity", "fee", "notes"]].copy()
    display_df = display_df.rename(columns={
        "date": "日期",
        "code": "代码",
        "stock_name": "名称",
        "side": "方向",
        "price": "价格",
        "quantity": "数量",
        "fee": "手续费",
        "notes": "备注",
    })
    display_df["方向"] = display_df["方向"].map({"BUY": "买入", "SELL": "卖出"})
    display_df["金额"] = trades_df["price"] * trades_df["quantity"]
    display_df["金额"] = display_df["金额"].map(lambda x: f"¥{x:,.2f}")

    st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)

    st.divider()

    # ── 操作按钮 ──────────────────────────────────────────────

    col1, col2 = st.columns(2)

    with col1:
        # CSV 导出
        csv_bytes = tm.export_csv(codes=[filter_code] if filter_code else None)
        st.download_button(
            "📥 导出 CSV",
            data=csv_bytes,
            file_name=f"trades_{date.today().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    with col2:
        # 删除记录
        if "th_delete_id" not in st.session_state:
            st.session_state["th_delete_id"] = None

        with st.expander("⚠️ 删除记录", expanded=False):
            trade_ids = trades_df["id"].tolist() if "id" in trades_df.columns else []
            if not trade_ids:
                st.caption("无可删除的记录")
            else:
                id_options = [
                    f"#{row['id']} {row['date']} {row['code']} "
                    f"{'买入' if row['side'] == 'BUY' else '卖出'} "
                    f"{row['quantity']}股@{row['price']:.3f}"
                    for _, row in trades_df.iterrows()
                ]
                selected = st.selectbox("选择要删除的记录", options=id_options, key="th_delete_select")
                if selected and st.button("确认删除", type="secondary", use_container_width=True, key="th_delete_btn"):
                    # 提取 id
                    del_id = int(selected.split(" ")[0].lstrip("#"))
                    ok = tm.delete_trade(del_id)
                    if ok:
                        st.success(f"已删除记录 #{del_id}")
                        st.rerun()
                    else:
                        st.error("删除失败")

    # ── 已平仓盈亏统计 ────────────────────────────────────────

    fifo_trades = tm.get_trades_for_fifo()
    if not fifo_trades.empty:
        _, closed = compute_positions(fifo_trades)
        if closed:
            st.divider()
            st.subheader("已平仓盈亏")
            closed_df = closed_trades_to_dataframe(closed)
            pnls = closed_df["单笔收益(元)"]
            win = (pnls > 0).sum()
            total = len(pnls)
            st.caption(
                f"共 {total} 笔平仓 | 胜率 {win/total*100:.1f}% | "
                f"总盈亏 ¥{pnls.sum():+,.2f}"
            )
            st.dataframe(closed_df, use_container_width=True, hide_index=True, height=300)
