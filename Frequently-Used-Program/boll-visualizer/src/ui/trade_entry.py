"""交易录入页面 — 移动端友好的交易录入表单。"""

from __future__ import annotations

from datetime import date

import streamlit as st

from core.trade_manager import TradeManager


def render_trade_entry() -> None:
    st.title("📝 交易录入")

    # Streamlit Cloud 数据持久化提示
    import os
    if os.environ.get("STREAMLIT_SHARING_MODE") or os.environ.get("STREAMLIT_CLOUD"):
        st.info(
            "💡 当前运行在 Streamlit Cloud，交易数据在应用重新部署时会重置。"
            "建议定期在「交易历史」页导出 CSV 备份。",
            icon="ℹ️",
        )

    tm = TradeManager()

    # ── 手动录入表单 ──────────────────────────────────────────

    with st.form("trade_form", clear_on_submit=True):
        st.subheader("手动录入")

        col1, col2 = st.columns(2)
        with col1:
            trade_date = st.date_input("交易日期", value=date.today(), key="te_date")
        with col2:
            side = st.selectbox("买卖方向", ["买入", "卖出"], key="te_side")

        col3, col4 = st.columns(2)
        with col3:
            code = st.text_input(
                "股票代码",
                placeholder="例如 600519",
                key="te_code",
            )
        with col4:
            name = st.text_input(
                "股票名称（选填）",
                placeholder="例如 贵州茅台",
                key="te_name",
            )

        col5, col6 = st.columns(2)
        with col5:
            price = st.number_input(
                "成交价格",
                min_value=0.01,
                step=0.01,
                format="%.3f",
                key="te_price",
            )
        with col6:
            quantity = st.number_input(
                "成交数量（股）",
                min_value=1,
                step=100,
                key="te_qty",
            )

        col7, col8 = st.columns(2)
        with col7:
            fee = st.number_input(
                "手续费",
                min_value=0.0,
                step=0.01,
                format="%.2f",
                value=0.0,
                key="te_fee",
            )
        with col8:
            notes = st.text_input("备注（选填）", key="te_notes")

        submitted = st.form_submit_button(
            "✅ 录入交易",
            type="primary",
            use_container_width=True,
        )

        if submitted:
            code_clean = code.strip()
            if not code_clean:
                st.error("请输入股票代码")
            elif price <= 0:
                st.error("请输入有效的成交价格")
            elif quantity <= 0:
                st.error("请输入有效的成交数量")
            else:
                side_en = "BUY" if side == "买入" else "SELL"
                try:
                    row_id = tm.add_trade(
                        trade_date=trade_date,
                        code=code_clean,
                        side=side_en,
                        price=price,
                        quantity=int(quantity),
                        fee=fee,
                        name=name.strip(),
                        notes=notes.strip(),
                    )
                    st.success(f"✅ 已录入：{code_clean} {side} {int(quantity)}股 @ {price:.3f}（#{row_id}）")
                    st.rerun()
                except Exception as e:
                    st.error(f"录入失败：{e}")

    st.divider()

    # ── CSV 批量导入 ──────────────────────────────────────────

    with st.expander("📂 CSV 批量导入", expanded=False):
        st.caption(
            "支持列名：日期/成交日期, 股票代码, 方向/买卖, 成交价, 数量, 手续费。"
            "兼容 backtest_tradebook.py 的 CSV 格式。"
        )
        uploaded = st.file_uploader(
            "上传交易 CSV",
            type=["csv"],
            key="te_csv_upload",
        )
        if uploaded is not None:
            if st.button("导入", key="te_csv_import_btn", use_container_width=True):
                try:
                    csv_bytes = uploaded.getvalue()
                    imported, skipped = tm.import_csv(csv_bytes)
                    st.success(f"导入完成：成功 {imported} 条，跳过 {skipped} 条")
                    st.rerun()
                except Exception as e:
                    st.error(f"导入失败：{e}")

    st.divider()

    # ── 最近录入 ──────────────────────────────────────────────

    st.subheader("最近录入")
    recent = tm.get_trades(limit=20)
    if recent.empty:
        st.info("暂无交易记录，请录入第一笔交易。")
    else:
        display_df = recent[["date", "code", "stock_name", "side", "price", "quantity", "fee", "notes"]].copy()
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
        st.dataframe(display_df, use_container_width=True, hide_index=True)
