"""持仓管理：交易录入 + 历史 + FIFO盈亏"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VIZ_SRC = ROOT / "Frequently-Used-Program" / "boll-visualizer" / "src"
for p in [str(ROOT), str(VIZ_SRC)]:
    if p not in sys.path:
        sys.path.insert(0, p)
os.environ.setdefault("KLINE_BACKEND", "akshare")

from auth import check_auth

check_auth()

import streamlit as st
import pandas as pd
import json
from datetime import date, datetime

st.set_page_config(page_title="持仓管理", page_icon="💼", layout="wide")

# ═══════════════════════════════════════════════
# 数据持久化（JSON 文件）
# ═══════════════════════════════════════════════

TRADES_FILE = ROOT / "stock_data" / "trades.json"


def _load_trades() -> list[dict]:
    if TRADES_FILE.exists():
        try:
            with open(TRADES_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []


def _save_trades(trades: list[dict]) -> None:
    TRADES_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(TRADES_FILE, "w", encoding="utf-8") as f:
        json.dump(trades, f, ensure_ascii=False, indent=2)


def _trades_to_df(trades: list[dict]) -> pd.DataFrame:
    if not trades:
        return pd.DataFrame(columns=["日期", "代码", "名称", "方向", "价格", "数量", "手续费", "备注"])
    df = pd.DataFrame(trades)
    # 统一列名
    col_map = {
        "date": "日期", "code": "代码", "name": "名称",
        "side": "方向", "price": "价格", "qty": "数量",
        "fee": "手续费", "notes": "备注",
    }
    df = df.rename(columns=col_map)
    for c in ["日期", "代码", "名称", "方向", "价格", "数量", "手续费", "备注"]:
        if c not in df.columns:
            df[c] = ""
    return df[["日期", "代码", "名称", "方向", "价格", "数量", "手续费", "备注"]]


def _compute_fifo_positions(trades: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """FIFO 持仓计算。"""
    if not trades:
        return pd.DataFrame(), pd.DataFrame()

    df = pd.DataFrame(trades)
    # 标准化
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    df["fee"] = pd.to_numeric(df["fee"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["price", "qty", "code"])

    # 按代码分组，FIFO 匹配
    positions = []
    closed_trades = []

    for code, group in df.groupby("code"):
        group = group.sort_values("date")
        buy_queue: list[dict] = []

        for _, row in group.iterrows():
            if row["side"] == "buy":
                buy_queue.append({
                    "date": row["date"],
                    "price": row["price"],
                    "qty": row["qty"],
                    "fee": row["fee"],
                })
            elif row["side"] == "sell":
                sell_qty = row["qty"]
                sell_price = row["price"]
                sell_fee = row["fee"]

                while sell_qty > 0 and buy_queue:
                    oldest = buy_queue[0]
                    matched_qty = min(sell_qty, oldest["qty"])
                    profit = (sell_price - oldest["price"]) * matched_qty - sell_fee * (matched_qty / row["qty"]) - oldest["fee"] * (matched_qty / oldest["qty"])
                    closed_trades.append({
                        "代码": code,
                        "买入日期": oldest["date"].strftime("%Y-%m-%d"),
                        "卖出日期": row["date"].strftime("%Y-%m-%d"),
                        "数量": matched_qty,
                        "买入价": oldest["price"],
                        "卖出价": sell_price,
                        "盈亏": round(profit, 2),
                        "收益率": round(profit / (oldest["price"] * matched_qty) * 100, 2),
                    })
                    sell_qty -= matched_qty
                    oldest["qty"] -= matched_qty
                    if oldest["qty"] <= 0:
                        buy_queue.pop(0)

        # 剩余持仓
        for remaining in buy_queue:
            if remaining["qty"] > 0:
                positions.append({
                    "代码": code,
                    "买入日期": remaining["date"].strftime("%Y-%m-%d"),
                    "数量": remaining["qty"],
                    "成本价": remaining["price"],
                    "成本金额": round(remaining["price"] * remaining["qty"], 2),
                })

    pos_df = pd.DataFrame(positions) if positions else pd.DataFrame()
    closed_df = pd.DataFrame(closed_trades) if closed_trades else pd.DataFrame()

    # 排序
    if not pos_df.empty:
        pos_df = pos_df.sort_values("买入日期", ascending=False)

    return pos_df, closed_df


# ═══════════════════════════════════════════════
# 页面渲染
# ═══════════════════════════════════════════════

st.title("💼 持仓管理")

tab1, tab2, tab3, tab4 = st.tabs(["📝 交易录入", "📋 交易历史", "📊 持仓盈亏", "📈 已平仓记录"])

trades = _load_trades()

# ── Tab 1：交易录入 ──
with tab1:
    st.subheader("录入新交易")

    col1, col2, col3 = st.columns(3)
    with col1:
        trade_date = st.date_input("日期", date.today())
        trade_code = st.text_input("股票代码", placeholder="000001")
    with col2:
        trade_side = st.selectbox("方向", ["buy", "sell"], format_func=lambda x: "买入" if x == "buy" else "卖出")
        trade_name = st.text_input("股票名称（可选）", placeholder="平安银行")
    with col3:
        trade_price = st.number_input("价格", 0.01, 9999.0, 10.0, step=0.01)
        trade_qty = st.number_input("数量（股）", 1, 999999, 100, step=100)

    trade_fee = st.number_input("手续费", 0.0, 9999.0, 0.0, step=0.01)
    trade_notes = st.text_input("备注（可选）", placeholder="例如：布林策略触发")

    if st.button("✅ 录入交易", type="primary"):
        if not trade_code.strip():
            st.error("请输入股票代码")
        else:
            new_trade = {
                "date": trade_date.strftime("%Y-%m-%d"),
                "code": trade_code.strip(),
                "name": trade_name.strip() or trade_code.strip(),
                "side": trade_side,
                "price": trade_price,
                "qty": trade_qty,
                "fee": trade_fee,
                "notes": trade_notes,
            }
            trades.append(new_trade)
            _save_trades(trades)
            st.success(f"已录入：{trade_side} {trade_code} {trade_price} x {trade_qty}股")
            st.rerun()

# ── Tab 2：交易历史 ──
with tab2:
    st.subheader("交易记录")

    trades_df = _trades_to_df(trades)
    if trades_df.empty:
        st.info("暂无交易记录，去「交易录入」添加吧")
    else:
        # 筛选
        codes = sorted(trades_df["代码"].unique())
        selected_codes = st.multiselect("按代码筛选（留空=全部）", codes)

        filtered = trades_df
        if selected_codes:
            filtered = filtered[filtered["代码"].isin(selected_codes)]

        st.dataframe(
            filtered.sort_values("日期", ascending=False),
            use_container_width=True,
            height=400,
            hide_index=True,
        )

        # 汇总
        buy_total = filtered[filtered["方向"] == "buy"]["价格"].astype(float).sum()
        sell_total = filtered[filtered["方向"] == "sell"]["价格"].astype(float).sum()
        st.write(f"买入总金额：¥{buy_total:,.2f} | 卖出总金额：¥{sell_total:,.2f}")

        # 导出
        csv = filtered.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button("📥 导出交易记录", csv, f"trades_{date.today()}.csv", "text/csv")

        # 导入
        uploaded = st.file_uploader("📤 导入交易记录", type=["csv", "json"])
        if uploaded:
            try:
                if uploaded.name.endswith(".csv"):
                    import_df = pd.read_csv(uploaded, encoding="utf-8-sig")
                    new_trades_list = []
                    for _, row in import_df.iterrows():
                        side = row.get("方向", row.get("side", "buy"))
                        if isinstance(side, str) and side in ("买", "买入"):
                            side = "buy"
                        elif isinstance(side, str) and side in ("卖", "卖出"):
                            side = "sell"
                        new_trades_list.append({
                            "date": str(row.get("日期", row.get("date", ""))),
                            "code": str(row.get("代码", row.get("code", ""))),
                            "name": str(row.get("名称", row.get("name", ""))),
                            "side": side,
                            "price": float(row.get("价格", row.get("price", 0))),
                            "qty": int(row.get("数量", row.get("qty", 0))),
                            "fee": float(row.get("手续费", row.get("fee", 0))),
                            "notes": str(row.get("备注", row.get("notes", ""))),
                        })
                    trades = new_trades_list
                    _save_trades(trades)
                    st.success(f"已导入 {len(trades)} 条交易记录")
                    st.rerun()
            except Exception as e:
                st.error(f"导入失败：{e}")

        # 清空按钮
        if st.button("🗑️ 清空所有记录", type="secondary"):
            _save_trades([])
            st.warning("已清空所有交易记录")
            st.rerun()

# ── Tab 3：持仓盈亏 ──
with tab3:
    st.subheader("当前持仓")

    pos_df, closed_df = _compute_fifo_positions(trades)

    if pos_df.empty:
        st.info("当前无持仓")
    else:
        # 获取实时价格
        codes = pos_df["代码"].unique().tolist()
        price_map: dict[str, float] = {}
        with st.spinner("获取实时价格..."):
            try:
                from smcore.data.quote import fetch_realtime_quotes
                quotes = fetch_realtime_quotes(codes)
                if not quotes.empty:
                    for _, row in quotes.iterrows():
                        price_map[row["code"]] = float(row["price"])
            except Exception:
                st.caption("⚠️ 实时价格获取失败，仅显示成本")

        # 计算浮动盈亏
        pnl_rows = []
        for _, row in pos_df.iterrows():
            code = row["代码"]
            current_price = price_map.get(code)
            cost = row["成本价"]
            qty = row["数量"]
            pnl = None
            pnl_pct = None
            if current_price:
                pnl = (current_price - cost) * qty
                pnl_pct = (current_price / cost - 1) * 100
            pnl_rows.append({
                "代码": code,
                "买入日期": row["买入日期"],
                "数量": qty,
                "成本价": cost,
                "现价": current_price if current_price else "N/A",
                "浮动盈亏": round(pnl, 2) if pnl else "N/A",
                "收益率%": round(pnl_pct, 2) if pnl_pct else "N/A",
            })

        pnl_df = pd.DataFrame(pnl_rows)

        # 用颜色标记
        def color_pnl(val):
            if isinstance(val, str):
                return ""
            try:
                v = float(val)
                return "color: #E33E3E" if v > 0 else "color: #009966"
            except (ValueError, TypeError):
                return ""

        st.dataframe(
            pnl_df.style.map(color_pnl, subset=["浮动盈亏", "收益率%"]),
            use_container_width=True,
            hide_index=True,
        )

        # 持仓汇总
        total_cost = pos_df["成本金额"].sum()
        if price_map:
            total_value = sum(
                price_map.get(r["代码"], r["成本价"]) * r["数量"]
                for _, r in pos_df.iterrows()
            )
            total_pnl = total_value - total_cost
            col_m1, col_m2, col_m3 = st.columns(3)
            with col_m1:
                st.metric("持仓成本", f"¥{total_cost:,.2f}")
            with col_m2:
                st.metric("当前市值", f"¥{total_value:,.2f}")
            with col_m3:
                st.metric("总浮动盈亏", f"¥{total_pnl:,.2f}",
                         delta=f"{total_pnl/total_cost*100:+.2f}%" if total_cost else "")

# ── Tab 4：已平仓记录 ──
with tab4:
    st.subheader("已平仓交易")

    _, closed_df = _compute_fifo_positions(trades)

    if closed_df.empty:
        st.info("暂无已平仓记录")
    else:
        # 汇总
        total_profit = closed_df["盈亏"].sum()
        win_count = (closed_df["盈亏"] > 0).sum()
        loss_count = (closed_df["盈亏"] < 0).sum()
        win_rate = win_count / len(closed_df) * 100 if len(closed_df) > 0 else 0

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("已平仓笔数", len(closed_df))
        with col2:
            st.metric("总盈亏", f"¥{total_profit:,.2f}",
                     delta=f"{'+' if total_profit >= 0 else ''}{total_profit:,.2f}")
        with col3:
            st.metric("胜率", f"{win_rate:.1f}%")
        with col4:
            st.metric("盈利/亏损", f"{win_count}/{loss_count}")

        st.dataframe(
            closed_df.sort_values("卖出日期", ascending=False),
            use_container_width=True,
            hide_index=True,
        )
