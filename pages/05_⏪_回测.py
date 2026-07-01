"""策略回测：历史信号回测 + 收益分析"""
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
import numpy as np
from datetime import date, timedelta

st.set_page_config(page_title="策略回测", page_icon="⏪", layout="wide")

# ═══════════════════════════════════════════════
# 回测引擎
# ═══════════════════════════════════════════════


def run_backtest(
    signals: pd.DataFrame,
    hold_days: int = 5,
    initial_capital: float = 100000,
    max_positions: int = 10,
    slippage: float = 0.001,
) -> dict:
    """简易回测引擎。

    signals 需包含：代码, 日期, 建议买入价（可选）
    策略：次日开盘买入，持有 hold_days 个交易日后收盘卖出。
    """
    from smcore.data.kline import fetch_daily_k

    # 准备信号
    if "日期" in signals.columns:
        signals = signals.rename(columns={"日期": "date"})
    if "代码" in signals.columns:
        signals = signals.rename(columns={"代码": "code"})
    if "建议买入价" in signals.columns:
        signals = signals.rename(columns={"建议买入价": "price"})

    if "date" not in signals.columns or "code" not in signals.columns:
        return {"error": "信号文件缺少「日期」或「代码」列"}

    signals["date"] = pd.to_datetime(signals["date"], errors="coerce")
    signals = signals.dropna(subset=["date", "code"])
    signals = signals.sort_values("date")

    trades = []
    equity_curve: list[dict] = []
    cash = initial_capital
    holdings: dict[str, dict] = {}  # code -> {buy_date, buy_price, qty, capital}

    all_dates = sorted(signals["date"].unique())

    for i, signal_date in enumerate(all_dates):
        day_signals = signals[signals["date"] == signal_date]

        # 先处理到期卖出
        to_sell = []
        for code, h in holdings.items():
            if (signal_date - h["buy_date"]).days >= hold_days:
                to_sell.append(code)

        for code in to_sell:
            h = holdings.pop(code)
            # 获取卖出日收盘价
            sell_df = fetch_daily_k(code, days_back=5)
            if not sell_df.empty:
                sell_df["_dt"] = pd.to_datetime(sell_df["date"])
                sell_row = sell_df[sell_df["_dt"].dt.date <= signal_date.date()]
                if not sell_row.empty:
                    sell_price = float(sell_row.iloc[-1]["close"]) * (1 - slippage)
                    sell_amount = sell_price * h["qty"]
                    cash += sell_amount
                    trades.append({
                        "code": code,
                        "buy_date": h["buy_date"].strftime("%Y-%m-%d"),
                        "sell_date": signal_date.strftime("%Y-%m-%d"),
                        "buy_price": h["buy_price"],
                        "sell_price": sell_price,
                        "qty": h["qty"],
                        "return_pct": round((sell_price / h["buy_price"] - 1) * 100, 2),
                    })

        # 买入新信号
        available_slots = max_positions - len(holdings)
        buy_candidates = day_signals[
            ~day_signals["code"].isin(holdings.keys())
        ]
        if available_slots > 0 and not buy_candidates.empty:
            for _, sig in buy_candidates.head(available_slots).iterrows():
                code = sig["code"]
                kdf = fetch_daily_k(code, days_back=10)
                if kdf.empty:
                    continue
                kdf["_dt"] = pd.to_datetime(kdf["date"])
                next_day = kdf[kdf["_dt"].dt.date > signal_date.date()]
                if next_day.empty:
                    continue
                buy_price = float(next_day.iloc[0]["open"]) * (1 + slippage)
                per_trade = cash / available_slots if available_slots > 0 else 0
                if per_trade <= 0:
                    continue
                qty = int(per_trade / buy_price / 100) * 100
                if qty < 100:
                    continue
                cost = buy_price * qty
                if cost > cash:
                    continue
                cash -= cost
                holdings[code] = {
                    "buy_date": signal_date,
                    "buy_price": buy_price,
                    "qty": qty,
                    "capital": cost,
                }
                available_slots -= 1

        # 记录权益
        holding_value = sum(
            h["capital"] for h in holdings.values()
        )
        equity_curve.append({
            "date": signal_date.strftime("%Y-%m-%d"),
            "cash": round(cash, 2),
            "holding_value": round(holding_value, 2),
            "total": round(cash + holding_value, 2),
        })

    eq_df = pd.DataFrame(equity_curve)
    trades_df = pd.DataFrame(trades)

    # 计算指标
    if trades_df.empty or eq_df.empty:
        return {"error": "回测未产生任何交易", "equity": eq_df}

    total_return = (eq_df["total"].iloc[-1] / initial_capital - 1) * 100

    # 最大回撤
    eq_df["peak"] = eq_df["total"].cummax()
    eq_df["drawdown"] = (eq_df["total"] - eq_df["peak"]) / eq_df["peak"] * 100
    max_drawdown = eq_df["drawdown"].min()

    # 胜率
    win_rate = (trades_df["return_pct"] > 0).sum() / len(trades_df) * 100 if len(trades_df) > 0 else 0

    # 夏普比率（简化）
    if len(eq_df) > 1:
        eq_df["daily_return"] = eq_df["total"].pct_change()
        sharpe = eq_df["daily_return"].mean() / eq_df["daily_return"].std() * np.sqrt(252) if eq_df["daily_return"].std() > 0 else 0
    else:
        sharpe = 0

    return {
        "total_return": round(total_return, 2),
        "max_drawdown": round(max_drawdown, 2),
        "win_rate": round(win_rate, 1),
        "sharpe": round(sharpe, 2),
        "num_trades": len(trades_df),
        "equity": eq_df,
        "trades": trades_df,
    }


# ═══════════════════════════════════════════════
# 页面渲染
# ═══════════════════════════════════════════════

st.title("⏪ 策略回测")

st.caption("上传历史选股信号文件，模拟按信号执行的效果")

# 参数区
col1, col2, col3, col4 = st.columns(4)
with col1:
    hold_days = st.slider("持有天数", 1, 30, 5, help="买入后持有多久卖出")
with col2:
    initial_capital = st.number_input("初始资金", 10000, 10000000, 100000, step=10000)
with col3:
    max_positions = st.slider("最大持仓数", 1, 30, 10)
with col4:
    slippage = st.number_input("滑点（%）", 0.0, 2.0, 0.1, step=0.05) / 100

# 信号来源
source = st.radio(
    "信号来源",
    ["使用已有操作清单", "上传信号文件"],
    horizontal=True,
)

signals_df = None

if source == "使用已有操作清单":
    action_lists = sorted(ROOT.glob("stock_data/Daily-Action-List-*.csv"), reverse=True)
    if not action_lists:
        st.warning("暂无操作清单，先去「选股中心」跑一次")
    else:
        al_options = [f.name for f in action_lists]
        selected = st.selectbox("选择操作清单", al_options)
        file_path = ROOT / "stock_data" / selected
        signals_df = pd.read_csv(file_path, encoding="utf-8-sig")
        st.caption(f"已加载 {len(signals_df)} 条信号")

        # 显示信号日期范围
        if "日期" in signals_df.columns:
            dates = pd.to_datetime(signals_df["日期"], errors="coerce")
            st.info(f"信号日期范围：{dates.min().strftime('%Y-%m-%d')} ~ {dates.max().strftime('%Y-%m-%d')}")

else:
    uploaded = st.file_uploader("上传信号 CSV", type=["csv"])
    if uploaded:
        signals_df = pd.read_csv(uploaded, encoding="utf-8-sig")
        st.success(f"已加载 {len(signals_df)} 条信号")

if signals_df is not None and not signals_df.empty:
    st.markdown("---")
    st.subheader("📋 信号预览")
    st.dataframe(signals_df.head(10), use_container_width=True, hide_index=True)

    if st.button("🚀 开始回测", type="primary"):
        with st.spinner("正在运行回测... 这可能需要几分钟"):
            result = run_backtest(
                signals_df,
                hold_days=hold_days,
                initial_capital=initial_capital,
                max_positions=max_positions,
                slippage=slippage,
            )

        if "error" in result:
            st.warning(result["error"])
        else:
            st.success("回测完成！")

            # 指标卡片
            st.subheader("📊 回测指标")
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                st.metric("总收益率", f"{result['total_return']:.2f}%",
                         delta=f"{result['total_return']:+.2f}%")
            with col2:
                st.metric("最大回撤", f"{result['max_drawdown']:.2f}%")
            with col3:
                st.metric("胜率", f"{result['win_rate']:.1f}%")
            with col4:
                st.metric("夏普比率", f"{result['sharpe']:.2f}")
            with col5:
                st.metric("交易笔数", result["num_trades"])

            # 权益曲线
            st.subheader("📈 权益曲线")
            eq = result["equity"]
            if not eq.empty:
                import matplotlib.pyplot as plt

                fig, axes = plt.subplots(2, 1, figsize=(12, 8),
                                         gridspec_kw={"height_ratios": [3, 1]})

                ax1, ax2 = axes
                dates = pd.to_datetime(eq["date"])
                ax1.plot(dates, eq["total"].values, "b-", linewidth=2, label="总权益")
                ax1.axhline(y=initial_capital, color="gray", linewidth=0.5, linestyle="--", alpha=0.5, label="初始资金")
                ax1.fill_between(dates, eq["total"].values, initial_capital,
                                 where=(eq["total"].values >= initial_capital),
                                 color="#E33E3E", alpha=0.1)
                ax1.fill_between(dates, eq["total"].values, initial_capital,
                                 where=(eq["total"].values < initial_capital),
                                 color="#009966", alpha=0.1)
                ax1.set_ylabel("权益（元）")
                ax1.set_title("回测权益曲线", fontsize=14, fontweight="bold")
                ax1.legend()
                ax1.grid(True, alpha=0.3)

                # 回撤
                ax2.fill_between(dates, eq["drawdown"].values, 0,
                                 color="#009966", alpha=0.3)
                ax2.set_ylabel("回撤（%）")
                ax2.set_xlabel("日期")
                ax2.grid(True, alpha=0.3)
                ax2.set_ylim(eq["drawdown"].min() * 1.1, 1)

                plt.tight_layout()
                st.pyplot(fig)
                plt.close()

            # 交易明细
            st.subheader("📋 交易明细")
            trades = result["trades"]
            if not trades.empty:
                def color_ret(val):
                    try:
                        return "color: #E33E3E" if float(val) > 0 else "color: #009966"
                    except (ValueError, TypeError):
                        return ""
                st.dataframe(
                    trades.style.map(color_ret, subset=["return_pct"]),
                    use_container_width=True,
                    hide_index=True,
                )
