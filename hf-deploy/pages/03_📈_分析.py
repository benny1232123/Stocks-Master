"""个股分析：K线 + 布林带 + MACD/RSI/KDJ"""
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

st.set_page_config(page_title="个股分析", page_icon="📈", layout="wide")

# ═══════════════════════════════════════════════
# 技术指标计算
# ═══════════════════════════════════════════════


def calc_ma(close: pd.Series, periods: list[int] = [5, 10, 20, 60]) -> pd.DataFrame:
    """计算多条均线。"""
    df = pd.DataFrame(index=close.index)
    for p in periods:
        df[f"MA{p}"] = close.rolling(window=p).mean()
    return df


def calc_macd(close: pd.Series, fast=12, slow=26, signal=9) -> pd.DataFrame:
    """MACD 指标。"""
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    hist = 2 * (dif - dea)
    return pd.DataFrame({"DIF": dif, "DEA": dea, "MACD": hist}, index=close.index)


def calc_rsi(close: pd.Series, period=14) -> pd.Series:
    """RSI 指标。"""
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(span=period, adjust=False).mean()
    avg_loss = loss.ewm(span=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def calc_kdj(high, low, close, n=9, m1=3, m2=3) -> pd.DataFrame:
    """KDJ 指标。"""
    lowest = low.rolling(window=n).min()
    highest = high.rolling(window=n).max()
    rsv = ((close - lowest) / (highest - lowest).replace(0, np.nan)) * 100
    k = rsv.ewm(span=m1, adjust=False).mean()
    d = k.ewm(span=m2, adjust=False).mean()
    j = 3 * k - 2 * d
    return pd.DataFrame({"K": k, "D": d, "J": j}, index=close.index)


# ═══════════════════════════════════════════════
# 页面渲染
# ═══════════════════════════════════════════════

st.title("📈 个股技术分析")

# 输入区
col_code, col_window, col_k, col_days = st.columns([2, 1, 1, 1])
with col_code:
    code_input = st.text_input("股票代码", placeholder="例如：000001、600519", value="")
with col_window:
    window = st.slider("布林窗口", 10, 60, 20)
with col_k:
    k = st.slider("K 值", 1.0, 3.0, 1.645, 0.005)
with col_days:
    days_back = st.slider("回看天数", 30, 500, 180)

code = code_input.strip()

if not code:
    st.info("👆 输入股票代码开始分析")
    st.stop()

from smcore.data.kline import fetch_daily_k
from smcore.indicators.boll import calc_bollinger, evaluate_boll_signal
from smcore.data.quote import fetch_realtime_quotes

# 获取数据
with st.spinner(f"正在获取 {code} 的K线数据..."):
    kdf = fetch_daily_k(code, days_back=days_back)

if kdf.empty:
    st.error(f"未获取到 {code} 的K线数据，请检查代码是否正确")
    st.stop()

# 计算指标
kdf = calc_bollinger(kdf, window=window, k=k)
signal_info = evaluate_boll_signal(kdf, window=window, k=k)

# 转为数值用于绘图
plot_df = kdf.copy()
plot_df["close"] = pd.to_numeric(plot_df["close"], errors="coerce")
plot_df["open"] = pd.to_numeric(plot_df["open"], errors="coerce")
plot_df["high"] = pd.to_numeric(plot_df["high"], errors="coerce")
plot_df["low"] = pd.to_numeric(plot_df["low"], errors="coerce")
plot_df["volume"] = pd.to_numeric(plot_df["volume"], errors="coerce")
plot_df["middle"] = pd.to_numeric(plot_df["middle"], errors="coerce")
plot_df["upper"] = pd.to_numeric(plot_df["upper"], errors="coerce")
plot_df["lower"] = pd.to_numeric(plot_df["lower"], errors="coerce")
plot_df["date"] = pd.to_datetime(plot_df["date"], errors="coerce")
plot_df = plot_df.dropna(subset=["close"])

# 计算其他指标
ma_df = calc_ma(plot_df["close"])
plot_df = pd.concat([plot_df, ma_df], axis=1)

macd_df = calc_macd(plot_df["close"])
rsi_series = calc_rsi(plot_df["close"])
kdj_df = calc_kdj(plot_df["high"], plot_df["low"], plot_df["close"])

# ── 股票基本信息 ──
col_info1, col_info2, col_info3, col_info4 = st.columns(4)
latest = plot_df.iloc[-1]
with col_info1:
    st.metric("最新收盘价", f"{latest['close']:.2f}")
with col_info2:
    delta_val = signal_info.get("dist_to_lower_pct", 0)
    st.metric("距下轨", f"{delta_val:.1f}%" if delta_val else "N/A")
with col_info3:
    boll_sig = signal_info.get("signal", "N/A")
    st.metric("布林信号", boll_sig)
with col_info4:
    rsi_val = rsi_series.iloc[-1] if not pd.isna(rsi_series.iloc[-1]) else 0
    st.metric("RSI(14)", f"{rsi_val:.1f}")

st.markdown("---")

# ── K线 + 布林带 主图 ──
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# 取最近 120 个交易日绘图（太多挤在一起看不清）
plot_n = min(120, len(plot_df))
plot_data = plot_df.iloc[-plot_n:].copy()

fig, axes = plt.subplots(4, 1, figsize=(14, 12),
                         gridspec_kw={"height_ratios": [3, 1, 1, 1]},
                         sharex=True)

ax1, ax2, ax3, ax4 = axes

# === 子图1：K线 + 布林带 + 均线 ===
dates = plot_data["date"].values
for i, (_, row) in enumerate(plot_data.iterrows()):
    color = "#E33E3E" if row["close"] >= row["open"] else "#009966"
    ax1.plot([dates[i], dates[i]], [row["low"], row["high"]], color=color, linewidth=0.8)
    body_bottom = min(row["open"], row["close"])
    body_height = abs(row["close"] - row["open"])
    ax1.bar(dates[i], body_height, bottom=body_bottom, color=color, width=0.6, alpha=0.85)

# 布林带
ax1.plot(dates, plot_data["upper"].values, "b--", alpha=0.4, linewidth=1, label=f"上轨(k={k})")
ax1.plot(dates, plot_data["middle"].values, "orange", alpha=0.5, linewidth=1, label=f"中轨({window}MA)")
ax1.plot(dates, plot_data["lower"].values, "b--", alpha=0.4, linewidth=1, label="下轨")
ax1.fill_between(dates, plot_data["upper"].values, plot_data["lower"].values,
                  alpha=0.05, color="blue")

# MA 均线
for ma_col in ["MA5", "MA10", "MA20", "MA60"]:
    if ma_col in plot_data.columns:
        vals = plot_data[ma_col].values
        if not np.all(np.isnan(vals)):
            ax1.plot(dates, vals, linewidth=0.8, alpha=0.6, label=ma_col)

ax1.set_ylabel("价格")
ax1.legend(loc="upper left", fontsize=7, ncol=2)
ax1.set_title(f"{code} 技术分析", fontsize=14, fontweight="bold")
ax1.grid(True, alpha=0.3)

# === 子图2：成交量 ===
vol_colors = ["#E33E3E" if plot_data["close"].iloc[i] >= plot_data["open"].iloc[i]
              else "#009966" for i in range(len(plot_data))]
ax2.bar(dates, plot_data["volume"].values, color=vol_colors, width=0.6, alpha=0.7)
ax2.set_ylabel("成交量")
ax2.grid(True, alpha=0.3)

# === 子图3：MACD ===
macd_plot = macd_df.iloc[-plot_n:]
macd_colors = ["#E33E3E" if v >= 0 else "#009966" for v in macd_plot["MACD"].values]
ax3.bar(dates, macd_plot["MACD"].values, color=macd_colors, width=0.6, alpha=0.7)
ax3.plot(dates, macd_plot["DIF"].values, "b-", linewidth=1, alpha=0.8, label="DIF")
ax3.plot(dates, macd_plot["DEA"].values, "orange", linewidth=1, alpha=0.8, label="DEA")
ax3.axhline(y=0, color="gray", linewidth=0.5, alpha=0.5)
ax3.set_ylabel("MACD")
ax3.legend(loc="upper left", fontsize=7)
ax3.grid(True, alpha=0.3)

# === 子图4：RSI + KDJ ===
rsi_plot = rsi_series.iloc[-plot_n:]
kdj_plot = kdj_df.iloc[-plot_n:]
ax4.plot(dates, rsi_plot.values, "purple", linewidth=1, alpha=0.8, label="RSI(14)")
ax4.plot(dates, kdj_plot["K"].values, "blue", linewidth=0.8, alpha=0.6, label="K")
ax4.plot(dates, kdj_plot["D"].values, "orange", linewidth=0.8, alpha=0.6, label="D")
ax4.axhline(y=80, color="red", linewidth=0.5, alpha=0.3, linestyle="--")
ax4.axhline(y=20, color="green", linewidth=0.5, alpha=0.3, linestyle="--")
ax4.axhline(y=50, color="gray", linewidth=0.5, alpha=0.3, linestyle="--")
ax4.set_ylabel("RSI / KDJ")
ax4.legend(loc="upper left", fontsize=7)
ax4.grid(True, alpha=0.3)

# 日期格式
ax4.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
ax4.xaxis.set_major_locator(mdates.AutoDateLocator())
for ax in axes:
    ax.tick_params(axis="x", rotation=45)

plt.tight_layout()
st.pyplot(fig)
plt.close()

# ── 布林带信号详情 ──
st.markdown("---")
st.subheader("📡 布林带信号详情")

sig_cols = st.columns(4)
signal_map = {
    "信号": signal_info.get("signal", "N/A"),
    "最新价": f"{signal_info.get('price', 0):.2f}",
    "下轨": f"{signal_info.get('lower', 0):.2f}",
    "上轨": f"{signal_info.get('upper', 0):.2f}",
    "距下轨": f"{signal_info.get('dist_to_lower_pct', 0):.1f}%",
    "距上轨": f"{signal_info.get('dist_to_upper_pct', 0):.1f}%",
    "中轨": f"{signal_info.get('middle', 0):.2f}",
    "带宽比": f"{signal_info.get('bandwidth', 0):.1f}%" if signal_info.get('bandwidth') else "N/A",
}
for i, (key, val) in enumerate(signal_map.items()):
    with sig_cols[i % 4]:
        st.metric(label=key, value=val)

st.caption(f"参数：window={window}, k={k}, days_back={days_back}")
