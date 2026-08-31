"""Shared helpers for single-stock technical analysis."""
from __future__ import annotations

import threading
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd

from smcore.data.kline import fetch_daily_k
from smcore.indicators.boll import calc_bollinger, evaluate_boll_signal


def calc_ma(close: pd.Series, periods: list[int] | None = None) -> pd.DataFrame:
    """Calculate moving averages for a close-price series."""
    periods = periods or [5, 10, 20, 60]
    frame = pd.DataFrame(index=close.index)
    for period in periods:
        frame[f"MA{period}"] = close.rolling(window=period).mean()
    return frame


def calc_macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """Calculate MACD indicators."""
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    hist = 2 * (dif - dea)
    return pd.DataFrame({"DIF": dif, "DEA": dea, "MACD": hist}, index=close.index)


def calc_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Calculate RSI."""
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(span=period, adjust=False).mean()
    avg_loss = loss.ewm(span=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def calc_kdj(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 9, m1: int = 3, m2: int = 3) -> pd.DataFrame:
    """Calculate KDJ."""
    lowest = low.rolling(window=n).min()
    highest = high.rolling(window=n).max()
    rsv = ((close - lowest) / (highest - lowest).replace(0, np.nan)) * 100
    k = rsv.ewm(span=m1, adjust=False).mean()
    d = k.ewm(span=m2, adjust=False).mean()
    j = 3 * k - 2 * d
    return pd.DataFrame({"K": k, "D": d, "J": j}, index=close.index)


def build_stock_analysis(
    code: str,
    window: int = 20,
    k: float = 1.645,
    days_back: int = 180,
    as_of: date | None = None,
    with_fundamentals: bool = True,
) -> dict[str, Any]:
    """Build a JSON-friendly technical analysis snapshot for a stock.

    as_of: 指定截至日期（默认今天），用于历史回看；历史日期仅取缓存 K 线，不联网。
    with_fundamentals: 是否补取基本面（历史回看建议关掉以提速）。
    """
    end_date = as_of or date.today()
    start_date = end_date - timedelta(days=days_back)
    kdf = fetch_daily_k(code, start_date, end_date)
    if kdf.empty:
        return {"code": code, "error": "未获取到K线数据"}

    kdf = calc_bollinger(kdf, window=window, k=k)
    signal_info = evaluate_boll_signal(kdf, near_ratio=1.015)

    plot_df = kdf.copy()
    for column in ["close", "open", "high", "low", "volume", "MA", "Upper", "Lower"]:
        if column in plot_df.columns:
            plot_df[column] = pd.to_numeric(plot_df[column], errors="coerce")
    plot_df["date"] = pd.to_datetime(plot_df["date"], errors="coerce")
    plot_df = plot_df.dropna(subset=["close"])

    ma_df = calc_ma(plot_df["close"])
    macd_df = calc_macd(plot_df["close"])
    rsi_series = calc_rsi(plot_df["close"])
    kdj_df = calc_kdj(plot_df["high"], plot_df["low"], plot_df["close"])

    latest = plot_df.iloc[-1]
    last_n = min(120, len(plot_df))

    payload: dict[str, Any] = {
        "code": code,
        "window": window,
        "k": k,
        "days_back": days_back,
        "signal": signal_info,
        "latest": {
            "date": latest["date"].strftime("%Y-%m-%d") if pd.notna(latest["date"]) else None,
            "close": float(latest["close"]),
            "lower": float(latest["Lower"]) if pd.notna(latest.get("Lower")) else None,
            "upper": float(latest["Upper"]) if pd.notna(latest.get("Upper")) else None,
            "middle": float(latest["MA"]) if pd.notna(latest.get("MA")) else None,
            "rsi": float(rsi_series.iloc[-1]) if not pd.isna(rsi_series.iloc[-1]) else None,
            # MACD
            "dif": float(macd_df["DIF"].iloc[-1]) if not pd.isna(macd_df["DIF"].iloc[-1]) else None,
            "dea": float(macd_df["DEA"].iloc[-1]) if not pd.isna(macd_df["DEA"].iloc[-1]) else None,
            "macd_hist": float(macd_df["MACD"].iloc[-1]) if not pd.isna(macd_df["MACD"].iloc[-1]) else None,
            # KDJ
            "k_val": float(kdj_df["K"].iloc[-1]) if not pd.isna(kdj_df["K"].iloc[-1]) else None,
            "d_val": float(kdj_df["D"].iloc[-1]) if not pd.isna(kdj_df["D"].iloc[-1]) else None,
            "j_val": float(kdj_df["J"].iloc[-1]) if not pd.isna(kdj_df["J"].iloc[-1]) else None,
            # MA
            "ma5": float(ma_df["MA5"].iloc[-1]) if not pd.isna(ma_df["MA5"].iloc[-1]) else None,
            "ma10": float(ma_df["MA10"].iloc[-1]) if not pd.isna(ma_df["MA10"].iloc[-1]) else None,
            "ma20": float(ma_df["MA20"].iloc[-1]) if not pd.isna(ma_df["MA20"].iloc[-1]) else None,
            "ma60": float(ma_df["MA60"].iloc[-1]) if not pd.isna(ma_df["MA60"].iloc[-1]) else None,
        },
        "metrics": {
            "latest_close": float(latest["close"]),
            "dist_to_lower_pct": signal_info.get("dist_to_lower_pct"),
            "dist_to_upper_pct": signal_info.get("dist_to_upper_pct"),
            "signal_text": signal_info.get("signal"),
            "bandwidth": signal_info.get("bandwidth"),
        },
        "series": {
            "rows": plot_df.tail(last_n).assign(
                MA5=ma_df.get("MA5"),
                MA10=ma_df.get("MA10"),
                MA20=ma_df.get("MA20"),
                MA60=ma_df.get("MA60"),
                DIF=macd_df.get("DIF"),
                DEA=macd_df.get("DEA"),
                MACD=macd_df.get("MACD"),
                RSI=rsi_series,
                K=kdj_df.get("K"),
                D=kdj_df.get("D"),
                J=kdj_df.get("J"),
            ).replace({pd.NA: None, np.nan: None}).to_dict(orient="records")
        },
        # ── 基本面 / 资金面（缓存优先，未命中实时补取并写回缓存）──
        "fundamentals": _build_fundamentals(code) if with_fundamentals else None,
    }
    return payload


def recommendation_from_analysis(analysis: dict) -> dict[str, object]:
    """由技术面指标综合给出持仓建议（仅供参考，不构成投资建议）。

    综合维度：均线趋势(MA20/MA60)、MACD 柱、布林带位置/买卖信号、
    RSI 超买超卖、KDJ(J 值) 超买超卖。各维度加权打分，按总分分档：
        加仓 / 持有偏多 / 持有观望 / 减仓偏空 / 减仓
    返回 {action, score, reason, drivers}。
    """
    if analysis.get("error"):
        return {"action": "未知", "score": 0.0, "reason": "分析失败", "drivers": []}

    latest = analysis.get("latest", {}) or {}
    metrics = analysis.get("metrics", {}) or {}
    signal_info = analysis.get("signal", {}) or {}

    close = latest.get("close")
    up = latest.get("upper")
    lo = latest.get("lower")
    ma20 = latest.get("ma20")
    ma60 = latest.get("ma60")
    rsi = latest.get("rsi")
    hist = latest.get("macd_hist")
    j = latest.get("j_val")
    sig = metrics.get("signal_text") or latest.get("signal_text") or ""
    sig_selected = bool(signal_info.get("selected"))

    factors: list[tuple[float, str]] = []

    # 均线趋势
    if close is not None and ma20 is not None and ma60 is not None:
        if close > ma20 > ma60:
            factors.append((2.0, "多头排列(价>MA20>MA60)"))
        elif close < ma20 < ma60:
            factors.append((-2.0, "空头排列(价<MA20<MA60)"))
        else:
            factors.append((0.0, "均线缠绕震荡"))

    # MACD 柱
    if hist is not None:
        factors.append((1.0 if hist > 0 else -1.0, "MACD红柱" if hist > 0 else "MACD绿柱"))

    # 布林带位置 / 买卖信号
    if close is not None and up and lo and up > lo:
        if close >= up * 0.995:
            factors.append((-2.0, "触及/突破布林上轨(超买)"))
        elif close <= lo * 1.005:
            factors.append((2.0, "触及/跌破布林下轨(超卖)"))
        elif "near_upper" in sig or "overbought" in sig:
            factors.append((-1.5, "高位接近上轨"))
        elif sig_selected or any(
            kw in sig for kw in ("near_lower", "mid_pullback", "squeeze", "oversold")
        ):
            factors.append((1.5, "布林买点信号"))

    # RSI
    if rsi is not None:
        if rsi > 70:
            factors.append((-1.0, f"RSI超买({rsi:.0f})"))
        elif rsi < 30:
            factors.append((1.0, f"RSI超卖({rsi:.0f})"))

    # KDJ J 值
    if j is not None:
        if j > 100:
            factors.append((-1.0, f"KDJ超买(J={j:.0f})"))
        elif j < 0:
            factors.append((1.0, f"KDJ超卖(J={j:.0f})"))

    score = sum(w for w, _ in factors)
    if score >= 3:
        action = "加仓"
    elif score >= 1:
        action = "持有偏多"
    elif score > -1:
        action = "持有观望"
    elif score > -3:
        action = "减仓偏空"
    else:
        action = "减仓"

    top = sorted(factors, key=lambda x: abs(x[0]), reverse=True)[:2]
    reason = "，".join(lbl for _, lbl in top) if top else "无明显多空信号"
    return {
        "action": action,
        "score": round(score, 1),
        "reason": reason,
        "drivers": [lbl for _, lbl in factors],
    }


def _build_fundamentals(code: str, timeout: float = 12.0) -> dict | None:
    """汇总个股基本面 + 资金面快照，供前端做综合分析。

    优先读本地缓存；未命中时通过 smcore.strategy.fundamental.fetch_fundamental
    实时联网补取（腾讯估值 + baostock 质量/成长/换手）并写回缓存。
    全程在后台线程执行 + 超时保护，避免慢网络拖垮技术面分析响应；
    超时/失败返回 None（前端显示「暂无基本面数据」提示，不影响技术面）。
    """
    try:
        from smcore.strategy.fundamental import fetch_fundamental
    except Exception:
        return None

    result: dict | None = None

    def _run() -> None:
        nonlocal result
        try:
            result = fetch_fundamental(code)
        except Exception:
            result = None

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    # 超时则放弃本次补取（技术面已就绪返回）；残留线程会在后台完成并写缓存，供下次命中
    return result