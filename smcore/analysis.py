"""Shared helpers for single-stock technical analysis."""
from __future__ import annotations

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


def build_stock_analysis(code: str, window: int = 20, k: float = 1.645, days_back: int = 180) -> dict[str, Any]:
    """Build a JSON-friendly technical analysis snapshot for a stock."""
    end_date = date.today()
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
    }
    return payload