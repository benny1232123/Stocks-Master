from __future__ import annotations

import pandas as pd


def calc_bollinger(df: pd.DataFrame, window: int = 20, k: float = 1.645) -> pd.DataFrame:
    if "close" not in df.columns:
        raise ValueError("输入数据缺少 close 列")

    out = df.copy()
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out = out.dropna(subset=["close"]).reset_index(drop=True)
    out["MA"] = out["close"].rolling(window=window).mean()
    out["STD"] = out["close"].rolling(window=window).std()
    out["Upper"] = out["MA"] + k * out["STD"]
    out["Lower"] = out["MA"] - k * out["STD"]
    return out


def evaluate_boll_signal(
    df: pd.DataFrame,
    near_ratio: float = 1.015,
    upper_near_ratio: float = 0.985,
) -> dict[str, object]:
    if df.empty:
        return {"signal": "无数据", "selected": False, "signal_type": "empty"}

    latest = df.iloc[-1]
    if pd.isna(latest.get("Lower")) or pd.isna(latest.get("Upper")):
        return {"signal": "数据不足（至少20个交易日）", "selected": False, "signal_type": "insufficient"}

    close = float(latest["close"])
    lower = float(latest["Lower"])
    upper = float(latest["Upper"])

    if close < lower:
        return {"signal": "超卖：收盘价低于下轨", "selected": True, "signal_type": "oversold"}
    if close <= lower * near_ratio:
        return {"signal": "关注：收盘价接近下轨", "selected": True, "signal_type": "near_lower"}
    if close > upper:
        return {"signal": "偏热：收盘价高于上轨", "selected": False, "signal_type": "overbought"}
    if close >= upper * upper_near_ratio:
        return {"signal": "高位：收盘价接近上轨", "selected": False, "signal_type": "near_upper"}
    return {"signal": "中性：位于布林带中部", "selected": False, "signal_type": "neutral"}
