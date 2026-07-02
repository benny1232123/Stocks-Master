"""布林带（Bollinger Bands）指标与信号 —— 全项目唯一实现。

此前 Boll 逻辑散落在三处：
- Frequently-Used-Program/Stock-Selection-Boll.py（命令行，不复权）
- boll-visualizer/src/core/indicators.py（可视化，前复权）
- auto_notify_boll.py:_calc_boll_levels（主流程复算，tail(20)）
三处参数名、边界条件、返回结构各不相同。本模块统一为单一真相源。
"""
from __future__ import annotations

import pandas as pd


def calc_bollinger(df: pd.DataFrame, window: int = 20, k: float = 1.645) -> pd.DataFrame:
    """计算布林带：MA ± k·STD。

    Args:
        df: 至少包含 close 列的行情数据。
        window: 均线/标准差窗口（默认 20）。
        k: 标准差倍数（默认 1.645，对应 90% 概率区间）。

    Returns:
        追加 MA / STD / Upper / Lower 列的副本。
    """
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


def _trailing_true_count(mask: pd.Series) -> int:
    """从末尾向前统计连续 True 的个数。"""
    flags = mask.fillna(False).astype(bool).tolist()
    count = 0
    for item in reversed(flags):
        if not item:
            break
        count += 1
    return count


def evaluate_boll_signal(
    df: pd.DataFrame,
    near_ratio: float = 1.015,
    upper_near_ratio: float = 0.985,
    suppress_continuous_oversold: bool = True,
    max_oversold_streak_for_entry: int = 1,
) -> dict[str, object]:
    """评估最新一根 K 线的布林带信号。

    信号类型：
    - oversold: 收盘价低于下轨（超卖，触发）
    - near_lower: 收盘价接近下轨（触发）
    - oversold_continuous: 连续超卖，本日不重复触发
    - overbought / near_upper / neutral: 不触发
    - insufficient / empty: 数据不足
    """
    if df.empty:
        return {"signal": "无数据", "selected": False, "signal_type": "empty"}

    latest = df.iloc[-1]
    if pd.isna(latest.get("Lower")) or pd.isna(latest.get("Upper")):
        return {"signal": "数据不足（至少 20 个交易日）", "selected": False, "signal_type": "insufficient"}

    close = float(latest["close"])
    lower = float(latest["Lower"])
    upper = float(latest["Upper"])

    close_series = pd.to_numeric(df.get("close", pd.Series(dtype=float)), errors="coerce")
    lower_series = pd.to_numeric(df.get("Lower", pd.Series(dtype=float)), errors="coerce")
    oversold_mask = (close_series < lower_series) if len(close_series) == len(lower_series) else pd.Series(dtype=bool)
    oversold_streak = _trailing_true_count(oversold_mask) if not oversold_mask.empty else 0

    if close < lower:
        if suppress_continuous_oversold and oversold_streak > max(1, int(max_oversold_streak_for_entry)):
            return {
                "signal": f"连续超卖：已连续{oversold_streak}日低于下轨（本日不重复触发）",
                "selected": False,
                "signal_type": "oversold_continuous",
                "streak": oversold_streak,
            }
        return {"signal": "超卖：收盘价低于下轨", "selected": True, "signal_type": "oversold"}
    if close <= lower * near_ratio:
        return {"signal": "关注：收盘价接近下轨", "selected": True, "signal_type": "near_lower"}
    if close > upper:
        return {"signal": "偏热：收盘价高于上轨", "selected": False, "signal_type": "overbought"}
    if close >= upper * upper_near_ratio:
        return {"signal": "高位：收盘价接近上轨", "selected": False, "signal_type": "near_upper"}
    return {"signal": "中性：位于布林带中部", "selected": False, "signal_type": "neutral"}
