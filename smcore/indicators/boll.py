"""布林带（Bollinger Bands）指标与信号 —— 全项目唯一实现。

此前 Boll 逻辑散落在三处：
- smcore/strategies/boll.py（命令行经 python -m smcore.strategies.boll，前复权）
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


def _band_metrics(close: float, lower: float, upper: float, middle: float | None = None) -> dict:
    """计算布林带位置指标（供所有信号分支统一返回）。"""
    dist_lower = (close - lower) / lower * 100 if lower else None
    dist_upper = (close - upper) / upper * 100 if upper else None
    bandwidth = ((upper - lower) / middle * 100) if (middle and middle != 0) else None
    return {
        "dist_to_lower_pct": round(dist_lower, 2) if dist_lower is not None else None,
        "dist_to_upper_pct": round(dist_upper, 2) if dist_upper is not None else None,
        "bandwidth": round(bandwidth, 2) if bandwidth is not None else None,
    }


def evaluate_boll_signal(
    df: pd.DataFrame,
    near_ratio: float = 1.015,
    upper_near_ratio: float = 0.985,
    mid_pullback_pct: float = 0.02,
    squeeze_enabled: bool = True,
    squeeze_window: int = 20,
    squeeze_pctile: float = 0.20,
    continuous_streak_cap: int = 3,
) -> dict[str, object]:
    """评估最新一根 K 线的布林带信号（重构版：优质股回踩买点）。

    触发分支（互斥优先级，先到先得）：
    - oversold:      close < lower（超卖，极端兜底）
    - near_lower:    close <= lower*near_ratio（接近下轨，兜底）
    - mid_pullback:  |close-MA|/MA < mid_pullback_pct（中轨回踩，主触发①）
    - squeeze:       bandwidth < 近 squeeze_window 日 squeeze_pctile 分位（带宽收缩，主触发②）
    - *_continuous:  同一分支连续触发超过 continuous_streak_cap 天 → 本日不重复选
    - overbought / near_upper / neutral / insufficient / empty: 不触发

    设计动机：Boll 前置「资金流好+基本面好+国家队重仓」已锁定优质股，要求其同时
    超卖（close<下轨）天然矛盾，导致候选长期 0~4 只/天。新增「中轨回踩」「带宽收缩」
    两类与优质股特征自洽的买点，使其常态产生信号；超卖/近下轨保留为极端兜底。
    """
    def _res(signal, selected, signal_type, streak=None, is_squeeze=False):
        dist_mid = (close - middle) / middle * 100 if middle else None
        return {
            "signal": signal,
            "selected": selected,
            "signal_type": signal_type,
            **bm,
            "dist_to_mid_pct": round(dist_mid, 2) if dist_mid is not None else None,
            "is_squeeze": is_squeeze,
            **({"streak": streak} if streak is not None else {}),
        }

    if df.empty:
        return {"signal": "无数据", "selected": False, "signal_type": "empty",
                "dist_to_lower_pct": None, "dist_to_upper_pct": None,
                "bandwidth": None, "dist_to_mid_pct": None, "is_squeeze": False}

    latest = df.iloc[-1]
    if pd.isna(latest.get("Lower")) or pd.isna(latest.get("Upper")) or pd.isna(latest.get("MA")):
        return {"signal": "数据不足（至少 20 个交易日）", "selected": False, "signal_type": "insufficient",
                "dist_to_lower_pct": None, "dist_to_upper_pct": None,
                "bandwidth": None, "dist_to_mid_pct": None, "is_squeeze": False}

    close = float(latest["close"])
    lower = float(latest["Lower"])
    upper = float(latest["Upper"])
    middle = float(latest["MA"])
    bm = _band_metrics(close, lower, upper, middle)

    # ── 序列与分支 mask（互斥：后续分支排除已命中的前序分支）──
    close_series = pd.to_numeric(df["close"], errors="coerce")
    lower_series = pd.to_numeric(df["Lower"], errors="coerce")
    middle_series = pd.to_numeric(df["MA"], errors="coerce")
    upper_series = pd.to_numeric(df["Upper"], errors="coerce")
    bandwidth_series = (upper_series - lower_series) / middle_series

    oversold_mask = close_series < lower_series
    near_lower_mask = (close_series <= lower_series * near_ratio) & ~oversold_mask
    mid_mask = (
        ((close_series - middle_series).abs() / middle_series < mid_pullback_pct)
        & ~near_lower_mask & ~oversold_mask
    )

    is_squeeze = False
    squeeze_mask = pd.Series(False, index=df.index)
    if squeeze_enabled and bandwidth_series.notna().sum() >= squeeze_window:
        squeeze_thresh_series = bandwidth_series.rolling(squeeze_window).quantile(squeeze_pctile)
        valid = bandwidth_series.notna() & squeeze_thresh_series.notna()
        squeeze_mask = (bandwidth_series < squeeze_thresh_series) & valid
        is_squeeze = bool(squeeze_mask.iloc[-1])
    squeeze_mask = squeeze_mask & ~mid_mask & ~near_lower_mask & ~oversold_mask

    oversold_streak = _trailing_true_count(oversold_mask)
    near_lower_streak = _trailing_true_count(near_lower_mask)
    mid_streak = _trailing_true_count(mid_mask)
    squeeze_streak = _trailing_true_count(squeeze_mask)

    cap = max(1, int(continuous_streak_cap))

    if close < lower:
        if oversold_streak > cap:
            return _res(f"连续超卖：已连续{oversold_streak}日低于下轨（本日不重复触发）",
                        False, "oversold_continuous", streak=oversold_streak)
        return _res("超卖：收盘价低于下轨", True, "oversold", is_squeeze=is_squeeze)
    if close <= lower * near_ratio:
        if near_lower_streak > cap:
            return _res(f"连续接近下轨：已连续{near_lower_streak}日（本日不重复触发）",
                        False, "near_lower_continuous", streak=near_lower_streak)
        return _res("关注：收盘价接近下轨", True, "near_lower", is_squeeze=is_squeeze)
    if bool(mid_mask.iloc[-1]):
        if mid_streak > cap:
            return _res(f"连续中轨回踩：已连续{mid_streak}日（本日不重复触发）",
                        False, "mid_pullback_continuous", streak=mid_streak)
        return _res("中轨回踩：价格贴近20日线", True, "mid_pullback", is_squeeze=is_squeeze)
    if is_squeeze:
        if squeeze_streak > cap:
            return _res(f"连续带宽收缩：已连续{squeeze_streak}日（本日不重复触发）",
                        False, "squeeze_continuous", streak=squeeze_streak, is_squeeze=True)
        return _res("带宽收缩：波动率收敛蓄势", True, "squeeze", is_squeeze=True)
    if close > upper:
        return _res("偏热：收盘价高于上轨", False, "overbought", is_squeeze=is_squeeze)
    if close >= upper * upper_near_ratio:
        return _res("高位：收盘价接近上轨", False, "near_upper", is_squeeze=is_squeeze)
    return _res("中性：位于布林带中部", False, "neutral", is_squeeze=is_squeeze)
