"""连续组合回测纯函数：把多个信号日的前向回测 sleeve 组合成一条连续净值曲线。"""
from __future__ import annotations

import math
from typing import Dict

import pandas as pd


def build_continuous_curve(
    sleeves: Dict[str, Dict[object, float]], normalize: bool = True
) -> pd.Series:
    """组合权益曲线：每个日历日所有活跃 sleeve 的 total 均值（跨 sleeve 等权）。

    sleeves: {tag: {date: total}}。返回按日期升序的 pd.Series(date -> 日均值)。
    normalize=True 时额外缩放使得第一个值 = 1.0。
    """
    all_dates = sorted({d for s in sleeves.values() for d in s})
    if not all_dates:
        return pd.Series(dtype=float)
    vals = []
    for d in all_dates:
        tot = [s[d] for s in sleeves.values() if d in s]
        vals.append(sum(tot) / len(tot) if tot else float("nan"))
    s = pd.Series(vals, index=all_dates).dropna()
    if normalize and len(s):
        s = s / s.iloc[0]
    return s


def compute_metrics(nav: pd.Series, annualization: int = 252) -> dict:
    """从连续净值计算完整指标集。"""
    nav = nav.dropna()
    if len(nav) < 2:
        return {"error": "insufficient_data"}
    rets = nav.pct_change().dropna()
    total = nav.iloc[-1] / nav.iloc[0] - 1.0
    n = len(nav) - 1
    years = n / annualization
    annual = (
        (nav.iloc[-1] / nav.iloc[0]) ** (1.0 / years) - 1.0
        if years > 0 and nav.iloc[0] > 0
        else float("nan")
    )
    vol = rets.std(ddof=0) * math.sqrt(annualization)
    sharpe = annual / vol if vol and not math.isnan(vol) and vol > 0 else 0.0
    dd = (nav / nav.cummax() - 1.0).min()
    win = float((rets > 0).mean())
    nav2 = nav.copy()
    nav2.index = pd.DatetimeIndex(nav.index)
    monthly_last = nav2.groupby(nav2.index.to_period("M")).last()
    monthly = monthly_last.pct_change().dropna() * 100.0
    return {
        "total_return_pct": round(total * 100, 2),
        "annual_return_pct": round(annual * 100, 2),
        "annual_vol_pct": round(vol * 100, 2) if not math.isnan(vol) else None,
        "sharpe": round(sharpe, 3),
        "max_drawdown_pct": round(dd * 100, 2),
        "win_rate_pct": round(win * 100, 1),
        "n_days": int(n),
        "monthly": {k.strftime("%Y-%m"): round(v, 2) for k, v in monthly.items()},
    }