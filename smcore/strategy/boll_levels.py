"""Boll 通道水位计算（止损/止盈/近 20 日收益/流动性/波动率）。

从 fusion.py 抽出的 K 线派生指标，供融合主流程复用同一次拉取的 K 线。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

from smcore.config.defaults import DEFAULT_K, DEFAULT_WINDOW
from smcore.data import fetch_daily_k
from smcore.indicators import calc_bollinger

from .regime_filter import RS_LOOKBACK


def _compute_boll_levels(code: str, as_of_date: Optional[str] = None) -> dict:
    """拉前复权 K 线算 Boll 水位（止损=下轨，止盈=上轨）+ 近 RS_LOOKBACK 日收益率。

    as_of_date: 指定截止日期 YYYYMMDD（默认今天）。用于历史回测/测量时点对齐。
    """
    if as_of_date:
        try:
            end = datetime.strptime(as_of_date, "%Y%m%d").date()
        except (ValueError, TypeError):
            end = date.today()
    else:
        end = date.today()
    start = end - timedelta(days=120)  # 120 天前
    df = fetch_daily_k(code, start, end, adjust="qfq")
    if len(df) < DEFAULT_WINDOW:
        return {}
    boll = calc_bollinger(df, window=DEFAULT_WINDOW, k=DEFAULT_K)
    last = boll.iloc[-1]
    close = pd.to_numeric(df["close"], errors="coerce").dropna()
    ret20 = None
    if len(close) >= RS_LOOKBACK + 1:
        prev = close.iloc[-(RS_LOOKBACK + 1)]
        if prev and not pd.isna(prev):
            ret20 = float(close.iloc[-1]) / float(prev) - 1
    # 信号日成交额（元），用于流动性门槛过滤
    amount = None
    if "amount" in df.columns:
        amt_series = pd.to_numeric(df["amount"], errors="coerce").dropna()
        if len(amt_series) > 0:
            amount = float(amt_series.iloc[-1])
    # 个股近 20 日波动率（日收益 std），供波动率自适应止损使用
    vol20 = None
    if len(close) >= 21:
        dret = close.iloc[-20:].pct_change().dropna()
        if len(dret) >= 5:
            vol20 = float(dret.std())
    return {
        "close": float(last["close"]),
        "lower": float(last["Lower"]) if pd.notna(last.get("Lower")) else None,
        "upper": float(last["Upper"]) if pd.notna(last.get("Upper")) else None,
        "ma20": float(last["MA"]) if pd.notna(last.get("MA")) else None,
        "ret20": ret20,
        "amount": amount,
        "vol20": vol20,
    }
