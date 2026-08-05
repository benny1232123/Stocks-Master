"""多因子打分（第四档改进）：在融合「综合评分」之外，对候选票做个股层面的多因子二次打分。

设计原则（沿用 risk_rules / position_sizing 的配置驱动范式）：
- 全部超参集中在 risk_config.json 的 factor_scoring 块，代码零硬编码、可热更新。
- 因子全部离线安全：读本地 k_data 缓存（stock_data/k_data/{code}_qfq_full.csv），
  不依赖联网；沪深300 缓存缺失时相对强度(RS)自动回退为纯动量，绝不因数据缺失而崩溃。
- 截面 z-score 归一化 → 按配置权重加权 → clamp 到 [-max_bonus, max_bonus]，
  作为「综合评分」的可加增量（单位与综合评分一致，分数点）。

因子清单：
- mom20  : 近 20 日收益率（短期动量）
- mom60  : 近 60 日收益率（中期动量）
- rs     : 相对强度 = 个股 ret20 − 沪深300 ret20（跑赢大盘的部分；缺失回退 ret20）
- vol    : 近 20 日年化波动率（低波动加分 → 配置权重为负向）
- liq    : 近 20 日平均成交额（高流动性加分，规避庄股/难出场）

调用方（fusion.fuse_signals）在 df 构建后、排序前统一调用 compute_factor_scores，
把因子分增量并入「综合评分」；enabled=False 时不调用（向后兼容）。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

from smcore.data import fetch_daily_k

_INDEX_CODE = "000300"  # 沪深300，相对强度基准
_LOOKBACK = 120  # 拉取窗口（天），需覆盖 ret60 + 缓冲


def _parse_date(as_of_date) -> date:
    if isinstance(as_of_date, date):
        return as_of_date
    try:
        return datetime.strptime(str(as_of_date), "%Y%m%d").date()
    except (ValueError, TypeError):
        return date.today()


def _raw_factors(code: str, as_of_date, window: int = 20) -> Optional[dict]:
    """单只票的原始因子值（缺失为 None）。读本地 k_data 缓存，离线安全。"""
    end = _parse_date(as_of_date)
    start = end - timedelta(days=_LOOKBACK)
    try:
        df = fetch_daily_k(code, start, end, adjust="qfq")
    except Exception:
        return None
    if df is None or len(df) < window + 1:
        return None
    close = pd.to_numeric(df["close"], errors="coerce").dropna()
    if len(close) < window + 1:
        return None
    mom20 = float(close.iloc[-1]) / float(close.iloc[-(window + 1)]) - 1.0
    mom60 = (
        float(close.iloc[-1]) / float(close.iloc[-(60 + 1)]) - 1.0
        if len(close) >= 61
        else None
    )
    dret = close.iloc[-window:].pct_change().dropna()
    vol = float(dret.std() * np.sqrt(252)) if len(dret) >= 5 else None
    liq = None
    if "amount" in df.columns:
        a = pd.to_numeric(df["amount"], errors="coerce").dropna()
        if len(a) >= window:
            liq = float(a.iloc[-window:].mean())
    return {"mom20": mom20, "mom60": mom60, "vol": vol, "liq": liq}


def _index_ret20(as_of_date, window: int = 20) -> Optional[float]:
    """沪深300 近 window 日收益；缓存缺失/异常返回 None（调用方回退为纯动量）。"""
    end = _parse_date(as_of_date)
    start = end - timedelta(days=_LOOKBACK)
    try:
        df = fetch_daily_k(_INDEX_CODE, start, end, adjust="qfq")
    except Exception:
        return None
    if df is None or len(df) < window + 1:
        return None
    close = pd.to_numeric(df["close"], errors="coerce").dropna()
    if len(close) < window + 1:
        return None
    prev = close.iloc[-(window + 1)]
    if prev is None or pd.isna(prev) or prev == 0:
        return None
    return float(close.iloc[-1]) / float(prev) - 1.0


def compute_factor_scores(codes, as_of_date, params: dict) -> dict:
    """对候选票做多因子截面打分，返回 {code: 因子分增量(float)}。

    params 字段（来自 compute_factor_scoring_params）：
      w_momentum_20 / w_momentum_60 / w_rel_strength / w_volatility / w_liquidity
        各因子权重（volatility 取负向=低波动加分）；0 表示该因子不参与。
      scale   : 加权 z-score 总分的最终放大系数
      max_bonus: 单票因子分上下限绝对值（clamp）

    纯数据驱动：缺失因子值的票不参与该因子均值/std 计算，其该项贡献为 0。
    """
    codes = [str(c).strip() for c in codes if c]
    raw: dict[str, Optional[dict]] = {}
    for c in codes:
        raw[c] = _raw_factors(c, as_of_date)

    idx_ret = _index_ret20(as_of_date)
    for c, f in raw.items():
        if not f:
            continue
        f["rs"] = (f["mom20"] - idx_ret) if (idx_ret is not None and f["mom20"] is not None) else f["mom20"]

    weights = {
        "mom20": float(params.get("w_momentum_20", 0.0)),
        "mom60": float(params.get("w_momentum_60", 0.0)),
        "rs": float(params.get("w_rel_strength", 0.0)),
        "vol": float(params.get("w_volatility", 0.0)),
        "liq": float(params.get("w_liquidity", 0.0)),
    }
    scale = float(params.get("scale", 4.0))
    max_bonus = float(params.get("max_bonus", 15.0))

    accum = {c: 0.0 for c in codes}
    for key, w in weights.items():
        if w == 0:
            continue
        vals = np.array(
            [raw[c][key] for c in codes if raw[c] and raw[c].get(key) is not None],
            dtype=float,
        )
        if len(vals) < 2:
            continue
        mu = float(vals.mean())
        sd = float(vals.std())
        if sd <= 0:
            continue
        for c in codes:
            f = raw[c]
            if not f or f.get(key) is None:
                continue
            accum[c] += w * ((f[key] - mu) / sd)

    return {c: float(min(max_bonus, max(-max_bonus, scale * s))) for c, s in accum.items()}
