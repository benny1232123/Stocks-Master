"""仓位分配与风险中性化（单名上限 / 行业分散 / 组合 β 软约束）。

从 fusion.py 抽出的「仓位稀释修复 + 风险层」职责。β 估计读本地 k_data + 沪深300，
零联网；缺数据回退 BETA_FALLBACK，不阻断清单生成。
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

from smcore.config.defaults import (
    BETA_FALLBACK,
    BETA_MIN_KEEP,
    BETA_WINDOW,
    STOCK_DATA_DIR,
)
from smcore.utils.code import format_stock_code

from .regime_filter import _get_hs300_close


def _apply_strategy_cap(df: pd.DataFrame, max_per: int) -> pd.DataFrame:
    """最终名单按策略分散：每个策略最多保留 max_per 只（取已排序的前 max_per）。

    避免单策略（如 CCTV）占满最终名单导致回测池同质化、自适应策略权重失效。
    多策略命中票按「来源策略」首个策略归属（与仓位分配取最高权重策略口径近似一致）。
    """
    if max_per <= 0 or "来源策略" not in df.columns or df.empty:
        return df
    keep = []
    cnt: dict[str, int] = {}
    for _, r in df.iterrows():
        parts = [s.strip().lower() for s in str(r.get("来源策略", "")).replace("/", "，").split("，") if s.strip()]
        owner = parts[0] if parts else "__none__"
        c = cnt.get(owner, 0)
        if c < max_per:
            keep.append(True)
            cnt[owner] = c + 1
        else:
            keep.append(False)
    return df[keep].reset_index(drop=True)


# ── 组合 β 软约束（风险中性化：组合对沪深300 的暴露不要过高）─────────────
def _estimate_betas(codes, as_of_yyyymmdd: str, window: int = BETA_WINDOW) -> dict[str, float]:
    """估计候选股对沪深300 的 β（本地 k_data + 沪深300 序列，零联网）。

    用近 window 个交易日的个股日收益与沪深300 日收益对齐，β = cov/var。
    个股缺本地 k_data 时回退 BETA_FALLBACK（中性 1.0），不阻断清单生成。

    Returns:
        {code(6位): beta}
    """
    idx = _get_hs300_close()
    out: dict[str, float] = {}
    if idx is None:
        return {format_stock_code(c): BETA_FALLBACK for c in codes}
    try:
        target = pd.Timestamp(as_of_yyyymmdd)
        idx_prior = idx.loc[:target]
        if len(idx_prior) < 2:
            return {format_stock_code(c): BETA_FALLBACK for c in codes}
        idx_series = idx_prior.tail(window + 1)
        idx_ret = idx_series.pct_change().dropna()
    except Exception:
        return {format_stock_code(c): BETA_FALLBACK for c in codes}

    for c in codes:
        c6 = format_stock_code(c)
        if not c6:
            continue
        try:
            p = STOCK_DATA_DIR / "k_data" / f"{c6}_qfq_full.csv"
            if not p.exists():
                out[c6] = BETA_FALLBACK
                continue
            d = pd.read_csv(p)
            if "date" not in d.columns or "close" not in d.columns or len(d) < 3:
                out[c6] = BETA_FALLBACK
                continue
            d["date"] = pd.to_datetime(d["date"], errors="coerce")
            d = d.dropna(subset=["date"]).set_index("date").sort_index()
            close = pd.to_numeric(d["close"], errors="coerce").dropna()
            close = close.loc[:target].tail(window + 1)
            sret = close.pct_change().dropna()
            joined = pd.concat([sret.rename("s"), idx_ret.rename("i")], axis=1, join="inner").dropna()
            if len(joined) < 10:
                out[c6] = BETA_FALLBACK
                continue
            cov = joined["s"].cov(joined["i"])
            var = joined["i"].var()
            out[c6] = float(cov / var) if var and var > 0 else BETA_FALLBACK
        except Exception:
            out[c6] = BETA_FALLBACK
    return out


def _portfolio_beta(df, betas: dict[str, float]) -> float:
    """按 建议仓位% 加权计算组合 β。缺 β 的票按 BETA_FALLBACK 计。"""
    if df is None or df.empty or "建议仓位%" not in df.columns:
        return BETA_FALLBACK
    tot = float(df["建议仓位%"].sum())
    if tot <= 0:
        return BETA_FALLBACK
    wsum = 0.0
    for _, r in df.iterrows():
        b = betas.get(format_stock_code(r["股票代码"]), BETA_FALLBACK)
        wsum += float(r.get("建议仓位%") or 0.0) * b
    return wsum / tot


def _apply_position_sizing(
    df,
    weights: dict[str, float],
    surv: dict[str, int],
    total_capital: float,
    max_single_weight_frac: float,
) -> tuple[pd.DataFrame, int]:
    """按「命中策略中权重最高者 / 该策略最终存活票数」重算每只票的建议仓位%与金额。

    这是风险层最后一道集中度闸：单名仓位不得超过 max_single_weight_frac
    （单名仓位上限，与行业权重上限、组合 β 上限互补——后两者管不到「单只票吃光仓位」）。
    返回 (df, n_hit)：df 已就地写入「建议仓位%」「建议金额」列；n_hit 为被上限截断的只数。
    不修改传入 df 之外的状态。
    """
    if df is None or df.empty:
        return df, 0
    new_pct, new_amt = [], []
    n_hit = 0
    for _, r in df.iterrows():
        hits = [
            s.strip().lower()
            for s in str(r.get("来源策略", "")).replace("/", "，").split("，")
            if s.strip()
        ]
        best = 0.0
        for s in hits:
            w = weights.get(s, 0)
            c = max(surv.get(s, 1), 1)
            share = w / c
            if share > best:
                best = share
        p = min(best / 100.0, max_single_weight_frac)
        if best / 100.0 > max_single_weight_frac + 1e-9:
            n_hit += 1
        new_pct.append(round(p * 100, 1))
        new_amt.append(round(total_capital * p, 0))
    df = df.copy()
    df["建议仓位%"] = new_pct
    df["建议金额"] = new_amt
    return df, n_hit


def _apply_beta_cap(df, betas: dict[str, float], max_beta: float, min_keep: int = BETA_MIN_KEEP):
    """组合 β 超上限时，逐步剔除当前 β 最高的个股，直到 ≤ max_beta 或只剩 min_keep 只。

    返回 (trimmed_df, n_trimmed)。不修改传入 df。
    """
    if df is None or df.empty:
        return df, 0
    work = df.copy()
    n_trimmed = 0
    while len(work) > min_keep:
        pb = _portfolio_beta(work, betas)
        if pb <= max_beta:
            break
        # 找当前 β 最高者剔除（降 β 最快）
        best_idx = None
        best_b = float("-inf")
        for idx, r in work.iterrows():
            b = betas.get(format_stock_code(r["股票代码"]), BETA_FALLBACK)
            if b > best_b:
                best_b = b
                best_idx = idx
        if best_idx is None:
            break
        work = work.drop(index=best_idx).reset_index(drop=True)
        n_trimmed += 1
    return work, n_trimmed
