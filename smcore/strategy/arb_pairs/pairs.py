"""统计配对套利核心逻辑。

流水线：加载收盘价宽表 → 流动性选股 → 相关性预筛 → 协整检验(Engle-Granger) → 残差 z 均值回复回测。

所有阈值走函数参数（不写死在逻辑里），便于 walk-forward 调参与 CI 接入。
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import coint

from smcore.config.defaults import STOCK_DATA_DIR

# ── 板别前缀（A 股）────────────────────────────────────────────────────
# 主板：沪 600/601/603/605，深 000/001/002/003（002 中小板 2021 并入主板）。
# 排除：创业板 300/301、科创板 688/689、北交所 43/83/87/92。用户「只玩主板」→ 默认主板。
MAIN_BOARD_PREFIXES = ("600", "601", "603", "605", "000", "001", "002", "003")


def is_main_board(code: str) -> bool:
    return str(code)[:3] in MAIN_BOARD_PREFIXES

# ── 数据读取（复用 factor_engine 的 qfq 宽表）─────────────────────────────
def load_closes(load_start: str = "2024-01-01") -> pd.DataFrame:
    from smcore.strategy import factor_engine as fe

    return fe.load_matrices(cols=("close",), load_start=load_start)["close"]


def load_amounts(load_start: str = "2024-01-01") -> pd.DataFrame:
    from smcore.strategy import factor_engine as fe

    return fe.load_matrices(cols=("amount",), load_start=load_start)["amount"]


def load_sector_map(path=None) -> dict:
    """{code: 行业名}，用于行业内配对（经济更合理、减少伪相关）。"""
    if path is None:
        path = Path(STOCK_DATA_DIR) / "sector_map.json"
    try:
        return json.load(open(path, encoding="utf-8"))
    except Exception:
        return {}


# ── 1. 流动性选股 ────────────────────────────────────────────────────────
def liquid_universe(
    closes: pd.DataFrame,
    amounts: pd.DataFrame,
    top_n: int = 300,
    min_obs: int = 400,
    boards: tuple[str, ...] | None = None,
) -> list[str]:
    """按中位日成交额取最流动的 top_n 只，且要求收盘价非空天数 ≥ min_obs（保证样本量）。

    boards：前缀白名单（如前 3 位）。传 MAIN_BOARD_PREFIXES 即只保留主板（用户「只玩主板」）。
    """
    liq = amounts.median().sort_values(ascending=False)
    codes = [c for c in liq.index if closes[c].notna().sum() >= min_obs]
    if boards is not None:
        bs = tuple(boards)
        codes = [c for c in codes if str(c)[:3] in bs]
    return list(codes[:top_n])


# ── 2. 相关性预筛 ────────────────────────────────────────────────────────
def candidate_pairs(
    closes: pd.DataFrame,
    codes: list[str],
    min_corr: float = 0.85,
    max_pairs: int | None = 2000,
    same_sector: bool = False,
    sector_map: dict | None = None,
    max_corr: float | None = None,
) -> list[tuple[str, str, float]]:
    """日收益相关系数 ≥ min_corr 的候选对（对角以上，避免重复）。返回 [(a,b,corr), ...] 按相关降序。

    same_sector=True：仅保留同行业对（需 sector_map），经济更合理、剔除伪相关。
    max_corr：剔除「克隆对」——相关高于此值视为同一资产的复制品（如多只沪深300ETF、
    黄金ETF 互配），其价差只是跟踪误差噪声、非有经济意义的均值回复，回测纯亏还吃摩擦。
    """
    ret = closes[codes].ffill().pct_change(fill_method=None).dropna()
    corr = ret.corr().abs()
    if same_sector:
        if sector_map is None:
            sector_map = load_sector_map()
        def insec(a, b):
            sa, sb = sector_map.get(a), sector_map.get(b)
            return sa is not None and sa == sb
    pairs: list[tuple[str, str, float]] = []
    for i, a in enumerate(codes):
        for b in codes[i + 1:]:
            if same_sector and not insec(a, b):
                continue
            v = corr.loc[a, b]
            if pd.notna(v) and v >= min_corr and (max_corr is None or v <= max_corr):
                pairs.append((a, b, float(v)))
    pairs.sort(key=lambda x: -x[2])
    if max_pairs is not None:
        pairs = pairs[:max_pairs]
    return pairs


# ── 3. 协整检验（Engle-Granger）+ 均值回复半衰期过滤 ──────────────────────
def _halflife(spread: pd.Series) -> float | None:
    """OU 均值回复半衰期（交易日）：AR(1) Δs = a + b·s_{t-1}，hl = -ln2/ln(1+b)。

    b>=0（不回复）或样本不足 → None。半衰期越短回复越快；太长则价差趋势性强、易被拖死。
    """
    s = pd.Series(spread).dropna()
    if len(s) < 40:
        return None
    lag = s.shift(1)
    ds = (s - lag)
    df = pd.concat([ds, lag], axis=1).dropna()
    df.columns = ["ds", "lag"]
    if len(df) < 30:
        return None
    b, _a = np.polyfit(df["lag"].values, df["ds"].values, 1)
    b = float(b)
    if b >= 0 or (1.0 + b) <= 0:
        return None
    return float(-np.log(2.0) / np.log(1.0 + b))


def cointegration_filter(
    closes: pd.DataFrame,
    pairs: list[tuple[str, str, float]],
    p_thresh: float = 0.05,
    min_obs: int = 120,
    hl_min: float = 2.0,
    hl_max: float = 40.0,
) -> list[tuple[str, str, float, float, float]]:
    """EG 双向协整 + 半衰期过滤。返回 [(a,b,corr,p,halflife), ...]。

    半衰期过滤是配对质量硬门：hl 落在 [hl_min, hl_max] 才留——剔除「不回复 / 回复太慢（趋势性）」
    的伪配对（宽基跨指数对常见此病）。hl_max=None 关闭上限。
    """
    out: list[tuple[str, str, float, float, float]] = []
    for a, b, corr in pairs:
        ya = closes[a].dropna()
        xb = closes[b].dropna()
        joined = pd.concat([ya, xb], axis=1).dropna()
        if len(joined) < min_obs:
            continue
        y = joined.iloc[:, 0].values.astype(float)
        x = joined.iloc[:, 1].values.astype(float)
        try:
            # EG 检验双向（y~x 与 x~y），取更显著一侧——协整是对称性质，单向检验会漏。
            _t, p1, _ = coint(y, x)
            _t, p2, _ = coint(x, y)
            p = min(p1, p2)
        except Exception:
            continue
        if p >= p_thresh:
            continue
        # 半衰期：用全样本 OLS beta 构价差 spread = y - beta*x
        beta = float(np.polyfit(x, y, 1)[0])
        hl = _halflife(pd.Series(y - beta * x))
        if hl is None:
            continue
        if hl < hl_min or (hl_max is not None and hl > hl_max):
            continue
        out.append((a, b, corr, float(p), hl))
    return out


# ── 4. 单对回测（残差 z 均值回复）──────────────────────────────────────
def backtest_pair(
    closes: pd.DataFrame,
    a: str,
    b: str,
    window: int = 120,
    entry: float = 2.0,
    exit_z: float = 0.3,
    stop: float = 3.0,
    long_only: bool = False,
) -> dict:
    """滚动 beta 拟合价差 spread = y - beta*x；z = (spread - rolling_mu)/rolling_sigma。

    - z > entry：价差偏贵 → 做空价差（空 y、多 x）
    - z < -entry：价差偏便宜 → 做多价差（多 y、空 x）
    - |z| < exit_z：平仓
    - |z| > stop：强制平仓（防扩散）
    long_only=True：只做「便宜时做多价差」一侧（A 股个股难做空的可行变体），贵时不动。

    返回每日价差 pnl（标记盯市，单位价差收益），及基础统计。
    """
    df = pd.concat([closes[a], closes[b]], axis=1).dropna()
    df.columns = ["y", "x"]
    if len(df) <= window + 5:
        return {"a": a, "b": b, "ok": False, "reason": "样本不足"}
    y = df["y"].values.astype(float)
    x = df["x"].values.astype(float)
    n = len(df)
    spread = np.full(n, np.nan)
    for t in range(window, n):
        beta, _, _, _ = np.linalg.lstsq(x[t - window:t, None], y[t - window:t], rcond=None)
        beta = float(beta[0]) if beta.size else 1.0
        spread[t] = y[t] - beta * x[t]
    mu = pd.Series(spread).rolling(window).mean().values
    sd = pd.Series(spread).rolling(window).std().values
    pos = np.zeros(n)
    for t in range(window, n):
        if not np.isfinite(spread[t]) or not np.isfinite(mu[t - 1]) or not np.isfinite(sd[t - 1]) or sd[t - 1] <= 0:
            pos[t] = pos[t - 1]
            continue
        z = (spread[t] - mu[t - 1]) / sd[t - 1]
        cur = pos[t - 1]
        if cur == 0:
            if z > entry:
                cur = -1.0
            elif z < -entry:
                if not long_only:
                    cur = 1.0
                else:
                    cur = 1.0  # 仅多头：便宜侧做多价差
        else:
            if z > -exit_z and z < exit_z:
                cur = 0.0
            elif abs(z) > stop:
                cur = 0.0
            elif long_only and z > entry:
                # 仅多头模式下，做多价差后若价差变贵（z 转正过 entry）也平
                cur = 0.0
        pos[t] = cur
    pnl = np.zeros(n)
    for t in range(window + 1, n):
        prev = spread[t - 1]
        # 价差收益率（量纲无关）：pos=+1 做多价差，pos=-1 做空价差
        pnl[t] = pos[t - 1] * ((spread[t] / prev - 1.0) if prev != 0 else 0.0)
    pnl = pd.Series(pnl, index=df.index)
    active = pos != 0
    return {
        "a": a,
        "b": b,
        "ok": True,
        "long_only": long_only,
        "pnl": pnl,
        "pos": pd.Series(pos, index=df.index),
        "n_days": int(active.sum()),
        "mean_pnl": float(pnl[pnl != 0].mean()) if (pnl != 0).any() else 0.0,
        "total_pnl": float(pnl.sum()),
        "hit": float((pnl[pnl != 0] > 0).mean()) if (pnl != 0).any() else 0.0,
    }


# ── 5. 组合聚合 ──────────────────────────────────────────────────────────
def backtest_portfolio(
    closes: pd.DataFrame,
    pairs: list[tuple[str, str, float, float]],
    *,
    window: int = 120,
    entry: float = 2.0,
    exit_z: float = 0.3,
    stop: float = 3.0,
    long_only: bool = False,
    max_active: int = 30,
) -> dict:
    """逐对回测后等风险聚合（每日取活跃对 pnl 均值，且活跃对数量 ≤ max_active 防拥挤）。

    返回组合每日 pnl、累计收益、夏普、胜率、最大回撤。
    """
    pnls = []
    for a, b, *_ in pairs:
        r = backtest_pair(closes, a, b, window=window, entry=entry,
                          exit_z=exit_z, stop=stop, long_only=long_only)
        if r.get("ok"):
            pnls.append(r)
    if not pnls:
        return {"ok": False, "reason": "无可用对"}
    # 对齐到公共日期
    aligned = pd.concat([p["pnl"] for p in pnls], axis=1)
    aligned.columns = [f"{p['a']}-{p['b']}" for p in pnls]
    port = aligned.mean(axis=1)  # 等风险：每日活跃对均值
    # 配对套利权益 = 累计 P&L 指数（价差收益累加，非复利——价差收益非资本收益，复利会失真）
    eq = port.cumsum()
    ret = port.dropna()
    sharpe = float(ret.mean() / ret.std() * np.sqrt(252)) if ret.std() > 0 else 0.0
    win = float((ret > 0).mean()) if len(ret) else 0.0
    peak = eq.cummax()
    dd = (eq - peak)
    maxdd = float(dd.min())
    return {
        "ok": True,
        "long_only": long_only,
        "n_pairs": len(pnls),
        "n_active_days": int((~port.isna()).sum()),
        "total_return": float(eq.iloc[-1]),
        "sharpe": sharpe,
        "hit_rate": win,
        "max_drawdown": maxdd,
        "equity": eq,
        "per_pair": pnls,
    }


# ── 流水线入口 ──────────────────────────────────────────────────────────
def run_pipeline(
    *,
    load_start: str = "2024-01-01",
    top_n: int = 500,
    min_corr: float = 0.70,
    p_thresh: float = 0.05,
    window: int = 120,
    entry: float = 2.0,
    exit_z: float = 0.3,
    stop: float = 3.0,
    same_sector: bool = False,
    boards: tuple[str, ...] | None = MAIN_BOARD_PREFIXES,
    max_pairs_for_bt: int = 60,
) -> dict:
    """端到端跑一遍，返回含候选对、协整对、长-短与仅多头组合统计的字典。

    boards 默认只留主板（用户「只玩主板」，剔除创业板/科创板/北交所）。
    """
    closes = load_closes(load_start)
    amounts = load_amounts(load_start)
    codes = liquid_universe(closes, amounts, top_n=top_n, boards=boards)
    cands = candidate_pairs(closes, codes, min_corr=min_corr, same_sector=same_sector)
    coint = cointegration_filter(closes, cands, p_thresh=p_thresh)
    ls = backtest_portfolio(closes, coint[:max_pairs_for_bt], window=window,
                            entry=entry, exit_z=exit_z, stop=stop, long_only=False)
    lo = backtest_portfolio(closes, coint[:max_pairs_for_bt], window=window,
                            entry=entry, exit_z=exit_z, stop=stop, long_only=True)
    return {
        "params": dict(top_n=top_n, min_corr=min_corr, p_thresh=p_thresh,
                       window=window, entry=entry, exit_z=exit_z, stop=stop,
                       same_sector=same_sector, boards=list(boards) if boards else None,
                       load_start=load_start, n_codes=len(codes)),
        "n_candidates": len(cands),
        "n_cointegrated": len(coint),
        "cointegrated": [(a, b, round(c, 3), round(p, 4), round(hl, 1)) for a, b, c, p, hl in coint],
        "long_short": ls,
        "long_only": lo,
    }


def run_etf_pipeline(
    *,
    min_corr: float = 0.70,
    max_corr: float = 0.97,
    p_thresh: float = 0.05,
    hl_min: float = 2.0,
    hl_max: float | None = 40.0,
    window: int = 120,
    entry: float = 2.0,
    exit_z: float = 0.3,
    stop: float = 3.0,
    max_pairs_for_bt: int = 40,
) -> dict:
    """ETF 配对流水线（数据来自 smcore.data.etf_kline 的腾讯 ifzq 缓存）。

    ETF 池为精选流动池（无流动性选股步骤，池本身即流动的）。ETF 无个股做空约束那么严
    （不少 ETF 是融券标的、跨境 ETF 支持 T+0），故长-短版的可执行性优于个股。
    max_corr=0.97：剔除同指数克隆对（多只沪深300/黄金 ETF 互配，价差只是跟踪误差噪声）。
    """
    from smcore.data.etf_kline import load_etf_closes

    closes = load_etf_closes()
    codes = [str(c) for c in closes.columns]
    cands = candidate_pairs(closes, codes, min_corr=min_corr, max_corr=max_corr)
    coint = cointegration_filter(closes, cands, p_thresh=p_thresh, hl_min=hl_min, hl_max=hl_max)
    ls = backtest_portfolio(closes, coint[:max_pairs_for_bt], window=window,
                            entry=entry, exit_z=exit_z, stop=stop, long_only=False)
    lo = backtest_portfolio(closes, coint[:max_pairs_for_bt], window=window,
                            entry=entry, exit_z=exit_z, stop=stop, long_only=True)
    return {
        "params": dict(kind="etf", n_etf=len(codes), min_corr=min_corr, max_corr=max_corr,
                       p_thresh=p_thresh, window=window, entry=entry, exit_z=exit_z, stop=stop),
        "n_candidates": len(cands),
        "n_cointegrated": len(coint),
        "cointegrated": [(a, b, round(c, 3), round(p, 4), round(hl, 1)) for a, b, c, p, hl in coint],
        "long_short": ls,
        "long_only": lo,
    }
