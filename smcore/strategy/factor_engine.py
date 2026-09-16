#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""纯价格因子的共享计算引擎：数据准备 + 因子构造 + 横截面 IC 原语。

单源约定：``scripts/factor_ic_replay.py``（预注册 v1 历史 IC 回放）与
``scripts/mine_factors.py``（因子自动挖掘）共用本模块，避免「坏柱标记 / 有效域 /
前向收益 / 横截面 IC」这类细节出现两份实现静默分叉（factor_timing 的教训：
两条路径各自实现同一算法 → 迟早分叉且无人察觉）。

⚠️ 本模块只读 k_data parquet，刻意不走 ``smcore/data/kline.py`` —— 其读路径会触发
``write_kline_cache``（kline.py 检测到断层时 force_refresh 全历史重拉）。回放只读不写。

预注册常数与 8 个基线因子定义整体在此声明（2026-09-15 与用户约定），**数值与 v1 完全一致**。
改动其中任何一项即视为新一轮预注册，须在报告中注明。
"""
from __future__ import annotations

import glob
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from smcore.config.defaults import STOCK_DATA_DIR
except Exception:  # pragma: no cover
    STOCK_DATA_DIR = Path(__file__).resolve().parents[2] / "stock_data"

ROOT = Path(__file__).resolve().parents[2]
STOCK_DATA_DIR = Path(STOCK_DATA_DIR)
KDATA_DIR = STOCK_DATA_DIR / "k_data"
OUT_DIR = STOCK_DATA_DIR / "factor_ic_replay"

# ── 预注册常数（v1，禁随意改动；改 = 新一轮并注明）────────────────────────
LOAD_START = "2017-06-01"    # 预热缓冲起点
WINDOW_START = "2018-01-01"  # 结论窗口起点
WARMUP_WIN = 120             # 预热观察窗（自然日 → 交易日数）
WARMUP_MIN = 60              # 窗内最少有效收盘数
PRICE_FLOOR = 2.0            # 剔低价股
FWD = 10                     # 前向持有交易日
TOP_N = 50                   # 单因子组合持仓数
REBAL_EVERY = 10             # 非重叠换仓间隔（交易日）
COST_RT = 0.003              # 往返成本 0.3%
MIN_N_DAY = 300              # 当日有效横截面下限
ROLL_MIN = 15                # 滚动统计 min_periods（容忍短停牌）
BAD_TOL = 0.002              # 坏柱判定容差

# 8 个预注册纯价格因子 -> (经济先验方向 +1=做多高值 / -1=做多低值, 回看窗)
PRICE_FACTORS: dict[str, tuple[int, int]] = {
    "mom20": (+1, 20),
    "mom60": (+1, 60),
    "rev5": (-1, 5),
    "vol20": (-1, 20),
    "amp20": (-1, 20),
    "liq20": (-1, 20),
    "illiq20": (+1, 20),
    "dist20": (-1, 20),
}
FIRST_FACTOR = next(iter(PRICE_FACTORS))  # dict 保序，取第一个作为「有效日」代表

# 有效域所需的坏柱回看窗（与因子值回看窗不同：mom60 需 60 日干净窗口，其余压到 ≤20）
FACTOR_LOOKBACK_CAP = 20


def baseline_lookback(name: str) -> int:
    """基线因子有效域所需的坏柱回看窗（交易日）。

    与 v1 完全一致：mom60 需 60 日干净窗口，其余压到 ≤FACTOR_LOOKBACK_CAP
    （长回看窗会把有效样本压得过少，8 个基线里只有 mom60 值 60）。
    """
    lb = PRICE_FACTORS[name][1]
    return lb if name == "mom60" else min(lb, FACTOR_LOOKBACK_CAP)


def roll_min(w: int, cap: int = ROLL_MIN) -> int:
    """滚动统计的 min_periods：短窗按窗长放宽，长窗恒为 ROLL_MIN。

    v1 只用 ≥20 的窗口，故 min_periods 恒 = ROLL_MIN(15)。文法引入了 5/10 的短窗，
    若仍用 15 会直接抛 ``min_periods 15 must be <= window 10``（曾静默跳过 29 个候选）。
    改为 ``min(w, ROLL_MIN)``：
    - w ≥ 15 → 15，**与 v1 完全一致**，8 个预注册基线数值不变；
    - w <  15 → w，短窗按窗长要求样本（仍只看过去 w 期，不引入未来信息）。
    """
    return max(1, min(int(w), int(cap)))


# ── 数据读取 ────────────────────────────────────────────────────────────
def load_matrices(cols: tuple[str, ...] = ("close", "high", "low", "amount"),
                  kdata_dir: Path | None = None,
                  load_start: str | None = LOAD_START) -> dict[str, pd.DataFrame]:
    """批量直读 parquet 桶 → {列名: 宽表矩阵（index=date, columns=code）}。"""
    kd = Path(kdata_dir) if kdata_dir else KDATA_DIR
    files = sorted(glob.glob(str(kd / "qfq_*.parquet")))
    if not files:
        raise SystemExit(f"no parquet under {kd}")
    frames = []
    for f in files:
        d = pd.read_parquet(f)
        d["date"] = pd.to_datetime(d["date"])
        d["code"] = d["code"].astype("category")
        if load_start:
            d = d[d["date"] >= pd.Timestamp(load_start)]
        frames.append(d)
        del d
    df = pd.concat(frames, ignore_index=True).drop_duplicates(["code", "date"])
    del frames
    df = df.sort_values(["date", "code"])
    mats: dict[str, pd.DataFrame] = {}
    for col in cols:
        mats[col] = df.pivot(index="date", columns="code", values=col).sort_index()
    del df
    return mats


# ── 掩码 ────────────────────────────────────────────────────────────────
def limit_series(codes) -> pd.Series:
    """板内涨跌停幅度（近似）：创业板/科创板 20%，北交所 30%，其余 10%。"""
    def lim(c: str) -> float:
        c = str(c)
        if c.startswith(("300", "301", "688")):
            return 0.20
        if c.startswith(("43", "83", "87", "92")):
            return 0.30
        return 0.10
    return pd.Series([lim(c) for c in codes], index=codes)


def daily_returns_and_bad(close: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """日收益 + 坏柱标记（超板内涨跌停限制×1.005+0.002 视为坏柱，hithink 深历史跳变）。"""
    ret1 = close.pct_change(fill_method=None)
    lim = limit_series(close.columns)
    lim_mat = pd.DataFrame(np.tile(lim.values, (close.shape[0], 1)),
                           index=close.index, columns=close.columns)
    bad = (ret1.abs() > lim_mat * 1.005 + BAD_TOL).fillna(0.0)
    return ret1, bad


def lookback_bad(bad: pd.DataFrame, w: int, cache: dict | None = None) -> pd.DataFrame:
    """回看窗 w（含当日）内是否出现坏柱。``cache`` 为 {w: DataFrame} 时按窗缓存。"""
    if cache is not None and w in cache:
        return cache[w]
    acc = bad.copy()
    for k in range(1, w):
        acc = np.maximum(acc, bad.shift(k, fill_value=0.0))
    if cache is not None:
        cache[w] = acc
    return acc


def forward_bad_mask(bad: pd.DataFrame, horizon: int = FWD) -> pd.DataFrame:
    """前向窗 t..t+horizon 内是否出现坏柱。

    ⚠️ **含当日 t**（与 v1 完全一致：``bad.copy()`` 起手再叠加 shift(-1..-horizon)）。
    等价于「入场当天或持有期内有坏柱 → 该样本作废」，比 t+1.. 起算更保守。
    """
    fwd_bad = bad.copy()
    for k in range(1, horizon + 1):
        fwd_bad = np.maximum(fwd_bad, bad.shift(-k, fill_value=0.0))
    return fwd_bad


def base_valid_mask(close: pd.DataFrame) -> pd.DataFrame:
    """基础有效域：非空 + 价格下限 + 预热期通过。"""
    warm = close.rolling(WARMUP_WIN, min_periods=1).count() >= WARMUP_MIN
    return (close.notna()) & (close >= PRICE_FLOOR) & warm


def forward_return_matrix(close: pd.DataFrame, fwd_bad: pd.DataFrame,
                          base_valid: pd.DataFrame, horizon: int = FWD) -> pd.DataFrame:
    """T+horizon 前向收益，污染窗口与无效域置 NaN。"""
    return (close.shift(-horizon) / close - 1).where((fwd_bad == 0) & base_valid)


def forward_rank_matrix(fwd: pd.DataFrame) -> pd.DataFrame:
    """前向收益的横截面百分位秩（Spearman IC 用）。"""
    return fwd.rank(axis=1, pct=True)


# ── 因子构造 ────────────────────────────────────────────────────────────
def build_price_factors(close, high, low, amount, ret1) -> dict[str, pd.DataFrame]:
    """8 个预注册纯价格因子的原始值（未加有效域掩码）。"""
    ma20 = close.rolling(20, min_periods=ROLL_MIN).mean()
    amt20 = amount.rolling(20, min_periods=ROLL_MIN).mean().where(lambda x: x > 0)
    amt_pos = amount.where(amount > 0)
    return {
        "mom20": close / close.shift(20) - 1,
        "mom60": close / close.shift(60) - 1,
        "rev5": close / close.shift(5) - 1,
        "vol20": ret1.rolling(20, min_periods=ROLL_MIN).std(),
        "amp20": ((high - low) / close).rolling(20, min_periods=ROLL_MIN).mean(),
        "liq20": np.log(amt20),
        "illiq20": (ret1.abs() / amt_pos).rolling(20, min_periods=ROLL_MIN).mean(),
        "dist20": close / ma20 - 1,
    }


def apply_factor_validity(fac: dict[str, pd.DataFrame], base_valid: pd.DataFrame,
                          bad: pd.DataFrame, lookbacks: dict[str, int] | None = None,
                          cache: dict | None = None
                          ) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """按有效域（基础域 + 因子非空 + 回看窗无坏柱）掩码因子；返回 (掩码后因子, 有效域)。

    ``lookbacks`` 指定各因子的坏柱回看窗；未指定则回落到「基线规则」（mom60→60，其余 ≤20），
    与 v1 对长回看窗的近似一致（长窗全要求干净会把样本压得过少）。
    """
    lb_cache = cache if cache is not None else {}
    out: dict[str, pd.DataFrame] = {}
    valid: dict[str, pd.DataFrame] = {}
    for name, df in fac.items():
        w = (lookbacks or {}).get(name)
        if w is None:
            w = baseline_lookback(name) if name in PRICE_FACTORS else FACTOR_LOOKBACK_CAP
        m = base_valid & df.notna() & (lookback_bad(bad, int(w), lb_cache) == 0)
        valid[name] = m
        out[name] = df.where(m)
    return out, valid


# ── 横截面 IC ───────────────────────────────────────────────────────────
def cross_sectional_ic(fac: pd.DataFrame, fwd_rank: pd.DataFrame,
                       min_n_day: int = MIN_N_DAY) -> pd.Series:
    """每日横截面 Spearman IC（百分位秩上的 Pearson，向量化）。样本不足 → NaN 并剔除。"""
    r = fac.rank(axis=1, pct=True)
    m = r.notna() & fwd_rank.notna()
    n = m.sum(axis=1)
    a, b = r.where(m), fwd_rank.where(m)
    am, bm = a.mean(axis=1), b.mean(axis=1)
    cov = (a * b).mean(axis=1) - am * bm
    va = (a * a).mean(axis=1) - am * am
    vb = (b * b).mean(axis=1) - bm * bm
    ic = cov / np.sqrt(np.maximum(va * vb, 1e-24))
    ic[n < min_n_day] = np.nan
    return ic


# ── 分位组合（单因子 TOP_N 多空/多头，逐日横截面）─────────────────────
def decile_means(fac: pd.DataFrame, fwd: pd.DataFrame, mask: pd.DataFrame,
                 days, n_decile: int = 10, min_n_day: int = MIN_N_DAY
                 ) -> tuple[list[float], int]:
    """在给定换仓日上按因子分位聚合前向收益均值（低→高），返回 (各分位均值, 有效日数)。

    用于「整体排序效应 vs 极端尾部毒性」诊断：Spearman 由截面身体主导，
    TOP 分位却是极端尾部，A 股尾部聚集 crash / 涨跌停锁死股，二者可能反向。
    """
    buckets: list[list[float]] = [[] for _ in range(n_decile)]
    used = 0
    for d in days:
        col = fac.loc[d]
        cand = col[mask.loc[d] & col.notna()].dropna()
        if len(cand) < min_n_day:
            continue
        q = pd.qcut(cand.rank(method="first"), n_decile, labels=False) + 1
        fr = fwd.loc[d].reindex(q.index).dropna()
        if fr.empty:
            continue
        used += 1
        for g, vals in fr.groupby(q.loc[fr.index]):
            buckets[int(g) - 1].append(float(vals.mean()))
    return [float(np.mean(x)) if x else float("nan") for x in buckets], used
