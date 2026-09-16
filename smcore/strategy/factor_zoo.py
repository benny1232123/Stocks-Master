#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""因子池（预注册文法）：候选枚举 + 统计检验 + 筛选闸门 + 报告渲染。

动机：此前「因子」只有 5 个策略，无法回答「到底哪些因子真有横截面预测力」。
本模块把因子从「手挑几个」扩展为**可枚举、可审计、可自动挖掘**的因子池。

预注册纪律（2026-09-16 与用户约定；改动任一项 = 新一轮，须在报告中注明）
--------------------------------------------------------------------------------
1. **原料**：k_data 前复权 OHLCV + 成交额 → 纯价格因子，零 PIT 风险（无公告日歧义）。
2. **文法**：原语 × 模板 × 窗口，**确定性枚举**（无随机、不扫参、不选优）；
   候选数与顺序由 TEMPLATES 表唯一确定；每个模板自带**经济先验方向**。
3. **样本切分**：发现集 WINDOW_START ~ SPLIT_END，验证集 SPLIT_END+1 ~ 至今。
   ⚠️ 排名、筛选与显著性**只看验证集**；发现集只用于「两段符号是否一致」的一致性检查。
4. **显著性**：验证集 IC 按**非重叠**取样（每 REBAL_EVERY 个有效 IC 日取 1）再做 t 检验——
   T+10 前向收益互相重叠会让日 IC 高度自相关，朴素 t 值虚高约 √10 倍，必须扣掉。
   单侧与否：用**双侧** p（方向可能被证伪），再做多重检验校正（BH-FDR + Bonferroni 双报）。
5. **稳定性**：分年 IC 同号率 ≥ STABLE_FRAC，且发现集/验证集均值 IC 同号。
6. **冗余剔重**：候选两两按验证集 IC 序列；|ρ| > REDUNDANCY_RHO 判为冗余，只保留一个。
   保留顺序 = **锚优先（8 个预注册基线必须留在对比集内），其余按 |ICIR| 降序**——
   因此可能出现「|ICIR| 更高的近似复制品被较低的锚剔掉」（如 vol5 被 vol20 剔）。
   这是刻意选择：保证存活清单始终含 v1 基线的可比参照；被剔者仍留在全排名表内可查。
   冗余 ≠ 失败：与保留者 IC 几乎等价，可由保留者代表。
7. **先验方向被证伪的处理**：IC 显著但与先验反向 → 裁决为「方向反转」而非「存活」，
   不进入存活清单（需人工复核后另立新轮）。

本模块只做纯计算（不读盘、不写盘），数据与 IO 由 ``scripts/mine_factors.py`` 负责。
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass

import numpy as np
import pandas as pd

from smcore.strategy import factor_engine as fe

# ── 预注册常数 ──────────────────────────────────────────────────────────
MAX_CANDIDATES = 200          # 候选上限（当前文法展开 100 个，未触顶）
SPLIT_END = "2022-12-31"      # 发现集结束；之后为验证集
MIN_OOS_EVAL_DAYS = 40        # 验证集非重叠样本下限（≈1.6 年，10 日一档）
MIN_IC_ABS = 0.010            # 存活所需最小 |均值 IC|
STABLE_FRAC = 2.0 / 3.0       # 分年同号率下限
REDUNDANCY_RHO = 0.85         # IC 序列相关剔重阈值
FDR_Q = 0.10                  # BH-FDR 控制水平
BONFERRONI_ALPHA = 0.05       # Bonferroni 家族错误率

# ── 因子家族标签（报告分组用；按**因子名前缀**索引，不是 kind）──────────
# 注意：mom/rev 的 kind 同为 "chg"，用 kind 索引会把动量与反转混成一个家族，
# 故一律按名字前缀（去掉数字与下划线后的字母部分）归类。
FAMILY = {
    "mom": "动量", "rev": "反转", "vol": "波动", "amp": "振幅",
    "liq": "流动性", "illiq": "非流动性", "distma": "均线乖离",
    "disthi": "距高点", "distlo": "距低点", "drawdown": "回撤",
    "skew": "偏度", "kurt": "峰度", "maxret": "极值日", "minret": "极值日",
    "upfrac": "上涨占比", "vratio": "波动比", "hlspread": "振幅趋势",
    "tsrank": "时序位置", "matrend": "均线趋势", "stoch": "区间位置",
    "volratio": "量能比", "amtratio": "量能比", "cvvol": "成交稳定性",
    "cvamt": "成交稳定性", "pvcorr": "量价相关", "pamtcorr": "量价相关",
    "gap": "跳空", "intraday": "日内收益",
}
_NAME_PREFIX_RE = re.compile(r"^[a-z]+")

# ── 预注册文法表：(计算类型, 先验方向, 参数网格, 命名模板)──────────────────
TEMPLATES: list[tuple[str, int, list, str]] = [
    # 动量 / 反转
    ("chg", +1, [3, 5, 10, 20, 40, 60, 120, 250], "mom{w}"),
    ("chg", -1, [1, 2, 3, 5, 10], "rev{w}"),
    # 波动 / 振幅
    ("vol", -1, [5, 10, 20, 40, 60, 120], "vol{w}"),
    ("amp", -1, [10, 20, 40, 60], "amp{w}"),
    ("volratio2", -1, [(5, 60), (10, 60), (20, 120)], "vratio{w1}_{w2}"),
    ("hlspread", +1, [(5, 20), (5, 60)], "hlspread{w1}_{w2}"),
    # 流动性 / 非流动性
    ("liq", -1, [10, 20, 60], "liq{w}"),
    ("illiq", +1, [5, 10, 20, 60], "illiq{w}"),
    # 均线 / 形态位置
    ("distma", -1, [5, 10, 20, 60, 120, 250], "distma{w}"),
    ("disthi", +1, [10, 20, 60, 120, 250], "disthi{w}"),
    ("distlo", -1, [10, 20, 60], "distlo{w}"),
    ("drawdown", -1, [20, 60, 120], "drawdown{w}"),
    ("matrend", +1, [(5, 20), (5, 60), (10, 20), (20, 60), (20, 120), (60, 120)], "matrend{w1}_{w2}"),
    ("stoch", +1, [20, 60, 120], "stoch{w}"),
    # 高阶矩
    ("skew", -1, [10, 20, 60], "skew{w}"),
    ("kurt", -1, [20, 60], "kurt{w}"),
    ("maxret", -1, [10, 20, 60], "maxret{w}"),
    ("minret", +1, [10, 20, 60], "minret{w}"),
    ("upfrac", +1, [10, 20, 60], "upfrac{w}"),
    # 量能 / 量价
    ("volratio", +1, [(5, 20), (5, 60), (20, 60), (5, 120)], "volratio{w1}_{w2}"),
    ("amtratio", +1, [(5, 20), (5, 60), (20, 60)], "amtratio{w1}_{w2}"),
    ("cvvol", -1, [20, 60], "cvvol{w}"),
    ("cvamt", -1, [20, 60], "cvamt{w}"),
    ("pvcorr", -1, [10, 20, 60], "pvcorr{w}"),
    ("priceamtcorr", +1, [20, 60], "pamtcorr{w}"),
    # 时序位置 / 微观结构
    ("tsrank", +1, [(5, 60), (5, 120), (20, 60), (20, 120), (20, 250), (60, 250)], "tsrank{w1}_{w2}"),
    ("gapmean", -1, [5, 20, 60], "gap{w}"),
    ("intraday", +1, [5, 20, 60], "intraday{w}"),
]


# ── 候选 ────────────────────────────────────────────────────────────────
# 文法因子名 ↔ 预注册基线名（dist20 在文法中名为 distma20，公式同一）
BASELINE_ALIAS: dict[str, str] = {
    "mom20": "mom20", "mom60": "mom60", "rev5": "rev5", "vol20": "vol20",
    "amp20": "amp20", "liq20": "liq20", "illiq20": "illiq20", "distma20": "dist20",
}


def is_baseline(name: str) -> bool:
    """该文法候选是否就是 v1 预注册 8 个基线之一（作为冗余剔重的锚）。"""
    return name in BASELINE_ALIAS


@dataclass(frozen=True)
class Candidate:
    name: str
    kind: str
    prior: int            # +1 做多高值 / -1 做多低值
    params: tuple = ()

    @property
    def family(self) -> str:
        m = _NAME_PREFIX_RE.match(self.name)
        return FAMILY.get(m.group(0) if m else self.kind, self.kind)

    @property
    def lookback(self) -> int:
        """有效域所需坏柱回看窗。

        基线候选走 ``factor_engine.baseline_lookback``（mom60→60，其余 ≤20），
        保证与预注册 v1 逐值一致；其余候选统一压到 ≤FACTOR_LOOKBACK_CAP。
        """
        base = BASELINE_ALIAS.get(self.name)
        if base:
            return fe.baseline_lookback(base)
        return min(int(max(self.params)), fe.FACTOR_LOOKBACK_CAP) if self.params else fe.FACTOR_LOOKBACK_CAP


def enumerate_candidates(max_candidates: int = MAX_CANDIDATES) -> list[Candidate]:
    """按 TEMPLATES 表确定性展开候选（顺序唯一、无随机）。"""
    out: list[Candidate] = []
    for kind, prior, grid, fmt in TEMPLATES:
        for p in grid:
            params = tuple(p) if isinstance(p, (tuple, list)) else (p,)
            kw = {"w": params[0]} if len(params) == 1 else {f"w{i+1}": v for i, v in enumerate(params)}
            out.append(Candidate(name=fmt.format(**kw), kind=kind, prior=prior, params=params))
            if max_candidates and len(out) >= max_candidates:
                return out
    return out


# ── 滚动相关（列内，避免 pandas 两表 rolling.corr 的 n×n 爆炸）──────────
def _roll_corr(x: pd.DataFrame, y: pd.DataFrame, w: int) -> pd.DataFrame:
    """两矩阵**同列**滚动相关。等价于 pandas rolling.corr（总体/样本矩之比相同）。"""
    mn = fe.roll_min(w)
    mx = x.rolling(w, min_periods=mn).mean()
    my = y.rolling(w, min_periods=mn).mean()
    cov = (x * y).rolling(w, min_periods=mn).mean() - mx * my
    sx = x.rolling(w, min_periods=mn).std()
    sy = y.rolling(w, min_periods=mn).std()
    den = (sx * sy).replace(0.0, np.nan)
    return cov / den


def _ts_rank(x: pd.DataFrame, w: int) -> pd.DataFrame:
    """当前值在自身过去 w 期窗口内的百分位（0~1）。"""
    mn = fe.roll_min(w)
    try:
        return x.rolling(w, min_periods=mn).rank(pct=True)
    except AttributeError:  # pragma: no cover - 老 pandas 回退
        lo = x.rolling(w, min_periods=mn).min()
        hi = x.rolling(w, min_periods=mn).max()
        return (x - lo) / (hi - lo).replace(0.0, np.nan)


# ── 因子计算 ────────────────────────────────────────────────────────────
def compute_factor(ctx: dict, cand: Candidate) -> pd.DataFrame:
    """按候选规格计算因子原始值（未加有效域掩码）。

    ctx 需含 close/high/low/open/volume/amount/ret1 宽表。
    """
    kind, p = cand.kind, cand.params
    close, high, low = ctx["close"], ctx["high"], ctx["low"]
    op, vol, amt = ctx["open"], ctx["volume"], ctx["amount"]
    ret1 = ctx["ret1"]
    w = int(p[0]) if p else 20
    m1 = fe.roll_min(w)   # 短窗按窗长放宽，长窗 = ROLL_MIN（与 v1 一致）

    if kind == "chg":
        return close / close.shift(w) - 1
    if kind == "vol":
        return ret1.rolling(w, min_periods=m1).std()
    if kind == "amp":
        return ((high - low) / close).rolling(w, min_periods=m1).mean()
    if kind == "volratio2":
        w2 = int(p[1]); m2 = fe.roll_min(w2)
        return (ret1.rolling(w, min_periods=m1).std()
                / ret1.rolling(w2, min_periods=m2).std().replace(0.0, np.nan))
    if kind == "hlspread":
        w2 = int(p[1]); m2 = fe.roll_min(w2)
        rng = (high - low) / close
        return (rng.rolling(w, min_periods=m1).mean()
                / rng.rolling(w2, min_periods=m2).mean().replace(0.0, np.nan))
    if kind == "liq":
        return np.log(amt.rolling(w, min_periods=m1).mean().where(lambda x: x > 0))
    if kind == "illiq":
        return (ret1.abs() / amt.where(amt > 0)).rolling(w, min_periods=m1).mean()
    if kind == "distma":
        return close / close.rolling(w, min_periods=m1).mean() - 1
    if kind == "disthi":
        return close / high.rolling(w, min_periods=m1).max() - 1
    if kind == "distlo":
        return close / low.rolling(w, min_periods=m1).min() - 1
    if kind == "drawdown":
        return close / close.rolling(w, min_periods=m1).max() - 1
    if kind == "matrend":
        w2 = int(p[1]); m2 = fe.roll_min(w2)
        return (close.rolling(w, min_periods=m1).mean()
                / close.rolling(w2, min_periods=m2).mean().replace(0.0, np.nan) - 1)
    if kind == "stoch":
        rng = high.rolling(w, min_periods=m1).max() - low.rolling(w, min_periods=m1).min()
        return (close - low.rolling(w, min_periods=m1).min()) / rng.replace(0.0, np.nan)
    if kind == "skew":
        return ret1.rolling(w, min_periods=max(m1, 5)).skew()
    if kind == "kurt":
        return ret1.rolling(w, min_periods=max(m1, 5)).kurt()
    if kind == "maxret":
        return ret1.rolling(w, min_periods=m1).max()
    if kind == "minret":
        return ret1.rolling(w, min_periods=m1).min()
    if kind == "upfrac":
        up = (ret1 > 0).where(ret1.notna()).astype(float)
        return up.rolling(w, min_periods=m1).mean()
    if kind == "volratio":
        w2 = int(p[1]); m2 = fe.roll_min(w2)
        return (vol.rolling(w, min_periods=m1).mean()
                / vol.rolling(w2, min_periods=m2).mean().replace(0.0, np.nan))
    if kind == "amtratio":
        w2 = int(p[1]); m2 = fe.roll_min(w2)
        return (amt.rolling(w, min_periods=m1).mean()
                / amt.rolling(w2, min_periods=m2).mean().replace(0.0, np.nan))
    if kind == "cvvol":
        return (vol.rolling(w, min_periods=m1).std()
                / vol.rolling(w, min_periods=m1).mean().replace(0.0, np.nan))
    if kind == "cvamt":
        return (amt.rolling(w, min_periods=m1).std()
                / amt.rolling(w, min_periods=m1).mean().replace(0.0, np.nan))
    if kind == "pvcorr":
        return _roll_corr(ret1, amt.pct_change(fill_method=None), w)
    if kind == "priceamtcorr":
        return _roll_corr(close, amt, w)
    if kind == "tsrank":
        w2 = int(p[1])
        return _ts_rank(close / close.shift(w) - 1, w2)
    if kind == "gapmean":
        return (op / close.shift(1) - 1).rolling(w, min_periods=m1).mean()
    if kind == "intraday":
        return (close / op - 1).rolling(w, min_periods=m1).mean()
    raise KeyError(f"unknown factor kind: {kind}")


# ── 统计检验 ────────────────────────────────────────────────────────────
def _p_two_sided(t: float) -> float:
    """双侧 p 值（正态近似，无 scipy 依赖）。"""
    return math.erfc(abs(t) / math.sqrt(2.0))


def ic_stats(ic_oos: pd.Series, ic_is: pd.Series | None = None,
             eval_step: int = fe.REBAL_EVERY) -> dict:
    """由验证集 IC 序列算统计量。

    显著性用**非重叠**子样本（每 eval_step 个有效 IC 日取 1）：T+10 前向收益互相重叠、
    日 IC 高度自相关，朴素 t 值会虚高约 √FWD 倍。均值/标准差/t/p 均取非重叠样本；
    分年衰减与同号率仍用全样本（看形态）。
    """
    s = ic_oos.dropna()
    n_all = int(len(s))
    thin = s.iloc[::max(1, int(eval_step))]
    n = int(len(thin))
    if n < 2:
        return {"n_days": n_all, "n_eval": n, "mean_ic": None, "std": None,
                "icir": None, "t_stat": None, "p_value": 1.0, "win_rate": None,
                "by_year": {}, "stable_frac": None, "sign_match": None,
                "is_mean_ic": None, "seg_match": None}
    mu, sd = float(thin.mean()), float(thin.std())
    # sd≈0（IC 为常数）→ 退化：ICIR 无定义；t 必须归 0 而非 inf，
    # 否则一个「恒定但无信息」的候选会以 p=0 通过 BH，污染整个家族错误率。
    sd_eff = sd if sd > 1e-12 else 0.0
    t = mu / sd_eff * math.sqrt(n) if sd_eff > 0 else 0.0
    by_year = {str(k): round(float(v), 4) for k, v in s.groupby(s.index.year).mean().items()}
    years = list(by_year.values())
    nonneg = sum(1 for v in years if v >= 0)
    stable = max(nonneg, len(years) - nonneg) / len(years) if years else 0.0
    is_mu = float(ic_is.dropna().mean()) if ic_is is not None and len(ic_is.dropna()) else None
    seg_match = None if is_mu is None else bool(is_mu * mu > 0)
    # 先验方向一致性以「全样本均值」判定（形态量，不用非重叠子样本）
    full_mu = float(s.mean())
    return {
        "n_days": n_all, "n_eval": n,
        "mean_ic": round(mu, 4), "std": round(sd, 4),
        "icir": round(mu / sd_eff, 3) if sd_eff > 0 else 0.0,
        "t_stat": round(t, 2), "p_value": _p_two_sided(t),
        "win_rate": round(float((thin > 0).mean()), 3),
        "by_year": by_year, "stable_frac": round(stable, 3),
        "full_mean_ic": round(full_mu, 4),
        "is_mean_ic": None if is_mu is None else round(is_mu, 4),
        "seg_match": seg_match,
    }


def benjamini_hochberg(pvals: list[float], q: float = FDR_Q) -> list[bool]:
    """BH-FDR：返回每个假设是否在 q 水平下被拒绝（保持输入顺序）。"""
    m = len(pvals)
    if m == 0:
        return []
    order = sorted(range(m), key=lambda i: pvals[i])
    kmax = -1
    for rank, i in enumerate(order, start=1):
        if pvals[i] <= rank / m * q:
            kmax = rank
    reject = [False] * m
    if kmax > 0:
        for rank, i in enumerate(order, start=1):
            if rank <= kmax:
                reject[i] = True
    return reject


def bonferroni(pvals: list[float], alpha: float = BONFERRONI_ALPHA) -> list[bool]:
    """Bonferroni：p <= alpha/m 才拒绝。"""
    m = len(pvals)
    if m == 0:
        return []
    thr = alpha / m
    return [p <= thr for p in pvals]


def prune_redundant(rows: list[dict], ic_frame: pd.DataFrame | None,
                    rho: float = REDUNDANCY_RHO,
                    anchors: set[str] | None = None) -> None:
    """就地标注冗余：与「已保留且 |ICIR| 更高」的因子 IC 相关 > rho → redundant_with。

    anchors（预注册基线）不参与被剔，但其复制品会被剔掉。ic_frame 列为因子名。
    """
    anchors = anchors or set()
    for r in rows:
        r.setdefault("redundant_with", None)
    if ic_frame is None or ic_frame.shape[1] < 2:
        return
    # 保留顺序：锚优先，再按 |ICIR| 降序
    def key(r: dict):
        return (0 if r["name"] in anchors else 1, -abs(r.get("icir") or 0.0))
    ordered = sorted(rows, key=key)
    kept: list[str] = []
    corr = ic_frame.corr(min_periods=30)
    for r in ordered:
        n = r["name"]
        if n not in corr.columns:
            kept.append(n)
            continue
        hit = None
        for k in kept:
            if k not in corr.columns:
                continue
            c = corr.at[n, k]
            if c == c and abs(c) > rho:      # NaN 不算
                hit = (k, float(c))
                break
        if hit is None:
            kept.append(n)
        elif n not in anchors:
            r["redundant_with"] = hit[0]
            r["redundant_rho"] = round(hit[1], 3)


# ── 闸门 ────────────────────────────────────────────────────────────────
def judge(row: dict, alive_by_fdr: bool, alive_by_bonf: bool,
          prior: int) -> tuple[str, str]:
    """返回 (裁决, 原因)。裁决 ∈ {存活, 方向反转, 样本不足, 不显著, IC过弱, 分年不稳, 两段矛盾, 冗余}。"""
    if row["redundant_with"]:
        return "冗余", f"与 {row['redundant_with']} 的 IC 相关 {row.get('redundant_rho')}"
    if (row.get("n_eval") or 0) < MIN_OOS_EVAL_DAYS:
        return "样本不足", f"验证集非重叠样本 {row.get('n_eval')} < {MIN_OOS_EVAL_DAYS}"
    if not alive_by_fdr:
        return "不显著", f"BH-FDR(q={FDR_Q}) 未通过（p={row.get('p_value'):.2g}）"
    if abs(row.get("full_mean_ic") or 0.0) < MIN_IC_ABS:
        return "IC过弱", f"|均值IC| {abs(row.get('full_mean_ic') or 0):.4f} < {MIN_IC_ABS}"
    if (row.get("stable_frac") or 0.0) < STABLE_FRAC:
        return "分年不稳", f"分年同号率 {row.get('stable_frac')} < {STABLE_FRAC:.2f}"
    if row.get("seg_match") is False:
        return "两段矛盾", "发现集与验证集均值 IC 反号"
    sig = row.get("full_mean_ic") or 0.0
    if prior and sig * prior < 0:
        return "方向反转", f"验证集 IC {sig:+.4f} 与先验方向反向"
    tag = "存活" if alive_by_bonf else "存活(FDR)"
    return tag, ""


# ── 报告 ────────────────────────────────────────────────────────────────
def _fmt(v, nd=4, plus=True) -> str:
    if v is None:
        return "n/a"
    if isinstance(v, float):
        return f"{v:+.{nd}f}" if plus else f"{v:.{nd}f}"
    return str(v)


def _nm(r: dict) -> str:
    """因子名（★ 标记是 v1 预注册基线，作为冗余剔重的锚）。"""
    return f"{r['name']}★" if r.get("is_baseline") else r["name"]


def build_rows(cands: list[Candidate], results: dict[str, dict]) -> list[dict]:
    """把每候选的统计结果整理成表行（含先验、家族、基线标记）。"""
    rows = []
    for c in cands:
        st = results.get(c.name)
        if st is None:
            continue
        r = {"name": c.name, "kind": c.kind, "family": c.family,
             "prior": c.prior, "is_baseline": is_baseline(c.name)}
        r.update(st)
        rows.append(r)
    return rows


def format_report(rows: list[dict], meta: dict) -> str:
    """渲染 markdown 报告。rows = 已带 verdict 的排名表。"""
    alive = [r for r in rows if r["verdict"].startswith("存活")]
    flipped = [r for r in rows if r["verdict"] == "方向反转"]
    L: list[str] = []
    L += ["# 因子池回放与自动挖掘（预注册文法 v1）", "",
          f"- 生成：{meta.get('generated_at', '')}",
          f"- 候选数 **{meta.get('n_candidates', 0)}**　发现集 {meta.get('is_start', fe.WINDOW_START)} ~ {SPLIT_END}　"
          f"验证集 {meta.get('oos_start', '')} ~ 至今",
          f"- 数据：{meta.get('grid_days')} 交易日 × {meta.get('grid_codes')} 只；"
          f"坏柱 {meta.get('corrupted_bars')} 根已剔除污染窗口",
          f"- 检验：验证集 IC 非重叠取样（每 {fe.REBAL_EVERY} 日一档）→ t 检验（双侧）；"
          f"多重检验 BH-FDR q={FDR_Q} + Bonferroni α={BONFERRONI_ALPHA} 双报",
          f"- 剔重：|IC 序列相关| > {REDUNDANCY_RHO} 判冗余；锚（★ 8 个预注册基线）优先，其余按 |ICIR| 降序",
          f"- **存活 {len(alive)} 个**（含 FDR 档）　方向反转 {len(flipped)} 个", ""]

    L += ["## 一、存活清单（验证集口径，按 |ICIR| 降序）", "",
          "| 因子 | 家族 | 先验 | 验证集IC | ICIR | t | p | 非重叠n | 分年同号率 | 两段一致 | 分位单调ρ | TOP50超额(pp) | 档 |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in sorted(alive, key=lambda x: -abs(x.get("icir") or 0.0)):
        dec = r.get("deciles") or {}
        pf = r.get("portfolio") or {}
        L.append(f"| {_nm(r)} | {r['family']} | {'多高' if r['prior'] > 0 else '多低'} | "
                 f"{_fmt(r.get('full_mean_ic'))} | {_fmt(r.get('icir'), 3)} | {_fmt(r.get('t_stat'), 2)} | "
                 f"{r.get('p_value', 1):.2g} | {r.get('n_eval')} | {r.get('stable_frac')} | "
                 f"{'是' if r.get('seg_match') else '否'} | {_fmt(dec.get('mono_rho'), 2, plus=False)} | "
                 f"{_fmt(pf.get('total_excess_pp'), 1)} | {r['verdict']} |")
    L += ["",
          "- ⚠️ **IC 显著 ≠ 可交易**：IC 由截面「身体」主导，TOP50 组合却是极端尾部。"
          "若「分位单调ρ 强但 TOP50 超额为负」→ 毒性集中在尾部（crash / 涨跌停锁死股）。",
          "- ⚠️ 流动性类（liq / illiq）的 TOP50 超额**系统性虚高**（微盘 + 幸存者 + "
          "不可交易流动性三重折扣，见 simple_strategy 报告同款 caveat），须大幅打折看。",
          "- 组合与分位为**验证集内**口径，每 10 交易日非重叠换仓、扣 0.3% 往返成本。", ""]
    if flipped:
        L += ["### 方向反转（显著但与经济先验相反 —— 需人工复核，不自动采纳）", "",
              "| 因子 | 家族 | 先验 | 验证集IC | ICIR | t | 非重叠n |",
              "|---|---|---|---|---|---|---|"]
        for r in sorted(flipped, key=lambda x: -abs(x.get("icir") or 0.0)):
            L.append(f"| {_nm(r)} | {r['family']} | {'多高' if r['prior'] > 0 else '多低'} | "
                     f"{_fmt(r.get('full_mean_ic'))} | {_fmt(r.get('icir'), 3)} | "
                     f"{_fmt(r.get('t_stat'), 2)} | {r.get('n_eval')} |")
        L += [""]

    L += ["## 二、全候选排名（发现集 IC 仅作一致性参考，不作筛选依据）", "",
          "| 因子 | 家族 | 先验 | 发现集IC | 验证集IC | ICIR | t | p | n | 裁决 | 原因 |",
          "|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in sorted(rows, key=lambda x: (0 if x["verdict"].startswith("存活") else
                                         1 if x["verdict"] == "方向反转" else 2,
                                         -abs(x.get("icir") or 0.0))):
        L.append(f"| {_nm(r)} | {r['family']} | {'多高' if r['prior'] > 0 else '多低'} | "
                 f"{_fmt(r.get('is_mean_ic'))} | {_fmt(r.get('full_mean_ic'))} | "
                 f"{_fmt(r.get('icir'), 3)} | {_fmt(r.get('t_stat'), 2)} | "
                 f"{r.get('p_value', 1):.2g} | {r.get('n_eval')} | {r['verdict']} | {r.get('reason', '')} |")
    L += [""]

    # 拒绝原因分布
    dist: dict[str, int] = {}
    for r in rows:
        dist[r["verdict"]] = dist.get(r["verdict"], 0) + 1
    L += ["### 裁决分布", "",
          "| 裁决 | 个数 |", "|---|---|"]
    for k, v in sorted(dist.items(), key=lambda kv: -kv[1]):
        L.append(f"| {k} | {v} |")
    L += [""]

    # 分年衰减（存活 + 反转）
    show = sorted(alive + flipped, key=lambda x: -abs(x.get("icir") or 0.0))
    if show:
        years = sorted({y for r in show for y in (r.get("by_year") or {})})
        L += ["## 三、分年度均值 IC（衰减形态）", "",
              "| 因子 | " + " | ".join(years) + " |",
              "|---|" + "---|" * len(years)]
        for r in show:
            by = r.get("by_year") or {}
            L.append(f"| {r['name']} | " + " | ".join(_fmt(by.get(y), 3) if y in by else "n/a"
                                                      for y in years) + " |")
        L += [""]

    L += ["## 四、判读规则（预注册）", "",
          f"- **存活** = BH-FDR(q={FDR_Q}) 通过 + |均值IC| ≥ {MIN_IC_ABS} + 分年同号率 ≥ "
          f"{STABLE_FRAC:.2f} + 发现/验证两段同号 + 与先验同向 + 不与更强因子冗余。",
          "- 显著性用**非重叠**子样本：T+10 前向收益重叠会让日 IC 自相关，朴素 t 虚高约 √10。",
          f"- **冗余规则**：|IC 序列相关| > {REDUNDANCY_RHO} 判冗余，保留顺序 = 锚优先（★ 8 个预注册"
          "基线必须留在对比集内）→ 其余按 |ICIR| 降序。故可能出现「|ICIR| 更高的近似复制品被较低的"
          "锚剔掉」（如 vol5 被 vol20 剔）；被剔者仍留在全排名表可查，冗余 ≠ 失败。",
          "- 文法高度共线（同一批原语换窗），冗余率通常 > 50% → **可用的独立维度远少于候选数**，"
          "存活数才是有效自由度；想扩自由度应引入正交原语（基本面/事件/资金流），而非继续换窗。",
          "- 「方向反转」不自动采纳：经济先验被证伪是重要信息，但需人工复核后另立新一轮。",
          "- 本报告只做**因子有效性筛选**，不产出配权、不改变生产选股；接入需另立一轮 + OOS 门控。",
          "", "## 五、Caveats（必读）", "",
          "- **幸存者偏差**：universe = 今天还活着的票 → 绝对收益偏乐观，**只用于因子间相对比较**。",
          "- 未剔 ST（无历史名单）；新股用 60 日预热近似剔除；换手率用成交额/成交量代理（无历史流通股本）。",
          "- 深历史 ≤2017 未纳入（hithink 失真未修）；2018-2022 仍有零星跳变，已按坏柱规则剔除污染窗口。",
          "- 显著性为**正态近似**（无 scipy），非重叠取样后 n 较小 → p 值仅供参考，以 Bonferroni 档最保守。",
          "- 候选池来自有限文法：未发现的因子不代表不存在；扩充文法 = 新一轮预注册。"]
    return "\n".join(L) + "\n"
