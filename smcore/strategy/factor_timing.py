#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""因子生效开关（factor timing overlay）——生产路径（2026-09-16）.

每日按「信念 IC」（分配器给策略 s 的权重 vs s 选中票当日平均前向收益的滚动 Spearman）
给 5 个策略（=5 类因子）打分：仅当近期信念 IC **显著为负**才把该因子权重清零，
其余（正 IC / 近零 IC）一律保留——防止好因子因"不显著"被误杀。纯数据驱动、零硬编码。

开关：``CONFIG["factor_timing"].enabled``（默认关）。由
``scripts/walk_forward_factor_timing.py`` 的 walk-forward 稳健门控判定后写回；
月度自动化「因子生效开关月度回滚 tripwire」按 ``--enforce`` 幂等回滚。

设计：
- 因果安全：第 d 天的权重只用严格早于 d 的历史；mask 只用严格早于 signal_date 的点。
- **算法单源**：核心 :func:`factor_timing_mask_from_points` 被本模块与
  ``scripts/walk_forward_validator._factor_timing_mask`` 共用，二者不会分叉。
- 数据访问自包含（读 stock_data 的 DAL / Multi-Backtest / k_data），不 import ``scripts/``。
- 仅在 ``CONFIG.enabled`` 为真时被 ``compute_adaptive_allocation`` 惰性 import（不增启动内存）。
"""
from __future__ import annotations

import math
import re

import pandas as pd

from smcore.config.defaults import STOCK_DATA_DIR
from smcore.data.kline import read_kline_cache
from smcore.strategy.adaptive_weights import (
    ALL_STRATEGIES,
    CONFIG,
    _aggregate_excess,
    _benchmark_forward_ret,
    _norm_code,
    _norm_strategies,
    adaptive_weights,
)

DEFAULT_WINDOW = 10        # 滚动窗口：最近 N 个严格早于 signal_date 的信号日
DEFAULT_MIN_N = 5          # 窗口内合并点数下限（不足则不判，因子保留）
DEFAULT_Z = 1.96           # Spearman 显著性临界 z（α=0.05）
EDGE_WINDOW = 20           # 因果 edge 窗口（与生产 compute_adaptive_allocation 默认一致）
MIN_N = 8                  # 因果 edge 总样本不足则冷启动等权（与自适应权重一致）
HOLD_DAYS = 10             # k_data 回补持有期（与 walk_forward_validator.WF_HOLD_DAYS 一致）

_DAY_RECORDS_CACHE: dict = {}


# ── 纯统计（平均秩 + Spearman，不依赖 scipy）─────────────────────────
def _rank(xs: list[float]) -> list[float]:
    """平均秩（处理并列），返回与 xs 等长秩列表。"""
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    ranks = [0.0] * len(xs)
    i = 0
    while i < len(xs):
        j = i
        while j + 1 < len(xs) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def spearman_ic(xs: list[float], ys: list[float]) -> float | None:
    """手写 Spearman 秩相关；样本<3 或任一方无变化返回 None。"""
    n = len(xs)
    if n != len(ys) or n < 3:
        return None
    rx, ry = _rank(xs), _rank(ys)
    mx, my = sum(rx) / n, sum(ry) / n
    cov = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    vx = sum((rx[i] - mx) ** 2 for i in range(n))
    vy = sum((ry[i] - my) ** 2 for i in range(n))
    if vx <= 0 or vy <= 0:
        return None
    return cov / math.sqrt(vx * vy)


# ── 核心：给定信念 IC 点 → 因子生效 mask（与验证器共用，勿分叉）──────────
def factor_timing_mask_from_points(
    points: dict, prev_days, *, min_n: int = DEFAULT_MIN_N, z: float = DEFAULT_Z
) -> dict[str, bool]:
    """纯函数。points={strategy:[(day,w,ret)]}；prev_days=严格早于 signal_date 的信号日（已按窗口截取）。

    判据（保守、防误杀）：窗口内点数不足 / IC 无定义 → 保留（无证据不判失效）；
    否则**仅当信念 IC 显著为负才清零**，其余（正 IC、近零 IC）一律保留。
    ⇒ 好因子不会因"不显著"被枪毙，只有被证据明确证伪（显著为负）的坏因子才出局。
    """
    past_set = set(prev_days)
    mask: dict[str, bool] = {}
    for s in ALL_STRATEGIES:
        series = [(w, r) for (d, w, r) in points.get(s, []) if d in past_set]
        n = len(series)
        if n < min_n:
            mask[s] = True
            continue
        ic = spearman_ic([w for w, _ in series], [r for _, r in series])
        if ic is None:
            mask[s] = True
            continue
        crit = z / math.sqrt(n - 1)
        # 仅当 IC 显著为负才清零；正 IC / 近零 IC 一律保留（防止误杀好因子）
        mask[s] = bool(ic >= -crit)
    # ── 因果闸（可选、失败软）── Double ML 因果体检 verdicts 门控
    # 启用且 verdicts 可用时，把「非稳定 / mirage」因子额外清零（只放真因果因子进信号）；
    # enabled=False（默认）或 verdicts 缺失/异常 → 原样返回，零行为变化（与 factor_timing 同纪律）。
    try:
        from smcore.config.defaults import FACTOR_CAUSAL_GATE
        if FACTOR_CAUSAL_GATE.get("enabled"):
            from smcore.strategy.causal_validation import (
                load_causal_verdicts, apply_causal_gate_to_mask,
            )
            verdicts = load_causal_verdicts(FACTOR_CAUSAL_GATE.get("source"))
            if verdicts:
                mask = apply_causal_gate_to_mask(mask, verdicts, FACTOR_CAUSAL_GATE)
    except Exception:
        pass
    return mask


# ── 数据访问（自包含，读 stock_data）─────────────────────────────────
def _signal_days() -> list[str]:
    """所有存在 Daily-Action-List 的信号日（升序）。"""
    days = set()
    for p in STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"):
        m = re.search(r"(\d{8})", p.name)
        if m:
            days.add(m.group(1))
    return sorted(days)


def _read_dal_sources(sd: str) -> dict[str, set[str]]:
    """DAL → {code: 来源策略集合}。"""
    src: dict[str, set[str]] = {}
    dal = STOCK_DATA_DIR / f"Daily-Action-List-{sd}.csv"
    if not dal.exists():
        return src
    try:
        d = pd.read_csv(dal, encoding="utf-8-sig")
    except Exception:
        return src
    if "股票代码" not in d.columns:
        return src
    for _, r in d.iterrows():
        c = _norm_code(r.get("股票代码"))
        if c:
            src[c] = _norm_strategies(r.get("来源策略"))
    return src


def _forward_return_from_kdata(code: str, sd: str, hold: int = HOLD_DAYS) -> float | None:
    """本地 k_data 回补：信号日次开盘买入 → hold 日后开盘卖出。只读缓存，不联网。"""
    try:
        df = read_kline_cache(code, base_dir=STOCK_DATA_DIR / "k_data")
    except Exception:
        return None
    if df is None or getattr(df, "empty", True):
        return None
    if "date" not in df.columns or "open" not in df.columns:
        return None
    df = df.copy()
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "open"]).sort_values("date")
    future = df[df["date"] > pd.to_datetime(sd)]
    if future.empty:
        return None
    buy = float(future.iloc[0]["open"])
    if buy <= 0:
        return None
    idx = min(hold, len(future) - 1)
    sell = float(future.iloc[idx]["open"])
    return (sell - buy) / buy * 100.0


def _day_records(sd: str) -> list[tuple[str, set[str], float]]:
    """[(code, sources, return_pct)]；优先生产 Multi-Backtest，缺失回退 k_data 因果回补。"""
    cached = _DAY_RECORDS_CACHE.get(sd)
    if cached is not None:
        return cached
    src = _read_dal_sources(sd)
    rows: list[tuple[str, set[str], float]] = []
    tr = STOCK_DATA_DIR / f"Multi-Backtest-{sd}-trades.csv"
    if tr.exists():
        try:
            t = pd.read_csv(tr, encoding="utf-8-sig")
        except Exception:
            t = None
        if t is not None and not t.empty and "code" in t.columns:
            for _, r in t.iterrows():
                c = _norm_code(r.get("code"))
                try:
                    rp = float(r.get("return_pct"))
                except (TypeError, ValueError):
                    continue
                rows.append((c, src.get(c) or {"__unknown__"}, rp))
    if not rows:  # 无生产成交 → k_data 因果回补
        for code, sources in src.items():
            rp = _forward_return_from_kdata(code, sd)
            if rp is not None:
                rows.append((code, sources, rp))
    _DAY_RECORDS_CACHE[sd] = rows
    return rows


def _causal_weights(sd: str) -> dict[str, float]:
    """第 sd 天的因果权重：仅用严格早于 sd 的历史 edge → adaptive_weights。

    内部 edge 聚合用等权口径（与 CONFIG["edge"]["position_weighted"] 默认一致），
    且超额收益**相对沪深300基准**，与生产 ``compute_universe_edge(use_benchmark=True)``
    同口径——避免把"市场β"误计为策略 edge（旧实现直接用绝对收益 rp，导致信念 IC
    在绝对口径下被市场系统性漂移污染）。基准不可用时退化为绝对收益（与生产一致）。
    """
    past = [d for d in _signal_days() if d < sd][-EDGE_WINDOW:]
    bench_cache: dict[str, float | None] = {}
    strat_pairs: dict[str, list] = {s: [] for s in ALL_STRATEGIES}
    for d in past:
        if d not in bench_cache:
            bench_cache[d] = _benchmark_forward_ret(d, HOLD_DAYS)
        b = bench_cache[d]
        for _code, sources, rp in _day_records(d):
            excess = rp - b if b is not None else rp
            for s in sources:
                if s in strat_pairs:
                    strat_pairs[s].append((excess, None))
    edge: dict[str, dict] = {}
    for s, prs in strat_pairs.items():
        e, win, n, sdv = _aggregate_excess(prs, False)
        edge[s] = {"n": n, "avg_return": e, "win_rate": win, "edge": e, "std": sdv}
    total_n = sum(v["n"] for v in edge.values())
    if total_n < MIN_N:  # 冷启动：等权（不依赖业绩）
        eq = round(100 / len(ALL_STRATEGIES))
        return {s: eq for s in ALL_STRATEGIES}
    return adaptive_weights(edge)


def _points_for_days(days: list[str]) -> dict[str, list[tuple[str, float, float]]]:
    """仅为给定信号日构建信念 IC 点（生产只需窗口内少数日，避免全历史重算）。"""
    pts: dict[str, list[tuple[str, float, float]]] = {s: [] for s in ALL_STRATEGIES}
    for sd in days:
        recs = _day_records(sd)
        if not recs:
            continue
        w = _causal_weights(sd)
        by: dict[str, list[float]] = {s: [] for s in ALL_STRATEGIES}
        for _code, sources, rp in recs:
            for s in sources:
                if s in by:
                    by[s].append(rp)
        for s in ALL_STRATEGIES:
            if by[s]:
                pts[s].append((sd, float(w.get(s, 0.0)), sum(by[s]) / len(by[s])))
    return pts


def conviction_points(days: list[str] | None = None) -> dict[str, list[tuple[str, float, float]]]:
    """{strategy: [(day, w_s, day_avg_ret_s)]}——信念 IC 的原始点（与验证器同源口径）。

    注意：全历史口径较重（每个信号日都重算其因果 edge）；生产 mask 只需窗口内少数日，
    走 :func:`_points_for_days`。本函数保留给验证/监控脚本做全样本诊断。
    """
    return _points_for_days(_signal_days() if days is None else days)


def latest_signal_date() -> str | None:
    days = _signal_days()
    return days[-1] if days else None


def factor_timing_mask(
    signal_date: str,
    points: dict | None = None,
    *,
    window: int | None = None,
    min_n: int | None = None,
    z: float | None = None,
) -> dict[str, bool]:
    """信号日 signal_date 的因子生效 mask（window/min_n/z 读 CONFIG，可显式覆盖）。"""
    ft = CONFIG.get("factor_timing") or {}
    window = int(window if window is not None else ft.get("window", DEFAULT_WINDOW))
    min_n = int(min_n if min_n is not None else ft.get("min_n", DEFAULT_MIN_N))
    z = float(z if z is not None else ft.get("z", DEFAULT_Z))
    pts = conviction_points() if points is None else points
    prev = [d for d in _signal_days() if d < signal_date][-window:]
    return factor_timing_mask_from_points(pts, prev, min_n=min_n, z=z)


def is_enabled() -> bool:
    return bool((CONFIG.get("factor_timing") or {}).get("enabled", False))


def apply_mask(weights: dict, signal_date: str | None = None, points: dict | None = None) -> dict:
    """把因子生效 mask 应用到 pct 权重：失效因子清零后按比例重分配（和为 100）。

    全被清零 / 无法判定 / signal_date 缺失 → 原样返回（避免空分配或误伤现状）。
    """
    sd = signal_date or latest_signal_date()
    if not sd:
        return dict(weights)
    mask = factor_timing_mask(sd, points=points)
    kept = {s: (float(weights.get(s, 0.0)) if mask.get(s, True) else 0.0) for s in ALL_STRATEGIES}
    tot = sum(kept.values())
    if tot <= 0:
        return dict(weights)
    return {s: kept[s] / tot * 100.0 for s in ALL_STRATEGIES}
