#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""统计显著性校验：削减夏普(Deflated Sharpe) + 多重检验 t 阈值 + 概率夏普(PSR)。

背景：walk-forward / 参数网格扫描在 N 个候选里挑最优，天然面临"数据挖矿 / 多重检验"
偏差——纯随机噪声也能在 N 次尝试里凑出一个"显著"最优。Harvey & Liu (2014) 与
Bailey & López de Prado (2014) 给出标准做法：
  * 多重检验后 t 统计阈值从 2.0 提升到 ~3.0；
  * 用 Deflated Sharpe Ratio 把"尝试次数 N"纳入临界值，判断观测夏普是否超过
    "随机 N 次尝试本就能达到的最大夏普"。

所有阈值走 risk_config（calibration_significance 块），不硬编码魔数。
纯标准库实现，不依赖 pandas/numpy。
"""
from __future__ import annotations

import math
from statistics import NormalDist


def _mean(xs: list[float]) -> float:
    n = len(xs)
    return sum(xs) / n if n else 0.0


def _std(xs: list[float], ddof: int = 1) -> float:
    n = len(xs)
    if n - ddof <= 0:
        return 0.0
    m = _mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - ddof))


def _skew(xs: list[float]) -> float:
    n = len(xs)
    if n < 3:
        return 0.0
    m = _mean(xs)
    s = _std(xs, ddof=0)
    if s == 0:
        return 0.0
    return (sum((x - m) ** 3 for x in xs) / n) / (s ** 3)


def _excess_kurtosis(xs: list[float]) -> float:
    """总峰度（scipy 约定，正态=3）。返回 3 表示 excess kurtosis=0。"""
    n = len(xs)
    if n < 4:
        return 3.0
    m = _mean(xs)
    s = _std(xs, ddof=0)
    if s == 0:
        return 3.0
    return (sum((x - m) ** 4 for x in xs) / n) / (s ** 4)


def sharpe_ratio(returns: list[float], ddof: int = 1) -> float | None:
    """每期夏普比率 = mean / std（未年化；跨策略比较只需同口径）。样本不足返回 None。"""
    if len(returns) < 3:
        return None
    s = _std(returns, ddof=ddof)
    if s == 0:
        return None
    return _mean(returns) / s


def probabilistic_sharpe(returns: list[float], sr_benchmark: float = 0.0) -> float | None:
    """概率夏普比率 PSR：真实 SR > sr_benchmark 的概率（Φ 正态 CDF）。
    考虑偏度/峰度修正（Bailey & López de Prado 2014）。"""
    n = len(returns)
    sr = sharpe_ratio(returns)
    if sr is None or n < 3:
        return None
    g3 = _skew(returns)
    g4 = _excess_kurtosis(returns)
    denom = math.sqrt(max(1e-12, 1 - g3 * sr + (g4 - 1) / 4 * sr * sr))
    z = (sr - sr_benchmark) * math.sqrt(n - 1) / denom
    return NormalDist().cdf(z)


def deflated_sharpe_critical(sr_benchmark: float, n_obs: int, n_trials: int,
                            significance: float = 0.05,
                            gamma3: float = 0.0, gamma4: float = 3.0) -> float:
    """多重检验调整后的临界夏普 SR*（Bailey & López de Prado 2014, eq.32）。

      SR* = SR_bench * sqrt(1 + θ·(1 - γ3·SR_bench + (γ4-1)/4·SR_bench²) / (SR_bench²·(n-1)))
      θ = (1-ψ)^{-1} · V · ζ,  V=1, ζ=Euler-Mascheroni≈0.5772, ψ=significance
    当 SR_bench=0 时退化为 0（此时由 t_stat 与 PSR 把关，见 significance_report）。
    """
    if n_obs < 3 or sr_benchmark == 0:
        return 0.0
    zeta = 0.5772156649015329
    theta = ((1 - significance) ** -1) * 1.0 * zeta
    inner = 1 - gamma3 * sr_benchmark + (gamma4 - 1) / 4 * sr_benchmark * sr_benchmark
    ratio = theta * inner / (sr_benchmark * sr_benchmark * (n_obs - 1))
    return sr_benchmark * math.sqrt(1 + ratio)


def t_stat_multiple_testing(returns: list[float], sr_benchmark: float = 0.0,
                            min_t_stat: float = 3.0) -> tuple[float, bool]:
    """返回 (t_stat, ok)。多重检验修正后的 t 阈值比较。

    t = SR·sqrt(n-1) / sqrt(1 - γ3·SR + (γ4-1)/4·SR²)（与 PSR 同口径）；
    阈值 min_t_stat 默认 3.0（Harvey-Liu 2014：单测 2.0 在多次尝试下仍会放过多重检验伪信号）。
    """
    n = len(returns)
    sr = sharpe_ratio(returns)
    if sr is None or n < 3:
        return 0.0, False
    g3 = _skew(returns)
    g4 = _excess_kurtosis(returns)
    denom = math.sqrt(max(1e-12, 1 - g3 * sr + (g4 - 1) / 4 * sr * sr))
    t = sr * math.sqrt(n - 1) / denom if denom > 0 else sr * math.sqrt(n - 1)
    return t, (t >= min_t_stat)


def significance_report(returns: list[float], n_trials: int = 1, sr_benchmark: float = 0.0,
                       significance: float = 0.05, min_t_stat: float = 3.0) -> dict:
    """综合显著性报告。

    返回 dict：{n, sharpe, t_stat, t_ok, psr, sr_critical, dsr_ok, significant, ...}
      significant = dsr_ok 且 t_ok
        dsr_ok = 观测 SR > 多重检验临界 SR*（SR_bench≠0 时）；SR_bench=0 时退化为 True，交由 t/PSR 判定
        t_ok   = t_stat >= min_t_stat（多重检验阈值）
    样本不足(<3)时保守返回 significant=False（不写回）。
    """
    n = len(returns)
    sr = sharpe_ratio(returns)
    if sr is None or n < 3:
        return {
            "n": n, "sharpe": None, "t_stat": None, "t_ok": False,
            "psr": None, "sr_critical": 0.0, "dsr_ok": False,
            "significant": False, "reason": "样本不足(<3)",
            "n_trials": n_trials, "sr_benchmark": sr_benchmark,
            "significance": significance, "min_t_stat": min_t_stat,
        }
    g3 = _skew(returns)
    g4 = _excess_kurtosis(returns)
    t, t_ok = t_stat_multiple_testing(returns, sr_benchmark, min_t_stat)
    psr = probabilistic_sharpe(returns, sr_benchmark)
    sr_crit = deflated_sharpe_critical(sr_benchmark, n, n_trials, significance, g3, g4)
    dsr_ok = (sr > sr_crit) if sr_benchmark != 0 else True
    significant = bool(dsr_ok and t_ok)
    return {
        "n": n,
        "sharpe": round(sr, 4),
        "t_stat": round(t, 4),
        "t_ok": t_ok,
        "psr": round(psr, 4) if psr is not None else None,
        "sr_critical": round(sr_crit, 4),
        "dsr_ok": dsr_ok,
        "significant": significant,
        "n_trials": n_trials,
        "sr_benchmark": sr_benchmark,
        "significance": significance,
        "min_t_stat": min_t_stat,
    }
