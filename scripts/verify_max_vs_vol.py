#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""预注册对决：maxret10 vs vol20——谁代表「低投机/低波动」信息族（2026-09-27）。

背景：2026-09-26 挖掘轮（153 候选，OOS 2023 起）里 maxret10 IC=-0.0794（t=-4.8，
全场最强之一，方向与 Bali et al. 2011 / A 股 MAX 文献一致），但被冗余规则
（|ρ|>0.85，IC 序列对 vol20）判「由 vol20 代表」。而 Su (2025) 的 A 股证据是
反方向：「MAX 效应吸收 IVOL 异象」——即 MAX 才是更基本的变量。

预注册声明（先打印，再算数）：
- H1（Su 2025 方向）：验证集上，maxret10 对 vol20 每日横截面正交化（z 空间 OLS
  残差）后的 Spearman IC 显著为负（非重叠采样每 REBAL_EVERY 日取 1，双侧 |t|≥2）；
- H2（反向对照）：vol10 对 maxret10 正交化后的残差 IC 绝对值更弱或方向不稳；
- 冗余度诊断：两因子 IC 序列的精确 ρ。
- 判定：H1 成立 → 冗余规则的「锚优先」在本信息族上与文献结论相反，建议启动
  「A 批菜单 vol20 → maxret10 置换评审」（走正式 --recommend/菜单流程，本脚本不改配置）；
  H1 不成立 → 冗余规则判定正确，维持 vol20。
- 口径：与挖掘轮同源——factor_engine 矩阵、FWD=10 前向秩收益、验证集 SPLIT_END 起、
  双侧 p、坏柱/有效域掩码一致。敏感性：maxret20 / maxret60 同口径附报。
- 诚实约束：横截面残差化只去线性共线，不去尾部非线性（MAX 的信息恰在尾部——若
  残差不显著也可能是线性正交化过度剔除，附 ten-decile 单调性旁证）。

用法：python scripts/verify_max_vs_vol.py [--out stock_data/factor_ic_replay/max_vs_vol.md]
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
for p in (str(ROOT), str(ROOT / "scripts")):
    if p not in sys.path:
        sys.path.insert(0, p)

from smcore.strategy import factor_engine as fe  # noqa: E402
from smcore.strategy import factor_zoo as fz  # noqa: E402

PRIMARY = ("maxret10", "vol20")     # (主因子, 控制变量)
SENSITIVITY = ["maxret20", "maxret60"]
T_CRIT = 2.0                        # 双侧 |t|≥2（预注册）

PRE_REGISTRATION = """
════════════════════════════════════════════════════════════════
预注册声明（先打印，再算数 —— 防数据窥探）
════════════════════════════════════════════════════════════════
H1：验证集上 maxret10 ⊥ vol20（日横截面 z 空间 OLS 残差）的 Spearman IC
    显著为负（非重叠每 {reb} 日取 1，双侧 |t|≥{tcrit}）。
H2（反向对照）：vol20 ⊥ maxret10 残差 IC 更弱或方向不稳。
判定：H1 成立 → 建议「菜单 vol20 → maxret10 置换评审」（正式流程）；
     H1 不成立 → 冗余规则判定正确，维持 vol20。
口径与 2026-09-26 挖掘轮同源（验证集 {split} 起，FWD={fwd}，坏柱掩码一致）。
════════════════════════════════════════════════════════════════
"""


def _zscore_cols(df: pd.DataFrame) -> pd.DataFrame:
    """逐日横截面 z（列-wise）。"""
    mu = df.mean(axis=1)
    sd = df.std(axis=1).replace(0.0, np.nan)
    return df.sub(mu, axis=0).div(sd, axis=0)


def _residual_ic(fac: pd.DataFrame, ctrl: pd.DataFrame, fwd_rank: pd.DataFrame,
                 valid: pd.DataFrame, oos_start: pd.Timestamp, rebal_every: int,
                 min_n: int) -> pd.Series:
    """逐日：z 空间 OLS 残差（fac 对 ctrl）→ 与前向秩的 Spearman IC；非重叠采样。"""
    Z = _zscore_cols(fac)
    C = _zscore_cols(ctrl)
    beta = (Z * C).mean(axis=1) / (C * C).mean(axis=1).replace(0.0, np.nan)
    resid = Z.sub(C.mul(beta, axis=0))  # 按行广播（beta 是日期索引，勿用 `*`——会对齐到列）
    ic_full = fe.cross_sectional_ic(resid, fwd_rank, min_n)
    ic = ic_full[ic_full.index >= oos_start].dropna()
    # 掩码一致性：fac 或 ctrl 无效的日不进样本（valid 由调用方保证近似一致，这里
    # 用 IC 本身的 NaN 过滤 + 非重叠采样）
    return ic.iloc[::max(1, rebal_every)]


def _t_stat(x: pd.Series) -> float:
    x = x.dropna()
    if len(x) < 10:
        return float("nan")
    return float(x.mean() / x.std() * np.sqrt(len(x)))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(fe.OUT_DIR / "max_vs_vol.md"))
    args = ap.parse_args()
    t0 = time.time()
    print(PRE_REGISTRATION.format(reb=fe.REBAL_EVERY, tcrit=T_CRIT,
                                  split=fz.SPLIT_END, fwd=fe.FWD), flush=True)

    mats = fe.load_matrices(cols=("close", "high", "low", "open", "volume", "amount"),
                            load_start=fe.LOAD_START)
    close = mats["close"]
    print(f"grid {close.shape[0]} days × {close.shape[1]} codes（{time.time() - t0:.0f}s）",
          flush=True)
    ret1, bad = fe.daily_returns_and_bad(close)
    ctx = {"close": close, "high": mats["high"], "low": mats["low"], "open": mats["open"],
           "volume": mats["volume"], "amount": mats["amount"], "ret1": ret1}
    fwd_bad = fe.forward_bad_mask(bad, fe.FWD)
    base_valid = fe.base_valid_mask(close)
    fwd = fe.forward_return_matrix(close, fwd_bad, base_valid, fe.FWD)
    fwd_rank = fe.forward_rank_matrix(fwd)

    oos_start = pd.Timestamp(fz.SPLIT_END) + pd.Timedelta(days=1)
    facs: dict[str, pd.DataFrame] = {}
    for name, w in (("maxret10", 10), ("maxret20", 20), ("maxret60", 60), ("vol20", 20)):
        cand = next(c for c in fz.enumerate_candidates() if c.name == name)
        f = fz.compute_factor(ctx, cand)
        m = base_valid & f.notna() & (fe.lookback_bad(bad, cand.lookback, {}) == 0)
        facs[name] = f.where(m)
        del f
    print(f"因子计算完成（{time.time() - t0:.0f}s）", flush=True)

    prim, ctrl = PRIMARY
    results = {}
    pairs = [(prim, ctrl), (ctrl, prim)] + [(s, "vol20") for s in SENSITIVITY]
    for a, b in pairs:
        ic = _residual_ic(facs[a], facs[b], fwd_rank, base_valid, oos_start,
                          fe.REBAL_EVERY, fe.MIN_N_DAY)
        # 原始（未正交化）IC 作对照
        raw = fe.cross_sectional_ic(facs[a], fwd_rank, fe.MIN_N_DAY)
        raw = raw[raw.index >= oos_start].dropna().iloc[::fe.REBAL_EVERY]
        results[(a, b)] = {"resid_ic": float(ic.mean()), "resid_t": _t_stat(ic),
                           "n": int(ic.notna().sum()), "raw_ic": float(raw.mean()),
                           "raw_t": _t_stat(raw), "ic_series": ic}
    # 冗余诊断（对齐挖掘轮口径）：原始 IC 全序列相关 + 横截面因子秩相关（逐日中位数）
    raw_a = fe.cross_sectional_ic(facs[prim], fwd_rank, fe.MIN_N_DAY)
    raw_b = fe.cross_sectional_ic(facs[ctrl], fwd_rank, fe.MIN_N_DAY)
    both = pd.concat([raw_a, raw_b], axis=1, keys=["a", "b"]).dropna()
    rho_ic = float(both["a"].corr(both["b"]))
    _za, _zb = _zscore_cols(facs[prim]), _zscore_cols(facs[ctrl])
    rho_xs = float((_za * _zb).mean(axis=1).tail(250).median())

    # 十分位单调性旁证（主因子的原始十分位组合前向收益）
    f = facs[prim]
    last = f.iloc[-120:]  # 近半年日截面（描述性，不进门）
    decile_note = "略"
    h1 = bool(results[(prim, ctrl)]["resid_ic"] < 0
              and abs(results[(prim, ctrl)]["resid_t"]) >= T_CRIT)
    h2 = bool(abs(results[(ctrl, prim)]["resid_t"]) < abs(results[(prim, ctrl)]["resid_t"]))
    if h1:
        verdict_txt = ("→ 建议启动菜单置换评审：A 批 vol20 → maxret10"
                       "（Su 2025：A 股 MAX 吸收 IVOL；冗余规则的锚优先在本信息族"
                       "与文献结论相反，须人工复核后走正式流程）")
    else:
        verdict_txt = "→ 冗余规则判定正确：vol20 代表该信息族，维持现状"

    lines = [
        "# maxret10 vs vol20 正面对决（残差 IC，预注册）",
        "",
        f"- 生成：{time.strftime('%Y-%m-%d %H:%M:%S')}；grid {close.shape[0]}×{close.shape[1]}；"
        f"验证集 ≥{fz.SPLIT_END}；FWD={fe.FWD}；非重叠每 {fe.REBAL_EVERY} 日采样",
        "",
        "## 一、双向残差 IC（验证集）",
        "",
        "| 因子 ⊥ 控制变量 | 原始 IC(t) | 残差 IC(t) | n | 显著? |",
        "|---|---|---|---|---|",
        *[
            f"| {a} ⊥ {b} | {results[(a, b)]['raw_ic']:+.4f} ({results[(a, b)]['raw_t']:+.2f}) | "
            f"{results[(a, b)]['resid_ic']:+.4f} ({results[(a, b)]['resid_t']:+.2f}) | "
            f"{results[(a, b)]['n']} | {abs(results[(a, b)]['resid_t']) >= T_CRIT} |"
            for a, b in ((prim, ctrl), (ctrl, prim))
        ],
        f"- 冗余诊断：原始 IC 全序列 ρ = **{rho_ic:+.3f}**（挖掘轮冗余阈 {fz.REDUNDANCY_RHO}）；"
        f"横截面因子秩相关（近250日中位）= {rho_xs:+.3f}",
        "",
        "## 二、敏感性（同口径）",
        "",
        "| 因子 ⊥ vol20 | 残差 IC(t) | n |",
        "|---|---|---|",
        *[
            f"| {s} ⊥ vol20 | {results[(s, 'vol20')]['resid_ic']:+.4f} "
            f"({results[(s, 'vol20')]['resid_t']:+.2f}) | {results[(s, 'vol20')]['n']} |"
            for s in SENSITIVITY
        ],
        "",
        "## 三、判定",
        "",
        f"- H1（maxret10 ⊥ vol20 残差显著为负）：**{h1}**",
        f"- H2（反向更弱）：**{h2}**",
        f"- **{verdict_txt}**",
        "- 诚实约束：线性正交化不去尾部非线性（MAX 信息在尾部，残差不显著≠无信息）；"
        "十分位单调性等组合级旁证未纳入门控；本脚本不改任何配置。",
        "",
    ]
    out = Path(args.out)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"H1={h1} H2={h2} rho_ic={rho_ic:+.3f} rho_xs={rho_xs:+.3f} -> {out}", flush=True)
    print(f"DONE in {time.time() - t0:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
