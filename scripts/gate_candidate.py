#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""单候选因子 OOS 门控验证（复用 walk_forward_factor_timing 稳健门语义，单因子版）。

在把某 zoo 候选因子「升 live 菜单」之前，用与生产 factor_timing 同样的稳健门检验其
「可交易性」——单因子 TOP-N 组合的验证集 OOS 超额是否被稳健门接受。

双轨（与生产 overlay 同构）：
- ON    : 始终持有该因子 TOP-N 组合；
- GATED : 仅当「滚动信念 IC」(因子日 IC 的滚动均值，非重叠 t 检验) 显著为正时才持有该组合，
          否则持基准（全宇宙等权）。

稳健门（与 walk_forward_factor_timing._gate 同源重规格）：
  improve_pp ≥ MIN_IMPROVE_PP  ∧  前后半段稳定  ∧  逐日改进单侧 t 显著
  ∧  跨 regime 稳健  ∧  换手率约束。

仅出报告，不改配置、不改 registry、不把因子写入 live 菜单。
"""
from __future__ import annotations

import argparse
import json
import math
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
from smcore.strategy.significance import significance_report  # noqa: E402

MIN_IMPROVE_PP = 2.0          # 与 walk_forward_factor_timing 一致
T_CRIT = 1.31                 # 单侧 α=0.10 临界（n_trials=1，固定规则非数据挖矿）
MAX_AVG_FLIP = 0.5
BELIEF_WIN = 63               # 滚动信念 IC 窗口（~3 个月交易日）


def _rolling_belief_on(ic_oos: pd.Series, win: int = BELIEF_WIN,
                       eval_step: int = fe.REBAL_EVERY) -> dict:
    """对日 IC 序列算滚动信念 IC 显著性，返回每个 OOS 日是否「显著为正」（用于门控）。"""
    s = ic_oos.dropna()
    on: dict = {}
    dates = list(s.index)
    for i, d in enumerate(dates):
        if i < win:
            on[d] = False
            continue
        thin = s.iloc[i - win:i].iloc[::max(1, eval_step)]
        n = len(thin)
        if n < 2:
            on[d] = False
            continue
        mu = float(thin.mean())
        sd = float(thin.std())
        sd_eff = sd if sd > 1e-12 else 0.0
        t = mu / sd_eff * math.sqrt(n) if sd_eff > 0 else 0.0
        on[d] = bool(mu > 0 and t >= T_CRIT)
    return on


def _portfolio_rows(fac, fwd, base_valid, bad, fwd_bad, rebal, pos, prior, top_n):
    """逐再平衡日产出 {date, pf, bench}。pf=TOP-N 组合前向收益；bench=全宇宙等权前向收益。"""
    rows = []
    for d in rebal:
        if pos.get(d, 0) + fe.FWD >= len(fac.index):
            continue
        fv = base_valid.loc[d] & (bad.loc[d] == 0) & (fwd_bad.loc[d] == 0)
        col = fac.loc[d]
        cand = col[fv & col.notna()].dropna()
        if len(cand) < top_n * 2:
            continue
        fr = fwd.loc[d]
        bench_vals = fr[fv & fr.notna()]
        if bench_vals.empty:
            continue
        top = cand.nlargest(top_n) if prior > 0 else cand.nsmallest(top_n)
        fr_h = fr.reindex(list(top.index)).dropna()
        if len(fr_h) < 10:
            continue
        rows.append({"date": d, "pf": float(fr_h.mean()),
                     "bench": float(bench_vals.mean())})
    return rows


def _cum(rets) -> float:
    # rets 为逐期收益**分数**（如 0.003 = 0.3%），直接累乘后转百分比。
    acc = 1.0
    for r in rets:
        if r is not None:
            acc *= (1 + r)
    return (acc - 1) * 100.0


def _md(name, on_rows, gated_rows, belief_on, meta, gate) -> str:
    on_cum = _cum([r["pf"] for r in on_rows])
    gat_cum = _cum([(r["pf"] if belief_on.get(r["date"], False) else r["bench"]) for r in gated_rows])
    bench_cum = _cum([r["bench"] for r in gated_rows])
    lines = [
        f"# 候选因子 OOS 门控验证：{name}",
        "",
        f"- 生成：{meta['generated_at']}",
        f"- 验证集：{meta['oos_start']} ~ 至今（{meta['n_oos_days']} 再平衡日）",
        f"- 网格：{meta['grid_days']} 交易日 × {meta['grid_codes']} 只",
        "",
        "## 一、三轨累计 OOS 收益（%）",
        "",
        "| 轨道 | 累计收益 | vs 基准 |",
        "|---|---|---|",
        f"| 基准（全宇宙等权） | {bench_cum:+.2f} | — |",
        f"| ON（始终持有因子 TOP{fe.TOP_N}） | {on_cum:+.2f} | {on_cum-bench_cum:+.2f}pp |",
        f"| GATED（信念 IC 正才持有） | {gat_cum:+.2f} | {gat_cum-bench_cum:+.2f}pp |",
        "",
        "## 二、稳健门（与生产 factor_timing 同源重规格）",
        "",
        "| 守卫 | 值 | 通过? |",
        "|---|---|---|",
        f"| 累计 OOS 改进 improve_pp（需≥{MIN_IMPROVE_PP}） | {gate['improve_pp']:+.2f} | {gate['checks']['improve_ok']} |",
        f"| 前后半段稳定 | {gate['stable']} | {gate['stable']} |",
        f"| 逐日改进单侧 t（t≥{T_CRIT}） | t={gate['checks']['sig_t_stat']} | {gate['significant']} |",
        f"| 跨 regime 稳健 | {gate['regime_robust']} | {gate['regime_robust']} |",
        f"| 换手率约束(avg_flip≤{MAX_AVG_FLIP}) | {gate['checks']['turnover_avg_flip']} | {gate['checks']['turnover_ok']} |",
        f"| **robust** | — | **{gate['robust']}** |",
        "",
        "## 三、判定",
        "",
        f"- **robust={gate['robust']}** → {'建议升 live 菜单（经 registry 编辑 + OOS 门控后）' if gate['robust'] else '不建议升 live：门控未通过，因子组合不可交易'}.",
        "",
        "> 口径：信念 IC = 因子日 IC（Spearman(因子排名, 前向收益排名)）的滚动均值；"
        "GATED 轨道在信念 IC 非重叠 t 检验显著为正时持有因子 TOP-N 组合，否则持基准。"
        "该稳健门与生产 factor_timing overlay 判定同一套标准，仅作用于单候选因子，不改任何配置。",
    ]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", default="a20_10", help="候选因子名（须存在于 factor_zoo 文法）")
    ap.add_argument("--load-start", default=fe.LOAD_START)
    ap.add_argument("--split-end", default=fz.SPLIT_END)
    ap.add_argument("--top-n", type=int, default=fe.TOP_N)
    ap.add_argument("--out", default=str(fe.OUT_DIR / "candidate_gate.md"))
    args = ap.parse_args()
    t0 = time.time()

    cands = fz.enumerate_candidates()
    c = next((x for x in cands if x.name == args.name), None)
    if c is None:
        print(f"no candidate named {args.name}", flush=True)
        return 1
    print(f"candidate: {c.name}  prior={'多高' if c.prior>0 else '多低'}  "
          f"load_start={args.load_start}  split_end={args.split_end}", flush=True)

    mats = fe.load_matrices(cols=("close", "high", "low", "open", "volume", "amount"),
                            load_start=args.load_start)
    close = mats["close"]
    ctx = {"close": close, "high": mats["high"], "low": mats["low"], "open": mats["open"],
           "volume": mats["volume"], "amount": mats["amount"]}
    del mats
    print(f"  grid {close.shape[0]} days x {close.shape[1]} codes ({time.time()-t0:.0f}s)", flush=True)

    ret1, bad = fe.daily_returns_and_bad(close)
    ctx["ret1"] = ret1
    fwd_bad = fe.forward_bad_mask(bad, fe.FWD)
    base_valid = fe.base_valid_mask(close)
    fwd = fe.forward_return_matrix(close, fwd_bad, base_valid, fe.FWD)
    fwd_rank = fe.forward_rank_matrix(fwd)
    idx = close.index
    split_ts = pd.Timestamp(args.split_end)
    pos = {d: i for i, d in enumerate(idx)}

    fac = fz.compute_factor(ctx, c)
    lb = c.lookback
    m = base_valid & fac.notna() & (fe.lookback_bad(bad, lb, {}) == 0)
    fac = fac.where(m)
    ic = fe.cross_sectional_ic(fac, fwd_rank, fe.MIN_N_DAY)
    ic_oos = ic[ic.index > split_ts].dropna()
    belief_on = _rolling_belief_on(ic_oos)

    oos_dates = [d for d in idx if d > split_ts]
    rebal = [d for d in oos_dates if pos[d] + fe.FWD < len(idx)][::fe.REBAL_EVERY]
    on_rows = _portfolio_rows(fac, fwd, base_valid, bad, fwd_bad, rebal, pos, c.prior, args.top_n)
    if not on_rows:
        print("no rebal day", flush=True)
        return 1
    gated_rows = on_rows  # 同一组再平衡日，GATED 用 belief_on 选轨道

    on_cum = _cum([r["pf"] for r in on_rows])
    bench_cum = _cum([r["bench"] for r in gated_rows])
    gat_ret = [(r["pf"] if belief_on.get(r["date"], False) else r["bench"]) for r in gated_rows]
    gat_cum = _cum(gat_ret)
    improve_pp = round(gat_cum - bench_cum, 2)

    half = max(1, len(gated_rows) // 2)
    first_ok = _cum([v for v in gat_ret[:half]]) > _cum([v for v in [r["bench"] for r in gated_rows[:half]]])
    second_ok = _cum([v for v in gat_ret[half:]]) > _cum([v for v in [r["bench"] for r in gated_rows[half:]]])
    stable = bool(first_ok and second_ok)

    daily_diff = [g - b for g, b in zip(gat_ret, [r["bench"] for r in gated_rows])]
    mean_diff = round(sum(daily_diff) / len(daily_diff), 4) if daily_diff else None
    sig = significance_report(daily_diff, n_trials=1, sr_benchmark=0.0, significance=0.05,
                              min_t_stat=T_CRIT)
    significant = bool(sig.get("significant")) and (mean_diff is not None and mean_diff > 0)

    # regime 分层（按年）
    by_year = {}
    for r in gated_rows:
        y = str(r["date"].year)
        by_year.setdefault(y, {"g": [], "b": []})
        by_year[y]["g"].append(r["pf"] if belief_on.get(r["date"], False) else r["bench"])
        by_year[y]["b"].append(r["bench"])
    regime_robust = all(_cum(v["g"]) > _cum(v["b"]) for v in by_year.values()) and len(by_year) >= 2

    n_on = sum(1 for r in gated_rows if belief_on.get(r["date"], False))
    avg_flip = round(1 - n_on / len(gated_rows), 4) if gated_rows else 0.0
    turnover_ok = bool(avg_flip <= MAX_AVG_FLIP)

    robust = (improve_pp >= MIN_IMPROVE_PP and stable and significant and regime_robust and turnover_ok)
    gate = {
        "improve_pp": improve_pp, "stable": stable, "significant": significant,
        "regime_robust": regime_robust,
        "turnover": {"avg_flip_fraction": avg_flip, "ok": turnover_ok},
        "robust": robust,
        "checks": {
            "improve_ok": improve_pp >= MIN_IMPROVE_PP,
            "sig_t_stat": sig.get("t_stat"), "turnover_avg_flip": avg_flip,
            "turnover_ok": turnover_ok,
        },
    }
    meta = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "oos_start": str((split_ts + pd.Timedelta(days=1)).date()),
        "n_oos_days": len(gated_rows), "grid_days": int(close.shape[0]),
        "grid_codes": int(close.shape[1]),
    }
    md = _md(c.name, on_rows, gated_rows, belief_on, meta, gate)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md, encoding="utf-8")

    print(f"ON={on_cum:+.2f}pp  GATED={gat_cum:+.2f}pp  BENCH={bench_cum:+.2f}pp  "
          f"improve={improve_pp:+.2f}pp  robust={robust}", flush=True)
    print(f"DONE in {time.time()-t0:.0f}s -> {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
