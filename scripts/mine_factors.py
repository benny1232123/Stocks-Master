#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""因子自动挖掘 CLI：枚举预注册文法候选 → 验证集口径筛选 → 出报告。

用法：
    python scripts/mine_factors.py                       # 全量（~100 候选）
    python scripts/mine_factors.py --smoke               # 冒烟（短历史 + 前 6 候选）
    python scripts/mine_factors.py --only mom20,vol20,skew20
    python scripts/mine_factors.py --max-candidates 40 --split-end 2024-06-30

输出：stock_data/factor_ic_replay/factor_zoo.md + factor_zoo.json

设计要点
- 数据准备全部走 smcore.strategy.factor_engine（与预注册 v1 回放同源），本脚本不重复实现；
  筛选纪律/文法/统计全部走 smcore.strategy.factor_zoo。
- 内存纪律：**逐个候选算完即释放**，只留 IC 序列（M × ~900 浮点，极小）。
  绝不缓存候选因子矩阵（100 × 4400 × 3400 float64 ≈ 12GB 会直接爆）。
- 只有「存活」候选会被重算一次，用于十分位单调性与 TOP50 组合超额（可交易性检查）。
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy import factor_engine as fe  # noqa: E402
from smcore.strategy import factor_zoo as fz  # noqa: E402


def _portfolio_oos(fac_masked: pd.DataFrame, fwd: pd.DataFrame, base_valid: pd.DataFrame,
                   bad: pd.DataFrame, fwd_bad: pd.DataFrame, days: list,
                   pos: dict, prior: int, top_n: int, cost_rt: float) -> dict:
    """验证集内单因子 TOP_N 多头组合（vs 全宇宙等权基准，扣往返成本 × 换手）。"""
    rets, bens, dates_out = [], [], []
    prev: set | None = None
    for d in days:
        if pos.get(d, 0) + fe.FWD >= len(fac_masked.index):
            continue
        fv = base_valid.loc[d] & (bad.loc[d] == 0) & (fwd_bad.loc[d] == 0)
        col = fac_masked.loc[d]
        cand = col[fv & col.notna()].dropna()
        if len(cand) < top_n * 2:
            prev = None
            continue
        fr = fwd.loc[d]
        bench_vals = fr[fv & fr.notna()]
        if bench_vals.empty:
            prev = None
            continue
        top = cand.nlargest(top_n) if prior > 0 else cand.nsmallest(top_n)
        held = set(top.index)
        fr_h = fr.reindex(list(held)).dropna()
        if len(fr_h) < 10:
            prev = None
            continue
        cost = cost_rt if prev is None else cost_rt * len(prev ^ held) / max(1, len(held))
        rets.append(float(fr_h.mean()) - cost)
        bens.append(float(bench_vals.mean()))
        dates_out.append(d)
        prev = held
    if len(rets) < 5:
        return {"n_windows": len(rets)}
    p = pd.Series(rets, index=pd.DatetimeIndex(dates_out))
    b = pd.Series(bens, index=pd.DatetimeIndex(dates_out))
    yrs = len(p) * fe.FWD / 244.0
    tot_p, tot_b = float((1 + p).prod() - 1), float((1 + b).prod() - 1)
    by_year = {}
    for y in sorted(set(p.index.year)):
        py, by = p[p.index.year == y], b[b.index.year == y]
        by_year[str(y)] = round(float(((1 + py).prod() - (1 + by).prod()) * 100), 2)
    return {
        "n_windows": len(p),
        "total_ret_pp": round(tot_p * 100, 1),
        "bench_ret_pp": round(tot_b * 100, 1),
        "total_excess_pp": round((tot_p - tot_b) * 100, 1),
        "annual_excess_pp": round(((1 + tot_p) ** (1 / yrs) - (1 + tot_b) ** (1 / yrs)) * 100, 2)
        if yrs > 0 else 0.0,
        "win_rate": round(float((p - b > 0).mean()), 3),
        "by_year": by_year,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-candidates", type=int, default=fz.MAX_CANDIDATES)
    ap.add_argument("--split-end", default=fz.SPLIT_END)
    ap.add_argument("--load-start", default=fe.LOAD_START)
    ap.add_argument("--only", default=None, help="逗号分隔的候选名（调试用）")
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--smoke", action="store_true", help="冒烟：短历史 + 前 6 候选")
    ap.add_argument("--no-portfolio", action="store_true", help="跳过存活因子的组合/分位诊断")
    ap.add_argument("--regen", action="store_true",
                    help="只从已有 factor_zoo.json 重渲染 factor_zoo.md（改文案/加列不必重跑）")
    args = ap.parse_args()

    t0 = time.time()
    out_dir = Path(args.out_dir) if args.out_dir else fe.OUT_DIR
    if args.regen:
        src = out_dir / "factor_zoo.json"
        if not src.exists():
            print(f"no {src}", flush=True)
            return 1
        data = json.loads(src.read_text(encoding="utf-8"))
        (out_dir / "factor_zoo.md").write_text(
            fz.format_report(data["factors"], data.get("meta", {})), encoding="utf-8")
        print(f"REGEN -> {out_dir / 'factor_zoo.md'}", flush=True)
        return 0
    if args.smoke:
        args.max_candidates = 6
        args.load_start = "2021-06-01"
        args.split_end = "2024-06-30"

    cands = fz.enumerate_candidates(args.max_candidates)
    if args.only:
        keep = {s.strip() for s in args.only.split(",") if s.strip()}
        cands = [c for c in cands if c.name in keep]
    if not cands:
        print("no candidate", flush=True)
        return 1
    print(f"candidates: {len(cands)}  load_start={args.load_start}  "
          f"split_end={args.split_end}", flush=True)

    # ── 数据准备（全部走共享引擎）──────────────────────────────────
    print("load...", flush=True)
    mats = fe.load_matrices(cols=("close", "high", "low", "open", "volume", "amount"),
                            load_start=args.load_start)
    close, high, low = mats["close"], mats["high"], mats["low"]
    ctx = {"close": close, "high": high, "low": low, "open": mats["open"],
           "volume": mats["volume"], "amount": mats["amount"]}
    del mats
    print(f"  grid {close.shape[0]} days x {close.shape[1]} codes  ({time.time()-t0:.0f}s)", flush=True)

    ret1, bad = fe.daily_returns_and_bad(close)
    ctx["ret1"] = ret1
    n_bad = int(bad.values.sum())
    lb_cache: dict = {}
    fwd_bad = fe.forward_bad_mask(bad, fe.FWD)
    base_valid = fe.base_valid_mask(close)
    fwd = fe.forward_return_matrix(close, fwd_bad, base_valid, fe.FWD)
    fwd_rank = fe.forward_rank_matrix(fwd)

    idx = close.index
    split_ts = pd.Timestamp(args.split_end)
    win_ts = pd.Timestamp(fe.WINDOW_START)
    pos = {d: i for i, d in enumerate(idx)}

    # ── 逐候选：算 → 评估 → 释放 ───────────────────────────────────
    print("evaluating...", flush=True)
    results: dict[str, dict] = {}
    ic_oos_map: dict[str, pd.Series] = {}
    for i, c in enumerate(cands, start=1):
        try:
            fac = fz.compute_factor(ctx, c)
        except Exception as e:  # fail-soft：单候选失败不拖垮整轮
            print(f"  [{i}/{len(cands)}] {c.name}: SKIP ({e})", flush=True)
            continue
        lb = c.lookback
        m = base_valid & fac.notna() & (fe.lookback_bad(bad, lb, lb_cache) == 0)
        fac = fac.where(m)
        ic = fe.cross_sectional_ic(fac, fwd_rank, fe.MIN_N_DAY)
        ic_is = ic[(ic.index >= win_ts) & (ic.index <= split_ts)]
        ic_oos = ic[ic.index > split_ts]
        st = fz.ic_stats(ic_oos, ic_is)
        results[c.name] = st
        ic_oos_map[c.name] = ic_oos.dropna()
        del fac
        print(f"  [{i}/{len(cands)}] {c.name:<16} n_eval={st.get('n_eval'):>4} "
              f"IC={st.get('full_mean_ic')} ICIR={st.get('icir')} "
              f"({time.time()-t0:.0f}s)", flush=True)

    if not results:
        print("no result", flush=True)
        return 1

    # ── 多重检验 + 冗余剔重 + 裁决 ─────────────────────────────────
    rows = fz.build_rows(cands, results)
    # 剔除样本不足到无法检验的行（p 无意义）后再做多重检验，避免污染家族错误率
    testable = [r for r in rows if (r.get("n_eval") or 0) >= 2]
    pvals = [float(r.get("p_value") or 1.0) for r in testable]
    fdr = fz.benjamini_hochberg(pvals, fz.FDR_Q)
    bonf = fz.bonferroni(pvals, fz.BONFERRONI_ALPHA)
    verdict = {r["name"]: (f, b) for r, f, b in zip(testable, fdr, bonf)}

    ic_frame = pd.DataFrame(ic_oos_map)
    anchors = {c.name for c in cands if fz.is_baseline(c.name)}
    fz.prune_redundant(rows, ic_frame, fz.REDUNDANCY_RHO, anchors=anchors)

    for r in rows:
        f, b = verdict.get(r["name"], (False, False))
        v, why = fz.judge(r, f, b, r["prior"])
        r["verdict"], r["reason"] = v, why

    alive = [r for r in rows if r["verdict"].startswith("存活")]
    print(f"alive: {len(alive)}  -> {[r['name'] for r in alive]}", flush=True)

    # ── 存活因子的可交易性诊断（重算因子，逐候选释放）──────────────
    oos_dates = [d for d in idx if d > split_ts]
    rebal = [d for d in oos_dates if pos[d] + fe.FWD < len(idx)][::fe.REBAL_EVERY]
    if not args.no_portfolio:
        for n, r in enumerate(alive, start=1):
            c = next(x for x in cands if x.name == r["name"])
            try:
                fac = fz.compute_factor(ctx, c)
                fac = fac.where(base_valid & fac.notna()
                                & (fe.lookback_bad(bad, c.lookback, lb_cache) == 0))
                dec, used = fe.decile_means(fac, fwd, base_valid, rebal,
                                            n_decile=10, min_n_day=fe.MIN_N_DAY)
                r["deciles"] = {"ann_pp": [
                    None if np.isnan(v) else round(float(v * (244.0 / fe.FWD) * 100), 1) for v in dec],
                    "n_days": used}
                ok = [i for i, v in enumerate(dec) if not np.isnan(v)]
                r["deciles"]["mono_rho"] = (
                    round(float(np.corrcoef(pd.Series(np.arange(1, 11)[ok]).rank(),
                                            pd.Series([dec[i] for i in ok]).rank())[0, 1]), 2)
                    if len(ok) >= 5 else None)
                r["portfolio"] = _portfolio_oos(fac, fwd, base_valid, bad, fwd_bad, rebal,
                                                pos, r["prior"], fe.TOP_N, fe.COST_RT)
                del fac
                print(f"  diag [{n}/{len(alive)}] {r['name']} "
                      f"mono={r['deciles']['mono_rho']} "
                      f"oos_excess={r['portfolio'].get('total_excess_pp')}pp", flush=True)
            except Exception as e:
                r["diag_error"] = str(e)
                print(f"  diag {r['name']} FAILED: {e}", flush=True)

    # ── 输出 ───────────────────────────────────────────────────────
    out_dir.mkdir(parents=True, exist_ok=True)
    meta = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "n_candidates": len(rows),
        "is_start": str(max(pd.Timestamp(args.load_start), win_ts).date()),
        "oos_start": str((split_ts + pd.Timedelta(days=1)).date()),
        "grid_days": int(close.shape[0]), "grid_codes": int(close.shape[1]),
        "corrupted_bars": n_bad,
        "rules": {"fdr_q": fz.FDR_Q, "bonferroni_alpha": fz.BONFERRONI_ALPHA,
                  "min_ic_abs": fz.MIN_IC_ABS, "stable_frac": fz.STABLE_FRAC,
                  "redundancy_rho": fz.REDUNDANCY_RHO,
                  "min_oos_eval_days": fz.MIN_OOS_EVAL_DAYS,
                  "eval_step": fe.REBAL_EVERY, "fwd": fe.FWD, "cost_rt": fe.COST_RT},
    }
    report = fz.format_report(rows, meta)
    plain = [{k: v for k, v in r.items()} for r in rows]
    (out_dir / "factor_zoo.json").write_text(
        json.dumps({"meta": meta, "factors": plain}, ensure_ascii=False, indent=1, default=str),
        encoding="utf-8")
    (out_dir / "factor_zoo.md").write_text(report, encoding="utf-8")
    print(f"DONE in {time.time()-t0:.0f}s -> {out_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
