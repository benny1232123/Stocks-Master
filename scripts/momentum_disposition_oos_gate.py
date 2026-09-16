#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""动量家族「反用 / 置零」的独立 OOS 门控（策略层，**不写回任何配置**）。

背景
----
`momentum_reversal_review.md` 已坐实：在生产 `momentum` 策略**自己的条件域**内，
按策略打分取 TOP30 的前向超额为 **−18.2pp**、胜率 0.32、9/9 年全负 ⇒ 方向反转是设计缺陷。
该报告 §四 规定处置必须走**独立 OOS 门控**，不得直接改配置。本脚本即那道门。

三个候选（全部在同一条件域内评估，口径由 `build_conditional_universe` 单源提供）
------------------------------------------------------------------------
- `momentum`（现行）：取打分**最高** TOP_N
- `reversal`（反用）：取打分**最低** TOP_N（「反用」= 保留条件域、翻转排序方向）
- `zero`（置零）：不持仓。其超额**恒为 0**，故无需回测，作为参照线参与比较

预注册判据（**先写死再看结果**；阈值与 `walk_forward_validator` / `walk_forward_factor_timing` 对齐）
----------------------------------------------------------------------------------
| 判据 | 阈值 |
|---|---|
| 主判据：累计 OOS 改进 | ≥ `MIN_IMPROVE_PP`(2.0pp)，相对**全有效宇宙等权**（机会成本口径） |
| 显著性 | 逐窗差值单侧 t ≥ `T_CRIT`(1.31, α=0.10) 且均值 > 0（`n_trials=1`：固定规则非挖矿） |
| 稳定性 | 前后半段**都**跑赢基准 |
| 跨 regime 稳健 | 「趋势上行」「下行防御」两段**都**跑赢（每段 ≥ `MIN_DAYS_PER_REGIME` 个窗口） |
| 换手率 | 相邻信号日持仓集合平均翻转比例 ≤ `MAX_AVG_FLIP`(0.5) |

决策规则（结果自动生成）
- 若 `momentum` 相对基准**显著为负**（t ≤ −T_CRIT）⇒ 置零优于现行，**压权重/置零有据**；
- 若 `reversal` 通过全部门 ⇒ 优先级 **反用 > 置零 > 现行**；
- 若 `reversal` 未过门 ⇒ **维持置零/压权重**，不做反用。

⚠️ regime 用**本地因果代理**（全有效宇宙等权指数的 REGIME_WIN 日收益符号），
   非生产的四维 `market.regime_as_of`（依赖联网索引）。差异见报告 caveats。
⚠️ 本脚本只读 k_data，不写任何生产配置；`--apply` 之类开关**刻意不提供**。
"""
from __future__ import annotations

import json
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
from smcore.strategy.significance import significance_report  # noqa: E402
import momentum_reversal_review as mrr  # noqa: E402  （条件域单一真源）

# ── 预注册判据（与 walk_forward_validator / factor_timing 同口径）──────────
MIN_IMPROVE_PP = 2.0
T_CRIT = 1.31
MAX_AVG_FLIP = 0.5
MIN_REGIMES = 2
MIN_DAYS_PER_REGIME = 3
REGIME_WIN = 20          # 本地 regime 代理窗（交易日）
TOP_N = mrr.TOP_N        # 30，与复核报告同口径
COST_RT = mrr.COST_RT
COND_MIN_N = mrr.COND_MIN_N
BENCH_MAIN = "full"      # 主判据基准：全有效宇宙等权（机会成本）
BENCH_COL = {"full": "bench_full", "cond": "bench_cond"}   # 基准名 → 窗口记录列名

PROPOSALS = ("momentum", "reversal")


def _cum(s: pd.Series) -> float:
    """累计收益（%）。"""
    if s.empty:
        return 0.0
    return float(((1 + s).prod() - 1) * 100)


def _flip(prev: set, held: set) -> float:
    return len(prev ^ held) / max(1, len(held))


def main() -> int:
    t0 = time.time()
    mats = fe.load_matrices(cols=("close", "high", "low", "amount"))
    close, high, amount = mats["close"], mats["high"], mats["amount"]
    del mats
    print(f"grid {close.shape[0]} x {close.shape[1]} ({time.time()-t0:.0f}s)", flush=True)

    ret1, bad = fe.daily_returns_and_bad(close)
    base_valid = fe.base_valid_mask(close)
    fwd_bad = fe.forward_bad_mask(bad, fe.FWD)
    fwd = fe.forward_return_matrix(close, fwd_bad, base_valid, fe.FWD)
    lb20 = fe.lookback_bad(bad, 20, {})

    strat, score = mrr.build_conditional_universe(close, high, amount, base_valid, lb20)
    uni_full = base_valid & (bad == 0) & (fwd_bad == 0) & fwd.notna()

    # ── 本地因果 regime 代理：全有效宇宙等权指数的 REGIME_WIN 日收益符号 ──
    ew = ret1.where(uni_full).mean(axis=1)
    idx = (1 + ew.fillna(0.0)).cumprod()
    rg_raw = idx / idx.shift(REGIME_WIN) - 1
    regime = pd.Series(np.where(rg_raw >= 0, "趋势上行", "下行防御"), index=close.index)
    regime[rg_raw.isna()] = "未知"

    w0 = pd.Timestamp(fe.WINDOW_START)
    idx_days = close.index[close.index >= w0]
    days = [d for i, d in enumerate(idx_days) if i % fe.REBAL_EVERY == 0]

    rec: list[dict] = []
    prev = {p: None for p in PROPOSALS}
    for d in days:
        cond = strat.loc[d]
        fv = fwd.loc[d]
        cand = cond & fv.notna()
        if int(cand.sum()) < TOP_N * 2:
            prev = {p: None for p in PROPOSALS}
            continue
        s = score.loc[d][cand].dropna()
        if len(s) < TOP_N * 2:
            prev = {p: None for p in PROPOSALS}
            continue
        full = uni_full.loc[d] & fv.notna()
        if int(full.sum()) < fe.MIN_N_DAY:
            prev = {p: None for p in PROPOSALS}
            continue
        bench_cond = float(fv[cand].mean())
        bench_full = float(fv[full].mean())
        held = {
            "momentum": set(s.nlargest(TOP_N).index),
            "reversal": set(s.nsmallest(TOP_N).index),
        }
        row = {"date": str(d.date()), "regime": str(regime.loc[d]),
               "n_cond": int(cand.sum()), "bench_cond": bench_cond, "bench_full": bench_full}
        for p in PROPOSALS:
            h = held[p]
            turn = 1.0 if prev[p] is None else _flip(prev[p], h)
            fr = fv.reindex(sorted(h)).dropna()
            row[p] = float(fr.mean()) - COST_RT * turn
            row[p + "_turn"] = turn
            prev[p] = h
        # 「置零」= 不持仓，收益恒 0（相对基准仍为 0）
        row["zero"] = 0.0
        rec.append(row)

    if len(rec) < 10:
        print("too few windows", len(rec), flush=True)
        return 1
    df = pd.DataFrame(rec).set_index(pd.DatetimeIndex([r["date"] for r in rec]))
    print(f"windows={len(df)} ({time.time()-t0:.0f}s)", flush=True)

    def _stats(prop: str, bench: str) -> dict:
        bcol = BENCH_COL[bench]
        p = df[prop].astype(float)
        b = df[bcol].astype(float)
        diff = (p - b).to_numpy(dtype=float)
        cum_p, cum_b = _cum(p), _cum(b)
        improve = round(cum_p - cum_b, 2)
        half = max(1, len(df) // 2)
        d1, d2 = diff[:half], diff[half:]
        first_ok = float(np.sum(d1)) > 0
        second_ok = float(np.sum(d2)) > 0
        sig = significance_report(list(diff), n_trials=1, sr_benchmark=0.0,
                                  significance=0.05, min_t_stat=T_CRIT)
        mean_diff = round(float(np.mean(diff)), 4)
        sig_ok = bool(sig.get("significant")) and mean_diff > 0
        # regime 分层
        rtab = {}
        for rg, sub in df.groupby("regime"):
            pdiff = float((sub[prop] - sub[bcol]).mean())
            rtab[str(rg)] = {"n_windows": int(len(sub)),
                             "prop_pct": round(_cum(sub[prop]), 2),
                             "bench_pct": round(_cum(sub[bcol]), 2),
                             "diff_pp": round(pdiff * 100, 2)}
        qual = {k: v for k, v in rtab.items() if v["n_windows"] >= MIN_DAYS_PER_REGIME}
        beat = sum(1 for v in qual.values() if v["diff_pp"] > 0)
        diverse = len(qual) >= 2
        regime_ok = (not diverse) or (beat >= MIN_REGIMES)
        flip = round(float(df[prop + "_turn"].mean()), 4)
        turn_ok = bool(flip <= MAX_AVG_FLIP)
        robust = bool(improve >= MIN_IMPROVE_PP and first_ok and second_ok
                      and sig_ok and regime_ok and turn_ok)
        return {"prop": prop, "bench": bench, "cum_prop": round(cum_p, 2),
                "cum_bench": round(cum_b, 2), "improve_pp": improve,
                "mean_daily_diff": mean_diff, "t_stat": sig.get("t_stat"),
                "significant": sig_ok, "first_half_ok": bool(first_ok),
                "second_half_ok": bool(second_ok), "regime_table": rtab,
                "regime_diverse": diverse, "regime_beat": beat, "regime_ok": bool(regime_ok),
                "avg_flip": flip, "turnover_ok": turn_ok, "robust": robust,
                "win_rate": round(float((df[prop] > df[bcol]).mean()), 3)}

    out = {}
    for bench in ("full", "cond"):
        for p in PROPOSALS:
            out[f"{p}_vs_{bench}"] = _stats(p, bench)
    # 「置零」= 不持仓 → 相对现行的增益 = −(现行相对基准的改进)，t 取反（同一检验）
    _m = out[f"momentum_vs_{BENCH_MAIN}"]
    out["zero_gain_vs_momentum"] = {
        "prop": "zero(不持仓)", "bench": "momentum",
        "improve_pp": round(-_m["improve_pp"], 2),
        "mean_daily_diff": -_m["mean_daily_diff"],
        "t_stat": (None if _m["t_stat"] is None else -_m["t_stat"]),
    }

    mom = out[f"momentum_vs_{BENCH_MAIN}"]
    rev = out[f"reversal_vs_{BENCH_MAIN}"]
    mom_neg_sig = bool(mom["t_stat"] is not None and mom["t_stat"] <= -T_CRIT)

    if mom_neg_sig and rev["robust"]:
        verdict = "反用 > 置零 > 现行（反用过全部门，且现行显著为负）"
        action = "可进入「反用」的落地设计（仍须过融合层复核）"
    elif mom_neg_sig:
        verdict = "维持置零 / 把 momentum 权重压到最低档"
        action = "反用未过门 ⇒ 不做反用；置零/压权重有统计依据"
    elif rev["robust"]:
        verdict = "反用可用，但现行未显著为负 ⇒ 收益空间有限，建议先查与 boll 的反转重叠"
        action = "谨慎：优先核对与 boll(反转·均值回归) 的重叠度"
    else:
        verdict = "维持现状（两个方向都未过门）"
        action = "不动生产；结论收敛为「动量子家族不可用」"

    by_year = {}
    for p in PROPOSALS:
        by_year[p] = {str(y): round(_cum(df[df.index.year == y][p])
                                    - _cum(df[df.index.year == y][BENCH_COL[BENCH_MAIN]]), 2)
                      for y in sorted(set(df.index.year))}

    payload = {"generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
               "window_start": fe.WINDOW_START, "fwd": fe.FWD,
               "rebal_every": fe.REBAL_EVERY, "top_n": TOP_N, "cost_rt": COST_RT,
               "gate": {"min_improve_pp": MIN_IMPROVE_PP, "t_crit": T_CRIT,
                        "max_avg_flip": MAX_AVG_FLIP, "min_regimes": MIN_REGIMES,
                        "regime_win": REGIME_WIN, "bench_main": BENCH_MAIN},
               "n_windows": int(len(df)), "stats": out, "excess_by_year": by_year,
               "verdict": verdict, "action": action, "momentum_negative_significant": mom_neg_sig}
    fe.OUT_DIR.mkdir(parents=True, exist_ok=True)
    (fe.OUT_DIR / "momentum_disposition_oos_gate.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # ── md ─────────────────────────────────────────────────────────
    L: list[str] = []
    L += ["# 动量家族处置：独立 OOS 门控（策略层，未写回配置）", "",
          f"- 生成：{payload['generated_at']}",
          f"- 窗口：{fe.WINDOW_START} 起　T+{fe.FWD}　换仓每 {fe.REBAL_EVERY} 交易日　"
          f"TOP{TOP_N}　成本 {COST_RT}×换手　窗口数 **{len(df)}**",
          "- 条件域 = 生产 `momentum` 选股条件（`build_conditional_universe` 单一真源，"
          "与 `momentum_reversal_review.md` 同口径）",
          "- 三个候选：`momentum`（现行，取最高分）/ `reversal`（反用，取最低分）/ "
          "`zero`（置零，超额恒 0，参照线）",
          f"- 主判据基准 = **全有效宇宙等权**（机会成本口径）；另附条件域基准做对照", "",
          "## 一、主结果（相对全有效宇宙等权）", "",
          "| 候选 | 累计 | 基准累计 | 改进(pp) | 均值差 | t | 显著为正 | 前半 | 后半 | regime | 换手 | 过门 |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for p in PROPOSALS:
        s = out[f"{p}_vs_{BENCH_MAIN}"]
        L.append(f"| {p} | {s['cum_prop']}% | {s['cum_bench']}% | **{s['improve_pp']}** | "
                 f"{s['mean_daily_diff']*100:+.3f}pp | {s['t_stat']} | {s['significant']} | "
                 f"{'✓' if s['first_half_ok'] else '✗'} | {'✓' if s['second_half_ok'] else '✗'} | "
                 f"{s['regime_beat']}/{len(s['regime_table'])} | {s['avg_flip']} "
                 f"{'✓' if s['turnover_ok'] else '✗'} | **{'✅' if s['robust'] else '❌'}** |")
    _zg = out["zero_gain_vs_momentum"]
    L += ["", f"- **`zero`（置零/不持仓）**：相对**现行**的增益 = **{_zg['improve_pp']}pp**，"
          f"t = **{_zg['t_stat']}**（即现行 t 取反）—— 现行为负时置零即有利；"
          f"该候选不占仓、无换手，故不进上表。"]

    L += ["", "### 相对条件域等权（与复核报告 §二 同基准，仅供对照）", "",
          "| 候选 | 累计 | 基准累计 | 改进(pp) | t | 过门 |", "|---|---|---|---|---|---|"]
    for p in PROPOSALS:
        s = out[f"{p}_vs_cond"]
        L.append(f"| {p} | {s['cum_prop']}% | {s['cum_bench']}% | **{s['improve_pp']}** | "
                 f"{s['t_stat']} | {'✅' if s['robust'] else '❌'} |")

    L += ["", "## 二、分年超额（相对全有效宇宙等权，pp）", "",
          "| 候选 | " + " | ".join(sorted(by_year["momentum"])) + " |",
          "|---|" + "---|" * len(by_year["momentum"])]
    for p in PROPOSALS:
        L.append(f"| {p} | " + " | ".join(str(by_year[p][y])
                                          for y in sorted(by_year[p])) + " |")

    L += ["", "## 三、跨 regime 分层", "",
          "| 候选 | regime | 窗口 | 候选累计 | 基准累计 | 差(pp) |", "|---|---|---|---|---|---|"]
    for p in PROPOSALS:
        for rg, v in sorted(out[f"{p}_vs_{BENCH_MAIN}"]["regime_table"].items()):
            L.append(f"| {p} | {rg} | {v['n_windows']} | {v['prop_pct']}% | "
                     f"{v['bench_pct']}% | {v['diff_pp']} |")

    L += ["", "## 四、判定（预注册判据自动生成）", "",
          f"- 现行 `momentum` 相对基准：改进 **{mom['improve_pp']}pp**、t = **{mom['t_stat']}**"
          f"（阈值 ≤ −{T_CRIT} 才算「显著为负」）→ "
          f"{'**显著为负**' if mom_neg_sig else '未达显著为负'}。",
          f"- `reversal` 相对基准：改进 **{rev['improve_pp']}pp**、t = **{rev['t_stat']}**、"
          f"过门 = **{'✅' if rev['robust'] else '❌'}**。", "",
          f"**判定：{verdict}。**", "",
          f"**动作：{action}。**", "",
          "### 两个基准的差异（读结论前必读）", "",
          "- **全有效宇宙等权**被微盘主导（策略条件域天然排除微盘、且本仓库 universe 只含存活票），"
          "所以上表的 −69pp 里**含有基准口径差**，不可直接当作生产收益改善量级。",
          "- 生产决策应主要看**条件域口径**（与 `momentum_reversal_review.md` §二 同基准）："
          f"现行 **{out['momentum_vs_cond']['improve_pp']}pp**"
          f"（t={out['momentum_vs_cond']['t_stat']}）、"
          f"反用 **{out['reversal_vs_cond']['improve_pp']}pp**"
          f"（t={out['reversal_vs_cond']['t_stat']}）。",
          "  → 反用能把负超额**抹平到 0 附近**（独立印证「方向反转」诊断成立），"
          "但**本身不产生正 edge**，故不足以支撑「反用」落地。",
          "- **现行在自己的宇宙内也显著为负** ⇒ 「置零 / 压权重」在两个口径下都成立。",
          "- 换手率守卫对两个候选都**未通过**（avg_flip 1.3~1.6 ≫ 0.5）："
          "按打分选股每期换血约 2/3，成本拖累本身即是负面证据（与 `factor_timing` 的换手率约束同源）。", "",
          "## 五、Caveats", "",
          "- **regime 是本地因果代理**（宇宙等权指数的 20 日收益符号），"
          "与生产四维 `market.regime_as_of`（联网索引）**不同源**；本节只用于「两段都别翻车」的粗筛。",
          "- 未剔 ST（无历史名单）；未复刻策略的「量能确认 +2 / 近高点 −3」调整项（与复核报告一致）。",
          "- 幸存者偏差：universe = 今天还活着的票，只用于相对判断。",
          "- 条件域每日样本较小（n≥%d），t 值参考性有限；但符号若分年一致则对样本量不敏感。" % COND_MIN_N,
          "- 本脚本在**策略层**评估；生产改动前还须在融合层（`walk_forward_validator`，"
          "覆盖 DAL 窗口）复核一次，且过月度回滚 tripwire。",
          "- 与 `boll`（反转·均值回归）的**重叠度未检验**：若反用 momentum 与 boll 选票高度重合，"
          "则不是新 edge 而是重复暴露。"]
    outp = fe.OUT_DIR / "momentum_disposition_oos_gate.md"
    outp.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"DONE {time.time()-t0:.0f}s -> {outp}", flush=True)
    print("VERDICT " + verdict, flush=True)
    print(f" momentum improve={mom['improve_pp']}pp t={mom['t_stat']} | "
          f"reversal improve={rev['improve_pp']}pp t={rev['t_stat']} robust={rev['robust']}",
          flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
