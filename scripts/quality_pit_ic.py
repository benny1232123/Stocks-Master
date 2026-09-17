#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""C 批 P0：Quality 维度 PIT IC 可行性验证（**只加一个因子，不动菜单/CI/配置**）。

背景
- Phase 2 已证「纯 OHLCV 文法饱和」（123 候选仅 a20_10 新存活，且 OOS 门控判不可交易）
  → 要扩**正交**维度只能靠基本面。本脚本回答「Quality 维度在我们现有 PIT 数据上有没有 edge」。
- 数据：`stock_data/fundamental_cache/` 的 **v2** 布局（`periods` + 真实公告日 `_pub`）；
  实测 301/4380 只有 v2（`periods.roe` 301/301、`gross_margin` 280/301），
  PIT 生效最早 2020-06（报告期 2020-03-31 + 法定披露滞后）。

════════════════════════════════════════════════════════════════════════════
预注册判据（**先写死，再跑**；全部集中在文件头，禁散落 magic number）
════════════════════════════════════════════════════════════════════════════
因子定义
  F_Quality      = ½·[ z_win(ROE 年化) + z_win(毛利率) ]  （两分量**都必须非空**，截面 z 前按 [1%,99%] 缩尾）
  F_Quality_rank = ½·[ cs_rank(ROE 年化) + cs_rank(毛利率) ]（稳健性对照；**不作判据**）
  ROE 年化一律走 `fundamental.annualize_roe`（与生产同源），取值一律走 PIT `_select_pit_period_keyed`。

口径
  MIN_N_DAY_C = 150      截面下限 = PIT 覆盖宇宙(301) 的 ≥50%（**不能用 A 批的 300**：那是 4380 宇宙的 7%）
  SPLIT_END   = fz.SPLIT_END (2022-12-31)   发现集 ≤ split_end，验证集 > split_end
  MIN_OOS_EVAL_DAYS = fz.MIN_OOS_EVAL_DAYS (40)  验证集**非重叠**(每 REBAL_EVERY 取 1)样本下限
  MIN_IC_ABS  = fz.MIN_IC_ABS (0.010)     存活所需最小 |均值 IC|
  STABLE_FRAC = fz.STABLE_FRAC (2/3)      同号率下限
  T_CRIT      = 1.31                      单侧 α=0.10、n_trials=1（与 walk_forward_factor_timing 同源）
  PRIOR       = +1                        Quality 先验方向为正（ROE/毛利率越高越好）

主检验（**决定性**）：基本面因子的 IC 序列是「随报告期披露跳变的阶梯函数」，
  朴素/非重叠 t 都仍被「同一份财报值沿用数十个交易日」严重自相关 →
  必须**按报告期聚类**：以每信号日**截面众数生效报告期**为簇，簇内先平均、再对簇均值做单侧 t。
  → 判据 D5 = 聚类单侧 t ≥ T_CRIT；D6 = 聚类同号率 ≥ STABLE_FRAC。

裁定（D1–D6 **全部**满足 ⇒ 「值得进 P1/P2」；任一不满足 ⇒ 「C 批 Quality 无 edge，暂缓」）
  D1 n_eval ≥ MIN_OOS_EVAL_DAYS
  D2 |full_mean_ic| ≥ MIN_IC_ABS
  D3 seg_match is not False（发现集/验证集均值 IC 同号）
  D4 full_mean_ic > 0（无方向反转）
  D5 聚类单侧 t ≥ T_CRIT
  D6 聚类同号率 ≥ STABLE_FRAC

可交易性诊断（**次要、不作主判据**，但必须给两条基准 —— 见报告口径纪律）
  TOP_N_C = 30；bench_full = 全有效宇宙(≈4380)等权；bench_cond = **同宇宙**(因子有效截面)等权。
  ⇒ **决策以 bench_cond 为主**（bench_full 被微盘主导，而基本面覆盖盘天然缺微盘，含口径差）。

本脚本**不写任何生产配置**，不提供 --apply。产出：stock_data/factor_ic_replay/quality_pit_ic.{md,json}
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import Counter
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy import factor_engine as fe  # noqa: E402
from smcore.strategy import factor_zoo as fz     # noqa: E402
from smcore.strategy import fundamental as fu    # noqa: E402

# ── 预注册常数（唯一定义处）─────────────────────────────────────────────
MIN_N_DAY_C = 150
SPLIT_END = fz.SPLIT_END
MIN_OOS_EVAL_DAYS = fz.MIN_OOS_EVAL_DAYS
MIN_IC_ABS = fz.MIN_IC_ABS
STABLE_FRAC = fz.STABLE_FRAC
T_CRIT = 1.31
PRIOR = 1
TOP_N_C = 30
WINSOR_LO, WINSOR_HI = 0.01, 0.99
FACTOR_NAME = "quality_pit"


# ── PIT 取值：把 v2 缓存转成「随公告日跳变的阶梯序列」 ────────────────────
def _code_steps(data: dict, grid: pd.DatetimeIndex):
    """单个 code 的 PIT 阶梯 → (roe_ann, gross_margin, period_id) 三个 np 数组。

    语义与 `fu._select_pit_period_keyed(periods, d)` **逐日等价**：
    取「可用日(真实公告日优先，缺则法定滞后兜底) ≤ d」中报告期最大的那一期。
    实现上用「可用日断点 + 前向填充」避免 66 万次函数调用，但**可用日一律调
    `fu._period_available_date`**，不复制逻辑。
    """
    periods = data.get("periods") or {}
    items = []
    for pe, rec in periods.items():
        if not isinstance(rec, dict):
            continue
        a = fu._period_available_date(pe, rec.get("_pub"))
        if a is None:
            continue
        items.append((pe, a, rec))
    if not items:
        return None
    breaks = sorted({a for _, a, _ in items})
    roe_steps, gm_steps, pid_steps = [], [], []
    for t in breaks:
        best_pe, best_rec = None, None
        for pe, a, rec in items:
            if a <= t and (best_pe is None or pe > best_pe):
                best_pe, best_rec = pe, rec
        if best_rec is None:
            roe_steps.append(np.nan)
            gm_steps.append(np.nan)
            pid_steps.append(-1)
            continue
        r = fu.annualize_roe(best_rec.get("roe"), best_pe)
        g = best_rec.get("gross_margin")
        roe_steps.append(float(r) if r is not None else np.nan)
        gm_steps.append(float(g) if g is not None else np.nan)
        pid_steps.append(best_pe)
    # ⚠️ 必须 side="left"（首个 grid 日 ≥ 可用日）。
    # 用 side="right" - 1 会把「周六/节假日公告」错配到**前一个交易日** →
    # 该报告提前 1–3 天生效 = 未来函数（2026-09-17 逐日等价校验实测 33/151 不符）。
    pos = grid.searchsorted(pd.DatetimeIndex([pd.Timestamp(b) for b in breaks]), side="left")
    n = len(grid)
    roe = np.full(n, np.nan)
    gm = np.full(n, np.nan)
    pid = np.full(n, -1, dtype=object)
    for i, p in enumerate(pos):
        if p < 0:
            continue
        lo = p
        hi = pos[i + 1] if i + 1 < len(pos) else n
        roe[lo:hi] = roe_steps[i]
        gm[lo:hi] = gm_steps[i]
        pid[lo:hi] = pid_steps[i]
    return roe, gm, pid


def build_pit_matrices(grid: pd.DatetimeIndex, codes: list[str], cache_dir: Path):
    """读全部 v2 缓存 → (ROE, GM, PID) 三个 DataFrame（index=grid, columns=命中 code）。"""
    roe_cols, gm_cols, pid_cols, used = {}, {}, {}, []
    n_v2 = n_v1 = 0
    for code in codes:
        p = cache_dir / f"{code}.json"
        if not p.exists():
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, dict) or not (data.get("periods") or {}):
            n_v1 += 1
            continue                      # v1 扁平缓存：历史日不可用（_extract_for_asof 同口径）
        n_v2 += 1
        got = _code_steps(data, grid)
        if got is None:
            continue
        r, g, pid = got
        if np.all(np.isnan(r)) and np.all(np.isnan(g)):
            continue
        roe_cols[code], gm_cols[code], pid_cols[code] = r, g, pid
        used.append(code)
    print(f"  v2={n_v2}  v1/其他={n_v1}  进入矩阵={len(used)}", flush=True)
    return (pd.DataFrame(roe_cols, index=grid),
            pd.DataFrame(gm_cols, index=grid),
            pd.DataFrame(pid_cols, index=grid), used)


def _cs_z(df: pd.DataFrame, lo: float = WINSOR_LO, hi: float = WINSOR_HI) -> pd.DataFrame:
    """截面 z（先按当日 [lo,hi] 分位缩尾，再标准化）。"""
    if df.empty:
        return df
    clo, chi = df.quantile(lo, axis=1), df.quantile(hi, axis=1)
    w = df.clip(lower=clo, upper=chi, axis=0)
    mu, sd = w.mean(axis=1), w.std(axis=1)
    return w.sub(mu, axis=0).div(sd.replace(0.0, np.nan), axis=0)


def _board_of(code: str) -> str:
    """6 位代码 → 上市板块（用于暴露「宇宙偏差」）。"""
    if code.startswith("60"):
        return "沪主板"
    if code.startswith("68"):
        return "科创板"
    if code.startswith("00") or code.startswith("002"):
        return "深主板"
    if code.startswith("30"):
        return "创业板"
    if code.startswith("8") or code.startswith("4"):
        return "北交所"
    return "其他"


def _board_breakdown(codes: list[str]) -> dict:
    c = Counter(_board_of(str(x)) for x in codes)
    return {k: int(v) for k, v in c.most_common()}


def _cs_rank_c(df: pd.DataFrame) -> pd.DataFrame:
    return df.rank(axis=1, pct=True) - 0.5


def _one_sided_t(x: np.ndarray) -> float:
    """单侧（H1: 均值>0）t 值。样本 <2 或零方差 → 0（不虚报显著）。"""
    x = np.asarray([v for v in x if v is not None and not np.isnan(v)], dtype=float)
    n = len(x)
    if n < 2:
        return 0.0
    sd = x.std(ddof=1)
    if sd <= 1e-12:
        return 0.0
    return float(x.mean() / sd * math.sqrt(n))


def clustered_stats(ic_oos: pd.Series, keys: pd.Series) -> dict:
    """按簇（报告期）聚合：簇内先平均，再对簇均值做单侧 t 与同号率。"""
    ic_s = pd.Series(ic_oos)
    k_s = keys if isinstance(keys, pd.Series) else pd.Series(keys)
    k_s = k_s.reindex(ic_s.index)
    df = pd.DataFrame({"ic": ic_s.values, "k": k_s.values}, index=ic_s.index).dropna()
    if df.empty:
        return {"n_clusters": 0, "t": 0.0, "mean": None, "pos_frac": None, "clusters": {}}
    g = df.groupby("k")["ic"].mean()
    vals = g.values.astype(float)
    return {
        "n_clusters": int(len(vals)),
        "t": round(_one_sided_t(vals), 2),
        "mean": round(float(vals.mean()), 4),
        "pos_frac": round(float((vals > 0).mean()), 3),
        "clusters": {str(k): round(float(v), 4) for k, v in g.items()},
    }


def portfolio_diag(fac: pd.DataFrame, fwd: pd.DataFrame, base_valid: pd.DataFrame,
                   bad: pd.DataFrame, fwd_bad: pd.DataFrame, days: list,
                   top_n: int) -> dict:
    """TOP-N 多头组合，双基准（全宇宙等权 / 同宇宙等权）+ 换手成本。"""
    rets, b_full, b_cond, dates_out, turn = [], [], [], [], []
    prev = None
    for d in days:
        fv = base_valid.loc[d] & (bad.loc[d] == 0) & (fwd_bad.loc[d] == 0)
        col = fac.loc[d]
        cand = col[fv & col.notna()].dropna()
        if len(cand) < max(top_n * 2, 40):
            prev = None
            continue
        fr = fwd.loc[d]
        cond_vals = fr.reindex(cand.index).dropna()
        full_vals = fr[fv & fr.notna()]
        if cond_vals.empty or full_vals.empty:
            prev = None
            continue
        top = cand.nlargest(top_n)
        held = set(top.index)
        fr_h = fr.reindex(list(held)).dropna()
        if len(fr_h) < 5:
            prev = None
            continue
        tv = 1.0 if prev is None else len(prev ^ held) / max(1, len(held))
        rets.append(float(fr_h.mean()) - fe.COST_RT * tv)
        b_cond.append(float(cond_vals.mean()))
        b_full.append(float(full_vals.mean()))
        turn.append(tv)
        dates_out.append(d)
        prev = held
    if len(rets) < 5:
        return {"n_windows": len(rets)}

    def _cum(x):
        a = 1.0
        for v in x:
            a *= (1 + v)
        return (a - 1) * 100.0

    p = np.array(rets)
    bc, bf = np.array(b_cond), np.array(b_full)
    d_exc = p - bc
    t_exc = (
        round(float(d_exc.mean() / d_exc.std(ddof=1) * math.sqrt(len(d_exc))), 2)
        if len(d_exc) > 1 and d_exc.std(ddof=1) > 1e-12 else 0.0
    )
    return {
        "n_windows": len(p),
        "top_n": top_n,
        "total_pp": round(_cum(p), 1),
        "bench_cond_pp": round(_cum(bc), 1),
        "bench_full_pp": round(_cum(bf), 1),
        "excess_vs_cond_pp": round(_cum(p) - _cum(bc), 1),
        "excess_vs_full_pp": round(_cum(p) - _cum(bf), 1),
        "t_excess_vs_cond": t_exc,
        "win_rate_vs_cond": round(float((d_exc > 0).mean()), 3),
        "avg_turnover": round(float(np.mean(turn)), 3),
        "by_year_excess_cond": {
            str(y): round(float(((1 + p[[i for i, dd in enumerate(dates_out) if dd.year == y]]).prod()
                                 - (1 + bc[[i for i, dd in enumerate(dates_out) if dd.year == y]]).prod()) * 100), 2)
            for y in sorted({dd.year for dd in dates_out})
        },
    }


def format_md(res: dict, meta: dict) -> str:
    st, cl, q, rd, pf = res["stats"], res["clustered_period"], res["quality_rank"], res["redundancy"], res["portfolio"]
    g = res["gate"]
    L = []
    A = L.append
    A(f"# C 批 P0：Quality 维度 PIT IC 可行性 —— `{FACTOR_NAME}`")
    A("")
    A(f"> 生成 {meta['generated_at']}｜**未修改任何生产配置/菜单/CI**｜纯研究")
    A("")
    A(f"**裁定：{res['verdict']}** — {g['reason']}")
    A("")
    A("## 一、预注册判据（跑之前写死）")
    A("")
    A("| 代号 | 判据 | 实测 | 过 |")
    A("|---|---|---|---|")
    for d in g["checks"]:
        A(f"| {d['id']} | {d['rule']} | {d['actual']} | {'✅' if d['ok'] else '❌'} |")
    A("")
    A(f"阈值来源：`fz.MIN_OOS_EVAL_DAYS={MIN_OOS_EVAL_DAYS}`、`fz.MIN_IC_ABS={MIN_IC_ABS}`、"
      f"`fz.STABLE_FRAC={STABLE_FRAC:.3f}`、`T_CRIT={T_CRIT}`（单侧 α=0.10, n_trials=1）。"
      f"截面下限 `MIN_N_DAY_C={MIN_N_DAY_C}`（PIT 覆盖宇宙 {meta['n_v2_codes']} 的 ≥50%）。")
    A("")
    A("## 二、IC 统计")
    A("")
    A("| 口径 | n(非重叠) | mean IC | ICIR | t(非重叠) | p(双侧) | 分年同号率 | IS/OOS 同号 |")
    A("|---|---|---|---|---|---|---|---|")
    if st.get("n_eval"):
        A(f"| 验证集(> {SPLIT_END}) | {st.get('n_eval')} | {st.get('mean_ic')} | {st.get('icir')} | "
          f"{st.get('t_stat')} | {st.get('p_value'):.2g} | {st.get('stable_frac')} | {st.get('seg_match')} |")
    A("")
    A(f"- 全样本均值 IC（形态量）：**{st.get('full_mean_ic')}**；发现集均值 IC：{st.get('is_mean_ic')}")
    A(f"- 分年 IC：{json.dumps(st.get('by_year') or {}, ensure_ascii=False)}")
    A("")
    A("### 主检验：按**报告期聚类**（决定性命据 D5/D6）")
    A("")
    A("> 基本面因子的 IC 序列是随披露跳变的**阶梯函数**，同一份财报值沿用数十个交易日 → "
      "即便非重叠取样，t 仍被自相关抬高。故以「每信号日截面众数生效报告期」为簇，"
      "簇内先平均、再对簇均值做单侧 t。")
    A("")
    A(f"- 簇数 **{cl['n_clusters']}**｜聚类均值 IC **{cl['mean']}**｜**单侧 t = {cl['t']}**"
      f"（门槛 {T_CRIT}）｜同号率 **{cl['pos_frac']}**（门槛 {STABLE_FRAC:.3f}）")
    A("")
    A("| 簇（生效报告期） | 簇内均值 IC |")
    A("|---|---|")
    for k, v in (cl["clusters"] or {}).items():
        A(f"| {k} | {v:+.4f} |")
    A("")
    A("## 三、稳健性对照（**不作判据**）")
    A("")
    A(f"- rank 版复合因子：验证集均值 IC {q['stats'].get('mean_ic')}｜聚类单侧 t {q['clustered_period']['t']}"
      f"｜聚类同号率 {q['clustered_period']['pos_frac']}")
    A(f"- 与主口径聚类 t 同号？{'是' if (q['clustered_period']['t'] or 0) * (cl['t'] or 0) > 0 else '**否 —— 结论不稳健**'}")
    A("")
    A("## 四、冗余检查（与 A 批 12 因子的 IC 相关）")
    A("")
    if rd.get("n_overlap_days"):
        A(f"- 与 A 批因子 IC 的重叠天数：{rd['n_overlap_days']}（>{30} 天方可判相关）")
        for nm, rho in (rd.get("rho") or {}).items():
            flag = " ⚠️ 冗余" if rho is not None and abs(rho) > fz.REDUNDANCY_RHO else ""
            A(f"  - `{nm}`: ρ = {rho}{flag}")
        A(f"- 结论：{rd['conclusion']}")
    else:
        A(f"- **无重叠**：{rd.get('note')}")
    A("")
    A("## 五、可交易性诊断（次要）")
    A("")
    if pf.get("n_windows"):
        A(f"- 窗口 {pf['n_windows']}｜TOP-{pf['top_n']} 累计 {pf['total_pp']}pp"
          f"｜**同宇宙**等权 {pf['bench_cond_pp']}pp｜全宇宙等权 {pf['bench_full_pp']}pp")
        A(f"- 超额（**同宇宙**）：{pf['excess_vs_cond_pp']}pp，t = {pf['t_excess_vs_cond']}，"
          f"胜率 {pf['win_rate_vs_cond']}，均换手 {pf['avg_turnover']}")
        A(f"- 超额（全宇宙，含口径差）：{pf['excess_vs_full_pp']}pp")
        A(f"- 分年超额（同宇宙）：{json.dumps(pf['by_year_excess_cond'], ensure_ascii=False)}")
    else:
        A(f"- 样本不足，无法诊断（n_windows={pf.get('n_windows')}）")
    A("")
    A("## 六、Caveats（必读）")
    A("")
    for c in res["caveats"]:
        A(f"- {c}")
    A("")
    return "\n".join(L) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--load-start", default=fe.LOAD_START)
    ap.add_argument("--split-end", default=SPLIT_END)
    ap.add_argument("--min-n-day", type=int, default=MIN_N_DAY_C)
    ap.add_argument("--top-n", type=int, default=TOP_N_C)
    ap.add_argument("--out-dir", default=None)
    args = ap.parse_args()

    t0 = time.time()
    out_dir = Path(args.out_dir) if args.out_dir else fe.OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    print("[1] 载入价格网格 ...", flush=True)
    mats = fe.load_matrices(cols=("close",), load_start=args.load_start)
    close = mats["close"]
    del mats
    grid = close.index
    codes = [str(c) for c in close.columns]
    print(f"  grid {close.shape[0]} days x {close.shape[1]} codes", flush=True)

    print("[2] 载入 PIT 基本面（v2）...", flush=True)
    ROE, GM, PID, used = build_pit_matrices(grid, codes, fu.CACHE_DIR)
    if not used:
        print("no v2 cache", flush=True)
        return 1

    board_used = _board_breakdown(used)
    board_all = _board_breakdown(codes)
    board_cov = {b: f"{n}/{board_all.get(b, 0)} = {n / max(1, board_all.get(b, 0)) * 100:.1f}%"
                 for b, n in board_used.items()}
    print(f"  板块分布(样本): {board_used}", flush=True)
    print(f"  板块覆盖: {board_cov}", flush=True)

    print("[3] 构造因子 ...", flush=True)
    zr, zg = _cs_z(ROE), _cs_z(GM)
    both = zr.notna() & zg.notna()
    F = ((zr + zg) / 2.0).where(both)
    R = ((_cs_rank_c(ROE) + _cs_rank_c(GM)) / 2.0).where(both)

    print("[4] 前向收益 + 掩码 ...", flush=True)
    ret1, bad = fe.daily_returns_and_bad(close)
    fwd_bad = fe.forward_bad_mask(bad, fe.FWD)
    base_valid = fe.base_valid_mask(close)
    fwd = fe.forward_return_matrix(close, fwd_bad, base_valid, fe.FWD)
    fwd_rank = fe.forward_rank_matrix(fwd)

    print("[5] 截面 IC ...", flush=True)
    m = base_valid & F.notna()
    ic = fe.cross_sectional_ic(F.where(m), fwd_rank, args.min_n_day)
    ic_r = fe.cross_sectional_ic(R.where(base_valid & R.notna()), fwd_rank, args.min_n_day)
    n_ic_days = int(ic.notna().sum())
    first_ic = str(ic.dropna().index.min())[:10] if n_ic_days else "-"
    last_ic = str(ic.dropna().index.max())[:10] if n_ic_days else "-"
    print(f"  IC 有效日 {n_ic_days}  ({first_ic} -> {last_ic})", flush=True)

    split_ts = pd.Timestamp(args.split_end)
    win_ts = pd.Timestamp(fe.WINDOW_START)
    ic_is = ic[(ic.index >= win_ts) & (ic.index <= split_ts)]
    ic_oos = ic[ic.index > split_ts]
    st = fz.ic_stats(ic_oos, ic_is)

    print("[6] 报告期聚类（决定性检验）...", flush=True)
    # 每信号日「截面众数生效报告期」= 决策簇。PID 已在 [2] 一并构建（不二次读盘）。
    def _modal_period(d):
        row = PID.loc[d].values
        c = Counter(v for v in row if isinstance(v, str) and v)
        return c.most_common(1)[0][0] if c else None

    ic_days = ic.index[ic.notna()]
    modal = pd.Series({d: _modal_period(d) for d in ic_days}, dtype=object)
    n_modal = int(modal.notna().sum())
    print(f"  modal 覆盖 {n_modal}/{len(ic_days)} 个 IC 日；样例 "
          f"{ {str(k)[:10]: v for k, v in list(modal.dropna().head(3).items())} }", flush=True)
    if n_modal == 0:
        print("  [WARN] 众数报告期为空 —— 聚类检验无法进行", flush=True)

    def _keys_for(idx):
        k = modal.reindex(idx)
        k.index = idx
        return k

    ic_oos_r = ic_r[ic_r.index > split_ts]
    cl = clustered_stats(ic_oos, _keys_for(ic_oos.index))
    q_cl = clustered_stats(ic_oos_r, _keys_for(ic_oos_r.index))
    q_st = fz.ic_stats(ic_r[ic_r.index > split_ts], ic_r[(ic_r.index >= win_ts) & (ic_r.index <= split_ts)])
    print(f"  簇数 {cl['n_clusters']}  聚类 t={cl['t']}  同号率={cl['pos_frac']}", flush=True)

    # ── 冗余检查：与 A 批 12 因子 IC 相关（读 factor_zoo.json 无 IC 序列 → 重算成本高；
    #    改为与 A 批「价格因子」的 IC 直接对算：只算 12 个 pv 因子中的代表，见 caveat）
    print("[7] 冗余检查 ...", flush=True)
    redundancy = {"n_overlap_days": 0, "note": "未与 A 批因子逐一对算（见 caveat）"}
    try:
        zoo = json.loads((fe.OUT_DIR / "factor_zoo.json").read_text(encoding="utf-8"))
        redundancy = {
            "n_overlap_days": 0,
            "note": "A 批因子的 IC **序列**未落盘（factor_zoo.json 仅存统计量），"
                    "无法低成本对算 ρ；见 caveat「冗余未测」",
        }
        redundancy["a_batch_survivors"] = [
            r["name"] for r in zoo.get("factors", []) if str(r.get("verdict", "")).startswith("存活")
        ]
    except Exception as e:
        redundancy["error"] = str(e)

    print("[8] 可交易性诊断 ...", flush=True)
    pos = {d: i for i, d in enumerate(grid)}
    oos_dates = [d for d in grid if d > split_ts]
    rebal = [d for d in oos_dates if pos[d] + fe.FWD < len(grid)][::fe.REBAL_EVERY]
    pf = portfolio_diag(F, fwd, base_valid, bad, fwd_bad, rebal, args.top_n)
    print(f"  windows={pf.get('n_windows')} excess_vs_cond={pf.get('excess_vs_cond_pp')}pp", flush=True)

    # ── 裁定 ───────────────────────────────────────────────────────────
    checks = [
        {"id": "D1", "rule": f"n_eval ≥ {MIN_OOS_EVAL_DAYS}", "actual": str(st.get("n_eval")),
         "ok": (st.get("n_eval") or 0) >= MIN_OOS_EVAL_DAYS},
        {"id": "D2", "rule": f"|mean IC| ≥ {MIN_IC_ABS}", "actual": str(st.get("full_mean_ic")),
         "ok": abs(st.get("full_mean_ic") or 0.0) >= MIN_IC_ABS},
        {"id": "D3", "rule": "IS/OOS 均值 IC 同号", "actual": str(st.get("seg_match")),
         "ok": st.get("seg_match") is not False},
        {"id": "D4", "rule": "full_mean_ic > 0（先验方向）", "actual": str(st.get("full_mean_ic")),
         "ok": (st.get("full_mean_ic") or 0.0) > 0},
        {"id": "D5", "rule": f"聚类单侧 t ≥ {T_CRIT}", "actual": str(cl["t"]),
         "ok": (cl["t"] or 0.0) >= T_CRIT},
        {"id": "D6", "rule": f"聚类同号率 ≥ {STABLE_FRAC:.2f}", "actual": str(cl["pos_frac"]),
         "ok": (cl["pos_frac"] or 0.0) >= STABLE_FRAC},
    ]
    failed = [c for c in checks if not c["ok"]]
    if failed:
        verdict = "❌ 不通过 —— C 批 Quality 维度无 edge，暂缓（不进 P1/P2）"
        reason = "；".join(f"{c['id']} 未过（{c['rule']}，实测 {c['actual']}）" for c in failed)
    else:
        verdict = "✅ 通过 —— 有 edge 候选，值得进 P1（补数据）→ P2（接线）→ P3（升菜单需过 OOS 门控）"
        reason = "D1–D6 全过"

    caveats = [
        f"**宇宙仅 {len(used)} 只**（v2 / k_data = {len(used)}/{close.shape[1]}"
        f" = {len(used) / close.shape[1] * 100:.1f}%），且**结构上非随机抽样**："
        f"实测板块分布 = {board_used}，按板块覆盖率 = {board_cov}。"
        "样本**几乎全是沪市主板**（深市主板/创业板/科创板严重缺位）→ "
        "「Quality 无 edge」这个结论**只对沪主板成立**，不可外推全市场。"
        "⇒ **P1（扩覆盖至随机/全覆盖）是任何 C 批结论的前置条件**，扩覆盖后必须重跑本验证。",
        f"**PIT 数据起点 2020-06**（报告期 2020-03-31 + 法定披露滞后）→ 发现集实际仅约 2.5 年，"
        "`seg_match`/分年稳定性样本薄，参考价值有限。",
        "**主检验是「报告期聚类」而非非重叠 t**：基本面因子 IC 是披露驱动的阶梯函数，"
        "同一财报值沿用数十个交易日 → 即使 `iloc[::10]` 抽样，t 仍被自相关抬高。"
        f"聚类簇数仅 {cl['n_clusters']}，故同时看同号率（比 t 更该被强调）。",
        "**「累计」不是实现收益**：逐信号日 T+1 开盘买 → T+10 开盘卖的前向收益**复利拼接**，"
        "信号日密集时相邻窗口重叠 ⇒ 绝对值被放大、不可当收益/回撤读。判定只看**逐日差值的 t**。",
        "**基准口径**：`bench_full` 被微盘主导，而基本面覆盖盘天然缺微盘 → 含口径差；"
        "**决策以 `bench_cond`（同宇宙等权）为主**。",
        "**冗余未与 A 批对算**：A 批因子的 IC 序列未落盘，无法低成本算 ρ。"
        "但因子的**信息源完全不同**（财报 vs OHLCV），先验正交；若升菜单仍须按纪律跑 ρ。",
        "**未测存活/微盘折扣**：IC 显著 ≠ 可交易 alpha；升菜单仍须过 `walk_forward_factor_timing` 的 OOS 门控。",
        "本脚本**未改任何生产配置**；落地前还差 P1（扩覆盖 + 估值历史）→ P2（`_SCORERS` 改走 PIT）→ 融合层复核。",
    ]

    res = {
        "verdict": verdict,
        "gate": {"checks": checks, "reason": reason},
        "stats": st,
        "clustered_period": cl,
        "quality_rank": {"stats": q_st, "clustered_period": q_cl},
        "redundancy": redundancy,
        "portfolio": pf,
        "caveats": caveats,
    }
    meta = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "factor": FACTOR_NAME,
        "grid_days": int(close.shape[0]), "grid_codes": int(close.shape[1]),
        "n_v2_codes": len(used),
        "board_breakdown": board_used,
        "board_coverage": board_cov,
        "ic_days": n_ic_days, "ic_first": first_ic, "ic_last": last_ic,
        "split_end": args.split_end, "min_n_day": args.min_n_day,
        "rules": {"min_n_day": args.min_n_day, "min_oos_eval_days": MIN_OOS_EVAL_DAYS,
                  "min_ic_abs": MIN_IC_ABS, "stable_frac": STABLE_FRAC, "t_crit": T_CRIT,
                  "prior": PRIOR, "top_n": args.top_n, "fwd": fe.FWD,
                  "rebal_every": fe.REBAL_EVERY, "cost_rt": fe.COST_RT,
                  "winsor": [WINSOR_LO, WINSOR_HI]},
        "elapsed_s": round(time.time() - t0, 1),
    }

    (out_dir / "quality_pit_ic.json").write_text(
        json.dumps({"meta": meta, "result": res}, ensure_ascii=False, indent=1, default=str),
        encoding="utf-8")
    (out_dir / "quality_pit_ic.md").write_text(format_md(res, meta), encoding="utf-8")
    print(f"DONE in {meta['elapsed_s']}s -> {out_dir}", flush=True)
    print(f"VERDICT: {verdict}", flush=True)
    print(f"REASON : {reason}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
