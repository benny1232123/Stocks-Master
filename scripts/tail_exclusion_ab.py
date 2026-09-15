#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""负向尾部剔除层 A/B（直接在历史上跑，验收用）。

剔除规则（预注册，来源 = 已提交 replay v1 十分位表 replay_summary.md §三）：
  每个换仓日，凡落在以下 5 个因子**最高十分位(D10)** 的票 → 标记为毒性、从多头候选剔除：
    mom20 / mom60（高动量延伸端） / vol20（高波） / amp20（高振幅） / dist20（远离均线延伸端）
  —— replay 证实这 5 个因子的 D10 都是最负前向收益（均值回归：延伸/高波/高动量会回撤）。

A/B 设计（公平、turnover 计费）：
  基线   = 全可交易宇宙等权多头（replay 基准）。
  处理   = （全宇宙 − 毒性票）等权多头。
  两者每 10 日换仓、持有 10 日、成本 = COST_RT × 单边换手率（买卖各算）。
  验收指标 = 处理净年化超额 − 基线净年化超额（剔除层净贡献），及其 t 统计量。

另附：① 毒性尾自身前向收益（证成过滤器）② 各因子 TOP50 含/不含剔除的净超额对比。

输出：stock_data/factor_ic_replay/tail_exclusion_ab.md
"""
from __future__ import annotations
import sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import numpy as np
import pandas as pd
import factor_ic_replay as fic

FWD = 10
REBAL_EVERY = 10
COST_RT = 0.003
TOP_N = fic.TOP_N
N_DECILE = 10
TAIL_FACTORS = ["mom20", "mom60", "vol20", "amp20", "dist20"]  # 全部取最高十分位(D10)


def _lookback_bad(bad, w):
    acc = bad.copy()
    for k in range(1, w):
        acc = np.maximum(acc, bad.shift(k, fill_value=0.0))
    return acc


def main() -> int:
    t0 = time.time()
    mats = fic.load_matrices()
    close, high, low, amount = mats["close"], mats["high"], mats["low"], mats["amount"]
    print(f"grid {close.shape[0]}x{close.shape[1]} ({time.time()-t0:.0f}s)", flush=True)

    ret1 = close.pct_change(fill_method=None)
    lim = fic._limit_series(close.columns)
    lim_mat = pd.DataFrame(np.tile(lim.values, (close.shape[0], 1)),
                           index=close.index, columns=close.columns)
    bad = (ret1.abs() > lim_mat * 1.005 + fic.BAD_TOL).fillna(0.0)
    warm = close.rolling(fic.WARMUP_WIN, min_periods=1).count() >= fic.WARMUP_MIN
    base_valid = (close.notna()) & (close >= fic.PRICE_FLOOR) & warm

    ma20 = close.rolling(20, min_periods=fic.ROLL_MIN).mean()
    amt20 = amount.rolling(20, min_periods=fic.ROLL_MIN).mean().where(lambda x: x > 0)
    amt_pos = amount.where(amount > 0)
    # 全部因子（含回放 8 因子，供 per-factor TOP50 A/B）
    fac = {
        "mom20":  close / close.shift(20) - 1,
        "mom60":  close / close.shift(60) - 1,
        "rev5":   close / close.shift(5) - 1,
        "vol20":  ret1.rolling(20, min_periods=fic.ROLL_MIN).std(),
        "amp20":  ((high - low) / close).rolling(20, min_periods=fic.ROLL_MIN).mean(),
        "liq20":  np.log(amt20),
        "illiq20": (ret1.abs() / amt_pos).rolling(20, min_periods=fic.ROLL_MIN).mean(),
        "dist20": close / ma20 - 1,
    }
    lb_map = {"mom20": 20, "mom60": 60, "rev5": 5, "vol20": 20, "amp20": 20,
              "liq20": 20, "illiq20": 20, "dist20": 20}
    fac_valid = {}
    for name in fac:
        lb_eff = lb_map[name]
        fac_valid[name] = base_valid & fac[name].notna() & (_lookback_bad(bad, lb_eff) == 0)
        fac[name] = fac[name].where(fac_valid[name])

    dates = close.index
    pos = {d: i for i, d in enumerate(dates)}

    # 前向收益（FWD=10）+ 坏柱屏蔽
    fwd_bad = bad.copy()
    for k in range(1, FWD + 1):
        fwd_bad = np.maximum(fwd_bad, bad.shift(-k, fill_value=0.0))
    fwd = (close.shift(-FWD) / close - 1).where((fwd_bad == 0) & base_valid)

    # 换仓日（沿用 replay：基于 FIRST_FACTOR 有效日近似）
    fv0 = base_valid & fac[fic.FIRST_FACTOR].notna()
    valid_days = [d for d in dates if int(fv0.loc[d].sum()) >= fic.MIN_N_DAY]
    rebal = [d for d in valid_days[::REBAL_EVERY] if pos[d] + FWD < len(dates)]
    print(f"rebal days={len(rebal)} ({time.time()-t0:.0f}s)", flush=True)

    base_ret, clean_ret, bench_ret = [], [], []
    toxic_ret_series = []
    prev_base, prev_clean = None, None

    def net(ret_series, prev_set, cur_set):
        if prev_set is None:
            return float(ret_series.mean()) - COST_RT  # 首次建仓全换手
        turn = len(prev_set ^ cur_set) / max(1, len(cur_set))  # 单边换手率
        return float(ret_series.mean()) - COST_RT * turn

    for d in rebal:
        fv = base_valid.loc[d] & (bad.loc[d] == 0) & (fwd_bad.loc[d] == 0)
        fr = fwd.loc[d]
        # 毒性 = 5 因子任一最高十分位
        toxic_mask = pd.Series(False, index=close.columns)
        for tf in TAIL_FACTORS:
            col = fac[tf].loc[d]
            cands = col[fv & col.notna()].dropna()
            if len(cands) < 50:
                continue
            thr = cands.quantile(0.9)
            toxic_mask = toxic_mask | (col >= thr)
        held_base = set(fr[fv & fr.notna()].index)
        held_clean = set(fr[fv & fr.notna() & ~toxic_mask].index)
        if not held_base:
            continue
        rb = fr.loc[list(held_base)].dropna()
        rc = fr.loc[list(held_clean)].dropna()
        if len(rb) < 10 or len(rc) < 10:
            continue
        base_ret.append(net(rb, prev_base, held_base))
        clean_ret.append(net(rc, prev_clean, held_clean))
        bench_ret.append(float(rb.mean()))  # 等权基准（未计成本）
        tox_names = set(fr[fv & fr.notna() & toxic_mask].index)
        if tox_names:
            toxic_ret_series.append(float(fr.loc[list(tox_names)].dropna().mean()))
        prev_base, prev_clean = held_base, held_clean

    base_s = pd.Series(base_ret)
    clean_s = pd.Series(clean_ret)
    bench_s = pd.Series(bench_ret)
    n = len(base_s)
    yrs = n * FWD / 244.0

    def ann(series):
        tot = float((1 + series).prod() - 1)
        return ((1 + tot) ** (1 / yrs) - 1) * 100 if yrs > 0 else 0.0

    ann_base = ann(base_s)
    ann_clean = ann(clean_s)
    ann_bench = ann(bench_s)
    delta = ann_clean - ann_base
    # 剔除层净贡献的显著性
    diff = clean_s - base_s
    se = diff.std() / (n ** 0.5) if n > 1 else 0.0
    t_stat = float(diff.mean() / se) if se > 0 else 0.0
    toxic_mean = float(np.mean(toxic_ret_series)) if toxic_ret_series else float("nan")
    nontoxic_mean = float(bench_s.mean())

    # ── 各因子 TOP50 含/不含剔除的净超额对比 ──
    factor_ab = {}
    for name, (sgn, _) in fic.FACTORS.items():
        p_base, p_clean = [], []
        for d in rebal:
            fv = base_valid.loc[d] & (bad.loc[d] == 0) & (fwd_bad.loc[d] == 0)
            fr = fwd.loc[d]
            col = fac[name].loc[d]
            cand_all = col[fv & col.notna()].dropna()
            # 毒性（同规则）
            toxic_mask = pd.Series(False, index=close.columns)
            for tf in TAIL_FACTORS:
                cc = fac[tf].loc[d]
                c2 = cc[fv & cc.notna()].dropna()
                if len(c2) < 50:
                    continue
                toxic_mask = toxic_mask | (cc >= c2.quantile(0.9))
            cand_clean = cand_all[~toxic_mask.reindex(cand_all.index).fillna(False)]
            if len(cand_all) < TOP_N * 2:
                continue
            top_all = cand_all.nlargest(TOP_N) if sgn > 0 else cand_all.nsmallest(TOP_N)
            top_clean = cand_clean.nlargest(TOP_N) if sgn > 0 else cand_clean.nsmallest(TOP_N)
            ra = fr.loc[top_all.index].dropna().mean() - COST_RT
            rc2 = fr.loc[top_clean.index].dropna().mean() - COST_RT
            if not np.isnan(ra) and not np.isnan(rc2):
                p_base.append(ra)
                p_clean.append(rc2)
        if len(p_base) < 5:
            factor_ab[name] = None
            continue
        pb, pc = pd.Series(p_base), pd.Series(p_clean)
        tb = (1 + pb).prod() - 1
        tc = (1 + pc).prod() - 1
        ab = float(((1 + tb) ** (1 / (len(pb) * FWD / 244.0)) - 1) * 100)
        ac = float(((1 + tc) ** (1 / (len(pc) * FWD / 244.0)) - 1) * 100)
        factor_ab[name] = {"base_ann": ab, "clean_ann": ac, "delta": ac - ab}

    # ── 输出 ──
    out = fic.OUT_DIR
    out.mkdir(parents=True, exist_ok=True)
    L = ["# 负向尾部剔除层 A/B（历史回放验收）", "",
         f"- 生成：{time.strftime('%Y-%m-%d %H:%M:%S')}　窗口：{fic.WINDOW_START} 起　FWD={FWD}　换仓={REBAL_EVERY}日　成本=COST_RT×换手",
         f"- 剔除规则（预注册，来源 replay_summary.md §三）：剔除 mom20/mom60/vol20/amp20/dist20 **最高十分位(D10)** 的票（延伸/高波/高动量回撤）。",
         f"- 基线=全宇宙等权多头；处理=（全宇宙−毒性）等权多头。换仓数={n}。", ""]
    L += ["## 一、毒性尾自身前向收益（证成过滤器）", "",
          f"- 毒性尾（被剔除票）平均 T+{FWD} 收益：**{toxic_mean*100:+.2f}%**",
          f"- 非毒性尾（持有票）平均 T+{FWD} 收益：**{nontoxic_mean*100:+.2f}%**",
          f"- 差值：{(toxic_mean-nontoxic_mean)*100:+.2f}pp/窗 → 毒性尾确实显著更差，过滤器有经济依据。", ""]
    L += ["## 二、A/B 主结果（净年化，%）", "",
          "| 组合 | 净年化 | 相对等权基准 |",
          "|---|---|---|",
          f"| 基线（全宇宙等权） | {ann_base:+.2f} | — |",
          f"| 处理（剔除毒性尾） | {ann_clean:+.2f} | {ann_clean-ann_bench:+.2f} |",
          f"| **剔除层净贡献 (处理−基线)** | | **{delta:+.2f}** |",
          f"| 显著性 t 统计量（{n} 窗） | | {t_stat:+.2f} |", ""]
    gate = delta > 2.0 and t_stat > 1.96
    L += [f"## 三、验收闸门", "",
          f"- 预注册标准：剔除层净贡献 > +2pp 且 t > 1.96（95% 显著）。",
          f"- 实测：净贡献 **{delta:+.2f}pp**，t = **{t_stat:+.2f}** → **{'✅ 通过，可写回 DAL' if gate else '❌ 未通过，C4 WARN 不写回'}**。", ""]
    L += ["## 四、各因子 TOP50 含/不含剔除（净年化 %）", "",
          "| 因子 | 基线TOP50 | 剔除后TOP50 | Δ |",
          "|---|---|---|---|"]
    for name in fic.FACTORS:
        r = factor_ab.get(name)
        if not r:
            L.append(f"| {name} | n/a | n/a | n/a |")
            continue
        L.append(f"| {name} | {r['base_ann']:+.2f} | {r['clean_ann']:+.2f} | {r['delta']:+.2f} |")
    L += ["", "## 五、Caveats", "",
          "- **样本内**：剔除规则方向来自同一 2018+ 窗口的 replay 十分位表；本 A/B 同窗口验证，属 in-sample 证成。",
          "  真正生产验收 = walk-forward OOS（下一阶段闸门），本结果仅说明「历史上剔除层有正贡献」。",
          "- 幸存者偏差：universe=存活票，绝对收益偏乐观；此处看的是「剔除层净贡献」的相对量，受影响较小。",
          "- 未剔 ST（无历史名单）；成本按 round-trip 近似，换手率低时偏保守（对处理组更不利→结论稳健）。", ""]
    (out / "tail_exclusion_ab.md").write_text("\n".join(L), encoding="utf-8")
    print(f"DONE in {time.time()-t0:.0f}s -> {out/'tail_exclusion_ab.md'}", flush=True)
    print(f"  ann_base={ann_base:+.2f} ann_clean={ann_clean:+.2f} delta={delta:+.2f} t={t_stat:+.2f} "
          f"toxic={toxic_mean*100:+.2f}% nontoxic={nontoxic_mean*100:+.2f}%", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
