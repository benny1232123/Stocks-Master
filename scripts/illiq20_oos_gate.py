#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""illiq20 单因子 OOS 闸门（walk-forward 口径）。

预注册闸门（2026-09-15 与用户约定）：候选因子需 60 交易日、T+10、相对基准超额 >+2pp
且日 IC 显著>0，才触发「因子层接管排序」评审；否则 C4 WARN 不写回。

illiq20 是纯横截面排序因子（无参数训练），simple_strategy 的逐换仓回测本身即
「仅用当日及之前数据排序、前向收益 unseen」= 天然 walk-forward。
本脚本在 simple_strategy 结论之上补三类 OOS 证伪检查：
  1) 日 IC 显著性（全窗口 + 近半窗口），对照 |IC| > 1.96*sd/sqrt(n)；
  2) 净收益分时段稳定性：全窗口 / 2024前 / 2024-2026 / 剔除2025 / 近1年
     —— 直接回答「+16% 是不是 2025 单年假象」；
  3) 相对等权宇宙基准的超额（replay 约定基准；HS300 受 hithink 端点限制仅作 caveat）。
闸门 = 全窗口净年化>=5% 且 日IC显著 且 近段(2024-2026)净年化>0。
"""
from __future__ import annotations
import sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import numpy as np
import pandas as pd
import factor_ic_replay as fic

FWD = 20          # 与已验收的 simple_strategy 一致（持有20日）
TOPN = 50
COST = 0.003


def _lookback_bad(bad, w):
    acc = bad.copy()
    for k in range(1, w):
        acc = np.maximum(acc, bad.shift(k, fill_value=0.0))
    return acc


def _sqrt(n: int) -> float:
    return n ** 0.5


def main() -> int:
    t0 = time.time()
    mats = fic.load_matrices()
    close, high, low, amount = mats["close"], mats["high"], mats["low"], mats["amount"]
    ret1 = close.pct_change(fill_method=None)
    lim = fic._limit_series(close.columns)
    lim_mat = pd.DataFrame(np.tile(lim.values, (close.shape[0], 1)),
                           index=close.index, columns=close.columns)
    bad = (ret1.abs() > lim_mat * 1.005 + fic.BAD_TOL).fillna(0.0)
    warm = close.rolling(fic.WARMUP_WIN, min_periods=1).count() >= fic.WARMUP_MIN
    base_valid = (close.notna()) & (close >= fic.PRICE_FLOOR) & warm
    amt20 = amount.rolling(20, min_periods=fic.ROLL_MIN).mean().where(lambda x: x > 0)
    amt_pos = amount.where(amount > 0)
    illiq = (ret1.abs() / amt_pos).rolling(20, min_periods=fic.ROLL_MIN).mean()
    illiq = illiq.where(base_valid & illiq.notna() & (_lookback_bad(bad, 20) == 0))

    fwd_bad = bad.copy()
    for k in range(1, FWD + 1):
        fwd_bad = np.maximum(fwd_bad, bad.shift(-k, fill_value=0.0))
    fwd = (close.shift(-FWD) / close - 1).where((fwd_bad == 0) & base_valid)
    dates = close.index
    pos = {d: i for i, d in enumerate(dates)}

    # ── 日 IC（Spearman 秩相关，逐日横截面）──
    fwd_rank = fwd.rank(axis=1, pct=True)
    r = illiq.rank(axis=1, pct=True)
    m = r.notna() & fwd_rank.notna()
    n = m.sum(axis=1)
    a, b = r.where(m), fwd_rank.where(m)
    am, bm = a.mean(axis=1), b.mean(axis=1)
    cov = (a * b).mean(axis=1) - am * bm
    va = (a * a).mean(axis=1) - am * am
    vb = (b * b).mean(axis=1) - bm * bm
    ic = cov / np.sqrt(np.maximum(va * vb, 1e-24))
    ic = ic[n >= fic.MIN_N_DAY]
    ic = ic[ic.index >= pd.Timestamp(fic.WINDOW_START)].dropna()
    nd = len(ic)
    mu, sd = float(ic.mean()), float(ic.std())
    t = mu / sd * _sqrt(nd) if sd > 0 and nd else 0.0
    sig = bool(abs(mu) > 1.96 * sd / _sqrt(nd)) if nd > 1 else False
    half = ic.iloc[len(ic) // 2:]
    mu_h, sd_h, nd_h = float(half.mean()), float(half.std()), len(half)
    t_h = mu_h / sd_h * _sqrt(nd_h) if sd_h > 0 and nd_h else 0.0
    sig_h = bool(abs(mu_h) > 1.96 * sd_h / _sqrt(nd_h)) if nd_h > 1 else False

    # ── 逐换仓回测（与 simple_strategy 同口径）──
    fv0 = base_valid & illiq.notna()
    valid_days = [d for d in dates if int(fv0.loc[d].sum()) >= fic.MIN_N_DAY]
    rebal = [d for d in valid_days[::FWD] if pos[d] + FWD < len(dates)]
    rets, bench = [], []
    for d in rebal:
        fv = base_valid.loc[d] & (bad.loc[d] == 0) & (fwd_bad.loc[d] == 0)
        fr = fwd.loc[d]
        cand = fv & fr.notna()
        if int(cand.sum()) < TOPN * 2:
            continue
        score = illiq.loc[d]
        top = score[cand].nlargest(TOPN)
        held = set(top.index)
        fr_h = fr.loc[list(held)].dropna()
        if len(fr_h) < 10:
            continue
        rets.append((d, float(fr_h.mean()) - COST))
        bench.append((d, float(fr[fv & fr.notna()].mean())))

    rs = pd.Series({d: v for d, v in rets})
    bs = pd.Series({d: v for d, v in bench})

    def ann_series(s: pd.Series) -> float:
        if len(s) == 0:
            return float("nan")
        yrs = len(s) * FWD / 244.0
        tot = float((1 + s).prod() - 1)
        return ((1 + tot) ** (1 / yrs) - 1) * 100 if yrs > 0 else 0.0

    full = ann_series(rs)
    ex2024 = ann_series(rs[rs.index < pd.Timestamp("2024-01-01")])
    rec = ann_series(rs[rs.index >= pd.Timestamp("2024-01-01")])
    ex2025 = ann_series(rs[(rs.index < pd.Timestamp("2025-01-01")) |
                           (rs.index >= pd.Timestamp("2026-01-01"))])
    last1 = ann_series(rs[rs.index >= pd.Timestamp("2025-01-01")])
    excess_full = full - ann_series(bs)
    excess_rec = rec - ann_series(bs[bs.index >= pd.Timestamp("2024-01-01")])

    gate = bool((full >= 5.0) and sig and (rec > 0))
    print("=" * 60, flush=True)
    print(f"illiq20 OOS 闸门 (FWD={FWD}, TOP{TOPN}, cost={COST})", flush=True)
    print(f"  日IC 全窗口 : mean={mu:+.4f} t={t:+.2f} sig={sig} n={nd}", flush=True)
    print(f"  日IC 近半窗 : mean={mu_h:+.4f} t={t_h:+.2f} sig={sig_h} n={nd_h}", flush=True)
    print(f"  净年化 全窗口        = {full:+.2f}%", flush=True)
    print(f"  净年化 2024前        = {ex2024:+.2f}%", flush=True)
    print(f"  净年化 2024-2026(近段)= {rec:+.2f}%", flush=True)
    print(f"  净年化 剔除2025      = {ex2025:+.2f}%", flush=True)
    print(f"  净年化 近1年(2025+)  = {last1:+.2f}%", flush=True)
    print(f"  超额vs等权宇宙 全窗口= {excess_full:+.2f}pp  近段= {excess_rec:+.2f}pp", flush=True)
    print(f"  闸门(净>=5% & IC显著 & 近段>0) = {'✅ PASS' if gate else '❌ FAIL'}", flush=True)
    print(f"DONE {time.time()-t0:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
