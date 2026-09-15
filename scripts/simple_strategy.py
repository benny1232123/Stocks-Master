#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""简洁策略回测：一个因子 + 已验证负尾剔除层，迭代到净收益达标。

用法：
  python scripts/simple_strategy.py --factor illiq20 --topn 50 --hold 20 --cost 0.003 --exclude
  python scripts/simple_strategy.py --factor liq20   --topn 80 --hold 20 --cost 0.003 --exclude

信号：单因子排名 → 多头 TOP_N（illiq20 做多高非流动性 / liq20 做多低流动性）。
过滤：负尾剔除层（剔除 mom20/mom60/vol20/amp20/dist20 最高十分位，已验证）。
组合：等权多头，每 hold 日换仓、持 hold 日，成本 = cost × 单边换手率。
输出：净年化 / 胜率 / 分年，及是否达 --target 目标。
"""
from __future__ import annotations
import sys, time, argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import numpy as np
import pandas as pd
import factor_ic_replay as fic

TAIL_FACTORS = ["mom20", "mom60", "vol20", "amp20", "dist20"]


def _lookback_bad(bad, w):
    acc = bad.copy()
    for k in range(1, w):
        acc = np.maximum(acc, bad.shift(k, fill_value=0.0))
    return acc


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--factor", default="illiq20", choices=["illiq20", "liq20", "blend"])
    ap.add_argument("--topn", type=int, default=50)
    ap.add_argument("--hold", type=int, default=20)
    ap.add_argument("--cost", type=float, default=0.003)
    ap.add_argument("--exclude", action="store_true", help="启用负尾剔除层")
    ap.add_argument("--target", type=float, default=5.0, help="净年化目标(%)")
    args = ap.parse_args()

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

    ma20 = close.rolling(20, min_periods=fic.ROLL_MIN).mean()
    amt20 = amount.rolling(20, min_periods=fic.ROLL_MIN).mean().where(lambda x: x > 0)
    amt_pos = amount.where(amount > 0)
    fac = {
        "mom20":  close / close.shift(20) - 1,
        "mom60":  close / close.shift(60) - 1,
        "vol20":  ret1.rolling(20, min_periods=fic.ROLL_MIN).std(),
        "amp20":  ((high - low) / close).rolling(20, min_periods=fic.ROLL_MIN).mean(),
        "liq20":  np.log(amt20),
        "illiq20": (ret1.abs() / amt_pos).rolling(20, min_periods=fic.ROLL_MIN).mean(),
        "dist20": close / ma20 - 1,
    }
    lb_map = {"mom20": 20, "mom60": 60, "vol20": 20, "amp20": 20,
              "liq20": 20, "illiq20": 20, "dist20": 20}
    for n in fac:
        fac[n] = fac[n].where(base_valid & fac[n].notna() & (_lookback_bad(bad, lb_map[n]) == 0))

    H = args.hold
    fwd_bad = bad.copy()
    for k in range(1, H + 1):
        fwd_bad = np.maximum(fwd_bad, bad.shift(-k, fill_value=0.0))
    fwd = (close.shift(-H) / close - 1).where((fwd_bad == 0) & base_valid)

    # 信号方向：illiq20 高=好，liq20 低=好，blend=两者合成
    dates = close.index
    pos = {d: i for i, d in enumerate(dates)}
    fv0 = base_valid & fac[fic.FIRST_FACTOR].notna()
    valid_days = [d for d in dates if int(fv0.loc[d].sum()) >= fic.MIN_N_DAY]
    rebal = [d for d in valid_days[::H] if pos[d] + H < len(dates)]

    rets, prev = [], None
    by_year = {}
    for d in rebal:
        fv = base_valid.loc[d] & (bad.loc[d] == 0) & (fwd_bad.loc[d] == 0)
        fr = fwd.loc[d]
        cand_idx = fv & fr.notna()
        if int(cand_idx.sum()) < args.topn * 2:
            prev = None
            continue
        # 评分
        if args.factor == "illiq20":
            score = fac["illiq20"].loc[d]
            top = score[cand_idx].nlargest(args.topn)
        elif args.factor == "liq20":
            score = fac["liq20"].loc[d]
            top = score[cand_idx].nsmallest(args.topn)
        else:  # blend
            s1 = fac["liq20"].loc[d].rank(pct=True)
            s2 = fac["illiq20"].loc[d].rank(pct=True)
            score = (1 - s1) + s2  # 低流动性 + 高非流动性
            top = score[cand_idx].nlargest(args.topn)
        held = set(top.index)
        if args.exclude:
            toxic = pd.Series(False, index=close.columns)
            for tf in TAIL_FACTORS:
                cc = fac[tf].loc[d]
                c2 = cc[cand_idx & cc.notna()].dropna()
                if len(c2) < 50:
                    continue
                toxic = toxic | (cc >= c2.quantile(0.9))
            held = held - set(toxic[cand_idx].index)
        if len(held) < 10:
            prev = None
            continue
        fr_h = fr.loc[list(held)].dropna()
        if len(fr_h) < 10:
            prev = None
            continue
        if prev is None:
            cost = args.cost
        else:
            turn = len(prev ^ held) / max(1, len(held))
            cost = args.cost * turn
        r = float(fr_h.mean()) - cost
        rets.append(r)
        y = d.year
        by_year.setdefault(y, []).append(r)
        prev = held

    s = pd.Series(rets)
    n = len(s)
    if n < 5:
        print(f"too few windows ({n})"); return 1
    yrs = n * H / 244.0
    tot = float((1 + s).prod() - 1)
    ann = ((1 + tot) ** (1 / yrs) - 1) * 100
    win = float((s > 0).mean()) * 100
    ok = ann >= args.target
    print(f"[{args.factor} topn={args.topn} hold={H} exclude={args.exclude}] "
          f"windows={n} net_ann={ann:+.2f}% win={win:.0f}% target={args.target}% -> "
          f"{'PASS' if ok else 'LOW'}", flush=True)
    print(f"  by_year: " + " ".join(f"{y}:{((1+pd.Series(v)).prod()-1)*100:+.0f}%"
          for y, v in sorted(by_year.items())), flush=True)

    # 落盘（cost 入文件名，避免不同成本率互相覆盖）
    ctag = f"_c{args.cost:.3f}".rstrip("0").rstrip(".") if args.cost else "_c0"
    out = fic.OUT_DIR / f"simple_{args.factor}_n{args.topn}_h{H}{ctag}{'_ex' if args.exclude else ''}.md"
    L = [f"# 简洁策略回测：{args.factor} (TOP{args.topn}, 持有{H}日, 剔除={args.exclude})", "",
         f"- 净年化：**{ann:+.2f}%**　胜率：{win:.0f}%　窗口数：{n}　目标：{args.target}% → **{'✅达标' if ok else '❌未达标'}**",
         f"- 成本模型：cost={args.cost} × 单边换手率", "",
         "## 分年净收益", ""]
    for y, v in sorted(by_year.items()):
        L.append(f"- {y}: {((1+pd.Series(v)).prod()-1)*100:+.1f}%")
    L += ["", "## Caveats", "- 幸存者偏差：universe=存活票，绝对收益偏乐观；",
          "- 非流动性/微盘溢价三重折扣（幸存者/不可交易流动性/2024-01 踩踏），实盘需流动性感知仓位。"]
    out.write_text("\n".join(L), encoding="utf-8")
    print(f"DONE {time.time()-t0:.0f}s -> {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
