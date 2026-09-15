#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""持有期敏感性扫描（horizon sweep）。

复用 factor_ic_replay 的矩阵加载与因子定义，仅改变「前向持有交易日 FWD」
与「换仓间隔 REBAL_EVERY」（连续持有：REBAL=FWD）以及成本率 COST_RT，
检验：
  - 更短的持有期（短平快）是否真的带来更高的 NET 风险调整收益；
  - 成本拖累是否吞噬短持有的毛利（短持有 → 换仓更频繁 → 成本翻倍）。
  - 各因子的 horizon 效应是否一致（反转类 vs 流动性/动量类可能相反）。

输出：stock_data/factor_ic_replay/horizon_sweep.md
"""
from __future__ import annotations
import sys, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import numpy as np
import pandas as pd
import factor_ic_replay as fic

HORIZONS = [5, 10, 20]            # 前向持有 + 换仓间隔（连续持有：REBAL=FWD）
COSTS = [0.0, 0.001, 0.003, 0.005]  # 往返成本率敏感性（0=毛利）
TOP_N = fic.TOP_N


def _lookback_bad(bad: pd.DataFrame, w: int) -> pd.DataFrame:
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
    fac_valid = {}
    for name, (_, lb) in fic.FACTORS.items():
        lb_eff = min(lb, 20) if name != "mom60" else 60
        fac_valid[name] = base_valid & fac[name].notna() & (_lookback_bad(bad, lb_eff) == 0)
        fac[name] = fac[name].where(fac_valid[name])

    dates = close.index
    pos = {d: i for i, d in enumerate(dates)}

    # 结果容器
    ic_mean = {}      # (H, factor) -> mean IC
    port = {}         # (H, cost, factor) -> {ann_ex, win, n}

    for H in HORIZONS:
        fwd_bad = bad.copy()
        for k in range(1, H + 1):
            fwd_bad = np.maximum(fwd_bad, bad.shift(-k, fill_value=0.0))
        fwd = (close.shift(-H) / close - 1).where((fwd_bad == 0) & base_valid)
        fwd_rank = fwd.rank(axis=1, pct=True)

        # 每日横截面 Spearman IC
        for name in fic.FACTORS:
            r = fac[name].rank(axis=1, pct=True)
            m = r.notna() & fwd_rank.notna()
            n = m.sum(axis=1)
            a, b = r.where(m), fwd_rank.where(m)
            am, bm = a.mean(axis=1), b.mean(axis=1)
            cov = (a * b).mean(axis=1) - am * bm
            va = (a * a).mean(axis=1) - am * am
            vb = (b * b).mean(axis=1) - bm * bm
            s = (cov / np.sqrt(np.maximum(va * vb, 1e-24)))[n >= fic.MIN_N_DAY]
            s = s[s.index >= pd.Timestamp(fic.WINDOW_START)].dropna()
            ic_mean[(H, name)] = float(s.mean()) if len(s) else float("nan")
        print(f"  H={H} IC done ({time.time()-t0:.0f}s)", flush=True)

        # 组合回测（每 H 日非重叠换仓，持 H 日）
        valid_days = _valid_days(fwd_rank, fac, base_valid, bad, fwd_bad, dates, H)
        rebal = [d for d in valid_days[::H] if pos[d] + H < len(dates)]

        for C in COSTS:
            pbook = {name: {"p": [], "b": [], "d": []} for name in fic.FACTORS}
            for d in rebal:
                fv = base_valid.loc[d] & (bad.loc[d] == 0) & (fwd_bad.loc[d] == 0)
                fr = fwd.loc[d]
                bench = float(fr[fv & fr.notna()].mean()) if (fv & fr.notna()).any() else np.nan
                for name, (sgn, _) in fic.FACTORS.items():
                    col = fac[name].loc[d]
                    cand = col[fv & col.notna()].dropna()
                    if len(cand) < TOP_N * 2 or np.isnan(bench):
                        continue
                    top = cand.nlargest(TOP_N) if sgn > 0 else cand.nsmallest(TOP_N)
                    pr = float(fr.loc[top.index].dropna().mean()) - C
                    pbook[name]["p"].append(pr)
                    pbook[name]["b"].append(bench)
                    pbook[name]["d"].append(d)
            for name in fic.FACTORS:
                pp, bb, dd = pbook[name]["p"], pbook[name]["b"], pbook[name]["d"]
                if len(pp) < 5:
                    port[(H, C, name)] = None
                    continue
                p = pd.Series(pp, index=pd.DatetimeIndex(dd))
                b = pd.Series(bb, index=p.index)
                tot_p = float((1 + p).prod() - 1)
                tot_b = float((1 + b).prod() - 1)
                yrs = len(p) * H / 244.0
                ann_ex = ((1 + tot_p) ** (1 / yrs) - (1 + tot_b) ** (1 / yrs)) * 100 if yrs > 0 else 0.0
                win = float((p - b > 0).mean())
                port[(H, C, name)] = {"ann_ex": ann_ex, "win": win, "n": len(p)}
        print(f"  H={H} portfolios done ({time.time()-t0:.0f}s)", flush=True)

    # ── 输出 ────────────────────────────────────────────────────────
    out_dir = fic.OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    L = []
    L += ["# 持有期敏感性扫描（horizon sweep）", "",
          f"- 生成：{time.strftime('%Y-%m-%d %H:%M:%S')}　窗口：{fic.WINDOW_START} 起　TOP{TOP_N}　成本敏感性 {COSTS}",
          "- 目的：验证「短平快持有收益更大」是否成立——看 **NET（扣成本）年化超额**，不是毛利。",
          "- 成本模型：每 H 日换仓对 TOP50 多头扣 COST_RT（往返近似）；H 减半 → 换仓翻倍 → 成本拖累约翻倍。", ""]

    # 表1：各因子 NET 年化超额（成本=0.3%，即 replay 校准值）
    C0 = 0.003
    L += ["## 一、各因子 NET 年化超额（pp/年，成本=0.3%）", "",
          "| 因子 | H=5 | H=10 | H=20 | 短>长? |",
          "|---|---|---|---|---|"]
    for name in fic.FACTORS:
        def g(H):
            r = port.get((H, C0, name))
            return f"{r['ann_ex']:+.1f}" if r else "n/a"
        h5, h10, h20 = g(5), g(10), g(20)
        better = "✅" if (port.get((5, C0, name)) and port.get((20, C0, name))
                          and port[(5, C0, name)]["ann_ex"] > port[(20, C0, name)]["ann_ex"]) else "—"
        L.append(f"| {name} | {h5} | {h10} | {h20} | {better} |")
    L += ["", "（正=跑赢全宇宙等权基准；越大越好。注意 H=10 是 replay v1 的当前设定。）", ""]

    # 表2：IC（信号强度）随 horizon 变化——短持有是否信号更强？
    L += ["## 二、每日横截面 IC（信号强度）随持有期变化", "",
          "| 因子 | H=5 IC | H=10 IC | H=20 IC |",
          "|---|---|---|---|"]
    for name in fic.FACTORS:
        def ic(H):
            v = ic_mean.get((H, name))
            return f"{v:+.4f}" if v is not None and not (isinstance(v, float) and np.isnan(v)) else "n/a"
        L.append(f"| {name} | {ic(5)} | {ic(10)} | {ic(20)} |")
    L += ["", "（反转类因子 IC 通常随 horizon 变短而增强；流动性/动量类可能相反。）", ""]

    # 表3：成本拖累——H=5 vs H=10 的毛利→净利落差
    L += ["## 三、成本拖累（毛利 C=0 与 C=0.3% 的落差，pp/年）", "",
          "| 因子 | H=5 毛利 | H=5 净利(0.3%) | H=10 毛利 | H=10 净利(0.3%) | H=5 拖累 | H=10 拖累 |",
          "|---|---|---|---|---|---|---|"]
    for name in fic.FACTORS:
        g5 = port.get((5, 0.0, name)); n5 = port.get((5, 0.003, name))
        g10 = port.get((10, 0.0, name)); n10 = port.get((10, 0.003, name))
        def f(r): return f"{r['ann_ex']:+.1f}" if r else "n/a"
        def drag(a, b): return f"{a['ann_ex']-b['ann_ex']:.1f}" if a and b else "n/a"
        L.append(f"| {name} | {f(g5)} | {f(n5)} | {f(g10)} | {f(n10)} | {drag(g5,n5)} | {drag(g10,n10)} |")
    L += ["", "（H=5 拖累 ≈ H=10 的 2 倍——短持有必须把毛利做到 2 倍才能持平。）", ""]

    # 表4：成本敏感性（H=10，看系统对成本多敏感）
    L += ["## 四、成本敏感性（H=10，年化超额 pp）", "",
          "| 因子 | C=0(毛利) | C=0.1% | C=0.3% | C=0.5% |",
          "|---|---|---|---|---|"]
    for name in fic.FACTORS:
        def f(c):
            r = port.get((10, c, name)); return f"{r['ann_ex']:+.1f}" if r else "n/a"
        L.append(f"| {name} | {f(0.0)} | {f(0.001)} | {f(0.003)} | {f(0.005)} |")
    L += ["", "## 五、结论判读（预注册，不扫参选优）", "",
          "- 若「短持有 NET 超额 > 长持有」且覆盖成本拖累 → 可考虑把系统默认持有从 10 日下调；",
          "- 若短持有毛利高但被成本吃光 → **不能**靠缩短持有改善，应改降成本（降换手/降印花税冲击）或维持 10 日；",
          "- 因子层面效应不一致时，缩短持有对「反转类」有利、对「流动性/动量类」可能不利 → 需分因子处理，而非全局改一个旋钮。", ""]

    (out_dir / "horizon_sweep.md").write_text("\n".join(L), encoding="utf-8")
    print(f"DONE in {time.time()-t0:.0f}s -> {out_dir/'horizon_sweep.md'}", flush=True)
    return 0


def _valid_days(fwd_rank, fac, base_valid, bad, fwd_bad, dates, H):
    """复刻 replay 的 valid_days：基于 FIRST_FACTOR 当日 IC 是否有效的代理。
    这里直接用 fwd 在当日是否有足够有效样本近似。"""
    name = fic.FIRST_FACTOR
    fv = base_valid & fac[name].notna()
    # 当日有效横截面数 >= MIN_N_DAY 视为 IC 有效日
    daily_n = fv.sum(axis=1)
    return [d for d in dates if daily_n.get(d, 0) >= fic.MIN_N_DAY]


if __name__ == "__main__":
    raise SystemExit(main())
