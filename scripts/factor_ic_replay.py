#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""历史因子 IC 回放（纯价格因子 · 预注册版 v1）。

目的：用全宇宙（k_data 全部个股）历史横截面，检验一组**预注册**的纯价格因子
是否真的有横截面预测力——回答「要不要转因子主导」的第一步（历史回放先行）。

预注册纪律（2026-09-15 与用户约定，改动任何一项即视为新一轮、须注明）：
1. 回放窗口 2018-01-01 起（深历史 ≤2017 存在 hithink 加法式失真未修，预注册避开）。
2. 因子清单 8 个、全部纯价格可算（零 PIT 风险），方向按经济先验写死：
     mom20/mom60（+，动量）、rev5（−，反转做多跌得多的）、vol20（−，低波）、
     amp20（−，低振幅）、liq20=ln(20日均成交额)（−，低流动性溢价）、
     illiq20=Amihud（+，非流动性溢价）、dist20=close/ma20-1（−，均线均值回归）。
   ⚠️ 换手率不可用（无历史流通股本）→ liq20 用成交额代理，已在 caveats 声明。
3. 不扫参、不选优；EWM 半衰期等配权参数本脚本不产出（长历史只做淘汰与压力测试）。
4. 可交易性：每 10 个交易日非重叠换仓、持有 10 日（T+10）、往返成本 0.3%；
   基准=全宇宙等权（不计成本，使超额偏保守）。
5. 污染剔除：单日收益超板内涨跌停限制×1.005+0.002 视为坏柱（hithink 深历史跳变）；
   因子回看窗 / 前向收益窗内含坏柱 → 该样本作废（NaN）。
6. 输出分年度 / 分半年衰减表——**不出单一汇总数下结论**。

已知局限（caveats，报告须带上）：
- 幸存者偏差：k_data 是「今天还活着」的票，绝对收益偏乐观，**结论只用于因子间相对比较**。
- 未剔 ST（无历史 ST 名单）、未剔新股上市首期（用 60 日预热近似）。
- T+10 前向收益对停牌股缺失 → 从组合均值中剔除（小幅乐观）。

用法：python scripts/factor_ic_replay.py
输出：stock_data/factor_ic_replay/replay_summary.md + replay_detail.json
"""
from __future__ import annotations

import glob
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
KDATA_DIR = ROOT / "stock_data" / "k_data"
OUT_DIR = ROOT / "stock_data" / "factor_ic_replay"

# ── 预注册参数（禁 magic number 之外的随意改动；改 = 新一轮并注明）────────
LOAD_START = "2017-06-01"   # 预热缓冲
WINDOW_START = "2018-01-01" # 结论窗口
WARMUP_WIN = 120            # 预热观察窗
WARMUP_MIN = 60             # 窗内最少有效收盘数
PRICE_FLOOR = 2.0           # 剔低价股
FWD = 10                    # 前向持有交易日
TOP_N = 50                  # 单因子组合持仓数
REBAL_EVERY = 10            # 非重叠换仓间隔（交易日）
COST_RT = 0.003             # 往返成本 0.3%（佣金+印花税+滑点，预注册）
MIN_N_DAY = 300             # 当日有效横截面下限
ROLL_MIN = 15               # 滚动统计 min_periods（容忍短停牌）
BAD_TOL = 0.002             # 坏柱判定容差

# 因子 -> (prior 方向 +1=做多高值 / -1=做多低值, 回看窗)
FACTORS = {
    "mom20":  (+1, 20),
    "mom60":  (+1, 60),
    "rev5":   (-1, 5),
    "vol20":  (-1, 20),
    "amp20":  (-1, 20),
    "liq20":  (-1, 20),
    "illiq20": (+1, 20),
    "dist20": (-1, 20),
}


FIRST_FACTOR = next(iter(FACTORS))  # dict 保序，取第一个作为「有效日」的代表


def _limit_series(codes: pd.Index) -> pd.Series:
    """板内涨跌停幅度（近似）：创业板/科创板 20%，北交所 30%，其余 10%。"""
    def lim(c: str) -> float:
        c = str(c)
        if c.startswith(("300", "301", "688")):
            return 0.20
        if c.startswith(("43", "83", "87", "92")):
            return 0.30
        return 0.10
    return pd.Series([lim(c) for c in codes], index=codes)


def load_matrices() -> dict[str, pd.DataFrame]:
    """批量直读 parquet 桶 → 宽表矩阵。

    ⚠️ 刻意不走 smcore/data/kline.py：其读路径会触发 write_kline_cache
    （kline.py:780-781 检测断层时 force_refresh 全历史重拉）。本脚本只读不写。
    """
    files = sorted(glob.glob(str(KDATA_DIR / "qfq_*.parquet")))
    if not files:
        raise SystemExit(f"no parquet under {KDATA_DIR}")
    frames = []
    for f in files:
        d = pd.read_parquet(f)
        d["date"] = pd.to_datetime(d["date"])
        d["code"] = d["code"].astype("category")
        frames.append(d[d["date"] >= pd.Timestamp(LOAD_START)])
        del d
    df = pd.concat(frames, ignore_index=True).drop_duplicates(["code", "date"])
    del frames
    df = df.sort_values(["date", "code"])
    mats = {}
    for col in ("close", "high", "low", "amount"):
        mats[col] = df.pivot(index="date", columns="code", values=col).sort_index()
    return mats


def main() -> int:
    t0 = time.time()
    print("load...", flush=True)
    mats = load_matrices()
    close, high, low, amount = mats["close"], mats["high"], mats["low"], mats["amount"]
    print(f"  grid {close.shape[0]} days x {close.shape[1]} codes  ({time.time()-t0:.0f}s)", flush=True)

    # ── 日收益 / 坏柱标记 ────────────────────────────────────────────
    ret1 = close.pct_change(fill_method=None)
    lim = _limit_series(close.columns)
    lim_mat = pd.DataFrame(np.tile(lim.values, (close.shape[0], 1)),
                           index=close.index, columns=close.columns)
    bad = (ret1.abs() > lim_mat * 1.005 + BAD_TOL).fillna(0.0)
    n_bad = int(bad.values.sum())

    # 回看窗坏柱累积（含当日）：k=0..w-1（按窗口长度缓存，避免重复计算）
    _lb_cache: dict[int, pd.DataFrame] = {}

    def lookback_bad(w: int) -> pd.DataFrame:
        if w not in _lb_cache:
            acc = bad.copy()
            for k in range(1, w):
                acc = np.maximum(acc, bad.shift(k, fill_value=0.0))
            _lb_cache[w] = acc
        return _lb_cache[w]

    # 前向窗坏柱（t+1..t+FWD）
    fwd_bad = bad.copy()
    for k in range(1, FWD + 1):
        fwd_bad = np.maximum(fwd_bad, bad.shift(-k, fill_value=0.0))

    warm = close.rolling(WARMUP_WIN, min_periods=1).count() >= WARMUP_MIN
    base_valid = (close.notna()) & (close >= PRICE_FLOOR) & warm

    # ── 因子 ────────────────────────────────────────────────────────
    print("factors...", flush=True)
    ma20 = close.rolling(20, min_periods=ROLL_MIN).mean()
    amt20 = amount.rolling(20, min_periods=ROLL_MIN).mean().where(lambda x: x > 0)
    amt_pos = amount.where(amount > 0)
    fac: dict[str, pd.DataFrame] = {
        "mom20": close / close.shift(20) - 1,
        "mom60": close / close.shift(60) - 1,
        "rev5": close / close.shift(5) - 1,
        "vol20": ret1.rolling(20, min_periods=ROLL_MIN).std(),
        "amp20": ((high - low) / close).rolling(20, min_periods=ROLL_MIN).mean(),
        "liq20": np.log(amt20),
        "illiq20": (ret1.abs() / amt_pos).rolling(20, min_periods=ROLL_MIN).mean(),
        "dist20": close / ma20 - 1,
    }
    fac_valid = {}
    for name, (_, lb) in FACTORS.items():
        lb_eff = min(lb, 20) if name != "mom60" else 60
        fac_valid[name] = base_valid & fac[name].notna() & (lookback_bad(lb_eff) == 0)
        fac[name] = fac[name].where(fac_valid[name])

    # ── 前向收益 ────────────────────────────────────────────────────
    fwd = (close.shift(-FWD) / close - 1).where((fwd_bad == 0) & base_valid)

    dates = close.index
    pos = {d: i for i, d in enumerate(dates)}

    # ── 每日横截面 Spearman IC（秩相关，向量化）──────────────────────
    print("daily IC...", flush=True)
    fwd_rank = fwd.rank(axis=1, pct=True)
    ic_days = {}
    daily_ic: dict[str, pd.Series] = {}
    for name in FACTORS:
        r = fac[name].rank(axis=1, pct=True)
        m = r.notna() & fwd_rank.notna()
        n = m.sum(axis=1)
        a, b = r.where(m), fwd_rank.where(m)
        am, bm = a.mean(axis=1), b.mean(axis=1)
        cov = (a * b).mean(axis=1) - am * bm
        va = (a * a).mean(axis=1) - am * am
        vb = (b * b).mean(axis=1) - bm * bm
        ic = cov / np.sqrt(np.maximum(va * vb, 1e-24))
        ic[n < MIN_N_DAY] = np.nan
        # 预注册结论窗口：2018-01-01 起（2017-06~12 仅作因子预热，不计入统计）
        daily_ic[name] = ic[ic.index >= pd.Timestamp(WINDOW_START)].dropna()
        ic_days[name] = len(daily_ic[name])
        print(f"  {name}: ic_days={ic_days[name]}  ({time.time()-t0:.0f}s)", flush=True)

    # ── 统计与分年/分半年表 ─────────────────────────────────────────
    def period_table(s: pd.Series, freq: str) -> dict[str, float]:
        if freq == "Y":
            g = s.groupby(s.index.year)
        else:
            g = s.groupby(s.index.year.astype(str) + np.where(s.index.month <= 6, "H1", "H2"))
        return {str(k): round(float(v), 4) for k, v in g.mean().items()}

    stats = {}
    for name in FACTORS:
        s = daily_ic[name]
        mu, sd = float(s.mean()), float(s.std())
        nd = len(s)
        t = mu / sd * math_sqrt(nd) if sd > 0 and nd else 0.0
        stats[name] = {
            "prior_sign": FACTORS[name][0],
            "n_days": nd,
            "mean_ic": round(mu, 4),
            "std": round(sd, 4),
            "icir": round(mu / sd, 3) if sd > 0 else 0.0,
            "t_stat": round(t, 2),
            "win_rate": round(float((s > 0).mean()), 3),
            "significant": bool(abs(mu) > 1.96 * sd / math_sqrt(nd) if nd > 1 else False),
            "by_year": period_table(s, "Y"),
            "by_half": period_table(s, "H"),
        }

    # ── 单因子组合：每 REBAL_EVERY 日非重叠换仓，持 FWD 日，扣成本 ────
    print("portfolios...", flush=True)
    valid_days = [d for d in dates if not np.isnan(daily_ic[FIRST_FACTOR].reindex([d]).iloc[0])]
    rebal = valid_days[::REBAL_EVERY]
    rebal = [d for d in rebal if pos[d] + FWD < len(dates)]
    port = {name: {"p": [], "b": [], "ls": [], "d": []} for name in FACTORS}
    for d in rebal:
        i = pos[d]
        fv = base_valid.loc[d] & (bad.loc[d] == 0) & (fwd_bad.loc[d] == 0)
        fr = fwd.loc[d]
        bench = float(fr[fv & fr.notna()].mean()) if (fv & fr.notna()).any() else np.nan
        for name, (sgn, _) in FACTORS.items():
            col = fac[name].loc[d]
            cand = col[fv & col.notna()].dropna()
            if len(cand) < TOP_N * 2 or np.isnan(bench):
                continue
            top = cand.nlargest(TOP_N) if sgn > 0 else cand.nsmallest(TOP_N)
            bot = cand.nsmallest(TOP_N) if sgn > 0 else cand.nlargest(TOP_N)
            pr = float(fr.loc[top.index].dropna().mean()) - COST_RT
            lsv = float(fr.loc[top.index].dropna().mean() - fr.loc[bot.index].dropna().mean())
            port[name]["p"].append(pr)
            port[name]["b"].append(bench)
            port[name]["ls"].append(lsv)
            port[name]["d"].append(d)
        print(f"  rebal {d.date()}  ({time.time()-t0:.0f}s)", flush=True)

    port_stats = {}
    for name in FACTORS:
        p = pd.Series(port[name]["p"], index=pd.DatetimeIndex(port[name]["d"]))
        b = pd.Series(port[name]["b"], index=pd.DatetimeIndex(port[name]["d"]))
        l = pd.Series(port[name]["ls"], index=pd.DatetimeIndex(port[name]["d"]))
        if len(p) < 5:
            port_stats[name] = {"n_windows": len(p)}
            continue
        years = sorted(set(p.index.year))
        by_year = {}
        for y in years:
            py, by, ly = p[p.index.year == y], b[b.index.year == y], l[l.index.year == y]
            by_year[str(y)] = {
                "excess_pp": round(float(((1 + py).prod() - (1 + by).prod()) * 100), 2),
                "ls_mean_pp": round(float(ly.mean() * 100), 2),
            }
        tot_p, tot_b = float((1 + p).prod() - 1), float((1 + b).prod() - 1)
        yrs = len(p) * FWD / 244.0
        port_stats[name] = {
            "n_windows": len(p),
            "total_ret_pp": round(tot_p * 100, 1),
            "bench_ret_pp": round(tot_b * 100, 1),
            "total_excess_pp": round((tot_p - tot_b) * 100, 1),
            "annual_excess_pp": round(((1 + tot_p) ** (1 / yrs) - (1 + tot_b) ** (1 / yrs)) * 100, 2) if yrs > 0 else 0.0,
            "win_rate": round(float((p - b > 0).mean()), 3),
            "by_year": by_year,
        }

    # ── 分位组合单调性（补充诊断：区分「整体排序效应」与「极端尾部毒性」）──
    # 动机：v1 首跑发现 vol20 等因子「IC 显著为负（低波占优）但极端 TOP50 亏损」
    # —— Spearman 是秩统计量（由截面的「身体」主导），而 TOP50 是极端尾部；
    # A股尾部聚集 crash 股/涨跌停锁死股 → 尾部行为与整体相反。十分位拆开看。
    print("deciles...", flush=True)
    N_DECILE = 10
    WINS_PER_YEAR = 244.0 / FWD  # 非重叠 10 日窗口 ≈ 24.4 个/年（简单年化，未计复利与成本）
    dec: dict[str, list[list[float]]] = {name: [[] for _ in range(N_DECILE)] for name in FACTORS}
    for d in rebal:
        fv = base_valid.loc[d] & (bad.loc[d] == 0) & (fwd_bad.loc[d] == 0)
        fr = fwd.loc[d]
        for name in FACTORS:
            col = fac[name].loc[d]
            cand = col[fv & col.notna()].dropna()
            if len(cand) < MIN_N_DAY:
                continue
            q = pd.qcut(cand.rank(method="first"), N_DECILE, labels=False) + 1  # 1..10，低→高
            m = fr.loc[q.index].dropna()
            for g, vals in m.groupby(q.loc[m.index]):
                dec[name][int(g) - 1].append(float(vals.mean()))
    dec_stats = {}
    for name in FACTORS:
        vals = np.array([np.mean(x) if x else np.nan for x in dec[name]], dtype=float)
        ok = ~np.isnan(vals)
        rho = None
        if ok.sum() >= 5:
            rx = pd.Series(np.arange(1, N_DECILE + 1)[ok]).rank()
            rv = pd.Series(vals[ok]).rank()
            rho = round(float(np.corrcoef(rx, rv)[0, 1]), 2)
        dec_stats[name] = {
            "decile_ann_pp": [round(float(v * WINS_PER_YEAR * 100), 1) if not np.isnan(v) else None
                              for v in vals],
            "mono_rho": rho,
        }

    # ── 输出 ────────────────────────────────────────────────────────
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    detail = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "config": {"window_start": WINDOW_START, "fwd": FWD, "top_n": TOP_N,
                   "rebal_every": REBAL_EVERY, "cost_rt": COST_RT,
                   "min_n_day": MIN_N_DAY, "warmup": f"{WARMUP_MIN}/{WARMUP_WIN}"},
        "data_quality": {
            "grid_days": int(close.shape[0]), "grid_codes": int(close.shape[1]),
            "corrupted_bars": n_bad,
            "ic_days_first_factor": ic_days[FIRST_FACTOR],
        },
        "ic_stats": stats,
        "portfolio": port_stats,
        "deciles": dec_stats,
        "daily_ic": {k: [[d.strftime("%Y-%m-%d"), round(float(v), 4)] for d, v in s.items()]
                     for k, s in daily_ic.items()},
    }
    (OUT_DIR / "replay_detail.json").write_text(
        json.dumps(detail, ensure_ascii=False, indent=1), encoding="utf-8")

    lines = ["# 历史因子 IC 回放（纯价格因子 · 预注册 v1）", "",
             f"- 生成：{detail['generated_at']}　窗口：{WINDOW_START} 起　T+{FWD}　TOP{TOP_N}　每{REBAL_EVERY}日非重叠换仓　成本{COST_RT:.1%}",
             f"- 数据：{close.shape[0]} 交易日 × {close.shape[1]} 只；坏柱（超涨跌停限制）{n_bad} 根已剔除污染窗口；IC 有效日 {ic_days[FIRST_FACTOR]}",
             f"- 方向 = 经济先验（写死）；**显著** = |均值IC| > 1.96×sd/√n", ""]
    lines += ["## 一、每日横截面 IC（长历史 → 淘汰与压力测试用）", "",
              "| 因子 | 先验方向 | 有效日 | 均值IC | ICIR | t | 胜率 | 显著 |",
              "|---|---|---|---|---|---|---|---|"]
    for name, st in stats.items():
        sgn = "多高值" if st["prior_sign"] > 0 else "多低值"
        lines.append(f"| {name} | {sgn} | {st['n_days']} | {st['mean_ic']:+.4f} | {st['icir']:+.3f} | "
                     f"{st['t_stat']:+.2f} | {st['win_rate']:.1%} | {'✅' if st['significant'] else '✗'} |")
    lines += ["", "### 分年度均值 IC（衰减形态——本报告的核心输出）", ""]
    years_all = sorted({y for st in stats.values() for y in st["by_year"]})
    lines += ["| 因子 | " + " | ".join(years_all) + " |",
              "|---|" + "---|" * len(years_all)]
    for name, st in stats.items():
        row = [f"{st['by_year'].get(y, 0):+.3f}" for y in years_all]
        lines.append(f"| {name} | " + " | ".join(row) + " |")
    lines += ["", "## 二、单因子组合（TOP50，扣成本，vs 全宇宙等权基准）", "",
              "| 因子 | 窗口数 | 全期超额(pp) | 年化超额(pp) | 胜率 |",
              "|---|---|---|---|---|"]
    for name, ps in port_stats.items():
        if "total_excess_pp" not in ps:
            lines.append(f"| {name} | {ps.get('n_windows', 0)} | n/a | n/a | n/a |")
            continue
        lines.append(f"| {name} | {ps['n_windows']} | {ps['total_excess_pp']:+.1f} | "
                     f"{ps['annual_excess_pp']:+.2f} | {ps['win_rate']:.0%} |")
    lines += ["", "### 组合分年超额（pp，扣成本后）", ""]
    pyears = sorted({y for ps in port_stats.values() for y in ps.get("by_year", {})})
    lines += ["| 因子 | " + " | ".join(pyears) + " |", "|---|" + "---|" * len(pyears)]
    for name, ps in port_stats.items():
        if "by_year" not in ps:
            continue
        row = [f"{ps['by_year'].get(y, {}).get('excess_pp', 0):+.1f}" for y in pyears]
        lines.append(f"| {name} | " + " | ".join(row) + " |")
    lines += ["", "## 三、十分位单调性（补充诊断，未计成本；年化≈单窗均值×24.4）", "",
              "区分「整体排序效应」与「极端 TOP50 尾部毒性」：若分位均值沿 D1→D10 单调、",
              "而极端 TOP50 亏损 → 毒性在尾部（crash/涨跌停股），排序本身仍有效。", "",
              "| 因子 | " + " | ".join(f"D{i}" for i in range(1, 11)) + " | 单调ρ |",
              "|---|" + "---|" * 11]
    for name, ds in dec_stats.items():
        row = [f"{v:+.1f}" if v is not None else "n/a" for v in ds["decile_ann_pp"]]
        lines.append(f"| {name} | " + " | ".join(row) + f" | {ds['mono_rho']} |")
    lines += ["", "## 四、结论判读规则（预注册）", "",
              "- 因子「存活」= 全期 IC 显著 + 分年 IC 方向大体一致（≥2/3 年份同号）；方向翻转或长期不显著 → 淘汰候选。",
              "- 本报告**不产出配权**；配权（EWM 半衰期敏感性 {21,63,126} 三档）在存活因子集确定后另行回放。",
              "", "## 五、Caveats（必读）", "",
              "- **幸存者偏差**：universe = 今天还活着的票 → 绝对收益偏乐观，**只用于因子间相对比较**。",
              "- 未剔 ST（无历史名单）；新股用 60 日预热近似剔除；换手率用成交额代理（无历史流通股本）。",
              "- T+10 内停牌的前向收益缺失 → 从均值中剔除（小幅乐观）；基准未计成本（超额偏保守）。",
              "- 深历史 ≤2017 未纳入（hithink 失真未修）；2018-2022 仍有零星跳变，已按坏柱规则剔除污染窗口。"]
    (OUT_DIR / "replay_summary.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"DONE in {time.time()-t0:.0f}s -> {OUT_DIR}", flush=True)
    return 0


def math_sqrt(n: int) -> float:
    return n ** 0.5


if __name__ == "__main__":
    raise SystemExit(main())
