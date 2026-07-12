"""在「RS + 流动性门槛(¥1亿)」宇宙上，按来源策略分组测 最优持有天数。
理论：动量(Momentum)需更长持有让趋势跑完；均值回归(Boll/Relativity)反弹快、短持有更优；
题材/板块轮动(Theme/CCTV)介于其间。当前全局 hold=10 未必每类最优。

做法：
- 构建 RS+流动性 改进宇宙（同 measure_more_filters 的 BASE）；
- 按主策略(来源策略首个)分组；
- 每组测 hold ∈ {7,10,12,15}，记录平均收益/胜率/盈亏比/交易数；
- 计算「各组取其最优持有」的混合同期组合，与全局 hold=10 对比。

仅做研究，不改生产文件。
"""
from __future__ import annotations

import glob
import os
import random
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

os.environ.setdefault("KLINE_BACKEND", "baostock")

from smcore.backtest.engine import run_forward_signal_backtest
import smcore.data.kline as kline_mod
from smcore.strategy.fusion import _passes_relative_strength_filter, _index_20d_return
from smcore.utils.code import format_stock_code

STUDY_DIR = "stock_data"
TODAY = date.today()
RS_TOL = 0.03
MIN_AMOUNT = 1e8
SAMPLE_N = 700
RNG_SEED = 20260626
CURRENT_EXIT = dict(stop_loss_pct=0.08, take_profit_pct=0.06, trailing_stop_pct=0.05, trend_exit_ma=60)
HOLD_LIST = [7, 10, 12, 15]


def _signal_date_from_name(path: str) -> date | None:
    name = os.path.basename(path)
    suffix = name.replace("Daily-Action-List-", "").replace(".csv", "")
    if len(suffix) == 8 and suffix.isdigit():
        return date(int(suffix[:4]), int(suffix[4:6]), int(suffix[6:8]))
    return None


def _stock_20d_return(code: str, sd: date, cached_fetch) -> float | None:
    end = sd.strftime("%Y-%m-%d")
    start = (sd - timedelta(days=45)).strftime("%Y-%m-%d")
    df = cached_fetch(code, start, end)
    if df is None or len(df) < 22:
        return None
    close = pd.to_numeric(df["close"], errors="coerce").dropna()
    if len(close) < 22:
        return None
    prev = close.iloc[-22]
    if prev == 0 or pd.isna(prev):
        return None
    return float(close.iloc[-1]) / float(prev) - 1


def _primary_strategy(rec: dict) -> str:
    hit = [s.strip().lower() for s in rec["来源策略"].split("/") if s.strip()]
    # 规范化到英文键
    for h in hit:
        if "momentum" in h or "动量" in h:
            return "momentum"
        if "boll" in h or "布林" in h:
            return "boll"
        if "relativity" in h or "相对" in h:
            return "relativity"
        if "theme" in h or "题材" in h:
            return "theme"
        if "cctv" in h:
            return "cctv"
    return hit[0] if hit else "other"


def main() -> int:
    files = sorted(glob.glob(os.path.join(STUDY_DIR, "Daily-Action-List-*.csv")))
    study = []
    for f in files:
        sd = _signal_date_from_name(f)
        if sd and (TODAY - sd).days >= 15:
            study.append((sd, f))
    if not study:
        print("无已完成窗口的信号日")
        return 1
    print(f"测量信号日数: {len(study)}  ({study[0][0]} ~ {study[-1][0]})")

    universe: list[dict] = []
    unique_codes: set[str] = set()
    for sd, f in study:
        df = pd.read_csv(f)
        sd_str = sd.strftime("%Y-%m-%d")
        for _, r in df.iterrows():
            code = format_stock_code(r.get("股票代码", ""))
            if not code:
                continue
            unique_codes.add(code)
            universe.append({"日期": sd_str, "代码": code,
                             "综合评分": float(r.get("综合评分", 0) or 0),
                             "来源策略": str(r.get("来源策略", "") or "")})
    print(f"候选总数: {len(universe)}  唯一标的: {len(unique_codes)}")

    _rng = random.Random(RNG_SEED)
    _sampled = set(_rng.sample(sorted(unique_codes), min(SAMPLE_N, len(unique_codes))))
    universe = [r for r in universe if r["代码"] in _sampled]
    unique_codes = _sampled
    print(f"[采样] 取 {len(unique_codes)} 只（种子={RNG_SEED}）")

    earliest = min(sd for sd, _ in study) - timedelta(days=60)
    latest = max(sd for sd, _ in study) + timedelta(days=30)
    cache: dict[str, pd.DataFrame] = {}

    def cached_fetch(code6, start, end, adjust="qfq"):
        if code6 not in cache or cache[code6] is None or cache[code6].empty:
            cache[code6] = kline_mod.fetch_daily_k(code6, earliest, latest, adjust)
        df = cache[code6]
        if df is None or df.empty:
            return df
        mask = (df["date"] >= str(start)) & (df["date"] <= str(end))
        return df.loc[mask].copy()

    print("预拉 K 线 ...")
    t0 = time.time()
    done = 0
    for code in sorted(unique_codes):
        cached_fetch(code, earliest, latest)
        done += 1
        if done % 100 == 0:
            print(f"  已拉 {done}/{len(unique_codes)}  ({(time.time()-t0):.0f}s)")
    print(f"  K线预拉完成 {(time.time()-t0):.0f}s")

    kline_mod.fetch_daily_k = cached_fetch
    import smcore.backtest.engine as eng_mod
    eng_mod.fetch_daily_k = cached_fetch

    improved: list[dict] = []
    for rec in universe:
        sd = datetime.strptime(rec["日期"], "%Y-%m-%d").date()
        hit = [s.strip() for s in rec["来源策略"].split("/") if s.strip()]
        stock_ret = _stock_20d_return(rec["代码"], sd, cached_fetch)
        index_ret = _index_20d_return(sd.strftime("%Y%m%d"))
        if not _passes_relative_strength_filter(hit, stock_ret, index_ret, tol=RS_TOL):
            continue
        sd_str = rec["日期"]
        kdf = cached_fetch(rec["代码"], sd_str, sd_str)
        amt = None
        if kdf is not None and not kdf.empty:
            a = pd.to_numeric(kdf.iloc[-1].get("amount"), errors="coerce")
            amt = float(a) if not pd.isna(a) else None
        if amt is None or amt < MIN_AMOUNT:
            continue
        rec["primary"] = _primary_strategy(rec)
        improved.append(rec)
    print(f"RS+流动性 保留 {len(improved)}")

    by_strat: dict[str, list[dict]] = defaultdict(list)
    for rec in improved:
        by_strat[rec["primary"]].append(rec)
    print("策略分组规模: " + ", ".join(f"{k}={len(v)}" for k, v in sorted(by_strat.items())))

    def backtest(rows: list[dict], hold: int) -> dict | None:
        if not rows:
            return None
        sdf = pd.DataFrame(rows)
        res = run_forward_signal_backtest(
            sdf, hold_days=hold, initial_capital=20_000_000.0, max_positions=500,
            slippage=0.001, enable_exits=True, use_signal_bands=True,
            stop_loss_pct=CURRENT_EXIT["stop_loss_pct"], take_profit_pct=CURRENT_EXIT["take_profit_pct"],
            trailing_stop_pct=CURRENT_EXIT["trailing_stop_pct"], trend_exit_ma=CURRENT_EXIT["trend_exit_ma"],
        )
        return res.summary if "error" not in res.summary else None

    print(f"\n[按策略最优持有] 每组 hold ∈ {HOLD_LIST}")
    print(f"{'策略':<11s} {'hold':>4s} {'平均收益%':>9s} {'胜率%':>7s} {'盈亏比':>7s} {'交易':>6s}")
    best_per: dict[str, tuple[int, dict]] = {}
    for strat, rows in sorted(by_strat.items()):
        if len(rows) < 10:
            print(f"{strat:<11s} (样本{n}不足, 跳过)" % {"n": len(rows)})
            continue
        for hd in HOLD_LIST:
            s = backtest(rows, hd)
            if s is None:
                print(f"{strat:<11s} {hd:>4d}  (失败)")
                continue
            print(f"{strat:<11s} {hd:>4d} {s['avg_return']:>9.2f} {s['win_rate']:>7.1f} "
                  f"{s['profit_factor']:>7.2f} {s['num_trades']:>6d}")
            if strat not in best_per or s["avg_return"] > best_per[strat][1]["avg_return"]:
                best_per[strat] = (hd, s)
    # 混合同期组合：每组取各自最优持有
    print("\n[混合同期组合] 各组取最优持有 vs 全局 hold=10")
    all_h10 = backtest([r for rs in by_strat.values() for r in rs], 10)
    if all_h10:
        print(f"  全局 hold=10: 均{all_h10['avg_return']:.2f}% 胜{all_h10['win_rate']:.1f}% 盈亏比{all_h10['profit_factor']:.2f} 交易{all_h10['num_trades']}")
    comb_trades = 0
    comb_avg_num = 0.0
    comb_win_num = 0.0
    for strat, (hd, s) in best_per.items():
        comb_trades += s["num_trades"]
        comb_avg_num += s["avg_return"] * s["num_trades"]
        comb_win_num += s["win_rate"] * s["num_trades"]
        print(f"  {strat:<11s} hold={hd}: 均{s['avg_return']:.2f}% 胜{s['win_rate']:.1f}% 交易{s['num_trades']}")
    if comb_trades > 0:
        comb_avg = comb_avg_num / comb_trades
        comb_win = comb_win_num / comb_trades
        print(f"  => 混合组合: 均{comb_avg:.2f}% 胜{comb_win:.1f}% 交易{comb_trades}")
        if all_h10:
            print(f"  => Δ vs 全局 hold=10: {comb_avg - all_h10['avg_return']:+.2f}pp 平均收益")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
