"""在「已 RS 过滤」的候选宇宙上，测试「按策略分组自适应出场」：
- 假设：均值回归类(Boll/Relativity)买点常低于 MA60，MA60 破位出场一进场就触发，
  反而砍掉反弹 → 应关闭 MA60 破位；顺势类(Momentum)保留 MA60 破位。
- 对比：GLOBAL_ON(全开,当前) / GLOBAL_OFF(全关) / HYBRID(均值回归关、顺势开)
- 另拆 Relativity-only / Momentum-only 子集看 MA60 开/关差异，定位真实作用对象。

仅做研究，不改生产文件。
"""
from __future__ import annotations

import glob
import os
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

MR_STRATS = {"Boll", "Relativity"}  # 均值回归类
TS_STRATS = {"Momentum"}            # 顺势类


def _signal_date_from_name(path: str) -> date | None:
    name = os.path.basename(path)
    suffix = name.replace("Daily-Action-List-", "").replace(".csv", "")
    if len(suffix) == 8 and suffix.isdigit():
        return date(int(suffix[:4]), int(suffix[4:6]), int(suffix[6:8]))
    return None


def _stock_20d_return(df: pd.DataFrame) -> float | None:
    if df is None or len(df) < 22:
        return None
    close = pd.to_numeric(df["close"], errors="coerce").dropna()
    if len(close) < 22:
        return None
    prev = close.iloc[-22]
    if prev == 0 or pd.isna(prev):
        return None
    return float(close.iloc[-1]) / float(prev) - 1


def main() -> int:
    files = sorted(glob.glob(os.path.join(STUDY_DIR, "Daily-Action-List-*.csv")))
    study = [(sd, f) for f in files if (sd := _signal_date_from_name(f)) and (TODAY - sd).days >= 15]
    if not study:
        print("无已完成窗口的信号日"); return 1
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
                             "来源策略": str(r.get("来源策略", "") or "")})
    print(f"候选总数: {len(universe)}  唯一标的: {len(unique_codes)}")

    # 采样：与 measure_exit_hold / measure_signal_quality 一致（700 唯一标的，种子 20260626），
    # 避免 1719 只全拉 K 线超过前台 600s 上限。相对排名/分组不依赖全样本。
    import random
    SAMPLE_N = 700
    _rng = random.Random(20260626)
    _sampled = set(_rng.sample(sorted(unique_codes), min(SAMPLE_N, len(unique_codes))))
    universe = [r for r in universe if r["代码"] in _sampled]
    unique_codes = _sampled
    print(f"[采样] 取 {len(unique_codes)} 只唯一标的（种子=20260626）")

    earliest = min(sd for sd, _ in study) - timedelta(days=60)
    latest = max(sd for sd, _ in study) + timedelta(days=30)
    cache: dict[str, pd.DataFrame] = {}

    def cached_fetch(code6, start, end, adjust="qfq"):
        if code6 not in cache or cache[code6] is None or (code6 in cache and cache[code6].empty):
            cache[code6] = kline_mod.fetch_daily_k(code6, earliest, latest, adjust)
        df = cache[code6]
        if df is None or df.empty:
            return df
        mask = (df["date"] >= str(start)) & (df["date"] <= str(end))
        return df.loc[mask].copy()

    print("预拉 K 线 (缓存命中应很快) ...")
    t0 = time.time()
    for i, code in enumerate(sorted(unique_codes), 1):
        cached_fetch(code, earliest, latest)
        if i % 400 == 0:
            print(f"  已拉 {i}/{len(unique_codes)}  ({(time.time()-t0):.0f}s)")
    print(f"  K线预拉完成 {(time.time()-t0):.0f}s")

    kline_mod.fetch_daily_k = cached_fetch
    import smcore.backtest.engine as eng_mod
    eng_mod.fetch_daily_k = cached_fetch

    improved: list[dict] = []
    for rec in universe:
        sd = datetime.strptime(rec["日期"], "%Y-%m-%d").date()
        hit = [s.strip() for s in rec["来源策略"].split("/") if s.strip()]
        sub = cached_fetch(rec["代码"], (sd - timedelta(days=45)).strftime("%Y-%m-%d"), sd.strftime("%Y-%m-%d"))
        stock_ret = _stock_20d_return(sub)
        index_ret = _index_20d_return(sd.strftime("%Y%m%d"))
        if _passes_relative_strength_filter(hit, stock_ret, index_ret, tol=RS_TOL):
            rec["is_mr"] = bool(MR_STRATS & set(hit))
            improved.append(rec)
    print(f"RS 过滤(TOL={RS_TOL}) 剔除 {len(universe)-len(improved)} → 保留 {len(improved)}")

    def backtest(rows: list[dict], trend_exit_ma) -> dict | None:
        if not rows:
            return None
        sdf = pd.DataFrame(rows)
        res = run_forward_signal_backtest(
            sdf, hold_days=10, initial_capital=20_000_000.0, max_positions=500,
            slippage=0.001, enable_exits=True, use_signal_bands=True,
            stop_loss_pct=0.08, take_profit_pct=0.06, trailing_stop_pct=0.05,
            trend_exit_ma=trend_exit_ma,
        )
        return res.summary if "error" not in res.summary else None

    def merge(results: list[dict]) -> dict | None:
        rs = [r for r in results if r]
        if not rs:
            return None
        n = sum(r["num_trades"] for r in rs)
        if n == 0:
            return None
        return {
            "avg_return": sum(r["avg_return"] * r["num_trades"] for r in rs) / n,
            "win_rate": sum(r["win_rate"] * r["num_trades"] for r in rs) / n,
            "profit_factor": sum(r["profit_factor"] * r["num_trades"] for r in rs) / n,
            "max_drawdown": sum(r["max_drawdown"] * r["num_trades"] for r in rs) / n,
            "num_trades": n,
        }

    def show(tag: str, s: dict | None):
        if s is None:
            print(f"{tag:<22s} (无数据)"); return
        print(f"{tag:<22s} 均{s['avg_return']:>7.2f}% 胜{s['win_rate']:>5.1f}% "
              f"盈亏比{s['profit_factor']:>5.2f} 回撤{s['max_drawdown']:>6.2f}% 交易{s['num_trades']:>5d}")

    mr_rows = [r for r in improved if r["is_mr"]]
    ts_rows = [r for r in improved if not r["is_mr"]]

    print(f"\n[按策略分组自适应出场] 改进宇宙(全)={len(improved)} 均值回归={len(mr_rows)} 顺势/其他={len(ts_rows)}")
    print(f"{'配置':<22s} {'平均收益%':>9s} {'胜率%':>7s} {'盈亏比':>7s} {'回撤%':>7s} {'交易':>6s}")
    g_on = backtest(improved, 60)
    g_off = backtest(improved, None)
    # HYBRID: 均值回归关 MA60，顺势/其他开 MA60
    hybrid = merge([backtest(mr_rows, None), backtest(ts_rows, 60)])
    show("GLOBAL_ON(当前)", g_on)
    show("GLOBAL_OFF", g_off)
    show("HYBRID(均值回归关)", hybrid)

    print(f"\n[子集洞察]")
    show("Relativity类 MA60开", backtest(mr_rows, 60))
    show("Relativity类 MA60关", backtest(mr_rows, None))
    show("顺势类 MA60开", backtest(ts_rows, 60))
    show("顺势类 MA60关", backtest(ts_rows, None))

    if g_on and hybrid:
        print(f"\n[结论] HYBRID 相对当前 GLOBAL_ON: Δ收益={hybrid['avg_return']-g_on['avg_return']:+.2f}% "
              f"Δ胜率={hybrid['win_rate']-g_on['win_rate']:+.1f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
