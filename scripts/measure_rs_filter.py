"""验证「个股相对大盘强度过滤」对前向回测的增量改善。

做法（与 measure_strategy_edge.py 同源、头对头）：
- 读所有已完成 10 日窗口的 Daily-Action-List（即当时融合实际产出的候选宇宙），
- 对每只候选算其信号日近 20 日收益 vs 沪深300 同期收益，
- 跑输大盘超阈值 TOL 的剔除（动量票豁免），
- 用与 v2 完全相同的改进引擎（硬止损+真实成本+MA60破位+收紧止盈）做前向 10 日回测，
- 对比「不过滤」与「过滤」(扫 TOL=0/0.03/0.05) 的总收益/胜率/回撤/夏普。

自包含：脚本内同时跑不过滤基线，无需依赖历史日志。仅做研究，不改生产文件。
"""
from __future__ import annotations

import glob
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

os.environ.setdefault("KLINE_BACKEND", "baostock")

from smcore.backtest.engine import run_forward_signal_backtest
import smcore.data.kline as kline_mod
from smcore.strategy.fusion import _passes_relative_strength_filter, _index_20d_return

STUDY_DIR = "stock_data"
TODAY = date.today()
HOLD_DAYS = 10
TOLS = [0.0, 0.03, 0.05]


def _signal_date_from_name(path: str) -> date | None:
    name = os.path.basename(path)
    suffix = name.replace("Daily-Action-List-", "").replace(".csv", "")
    if len(suffix) == 8 and suffix.isdigit():
        return date(int(suffix[:4]), int(suffix[4:6]), int(suffix[6:8]))
    return None


def _stock_20d_return(code: str, sd: date, cached_fetch) -> float | None:
    """信号日 sd 当日的近 20 日收益率。"""
    end = sd.strftime("%Y-%m-%d")
    start = (sd - timedelta(days=25)).strftime("%Y-%m-%d")
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


def main() -> int:
    files = sorted(glob.glob(os.path.join(STUDY_DIR, "Daily-Action-List-*.csv")))
    study = []
    for f in files:
        sd = _signal_date_from_name(f)
        if sd and (TODAY - sd).days >= 15:
            study.append((sd, f))
    if not study:
        print("无已完成窗口的信号日可供测量")
        return 1
    print(f"测量信号日数: {len(study)}  ({study[0][0]} ~ {study[-1][0]})")

    # 1) 收集候选宇宙（与 v2 基线相同：每个信号日融合产出全部候选）
    universe: list[dict] = []
    unique_codes: set[str] = set()
    for sd, f in study:
        df = pd.read_csv(f)
        sd_str = sd.strftime("%Y-%m-%d")
        for _, r in df.iterrows():
            code = str(r.get("股票代码", "")).strip()
            if not code:
                continue
            unique_codes.add(code)
            universe.append({
                "日期": sd_str,
                "代码": code,
                "综合评分": float(r.get("综合评分", 0) or 0),
                "来源策略": str(r.get("来源策略", "") or ""),
                "建议买入价": r.get("建议买入价"),
                "止损价(下轨)": r.get("止损价(下轨)"),
                "止盈价(上轨)": r.get("止盈价(上轨)"),
            })
    print(f"候选总数: {len(universe)}  唯一标的: {len(unique_codes)}")

    # 2) 预拉 K 线到缓存
    earliest = min(sd for sd, _ in study) - timedelta(days=5)
    latest = max(sd for sd, _ in study) + timedelta(days=HOLD_DAYS + 20)
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
        if done % 25 == 0:
            print(f"  已拉 {done}/{len(unique_codes)}  ({(time.time()-t0):.0f}s)")
    print(f"  K线预拉完成 {(time.time()-t0):.0f}s")

    kline_mod.fetch_daily_k = cached_fetch
    import smcore.backtest.engine as eng_mod
    eng_mod.fetch_daily_k = cached_fetch

    def run_set(rows: list[dict]):
        if not rows:
            return None
        sdf = pd.DataFrame(rows)
        res = run_forward_signal_backtest(
            sdf, hold_days=HOLD_DAYS, initial_capital=100000.0, max_positions=200,
            slippage=0.001, enable_exits=True, use_signal_bands=True,
            stop_loss_pct=0.08, take_profit_pct=0.06, trailing_stop_pct=0.05,
            trend_exit_ma=60,
        )
        return res.summary if "error" not in res.summary else None

    # 3) 不过滤基线
    base_sum = run_set(universe)
    print(f"\n[不过滤基线] 收益={base_sum['total_return']:.2f}% 胜率={base_sum['win_rate']:.1f}% "
          f"回撤={base_sum['max_drawdown']:.2f}% 夏普={base_sum['sharpe']:.2f} 交易={base_sum['num_trades']}")

    # 4) 按 TOL 过滤
    print(f"\n[出场配置] 硬止损=-8% 固定止盈=+6% 移动止盈=-5% 趋势破位=MA60 持有={HOLD_DAYS}日")
    print(f"{'TOL':>6s} {'剔除':>5s} {'保留':>5s} {'收益%':>8s} {'胜率%':>8s} {'回撤%':>8s} {'夏普':>7s} {'交易':>6s} {'Δ收益':>8s}")
    for tol in TOLS:
        kept = []
        dropped = 0
        for rec in universe:
            sd = datetime.strptime(rec["日期"], "%Y-%m-%d").date()
            sd_yyyymmdd = sd.strftime("%Y%m%d")
            hit = [s.strip() for s in rec["来源策略"].split("/") if s.strip()]
            stock_ret = _stock_20d_return(rec["代码"], sd, cached_fetch)
            index_ret = _index_20d_return(sd_yyyymmdd)
            if not _passes_relative_strength_filter(hit, stock_ret, index_ret, tol=tol):
                dropped += 1
                continue
            kept.append(rec)
        s = run_set(kept)
        if s is None:
            print(f"{tol:>6.2f} {dropped:>5d} {len(kept):>5d}  (回测失败)")
            continue
        delta = s["total_return"] - base_sum["total_return"]
        print(f"{tol:>6.2f} {dropped:>5d} {len(kept):>5d} {s['total_return']:>8.2f} "
              f"{s['win_rate']:>8.1f} {s['max_drawdown']:>8.2f} {s['sharpe']:>7.2f} "
              f"{s['num_trades']:>6d} {delta:>+8.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
