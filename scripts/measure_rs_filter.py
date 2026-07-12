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
from smcore.utils.code import format_stock_code

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
            code = format_stock_code(r.get("股票代码", ""))
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
    # earliest 必须覆盖每只票信号日「前 45 天」的回看窗口（_stock_20d_return
    # 用 sd-45d 起算 20 日收益），否则切片不足 22 个交易日→返回 None→过滤全放行。
    earliest = min(sd for sd, _ in study) - timedelta(days=60)
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

    # 按信号日分组（后续每个信号日独立回测，避免「全宇宙一次性塞入导致
    # 首波买日花光现金、后续 12 天全跳过」的资金池假象）。
    from collections import defaultdict as _dd

    universe_by_date: dict[str, list[dict]] = _dd(list)
    for rec in universe:
        universe_by_date[rec["日期"]].append(rec)

    def backtest_rows(rows: list[dict]) -> dict | None:
        """对一组候选（跨多信号日）做头对头回测：每个信号日独立资金池，
        汇总全部成交作为「等权每信号」样本。返回聚合指标。"""
        if not rows:
            return None
        by_date: dict[str, list[dict]] = _dd(list)
        for r in rows:
            by_date[r["日期"]].append(r)
        all_trades: list[pd.DataFrame] = []
        for sd in sorted(by_date):
            recs = by_date[sd]
            sdf = pd.DataFrame(recs)
            res = run_forward_signal_backtest(
                # 每个信号日独立资金池，给足资金让当天全部候选都能成交
                # （per = 资金/候选数，≈每只 4 万，能买得起 100 股）。
                sdf, hold_days=HOLD_DAYS, initial_capital=20_000_000.0, max_positions=600,
                slippage=0.001, enable_exits=True, use_signal_bands=True,
                stop_loss_pct=0.08, take_profit_pct=0.06, trailing_stop_pct=0.05,
                trend_exit_ma=60,
            )
            if "error" not in res.summary and res.trades is not None and not res.trades.empty:
                all_trades.append(res.trades)
        if not all_trades:
            return None
        t = pd.concat(all_trades, ignore_index=True)
        rets = pd.to_numeric(t["return_pct"], errors="coerce").dropna()
        if rets.empty:
            return None
        return {
            "num_trades": int(len(t)),
            "avg_return": round(float(rets.mean()), 2),
            "win_rate": round(float((rets > 0).mean() * 100), 1),
        }

    # 3) 不过滤基线
    base = backtest_rows(universe)
    if base is None:
        print("基线回测失败")
        return 1
    print(f"\n[不过滤基线] 平均收益={base['avg_return']:.2f}% 胜率={base['win_rate']:.1f}% 交易={base['num_trades']}")

    # 4) 按 TOL 过滤
    print(f"\n[出场配置] 硬止损=-8% 固定止盈=+6% 移动止盈=-5% 趋势破位=MA60 持有={HOLD_DAYS}日")
    # 诊断：RS 数据可用性（确认过滤这次真能算出、不再全放行）
    n_stock_ok = n_idx_ok = 0
    for rec in universe:
        sd = datetime.strptime(rec["日期"], "%Y-%m-%d").date()
        if _stock_20d_return(rec["代码"], sd, cached_fetch) is not None:
            n_stock_ok += 1
        if _index_20d_return(sd.strftime("%Y%m%d")) is not None:
            n_idx_ok += 1
    print(f"[RS数据] 个股20日收益有效={n_stock_ok}/{len(universe)}  沪深300收益有效={n_idx_ok}/{len(universe)}")
    print(f"{'TOL':>6s} {'剔除':>5s} {'保留':>5s} {'平均收益%':>10s} {'胜率%':>8s} {'交易':>6s} {'Δ收益':>8s}")
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
        s = backtest_rows(kept)
        if s is None:
            print(f"{tol:>6.2f} {dropped:>5d} {len(kept):>5d}  (回测失败)")
            continue
        delta = s["avg_return"] - base["avg_return"]
        print(f"{tol:>6.2f} {dropped:>5d} {len(kept):>5d} {s['avg_return']:>10.2f} "
              f"{s['win_rate']:>8.1f} {s['num_trades']:>6d} {delta:>+8.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
