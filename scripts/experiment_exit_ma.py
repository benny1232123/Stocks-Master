#!/usr/bin/env python3
"""离线 A/B 实验 —— 用本地 k_data 缓存对比不同 trend_exit_ma 的出场效果。

背景（2026-08 归因报告 stock_data/Attribution-latest.md）：
- trend_break（MA60 破位）出场 106 笔、均值 -4.60%、胜率 10.4% —— 最大出血点；
- max_hold 兜底 79 笔、均值 -2.30% —— 亏损单一路拖到持有期满；
- 持有 <=5 天的交易 +3.57%，12-15 天 -2.72%。

本实验对「全部已完成 10 日窗口」的信号日，用同一候选宇宙（各日 DAL 按综合评分
TOP_N）分别跑 trend_exit_ma = 60（现状）/ 20 / 30 / 关闭，比较合并逐笔收益与
逐日组合收益。全程走 fetch_daily_k 本地缓存（请求 end 被夹到 今天-1，强制离线
分支），零联网。

用法：python scripts/experiment_exit_ma.py [--top-n 30] [--hold 10]
"""
from __future__ import annotations

import argparse
import glob
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from smcore.artifacts import STOCK_DATA_DIR  # noqa: E402
from smcore.utils.code import format_stock_code  # noqa: E402
import smcore.data.kline as kline_mod  # noqa: E402
from smcore.backtest.engine import run_forward_signal_backtest  # noqa: E402

TOP_N_DEFAULT = 30


def _collect_completed(lookback_days: int) -> list[tuple[Path, date]]:
    """信号日 + hold + 缓冲 ≤ 今天-1 的清单（保证全程可离线取数）。"""
    today = date.today()
    out = []
    for path in STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"):
        m = re.search(r"(\d{8})", path.name)
        if not m:
            continue
        sd = datetime.strptime(m.group(1), "%Y%m%d").date()
        if sd + timedelta(days=lookback_days + 12) <= today - timedelta(days=1):
            out.append((path, sd))
    out.sort(key=lambda x: x[1])
    return out


def _build_signals(path: Path, sd: date, top_n: int) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", dtype={"股票代码": str})
    except Exception:
        return None
    if df.empty or "股票代码" not in df.columns:
        return None
    df["股票代码"] = df["股票代码"].apply(format_stock_code)
    df = df[df["股票代码"].str.len() >= 6]
    if "综合评分" in df.columns:
        df["_s"] = pd.to_numeric(df["综合评分"], errors="coerce")
        df = df.sort_values("_s", ascending=False).head(top_n).drop(columns=["_s"])
    if df.empty:
        return None
    sub = pd.DataFrame({"日期": [sd.strftime("%Y-%m-%d")] * len(df), "代码": df["股票代码"].values})
    for src, dst in (("止损价(下轨)", "止损价(下轨)"), ("止盈价(上轨)", "止盈价(上轨)"), ("权重", "权重")):
        if src in df.columns:
            sub[dst] = df[src].values
    if "stop_pct" in df.columns:
        sub["stop_pct"] = pd.to_numeric(df["stop_pct"], errors="coerce").where(
            lambda s: s > 0
        ).values
    return sub


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top-n", type=int, default=TOP_N_DEFAULT)
    ap.add_argument("--hold", type=int, default=10)
    ap.add_argument("--lookback", type=int, default=400, help="回溯信号日天数")
    args = ap.parse_args()

    # 离线强制：把请求 end 夹到「今天-1」，使 fetch_daily_k 走纯缓存分支（covers + 非今天）。
    # engine 在函数体内 `from smcore.data.kline import fetch_daily_k`，故 patch 源模块。
    # 另：本地 k_data 无该票时直接返回空（跳过 tdx/akshare/baostock 慢超时链），
    # 并做进程级去重（4 个 arm 共享同一批取数）。
    from smcore.data.kline import _cache_path  # noqa: E402

    _real_fetch = kline_mod.fetch_daily_k
    _cap = date.today() - timedelta(days=1)
    _seen: dict[tuple, pd.DataFrame | None] = {}

    def _offline_fetch(code, start, end, *a, **kw):
        key = (str(code), str(start), str(min(end, _cap)))
        if key in _seen:
            return _seen[key]
        if not _cache_path(format_stock_code(code), "qfq").exists():
            _seen[key] = pd.DataFrame()
            return _seen[key]
        kw.pop("force_refresh", None)
        kw["use_cache"] = True
        df = _real_fetch(code, start, min(end, _cap), *a, **kw)
        _seen[key] = df
        return df

    kline_mod.fetch_daily_k = _offline_fetch

    lists = [x for x in _collect_completed(args.hold) if x[1] >= date.today() - timedelta(days=args.lookback)]
    if not lists:
        print("无已完成窗口的信号日")
        return 1
    print(f"实验宇宙：{len(lists)} 个已完成信号日（{lists[0][1]} ~ {lists[-1][1]}），TOP {args.top_n}，hold={args.hold}")

    arms = {
        "ma60(现状)": dict(trend_exit_ma=60),
        "ma20": dict(trend_exit_ma=20),
        "ma30": dict(trend_exit_ma=30),
        "关闭趋势破位": dict(trend_exit_ma=None),
    }
    results: dict[str, list] = {k: [] for k in arms}
    day_returns: dict[str, list] = {k: [] for k in arms}

    for i, (path, sd) in enumerate(lists):
        sub = _build_signals(path, sd, args.top_n)
        if sub is None:
            continue
        for name, kw in arms.items():
            try:
                res = run_forward_signal_backtest(
                    sub.copy(),
                    hold_days=args.hold,
                    initial_capital=100000.0,
                    max_positions=200,
                    enable_exits=True,
                    use_signal_bands=True,
                    stop_loss_pct=0.08,
                    take_profit_pct=0.06,
                    trailing_stop_pct=0.05,
                    size_by="权重" if "权重" in sub.columns else None,
                    capital_scale=1.0,
                    **kw,
                )
            except Exception as e:
                print(f"  [{sd}] {name} 异常: {e}")
                continue
            if res.summary.get("error") or res.trades.empty:
                continue
            t = res.trades.copy()
            t["tag"] = sd.strftime("%Y%m%d")
            results[name].append(t)
            day_returns[name].append(res.summary.get("total_return", 0.0))
        if (i + 1) % 10 == 0:
            print(f"  ... {i + 1}/{len(lists)} 信号日完成", flush=True)

    print(f"\n{'=' * 72}")
    summary_rows = []
    for name, ts in results.items():
        if not ts:
            continue
        all_t = pd.concat(ts, ignore_index=True)
        dr = pd.Series(day_returns[name])
        tb = all_t[all_t["exit_reason"] == "trend_break"]["return_pct"]
        mh = all_t[all_t["exit_reason"] == "max_hold"]["return_pct"]
        summary_rows.append({
            "出场方案": name,
            "笔数": len(all_t),
            "毛均值%": round(all_t["return_pct"].mean(), 2),
            "胜率%": round((all_t["return_pct"] > 0).mean() * 100, 1),
            "最差%": round(all_t["return_pct"].min(), 2),
            "日均组合%": round(dr.mean(), 3),
            "组合>0天数%": round((dr > 0).mean() * 100, 1),
            "trend笔数": len(tb),
            "trend均值%": round(tb.mean(), 2) if len(tb) else None,
            "maxhold均值%": round(mh.mean(), 2) if len(mh) else None,
        })
    out = pd.DataFrame(summary_rows)
    print(out.to_string(index=False))
    out_path = STOCK_DATA_DIR / "Experiment-exit-ma.csv"
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n[已写出] {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
