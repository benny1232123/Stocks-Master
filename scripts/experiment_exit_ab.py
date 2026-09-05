#!/usr/bin/env python3
"""一次性实验驱动（2026-09-05）：出场参数 A/B 扫描。

交易明细诊断（基线 = 提交 1d87831 的 930 笔）：
  - max_hold(持有期满) 268 笔 均值 -3.13% 胜率 24% —— 持有期过长兜底出场的拖累
  - stop_hard(硬止损)  71 笔 均值 -16.8% —— VOL_STOP_MULT=8 把高波动股止损放到 15% 上限
  - 止盈侧健康（take_* 合计 548 笔全部为正）

实验矩阵（每组跑完整 82 信号日前向回测）：
  baseline = HOLD_DAYS=10, VOL_STOP_MULT=8（已提交，不重跑，用其已聚合数字）
  h7  = HOLD_DAYS=7          v4  = VOL_STOP_MULT=4
  h12 = HOLD_DAYS=12         h7v4= HOLD_DAYS=7 + VOL_STOP_MULT=4

注意：实验会移动/覆盖 stock_data/Multi-Backtest-*.csv（基线已提交可随时 git 恢复）。
"""
from __future__ import annotations

import glob
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "stock_data"
SCRATCH = DATA / "archive" / "ab_scratch"


def collect_summaries() -> pd.DataFrame:
    rows = []
    for f in glob.glob(str(DATA / "Multi-Backtest-*-summary.csv")):
        df = pd.read_csv(f, encoding="utf-8-sig")
        if not df.empty:
            rows.append(df.iloc[0])
    return pd.DataFrame(rows)


def collect_trades() -> pd.DataFrame:
    rows = []
    for f in glob.glob(str(DATA / "Multi-Backtest-*-trades.csv")):
        try:
            df = pd.read_csv(f, encoding="utf-8-sig")
        except pd.errors.EmptyDataError:
            continue
        if not df.empty:
            rows.append(df)
    if not rows:
        return pd.DataFrame(columns=["return_pct", "exit_reason"])
    # 各日列可能不一致（部分日含额外列），统一 concat
    cols = sorted(set().union(*(set(r.columns) for r in rows)))
    for r in rows:
        for c in cols:
            if c not in r.columns:
                r[c] = None
    return pd.concat(rows, ignore_index=True)


def aggregate(tag: str) -> dict:
    d = collect_summaries()
    d10 = d[d["hold_days"] == 10] if "hold_days" in d and (d["hold_days"] == 10).any() else d
    t = collect_trades()
    out = {
        "config": tag,
        "days": len(d10),
        "日均收益率%": round(d10["total_return"].astype(float).mean(), 3),
        "收益率中位%": round(d10["total_return"].astype(float).median(), 3),
        "正收益日%": round((d10["total_return"].astype(float) > 0).mean() * 100, 1),
        "平均胜率%": round(d10[d10["num_trades"] > 0]["win_rate"].astype(float).mean(), 2),
        "平均夏普": round(d10[d10["num_trades"] > 0]["sharpe"].astype(float).mean(), 3),
        "日均最大回撤%": round(d10["max_drawdown"].astype(float).mean(), 3),
        "交易笔数": len(t),
        "单笔均值%": round(t["return_pct"].mean(), 2) if len(t) else None,
    }
    if len(t):
        for reason in ("max_hold", "stop_hard", "trend_break"):
            sub = t[t["exit_reason"] == reason]["return_pct"]
            out[f"{reason}笔数"] = len(sub)
            out[f"{reason}均值%"] = round(sub.mean(), 2) if len(sub) else None
    return out


def move_results(tag: str) -> None:
    SCRATCH.mkdir(parents=True, exist_ok=True)
    dst = SCRATCH / tag
    dst.mkdir(exist_ok=True)
    for f in DATA.glob("Multi-Backtest-*.csv"):
        shutil.move(str(f), str(dst / f.name))


CONFIGS = [
    # h7 已单独跑完（日均-0.074% 胜率58.34 夏普-0.073 回撤-1.78 单笔+1.86，近中性）
    ("h12", {"HOLD_DAYS": "12"}),
    ("v4", {"VOL_STOP_MULT": "4.0"}),
    ("h7v4", {"HOLD_DAYS": "7", "VOL_STOP_MULT": "4.0"}),
]


def main() -> int:
    results = []
    for tag, env in CONFIGS:
        move_results(tag)
        e = dict(
            os.environ,
            LOOKBACK_DAYS="400",
            HOLD_DAYS=env.get("HOLD_DAYS", "10"),
            VOL_STOP_MULT=env.get("VOL_STOP_MULT", "8.0"),
            BACKTEST_MIN_STRATEGIES="2",
            BACKTEST_INLINE_FILTER="1",
            PREPULL_INTERVAL="0.2",
            MPLBACKEND="Agg",
        )
        print(f"=== 实验 {tag} (HOLD_DAYS={e['HOLD_DAYS']}, VOL_STOP_MULT={e['VOL_STOP_MULT']}) ===", flush=True)
        r = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "daily_backtest.py")],
            env=e, capture_output=True, text=True, cwd=str(ROOT), timeout=1800,
        )
        if r.returncode != 0:
            print(f"{tag} FAILED rc={r.returncode}\n{r.stderr[-500:]}", flush=True)
            continue
        stats = aggregate(tag)
        results.append(stats)
        print(json.dumps(stats, ensure_ascii=False), flush=True)
        move_results(tag)

    print("\n=== 实验汇总（基线=提交 1d87831：日均-0.067% 胜率60.26 夏普-0.084 回撤-2.06 单笔+2.02）===")
    for s in results:
        print(json.dumps(s, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
