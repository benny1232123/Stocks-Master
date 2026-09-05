#!/usr/bin/env python3
"""临时工具（20260905 分批止盈扫描）：聚合当前回测结果并归档到 ptp_scratch/<tag>。

采纳标准（预先注册，防事后挑数字）——相对基线 hold12（日均+0.022% 胜率61.11 夏普-0.047）：
  日均收益 >= +0.05pp 且 配对日中位差 >= 0 且 改善集中度（前5日贡献占比）< 60%
  且 胜率降幅 <= 1pp。无配置全部满足 → 保持 trigger 4% / tranche 33% 并冻结一季度。
"""
import argparse
import glob
import re
import shutil
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "stock_data"
SCRATCH = DATA / "archive" / "ptp_scratch"

ap = argparse.ArgumentParser()
ap.add_argument("--tag", required=True)
ap.add_argument("--window", type=int, default=12)
a = ap.parse_args()

rows = []
for f in glob.glob(str(DATA / "Multi-Backtest-*-summary.csv")):
    try:
        df = pd.read_csv(f, encoding="utf-8-sig")
        if not df.empty:
            rows.append(df.iloc[0])
    except Exception:
        pass
d = pd.DataFrame(rows)
dd = d[d["hold_days"] == a.window]

trows = []
for f in glob.glob(str(DATA / "Multi-Backtest-*-trades.csv")):
    try:
        t = pd.read_csv(f, encoding="utf-8-sig")
        if not t.empty:
            trows.append(t)
    except Exception:
        pass
t = pd.concat(trows, ignore_index=True) if trows else pd.DataFrame(columns=["return_pct", "exit_reason"])

out = {"tag": a.tag, "完成窗口日数": len(dd)}
if len(dd):
    out.update({
        "日均收益率%": round(dd["total_return"].astype(float).mean(), 3),
        "胜率%": round(dd[dd["num_trades"] > 0]["win_rate"].astype(float).mean(), 2),
        "夏普": round(dd[dd["num_trades"] > 0]["sharpe"].astype(float).mean(), 3),
        "回撤%": round(dd["max_drawdown"].astype(float).mean(), 3),
    })
if len(t):
    out["笔数"] = len(t)
    out["单笔均值%"] = round(t["return_pct"].mean(), 2)
    for r in ("max_hold", "take_partial", "stop_hard"):
        s = t[t["exit_reason"] == r]["return_pct"]
        if len(s):
            out[f"{r}"] = f"{len(s)}笔/{s.mean():+.2f}%"

# 保存配对分析所需的逐日数据（供事后配对比较）
per_day = {re.search(r"(\d{8})", f).group(1): pd.read_csv(f, encoding="utf-8-sig").iloc[0].to_dict()
           for f in glob.glob(str(DATA / "Multi-Backtest-*-summary.csv"))}
SCRATCH.mkdir(parents=True, exist_ok=True)
pd.DataFrame([{"date": k, **v} for k, v in per_day.items()]).to_csv(
    SCRATCH / f"per-day-{a.tag}.csv", index=False, encoding="utf-8-sig")

import json
print("[agg] " + json.dumps(out, ensure_ascii=False))

# 归档并清空当前结果
dst = SCRATCH / a.tag
dst.mkdir(exist_ok=True)
for f in DATA.glob("Multi-Backtest-*.csv"):
    shutil.move(str(f), str(dst / f.name))
