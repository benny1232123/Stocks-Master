#!/usr/bin/env python3
"""一次性：仅重跑指定两天的回测（0623/0702），验证是否仍 0 笔交易。

使用磁盘 K 线缓存（回填时已填充），避免重跑整批 23 天。
"""
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("KLINE_BACKEND", "akshare")

from daily_backtest import _backtest_one
from smcore.artifacts import STOCK_DATA_DIR

TARGETS = [
    ("20260623", date(2026, 6, 23)),
    ("20260702", date(2026, 7, 2)),
]

for tag, sd in TARGETS:
    p = STOCK_DATA_DIR / f"Daily-Action-List-{tag}.csv"
    if not p.exists():
        print(f"[skip] {tag} 无 Daily-Action-List 文件", flush=True)
        continue
    print(f"=== 重跑 {tag} ===", flush=True)
    res = _backtest_one(p, sd, 10)
    print(f"{tag} => {res}", flush=True)
print("DONE", flush=True)
