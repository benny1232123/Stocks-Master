#!/usr/bin/env python3
"""一次性：仅重跑 0702 回测（带异常捕获），确认是否仍 0 笔。"""
import os
import sys
import traceback
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("KLINE_BACKEND", "akshare")

from daily_backtest import _backtest_one
from smcore.artifacts import STOCK_DATA_DIR

tag, sd = "20260702", date(2026, 7, 2)
p = STOCK_DATA_DIR / f"Daily-Action-List-{tag}.csv"
print(f"=== 重跑 {tag} ===", flush=True)
try:
    res = _backtest_one(p, sd, 9)
    print(f"{tag} => {res}", flush=True)
except Exception as e:
    print(f"{tag} EXC: {repr(e)}", flush=True)
    traceback.print_exc()
print("DONE", flush=True)
