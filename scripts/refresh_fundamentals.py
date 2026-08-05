#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""填充基本面因子缓存（在**有网环境**运行；离线沙箱会优雅降级，无需运行此脚本）。

遍历所有 Daily-Action-List 的候选票，调用 fundamental.fetch_fundamental(force=True)
联网拉取并缓存到 stock_data/fundamental_cache/（spot_snapshot.csv 全 A 估值 + 个股 JSON）。
填充后，生产选股/回测的 factor_scoring 在 use_fundamentals=true 时即可激活质量/估值/资金流因子。

用法：
  python scripts/refresh_fundamentals.py
  VE_MAX_DAYS=30 python scripts/refresh_fundamentals.py   # 仅最近 N 个信号日的票
"""
from __future__ import annotations

import glob
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy.fundamental import refresh_all  # noqa: E402
from smcore.config.defaults import PROJECT_ROOT  # noqa: E402

STOCK_DATA_DIR = PROJECT_ROOT / "stock_data"
MAX_DAYS = int(os.environ.get("VE_MAX_DAYS", "0"))


def _collect_codes() -> list[str]:
    paths = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))
    if MAX_DAYS:
        paths = paths[-MAX_DAYS:]
    codes: set[str] = set()
    for p in paths:
        try:
            import pandas as pd
            d = pd.read_csv(p, encoding="utf-8-sig")
        except Exception:
            continue
        if "股票代码" not in d.columns:
            continue
        for c in d["股票代码"].dropna().astype(str).str.strip():
            if c:
                codes.add(c)
    return sorted(codes)


def main() -> int:
    codes = _collect_codes()
    print(f"候选票 {len(codes)} 只，开始联网填充基本面缓存（失败自动降级跳过）...")
    n = refresh_all(codes)
    print(f"成功填充 {n}/{len(codes)} 只。缓存目录：{STOCK_DATA_DIR / 'fundamental_cache'}")
    if n == 0:
        print("[warn] 全部拉取失败：本环境可能无网或被数据源限制。请在联网主机运行本脚本。")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
