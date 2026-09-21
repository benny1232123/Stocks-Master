#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""填充基本面因子缓存（在**有网环境**运行；离线沙箱会优雅降级，无需运行此脚本）。

遍历所有 Daily-Action-List 的候选票，调用 fundamental.fetch_fundamental(force=True)
联网拉取并缓存到 stock_data/fundamental_cache/（spot_snapshot.csv 全 A 估值 + 个股 JSON）。

缓存现为 **v2 按报告期存历史**（含 baostock 真实公告日 pubDate），供回测/选股按信号日
做 Point-in-Time 选期——历史回补绝不使用「信号日之后才公告」的财报（消除未来函数）。
填充后，生产选股/回测的 factor_scoring 在 use_fundamentals=true 时即可激活质量/估值/资金流因子。

用法：
  python scripts/refresh_fundamentals.py
  VE_MAX_DAYS=30 python scripts/refresh_fundamentals.py   # 仅最近 N 个信号日的票
  python scripts/refresh_fundamentals.py --from-holdings  # 只刷当前持仓（CI 持仓日报用）
  python scripts/refresh_fundamentals.py --codes 600519,000001
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy.fundamental import refresh_all, fund_cache_exists  # noqa: E402
from smcore.config.defaults import PROJECT_ROOT  # noqa: E402

STOCK_DATA_DIR = PROJECT_ROOT / "stock_data"
MAX_DAYS = int(os.environ.get("VE_MAX_DAYS", "0"))


def _held_codes() -> list[str]:
    """当前 FIFO 开仓持仓代码（去重保序）。失败返回 []。

    持仓日报场景：只刷这几只票的基本面缓存即可，避免为日报拉全候选池（几百只）。
    """
    try:
        from smcore.holdings import compute_fifo_positions, load_trades

        pos_df, _closed = compute_fifo_positions(load_trades())
        if pos_df is None or pos_df.empty or "代码" not in pos_df.columns:
            return []
        return list(
            dict.fromkeys(str(c).strip() for c in pos_df["代码"].tolist() if str(c).strip())
        )
    except Exception as exc:
        print(f"[refresh_fundamentals] 读取持仓失败：{type(exc).__name__}: {exc}", file=sys.stderr)
        return []


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
    ap = argparse.ArgumentParser(description="填充基本面因子缓存（需联网）")
    ap.add_argument("--from-holdings", action="store_true",
                    help="只刷新当前 FIFO 持仓（CI 持仓日报用；best-effort，失败不阻塞报告）")
    ap.add_argument("--codes", default="", help="逗号分隔，只刷新这些代码")
    args = ap.parse_args()

    scoped = args.from_holdings or bool(args.codes.strip())
    if args.from_holdings:
        codes = _held_codes()
        if not codes:
            print("[refresh_fundamentals] 当前无持仓 → 无需刷新基本面缓存")
            return 0
        print(f"[refresh_fundamentals] 持仓定向刷新：{len(codes)} 只")
    elif args.codes.strip():
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
        print(f"[refresh_fundamentals] 指定代码刷新：{len(codes)} 只")
    else:
        codes = _collect_codes()
        print(f"候选票 {len(codes)} 只，开始联网填充基本面缓存（失败自动降级跳过）...")

    n = refresh_all(codes)
    missing = [c for c in codes if not fund_cache_exists(c)]
    print(f"成功填充 {n}/{len(codes)} 只。缓存目录：{STOCK_DATA_DIR / 'fundamental_cache'}")
    if missing:
        print(f"::warning::[refresh_fundamentals] 仍有 {len(missing)} 只无缓存：{', '.join(missing)}"
              f"（报告将对这几只标注「纯技术面」）")
    if n == 0 and codes:
        print("[warn] 全部拉取失败：本环境可能无网或被数据源限制。请在联网主机运行本脚本。")
        # 归一化语义：定向模式（持仓日报）是 best-effort —— 报告自带 cache-only 降级与
        # 纯技术面标注，不该因基本面拉取失败而阻塞整条日报链路，故不返回 1。
        return 0 if scoped else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
