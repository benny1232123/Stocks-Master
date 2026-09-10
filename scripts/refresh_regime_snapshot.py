"""刷新 stock_data/regime-latest.json 快照。

用途
────
``regime-latest.json`` 记录「最新市场状态 + 自适应权重」，供前端与接口展示。
历史回放 / 补跑会按日期升序调用 :func:`fuse_signals`，若不加区分地写该文件，
跑完最后一个历史信号日就会把"最新"快照顶成数周前的日期（2026-09-09 实测：
快照停在 20260729，实际已到 20260909，权重长期显示等权）。

回放侧已在 ``save_regime_snapshot(source=...)`` 修复，本脚本用于：
1. 修复后把已经污染的快照恢复为当前真实市场状态；
2. 日常巡检 / 怀疑快照陈旧时手动重算。

用法::

    python scripts/refresh_regime_snapshot.py            # 刷新并打印
    python scripts/refresh_regime_snapshot.py --dry-run   # 只打印不落盘
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from smcore.config.defaults import STOCK_DATA_DIR  # noqa: E402
from smcore.strategy.adaptive_weights import (  # noqa: E402
    cash_from_regime,
    cash_from_volatility,
    compute_adaptive_allocation,
    save_regime_snapshot,
)
from smcore.strategy.market import compute_market_profile  # noqa: E402


def build_snapshot() -> dict:
    """按与 :func:`fuse_signals` 一致的口径重算当前市场状态与自适应权重。"""
    profile = compute_market_profile()
    regime = profile.regime if profile else "震荡轮动"

    edge, adaptive_pct, _, cold = compute_adaptive_allocation()
    cash_pct = cash_from_volatility(profile.volatility_pctile if profile else None)
    if profile:
        cash_pct = cash_from_regime(regime, cash_pct)

    total_n = sum(e.get("n", 0) for e in edge.values())
    # MarketProfile 不带 as_of 字段，快照日期取「重算当天」（本脚本语义就是刷新到最新）
    return {
        "date": datetime.now().strftime("%Y%m%d"),
        "regime": regime,
        "cash_pct": round(cash_pct, 2),
        "cold_start": cold,
        "method": "refresh_script(fully_adaptive)",
        "adaptive_weights": adaptive_pct,
        "strategy_edge": {s: edge.get(s, {}) for s in adaptive_pct},
        "market_profile": profile.summary() if profile else None,
        "edge_total_n": total_n,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="刷新 regime-latest.json 快照")
    ap.add_argument("--dry-run", action="store_true", help="只打印，不落盘")
    args = ap.parse_args()

    snap = build_snapshot()
    print("── 重算结果 ──")
    print(f"regime        : {snap['regime']}")
    print(f"cash_pct      : {snap['cash_pct']}")
    print(f"cold_start    : {snap['cold_start']}  (归因样本 n={snap['edge_total_n']})")
    print("adaptive_weights:")
    for k, v in snap["adaptive_weights"].items():
        e = snap["strategy_edge"].get(k, {})
        print(f"  {k:12s} {v:6.2f}%   edge={e.get('edge')}  n={e.get('n')}")

    if args.dry_run:
        print("\n[dry-run] 未落盘。")
        return 0

    path = save_regime_snapshot(snap, source="live")
    if path is None:
        print("\n落盘失败（见上方 WARN）。", file=sys.stderr)
        return 1
    print(f"\n已写入 {path}")
    print(f"旧快照备份检查: {STOCK_DATA_DIR / 'regime-latest.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
