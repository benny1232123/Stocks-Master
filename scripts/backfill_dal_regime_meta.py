#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按信号日回填 Daily-Action-List 的 regime 元数据（issue #13 修复的数据部分）。

背景：历史 DAL 的 meta.regime 有三类问题——
  1) 真空值：新浪指数缓存只有 320 根时，as_of 切片不足 65 根，
     _profile_from_full_series 返回 None → regime_as_of 兜底「震荡轮动」；
  2) 生成时刻值：早期版本把「今日 regime」写进历史 DAL；
  3) 缺失：CI 提交步骤只 add Daily-Action-List-*.csv，meta.json 从不落库
     （20260929/0930/20261008/0912 等 4 个 CSV 无 meta）。
另修 smcore/strategy/market.py 使 regime 成为 (closes, as_of) 的纯函数：
  · 新浪 datalen 320→1200（满足 MA60 斜率 120 根 + 分位 250 窗）；
  · 波动率分位固定取最近 250 个滚动观测（对齐 docstring 规格，消除
    同花顺 1141 根 vs 新浪 1200 根的分位基数漂移）。

本脚本对**每个** DAL 重算 pinned regime 并重写 meta（regime/regime_as_of/
date_pinned/is_today），保留 generated_at/ml_integration，变更写入
regime_backfill 审计块。幂等：已正确的 meta 重跑不变。

用法::

    python scripts/backfill_dal_regime_meta.py            # 回填并写盘
    python scripts/backfill_dal_regime_meta.py --dry-run  # 只报告会改哪些
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy.market import compute_market_profile  # noqa: E402

try:
    from smcore.config.defaults import STOCK_DATA_DIR
except Exception:  # pragma: no cover
    STOCK_DATA_DIR = ROOT / "stock_data"


def main() -> int:
    ap = argparse.ArgumentParser(description="按信号日回填 DAL meta.regime")
    ap.add_argument("--dry-run", action="store_true", help="只报告，不写盘")
    args = ap.parse_args()

    dals = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))
    if not dals:
        print("无 Daily-Action-List 文件")
        return 1

    live_regime = compute_market_profile().regime
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%S")
    n_changed = n_created = n_same = 0
    for dal in dals:
        sd = dal.name.split("Daily-Action-List-")[1].replace(".csv", "")
        pinned = compute_market_profile(as_of=sd).regime
        meta_path = dal.with_suffix(".meta.json")
        old: dict = {}
        if meta_path.exists():
            try:
                old = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                old = {}
        old_regime = old.get("regime")
        if old_regime == pinned and old.get("regime_as_of") == sd:
            n_same += 1
            continue
        meta = {
            "signal_date": sd,
            "generated_at": old.get("generated_at") or now_iso,
            "regime": pinned,
            "regime_as_of": sd,
            "live_regime": live_regime,
            "is_today": False,
            "date_pinned": True,
        }
        if "ml_integration" in old:
            meta["ml_integration"] = old["ml_integration"]
        meta["regime_backfill"] = {
            "at": now_iso,
            "from": old_regime,
            "to": pinned,
            "reason": "issue#13: regime 按信号日回填（原值可能为生成时刻状态/真空兜底/缺失）",
        }
        tag = "补建" if old_regime is None else "回填"
        if old_regime is None:
            n_created += 1
        else:
            n_changed += 1
        print(f"  {tag} {sd}: {old_regime} -> {pinned}")
        if not args.dry_run:
            meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2),
                                 encoding="utf-8")
    print(f"\n共 {len(dals)} 个 DAL：回填 {n_changed}，补建 {n_created}，已正确 {n_same}"
          f"{'（dry-run，未写盘）' if args.dry_run else ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
