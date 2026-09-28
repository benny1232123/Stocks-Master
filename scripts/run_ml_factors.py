#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""ML 因子挖掘 CLI：walk-forward 堆叠评估 + 激活闸门（数据门控，防过拟合）。

用法：
    python scripts/run_ml_factors.py [--emit-json stock_data/ml_factors.json] [--emit-md stock_data/ml_factors.md]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy import ml_factors as ml  # noqa: E402
from smcore.strategy.risk_rules import CONFIG  # noqa: E402


def _signal_days():
    from walk_forward_validator import _all_signal_days
    return _all_signal_days()


def _codes_from_latest_dal():
    """最新【非空】DAL 的股票代码（ML 评估的候选池 = 当前实盘清单口径）。

    从最新往回找第一个含有效代码的 DAL——最新文件可能是假期占位/全空表
    （如 20260925 假期周五），直接取 dals[-1] 会拿到空列表 → compute_exposures(None)
    返回空 → 整个评估 0 可用日（2026-09-26 实测）。
    """
    from smcore.config.defaults import STOCK_DATA_DIR
    import pandas as pd
    dals = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"), reverse=True)
    for dal in dals:
        try:
            d = pd.read_csv(dal, encoding="utf-8-sig")
        except Exception:
            continue
        if "股票代码" not in d.columns:
            continue
        codes = [str(c).strip() for c in d["股票代码"].dropna().astype(str).tolist() if str(c).strip()]
        if codes:
            return codes
    return []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--emit-json", default=None)
    ap.add_argument("--emit-md", default=None)
    args = ap.parse_args()

    cfg = CONFIG.get("ml_factors", {})
    days = _signal_days()
    codes = _codes_from_latest_dal() or None
    res = ml.run_ml_factor_report(days, codes, cfg)
    print(ml.format_ml_report(res))
    if args.emit_json:
        try:
            Path(args.emit_json).write_text(
                json.dumps(res, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
            print(f"\nJSON 已写：{args.emit_json}")
        except Exception as e:  # pragma: no cover
            print(f"写 JSON 失败：{e}")
    if args.emit_md:
        try:
            Path(args.emit_md).write_text(ml.format_ml_report(res), encoding="utf-8")
            print(f"Markdown 已写：{args.emit_md}")
        except Exception as e:  # pragma: no cover
            print(f"写 Markdown 失败：{e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
