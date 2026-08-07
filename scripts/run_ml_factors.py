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
    from smcore.config.defaults import STOCK_DATA_DIR
    import pandas as pd
    dals = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))
    if not dals:
        return []
    try:
        d = pd.read_csv(dals[-1], encoding="utf-8-sig")
        return d["股票代码"].astype(str).tolist()
    except Exception:
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
