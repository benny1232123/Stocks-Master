#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Barra 风格风险模型 CLI：估计因子协方差 + 组合预测波动 + 因子风险贡献。

用法：
    python scripts/run_risk_model.py [--signal-date YYYYMMDD] [--window 60]
        [--emit-json stock_data/risk_model.json] [--emit-md stock_data/risk_model.md]
    （默认读最新 Daily-Action-List 的标的与权重；也可 --codes 600000,600001 --weights 0.5,0.5）
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy import risk_model as rm  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--signal-date", default=None, help="信号日 YYYYMMDD（默认最新 DAL）")
    ap.add_argument("--window", type=int, default=60, help="因子收益估计窗口(交易日)")
    ap.add_argument("--codes", default=None, help="逗号分隔股票代码（覆盖 DAL）")
    ap.add_argument("--weights", default=None, help="逗号分隔权重（与 --codes 对应）")
    ap.add_argument("--emit-json", default=None)
    ap.add_argument("--emit-md", default=None)
    args = ap.parse_args()

    codes = None
    weights = None
    if args.codes:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
        if args.weights:
            weights = {c: float(w) for c, w in zip(codes, args.weights.split(","))}
    res = rm.run_risk_model_report(codes=codes, weights=weights,
                                   as_of=args.signal_date, window=args.window)
    print(rm.format_risk_report(res))
    if args.emit_json:
        try:
            Path(args.emit_json).write_text(
                json.dumps(res, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
            print(f"\nJSON 已写：{args.emit_json}")
        except Exception as e:  # pragma: no cover
            print(f"写 JSON 失败：{e}")
    if args.emit_md:
        try:
            Path(args.emit_md).write_text(rm.format_risk_report(res), encoding="utf-8")
            print(f"Markdown 已写：{args.emit_md}")
        except Exception as e:  # pragma: no cover
            print(f"写 Markdown 失败：{e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
