#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""实验台账 CLI。

用法
----
  python scripts/run_experiment_ledger.py --summary                 # 打印/汇总台账
  python scripts/run_experiment_ledger.py --record \
      --title "调高主题权重" --hypothesis "主题近期 alpha 更强" \
      --signal-date 20260807 --config-before-json '{"w_theme":0.3}' \
      --config-after-json '{"w_theme":0.4}' --outcome pending
  python scripts/run_experiment_ledger.py --from-walkforward-json stock_data/wf.json \
      --signal-date 20260807                                     # 由 recommend() 产出落一笔
  python scripts/run_experiment_ledger.py --summary --emit-json stock_data/experiment_ledger_summary.json

env STOCKS_LEDGER=0 可全局关闭落盘（默认开启）。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from smcore.strategy import experiment_ledger as el  # noqa: E402


def _jarg(s):
    if s is None:
        return {}
    return json.loads(s)


def _main():
    ap = argparse.ArgumentParser(description="实验台账（Experiment Ledger）")
    ap.add_argument("--summary", action="store_true", help="打印台账汇总")
    ap.add_argument("--record", action="store_true", help="手工记录一条实验")
    ap.add_argument("--from-walkforward-json", help="由 walk_forward recommend() JSON 落一笔重验记录")
    ap.add_argument("--signal-date", help="信号日 YYYYMMDD")
    ap.add_argument("--title", help="实验标题")
    ap.add_argument("--hypothesis", default="", help="假设")
    ap.add_argument("--config-before-json", default="{}")
    ap.add_argument("--config-after-json", default="{}")
    ap.add_argument("--metrics-before-json", default="{}")
    ap.add_argument("--metrics-after-json", default="{}")
    ap.add_argument("--outcome", default="pending",
                    choices=["pending", "adopted", "rejected", "invalid"])
    ap.add_argument("--author", default="manual")
    ap.add_argument("--notes", default="")
    ap.add_argument("--emit-json", help="汇总/记录结果写出 JSON")
    ap.add_argument("--emit-md", help="汇总报告写出 markdown")
    args = ap.parse_args()

    summary = None

    if args.record:
        eid = el.record_experiment(
            title=args.title or "(未命名)",
            hypothesis=args.hypothesis,
            signal_date=args.signal_date,
            config_before=_jarg(args.config_before_json),
            config_after=_jarg(args.config_after_json),
            metrics_before=_jarg(args.metrics_before_json),
            metrics_after=_jarg(args.metrics_after_json),
            outcome=args.outcome,
            author=args.author,
            notes=args.notes,
        )
        print(f"✅ 已记录实验：{eid}")

    if args.from_walkforward_json:
        rec = json.load(open(args.from_walkforward_json, encoding="utf-8"))
        eid = el.record_calibration(rec, signal_date=args.signal_date,
                                    author="walk_forward_validator")
        print(f"✅ 已由 walk-forward 推荐落台账：{eid}")

    if args.summary or (not args.record and not args.from_walkforward_json):
        summary = el.summarize_ledger()
        print(el.format_ledger_report(summary))

    if args.emit_json and summary is not None:
        with open(args.emit_json, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
        print(f"📊 汇总 JSON 已写：{args.emit_json}")

    if args.emit_md and summary is not None:
        with open(args.emit_md, "w", encoding="utf-8") as f:
            f.write(el.format_ledger_report(summary))
        print(f"📝 汇总报告已写：{args.emit_md}")

    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
