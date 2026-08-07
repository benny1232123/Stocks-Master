#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""TWAP/VWAP 执行质量 CLI。

用法
----
  python scripts/run_execution.py                 # 从最新 Daily-Action-List 推导订单并评估
  python scripts/run_execution.py --date 20260807 --code 600519 --amount 1000000
  python scripts/run_execution.py --algo TWAP --slices 30 --plan   # 同时落盘子单计划

输出：控制台打印 markdown 报告；--write 时写 stock_data/execution_plans/ 与报告 .md。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# 让脚本可直接运行（不依赖安装）
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from smcore.strategy import execution as ex  # noqa: E402
from smcore.strategy.risk_rules import CONFIG  # noqa: E402

try:
    from smcore.config.defaults import STOCK_DATA_DIR
except Exception:
    STOCK_DATA_DIR = Path("stock_data")


def _main():
    ap = argparse.ArgumentParser(description="TWAP/VWAP 执行质量评估")
    ap.add_argument("--date", help="信号日 YYYYMMDD（推导订单时取该日收盘）")
    ap.add_argument("--code", help="单独评估某标的")
    ap.add_argument("--amount", type=float, help="金额（元），与 --code 配合")
    ap.add_argument("--shares", type=float, help="直接给股数，与 --code 配合")
    ap.add_argument("--algo", default=None, help="TWAP / VWAP（默认取 config）")
    ap.add_argument("--slices", type=int, default=None, help="子单片数")
    ap.add_argument("--side", default="buy", help="buy / sell")
    ap.add_argument("--plan", action="store_true", help="落盘子单计划 CSV")
    ap.add_argument("--write", action="store_true", help="写出报告 md")
    ap.add_argument("--emit-json", help="写出报告 JSON 路径（CI 产物）")
    args = ap.parse_args()

    cfg = dict(CONFIG.get("execution", {}))
    if args.algo:
        cfg["default_algo"] = args.algo
    if args.slices:
        cfg["n_slices"] = args.slices

    orders = None
    if args.code and (args.amount or args.shares):
        code = args.code
        # 读取当日收盘与成交量
        from smcore.strategy.execution import _load_kdata
        kd = _load_kdata(code)
        if kd.empty:
            print(f"⚠️ {code} 无 K 线数据", file=sys.stderr)
            return 1
        row = kd.iloc[-1]
        close = float(row["close"])
        vol = float(row["volume"]) if "volume" in row else None
        if args.shares:
            total = float(args.shares)
        else:
            total = int(round(args.amount / close / 100.0)) * 100
        d0 = __import__("pandas").Timestamp(kd.index[-1]).strftime("%Y%m%d")
        orders = [{
            "side": args.side, "total_shares": total, "code": code,
            "date": args.date or d0, "daily_volume": vol,
        }]
        if args.plan:
            plan_path = ex.write_execution_plan(orders[0], cfg=cfg)
            if plan_path:
                print(f"📋 子单计划已写出: {plan_path}")

    res = ex.run_execution_report(orders=orders, as_of=args.date, cfg=cfg)
    md = ex.format_execution_report(res)
    print(md)

    if args.write and res.get("ok"):
        out = STOCK_DATA_DIR / f"Execution-Report-{args.date or 'latest'}.md"
        try:
            out.write_text(md, encoding="utf-8")
            print(f"\n📝 报告已写出: {out}")
        except Exception as e:
            print(f"⚠️ 写报告失败: {e}", file=sys.stderr)

    if args.emit_json:
        try:
            with open(args.emit_json, "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False, indent=2, default=str)
            print(f"📊 JSON 已写出: {args.emit_json}")
        except Exception as e:
            print(f"⚠️ 写 JSON 失败: {e}", file=sys.stderr)

    if not res.get("ok"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
