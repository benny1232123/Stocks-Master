#!/usr/bin/env python3
"""临时工具（20260905 分批止盈扫描）：写入 risk_config.json 的 exit.partial_take_profit。"""
import argparse
import json
from pathlib import Path

P = Path(__file__).resolve().parent.parent / "smcore" / "strategy" / "risk_config.json"

ap = argparse.ArgumentParser()
ap.add_argument("--trigger", type=float, required=True, help="分批止盈触发阈值，如 0.04")
ap.add_argument("--tranche", type=float, required=True, help="首批卖出比例，如 0.33")
a = ap.parse_args()

cfg = json.loads(P.read_text(encoding="utf-8"))
cfg["exit"]["partial_take_profit"]["trigger_pct"] = a.trigger
cfg["exit"]["partial_take_profit"]["tranche_pct"] = a.tranche
P.write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(f"[set_ptp] trigger={a.trigger} tranche={a.tranche}")
