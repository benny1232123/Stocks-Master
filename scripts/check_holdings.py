#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""持仓纪律核对（每日收盘后跑）：核对硬条件触发状态。

配置：stock_data/holdings_watch.json
  [{"code": "002284", "cost": 13.15, "qty": 900, "floor": 9.33, "target": 9.92, "note": "..."}]
  floor  = 跌破即清仓警戒（硬底线）
  target = 反弹减仓/收复确认位（可空）

用法：python scripts/check_holdings.py   （退出码 1 = 有 ALERT）
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.config.defaults import STOCK_DATA_DIR  # noqa: E402
from smcore.strategy.boll_levels import _compute_boll_levels  # noqa: E402
from smcore.data.kline import read_kline_cache  # noqa: E402  # noqa: E402

CONFIG = STOCK_DATA_DIR / "holdings_watch.json"


def main() -> int:
    if not CONFIG.exists():
        print(f"无配置 {CONFIG}")
        return 2
    pos = json.loads(CONFIG.read_text(encoding="utf-8"))
    alerts = 0
    print(f"{'代码':8s} {'现价':>8s} {'成本':>8s} {'盈亏':>8s} {'MA60距':>7s} {'状态':>4s}  触发/条件")
    for p in pos:
        code = p["code"]
        floor, target = p.get("floor"), p.get("target")
        lv = _compute_boll_levels(code) or {}
        px = lv.get("close")
        d = read_kline_cache(code, base_dir=STOCK_DATA_DIR / "k_data")
        d["date"] = pd_to_dt(d["date"])
        close = pd_to_num(d["close"]).dropna()
        ma60 = float(close.tail(60).mean()) if len(close) >= 60 else None
        dist60 = (px / ma60 - 1) if (px and ma60) else None
        note = []
        state = "持有"
        if floor and px is not None and px <= floor:
            state, alerts = "ALERT", alerts + 1
            note.append(f"跌破硬底线 {floor}")
        if target and px is not None and px >= target:
            state = "TAKE"
            note.append(f"到达目标位 {target}")
        if not note:
            auto_floor = lv.get("lower")
            note.append(f"观望：下轨参考 {auto_floor:.2f}" if auto_floor else "观望")
        pnl = (px / p["cost"] - 1) if px else None
        print(f"{code:8s} {px:8.2f} {p['cost']:8.2f} {pnl:+8.1%} "
              f"{dist60:+7.1%} {state:>4s}  {'；'.join(note)}")
    print(f"\nALERT {alerts} 项。状态口径：收盘价 vs 条件（盘中价可能回抽，ALERT 以收盘确认为准）。")
    return 1 if alerts else 0


def pd_to_dt(s):
    import pandas as pd
    return pd.to_datetime(s)


def pd_to_num(s):
    import pandas as pd
    return pd.to_numeric(s, errors="coerce")


if __name__ == "__main__":
    raise SystemExit(main())
