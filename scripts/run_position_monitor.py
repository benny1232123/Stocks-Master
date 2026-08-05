#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""纸盘重放对比：带止损(exit-aware) vs 裸持有(naive)。

验证 P0 命题——把出场/风控引擎接入组合闭环后，回撤是否被有效控制。

用法：
    python scripts/run_position_monitor.py [--invest-frac 1.0] [--emit-json stock_data/pm_replay.json]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy.position_monitor import run_paper_with_exits  # noqa: E402


def _fmt(res: dict) -> str:
    if res.get("error"):
        return f"# 纸盘重放\n\n错误：{res['error']}\n"
    ea = res["exit_aware"]
    nv = res["naive"]
    lines = [
        "# 纸盘重放对比（带止损 vs 裸持有）",
        "",
        f"- 信号日区间：**{res['first_day']} ~ {res['last_day']}**（共 {res['n_signal_days']} 个）",
        f"- 可投比例 invest_frac：**{res['invest_frac']:.2f}**",
        "",
        "| 口径 | 累计收益% | 最大回撤% | 平均每份名单只数 |",
        "|---|---|---|---|",
        f"| **带止损(exit-aware)** | {ea['total_return_pct']:+.2f} | **{ea['max_drawdown_pct']:+.2f}** | {ea['avg_names_per_list']} |",
        f"| 裸持有(naive) | {nv['total_return_pct']:+.2f} | {nv['max_drawdown_pct']:+.2f} | {nv['avg_names_per_list']} |",
        "",
    ]
    dd_delta = ea["max_drawdown_pct"] - nv["max_drawdown_pct"]
    ret_delta = ea["total_return_pct"] - nv["total_return_pct"]
    lines.append(f"> 回撤改善：**{dd_delta:+.2f}pp**（带止损更小=优）；收益差异：**{ret_delta:+.2f}pp**")
    lines.append("")
    lines.append("> 说明：两者持有窗口均为「信号日→下一信号日」，唯一区别是带止损口径在窗口内")
    lines.append("> 应用了硬止损/移动止盈/MA60趋势破位/持有期满；裸持有口径中途无任何止损。")
    lines.append("> 出场参数与 `daily_backtest.py` 验证过的配置一致（止损8%/止盈6%/trailing5%/MA60）。")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--invest-frac", type=float, default=1.0,
                    help="可投比例(其余为现金)，默认1.0=纯选股；生产约0.3")
    ap.add_argument("--emit-json", default=None)
    args = ap.parse_args()

    res = run_paper_with_exits(invest_frac=args.invest_frac)
    print(_fmt(res))
    if args.emit_json:
        try:
            with open(args.emit_json, "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False, indent=2, default=str)
            print(f"\nJSON 已写：{args.emit_json}")
        except Exception as e:  # pragma: no cover
            print(f"写 JSON 失败：{e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
