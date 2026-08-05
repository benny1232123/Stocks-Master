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
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.config.defaults import STOCK_DATA_DIR  # noqa: E402
from smcore.strategy.position_monitor import (  # noqa: E402
    STATE_PATH,
    PaperPortfolio,
    run_paper_with_exits,
)
from smcore.strategy.sectors import industry_of  # 离线行业映射（读 sector_map.json 缓存，零网络）


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


def run_daily_drive(initial_capital: float = 1_000_000.0, max_single_weight: float = 0.10,
                     cash_frac: float = 0.0, sector_resolver=industry_of, state_path=STATE_PATH,
                     limit_days: int | None = None, **exit_kwargs) -> dict:
    """真实每日链路（离线重放版）：按信号日逐日驱动 PaperPortfolio.process_day。

    每个信号日 sd：用「上一信号日的 DAL」作为 pending 建仓清单在 sd 开盘建仓，
    并对现有持仓盯市出场 + 漂移再平衡 + 记录组合回撤。这正是 live cron 每天会调用的
    同一段 process_day 代码，只是离线把历史信号日顺序回放一遍，验证执行闭环无误。

    **始终从 initial_capital 全新重放**（不读取旧 state）：DAL 历史是不可变的事实源，
    每次重放都确定性重建完整纸盘组合；旧 state 仅作输出快照（落盘 state_path），
    不作为累加器——否则 CI 定时器续跑时会把全部历史再处理一遍导致重复计数。
    """
    pf = PaperPortfolio(
        initial_capital=initial_capital, max_single_weight=max_single_weight,
        cash_frac=cash_frac, sector_resolver=sector_resolver, **exit_kwargs)

    files = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))
    days = []
    for f in files:
        suf = f.name.replace("Daily-Action-List-", "").replace(".csv", "")
        if len(suf) == 8 and suf.isdigit():
            try:
                days.append((datetime.strptime(suf, "%Y%m%d").date(), suf))
            except ValueError:
                continue
    days.sort()
    if limit_days:
        days = days[:limit_days]

    pending = None  # (prev_signal_date, prev_dal_path)
    for sd_date, sd_tag in days:
        dal_path = STOCK_DATA_DIR / f"Daily-Action-List-{sd_tag}.csv"
        if pending is not None:
            prev_date, prev_dal = pending
            pf.process_day(sd_date, dal_path=prev_dal, pending_signal_date=prev_date)
        else:
            pf.process_day(sd_date)
        pending = (sd_date, dal_path)
    pf.save(state_path)

    eq = pf.equity_curve
    # drawdown_pct 以正百分比存储（0~∞），最大回撤 = 曲线上的最大值。
    max_dd = max((r.get("drawdown_pct", 0.0) for r in eq), default=0.0) if eq else 0.0
    reasons: dict[str, int] = {}
    for r in pf.realized:
        reasons[r.get("exit_reason")] = reasons.get(r.get("exit_reason"), 0) + 1
    return {
        "n_days": len(days),
        "final_total": round(pf.cash + sum(pf._market_value(h) for h in pf.positions.values()), 2),
        "max_drawdown_pct": round(max_dd, 2),
        "n_positions_open": len(pf.positions),
        "n_realized": len(pf.realized),
        "exit_reasons": reasons,
        "equity_curve": eq,
    }


def _fmt_daily(res: dict) -> str:
    lines = [
        "# 纸盘每日驱动重放（真实 process_day 闭环）",
        "",
        f"- 信号日数：**{res['n_days']}**",
        f"- 期末总资产：**{res['final_total']:,.2f}**",
        f"- 最大组合回撤：**{res['max_drawdown_pct']:+.2f}%**",
        f"- 仍持仓数：**{res['n_positions_open']}**　已平仓/再平衡笔数：**{res['n_realized']}**",
        "",
        "**平仓/再平衡原因分布**：",
    ]
    if res["exit_reasons"]:
        for k, v in sorted(res["exit_reasons"].items(), key=lambda x: -x[1]):
            lines.append(f"- {k}: {v}")
    else:
        lines.append("- （无）")
    lines.append("")
    lines.append("> 说明：本重放按信号日顺序逐日调用 PaperPortfolio.process_day（开仓→盯市出场→漂移再平衡→回撤熔断），")
    lines.append("> 与 live cron 每日调用的代码路径完全一致；差异仅在定时器与实时 DAL 获取，执行逻辑已离线验证。")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--invest-frac", type=float, default=1.0,
                    help="可投比例(其余为现金)，默认1.0=纯选股；生产约0.3")
    ap.add_argument("--emit-json", default=None)
    ap.add_argument("--daily", action="store_true",
                    help="真实每日链路重放：逐日驱动 PaperPortfolio.process_day（开仓/出场/再平衡/回撤熔断）")
    ap.add_argument("--state-path", default=str(STATE_PATH),
                    help="PaperPortfolio 状态落盘路径（默认 stock_data/position_monitor_state.json）")
    ap.add_argument("--limit-days", type=int, default=None,
                    help="仅重放前 N 个信号日（调试用）")
    args = ap.parse_args()

    if args.daily:
        res = run_daily_drive(state_path=args.state_path, limit_days=args.limit_days)
        print(_fmt_daily(res))
        if args.emit_json:
            try:
                with open(args.emit_json, "w", encoding="utf-8") as f:
                    json.dump(res, f, ensure_ascii=False, indent=2, default=str)
                print(f"\nJSON 已写：{args.emit_json}")
            except Exception as e:  # pragma: no cover
                print(f"写 JSON 失败：{e}")
        return 0

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
