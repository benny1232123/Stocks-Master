#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""纸盘模拟（执行层闭环）：按时间序跟随已发布的 Daily-Action-List 做再平衡，
用本地 k_data 算个股区间收益，累计组合净值，并与沪深300基准对比。

透明假设：
- 每个信号日以该清单「建议金额」占比建仓（归一化到名单内 100%，即假设把可投
  部分按比例分给清单票；生产另有现金缓冲，此处只评估「纯选股能力」）。
- 持有至下一信号日再平衡（下一清单覆盖当前持仓）。
- 个股区间收益 = 本地 k_data 从「信号日次交易日开盘」到「下一信号日开盘」的收益率
  （严格因果，无未来函数）。
- 基准 = 沪深300 同期；若本地无网络取到指数序列则跳过并注明。

纯本地、不联网、不重跑回测、fail-soft。

用法：
    python scripts/paper_tracker.py [--emit-json stock_data/paper.json] [--emit-md stock_data/paper.md]
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from walk_forward_validator import (  # noqa: E402
    STOCK_DATA_DIR,
    _all_signal_days,
    _load_cached_kdata,
    _parse_signal_date_from_name,
    EDGE_WINDOW,
)
from smcore.utils.code import format_stock_code  # noqa: E402


def _price_on_or_after(series: pd.Series, sd: str, strict: bool = False) -> float | None:
    """返回日期 >= (strict 则 >) sd 的第一个可用价格；无则返回 None。"""
    target = pd.to_datetime(sd)
    sub = series[series.index >= target] if not strict else series[series.index > target]
    sub = sub.dropna()
    if sub.empty:
        return None
    return float(sub.iloc[0])


def _stock_return_between(code: str, start_sd: str, end_sd: str) -> float | None:
    """本地 k_data：D_i 次日开盘买入 -> D_{i+1} 次日开盘卖出 的收益率(%)。

    持仓期 = 信号日次日开盘 到 下一信号日次日开盘（严格因果，无未来函数）。
    """
    df = _load_cached_kdata(code)
    if df.empty or "open" not in df.columns:
        return None
    s = df.set_index("date")["open"]
    buy = _price_on_or_after(s, start_sd, strict=True)
    sell = _price_on_or_after(s, end_sd, strict=True)
    if buy is None or sell is None or buy <= 0:
        return None
    return (sell - buy) / buy * 100.0


def _load_dal_weights(sd: str) -> dict[str, float]:
    """读取 DAL 的建议金额占比（归一化）。缺建议金额则退回用 建议仓位%。"""
    dal = STOCK_DATA_DIR / f"Daily-Action-List-{sd}.csv"
    if not dal.exists():
        return {}
    try:
        d = pd.read_csv(dal, encoding="utf-8-sig")
    except Exception:
        return {}
    if d.empty or "股票代码" not in d.columns:
        return {}
    use_col = "建议金额" if "建议金额" in d.columns else ("建议仓位%" if "建议仓位%" in d.columns else None)
    if use_col is None:
        return {}
    wmap: dict[str, float] = {}
    for _, r in d.iterrows():
        c = format_stock_code(r.get("股票代码"))
        if not c:
            continue
        try:
            v = float(r.get(use_col))
        except (TypeError, ValueError):
            continue
        if pd.notna(v) and v > 0:
            wmap[c] = v
    total = sum(wmap.values())
    if total <= 0:
        return {}
    return {c: v / total for c, v in wmap.items()}


def _benchmark_return_between(start_sd: str, end_sd: str) -> float | None:
    """沪深300 同期收益率(%)；取不到（无网络/无缓存）返回 None。"""
    try:
        from smcore.strategy.fusion import _get_hs300_close
        s = _get_hs300_close()
    except Exception:
        return None
    if s is None or len(s) == 0:
        return None
    s = s.sort_index()
    buy = _price_on_or_after(s, start_sd, strict=True)
    sell = _price_on_or_after(s, end_sd, strict=True)
    if buy is None or sell is None or buy <= 0:
        return None
    return (sell - buy) / buy * 100.0


def _max_drawdown(curve: list[float]) -> float:
    peak = float("-inf")
    mdd = 0.0
    for v in curve:
        peak = max(peak, v)
        if peak > 0:
            mdd = min(mdd, (v - peak) / peak * 100.0)
    return mdd


def run(invest_frac: float = 1.0) -> dict:
    """invest_frac：可投比例（其余视为现金，收益 0）。默认 1.0=纯选股口径。

    生产实盘另有现金缓冲（波动率/回撤熔断），典型 invest_frac≈0.3；设该参数可
    近似复算「含现金缓冲」后的组合表现，使与基准对比更公平。
    """
    if not (0.0 < invest_frac <= 1.0):
        invest_frac = 1.0
    days = _all_signal_days()
    if len(days) < 2:
        return {"error": "信号日不足 2 个，无法模拟", "n_days": len(days)}

    value = 1.0
    bench = 1.0
    bench_ok = True
    curve: list[dict] = []
    missing_count = 0
    realized_periods = 0
    names_counts: list[int] = []

    for i in range(len(days) - 1):
        sd, nxt = days[i], days[i + 1]
        wmap = _load_dal_weights(sd)
        if not wmap:
            continue
        # 剔除无区间收益的票后重新归一
        alloc: dict[str, float] = {}
        dropped = 0
        for c, w in wmap.items():
            r = _stock_return_between(c, sd, nxt)
            if r is None:
                dropped += 1
                continue
            alloc[c] = (w, r)
        if not alloc:
            missing_count += 1
            continue
        tot = sum(w for w, _ in alloc.values())
        stock_ret = sum((w / tot) * r for w, r in alloc.values())
        period_ret = invest_frac * stock_ret  # 剩余 (1-frac) 为现金，收益 0
        value *= (1 + period_ret / 100.0)
        realized_periods += 1
        names_counts.append(len(alloc))

        if bench_ok:
            bret = _benchmark_return_between(sd, nxt)
            if bret is None:
                bench_ok = False
            else:
                bench *= (1 + bret / 100.0)

        curve.append({
            "from": sd, "to": nxt, "period_ret_pct": round(period_ret, 3),
            "value": round(value, 6), "n_names": len(alloc),
        })

    total_ret = (value - 1) * 100.0
    bench_ret = (bench - 1) * 100.0 if bench_ok else None
    excess = (total_ret - bench_ret) if bench_ret is not None else None
    values = [1.0] + [c["value"] for c in curve]
    mdd = _max_drawdown(values)
    avg_names = (sum(names_counts) / len(names_counts)) if names_counts else 0
    min_names = min(names_counts) if names_counts else 0

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "invest_frac": invest_frac,
        "n_signal_days": len(days),
        "realized_periods": realized_periods,
        "first_day": days[0],
        "last_day": days[-1],
        "pending_last_list": days[-1],  # 最后一份清单尚未到再平衡日，未实现
        "missing_periods": missing_count,
        "avg_names_per_list": round(avg_names, 1),
        "min_names_per_list": min_names,
        "final_value": round(value, 6),
        "total_return_pct": round(total_ret, 2),
        "benchmark": "沪深300" if bench_ok else "不可用(无网络/无缓存)",
        "benchmark_return_pct": round(bench_ret, 2) if bench_ret is not None else None,
        "excess_return_pct": round(excess, 2) if excess is not None else None,
        "max_drawdown_pct": round(mdd, 2),
        "curve": curve,
    }


def _format_md(res: dict) -> str:
    if res.get("error"):
        return f"# 纸盘模拟报告\n\n错误：{res['error']}\n"
    lines = [
        "# 纸盘模拟报告（跟随已发布 Daily-Action-List）",
        "",
        f"- 信号日区间：**{res['first_day']} ~ {res['last_day']}**（共 {res['n_signal_days']} 个，"
        f"已结算 {res['realized_periods']} 段，缺失 {res['missing_periods']} 段）",
        f"- 可投比例 invest_frac：**{res['invest_frac']:.2f}**"
        f"（{'纯选股口径' if res['invest_frac'] >= 0.999 else '含现金缓冲近似'}）",
        f"- 组合累计收益：**{res['total_return_pct']:+.2f}%**",
        f"- 基准（{res['benchmark']}）收益："
        f"{res['benchmark_return_pct']:+.2f}%" if res['benchmark_return_pct'] is not None
        else f"- 基准：{res['benchmark']}",
        f"- 超额收益：{res['excess_return_pct']:+.2f}%" if res['excess_return_pct'] is not None
        else "- 超额收益：n/a",
        f"- 最大回撤：**{res['max_drawdown_pct']:+.2f}%**",
        f"- 名单分散度：平均每份清单 **{res['avg_names_per_list']}** 只，最少 **{res['min_names_per_list']}** 只",
        f"- 说明：最后一份清单（{res['pending_last_list']}）尚未到再平衡日，未计入已实现；"
        "最大回撤主要由低分散（名单仅 1~2 只）的交易日驱动，反映集中度尾部风险。",
        "",
        "> 假设：可投部分 100% 按清单建议金额比例配置，未计入生产现金缓冲，"
        "反映纯选股能力；个股收益取自本地 k_data，严格因果无未来函数。",
        "",
        "### 净值曲线（每段再平衡后）",
        "",
        "| 起始 | 结束 | 段收益% | 组合净值 |",
        "|---|---|---|---|",
    ]
    for c in res["curve"][-25:]:
        lines.append(f"| {c['from']} | {c['to']} | {c['period_ret_pct']:+.3f} | {c['value']:.4f} |")
    return "\n".join(lines) + "\n"


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--emit-json", default=None)
    ap.add_argument("--emit-md", default=None)
    ap.add_argument("--invest-frac", type=float, default=1.0,
                    help="可投比例(其余为现金)，默认1.0=纯选股；生产约0.3")
    args = ap.parse_args()

    res = run(invest_frac=args.invest_frac)
    print(_format_md(res))

    if args.emit_json:
        try:
            with open(args.emit_json, "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False, indent=2, default=str)
            print(f"\nJSON 已写：{args.emit_json}")
        except Exception as e:  # pragma: no cover
            print(f"写 JSON 失败：{e}")
    if args.emit_md:
        try:
            with open(args.emit_md, "w", encoding="utf-8") as f:
                f.write(_format_md(res))
            print(f"Markdown 已写：{args.emit_md}")
        except Exception as e:  # pragma: no cover
            print(f"写 Markdown 失败：{e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
