#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""验证三项收益/真实性改进对组合收益率的真实贡献（walk-forward 风格，严格因果）。

方法：
- 锁定历史某天的信号清单（Daily-Action-List-{sd}.csv），用真实行情从信号日后持有，
  对比仅切换三个开关的 5 组组合表现。
- 5 组仅在 vol_target / partial_take_profit / market_friction(limit_down) 三个开关不同，
  其余（enable_exits=True、自适应出场参数、持有期、初始资金）完全一致 —— 差异纯归因三项改进。
- 离线安全：仅用「足够历史」的信号日（end_pad < 今天），行情全部走本地 k_data 缓存。

指标（跨信号日取均值，walk-forward 口径）：
- avg_total_return : 平均单信号日组合收益率(%)
- avg_max_dd       : 平均单信号日组合最大回撤(%)
- avg_win_rate     : 平均胜率(%)
- avg_sharpe       : 平均夏普
- total_trades     : 累计成交笔数（partial 会增加分批笔数）

用法：
  python scripts/verify_new_exits.py                # 全量历史信号日
  VE_MAX_DAYS=12 python scripts/verify_new_exits.py # 仅最近 12 个信号日（快验）
  VE_HOLD_DAYS=10 python scripts/verify_new_exits.py
"""
from __future__ import annotations

import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.backtest.engine import run_forward_signal_backtest  # noqa: E402
from smcore.strategy.adaptive_weights import STOCK_DATA_DIR  # noqa: E402
from smcore.utils.code import format_stock_code  # noqa: E402

HOLD_DAYS = int(os.environ.get("VE_HOLD_DAYS", "10"))
INIT_CAP = 1_000_000.0
MAX_SIGNAL_DAYS = int(os.environ.get("VE_MAX_DAYS", "999"))

# (vol_target, partial_take_profit, model_limit_down)
GROUPS = {
    "base": (False, False, False),
    "vol": (True, False, False),
    "partial": (False, True, False),
    "ld": (False, False, True),
    "all": (True, True, True),
}


def _cached_exists(code: str) -> bool:
    cache = STOCK_DATA_DIR / "k_data" / f"{format_stock_code(code)}_qfq_full.csv"
    if not cache.exists():
        return False
    try:
        df = pd.read_csv(cache)
    except Exception:
        return False
    return not df.empty


def _read_dal(dal_path: Path) -> list[tuple[str, str]]:
    try:
        d = pd.read_csv(dal_path, encoding="utf-8-sig")
    except Exception:
        return []
    if "股票代码" not in d.columns:
        return []
    out = []
    for _, r in d.iterrows():
        c = format_stock_code(r.get("股票代码"))
        if not c:
            continue
        s = r.get("来源策略")
        s = "" if (s is None or pd.isna(s)) else str(s).strip()
        out.append((c, s))
    return out


def _run_group(sig: pd.DataFrame, vol: bool, par: bool, ld: bool) -> dict:
    res = run_forward_signal_backtest(
        sig.copy(),
        hold_days=HOLD_DAYS,
        initial_capital=INIT_CAP,
        enable_exits=True,
        vol_target=vol,
        partial_take_profit=par,
        model_limit_down=ld,
    )
    return res.summary


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return round(sum(xs) / len(xs), 3) if xs else None


def main() -> int:
    today = date.today()
    # end_pad = sd + (HOLD_DAYS+15)；必须落在历史（<today）才能纯离线返回本地缓存切片
    cutoff = today - timedelta(days=HOLD_DAYS + 20)

    dals = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))
    sds: list[tuple[date, Path]] = []
    for dal in dals:
        m = re.search(r"(\d{8})", dal.name)
        if not m:
            continue
        y, mo, da = m.group(1)[:4], m.group(1)[4:6], m.group(1)[6:8]
        sd = date(int(y), int(mo), int(da))
        if sd > cutoff:
            continue
        sds.append((sd, dal))
    sds.sort()
    if MAX_SIGNAL_DAYS < len(sds):
        sds = sds[-MAX_SIGNAL_DAYS:]
    print(f"[verify] 离线安全信号日(<= {cutoff}): {len(sds)} 个", file=sys.stderr)

    agg = {g: {"ret": [], "dd": [], "win": [], "sharpe": [], "nt": 0} for g in GROUPS}
    per_day: list[dict] = []

    for sd, dal in sds:
        pairs = [(c, s) for c, s in _read_dal(dal) if _cached_exists(c)]
        if len(pairs) < 5:
            continue
        sig = pd.DataFrame(
            [{"日期": sd, "代码": c, "来源策略": s} for c, s in pairs]
        )
        day_row = {"sd": sd.strftime("%Y%m%d"), "n": len(pairs)}
        for g, (vol, par, ld) in GROUPS.items():
            s = _run_group(sig, vol, par, ld)
            if "error" in s:
                day_row[g] = None
                continue
            agg[g]["ret"].append(s["total_return"])
            agg[g]["dd"].append(s["max_drawdown"])
            agg[g]["win"].append(s["win_rate"])
            agg[g]["sharpe"].append(s["sharpe"])
            agg[g]["nt"] += int(s["num_trades"])
            day_row[g] = s["total_return"]
        per_day.append(day_row)
        print(f"[verify] done {sd} n={len(pairs)} "
              f"base={day_row.get('base')} all={day_row.get('all')}", file=sys.stderr)

    summary = {}
    for g in GROUPS:
        a = agg[g]
        summary[g] = {
            "n_days": len(a["ret"]),
            "avg_total_return": _mean(a["ret"]),
            "avg_max_dd": _mean(a["dd"]),
            "avg_win_rate": _mean(a["win"]),
            "avg_sharpe": _mean(a["sharpe"]),
            "total_trades": a["nt"],
        }

    # ---- 输出 markdown ----
    lines = []
    lines.append(f"# 三项改进收益贡献验证（walk-forward，hold_days={HOLD_DAYS}）\n")
    lines.append(f"- 信号日样本：{len(per_day)} 个（均离线安全，纯本地行情）")
    lines.append(f"- 对照组：均 enable_exits=True、自适应出场(8/6/5/60)、初始资金 {INIT_CAP:,.0f}")
    lines.append(f"- 仅切换三个开关：vol_target / partial_take_profit / market_friction(limit_down)\n")

    hdr = "| 组别 | vol | partial | limit_down | 样本日 | 平均组合收益% | 平均最大回撤% | 平均胜率% | 平均夏普 | 累计成交 |"
    sep = "|------|-----|---------|-----------|--------|-------------|-------------|----------|---------|----------|"
    lines.append(hdr)
    lines.append(sep)
    for g, (vol, par, ld) in GROUPS.items():
        s = summary[g]
        lines.append(
            f"| {g} | {'✓' if vol else '·'} | {'✓' if par else '·'} | {'✓' if ld else '·'} | "
            f"{s['n_days']} | {s['avg_total_return']} | {s['avg_max_dd']} | "
            f"{s['avg_win_rate']} | {s['avg_sharpe']} | {s['total_trades']} |"
        )

    base_ret = summary["base"]["avg_total_return"]
    lines.append("\n## 相对 base 的增量（pp = 百分点）\n")
    lines.append("| 组别 | Δ收益(pp) | Δ回撤(pp) | Δ胜率(pp) |")
    lines.append("|------|----------|-----------|-----------|")
    for g in ("vol", "partial", "ld", "all"):
        s = summary[g]
        d_ret = (s["avg_total_return"] - base_ret) if (s["avg_total_return"] is not None and base_ret is not None) else None
        d_dd = (s["avg_max_dd"] - summary["base"]["avg_max_dd"]) if (s["avg_max_dd"] is not None and summary["base"]["avg_max_dd"] is not None) else None
        d_win = (s["avg_win_rate"] - summary["base"]["avg_win_rate"]) if (s["avg_win_rate"] is not None and summary["base"]["avg_win_rate"] is not None) else None
        lines.append(
            f"| {g} | {round(d_ret, 3) if d_ret is not None else '—'} | "
            f"{round(d_dd, 3) if d_dd is not None else '—'} | "
            f"{round(d_win, 3) if d_win is not None else '—'} |"
        )

    lines.append("\n## 逐信号日组合收益(%)\n")
    pday_hdr = "| 信号日 | 票数 | base | vol | partial | ld | all |"
    pday_sep = "|--------|------|------|-----|---------|----|-----|"
    lines.append(pday_hdr)
    lines.append(pday_sep)
    for r in per_day:
        lines.append(
            f"| {r['sd']} | {r['n']} | {r.get('base')} | {r.get('vol')} | "
            f"{r.get('partial')} | {r.get('ld')} | {r.get('all')} |"
        )

    lines.append("\n## 结论\n")
    all_ret = summary["all"]["avg_total_return"]
    if all_ret is not None and base_ret is not None:
        delta = round(all_ret - base_ret, 3)
        verdict = "提升" if delta > 0 else "未提升"
        lines.append(
            f"- 三项全开(all) 相对基线(base) 平均组合收益 {verdict} **{delta:+} pp**"
            f"（{base_ret}% → {all_ret}%）。"
        )
    lines.append(
        "- vol_target：内部向低波动倾斜、总暴露不变，看是否提升收益/回撤比。"
    )
    lines.append(
        "- partial_take_profit：盈利后分批止盈+收紧跟踪，看是否改善收益同时增加成交笔数。"
    )
    lines.append(
        "- market_friction(limit_down)：封跌停日卖单顺延，去除回测虚高水分（真实收益应≤含虚高版本）。"
    )

    md = "\n".join(lines) + "\n"
    out_path = STOCK_DATA_DIR / "verify_new_exits.md"
    out_path.write_text(md, encoding="utf-8")
    print(md)
    print(f"\n[verify] 报告已写入 {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
