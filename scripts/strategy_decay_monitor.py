#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""策略衰减监控：检测「最近 N 个信号日」里哪些策略的 edge 持续为负/胜率塌陷。

设计要点：
- 复用 walk_forward_validator.causal_edge()：严格因果，只用早于 cutoff 的历史
  信号日算各策略 edge，与生产运行时一致，杜绝未来函数。
- 最近 RECENT_WINDOW 个信号日作为「近期样本」；另取更早的全部历史作为「基线样本」
  用于对照（近期明显差于自身历史 → 衰减信号更可信）。
- 判据（脚本顶部可调）：
    * 近期样本数 >= MIN_N_EDGE；
    * 且（近期均值收益 < 0 或 近期胜率 < WIN_RATE_FLOOR）→ 标记 decay。
- 纯本地、不联网、不重跑回测、fail-soft；输出 JSON 供 CI 开 issue。

用法：
    python scripts/strategy_decay_monitor.py [--emit-json stock_data/decay.json]
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from walk_forward_validator import _all_signal_days, causal_edge, EDGE_WINDOW  # noqa: E402

RECENT_WINDOW = 10      # 近期窗口：最近 10 个信号日
MIN_N_EDGE = 5          # 至少 5 个样本才判定（避免小样本误报）
WIN_RATE_FLOOR = 45.0   # 近期胜率低于 45% 视为塌陷


def _baseline_window(days: list[str], recent: int) -> int:
    """基线窗口 = 近期之前还能取多少天（最多 EDGE_WINDOW）。"""
    prior = len(days) - recent
    return max(0, min(prior, EDGE_WINDOW))


def analyze(recent_window: int = RECENT_WINDOW) -> dict:
    days = _all_signal_days()
    if not days:
        return {"as_of": None, "strategies": {}, "decayed": [], "error": "无可用信号日"}

    last = days[-1]
    cutoff = str(int(last) + 1)  # 含最后一天作为近期样本的一部分

    recent = days[-recent_window:]
    baseline_cutoff = str(int(recent[0]) + 1) if recent else cutoff
    baseline_window = _baseline_window(days, recent_window)

    recent_edge = causal_edge(cutoff, window=recent_window)
    baseline_edge = causal_edge(baseline_cutoff, window=baseline_window) if baseline_window else {}

    strategies: dict[str, dict] = {}
    decayed: list[str] = []
    for s, re in recent_edge.items():
        be = baseline_edge.get(s, {})
        n = re.get("n", 0)
        r_avg = re.get("avg_return", 0.0)
        r_win = re.get("win_rate", 0.0)
        b_avg = be.get("avg_return")
        b_win = be.get("win_rate")
        decay = False
        reason = ""
        if n >= MIN_N_EDGE and (r_avg < 0 or r_win < WIN_RATE_FLOOR):
            decay = True
            bits = []
            if r_avg < 0:
                bits.append(f"近期均值收益 {r_avg:+.2f}% < 0")
            if r_win < WIN_RATE_FLOOR:
                bits.append(f"近期胜率 {r_win:.1f}% < {WIN_RATE_FLOOR:.0f}%")
            reason = "；".join(bits)
        strategies[s] = {
            "recent_n": n,
            "recent_avg_return": r_avg,
            "recent_win_rate": r_win,
            "baseline_avg_return": b_avg,
            "baseline_win_rate": b_win,
            "decay": decay,
            "reason": reason,
        }
        if decay:
            decayed.append(s)

    return {
        "as_of": last,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "recent_window": recent_window,
        "min_n_edge": MIN_N_EDGE,
        "win_rate_floor": WIN_RATE_FLOOR,
        "strategies": strategies,
        "decayed": decayed,
    }


def _format_issue_body(res: dict) -> str:
    lines = [
        "## 策略衰减告警（自动生成）",
        "",
        f"- 截至信号日：**{res['as_of']}**",
        f"- 近期窗口：最近 **{res['recent_window']}** 个信号日（每策略至少 {res['min_n_edge']} 个样本才判定）",
        f"- 胜率塌陷阈值：< {res['win_rate_floor']:.0f}%",
        "",
        "### 触发衰减的策略",
        "",
    ]
    if not res["decayed"]:
        lines.append("_（无）_")
    for s in res["decayed"]:
        e = res["strategies"][s]
        lines.append(
            f"- **{s}**：近期均值 {e['recent_avg_return']:+.2f}%，胜率 {e['recent_win_rate']:.1f}%，"
            f"样本 {e['recent_n']} —— {e['reason']}"
        )
    lines += ["", "### 全部策略近期概况", ""]
    lines.append("| 策略 | 近期样本 | 近期均值% | 近期胜率% | 基线均值% | 基线胜率% | 衰减 |")
    lines.append("|---|---|---|---|---|---|---|")
    for s, e in sorted(res["strategies"].items()):
        b_avg = f"{e['baseline_avg_return']:+.2f}" if e["baseline_avg_return"] is not None else "n/a"
        b_win = f"{e['baseline_win_rate']:.1f}" if e["baseline_win_rate"] is not None else "n/a"
        lines.append(
            f"| {s} | {e['recent_n']} | {e['recent_avg_return']:+.2f} | {e['recent_win_rate']:.1f} "
            f"| {b_avg} | {b_win} | {'⚠️' if e['decay'] else ''} |"
        )
    lines += [
        "",
        "> 说明：本告警由 `.github/workflows/monitor.yml` 每周自动运行 "
        "`scripts/strategy_decay_monitor.py` 生成，基于历史信号日因果 edge，不含未来函数。",
        "> 触发仅代表「近期该策略选股能力偏弱」，请结合市场环境与历史基线人工复核，"
        "再决定是否临时降权或停用。",
    ]
    return "\n".join(lines)


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--emit-json", default=None, help="把分析结果写到该 JSON 路径")
    ap.add_argument("--recent-window", type=int, default=RECENT_WINDOW)
    args = ap.parse_args()

    res = analyze(recent_window=args.recent_window)
    print("=" * 64)
    print("策略衰减监控（基于最近 %d 个信号日因果 edge）" % args.recent_window)
    print("=" * 64)
    if res.get("error"):
        print("错误：", res["error"])
    else:
        print(f"截至信号日：{res['as_of']}  衰减策略：{res['decayed'] or '无'}")
        for s, e in sorted(res["strategies"].items()):
            flag = " ⚠️衰减" if e["decay"] else ""
            print(f"  {s:>14}  近期n={e['recent_n']:>3}  均值={e['recent_avg_return']:+.2f}%  "
                  f"胜率={e['recent_win_rate']:.1f}%  {e['reason']}{flag}")
        print("-" * 64)
        print(_format_issue_body(res))

    if args.emit_json:
        try:
            with open(args.emit_json, "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False, indent=2, default=str)
            print(f"\n结果已写：{args.emit_json}")
        except Exception as e:  # pragma: no cover
            print(f"写 JSON 失败：{e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
