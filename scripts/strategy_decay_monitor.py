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
- 窗口口径（2026-10-25 对齐，issue #16 复核发现）：近期窗口 / regime 构成 / regime
  分层全部改用与 causal_edge 完全相同的「收益已实现」信号日（cutoff 前 + 实现 lag
  过滤）；此前直接取最新 DAL 日，导致告警头部的窗口描述、regime 分层与 edge 统计
  错位约 lag 个日历日，且分层用了未实现收益的回补值。候选池探测仍用数据尾部 DAL
  日（管线存活看当下），正文里明确标注两个窗口的区别。
- 纯本地、不联网、不重跑回测、fail-soft；输出 JSON 供 CI 开 issue。

用法：
    python scripts/strategy_decay_monitor.py [--emit-json stock_data/decay_monitor.json]
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from walk_forward_validator import (  # noqa: E402
    _all_signal_days,
    causal_edge,
    EDGE_WINDOW,
    _edge_lag_cal_days,
    _load_day_picks,
    _regime_as_of,
)

RECENT_WINDOW = 10      # 近期窗口：最近 10 个信号日
MIN_N_EDGE = 5          # 至少 5 个样本才判定（避免小样本误报）
WIN_RATE_FLOOR = 45.0   # 近期胜率低于 45% 视为塌陷


def _realizable_before(days: list[str], cutoff: str) -> list[str]:
    """causal_edge 实际会采用的信号日：严格早于 cutoff 且收益已实现（lag 过滤）。

    与 causal_edge 内部逻辑同款（含日期不可解析时 fail-open 退回宽松口径），
    供 analyze() 取「与 edge 统计完全同窗」的近期/基线窗口，杜绝窗口错位。
    """
    lag = _edge_lag_cal_days()
    past = [d for d in days if d < cutoff]
    if lag > 0:
        try:
            import pandas as pd
            cut_ts = pd.Timestamp(cutoff)
            past = [d for d in past if (cut_ts - pd.Timestamp(d)).days >= lag]
        except Exception:
            pass
    return past


def analyze(recent_window: int = RECENT_WINDOW, pool_status="auto") -> dict:
    """pool_status: "auto"=现算候选池状态；显式 dict=测试注入；None/{} = 不探测。"""
    days = _all_signal_days()
    if not days:
        return {"as_of": None, "strategies": {}, "decayed": [],
                "pool_status": {}, "error": "无可用信号日"}

    last = days[-1]
    cutoff = str(int(last) + 1)  # 含最后一天作为近期样本的一部分

    # 近期窗口 = 与 causal_edge 完全同窗的「收益已实现」信号日（2026-10-25 对齐）
    realizable = _realizable_before(days, cutoff)
    recent = realizable[-recent_window:]
    baseline_cutoff = str(int(recent[0]) + 1) if recent else cutoff
    baseline_window = min(max(0, len(realizable) - recent_window), EDGE_WINDOW)
    # 候选池状态（fail-soft）：按「数据尾部」的最新 DAL 日探测管线存活——区分
    # 「零样本=负 edge 清零后等窗口滚出」与「因子管线真停摆」（窗口与 edge 不同，正文有标注）
    raw_recent = days[-recent_window:]
    ps = _safe_pool_status(raw_recent) if pool_status == "auto" else (pool_status or {})

    recent_edge = causal_edge(cutoff, window=recent_window)
    baseline_edge = causal_edge(baseline_cutoff, window=baseline_window) if baseline_window else {}
    regime_split = _regime_split(recent)

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
        # r_win 可能为 None（_aggregate_excess 在 n=0 时返回 win_rate=None）：
        # 零样本策略没有胜率可谈，不得判衰减（2026-10-13 修 None 崩溃）
        if n >= MIN_N_EDGE and (r_avg < 0 or (r_win is not None and r_win < WIN_RATE_FLOOR)):
            decay = True
            bits = []
            if r_avg < 0:
                bits.append(f"近期均值收益 {r_avg:+.2f}% < 0")
            if r_win is not None and r_win < WIN_RATE_FLOOR:
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
            "regime_split": regime_split.get(s, {}),
        }
        if decay:
            decayed.append(s)

    zero_sample = [s for s, e in strategies.items() if e["recent_n"] == 0]
    return {
        "as_of": last,
        "pool_status": ps,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "recent_window": recent_window,
        "window_range": [recent[0], recent[-1]] if recent else [],
        "pool_window": [raw_recent[0], raw_recent[-1]] if raw_recent else [],
        "pool_window_days": len(raw_recent),
        "regime_mix": _regime_mix(recent),
        "zero_sample": zero_sample,
        "min_n_edge": MIN_N_EDGE,
        "win_rate_floor": WIN_RATE_FLOOR,
        "strategies": strategies,
        "decayed": decayed,
    }


def _regime_split(recent_days: list) -> dict:
    """近期窗口内每策略按信号日 regime 分层的 (n, avg, win)（fail-soft）。"""
    acc: dict[str, dict[str, list]] = {}
    for sd in recent_days:
        try:
            reg = _regime_as_of(sd)
        except Exception:
            reg = "未知"
        try:
            picks = _load_day_picks(sd)
        except Exception:
            continue
        for p in picks:
            for s in p.get("sources", ()):  # 来源策略集合
                acc.setdefault(s, {}).setdefault(reg, []).append(p["return_pct"])
    out: dict[str, dict] = {}
    for s, byreg in acc.items():
        out[s] = {}
        for reg, rets in sorted(byreg.items()):
            n = len(rets)
            out[s][reg] = {
                "n": n,
                "avg_return": round(sum(rets) / n, 3) if n else 0.0,
                "win_rate": (round(sum(1 for r in rets if r > 0) / n * 100, 1)
                             if n else None),
            }
    return out


def _regime_mix(recent_days: list) -> dict:
    """近期窗口的 regime 天数构成（ reviewer 一眼看出窗口是否被防御市主导）。"""
    mix: dict[str, int] = {}
    for sd in recent_days:
        try:
            reg = _regime_as_of(sd)
        except Exception:
            reg = "未知"
        mix[reg] = mix.get(reg, 0) + 1
    return mix


def _pool_status(recent_days: list) -> dict:
    """近期窗口内各因子的「候选池」产票情况（Stock-Selection-<Label>-<sd>.csv）。

    衰减监控的 edge 口径只读 Daily-Action-List 命中：被清零（负 edge → 权重 0）的
    因子同样显示「零样本」，但它的候选池可能一直在正常产票——两者必须区分，
    否则「清零等窗口滚出」会被误读成「产出停摆」（2026-10-15 illiq20 实录）。

    恢复机制（adaptive_weights）：清零因子的旧样本滚出 edge 窗口（约 20 个信号日）
    后 n→0，落入无证据门 → 拿探索池权重（floor 3%，总额 ≤30%）重新进 DAL。

    返回 {策略id: {days_total, days_with_csv, median_picks, latest_date, status}}：
    - ok      ：候选池正常产票 → DAL 缺席源于清零/轮换，非故障
    - sparse  ：产票天数不足一半 → 因子管线可能部分失效
    - stalled ：窗口内全程无票 → 因子管线真停摆，需排查
    - unknown ：读取异常（fail-soft）
    """
    import pandas as pd

    from smcore.config.defaults import STOCK_DATA_DIR
    from smcore.strategy.factor_types import STRATEGY_LABEL, STRATEGY_ORDER

    out: dict[str, dict] = {}
    for sid in STRATEGY_ORDER:
        label = STRATEGY_LABEL[sid]
        counts: list[int] = []
        latest = None
        read_err = False
        for sd in recent_days:
            p = STOCK_DATA_DIR / f"Stock-Selection-{label}-{sd}.csv"
            try:
                if not p.exists():
                    counts.append(0)
                    continue
                n = len(pd.read_csv(p, encoding="utf-8-sig"))
                counts.append(n)
                if n > 0:
                    latest = sd
            except Exception:
                read_err = True
                counts.append(0)
        days_with = sum(1 for c in counts if c > 0)
        med = sorted(counts)[len(counts) // 2] if counts else 0
        if read_err and days_with == 0:
            status = "unknown"
        elif days_with == 0:
            status = "stalled"
        elif days_with < max(1, len(recent_days) // 2):
            status = "sparse"
        else:
            status = "ok"
        out[sid] = {
            "days_total": len(recent_days),
            "days_with_csv": days_with,
            "median_picks": med,
            "latest_date": latest,
            "status": status,
        }
    return out


def _safe_pool_status(recent_days: list) -> dict:
    """候选池状态探测的 fail-soft 包装：任何异常返回 {}，绝不阻塞衰减监控。"""
    try:
        return _pool_status(recent_days)
    except Exception:
        return {}


def _format_issue_body(res: dict) -> str:
    lines = [
        "## 策略衰减告警（自动生成）",
        "",
        f"- 数据截至信号日：**{res['as_of']}**",
        f"- edge 窗口：最近 **{res['recent_window']}** 个「收益已实现」信号日（与下方全部统计同窗；"
        "因信号日收益需 +hold+1 个交易日才实现，窗口尾部天然滞后数据尾部约 3 周）",
        f"- 每策略至少 {res['min_n_edge']} 个样本才判定；胜率塌陷阈值：< {res['win_rate_floor']:.0f}%",
        "",
        "### 触发衰减的策略",
        "",
    ]
    if not res["decayed"]:
        lines.append("_（无）_")
    def _rw(v):
        return f"{v:.1f}" if v is not None else "n/a"

    for s in res["decayed"]:
        e = res["strategies"][s]
        lines.append(
            f"- **{s}**：近期均值 {e['recent_avg_return']:+.2f}%，胜率 {_rw(e['recent_win_rate'])}%，"
            f"样本 {e['recent_n']} —— {e['reason']}"
        )
    rng = res.get("window_range") or []
    mix = res.get("regime_mix") or {}
    mix_s = " / ".join(f"{k} {v} 天" for k, v in sorted(mix.items(), key=lambda kv: -kv[1]))
    lines += [
        "",
        f"edge 窗口覆盖 **{rng[0] if rng else '?'} ~ {rng[-1] if rng else '?'}**，"
        f"regime 构成：{mix_s or '未知'}。",
        "",
        "### 衰减策略的 regime 分层（近期窗口内按信号日 regime 分组）",
        "",
        "解读：若某策略只在「下行防御」段为负、其他 regime 正常 → 防御市系统性现象，",
        "随 regime 切换自愈；若全 regime 为负 → 策略本身失效，才需要考虑降权/停用。",
        "",
    ]
    regs = sorted({r for e in res["strategies"].values() for r in e.get("regime_split", {})})
    if regs:
        head = "| 策略 | 全窗口 | " + " | ".join(regs) + " |"
        lines += [head, "|" + "---|" * (2 + len(regs))]
        for s in res["decayed"]:
            e = res["strategies"][s]
            rw = e["recent_win_rate"]
            full = f"n={e['recent_n']} {e['recent_avg_return']:+.2f}% ({rw:.0f}%)" \
                if rw is not None else f"n={e['recent_n']} {e['recent_avg_return']:+.2f}% (n/a)"
            cells = []
            for r in regs:
                cell = e.get("regime_split", {}).get(r)
                if not cell or not cell.get("n"):
                    cells.append("—")
                else:
                    w = cell["win_rate"]
                    cells.append(f"n={cell['n']} {cell['avg_return']:+.2f}%"
                                 + (f" ({w:.0f}%)" if w is not None else ""))
            lines.append(f"| {s} | {full} | " + " | ".join(cells) + " |")
        lines.append("")
    zs = res.get("zero_sample") or []
    if zs:
        ps = res.get("pool_status") or {}
        lines += ["### 零样本策略归因（edge 窗口 DAL 无命中）", ""]
        pw = res.get("pool_window") or []
        pw_s = f"{pw[0]} ~ {pw[-1]}" if len(pw) == 2 else "?"
        lines.append("edge 口径只读 DAL 命中；被清零（负 edge → 权重 0）的因子同样显示零样本。"
                     "下表用候选池（Stock-Selection CSV）产票情况区分「真停摆」与「清零等窗口」——"
                     f"探测按数据尾部最近 {res.get('pool_window_days', '?')} 个 DAL 日（{pw_s}），"
                     "与 edge 窗口不同：前者看管线当下是否产票，后者看滞后窗口内的命中收益。")
        lines.append("")
        lines.append("| 策略 | 候选池状态 | 窗口内有票天数 | 中位票数 | 最近产票日 | 判读 |")
        lines.append("|---|---|---|---|---|---|")
        for s in zs:
            p = ps.get(s) or {}
            st = p.get("status", "unknown")
            note = {
                "ok": "候选池正常 → DAL 缺席源于负 edge 清零，旧样本滚出 edge 窗口（约 20 个信号日）后经探索池自动恢复，非故障",
                "sparse": "候选池产票稀疏 → 关注因子管线是否部分失效",
                "stalled": "候选池也停摆 → 因子管线故障，需排查 Stock-Selection 生成链路",
                "unknown": "候选池状态读取失败 → 无法区分，人工排查",
            }.get(st, "—")
            lines.append(
                f"| {s} | {st} | {p.get('days_with_csv', '?')}/{p.get('days_total', '?')} "
                f"| {p.get('median_picks', '?')} | {p.get('latest_date') or '—'} | {note} |"
            )
        lines.append("")

    lines += ["", "### 全部策略近期概况", ""]
    lines.append("| 策略 | 近期样本 | 近期均值% | 近期胜率% | 基线均值% | 基线胜率% | 衰减 |")
    lines.append("|---|---|---|---|---|---|---|")
    for s, e in sorted(res["strategies"].items()):
        b_avg = f"{e['baseline_avg_return']:+.2f}" if e["baseline_avg_return"] is not None else "n/a"
        b_win = f"{e['baseline_win_rate']:.1f}" if e["baseline_win_rate"] is not None else "n/a"
        lines.append(
            f"| {s} | {e['recent_n']} | {e['recent_avg_return']:+.2f} | {_rw(e['recent_win_rate'])} "
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
        rng = res.get("window_range") or []
        mix = res.get("regime_mix") or {}
        print(f"截至信号日：{res['as_of']}（edge窗口 {rng[0] if rng else '?'}~{rng[-1] if rng else '?'}，"
              f"regime: {mix}）  衰减策略：{res['decayed'] or '无'}")
        if res.get("zero_sample"):
            ps = res.get("pool_status") or {}
            parts = [f"{s}(池:{(ps.get(s) or {}).get('status', 'unknown')})"
                     for s in res["zero_sample"]]
            print(f"  零样本策略（池状态）: {', '.join(parts)}")
        for s, e in sorted(res["strategies"].items()):
            flag = " ⚠️衰减" if e["decay"] else ""
            rw = e["recent_win_rate"]
            rw_s = f"{rw:.1f}%" if rw is not None else "n/a(零样本)"
            print(f"  {s:>14}  近期n={e['recent_n']:>3}  均值={e['recent_avg_return']:+.2f}%  "
                  f"胜率={rw_s}  {e['reason']}{flag}")
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
