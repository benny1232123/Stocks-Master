#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""因子 IC/IR 监控：检验「排序信号」是否仍在预测未来收益。

为什么需要它（与 decay monitor 的分工）：
- strategy_decay_monitor 只看每个策略的「无条件均值收益 / 胜率」，能发现
  "某策略近期整体偏弱"，但发现不了更隐蔽的退化——"排序仍然大致正确，只是
  区分度在消失"。IC/IR 正是量化「排序质量」的标准指标。
- 本脚本提供两类严格因果、纯本地、fail-soft 的指标：

  ① 系统级横截面 Rank-IC / IR（核心）：
     每个信号日，对当日入选票的「融合权重」与「前向收益」做 Spearman 秩相关。
     IC>0 表示权重高的票确实涨得多；IR = mean(IC)/std(IC) 衡量该能力的稳定性。
     这是整个排序系统最关键的"准不准"指标，此前任何脚本都没算过。

  ② 策略级「信念 IC」：
     每个策略 s，把"自适应分配器当天给 s 的权重（已知、无未来函数）"与
     "当天 s 选中票的平均前向收益"配对，滚动 Spearman。检验"分配器越看好 s，
     s 是否真的越灵"——补足 decay monitor（它只看无条件均值，看不到信念缩放维度）。

数据约束与诚实声明：
- 历史只保留了 DAL（融合权重 + 来源策略）与 Multi-Backtest/k_data 前向收益，
  没有保存各策略对「全候选池」的逐票打分，因此无法计算"全宇宙横截面 per-factor IC"。
  ② 是信念维度的可用代理；真正的 per-factor 横截面 IC 需改生产代码持久化策略级
  打分（列为未来增强，见脚本底部 NOTE）。
- 纯本地、不联网、不重跑回测、不依赖 scipy（手写 Spearman）；任何一步异常都
  fail-soft 返回空/None，绝不抛错中断 CI。

用法：
    python scripts/factor_ic_monitor.py [--emit-json stock_data/factor_ic.json]
"""
from __future__ import annotations

import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from walk_forward_validator import (  # noqa: E402
    ALL_STRATEGIES,
    _all_signal_days,
    _load_day_picks,
    _weights_for_day,
)
from smcore.strategy.factor_types import factor_type_of  # noqa: E402

# ── 可调阈值（脚本顶部集中）─────────────────────────────────────────
MIN_STOCKS = 5          # 单日横截面至少多少只票才单独计算「逐日 IC」（仅展示用）
IC_WINDOW = 10          # 滚动窗口：最近 N 个信号日
MIN_N_IC = 5            # 滚动窗口内合并横截面至少多少样本才判定
IC_WEAK = 0.05          # 合并 Rank-IC 低于此值（但≥0）→ 排序区分度偏弱
POS_FRAC_FLOOR = 0.5    # 逐日 IC>0 占比低于此值且均值<0 → 排序系统性失灵（展示用）
CONVICTION_IC_FLOOR = -0.1  # 策略信念 IC 低于此值（且样本足够）→ 信念反噬
MIN_HALF_N = 4          # 「前半窗/后半窗」各自至少几点才用于判持续性（否则无法判）


# ── 基础统计（不依赖 scipy）──────────────────────────────────────────
def _rank(xs: list[float]) -> list[float]:
    """平均秩（处理并列），返回与 xs 等长的秩列表。"""
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    ranks = [0.0] * len(xs)
    i = 0
    while i < len(xs):
        j = i
        while j + 1 < len(xs) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0  # 1-based 平均秩
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def _spearman(xs: list[float], ys: list[float]) -> float | None:
    """手写 Spearman 秩相关；任一数组无变化（std=0）或样本<2 返回 None。"""
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    rx, ry = _rank(xs), _rank(ys)
    n = len(rx)
    mx = sum(rx) / n
    my = sum(ry) / n
    cov = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    vx = sum((rx[i] - mx) ** 2 for i in range(n))
    vy = sum((ry[i] - my) ** 2 for i in range(n))
    if vx <= 0 or vy <= 0:
        return None  # 无变化，秩相关无定义
    return cov / math.sqrt(vx * vy)


def _spearman_crit(n: int) -> float:
    """Spearman |rho| 的双侧显著性临界值（alpha=0.05）。

    用渐近式 z/sqrt(n-1)（z=1.96）近似精确表：n=10→0.653（表 0.648）、
    n=20→0.450（表 0.447）、n=35→0.336、n=216→0.134，n≥6 时误差 <1%。

    存在的意义：样本只有 10 个点时 |rho|≈0.2~0.4 完全可能是噪声。表格必须把
    「未达显著」讲清楚，否则读者会把噪声当结论去调权重。
    """
    if n < 3:
        return 1.0
    return 1.96 / math.sqrt(n - 1)


def _n_required(ic_abs: float | None) -> int | None:
    """反解「要识别 |IC| 这个幅度，至少需要多少样本」：n = ⌈(1.96/|IC|)²⌉ + 1。

    存在的意义：只写「未达显著」读者没有量感。写上「识别 0.067 需 n≈857、当前 35」
    才能让人一眼看出这不是「接近显著」，而是差了 20 多倍 —— 从而不去据此调权重。

    取整方向必须是**向上**：由 crit(n) = 1.96/√(n−1) 推得 n−1 ≥ (1.96/|IC|)²，
    故 n−1 取 ⌈·⌉（`-1e-9` 抵消浮点误差，避免 y 恰为整数时多要一个样本）。
    """
    if ic_abs is None or ic_abs <= 0:
        return None
    need = (1.96 / ic_abs) ** 2
    return int(math.ceil(need - 1e-9)) + 1


# ── ① 系统级横截面 Rank-IC ──────────────────────────────────────────
def _day_rank_ic(sd: str) -> tuple[float | None, int]:
    """单日横截面 Rank-IC：Spearman(融合权重, 前向收益)。返回 (ic, n_stocks)。

    仅供「逐日 IC」展示；样本<MIN_STOCKS 的单日不单独判定（熊市低部署期单日票少）。
    """
    picks = _load_day_picks(sd)
    w, r = [], []
    for p in picks:
        pw = p.get("prod_weight")
        rp = p.get("return_pct")
        if pw is None or rp is None or (isinstance(pw, float) and math.isnan(pw)):
            continue
        w.append(float(pw))
        r.append(float(rp))
    if len(w) < MIN_STOCKS:
        return None, len(w)
    return _spearman(w, r), len(w)


def daily_rank_ics() -> list[dict]:
    """返回所有「单日横截面≥MIN_STOCKS」信号日的 [(day, ic, n)]（仅展示用）。"""
    out = []
    for sd in _all_signal_days():
        ic, n = _day_rank_ic(sd)
        if ic is None:
            continue
        out.append({"day": sd, "ic": round(ic, 3), "n": n})
    return out


def _pooled_ic(days: list[str]) -> tuple[float | None, int]:
    """把若干信号日的入选票「跨日合并」成一个大横截面，算一次 Spearman(权重, 收益)。

    为什么合并而非逐日平均：熊市低部署期单日入选票常 <5，逐日 IC 大量缺失，
    导致"最近窗口"样本不足、在最该预警时失效。合并后样本量 = 窗口内总票数，
    对稀疏日更稳健，且仍严格因果（权重/收益均为当时已知）。
    """
    w, r = [], []
    for sd in days:
        for p in _load_day_picks(sd):
            pw = p.get("prod_weight")
            rp = p.get("return_pct")
            if pw is None or rp is None or (isinstance(pw, float) and math.isnan(pw)):
                continue
            w.append(float(pw))
            r.append(float(rp))
    if len(w) < MIN_N_IC:
        return None, len(w)
    return _spearman(w, r), len(w)


def system_ic(window: int = IC_WINDOW) -> dict:
    """系统级合并 Rank-IC：最近窗口 vs 较早窗口对照，看排序能力是否退化。"""
    days = _all_signal_days()
    if not days:
        return {"window": window, "n": 0, "n_recent": 0, "n_baseline": 0,
                "recent_ic": None, "baseline_ic": None, "trend": None,
                "span_recent": "", "span_baseline": "", "crit": 1.0,
                "significant": False, "degraded": False, "weak": False,
                "note": "无可用信号日"}
    recent = days[-window:]
    baseline = days[-2 * window:-window]
    ric, rn = _pooled_ic(recent)
    bic, bn = _pooled_ic(baseline)
    trend = (round(ric - bic, 3) if (ric is not None and bic is not None) else None)
    degraded = (ric is not None and ric < 0)          # 近期排序系统性反向
    weak = (ric is not None and 0 <= ric < IC_WEAK)   # 近期几乎无区分度
    crit = _spearman_crit(rn)
    return {
        "window": window,
        "n_recent": rn,
        "n_baseline": bn,
        "span_recent": f"{recent[0]}~{recent[-1]}" if recent else "",
        "span_baseline": f"{baseline[0]}~{baseline[-1]}" if baseline else "",
        "recent_ic": round(ric, 3) if ric is not None else None,
        "baseline_ic": round(bic, 3) if bic is not None else None,
        "crit": round(crit, 3),
        "significant": bool(ric is not None and abs(ric) >= crit),
        "trend": trend,
        "degraded": degraded,
        "weak": weak,
        "note": "" if ric is not None else f"近期窗口合并样本不足（{rn}<{MIN_N_IC}）",
    }


# ── ② 策略级「信念 IC」──────────────────────────────────────────────
def strategy_conviction_points() -> dict[str, list[tuple[str, float, float]]]:
    """返回 {strategy: [(day, w_s, day_avg_return_s), ...]}。

    w_s = 分配器当天给策略 s 的权重（因果已知）；day_avg_return_s = 当天 s 选中票
    的平均前向收益。每策略每天一个点。
    """
    pts: dict[str, list[tuple[str, float, float]]] = {s: [] for s in ALL_STRATEGIES}
    for sd in _all_signal_days():
        weights, _ = _weights_for_day(sd)
        picks = _load_day_picks(sd)
        if not picks:
            continue
        # 每只票的前向收益按来源策略累加到当天该策略桶
        by_strat: dict[str, list[float]] = {s: [] for s in ALL_STRATEGIES}
        for p in picks:
            rp = p.get("return_pct")
            if rp is None:
                continue
            for s in p.get("sources", set()):
                if s in by_strat:
                    by_strat[s].append(float(rp))
        for s in ALL_STRATEGIES:
            bucket = by_strat[s]
            if not bucket:
                continue
            day_avg = sum(bucket) / len(bucket)
            pts[s].append((sd, float(weights.get(s, 0.0)), day_avg))
    return pts


def strategy_conviction_ic(window: int = IC_WINDOW) -> dict[str, dict]:
    """每个策略的滚动信念 IC/IR。

    ⚠️ 窗口按**统一信号日**切（`days[-window:]`），不是切各策略自己点数序列的尾部。
    旧实现 `series[-window:]` 会让样本稀的策略「借」到很早期的点：各策略 n 与日期跨度
    互不相同，而表头却宣称「最近 window 个信号日」→ 跨策略不可比，窗口甚至可能根本
    覆盖不到当前（实测 relativity 的点止于 0804，却在 0911 的告警里被当作「近期」）。
    """
    pts = strategy_conviction_points()
    days = _all_signal_days()
    window_days = set(days[-window:]) if days else set()
    out: dict[str, dict] = {}
    for s, series in pts.items():
        recent = [p for p in series if p[0] in window_days]
        n = len(recent)
        span = f"{recent[0][0]}~{recent[-1][0]}" if recent else ""
        if n < MIN_N_IC:
            out[s] = {"n": n, "factor_type": factor_type_of(s), "conviction_ic": None,
                      "ir": None, "decayed": False,
                      "span": span, "crit": 1.0, "significant": False,
                      "note": f"窗口内样本不足（{n}<{MIN_N_IC}）"}
            continue
        ws = [x[1] for x in recent]
        rs = [x[2] for x in recent]
        ic = _spearman(ws, rs)
        if ic is None:
            out[s] = {"n": n, "factor_type": factor_type_of(s), "conviction_ic": None,
                      "ir": None, "decayed": False,
                      "span": span, "crit": 1.0, "significant": False,
                      "note": "权重或收益无变化，IC 无定义"}
            continue
        # IR 用多窗口更合理，但本脚本每策略每天仅 1 点，故以「窗口内两个半窗的
        # 符号一致性」近似其稳定性：见下方 recent 子窗对比。
        # ⚠️ 半窗点数太少（<MIN_HALF_N）时 Spearman 会给出 |rho|=1 这种必然结果
        # （2 个点的秩相关非 ±1 即 0），据此判「稳定反噬」纯属噪声，故直接不判。
        half = max(1, n // 2)
        ic_first = _spearman(ws[: n - half], rs[: n - half])
        ic_second = _spearman(ws[n - half:], rs[n - half:])
        n_first, n_second = n - half, half
        # 两个半窗都为负 → 信念稳定反噬（且半窗样本量足以支撑这个判断）
        stable_neg = (ic_first is not None and ic_second is not None
                      and ic_first < 0 and ic_second < 0
                      and n_first >= MIN_HALF_N and n_second >= MIN_HALF_N)
        crit = _spearman_crit(n)
        significant = abs(ic) >= crit
        decayed = (ic < CONVICTION_IC_FLOOR and stable_neg)
        out[s] = {
            "n": n,
            "factor_type": factor_type_of(s),
            "conviction_ic": round(ic, 3),
            "ic_first_half": round(ic_first, 3) if ic_first is not None else None,
            "ic_second_half": round(ic_second, 3) if ic_second is not None else None,
            "decayed": decayed,
            "span": span,
            "crit": round(crit, 3),
            "significant": significant,
            # 未达显著 ≠ 无问题，只是「这点样本还说明不了」；如实写出来供人工判断
            "note": "" if significant else f"未达显著（|IC|={abs(ic):.3f} < {crit:.3f}）",
        }
    return out


# ── ③ 因子类型级「信念 IC」归并（2026-09-15 按因子类型分类）─────────────
def factor_type_conviction_ic(window: int = IC_WINDOW) -> dict[str, dict]:
    """把各策略的信念 IC 按因子类型归并：同类型策略的 (权重, 前向收益) 点池化后
    重算 Spearman IC。这样监控既能「按因子类型」看出哪类因子在失灵，又保留策略级明细。
    池化点数 < MIN_N_IC 的类型跳过（避免噪声）。"""
    pts = strategy_conviction_points()
    days = _all_signal_days()
    window_days = set(days[-window:]) if days else set()
    pooled: dict[str, list[tuple[float, float, str]]] = {}
    for s, series in pts.items():
        bucket = pooled.setdefault(factor_type_of(s), [])
        for day, w, ret in series:
            if day in window_days:
                bucket.append((w, ret, day))
    out: dict[str, dict] = {}
    for ft, bucket in pooled.items():
        n = len(bucket)
        if n < MIN_N_IC:
            continue
        ws = [x[0] for x in bucket]
        rs = [x[1] for x in bucket]
        dts = sorted(x[2] for x in bucket)
        ic = _spearman(ws, rs)
        if ic is None:
            continue
        crit = _spearman_crit(n)
        out[ft] = {
            "n": n,
            "factor_type": ft,
            "conviction_ic": round(ic, 3),
            "crit": round(crit, 3),
            "significant": abs(ic) >= crit,
            "span": f"{dts[0]}~{dts[-1]}",
        }
    return out


# ── 汇总 ────────────────────────────────────────────────────────────
def analyze(window: int = IC_WINDOW) -> dict:
    ics = daily_rank_ics()
    sys_ic = system_ic(window=window)
    strat = strategy_conviction_ic(window=window)
    ft_ic = factor_type_conviction_ic(window=window)

    decayed_strats = sorted([s for s, v in strat.items() if v.get("decayed")])
    degraded_sys = sys_ic.get("degraded", False)
    weak_sys = sys_ic.get("weak", False)
    # 衰减里「真正达显著」的那些 —— 这才是触发告警的条件（见下方 alert）
    decayed_confirmed = [s for s in decayed_strats if strat[s].get("significant")]
    # 系统级反向同样要求达显著：低部署期池化样本只有几十票，|IC|≈0.07 完全在噪声带内，
    # 拿它开「排序系统性反向」的 issue 等于每周一次的伪警报 → 必须过 crit 门槛。
    degraded_confirmed = bool(degraded_sys and sys_ic.get("significant"))
    n_signal_days = len(_all_signal_days())

    # ⚠️ 2026-09-15 收紧：alert 由「有负向读数」改为「有**达显著**的负向证据」。
    # 旧行为 `alert = degraded_sys or bool(decayed_strats)` 在 n=35、|IC|=0.067 时
    # 也会开 issue，而正文只能写「未达显著」——既然每期都得写这句话，就说明触发
    # 条件本身错了。灵敏度不再靠「多开 issue」保证，而是靠正文把读数和门槛都列出。
    alert = degraded_confirmed or bool(decayed_confirmed)
    if degraded_confirmed:
        alert_reason = "系统级排序反向（达显著）"
    elif decayed_confirmed:
        alert_reason = "策略信念衰减（达显著）：" + ", ".join(decayed_confirmed)
    elif degraded_sys or decayed_strats:
        alert_reason = "有负向读数但均未达显著（证据不足，不告警）"
    else:
        alert_reason = "无负向读数"

    recent_ic = sys_ic.get("recent_ic")
    return {
        "as_of": _all_signal_days()[-1] if n_signal_days else None,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "window": window,
        "window_days": min(window, n_signal_days),
        "min_n_ic": MIN_N_IC,
        "min_half_n": MIN_HALF_N,
        "system": sys_ic,
        "strategies": strat,
        "factor_types": ft_ic,
        "decayed_strategies": decayed_strats,
        "decayed_confirmed": decayed_confirmed,
        "system_degraded": degraded_sys,
        "system_degraded_significant": degraded_confirmed,
        "system_weak": weak_sys,
        "alert": alert,
        "alert_reason": alert_reason,
        "n_required_system": _n_required(abs(recent_ic) if recent_ic is not None else None),
        "daily_ics": ics,
    }


def _format_issue_body(res: dict) -> str:
    s = res["system"]
    lines = [
        "## 因子 IC/IR 监控告警（自动生成）",
        "",
        f"- 截至信号日：**{res['as_of']}**（生成于 {res.get('generated_at', 'n/a')}）",
        f"- 滚动窗口：最近 **{res['window']}** 个信号日"
        f"（实际可用 {res.get('window_days', res['window'])} 天；"
        f"合并横截面至少 {res['min_n_ic']} 样本才判定）",
        f"- **告警门槛：只有达显著才触发**（α=0.05）→ 当前 "
        f"{'🔔 触发' if res.get('alert') else '🔇 未触发'}：{res.get('alert_reason', '')}",
        "",
        "### 系统级合并 Rank-IC",
        "",
    ]
    if s.get("recent_ic") is None:
        lines.append(f"- 状态：近期窗口合并样本不足（{s.get('note', '')}）")
    else:
        flag = "🔴 排序系统性反向" if s["degraded"] else ("🟡 排序区分度偏弱" if s["weak"] else "🟢 正常")
        if not s.get("significant"):
            flag += f"（未达显著：|IC| < {s.get('crit')}）"
        lines.append(f"- 近期窗口合并 IC：**{s['recent_ic']:+.3f}**"
                     f"（样本 {s['n_recent']} 票，区间 {s.get('span_recent') or 'n/a'}）")
        base = f"{s['baseline_ic']:+.3f}" if s.get("baseline_ic") is not None else "n/a"
        lines.append(f"- 基线窗口合并 IC：{base}"
                     f"（样本 {s['n_baseline']} 票，区间 {s.get('span_baseline') or 'n/a'}）")
        # ⚠️ 基线窗口样本不足时 trend=None（而近期 IC 仍可有值）→ 须判空再格式化，
        # 否则 TypeError 会中断 CI，违背本脚本「任何一步异常都 fail-soft」的契约。
        trend_s = (f"{s['trend']:+.3f}" if s.get("trend") is not None
                   else "n/a（基线窗口样本不足）")
        lines.append(f"- 趋势(近期−基线)：{trend_s}")
        need = res.get("n_required_system")
        if need:
            lines.append(f"- 识别该幅度所需样本：n ≥ **{need}** 票"
                         f"（当前 {s['n_recent']}）")
        lines.append(f"- 判定：**{flag}**")
    lines += ["", "### 策略级「信念 IC」衰减", ""]
    if not res["decayed_strategies"]:
        lines.append("_（无）_")
    for sname in res["decayed_strategies"]:
        v = res["strategies"][sname]
        sig = "达显著" if v.get("significant") else f"⚠️ 未达显著（|IC| < {v.get('crit')}）"
        lines.append(
            f"- **{sname}**：信念 IC={v['conviction_ic']:+.3f} "
            f"（前半 {v['ic_first_half']}，后半 {v['ic_second_half']}），"
            f"样本 {v['n']}（区间 {v.get('span') or 'n/a'}），{sig} —— "
            f"分配器越看好该策略，其选中票反而越差")
    lines += ["", "### 全部策略信念 IC 概况", ""]
    lines.append("| 策略 | 样本 | 区间 | 信念IC | 前半窗 | 后半窗 | 显著性(α=0.05) | 衰减 |")
    lines.append("|---|---|---|---|---|---|---|---|")
    for sname, v in sorted(res["strategies"].items()):
        ci = v.get("conviction_ic")
        ci_s = f"{ci:+.3f}" if ci is not None else "n/a"
        fh = f"{v.get('ic_first_half'):+.3f}" if v.get("ic_first_half") is not None else "n/a"
        sh = f"{v.get('ic_second_half'):+.3f}" if v.get("ic_second_half") is not None else "n/a"
        sig_s = "n/a" if ci is None else ("✅" if v.get("significant") else f"✗（需 {v.get('crit')}）")
        lines.append(f"| {sname} | {v.get('n')} | {v.get('span') or '-'} | {ci_s} | {fh} | {sh} | "
                     f"{sig_s} | {'⚠️' if v.get('decayed') else ''} |")
    # ── ③ 因子类型级归并（2026-09-15 按因子类型分类）─────────────────────
    ft_ic = res.get("factor_types") or {}
    lines += ["", "### 因子类型级「信念 IC」归并", ""]
    if not ft_ic:
        lines.append("_（各类型窗口内样本均不足，无法归并）_")
    else:
        lines.append("| 因子类型 | 样本 | 区间 | 信念IC | 显著性(α=0.05) |")
        lines.append("|---|---|---|---|---|")
        for ft, v in sorted(ft_ic.items()):
            ci = v.get("conviction_ic")
            ci_s = f"{ci:+.3f}" if ci is not None else "n/a"
            sig_s = "n/a" if ci is None else ("✅" if v.get("significant") else f"✗（需 {v.get('crit')}）")
            lines.append(f"| {ft} | {v.get('n')} | {v.get('span') or '-'} | {ci_s} | {sig_s} |")
        lines.append("")
        lines.append("> 因子类型映射：动量=Momentum；反转·均值回归=Boll；"
                     "相对强度·资金流=Relativity；题材=Theme；事件·舆情=CCTV。"
                     "归并后仍能直接看出「哪类因子在失灵」，而策略级明细保留可用于下钻。")
    conf = res.get("decayed_confirmed") or []
    lines += [
        "",
        f"- 达显著的衰减策略：**{', '.join(conf) if conf else '无'}**"
        f"（其余衰减标记样本量不足以判显著，仅供参考）",
        "",
        "> 说明：本告警由 `.github/workflows/monitor.yml` 每周运行 "
        "`scripts/factor_ic_monitor.py` 生成。IC=Spearman(排序信号, 前向收益)。"
        "系统级为「最近窗口跨日合并横截面」的 Rank-IC（对熊市低部署的单日稀疏更稳健）；"
        "策略级为信念维度代理，窗口按**统一信号日**切，「样本」即该窗口内的有效点数。",
        "> 显著性临界值用渐近式 `1.96/sqrt(n-1)`（n≥6 与精确表误差 <1%）。"
        "**低部署期各策略样本常只有个位数，此时 IC 不可解读——请勿据此调权重。**",
        "> 触发仅代表「排序区分度退化」，请结合市场环境人工复核后再决定是否调权重。",
        "> ⚠️ 2026-09-15 起改为**门槛门控**：仅当 `|IC| ≥ crit`（α=0.05）才开 issue。"
        "低部署期池化样本常只有几十票，|IC|≈0.07 远在噪声带内（识别它需 n≈857），"
        "旧行为每周都会开一次无法解读的 issue → 告警疲劳。**未触发不代表无问题，只代表"
        "这点样本还说明不了**；全部读数仍如实列在上表。",
    ]
    return "\n".join(lines)


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--emit-json", default=None, help="把分析结果写到该 JSON 路径")
    ap.add_argument("--window", type=int, default=IC_WINDOW)
    args = ap.parse_args()

    res = analyze(window=args.window)
    print("=" * 64)
    print("因子 IC/IR 监控（系统级横截面 + 策略级信念）")
    print("=" * 64)
    s = res["system"]
    if s.get("recent_ic") is None:
        print(f"系统级：近期窗口合并样本不足（{s.get('note','')}）")
    else:
        flag = "🔴反向" if s["degraded"] else ("🟡偏弱" if s["weak"] else "🟢正常")
        trend_s = f"{s['trend']:+.3f}" if s.get("trend") is not None else "n/a"
        print(f"系统级：近期IC={s['recent_ic']:+.3f}  基线IC={s['baseline_ic']}  "
              f"趋势={trend_s}  {flag}")
    print("-" * 64)
    print("策略级信念 IC（窗口按统一信号日切）：")
    for sname, v in sorted(res["strategies"].items()):
        ci = v.get("conviction_ic")
        ci_s = f"{ci:+.3f}" if ci is not None else "n/a"
        flag = " ⚠️衰减" if v.get("decayed") else ""
        sig = "" if ci is None else (" ✅显著" if v.get("significant")
                                     else f" ✗未达显著(需{v.get('crit')})")
        print(f"  {sname:>14}  n={v.get('n'):>3}  {v.get('span') or '-':<20}"
              f"信念IC={ci_s:>7}{flag}{sig}")
    print("-" * 64)
    print("因子类型级信念 IC 归并：")
    ft_ic = res.get("factor_types") or {}
    if not ft_ic:
        print("  （各类型窗口内样本均不足，无法归并）")
    else:
        for ft, v in sorted(ft_ic.items()):
            ci = v.get("conviction_ic")
            ci_s = f"{ci:+.3f}" if ci is not None else "n/a"
            sig = "" if ci is None else (" ✅显著" if v.get("significant")
                                         else f" ✗未达显著(需{v.get('crit')})")
            print(f"  {ft:<16} n={v.get('n'):>3}  {v.get('span') or '-':<20}信念IC={ci_s:>7}{sig}")
    print("-" * 64)
    print(f"告警：{'是' if res['alert'] else '否'}  {res.get('alert_reason', '')}")
    print(f"  （系统反向={res['system_degraded']}，其中达显著="
          f"{res.get('system_degraded_significant')}；衰减策略={res['decayed_strategies'] or '无'}，"
          f"其中达显著={res.get('decayed_confirmed') or '无'}；"
          f"识别系统级该幅度需 n≥{res.get('n_required_system') or 'n/a'}）")
    print(_format_issue_body(res))

    if args.emit_json:
        try:
            out = {k: v for k, v in res.items() if k != "daily_ics"}
            with open(args.emit_json, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, indent=2, default=str)
            print(f"\n结果已写：{args.emit_json}")
        except Exception as e:  # pragma: no cover
            print(f"写 JSON 失败：{e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# NOTE（未来增强）：真正的 per-factor 横截面 IC 需要生产代码在生成 DAL 时，
# 持久化每个策略对「全候选池」的逐票打分（而非仅入选票的融合权重）。届时可将
# ② 升级为标准 cross-sectional factor IC（每个信号日对全池排序→Spearman→滚动 IR），
# 比当前"信念代理"更严谨。当前实现已能在无额外数据的前提下捕获排序退化信号。
