"""因子 IC/IR 监控脚本回归（scripts/factor_ic_monitor.py）。

背景（2026-09-15 修复的两处缺陷，本文件锁定它们不再回归）：

① **窗口必须按「统一信号日」切**。旧实现 `series[-window:]` 切的是各策略**自己**
   点数序列的尾部 —— 样本稀的策略会「借」到很早期的点。实测 20260911 那次告警里，
   momentum 只有 10 个点却一直借到 8 月，relativity 的点干脆止于 0804，
   而表头却宣称「最近 10/20/40 个信号日」→ 跨策略不可比、窗口甚至覆盖不到当前。
   本文件用「某策略只在窗外有样本」的合成数据，断言其窗口内样本数 = 0。

② **半窗样本不足时不得判「稳定反噬」**。n 个点的后半窗只有 n//2 个点，n 很小时
   Spearman 必然给出 |rho|=1（2 个点非 ±1 即 0），据此 flag 纯属噪声 → 加
   `MIN_HALF_N` 门槛。本文件用「5 点严格单调」验证：IC=-1 但 **不**判 decayed。

③ 显著性：所有 IC 必须给出 `crit`（α=0.05，渐近式 1.96/√(n-1)）与 `significant`，
   告警正文要把「未达显著」写出来 —— 防止把个位数样本的噪声当结论去调权重。
"""
from __future__ import annotations

import importlib.util
import math
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "factor_ic_monitor", _ROOT / "scripts" / "factor_ic_monitor.py")
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


# ── 合成数据工具 ─────────────────────────────────────────────────────
def _days(n: int) -> list[str]:
    """n 个升序可排序的信号日。"""
    return [f"2026-01-{i:02d}" for i in range(1, n + 1)]


def _pick(w: float, r: float, src: str) -> dict:
    return {"prod_weight": w, "return_pct": r, "sources": {src}}


def _install(monkeypatch, days, picks_by_day, weights_by_day):
    """把脚本的三个数据入口替换成合成数据（脚本内按模块全局名调用 → 可 monkeypatch）。"""
    monkeypatch.setattr(mod, "_all_signal_days", lambda: list(days))
    monkeypatch.setattr(mod, "_load_day_picks",
                        lambda sd: list(picks_by_day.get(sd, [])))
    monkeypatch.setattr(mod, "_weights_for_day",
                        lambda sd: (dict(weights_by_day.get(sd, {})), {}))


# ── ① 基础统计 ───────────────────────────────────────────────────────
def test_spearman_basic():
    assert mod._spearman([1, 2, 3, 4], [10, 20, 30, 40]) == pytest.approx(1.0)
    assert mod._spearman([1, 2, 3, 4], [40, 30, 20, 10]) == pytest.approx(-1.0)
    # 并列秩：全部相同 → 无变化 → 无定义
    assert mod._spearman([1, 1, 1], [1, 2, 3]) is None
    # 样本 <2 或长度不等 → None
    assert mod._spearman([1], [1]) is None
    assert mod._spearman([1, 2], [1]) is None


def test_spearman_crit_matches_asymptotic_table():
    """临界值 = 1.96/√(n-1)；与精确 Spearman 表 n≥6 误差 <1%。"""
    assert mod._spearman_crit(2) == 1.0        # n<3 无意义 → 取最严
    # n=3 的渐近式 >1 → 含义是「|rho|≤1 永远够不到门槛」，即 3 点不可能判显著
    assert mod._spearman_crit(3) == pytest.approx(1.96 / math.sqrt(2))
    assert mod._spearman_crit(3) > 1.0
    assert mod._spearman_crit(10) == pytest.approx(1.96 / math.sqrt(9), rel=1e-9)
    assert mod._spearman_crit(20) == pytest.approx(0.450, abs=5e-3)
    assert mod._spearman_crit(35) == pytest.approx(0.336, abs=5e-3)
    assert mod._spearman_crit(216) == pytest.approx(0.134, abs=5e-3)
    # 单调递减：样本越多，判显著的门槛越低
    assert mod._spearman_crit(10) > mod._spearman_crit(20) > mod._spearman_crit(50)


# ── ② 窗口按统一信号日切（旧实现系列的回归点）─────────────────────────
def test_window_uses_unified_signal_days_not_per_strategy_tail(monkeypatch):
    """只在自己序列尾部取 window 个点 → 会借到窗外点；按统一信号日切 → 窗外点为 0。"""
    days = _days(20)
    win = 5
    window_days = set(days[-win:])          # 01-16 ~ 01-20

    early_days = days[:5]                    # 01-01 ~ 01-05：全部在窗外
    late_days = days[-win:]                  # 01-16 ~ 01-20

    picks, weights = {}, {}
    for i, d in enumerate(early_days):
        picks[d] = [_pick(float(i), 1.0, "boll")]
        weights[d] = {"boll": float(i)}
    for i, d in enumerate(late_days):
        picks[d] = [_pick(float(i), 1.0, "theme")]
        weights[d] = {"theme": float(i)}

    _install(monkeypatch, days, picks, weights)
    res = mod.strategy_conviction_ic(window=win)

    # 关键断言：boll 的样本全在窗外 → 窗口内样本数必须是 0（旧实现会给 5）
    assert res["boll"]["n"] == 0
    assert res["boll"]["span"] == ""
    assert res["theme"]["n"] == win

    # 任何策略的样本数都不可能超过窗口长度；跨度起点不得早于窗口首个信号日
    first_window_day = days[-win]
    for s, v in res.items():
        assert v["n"] <= win, f"{s} 样本数超出窗口"
        if v["n"]:
            assert v["span"].split("~")[0] >= first_window_day, f"{s} 借到了窗外样本"


def test_relativity_absent_in_window_reports_zero(monkeypatch):
    """复刻真实故障：某策略窗口内无更新 → 必须如实报 0，而不是拿旧点冒充「近期」。"""
    days = _days(20)
    picks, weights = {}, {}
    # relativity 只在最早 3 天出现；theme 覆盖全窗
    for i, d in enumerate(days[:3]):
        picks[d] = [_pick(float(i), 1.0, "relativity")]
        weights[d] = {"relativity": float(i)}
    for i, d in enumerate(days[-10:]):
        picks.setdefault(d, []).append(_pick(float(i), 1.0, "theme"))
        weights.setdefault(d, {})["theme"] = float(i)

    _install(monkeypatch, days, picks, weights)
    res = mod.strategy_conviction_ic(window=10)
    assert res["relativity"]["n"] == 0
    assert "样本不足" in res["relativity"]["note"]


# ── ③ 半窗样本门槛（MIN_HALF_N）─────────────────────────────────────
def test_small_half_window_not_flagged_decayed(monkeypatch):
    """5 点严格单调 → IC=-1，但后半窗仅 2 点 → 不得判「稳定反噬」。"""
    days = _days(20)
    win = 10
    late = days[-5:]                          # 窗口内只有 5 个点
    picks, weights = {}, {}
    for i, d in enumerate(late):
        picks[d] = [_pick(float(i + 1), -float(i + 1), "momentum")]
        weights[d] = {"momentum": float(i + 1)}

    _install(monkeypatch, days, picks, weights)
    res = mod.strategy_conviction_ic(window=win)["momentum"]

    assert res["n"] == 5
    assert res["conviction_ic"] == pytest.approx(-1.0)
    assert res["conviction_ic"] < mod.CONVICTION_IC_FLOOR   # 确实低于地板
    assert res["decayed"] is False                          # 但样本不足以判
    assert res["ic_second_half"] == pytest.approx(-1.0)     # 半窗确实是 -1（噪声）


def test_sufficient_half_window_flagged_and_significant(monkeypatch):
    """10 点严格单调 → 两个半窗各 5 点 ≥ MIN_HALF_N → 判 decayed 且达显著。"""
    days = _days(20)
    win = 10
    late = days[-10:]
    picks, weights = {}, {}
    for i, d in enumerate(late):
        picks[d] = [_pick(float(i + 1), -float(i + 1), "boll")]
        weights[d] = {"boll": float(i + 1)}

    _install(monkeypatch, days, picks, weights)
    res = mod.strategy_conviction_ic(window=win)["boll"]

    assert res["n"] == 10
    assert res["decayed"] is True
    assert res["significant"] is True
    assert res["crit"] == pytest.approx(round(mod._spearman_crit(10), 3))
    assert mod.MIN_HALF_N == 4          # 门槛常量本身
    assert res["n"] // 2 >= mod.MIN_HALF_N


# ── ④ 显著性标注 / analyze 汇总 ──────────────────────────────────────
def test_significance_flag_consistent_with_crit(monkeypatch):
    """significant 必须严格等于 |IC| ≥ crit。"""
    days = _days(30)
    late = days[-12:]
    picks, weights = {}, {}
    for i, d in enumerate(late):
        # 权重与收益同向但带噪声 → IC 为正、未达显著
        picks[d] = [_pick(float(i + 1), float(i % 4), "theme")]
        weights[d] = {"theme": float(i + 1)}

    _install(monkeypatch, days, picks, weights)
    res = mod.strategy_conviction_ic(window=15)["theme"]

    assert res["conviction_ic"] is not None
    assert res["significant"] is (abs(res["conviction_ic"]) >= res["crit"])
    if not res["significant"]:
        assert "未达显著" in res["note"]
    else:
        assert res["note"] == ""


def test_analyze_decayed_confirmed_is_subset(monkeypatch):
    """decayed_confirmed ⊆ decayed_strategies，且每个都 significant。"""
    days = _days(24)
    picks, weights = {}, {}
    # boll：窗口内 12 点单调反向 → decayed + significant
    for i, d in enumerate(days[-12:]):
        picks.setdefault(d, []).append(_pick(float(i + 1), -float(i + 1), "boll"))
        weights.setdefault(d, {})["boll"] = float(i + 1)
    # momentum：窗口内仅 4 点单调反向 → 低于 MIN_N_IC(5)，只报样本不足
    for i, d in enumerate(days[-4:]):
        picks.setdefault(d, []).append(_pick(float(i + 1), -float(i + 1), "momentum"))
        weights.setdefault(d, {})["momentum"] = float(i + 1)

    _install(monkeypatch, days, picks, weights)
    res = mod.analyze(window=15)

    assert "boll" in res["decayed_strategies"]
    assert set(res["decayed_confirmed"]) <= set(res["decayed_strategies"])
    for s in res["decayed_confirmed"]:
        assert res["strategies"][s]["significant"] is True
    # momentum 样本不足 → 不参与衰减判定
    assert res["strategies"]["momentum"]["n"] == 4
    assert res["strategies"]["momentum"]["decayed"] is False
    assert "样本不足" in res["strategies"]["momentum"]["note"]

    # 汇总字段齐备
    assert res["min_half_n"] == mod.MIN_HALF_N
    assert res["window_days"] == min(15, len(days))
    assert res["alert"] is True


def test_issue_body_marks_insignificance(monkeypatch):
    """告警正文必须显式标注「未达显著」，并列出达显著的衰减策略。"""
    days = _days(24)
    picks, weights = {}, {}
    for i, d in enumerate(days[-12:]):
        picks[d] = [_pick(float(i + 1), -float(i + 1), "boll")]
        weights[d] = {"boll": float(i + 1)}

    _install(monkeypatch, days, picks, weights)
    body = mod._format_issue_body(mod.analyze(window=15))

    assert "显著性(α=0.05)" in body
    assert "达显著的衰减策略" in body
    assert "**boll**" in body
    assert "统一信号日" in body


# ── ⑤ 系统级合并 IC ─────────────────────────────────────────────────
def test_system_ic_reports_span_and_significance(monkeypatch):
    days = _days(20)
    win = 10
    picks, weights = {}, {}
    for i, d in enumerate(days[-10:]):
        picks[d] = [_pick(float(i + 1), -float(i + 1), "boll")]
        weights[d] = {"boll": float(i + 1)}

    _install(monkeypatch, days, picks, weights)
    s = mod.system_ic(window=win)

    assert s["n_recent"] == 10
    assert s["span_recent"] == f"{days[-win]}~{days[-1]}"
    assert s["crit"] == pytest.approx(round(mod._spearman_crit(10), 3))
    assert s["significant"] is (abs(s["recent_ic"]) >= s["crit"])
    assert s["degraded"] is True          # 严格反向


def test_format_issue_body_survives_missing_baseline(monkeypatch):
    """回归：近期窗口有样本、**基线窗口样本不足**时 trend=None。

    旧实现直接 `{trend:+.3f}` → TypeError 中断 CI，违背脚本「任何一步异常都
    fail-soft，绝不抛错中断 CI」的契约。此处断言既不崩、也不误报趋势数字。
    """
    days = _days(20)
    picks, weights = {}, {}
    # 只在最后 10 天有票 → 基线窗口（days[-30:-15]）完全没样本 → baseline_ic=None
    for i, d in enumerate(days[-10:]):
        picks[d] = [_pick(float(i + 1), -float(i + 1), "boll")]
        weights[d] = {"boll": float(i + 1)}

    _install(monkeypatch, days, picks, weights)
    res = mod.analyze(window=15)

    assert res["system"]["recent_ic"] is not None
    assert res["system"]["baseline_ic"] is None
    assert res["system"]["trend"] is None

    body = mod._format_issue_body(res)      # 旧实现此处抛 TypeError
    assert "趋势(近期−基线)" in body
    assert "基线窗口样本不足" in body
