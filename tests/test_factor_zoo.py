"""因子池（smcore.strategy.factor_zoo）纯函数测试。

守护四件事：
1. **文法 ↔ 预注册基线一致**：文法写出的 mom20/mom60/rev5/vol20/amp20/liq20/illiq20/
   distma20 必须与 factor_engine.build_price_factors 的 8 个 v1 基线逐值相同——
   否则「挖掘出的因子」与「已发表的基线」不可比，结论全废。
2. **无未来函数**：任一因子在 t 日的取值，截断到 t 日再算必须完全相同。
3. 统计与闸门：非重叠取样、双侧 p、BH-FDR、Bonferroni、冗余剔重、裁决优先级。
4. 枚举确定性 / 上限 / 唯一性。
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pytest  # noqa: E402

from smcore.strategy import factor_engine as fe  # noqa: E402
from smcore.strategy import factor_zoo as fz  # noqa: E402


# ── 合成上下文 ──────────────────────────────────────────────────────────
def _ctx(n_days=200, codes=("000001", "000002", "600000", "300001", "688001", "830001")):
    idx = pd.date_range("2020-01-01", periods=n_days, freq="D")
    t = np.arange(n_days)
    close = pd.DataFrame(
        {c: 10.0 * (1 + 0.002 * (i + 1)) ** t * (1 + 0.05 * np.sin(t * 0.3 + i)) for i, c in enumerate(codes)},
        index=idx)
    high = close * 1.015
    low = close * 0.985
    op = close.shift(1).fillna(close.iloc[0]) * 1.002
    volume = pd.DataFrame({c: 1e6 * (1 + 0.2 * np.cos(t * 0.2 + i)) for i, c in enumerate(codes)}, index=idx)
    amount = volume * close
    ret1, _ = fe.daily_returns_and_bad(close)
    return {"close": close, "high": high, "low": low, "open": op,
            "volume": volume, "amount": amount, "ret1": ret1}


def _slice(ctx, n):
    return {k: v.iloc[:n] for k, v in ctx.items()}


def _cand_map():
    return {c.name: c for c in fz.enumerate_candidates()}


# ── 1. 枚举 ─────────────────────────────────────────────────────────────
def test_enumerate_deterministic_and_unique():
    a = fz.enumerate_candidates()
    b = fz.enumerate_candidates()
    assert [c.name for c in a] == [c.name for c in b]
    assert len(a) >= 90, "文法候选太少，达不到『很多个因子可对比』的目的"
    names = [c.name for c in a]
    assert len(set(names)) == len(names), "候选名重复"


def test_enumerate_respects_cap():
    capped = fz.enumerate_candidates(7)
    assert len(capped) == 7
    assert [c.name for c in capped] == [c.name for c in fz.enumerate_candidates()][:7]


def test_all_pre_registered_baselines_are_in_grammar():
    names = {c.name for c in fz.enumerate_candidates()}
    assert set(fz.BASELINE_ALIAS).issubset(names)


# ── 2. 文法 ↔ 预注册基线逐值一致 ────────────────────────────────────────
def test_grammar_reproduces_pre_registered_baselines():
    ctx = _ctx(n_days=260)
    base = fe.build_price_factors(ctx["close"], ctx["high"], ctx["low"], ctx["amount"], ctx["ret1"])
    cm = _cand_map()
    for gram_name, base_name in fz.BASELINE_ALIAS.items():
        got = fz.compute_factor(ctx, cm[gram_name])
        exp = base[base_name]
        assert np.allclose(got.values, exp.values, equal_nan=True, rtol=1e-12, atol=1e-15), (
            f"{gram_name} 与基线 {base_name} 不一致——文法已偏离预注册定义")


def test_baseline_lookback_keeps_mom60_uncapped():
    cm = _cand_map()
    assert cm["mom60"].lookback == fe.baseline_lookback("mom60") == 60
    assert cm["mom20"].lookback == 20
    assert cm["illiq20"].lookback == 20
    assert cm["tsrank20_250"].lookback == fe.FACTOR_LOOKBACK_CAP  # 非基线 → 压到上限


# ── 3. 无未来函数 ───────────────────────────────────────────────────────
@pytest.mark.parametrize("name", ["mom60", "vol20", "disthi120", "tsrank20_120",
                                  "pvcorr60", "cvvol60", "stoch60", "illiq20", "upfrac20"])
def test_no_lookahead_factor_values(name):
    ctx = _ctx(n_days=300)
    cut = 250
    cm = _cand_map()
    full = fz.compute_factor(ctx, cm[name])
    part = fz.compute_factor(_slice(ctx, cut + 1), cm[name])
    assert np.allclose(full.iloc[cut].values, part.iloc[-1].values, equal_nan=True), (
        f"{name} 在 t 日的取值依赖了 t 之后的数据（未来函数）")


def test_factor_columns_align_with_input():
    ctx = _ctx(n_days=120)
    f = fz.compute_factor(ctx, _cand_map()["amp20"])
    assert list(f.columns) == list(ctx["close"].columns)
    assert list(f.index) == list(ctx["close"].index)


# ── 4. 统计 ─────────────────────────────────────────────────────────────
def test_p_two_sided_known_values():
    assert fz._p_two_sided(0.0) == pytest.approx(1.0)
    assert fz._p_two_sided(1.96) == pytest.approx(0.05, abs=1e-3)
    assert fz._p_two_sided(2.576) == pytest.approx(0.01, abs=1e-3)
    assert fz._p_two_sided(-1.96) == pytest.approx(fz._p_two_sided(1.96))


def test_ic_stats_non_overlapping_thinning():
    idx = pd.date_range("2023-01-02", periods=100, freq="D")
    s = pd.Series(np.linspace(-0.1, 0.1, 100), index=idx)
    st = fz.ic_stats(s, None, eval_step=10)
    assert st["n_days"] == 100
    assert st["n_eval"] == 10
    thin = s.iloc[::10]
    assert st["mean_ic"] == pytest.approx(round(float(thin.mean()), 4))
    assert st["t_stat"] == pytest.approx(round(float(thin.mean() / thin.std() * np.sqrt(10)), 2))
    assert 0.0 <= st["p_value"] <= 1.0


def test_ic_stats_degenerate_zero_variance():
    idx = pd.date_range("2023-01-02", periods=100, freq="D")
    st = fz.ic_stats(pd.Series(np.full(100, 0.05), index=idx), None, eval_step=10)
    assert st["std"] == 0.0
    assert st["t_stat"] == 0.0
    assert st["p_value"] == 1.0          # t=0 → 不显著
    assert st["stable_frac"] == 1.0      # 单年且正 → 同号率 100%


def test_ic_stats_stable_frac_and_seg_match():
    idx = pd.to_datetime(["2023-01-03"] * 30 + ["2024-01-02"] * 30 + ["2025-01-02"] * 30)
    s = pd.Series([0.02] * 30 + [0.03] * 30 + [-0.01] * 30, index=idx)
    st = fz.ic_stats(s, pd.Series([0.01] * 20, index=pd.date_range("2021-01-01", periods=20)),
                     eval_step=5)
    assert st["stable_frac"] == pytest.approx(2 / 3, abs=1e-3)   # 2 年正 1 年负
    assert st["seg_match"] is True
    st2 = fz.ic_stats(s, pd.Series([-0.01] * 20, index=pd.date_range("2021-01-01", periods=20)),
                      eval_step=5)
    assert st2["seg_match"] is False


def test_ic_stats_insufficient_samples():
    idx = pd.date_range("2023-01-02", periods=3, freq="D")
    st = fz.ic_stats(pd.Series([0.1, np.nan, 0.2], index=idx), None, eval_step=10)
    assert st["n_eval"] == 1
    assert st["mean_ic"] is None and st["p_value"] == 1.0


def test_benjamini_hochberg_rejects_front_loaded():
    rej = fz.benjamini_hochberg([0.001, 0.9, 0.9], q=0.1)
    assert rej == [True, False, False]
    assert fz.benjamini_hochberg([0.5, 0.6], q=0.1) == [False, False]
    assert fz.benjamini_hochberg([], q=0.1) == []
    # 单调性：p 被拒 → 比它小的 p 必被拒
    ps = [0.004, 0.02, 0.03, 0.04, 0.2, 0.5, 0.7]
    rej2 = fz.benjamini_hochberg(ps, q=0.1)
    for i, p in enumerate(ps):
        if rej2[i]:
            assert all(rej2[j] for j, q in enumerate(ps) if q <= p)


def test_bonferroni_threshold():
    ps = [0.0004, 0.0006]          # m=100 → 阈值 0.05/100 = 0.0005
    assert fz.bonferroni(ps + [0.5] * 98) == [True, False] + [False] * 98
    assert fz.bonferroni([]) == []


# ── 5. 冗余剔重 ─────────────────────────────────────────────────────────
def _ic_frame():
    t = np.arange(120)
    a = pd.Series(np.sin(t * 0.20))
    return pd.DataFrame({"a": a, "b": a.copy(), "c": np.sin(t * 2.30)})


def test_prune_redundant_keeps_higher_icir():
    rows = [{"name": "a", "icir": 0.5}, {"name": "b", "icir": 0.3}, {"name": "c", "icir": 0.1}]
    fz.prune_redundant(rows, _ic_frame(), rho=0.85)
    by = {r["name"]: r for r in rows}
    assert by["b"]["redundant_with"] == "a"
    assert by["a"]["redundant_with"] is None
    assert by["c"]["redundant_with"] is None


def test_prune_redundant_anchor_survives():
    rows = [{"name": "a", "icir": 0.5}, {"name": "b", "icir": 0.3}]
    fz.prune_redundant(rows, _ic_frame(), rho=0.85, anchors={"b"})
    by = {r["name"]: r for r in rows}
    assert by["b"]["redundant_with"] is None      # 锚不被剔
    assert by["a"]["redundant_with"] == "b"       # 其近似复制品被剔


def test_prune_redundant_noop_without_frame():
    rows = [{"name": "a", "icir": 0.5}]
    fz.prune_redundant(rows, None, rho=0.85)
    assert rows[0]["redundant_with"] is None


# ── 6. 裁决优先级 ───────────────────────────────────────────────────────
def _row(**kw):
    r = {"name": "x", "family": "测试", "prior": 1, "is_baseline": False,
         "redundant_with": None, "n_eval": 100, "p_value": 1e-6,
         "full_mean_ic": 0.03, "is_mean_ic": 0.03, "icir": 0.5, "t_stat": 5.0,
         "stable_frac": 1.0, "seg_match": True, "by_year": {"2023": 0.03, "2024": 0.02}}
    r.update(kw)
    return r


def test_judge_priority_and_verdicts():
    assert fz.judge(_row(redundant_with="a"), True, True, 1)[0] == "冗余"
    assert fz.judge(_row(n_eval=fz.MIN_OOS_EVAL_DAYS - 1), True, True, 1)[0] == "样本不足"
    assert fz.judge(_row(), False, False, 1)[0] == "不显著"
    assert fz.judge(_row(full_mean_ic=0.001), True, True, 1)[0] == "IC过弱"
    assert fz.judge(_row(stable_frac=0.5), True, True, 1)[0] == "分年不稳"
    assert fz.judge(_row(seg_match=False), True, True, 1)[0] == "两段矛盾"
    assert fz.judge(_row(full_mean_ic=-0.05), True, True, 1)[0] == "方向反转"
    assert fz.judge(_row(), True, True, 1)[0] == "存活"
    assert fz.judge(_row(), True, False, 1)[0] == "存活(FDR)"


def test_judge_reason_is_nonempty_for_rejects():
    for r in (_row(redundant_with="a", redundant_rho=0.99), _row(n_eval=1),
              _row(full_mean_ic=0.0), _row(stable_frac=0.1),
              _row(seg_match=False), _row(full_mean_ic=-0.05)):
        v, why = fz.judge(r, True, True, 1)
        assert v != "存活" and why, v


def test_judge_zero_std_factor_not_significant():
    """sd≈0 的退化候选不得因 t→∞ 而被判存活。"""
    idx = pd.date_range("2023-01-02", periods=100, freq="D")
    st = fz.ic_stats(pd.Series(np.full(100, 0.05), index=idx), None, eval_step=10)
    assert st["icir"] == 0.0 and st["p_value"] == 1.0


# ── 7. 报告渲染 ─────────────────────────────────────────────────────────
def test_build_rows_and_report_render():
    cands = fz.enumerate_candidates(5)
    results = {c.name: _row(name=c.name) for c in cands}
    rows = fz.build_rows(cands, results)
    assert len(rows) == len(cands)
    for r in rows:
        r["verdict"], r["reason"] = fz.judge(r, True, True, r["prior"])
    md = fz.format_report(rows, {"generated_at": "2026-09-16 00:00:00",
                                 "n_candidates": len(rows), "oos_start": "2023-01-01",
                                 "grid_days": 10, "grid_codes": 5, "corrupted_bars": 0})
    assert "因子池回放与自动挖掘" in md
    assert "存活清单" in md
    assert "裁决分布" in md
    assert "Caveats" in md
    assert md.count("|") > 50


def test_report_marks_baseline_with_star():
    rows = [dict(_row(name="mom20", is_baseline=True), verdict="存活", reason="")]
    md = fz.format_report(rows, {"n_candidates": 1})
    assert "mom20★" in md


# ── 8. 短窗 min_periods（曾静默跳过 29 个候选）──────────────────────────
def test_roll_min_matches_v1_for_long_windows():
    assert fe.ROLL_MIN == 15
    assert fe.roll_min(20) == 15      # 与 v1 一致：长窗 min_periods 不变
    assert fe.roll_min(60) == 15
    assert fe.roll_min(250) == 15
    assert fe.roll_min(10) == 10      # 短窗按窗长放宽
    assert fe.roll_min(5) == 5
    assert fe.roll_min(1) == 1


def test_short_window_factors_compute_without_error():
    ctx = _ctx(n_days=120)
    cm = _cand_map()
    for name in ("vol5", "vol10", "liq10", "distma5", "distma10", "skew10", "kurt20",
                 "gap5", "illiq5", "illiq10", "amp10", "vratio5_60", "hlspread5_20",
                 "matrend5_20", "volratio5_120", "maxret10", "minret10", "upfrac10",
                 "pvcorr10", "intraday5"):
        f = fz.compute_factor(ctx, cm[name])
        assert not f.dropna(how="all").empty, f"{name} 全空"


def test_short_windows_have_no_lookahead():
    """短窗同样不得引入未来函数（min_periods 放宽后重新校验）。"""
    ctx = _ctx(n_days=140)
    cut = 120
    cm = _cand_map()
    for name in ("vol5", "liq10", "distma5", "skew10", "pvcorr10"):
        full = fz.compute_factor(ctx, cm[name])
        part = fz.compute_factor(_slice(ctx, cut + 1), cm[name])
        assert np.allclose(full.iloc[cut].values, part.iloc[-1].values, equal_nan=True), name
