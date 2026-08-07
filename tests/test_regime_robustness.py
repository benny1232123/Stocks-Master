"""P1-3 机制稳健性（regime robustness）单元测试。

守卫：
1. 纯函数 _regime_robust_gate：跨市场状态都跑赢等权才放行；样本不足以分状态时真空通过。
2. run() 能为每个信号日打上当时的市场状态（as_of 历史切片，因果安全），并产出 regime_table。
3. recommend() 把 regime_robust 折入 robust，且 checks 暴露分状态明细。
"""
import sys
import types
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
for p in (str(ROOT), str(ROOT / "scripts")):
    if p not in sys.path:
        sys.path.insert(0, p)

import walk_forward_validator as wf  # noqa: E402

# 让某一策略（theme）在组合里占绝对权重，从而用单票收益方向控制 adaptive vs equal 的胜负
WEIGHTS = {"theme": 80.0, "momentum": 5.0, "boll": 5.0, "relativity": 5.0, "cctv": 5.0}

UP = ["20260104", "20260105", "20260106"]
DOWN = ["20260202", "20260203", "20260204"]


def _build_patch(up_dates, down_dates, down_beats=True):
    all_dates = up_dates + down_dates

    def fake_sig_days():
        return list(all_dates)

    def fake_weights(sd, shrinkage=None, floor=None, zero_negative_edge=True):
        return (dict(WEIGHTS), False)

    def fake_picks(sd, exit_kwargs=None):
        # 上行状态：theme 票 +10；下行状态：theme 票方向上由 down_beats 控制
        if sd in up_dates:
            theme_ret = 10.0
        else:
            theme_ret = 10.0 if down_beats else -10.0
        return [
            {"code": "600000", "sources": {"theme"}, "return_pct": theme_ret, "prod_weight": None},
            {"code": "600001", "sources": {"momentum"}, "return_pct": 2.0, "prod_weight": None},
        ]

    def fake_profile(as_of=None):
        regime = "趋势上行" if as_of in up_dates else "下行防御"
        return types.SimpleNamespace(regime=regime)

    return fake_sig_days, fake_weights, fake_picks, fake_profile


def _apply_patches(up_dates, down_dates, down_beats=True, risk_cfg=None):
    fsd, fw, fp, fp2 = _build_patch(up_dates, down_dates, down_beats)
    ctx = (
        mock.patch.object(wf, "_all_signal_days", fsd),
        mock.patch.object(wf, "_weights_for_day", fw),
        mock.patch.object(wf, "_load_day_picks", fp),
        mock.patch.object(wf, "_market_regime_as_of", lambda sd: fp2(sd).regime),
    )
    for c in ctx:
        c.start()
    if risk_cfg is not None:
        ctx = ctx + (mock.patch.object(wf, "RISK_CONFIG", risk_cfg),)
        ctx[-1].start()
    return ctx


def _stop(ctx):
    for c in ctx:
        c.stop()


# ── 纯函数闸门 ──────────────────────────────────────────────────────────────
def test_gate_two_regimes_one_beats_is_not_robust():
    rt = {
        "趋势上行": {"n_days": 3, "adaptive_pct": 5.0, "equal_pct": 2.0, "diff_pct": 3.0},
        "下行防御": {"n_days": 3, "adaptive_pct": 1.0, "equal_pct": 4.0, "diff_pct": -3.0},
    }
    g = wf._regime_robust_gate(rt, enabled=True, min_regimes=2, min_days_per_regime=3)
    assert g["diverse"] is True
    assert g["beat"] == 1
    assert g["robust"] is False  # 只跑赢 1/2 个状态 → 不稳健


def test_gate_both_regimes_beat_is_robust():
    rt = {
        "趋势上行": {"n_days": 3, "adaptive_pct": 5.0, "equal_pct": 2.0, "diff_pct": 3.0},
        "下行防御": {"n_days": 3, "adaptive_pct": 5.0, "equal_pct": 2.0, "diff_pct": 3.0},
    }
    g = wf._regime_robust_gate(rt, enabled=True, min_regimes=2, min_days_per_regime=3)
    assert g["beat"] == 2
    assert g["robust"] is True


def test_gate_single_regime_is_vacuous_pass():
    rt = {"趋势上行": {"n_days": 5, "adaptive_pct": 1.0, "equal_pct": 2.0, "diff_pct": -1.0}}
    g = wf._regime_robust_gate(rt, enabled=True, min_regimes=2, min_days_per_regime=3)
    assert g["diverse"] is False  # 只有 1 个状态 → 不足检验
    assert g["robust"] is True    # 真空通过，不阻断


def test_gate_disabled_is_vacuous_pass():
    rt = {
        "趋势上行": {"n_days": 3, "adaptive_pct": 5.0, "equal_pct": 2.0, "diff_pct": 3.0},
        "下行防御": {"n_days": 3, "adaptive_pct": 1.0, "equal_pct": 4.0, "diff_pct": -3.0},
    }
    g = wf._regime_robust_gate(rt, enabled=False, min_regimes=2, min_days_per_regime=3)
    assert g["robust"] is True


def test_gate_min_days_filtering():
    # 某一状态样本不足 min_days_per_regime → 不计入资格/不启用闸门
    rt = {
        "趋势上行": {"n_days": 3, "adaptive_pct": 5.0, "equal_pct": 2.0, "diff_pct": 3.0},
        "下行防御": {"n_days": 1, "adaptive_pct": -3.0, "equal_pct": 2.0, "diff_pct": -5.0},
    }
    g = wf._regime_robust_gate(rt, enabled=True, min_regimes=2, min_days_per_regime=3)
    assert g["diverse"] is False  # 仅 1 个状态达标
    assert g["robust"] is True    # 真空通过


# ── run() 分状态归因（mock 数据，无网络） ──────────────────────────────────
def test_run_builds_regime_table():
    ctx = _apply_patches(UP, DOWN, down_beats=False)
    try:
        res = wf.run()
        rt = res["regime_table"]
        assert set(rt.keys()) == {"趋势上行", "下行防御"}
        assert rt["趋势上行"]["n_days"] == 3
        assert rt["趋势上行"]["diff_pct"] > 0   # 上行状态 adaptive 跑赢
        assert rt["下行防御"]["diff_pct"] < 0   # 下行状态 adaptive 跑输
        # 每行都应带 regime 标签
        assert all(r.get("regime") in ("趋势上行", "下行防御") for r in res["rows"] if not r["skipped"])
    finally:
        _stop(ctx)


# ── recommend() 把 regime_robust 折入 checks ──────────────────────────────
def test_recommend_regime_gate_fails_when_not_robust():
    ctx = _apply_patches(UP, DOWN, down_beats=False)
    try:
        rec = wf.recommend()
        assert rec["checks"]["regime_diverse"] is True
        assert rec["checks"]["regime_beat_count"] == 1
        assert rec["checks"]["regime_robust_ok"] is False
    finally:
        _stop(ctx)


def test_recommend_regime_gate_passes_when_robust():
    ctx = _apply_patches(UP, DOWN, down_beats=True)
    try:
        rec = wf.recommend()
        assert rec["checks"]["regime_beat_count"] == 2
        assert rec["checks"]["regime_robust_ok"] is True
    finally:
        _stop(ctx)


def test_recommend_vacuous_pass_single_regime():
    ctx = _apply_patches(UP, [], down_beats=True)  # 只有上行状态
    try:
        rec = wf.recommend()
        assert rec["checks"]["regime_diverse"] is False
        assert rec["checks"]["regime_robust_ok"] is True
    finally:
        _stop(ctx)


def test_recommend_gate_disabled_in_config():
    cfg = {"regime_robustness": {"enabled": False, "min_regimes": 2, "min_days_per_regime": 3}}
    ctx = _apply_patches(UP, DOWN, down_beats=False, risk_cfg=cfg)
    try:
        rec = wf.recommend()
        assert rec["checks"]["regime_robust_enabled"] is False
        assert rec["checks"]["regime_robust_ok"] is True
    finally:
        _stop(ctx)
