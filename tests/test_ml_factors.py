#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""ML 因子挖掘单测（合成数据，monkeypatch 因子暴露与前向收益）。"""
from __future__ import annotations

import numpy as np
import pandas as pd
from contextlib import contextmanager
from unittest import mock

import smcore.strategy.ml_factors as ml


N_DAYS = 80
N_CODES = 20
FEAT = 5
_rng = np.random.default_rng(0)
_true_w = np.array([0.6, -0.4, 0.3, 0.2, -0.5])
_X = _rng.normal(0, 1, (N_DAYS, N_CODES, FEAT))
_Y = _X @ _true_w + _rng.normal(0, 0.03, (N_DAYS, N_CODES))
CODES = [f"{600000 + i:06d}" for i in range(N_CODES)]
DAYS = [f"2026{d:04d}" for d in range(1, N_DAYS + 1)]


def fake_exposures(codes, as_of):
    d = DAYS.index(as_of)
    return pd.DataFrame(_X[d], index=codes, columns=ml.rm.STYLE_FACTORS)


def fake_fwd(codes_list, as_of, horizon=10):
    d = DAYS.index(as_of)
    return {c: float(_Y[d, CODES.index(c)]) for c in codes_list}


@contextmanager
def _patch():
    with mock.patch.object(ml.rm, "compute_exposures", fake_exposures), \
         mock.patch.object(ml, "forward_returns", fake_fwd):
        yield


def test_spearman_ic_perfect():
    pred = np.array([1.0, 2.0, 3.0, 4.0])
    y = np.array([2.0, 5.0, 8.0, 11.0])  # 完全正相关
    ic = ml._spearman_ic(pred, y)
    assert ic > 0.99


def test_spearman_ic_anti():
    pred = np.array([1.0, 2.0, 3.0, 4.0])
    y = np.array([4.0, 3.0, 2.0, 1.0])
    ic = ml._spearman_ic(pred, y)
    assert ic < -0.99


def test_walk_forward_insufficient():
    # 仅 10 个信号日 → 远低于 min_train_days(60)
    short_days = DAYS[:10]
    with _patch():
        res = ml.walk_forward_ml(short_days, CODES, {})
    assert res["ok"] is False
    assert res["reason"] == "insufficient_signal_days"


def test_walk_forward_activates_with_signal():
    with _patch():
        res = ml.walk_forward_ml(DAYS, CODES, {})
    assert res["ok"] is True
    assert res["n_folds"] >= 10
    assert res["mean_ic"] > 0.02
    gate = ml.evaluate_ml_gate(res, {})
    assert gate["activate"] is True


def test_gate_low_thresholds_can_fail():
    # 阈值设为不可能达成（Rank-IC 上限=1.0），确保即便信号很强也不激活
    cfg = {"min_ic": 2.0, "min_ir": 1000.0, "min_positive_frac": 2.0}
    with _patch():
        res = ml.walk_forward_ml(DAYS, CODES, {})
    gate = ml.evaluate_ml_gate(res, cfg)
    assert gate["activate"] is False


def test_run_report_insufficient():
    with _patch():
        res = ml.run_ml_factor_report(DAYS[:10], CODES, {})
    assert res["ok"] is False
    assert "gate" in res


def test_run_report_with_signal():
    with _patch():
        res = ml.run_ml_factor_report(DAYS, CODES, {})
    assert res["ok"] is True
    md = ml.format_ml_report(res)
    assert "ML 因子挖掘" in md


def test_purged_fold_keeps_semantics():
    # 折划分：连续块覆盖全样本；训练折不含验证块本体、
    # 不含 label 窗口（horizon）触及验证块的样本、不含块后 embargo 内样本
    n, k, horizon, embargo = 60, 5, 10, 2
    folds = ml._purged_fold_keeps(n, k, horizon, embargo)
    assert len(folds) == k
    covered = []
    for lo, hi, keep in folds:
        assert 0 <= lo < hi <= n
        covered.append((lo, hi))
        ks = set(keep)
        for t in range(lo, hi):
            assert t not in ks          # 验证块本体不进训练
        for t in ks:
            if t < lo:                  # 左侧：label 窗口必须完全早于验证块
                assert t + horizon < lo
            else:                       # 右侧：必须晚于 embargo 区
                assert t > hi - 1 + embargo
    # 连续块首尾相接覆盖 [0, n)
    assert covered[0][0] == 0 and covered[-1][1] == n
    for (a_lo, a_hi), (b_lo, _b_hi) in zip(covered, covered[1:]):
        assert a_hi == b_lo


def test_purged_predict_close_to_random_cv_on_clean_signal():
    # 合成数据（无泄漏可利用）：purged CV 的样本外 IC 仍应为强正
    with _patch():
        res = ml.walk_forward_ml(DAYS, CODES, {})
    assert res["ok"] is True
    assert res["mean_ic"] > 0.02


def test_purged_predict_alpha_fallback_when_no_folds():
    # 折全被剔除（极端 horizon）→ 回退中位 alpha，不抛异常
    train = [("d", np.random.default_rng(i).normal(size=(6, 3)),
              np.random.default_rng(i).normal(size=6)) for i in range(6)]
    Xte = np.random.default_rng(99).normal(size=(4, 3))
    pred = ml._purged_ridge_predict(train, Xte, [0.1, 1.0, 10.0],
                                    cv_folds=5, embargo_days=2, horizon=100)
    assert pred.shape == (4,)


# ── 融合接入门控（2026-09-27）─────────────────────────────────────────

def test_walk_forward_returns_fold_records():
    with _patch():
        res = ml.walk_forward_ml(DAYS, CODES, {})
    assert res["ok"] is True
    fr = res["fold_records"]
    assert len(fr) == res["n_folds"] == len(res["ics"])
    sds = [sd for sd, _ in fr]
    assert sds == sorted(sds)                     # 测试日升序（因果过滤依赖）
    assert all(isinstance(ic, float) and ic == ic for _, ic in fr)


def _synth_report(with_decay: bool, gate_activate: bool = True) -> dict:
    """构造 walk-forward 报告：前 30 折 IC=+0.3；with_decay 时后 20 折 IC=-0.1。"""
    fr, base = [], pd.Timestamp("2026-01-01")
    for i in range(30):
        fr.append([(base + pd.Timedelta(days=i)).strftime("%Y%m%d"), 0.3])
    if with_decay:
        for i in range(30, 50):
            fr.append([(base + pd.Timedelta(days=i)).strftime("%Y%m%d"), -0.1])
    return {"ok": True, "gate": {"activate": gate_activate}, "fold_records": fr}


def test_fuse_gate_requires_research_gate():
    g = ml.evaluate_fuse_gate(_synth_report(with_decay=False, gate_activate=False), {})
    assert g["allow"] is False
    assert g["checks"]["research_gate"] is False


def test_fuse_gate_recent_window_blocks_decay():
    g = ml.evaluate_fuse_gate(_synth_report(with_decay=True), {})
    assert g["allow"] is False                     # 近窗 20 折全负 → 衰减监控拦截
    assert g["checks"]["recent_ic_ok"] is False
    assert g["recent_ic"] < 0


def test_fuse_gate_causal_asof_ignores_future_folds():
    # 回放历史日时，「label 尚未实现」的折不可见：须 as_of − test_sd ≥ purge 窗
    # （默认 horizon=10 → purge 17 日历日）。as_of=第 30 折观测日 → 只能看到前 13 折。
    asof = (pd.Timestamp("2026-01-01") + pd.Timedelta(days=29)).strftime("%Y%m%d")
    g = ml.evaluate_fuse_gate(_synth_report(with_decay=True), {}, as_of=asof)
    assert g["allow"] is True                      # 可见折全为 +0.3 → 当时门控放行
    assert g["n_folds_asof"] == 13
    assert g["recent_ic"] > 0


def test_fuse_gate_insufficient_recent_folds():
    report = _synth_report(with_decay=False)
    report["fold_records"] = report["fold_records"][:3]
    g = ml.evaluate_fuse_gate(report, {})
    assert g["allow"] is False
    assert g["checks"]["enough_recent_folds"] is False


def test_ml_scores_for_date_synthetic():
    with _patch():
        z = ml.ml_scores_for_date(DAYS[-1], CODES, {"min_train_days": 8},
                                  signal_days=DAYS)
    assert z is not None
    assert set(z.keys()) == set(CODES)
    vals = np.array(list(z.values()))
    assert abs(vals.mean()) < 1e-6 and abs(vals.std() - 1.0) < 1e-6  # 截面 z-score


def test_ml_scores_for_date_insufficient_history():
    with _patch():
        z = ml.ml_scores_for_date(DAYS[5], CODES, {"min_train_days": 8},
                                  signal_days=DAYS)
    assert z is None                               # 训练日不足 → 中性


def test_fuse_ml_bonus_gate_not_activated():
    with mock.patch.object(ml, "_load_wf_report",
                           return_value=_synth_report(with_decay=False, gate_activate=False)):
        bonus, note = ml.fuse_ml_bonus(CODES, DAYS[-1], {})
    assert bonus == {} and "门控" in note


def test_fuse_ml_bonus_neutral_without_report():
    with mock.patch.object(ml, "_load_wf_report", return_value=None):
        bonus, note = ml.fuse_ml_bonus(CODES, DAYS[-1], {})
    assert bonus == {} and "中性" in note


def test_fuse_ml_bonus_full_path_clamps():
    with mock.patch.object(ml, "_load_wf_report",
                           return_value=_synth_report(with_decay=False)), \
         mock.patch.object(ml, "ml_scores_for_date",
                           return_value={CODES[0]: 5.0, CODES[1]: -5.0, CODES[2]: 0.0}):
        bonus, note = ml.fuse_ml_bonus(CODES[:3], DAYS[-1],
                                       {"weight": 3.0, "max_bonus": 10.0})
    assert bonus[CODES[0]] == 10.0                 # 3×5=15 → clamp 上限
    assert bonus[CODES[1]] == -10.0
    assert bonus[CODES[2]] == 0.0
    assert "已接入" in note
