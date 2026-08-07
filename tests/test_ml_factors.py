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
