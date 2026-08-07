#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Barra 风格风险模型单测（合成数据，离线，monkeypatch 数据目录）。"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import smcore.strategy.risk_model as rm


CODES = ["600000", "600001", "600002", "600003", "600004"]


def _make_kdata(path: Path, code_idx: int, n: int = 80, seed: int = 0):
    rng = np.random.default_rng(seed * 13 + code_idx)
    dates = pd.date_range("2026-01-01", periods=n, freq="D")
    # 让不同 code 有不同的趋势/波动，使因子结构可估
    drift = 0.0005 * (code_idx - 2)
    vol = 0.01 + 0.004 * (code_idx % 3)
    rets = rng.normal(drift, vol, n)
    close = 10.0 * np.cumprod(1 + rets)
    amount = (1e8 + 5e7 * code_idx) * (1 + rng.normal(0, 0.1, n))  # 流动性差异化
    df = pd.DataFrame({
        "date": dates.strftime("%Y-%m-%d"),
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": amount / close,
        "amount": amount,
    })
    path.write_text(df.to_csv(index=False), encoding="utf-8")


def _make_fundamental(path: Path, code_idx: int):
    # 规模/估值差异化
    mkt_cap = 500.0 + 200.0 * code_idx
    pe = 8.0 + 4.0 * code_idx      # 高 code → 高 PE → 低价值
    pb = 0.8 + 0.3 * code_idx
    path.write_text(f'{{"代码":"{CODES[code_idx]}","pe":{pe},"pb":{pb},"mkt_cap":{mkt_cap}}}',
                    encoding="utf-8")


@pytest.fixture
def fake_data(tmp_path):
    kd = tmp_path / "k_data"
    kd.mkdir()
    fc = tmp_path / "fundamental_cache"
    fc.mkdir()
    rng = np.random.default_rng(42)
    for i, c in enumerate(CODES):
        _make_kdata(kd / f"{c}_qfq_full.csv", i, seed=rng.integers(0, 1000))
        _make_fundamental(fc / f"{c}.json", i)
    return tmp_path


def test_compute_exposures(fake_data):
    import smcore.strategy.risk_model as rm
    rm.STOCK_DATA_DIR = fake_data
    expo = rm.compute_exposures(CODES)
    assert list(expo.columns) == rm.STYLE_FACTORS
    assert expo.shape[0] == len(CODES)
    # 截面 z 化后每列均值≈0，且非全零
    for f in rm.STYLE_FACTORS:
        assert abs(expo[f].mean()) < 1e-9
    assert expo[f].std() > 0


def test_value_factor_low_pe_high_exposure(fake_data):
    rm.STOCK_DATA_DIR = fake_data
    expo = rm.compute_exposures(CODES)
    # code 600000 的 PE 最低(8) → value 暴露应最高（最大）
    assert expo.loc["600000", "value"] == expo["value"].max()


def test_estimate_risk_model(fake_data):
    rm.STOCK_DATA_DIR = fake_data
    model = rm.estimate_risk_model(CODES, window=60)
    assert model is not None
    assert model["factors"] == rm.STYLE_FACTORS
    assert model["F"].shape == (5, 5)
    # 协方差矩阵对称
    assert np.allclose(model["F"], model["F"].T)
    assert len(model["specific_var"]) >= 3
    assert model["n_days"] >= 15


def test_portfolio_risk(fake_data):
    rm.STOCK_DATA_DIR = fake_data
    model = rm.estimate_risk_model(CODES, window=60)
    weights = {c: 1.0 for c in CODES}
    risk = rm.portfolio_risk(weights, model)
    assert risk["ok"] is True
    assert risk["pred_vol_pct"] is not None and risk["pred_vol_pct"] > 0
    assert risk["n_factors"] == 5
    # 因子风险贡献合计 = 因子风险占比（factor_share），且风险占比之和≈100%
    total = sum(risk["risk_contrib"].values())
    assert abs(total * 100.0 - risk["factor_share_pct"]) < 1e-6
    assert abs(risk["factor_share_pct"] + risk["specific_share_pct"] - 100.0) < 1e-6


def test_portfolio_risk_failsafe_empty(fake_data):
    rm.STOCK_DATA_DIR = fake_data
    risk = rm.portfolio_risk({}, {"F": np.eye(5), "factors": rm.STYLE_FACTORS, "specific_var": {}})
    assert risk["ok"] is False


def test_estimate_insufficient_data(tmp_path):
    rm.STOCK_DATA_DIR = tmp_path  # 空目录
    assert rm.estimate_risk_model(CODES, window=60) is None


def test_run_report_with_codes(fake_data):
    rm.STOCK_DATA_DIR = fake_data
    res = rm.run_risk_model_report(codes=CODES, weights={c: 1.0 for c in CODES}, window=60)
    assert res["ok"] is True
    assert "risk" in res
    md = rm.format_risk_report(res)
    assert "Barra" in md and "预测组合年化波动" in md


def test_run_report_no_data(tmp_path):
    rm.STOCK_DATA_DIR = tmp_path
    res = rm.run_risk_model_report(codes=CODES)
    assert res["ok"] is False
