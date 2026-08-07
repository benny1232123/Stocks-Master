#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""样本外纪律报告 live_forward_discipline 的单测（全程 mock，无网络）。"""
from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest

import live_forward_discipline as lfd


class _Profile:
    def __init__(self, regime):
        self.regime = regime


def _make_profile_mock(up_dates, down_dates, live="震荡轮动"):
    def fake(as_of=None):
        if as_of is None:
            return _Profile(live)
        if as_of in up_dates:
            return _Profile("趋势上行")
        if as_of in down_dates:
            return _Profile("下行防御")
        return _Profile("震荡轮动")
    return fake


def test_c1_causal_edge_ok():
    days = ["20260104", "20260105", "20260106", "20260202"]
    with mock.patch.object(lfd, "_all_signal_days", lambda: list(days)), \
         mock.patch.object(lfd, "causal_edge", lambda sd: {"theme": {"n": 3}}):
        r = lfd._check_c1_causal_edge()
    assert r["status"] == "PASS"
    assert r["pass"] is True


def test_c1_causal_edge_future_in_sample():
    # sd 不是最后一天 → 样本含更晚信号日 → WARN
    days = ["20260104", "20260105", "20260202", "20260106"]
    with mock.patch.object(lfd, "_all_signal_days", lambda: list(days)), \
         mock.patch.object(lfd, "causal_edge", lambda sd: {}):
        r = lfd._check_c1_causal_edge()
    assert r["status"] == "WARN"
    assert r["pass"] is False


def test_c2_meta_pinned_ok(tmp_path):
    sd = "20260104"
    dal = tmp_path / f"Daily-Action-List-{sd}.csv"
    dal.write_text("股票代码,综合评分\n600000,1.0\n", encoding="utf-8")
    meta = tmp_path / f"Daily-Action-List-{sd}.meta.json"
    meta.write_text('{"signal_date":"%s","regime":"趋势上行","regime_as_of":"%s","live_regime":"震荡轮动","is_today":false,"date_pinned":true}' % (sd, sd), encoding="utf-8")
    prof = _make_profile_mock(up_dates=[sd], down_dates=["20260202"], live="震荡轮动")
    with mock.patch.object(lfd, "STOCK_DATA_DIR", tmp_path), \
         mock.patch.object(lfd, "compute_market_profile", prof):
        r = lfd._check_c2_regime_pinned()
    assert r["status"] == "PASS"
    assert r["pass"] is True
    assert r["per_day"][0]["status"] == "OK"


def test_c2_meta_leak_detected(tmp_path):
    sd = "20260104"
    dal = tmp_path / f"Daily-Action-List-{sd}.csv"
    dal.write_text("股票代码,综合评分\n600000,1.0\n", encoding="utf-8")
    # 泄漏特征：历史日 meta.regime == 今日 regime（震荡轮动），但钉死regime应为 趋势上行
    meta = tmp_path / f"Daily-Action-List-{sd}.meta.json"
    meta.write_text('{"signal_date":"%s","regime":"震荡轮动","regime_as_of":"%s","live_regime":"震荡轮动","is_today":false,"date_pinned":true}' % (sd, sd), encoding="utf-8")
    prof = _make_profile_mock(up_dates=[sd], down_dates=["20260202"], live="震荡轮动")
    with mock.patch.object(lfd, "STOCK_DATA_DIR", tmp_path), \
         mock.patch.object(lfd, "compute_market_profile", prof):
        r = lfd._check_c2_regime_pinned()
    assert r["status"] == "FAIL"
    assert r["pass"] is False
    assert r["per_day"][0]["status"] == "FAIL"


def test_c2_no_meta_historical_warns(tmp_path):
    sd = "20260104"
    dal = tmp_path / f"Daily-Action-List-{sd}.csv"
    dal.write_text("股票代码,综合评分\n600000,1.0\n", encoding="utf-8")
    # 无 meta，历史日，钉死regime(趋势上行) != 今日regime(震荡轮动) → WARN
    prof = _make_profile_mock(up_dates=[sd], down_dates=["20260202"], live="震荡轮动")
    with mock.patch.object(lfd, "STOCK_DATA_DIR", tmp_path), \
         mock.patch.object(lfd, "compute_market_profile", prof):
        r = lfd._check_c2_regime_pinned()
    assert r["status"] == "WARN"
    assert r["per_day"][0]["status"] == "WARN"


def test_c3_paper_boundary_clean(tmp_path):
    # 干净 paper_tracker.py → PASS
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "paper_tracker.py").write_text(
        "import pandas as pd\nfrom walk_forward_validator import _load_dal_weights\n", encoding="utf-8")
    with mock.patch.object(lfd, "ROOT", tmp_path):
        r = lfd._check_c3_paper_boundary()
    assert r["status"] == "PASS"
    assert r["pass"] is True


def test_c3_paper_boundary_tainted(tmp_path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "paper_tracker.py").write_text(
        "from smcore.strategy.adaptive_weights import compute_target_weights\n", encoding="utf-8")
    with mock.patch.object(lfd, "ROOT", tmp_path):
        r = lfd._check_c3_paper_boundary()
    assert r["status"] == "FAIL"
    assert r["pass"] is False


def test_c4_wf_gate_robust_true():
    fake = {"robust": True, "significance": {"significant": True},
            "checks": {"regime_robust_ok": True}}
    with mock.patch("walk_forward_validator.recommend", lambda: fake):
        r = lfd._check_c4_wf_gate()
    assert r["status"] == "PASS"
    assert r["robust"] is True


def test_c4_wf_gate_robust_false():
    fake = {"robust": False, "significance": {"significant": False},
            "checks": {"regime_robust_ok": False}}
    with mock.patch("walk_forward_validator.recommend", lambda: fake):
        r = lfd._check_c4_wf_gate()
    assert r["status"] == "WARN"
    assert r["robust"] is False


def test_c5_dal_columns_clean(tmp_path):
    dal = tmp_path / "Daily-Action-List-20260104.csv"
    dal.write_text("股票代码,综合评分,建议金额\n600000,1.0,1000\n", encoding="utf-8")
    with mock.patch.object(lfd, "STOCK_DATA_DIR", tmp_path):
        r = lfd._check_c5_dal_columns()
    assert r["status"] == "PASS"
    assert r["pass"] is True


def test_c5_dal_columns_tainted(tmp_path):
    dal = tmp_path / "Daily-Action-List-20260104.csv"
    dal.write_text("股票代码,综合评分,backtest_score\n600000,1.0,0.9\n", encoding="utf-8")
    with mock.patch.object(lfd, "STOCK_DATA_DIR", tmp_path):
        r = lfd._check_c5_dal_columns()
    assert r["status"] == "FAIL"
    assert r["pass"] is False


def test_run_aggregates(tmp_path):
    days = ["20260104", "20260105", "20260106"]
    prof = _make_profile_mock(up_dates=["20260104"], down_dates=["20260202"], live="震荡轮动")
    scripts = Path("/tmp/_lfd_nonexistent_scripts_xyz")
    fake_rec = {"robust": True, "significance": {"significant": True},
                "checks": {"regime_robust_ok": True}}
    with mock.patch.object(lfd, "_all_signal_days", lambda: list(days)), \
         mock.patch.object(lfd, "causal_edge", lambda sd: {}), \
         mock.patch.object(lfd, "compute_market_profile", prof), \
         mock.patch.object(lfd, "STOCK_DATA_DIR", tmp_path), \
         mock.patch("walk_forward_validator.recommend", lambda: fake_rec):
        # ROOT 指向不存在的路径使 C3 N/A（不报错）；STOCK_DATA_DIR 为空使 C2/C5 N/A
        with mock.patch.object(lfd, "ROOT", scripts):
            res = lfd.run()
    assert "checks" in res and len(res["checks"]) == 5
    # C1(PASS) 与 C4(PASS) 应通过；无硬失败
    assert res["hard_failures"] == 0
    assert res["discipline_ok"] is True
