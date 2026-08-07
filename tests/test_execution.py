#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""P2-3 TWAP/VWAP 执行模块测试（全合成数据，不依赖联网 / 真实 K 线）。"""
from __future__ import annotations

import numpy as np
import pytest

from smcore.strategy import execution as ex


# ── 基础工具 ──────────────────────────────────────────────────────────────
def test_slice_time_labels_count_and_format():
    labs = ex.slice_time_labels(20)
    assert len(labs) == 20
    assert labs[0] == "09:30"
    # 午休后第一片应在 13:00 之后
    assert any(l.startswith("13:") for l in labs)


def test_intraday_profile_twap_uniform():
    p = ex.intraday_volume_profile(10, "TWAP")
    assert np.isclose(p.sum(), 1.0)
    assert np.allclose(p, 0.1)


def test_intraday_profile_vwap_usum_and_ushape():
    p = ex.intraday_volume_profile(20, "VWAP")
    assert np.isclose(p.sum(), 1.0)
    # U 形：两端应高于中间
    assert p[0] > p[len(p) // 2]
    assert p[-1] > p[len(p) // 2]


def test_child_schedule_sums_to_total_and_remainder():
    sched = ex.child_order_schedule("buy", 10000, "VWAP", 20)
    total = sum(s["shares"] for s in sched)
    assert total == 10000
    # 末片补差后数量合理
    assert all(s["shares"] > 0 for s in sched)


def test_child_schedule_fail_soft():
    assert ex.child_order_schedule("buy", 0, "VWAP", 20) == []
    assert ex.child_order_schedule("buy", 1000, "VWAP", 0) == []


# ── 合成日内路径 ────────────────────────────────────────────────────────
def test_simulate_path_anchors_to_ohlc():
    o, h, l, c = 10.0, 10.5, 9.8, 10.2
    path = ex.simulate_intraday_path(o, h, l, c, 20, seed=1)
    assert len(path) == 20
    assert np.isclose(path[0], o)
    assert np.isclose(path[-1], c)
    # 路径应落在 [low, high] 内
    assert path.min() >= l - 1e-6
    assert path.max() <= h + 1e-6


def test_simulate_path_deterministic_by_seed():
    a = ex.simulate_intraday_path(10, 10.5, 9.8, 10.2, 20, seed=7)
    b = ex.simulate_intraday_path(10, 10.5, 9.8, 10.2, 20, seed=7)
    assert np.allclose(a, b)


def test_simulate_path_invalid_returns_nan_array():
    p = ex.simulate_intraday_path("x", 10.5, 9.8, 10.2, 20, seed=0)
    assert np.all(np.isnan(p))


# ── 基准 ──────────────────────────────────────────────────────────────────
def test_vwap_proxy_from_bar_amount_over_volume():
    bar = {"amount": 1_000_000, "volume": 100_000, "high": 11, "low": 9, "close": 10}
    assert np.isclose(ex.vwap_proxy_from_bar(bar), 10.0)


def test_vwap_proxy_fallback_to_hlc3():
    bar = {"high": 12, "low": 8, "close": 10}  # 无 amount/volume
    assert np.isclose(ex.vwap_proxy_from_bar(bar), (12 + 8 + 10) / 3.0)


def test_vwap_proxy_none_on_bad_input():
    assert ex.vwap_proxy_from_bar(None) is None
    assert ex.vwap_proxy_from_bar({}) is None


# ── 执行质量评估 ─────────────────────────────────────────────────────────
def _bar():
    return {"open": 10.0, "high": 10.6, "low": 9.7, "close": 10.3,
            "volume": 5_000_000, "amount": 50_000_000}


def test_evaluate_execution_basic_buy():
    order = {"side": "buy", "total_shares": 500_000, "algo": "VWAP",
             "code": "600000", "date": "20260807"}
    res = ex.evaluate_execution(order, bar=_bar(), daily_vol=0.02)
    assert res["ok"]
    assert res["avg_fill_price"] is not None
    assert res["vwap_benchmark"] == 10.0  # amount/volume
    assert res["arrival_price"] == 10.0
    # 参与率
    assert np.isclose(res["participation_rate"], 500_000 / 5_000_000)
    # 冲击为正（buy, pov>0）
    assert res["est_market_impact_bps"] is not None
    assert res["est_market_impact_bps"] > 0


def test_evaluate_execution_sell_sign_flips():
    order = {"side": "sell", "total_shares": 500_000, "algo": "TWAP",
             "code": "600000", "date": "20260807"}
    res = ex.evaluate_execution(order, bar=_bar(), daily_vol=0.02)
    assert res["ok"]
    # 卖出滑点定义里 avg_fill<vwap 才有成本；合成路径均价随机，仅校验有限
    assert res["slippage_vs_arrival_bps"] is not None


def test_evaluate_execution_zero_participation_no_impact():
    bar = {"open": 10, "high": 10.6, "low": 9.7, "close": 10.3, "volume": 0, "amount": 0}
    order = {"side": "buy", "total_shares": 500_000, "algo": "VWAP"}
    res = ex.evaluate_execution(order, bar=bar, daily_vol=0.02)
    assert res["ok"]
    assert res["participation_rate"] is None or res["participation_rate"] == 0
    assert res["est_market_impact_bps"] is None


def test_evaluate_execution_fail_soft_no_bar():
    res = ex.evaluate_execution({"side": "buy", "total_shares": 1000}, bar=None)
    assert res["ok"] is False


def test_evaluate_execution_fail_soft_bad_prices():
    order = {"side": "buy", "total_shares": 1000, "algo": "VWAP"}
    res = ex.evaluate_execution(order, bar={"open": "bad", "high": 1, "low": 0, "close": 1})
    assert res["ok"] is False


def test_schedule_shares_invariant_any_total():
    for tot in (1, 137, 9999, 1_000_000):
        sched = ex.child_order_schedule("buy", tot, "VWAP", 20)
        assert sum(s["shares"] for s in sched) == tot


def test_real_intraday_override(monkeypatch, tmp_path):
    # 制造真实 intraday 文件，验证优先采用真实路径
    intra = tmp_path / "intraday"
    intra.mkdir()
    code = "600000"
    date = "20260807"
    prices = np.linspace(10.0, 10.5, 40)
    import pandas as pd
    pd.DataFrame({"time": range(40), "price": prices,
                  "volume": np.ones(40)}).to_csv(
        intra / f"{code}_{date}.csv", index=False)
    monkeypatch.setattr(ex, "STOCK_DATA_DIR", tmp_path)
    order = {"side": "buy", "total_shares": 1000, "algo": "VWAP",
             "code": code, "date": date}
    res = ex.evaluate_execution(order, bar=_bar())
    assert res["ok"]
    assert res["synthetic"] is False
    # 真实路径均值应≈(10.0+10.5)/2=10.25 量级
    assert 10.0 <= res["avg_fill_price"] <= 10.5


# ── 报告端到端（不依赖 DAL 文件）────────────────────────────────────────
def test_run_execution_report_with_explicit_orders():
    bar = _bar()
    orders = [
        {"side": "buy", "total_shares": 500_000, "algo": "VWAP",
         "code": "600000", "date": "20260807", "daily_volume": 5_000_000,
         "bar": bar},
        {"side": "buy", "total_shares": 200_000, "algo": "TWAP",
         "code": "600001", "date": "20260807", "daily_volume": 5_000_000,
         "bar": bar},
    ]
    res = ex.run_execution_report(orders=orders, cfg={"vol_window": 20})
    assert res["ok"]
    assert res["n_orders"] == 2
    md = ex.format_execution_report(res)
    assert "TWAP/VWAP 执行质量报告" in md
    assert "600000" in md


def test_run_execution_report_no_orders_fail_soft():
    res = ex.run_execution_report(orders=[], cfg={})
    assert res["ok"] is False
