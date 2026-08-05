"""基本面因子（quality/value/fundflow）单测：合并逻辑 + 离线缺失优雅降级。

不依赖联网：monkeypatch factor_scoring 的行情因子与 fundamental 的批量拉取。
"""
from __future__ import annotations

import smcore.strategy.factor_scoring as fs
from smcore.strategy import fundamental as fd


def _patch(monkeypatch):
    # 行情因子（价格类）固定值，避免依赖本地 k_data
    raw = {
        "A": {"mom20": 0.10, "mom60": 0.15, "rs": 0.10, "vol": 0.20, "liq": 1e8},
        "B": {"mom20": 0.05, "mom60": 0.07, "rs": 0.05, "vol": 0.30, "liq": 5e7},
        "C": {"mom20": 0.02, "mom60": 0.03, "rs": 0.02, "vol": 0.40, "liq": 2e7},
    }
    monkeypatch.setattr(fs, "_raw_factors", lambda code, as_of, window=20: dict(raw.get(str(code).strip(), {})) or None)
    monkeypatch.setattr(fs, "_index_ret20", lambda as_of, window=20: None)
    # 基本面：A 优质低估值、B 中性、C 劣质高估值
    fund = {
        "A": {"roe": 20.0, "revenue_growth": 30.0, "pe": 10.0, "pb": 1.5, "ps": 1.0, "main_inflow_20": 5e7},
        "B": {"roe": 12.0, "revenue_growth": 8.0, "pe": 25.0, "pb": 3.0, "ps": 2.0, "main_inflow_20": 1e7},
        "C": {"roe": 5.0, "revenue_growth": -10.0, "pe": 50.0, "pb": 6.0, "ps": 4.0, "main_inflow_20": -3e7},
    }
    monkeypatch.setattr(fd, "fetch_fundamentals_batch",
                        lambda codes, as_of, force=False: {str(c).strip(): fund.get(str(c).strip()) for c in codes})


def test_fundamental_quality_value_ordering(monkeypatch):
    _patch(monkeypatch)
    params = {
        "w_momentum_20": 0.0, "w_momentum_60": 0.0, "w_rel_strength": 0.0,
        "w_volatility": 0.0, "w_liquidity": 0.0,
        "use_fundamentals": True, "w_quality": 1.0, "w_value": 1.0, "w_fund_flow": 1.0,
        "scale": 1.0, "max_bonus": 100.0,
    }
    scores = fs.compute_factor_scores(["A", "B", "C"], "20260101", params)
    # A 优质+低估值+主力流入 → 最高；C 劣质+高估值+主力流出 → 最低
    assert scores["A"] > scores["B"] > scores["C"], scores
    assert scores["A"] > 0 and scores["C"] < 0, scores


def test_value_inversion_low_pe_wins(monkeypatch):
    _patch(monkeypatch)
    params = {
        "w_momentum_20": 0.0, "w_momentum_60": 0.0, "w_rel_strength": 0.0,
        "w_volatility": 0.0, "w_liquidity": 0.0,
        "use_fundamentals": True, "w_quality": 0.0, "w_value": 1.0, "w_fund_flow": 0.0,
        "scale": 1.0, "max_bonus": 100.0,
    }
    scores = fs.compute_factor_scores(["A", "B", "C"], "20260101", params)
    assert scores["A"] > scores["C"], scores  # 低 PE 的 A 估值分高于高 PE 的 C
    assert scores["A"] > 0 > scores["C"], scores


def test_offline_degrade_neutral(monkeypatch):
    """use_fundamentals=True 但无任何基本面数据 → 因子分与关闭时一致（中性降级）。"""
    raw = {
        "A": {"mom20": 0.10, "mom60": 0.15, "rs": 0.10, "vol": 0.20, "liq": 1e8},
        "B": {"mom20": 0.05, "mom60": 0.07, "rs": 0.05, "vol": 0.30, "liq": 5e7},
    }
    monkeypatch.setattr(fs, "_raw_factors", lambda code, as_of, window=20: dict(raw.get(str(code).strip(), {})) or None)
    monkeypatch.setattr(fs, "_index_ret20", lambda as_of, window=20: None)
    # 拉取返回 None（离线/失败）→ 基本面因子全缺失
    monkeypatch.setattr(fd, "fetch_fundamentals_batch", lambda codes, as_of, force=False: {str(c).strip(): None for c in codes})

    base = {
        "w_momentum_20": 1.0, "w_momentum_60": 0.7, "w_rel_strength": 0.6,
        "w_volatility": -0.4, "w_liquidity": 0.3,
        "use_fundamentals": False, "w_quality": 0.0, "w_value": 0.0, "w_fund_flow": 0.0,
        "scale": 4.0, "max_bonus": 15.0,
    }
    on = dict(base, use_fundamentals=True, w_quality=1.0, w_value=1.0, w_fund_flow=1.0)
    s_base = fs.compute_factor_scores(["A", "B"], "20260101", base)
    s_on = fs.compute_factor_scores(["A", "B"], "20260101", on)
    assert s_base == s_on, (s_base, s_on)  # 无数据 → 不影响原动量分


def test_save_risk_config_roundtrip_fundamental(monkeypatch):
    """新增基本面键能被 save_risk_config 写回（不丢字段）。"""
    import copy
    from smcore.strategy.risk_rules import (
        CONFIG as RISK_CONFIG,
        save_risk_config,
        compute_factor_scoring_params,
    )
    new_full = copy.deepcopy(RISK_CONFIG)
    fs_block = new_full.setdefault("factor_scoring", {})
    fs_block["use_fundamentals"] = True
    fs_block["w_quality"] = 0.8
    fs_block["w_value"] = 0.4
    try:
        save_risk_config(new_full)
        reread = compute_factor_scoring_params()
        assert reread["use_fundamentals"] is True
        assert reread["w_quality"] == 0.8
        assert reread["w_value"] == 0.4
    finally:
        # 还原：关闭基本面，避免污染生产配置
        reset = copy.deepcopy(RISK_CONFIG)
        rb = reset.setdefault("factor_scoring", {})
        rb["use_fundamentals"] = False
        rb["w_quality"] = 0.0
        rb["w_value"] = 0.0
        rb["w_fund_flow"] = 0.0
        save_risk_config(reset)
