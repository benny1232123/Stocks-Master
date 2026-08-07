"""P1-1 组合优化层单元测试：纯函数 compute_target_weights + 仓位应用 _apply_portfolio_weights。

注：compute_target_weights 内部用 format_stock_code 把代码归一为 6 位字符串，
故测试必须用真实 6 位代码（如 600000），否则会被当作无效代码丢弃。
"""
from __future__ import annotations

import pandas as pd

from smcore.strategy.portfolio import compute_target_weights
from smcore.strategy.position_sizing import _apply_portfolio_weights


def test_equal_weight_sums_to_one_and_uniform():
    codes = ["600000", "600036", "000001"]
    w = compute_target_weights(codes, {}, {}, method="equal_weight")
    assert abs(sum(w.values()) - 1.0) < 1e-9
    assert all(abs(v - 1 / 3) < 1e-9 for v in w.values())


def test_score_weighted_ranks_correctly():
    codes = ["600000", "600036", "000001"]
    scores = {"600000": 10.0, "600036": 5.0, "000001": 1.0}
    w = compute_target_weights(codes, scores, {}, method="score_weighted", score_power=1.5)
    assert w["600000"] > w["600036"] > w["000001"]
    assert abs(sum(w.values()) - 1.0) < 1e-9


def test_score_weighted_handles_negative_and_equal():
    codes = ["600000", "600036", "000001"]
    # 全相等 → 等权
    w = compute_target_weights(codes, {"600000": 3.0, "600036": 3.0, "000001": 3.0}, {}, method="score_weighted")
    assert abs(sum(w.values()) - 1.0) < 1e-9
    assert all(abs(v - 1 / 3) < 1e-9 for v in w.values())
    # 含负分 → 不抛错，排序仍正确
    w2 = compute_target_weights(codes, {"600000": -5.0, "600036": 0.0, "000001": 5.0}, {}, method="score_weighted")
    assert abs(sum(w2.values()) - 1.0) < 1e-9
    assert w2["000001"] > w2["600000"]


def test_risk_parity_low_vol_gets_more():
    codes = ["600000", "600036", "000001"]
    vols = {"600000": 0.10, "600036": 0.30, "000001": 0.50}
    w = compute_target_weights(codes, {}, vols, method="risk_parity_erc")
    assert w["600000"] > w["600036"] > w["000001"]
    assert abs(sum(w.values()) - 1.0) < 1e-9
    # 对角 ERC：w_i ∝ 1/vol_i → 比例应等于波动倒数比
    assert abs(w["600000"] / w["600036"] - (0.30 / 0.10)) < 1e-6
    assert abs(w["600000"] / w["000001"] - (0.50 / 0.10)) < 1e-6


def test_risk_parity_missing_vols_falls_back_to_equal():
    codes = ["600000", "600036", "000001"]
    w = compute_target_weights(codes, {}, {}, method="risk_parity_erc")
    assert abs(sum(w.values()) - 1.0) < 1e-9
    assert all(abs(v - 1 / 3) < 1e-9 for v in w.values())


def test_empty_input_returns_empty():
    assert compute_target_weights([], {}, {}, method="score_weighted") == {}


def test_unknown_method_falls_back_to_equal():
    codes = ["600000", "600036"]
    w = compute_target_weights(codes, {"600000": 9, "600036": 1}, {}, method="bogus_method")
    assert abs(sum(w.values()) - 1.0) < 1e-9


def test_apply_portfolio_weights_caps_and_sets_columns():
    df = pd.DataFrame({
        "股票代码": ["600000", "600036", "000001"],
        "建议买入价": [10.0, 20.0, 5.0],
    })
    raw = {"600000": 0.5, "600036": 0.3, "000001": 0.2}
    out, n_hit = _apply_portfolio_weights(
        df, raw, total_capital=100000.0, max_single_weight_frac=0.10, apply_vol_tilt=False
    )
    assert "建议仓位%" in out.columns and "建议金额" in out.columns
    # 三档原始权重 0.5/0.3/0.2 均超过单名上限 0.10 → 全部截断为 10%，n_hit=3
    assert out.loc[out["股票代码"] == "600000", "建议仓位%"].iloc[0] == 10.0
    assert n_hit == 3
    # 金额按手数取整且 > 0
    assert (out["建议金额"] > 0).all()
