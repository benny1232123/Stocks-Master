"""自适应权重 + 现金曲线的纯函数单元测试（无网络依赖）。"""
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from smcore.strategy.adaptive_weights import (
    ALL_STRATEGIES,
    CONFIG,
    adaptive_weights,
    cash_from_regime,
    cash_from_volatility,
)

FLOOR = CONFIG["FLOOR"]  # 与 adaptive_weights_config.json 一致（默认 3.0）


def _edge_map(values):
    """values: dict[str, (edge, n)] -> 与 compute_strategy_edge 返回结构一致。"""
    return {
        s: {"edge": v[0], "n": v[1], "win_rate": 50, "avg": v[0]}
        for s, v in values.items()
    }


def test_adaptive_weights_sum_to_100():
    edge = _edge_map({s: (1.0, 30) for s in ALL_STRATEGIES})
    w = adaptive_weights(edge)
    assert set(w) == set(ALL_STRATEGIES)
    assert sum(w.values()) == 100


def test_adaptive_weights_floor_keeps_no_strategy_zero():
    """负 edge + 低样本的策略不应被归零（历史坑：CCTV 归零导致单票爆雷）。"""
    edge = _edge_map(
        {
            "boll": (-2.0, 1),
            "theme": (3.0, 30),
            "relativity": (-5.0, 2),
            "momentum": (0.5, 20),
            "cctv": (-3.0, 1),
        }
    )
    w = adaptive_weights(edge)
    assert sum(w.values()) == 100
    for s, v in w.items():
        assert v >= FLOOR, f"{s}={v} 低于地板 {FLOOR}"


def test_bayesian_shrinkage_low_sample_not_dominant():
    """1 笔 +7% 的低样本 edge 不应碾压 50 笔 +1% 的高样本 edge。"""
    edge = _edge_map({"boll": (7.0, 1), "theme": (1.0, 50)})
    w = adaptive_weights(edge)
    assert w["boll"] < 70
    assert w["theme"] > w["boll"]


def test_cash_from_volatility_bounds_and_monotonic():
    assert cash_from_volatility(None) == 0
    # 平滑 S 曲线：极低波动现金接近 0（不严格为 0，但 <5）
    assert cash_from_volatility(0.1) < 5
    lo = cash_from_volatility(0.5)
    hi = cash_from_volatility(0.85)
    assert 0 < lo < hi, f"lo={lo} hi={hi}"  # 单调上升
    assert hi >= 35
    assert cash_from_volatility(1.0) <= 50  # 封顶 ~50


def test_cash_from_volatility_full_range_valid():
    for p in [0.0, 0.2, 0.5, 0.8, 1.0]:
        c = cash_from_volatility(p)
        assert 0 <= c <= 50


def test_cash_from_regime_downward_defense():
    assert cash_from_regime("下行防御", 10) == min(max(20, 20), 70)
    assert cash_from_regime("下行防御", 40) == min(max(80, 20), 70)  # 封顶 70


def test_cash_from_regime_uptrend():
    up = CONFIG["cash_from_regime"]["up_mult"]
    assert cash_from_regime("趋势上行", 30) == max(int(30 * up), 0)
    assert cash_from_regime("趋势上行", 3) == max(int(3 * up), 0)


def test_cash_from_regime_neutral_passthrough():
    assert cash_from_regime("震荡轮动", 25) == 25
    assert cash_from_regime(None, 25) == 25
