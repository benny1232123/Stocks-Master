"""Dynamic risk engine unit tests: single continuous monotonic parameter source.

中性点 = 现值（与 risk_rules.compute_adaptive_exit_params / compute_adaptive_risk_params /
adaptive_weights 的 cash_from_* 手调链逐值一致）；仅在「下行防御 + 高波动」双信号下
对 capital_scale 施加连续防御下线。
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from smcore.strategy.dynamic_risk import DynamicParams, compute_dynamic_risk
from smcore.strategy import adaptive_weights as aw
from smcore.strategy.risk_rules import (
    compute_adaptive_exit_params,
    compute_adaptive_risk_params,
)


def _profile(regime: str, vol_pctile: float):
    return type("MP", (), {"regime": regime, "volatility_pctile": vol_pctile})()


def _manual_chain(profile, regime, drawdown_pct, n_picks=None, n_sectors=None):
    """手调现有函数链（= 接入前的实际行为），作为中性基准。"""
    exit_p = compute_adaptive_exit_params(profile, regime)
    risk_p = compute_adaptive_risk_params(regime, profile, n_picks=n_picks, n_sectors=n_sectors)
    vol = getattr(profile, "volatility_pctile", None) if profile is not None else None
    cash = aw.cash_from_volatility(vol)
    cash = aw.cash_from_regime(regime, cash)
    if drawdown_pct:
        cash = min(100, cash + aw.cash_from_drawdown(drawdown_pct))
    return exit_p, risk_p, round(cash, 1), round(max(0.0, 1.0 - cash / 100.0), 3)


def test_neutral_no_profile_matches_manual_chain():
    """无市场信息：与现有手调链完全一致（现金 0、仓位 1.0、出场=中性值）。"""
    dr = compute_dynamic_risk()
    exit_p, risk_p, cash, scale = _manual_chain(None, None, None)
    assert dr.cash_pct == cash == 0.0
    assert dr.capital_scale == scale == 1.0
    assert dr.stop_loss_pct == exit_p["stop_loss_pct"]
    assert dr.take_profit_pct == exit_p["take_profit_pct"]
    assert dr.trailing_stop_pct == exit_p["trailing_stop_pct"]
    assert dr.trend_exit_ma == exit_p["trend_exit_ma"]
    assert dr.hold_days == exit_p["hold_days"]
    assert dr.slippage == exit_p["slippage"]
    assert dr.max_single_weight_pct == risk_p["max_single_weight_pct"]
    assert dr.max_portfolio_beta == risk_p["max_portfolio_beta"]


def test_neutral_oscillating_mid_vol_matches_manual_chain():
    """震荡轮动 + 中位波动：动态引擎与手调链一致（cash≈18%，scale≈0.82）。"""
    p = _profile("震荡轮动", 0.5)
    dr = compute_dynamic_risk(profile=p, regime="震荡轮动", n_picks=30, n_sectors=10)
    exit_p, risk_p, cash, scale = _manual_chain(p, "震荡轮动", None)
    assert dr.cash_pct == cash
    assert dr.capital_scale == scale
    assert dr.stop_loss_pct == exit_p["stop_loss_pct"]


def test_drawdown_appends_cash():
    """回撤熔断在波动率/regime 现金之上只追加不减少。"""
    p = _profile("震荡轮动", 0.5)
    dr0 = compute_dynamic_risk(profile=p, regime="震荡轮动")
    dr = compute_dynamic_risk(profile=p, regime="震荡轮动", drawdown_pct=12.0)
    assert dr.cash_pct > dr0.cash_pct
    assert dr.capital_scale < dr0.capital_scale


def test_defensive_high_vol_caps_scale_hard():
    """下行防御 + vol>=0.7 → capital_scale <= 0.3（双信号强制下线）。"""
    p = _profile("下行防御", 0.85)
    dr = compute_dynamic_risk(profile=p, regime="下行防御")
    assert dr.capital_scale <= 0.3


def test_defensive_high_vol_exact_hard_floor():
    """vol>=0.7 时精确到 0.3 地板，且不只靠 cash 达到。"""
    p = _profile("下行防御", 0.9)
    dr = compute_dynamic_risk(profile=p, regime="下行防御")
    assert dr.capital_scale == pytest.approx(0.3, abs=1e-6)


def test_defensive_low_vol_not_capped():
    """下行防御但温和波动（<0.60）→ 不触发钳制，scale 只由现值现金链决定。"""
    p = _profile("下行防御", 0.45)
    dr = compute_dynamic_risk(profile=p, regime="下行防御")
    _, _, cash, scale = _manual_chain(p, "下行防御", None)
    assert dr.capital_scale == pytest.approx(scale, abs=1e-9)
    assert dr.capital_scale >= 0.5


def test_scale_monotonic_in_volatility():
    """同一 regime 下，volatile 上升 → capital_scale 单调不减（连续无跳变）。"""
    scales = []
    for v in (0.55, 0.60, 0.65, 0.70, 0.75, 0.85, 0.95):
        p = _profile("下行防御", v)
        dr = compute_dynamic_risk(profile=p, regime="下行防御")
        scales.append(dr.capital_scale)
    assert scales == sorted(scales, reverse=True)


def test_uptrend_extreme_vol_capped_hard():
    """趋势上行 + vol>=0.85 → 无条件硬地板 0.3（防 regime 误判满仓扛亏）。"""
    p = _profile("趋势上行", 0.9)
    dr = compute_dynamic_risk(profile=p, regime="趋势上行")
    assert dr.capital_scale == pytest.approx(0.3, abs=1e-6)


def test_uptrend_mid_vol_soft_cap():
    """趋势上行 + 0.75<=vol<0.85 → 缓坡钳制（不直接 0.3，保留上行弹性）。"""
    p = _profile("趋势上行", 0.8)
    dr = compute_dynamic_risk(profile=p, regime="趋势上行")
    assert dr.capital_scale <= 0.45 + 1e-9
    cap = 0.45 + (0.3 - 0.45) * (0.8 - 0.75) / (0.85 - 0.75)
    assert dr.capital_scale <= cap + 1e-9


def test_high_vol_tightens_stop_loss():
    """vol>=0.85 止损收紧到 <=8%；0.70~0.85 → <=10%（阻止高波放宽止损）。"""
    p = _profile("趋势上行", 0.9)
    dr = compute_dynamic_risk(profile=p, regime="趋势上行")
    assert dr.stop_loss_pct <= 0.08 + 1e-9

    p2 = _profile("震荡轮动", 0.75)
    dr2 = compute_dynamic_risk(profile=p2, regime="震荡轮动")
    assert dr2.stop_loss_pct <= 0.10 + 1e-9


def test_low_vol_stop_loss_keeps_manual_chain():
    """vol<0.70：止损不受收紧规则影响，与现值手调链一致。"""
    p = _profile("震荡轮动", 0.5)
    dr = compute_dynamic_risk(profile=p, regime="震荡轮动")
    exit_p, _, _, _ = _manual_chain(p, "震荡轮动", None)
    assert dr.stop_loss_pct == pytest.approx(exit_p["stop_loss_pct"], abs=1e-9)


def test_all_params_are_in_dataclass():
    """DynamicParams 覆盖出场 + 仓位上限 + 现金/规模全字段。"""
    import dataclasses

    names = {f.name for f in dataclasses.fields(DynamicParams)}
    for k in (
        "stop_loss_pct", "take_profit_pct", "trailing_stop_pct", "trend_exit_ma",
        "hold_days", "slippage", "max_single_weight_pct", "max_sector_weight_pct",
        "max_per_sector", "max_portfolio_beta", "beta_min_keep", "max_per_strategy",
        "dd_full", "dd_cash_ceiling", "cash_pct", "capital_scale",
    ):
        assert k in names