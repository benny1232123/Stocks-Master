"""自适应风险层契约测试。

核心契约：所有风控/出场数值都由 risk_config.json（可热更新）驱动，代码内**零硬编码**。
因此：
  - 数值必须随名单广度(N)/行业数(M)/regime/波动率分位变化；
  - 数值必须被 floor/ceil 约束；
  - 修改 CONFIG 必须直接反映到 compute_* 输出（证明无魔法数字绕过配置）。
LOT_SIZE=100（交易所硬规则）与 defaults 派生的「绝对安全天花板」不在本测试范围——
后者是结构性兜底，取值同样来自 CONFIG。
"""
from __future__ import annotations

import smcore.strategy.risk_rules as rr
from smcore.config import defaults as d


class _FakeProfile:
    def __init__(self, vol=None, regime=None):
        self.volatility_pctile = vol
        self.regime = regime


def test_risk_params_vary_with_breadth():
    narrow = rr.compute_adaptive_risk_params(regime="震荡轮动", n_picks=8)
    wide = rr.compute_adaptive_risk_params(regime="震荡轮动", n_picks=50)
    # 名单越窄 → 单名可越重、行业越集中可占比越高
    assert narrow["max_single_weight_pct"] > wide["max_single_weight_pct"]
    assert narrow["max_sector_weight_pct"] > wide["max_sector_weight_pct"]


def test_risk_params_bounded_by_floor_ceil():
    sw = rr.CONFIG["single_weight"]
    sec = rr.CONFIG["sector_weight"]
    pb = rr.CONFIG["portfolio_beta"]
    for n in [1, 8, 15, 30, 100]:
        p = rr.compute_adaptive_risk_params(regime="震荡轮动", n_picks=n)
        assert sw["floor_pct"] <= p["max_single_weight_pct"] <= sw["ceil_pct"]
        assert sec["floor_pct"] <= p["max_sector_weight_pct"] <= sec["ceil_pct"]
        assert pb["min"] <= p["max_portfolio_beta"] <= pb["max"]
        # 单行业数量与单策略数量也受 config 约束
        mps = rr.CONFIG["max_per_sector"]
        assert mps["min_count"] <= p["max_per_sector"] <= mps["max_count"]
        mps2 = rr.CONFIG["max_per_strategy"]
        assert mps2["min"] <= p["max_per_strategy"] <= mps2["max"]


def test_beta_floats_with_regime():
    down = rr.compute_adaptive_risk_params(regime="下行防御")["max_portfolio_beta"]
    neutral = rr.compute_adaptive_risk_params(regime="震荡轮动")["max_portfolio_beta"]
    up = rr.compute_adaptive_risk_params(regime="趋势上行")["max_portfolio_beta"]
    assert down < neutral < up


def test_single_weight_tightens_at_high_vol():
    low = rr.compute_adaptive_risk_params(
        regime="震荡轮动", n_picks=15, profile=_FakeProfile(vol=0.05))
    high = rr.compute_adaptive_risk_params(
        regime="震荡轮动", n_picks=15, profile=_FakeProfile(vol=0.95))
    assert high["max_single_weight_pct"] <= low["max_single_weight_pct"]


def test_exit_baseline_in_neutral():
    e = rr.compute_adaptive_exit_params(regime="震荡轮动")
    assert e["stop_loss_pct"] == 0.08
    assert e["take_profit_pct"] == 0.06
    assert e["trailing_stop_pct"] == 0.05
    assert e["trend_exit_ma"] == 60
    assert e["hold_days"] == 10
    assert e["slippage"] == 0.001


def test_exit_trend_ma_follows_regime():
    down = rr.compute_adaptive_exit_params(regime="下行防御")["trend_exit_ma"]
    neutral = rr.compute_adaptive_exit_params(regime="震荡轮动")["trend_exit_ma"]
    up = rr.compute_adaptive_exit_params(regime="趋势上行")["trend_exit_ma"]
    assert down == 40
    assert neutral == 60
    assert up == 90


def test_exit_scales_with_volatility():
    # 高波动 → 止损更宽（避免被洗），低波动 → 更紧
    lo = rr.compute_adaptive_exit_params(profile=_FakeProfile(vol=0.05))["stop_loss_pct"]
    hi = rr.compute_adaptive_exit_params(profile=_FakeProfile(vol=0.95))["stop_loss_pct"]
    assert hi > lo
    # 但都被 min/max 约束
    sl = rr.CONFIG["exit"]["stop_loss_pct"]
    assert sl["min"] <= lo <= sl["max"]
    assert sl["min"] <= hi <= sl["max"]


def test_sector_bonus_scales_with_dispersion():
    # 板块 median ret20 离散度越大 → 对强势板块倾斜越强（calibration: 典型离散≈0.05→≈6.0）
    low = rr.compute_sector_momentum_bonus({"a": 0.01, "b": 0.02, "c": 0.015})   # 微小离散 → floor
    mid = rr.compute_sector_momentum_bonus({"a": 0.0, "b": 0.08, "c": 0.16})     # 中等离散 → 区间内
    high = rr.compute_sector_momentum_bonus({"a": 0.01, "b": 0.30, "c": 0.50})   # 极大离散 → ceil
    assert low < mid < high
    # 样本不足 → 收敛到 floor
    floor = rr.CONFIG["sector_momentum_bonus"]["floor"]
    assert rr.compute_sector_momentum_bonus({"a": 0.05}) >= floor


def test_no_hardcoded_contract_config_driven(monkeypatch):
    """改 CONFIG 必须直接改变输出 —— 这是『无代码内硬编码』的硬契约。"""
    custom = rr._deep_copy(rr._BUILTIN_DEFAULTS)
    custom["single_weight"]["ceil_pct"] = 25.0
    custom["single_weight"]["expansion"] = 3.0
    custom["exit"]["stop_loss_pct"]["base"] = 0.10
    monkeypatch.setattr(rr, "CONFIG", custom)
    monkeypatch.setattr(rr, "RISK_CONFIG", custom)

    p = rr.compute_adaptive_risk_params(regime="震荡轮动", n_picks=8)
    # expansion=3.0, N=8 → 37.5，被新 ceil 25 约束（原 ceil 15 会得到 15）
    assert p["max_single_weight_pct"] <= 25.0
    assert p["max_single_weight_pct"] > 15.0

    e = rr.compute_adaptive_exit_params(regime="震荡轮动")
    assert e["stop_loss_pct"] == 0.10


def test_defaults_derive_safety_caps_from_config():
    """defaults 的『绝对安全天花板』必须取自 CONFIG，而非独立写死。"""
    assert d.MAX_SINGLE_WEIGHT_PCT == rr.CONFIG["single_weight"]["ceil_pct"]
    assert d.MAX_SECTOR_WEIGHT_PCT == rr.CONFIG["sector_weight"]["ceil_pct"]
    assert d.PORTFOLIO_BETA_CEILING == rr.CONFIG["portfolio_beta"]["max"]
    assert d.BETA_MIN_KEEP == int(rr.CONFIG["beta_min_keep"]["min"])


def test_vol_mult_bounds():
    # _vol_mult 用于出场：高波动→放宽(>1)、低波动→收紧(<1)
    assert rr._vol_mult(None, 0.5) == 1.0
    assert rr._vol_mult(0.99, 0.5) > 1.0
    assert rr._vol_mult(0.01, 0.5) < 1.0


def test_vol_tighten_bounds():
    # _vol_tighten 用于单名上限：高波动→收紧(<1)、低波动→放宽(>1)
    assert rr._vol_tighten(None, 0.5) == 1.0
    assert rr._vol_tighten(0.99, 0.5) < 1.0
    assert rr._vol_tighten(0.01, 0.5) > 1.0


def test_save_risk_config_preserves_tuned_values():
    """回归：save_risk_config 必须以 live CONFIG 为基底合并，不能把已调优值回退成默认值。

    历史上 save_risk_config 以 _BUILTIN_DEFAULTS 为基底，曾把文件里已调优的
    vol_target.enabled=false 与 sector_momentum_bonus.dispersion_k=120 静默回退成
    默认值(true / 1.2)，误伤其他配置。本测试锁定该行为不再复发。
    """
    import copy
    import json
    from pathlib import Path

    path = Path(rr.__file__).resolve().parent / "risk_config.json"
    original = path.read_text(encoding="utf-8")
    try:
        # 确保 live CONFIG 持有非默认调优值（与文件一致）
        assert rr.CONFIG["vol_target"]["enabled"] is False  # 默认是 True
        assert rr.CONFIG["sector_momentum_bonus"]["dispersion_k"] == 120.0  # 默认是 1.2
        # 仅更新 factor_scoring 的一个键后整盘写回
        new_full = copy.deepcopy(rr.CONFIG)
        new_full["factor_scoring"]["scale"] = 8.0
        rr.save_risk_config(new_full)
        reloaded = json.loads(path.read_text(encoding="utf-8"))
        # 无关键必须保留已调优值，不被回退成默认值
        assert reloaded["vol_target"]["enabled"] is False
        assert reloaded["sector_momentum_bonus"]["dispersion_k"] == 120.0
        assert reloaded["factor_scoring"]["scale"] == 8.0
    finally:
        path.write_text(original, encoding="utf-8")
        # 刷新模块缓存
        rr.CONFIG.update(json.loads(original))
