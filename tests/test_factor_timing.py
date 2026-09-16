"""因子生效开关（生产路径 smcore.strategy.factor_timing）纯函数测试。

守护：
1. Spearman/秩纯函数边界；
2. 核心 mask 逻辑（显著正 → 生效、显著负 → 关闭、点数不足 → 保留）；
3. **分歧守卫**：验证器 _factor_timing_mask 与本模块 factor_timing_mask_from_points 给同一 mask；
4. apply_mask 清零+重归一化；全关则原样返回；
5. 生产接线 _apply_factor_timing 取整且和为 100。
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: F401

from smcore.strategy import factor_timing as ft
from smcore.strategy.adaptive_weights import ALL_STRATEGIES

_DAYS6 = [f"2024010{i}" for i in range(1, 7)]  # 6 个信号日


def _pts(pairs_by_strat):
    """pairs_by_strat: {strategy: [(w, r), ...]} → points（日取自 _DAYS6）。"""
    pts = {s: [] for s in ALL_STRATEGIES}
    for s, seq in pairs_by_strat.items():
        pts[s] = [(_DAYS6[i], float(w), float(r)) for i, (w, r) in enumerate(seq)]
    return pts


def test_spearman_ic_bounds():
    assert ft.spearman_ic([1, 2, 3, 4, 5], [2, 4, 6, 8, 10]) > 0.99
    assert ft.spearman_ic([1, 2, 3, 4, 5], [10, 8, 6, 4, 2]) < -0.99
    assert ft.spearman_ic([1, 1, 1], [1, 2, 3]) is None  # 一方无变化
    assert ft.spearman_ic([1, 2], [1, 2]) is None        # n<3


def test_mask_from_points_switches_off_negative_ic():
    pts = _pts({
        "boll": [(0.4 + 0.1 * i, 0.1 * i) for i in range(6)],      # +IC → 生效
        "momentum": [(0.4 + 0.1 * i, -0.1 * i) for i in range(6)],  # -IC → 关闭
    })
    mask = ft.factor_timing_mask_from_points(pts, _DAYS6, min_n=5, z=1.96)
    assert set(mask) == set(ALL_STRATEGIES)
    assert mask["boll"] is True
    assert mask["momentum"] is False


def test_mask_keeps_factor_when_insufficient_points():
    pts = _pts({"boll": [(0.1, 0.1), (0.2, 0.2)]})  # 仅 2 点 < min_n
    mask = ft.factor_timing_mask_from_points(pts, _DAYS6, min_n=5, z=1.96)
    assert mask["boll"] is True  # 无证据不判失效


def test_shared_core_matches_validator(monkeypatch):
    """分歧守卫：验证器委托本模块核心，同一输入必须给同一 mask。"""
    import importlib

    wf = importlib.import_module("walk_forward_validator")
    days = [f"202401{i:02d}" for i in range(1, 8)]  # 7 天
    pts = {s: [] for s in ALL_STRATEGIES}
    pts["boll"] = [(days[i], 0.4 + 0.1 * i, 0.1 * i) for i in range(7)]
    pts["momentum"] = [(days[i], 0.4 + 0.1 * i, -0.1 * i) for i in range(7)]
    monkeypatch.setattr(wf, "_ensure_factor_timing_points", lambda: pts)
    monkeypatch.setattr(wf, "_all_signal_days", lambda: days)
    vmask = wf._factor_timing_mask(days[-1])
    fmask = ft.factor_timing_mask_from_points(
        pts, days[:-1][-10:], min_n=wf.FACTOR_TIMING_MIN_N, z=wf.FACTOR_TIMING_Z
    )
    assert vmask == fmask


def test_apply_mask_renormalizes(monkeypatch):
    pts = _pts({
        "boll": [(0.4 + 0.1 * i, 0.1 * i) for i in range(6)],
        "momentum": [(0.4 + 0.1 * i, -0.1 * i) for i in range(6)],
    })
    monkeypatch.setattr(ft, "_signal_days", lambda: _DAYS6 + ["20240107"])
    weights = {s: 20.0 for s in ALL_STRATEGIES}
    out = ft.apply_mask(weights, signal_date="20240107", points=pts)
    assert out["momentum"] == 0.0
    assert abs(sum(out.values()) - 100.0) < 1e-6
    # 其余 4 个（含无数据的 3 个，保留）均分 → 25
    for s in ALL_STRATEGIES:
        if s != "momentum":
            assert abs(out[s] - 25.0) < 1e-6


def test_apply_mask_all_off_returns_original(monkeypatch):
    # 所有有数据的因子信念 IC 都为负 → 全关 → 原样返回（避免空分配）
    pts = _pts({s: [(0.4 + 0.1 * i, -0.1 * i) for i in range(6)] for s in ALL_STRATEGIES})
    monkeypatch.setattr(ft, "_signal_days", lambda: _DAYS6 + ["20240107"])
    weights = {s: 20.0 for s in ALL_STRATEGIES}
    out = ft.apply_mask(weights, signal_date="20240107", points=pts)
    assert out == {s: 20.0 for s in ALL_STRATEGIES}


def test_apply_factor_timing_rounds_to_100(monkeypatch):
    """生产接线：_apply_factor_timing 取整且和为 100，并 fail-soft 穿透。"""
    from smcore.strategy import adaptive_weights as aw

    def fake_apply(weights, signal_date=None, points=None):
        return {s: 20.7 for s in ALL_STRATEGIES}  # 和 103.5 → 取整需修正回 100

    monkeypatch.setattr(ft, "apply_mask", fake_apply)
    out = aw._apply_factor_timing({s: 20 for s in ALL_STRATEGIES}, signal_date="20240107")
    assert all(isinstance(v, int) for v in out.values())
    assert sum(out.values()) == 100


def test_factor_timing_is_enabled_mirrors_config():
    """is_enabled() 镜像 CONFIG["factor_timing"]["enabled"]（生产开关唯一真源）。

    注意：**不硬断言「关」**——该值由门控脚本 / 月度 tripwire 写回（2026-09-16 已启用 true）。
    内置缺省为关（由 test_adaptive_weights.test_factor_timing_config_registered 守护）。
    """
    from smcore.strategy.adaptive_weights import CONFIG

    expected = bool((CONFIG.get("factor_timing") or {}).get("enabled", False))
    assert ft.is_enabled() is expected
