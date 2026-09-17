"""自适应权重 + 现金曲线的纯函数单元测试（无网络依赖）。"""
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from smcore.strategy.adaptive_weights import (
    ALL_STRATEGIES,
    CONFIG,
    _aggregate_excess,
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


def test_aggregate_excess_unweighted_equals_mean():
    """position_weighted=False 退化成等权平均（与历史行为一致）。"""
    pairs = [(10.0, 0.0), (0.0, 0.0), (-5.0, 0.0)]  # pos 全 0 → 等权；仅 10>0 一票为胜
    edge, win, n, sd = _aggregate_excess(pairs, False)
    assert n == 3
    assert edge == (10.0 + 0.0 - 5.0) / 3
    assert win == round(1 / 3 * 100, 1)


def test_aggregate_excess_position_weighted():
    """position_weighted=True 按仓位% 加权聚合。"""
    # A 仓位 90、收益 10；B 仓位 10、收益 -10 → 应被 A 主导 ≈ (10*90 + -10*10)/100 = 8.0
    pairs = [(10.0, 90.0), (-10.0, 10.0)]
    edge, win, n, sd = _aggregate_excess(pairs, True)
    assert n == 2
    assert abs(edge - 8.0) < 1e-9
    # 仓位加权胜率：A 胜(90)、B 负(10) → 90/100 = 90%
    assert abs(win - 90.0) < 1e-9


def test_aggregate_excess_position_weighted_degrade_when_no_pos():
    """仓位权重全缺失(全 0) 时退化为等权，不报错。"""
    pairs = [(10.0, 0.0), (2.0, 0.0)]
    edge, win, n, sd = _aggregate_excess(pairs, True)
    assert n == 2
    assert edge == 6.0  # (10+2)/2


def test_compute_universe_edge_position_weighted_meta_flag():
    """position_weighted 参数透传到 __meta__；DAL 缺「建议仓位%」时自动退化为等权。"""
    from smcore.strategy.adaptive_weights import compute_universe_edge
    edge_off = compute_universe_edge(position_weighted=False)
    assert edge_off["__meta__"]["position_weighted"] is False
    edge_on = compute_universe_edge(position_weighted=True)
    assert edge_on["__meta__"]["position_weighted"] is True
    # 字段结构在两种模式下一致
    assert set(edge_off.keys()) == set(edge_on.keys())


def test_factor_timing_config_registered():
    """2026-09-16 踩坑回归：factor_timing 必须注册在 _BUILTIN_DEFAULTS。

    否则 _load_config 会忽略文件里的该键（开关静默失效），且 save_config 写回时会
    把整个 factor_timing 块丢弃（实测把 enabled/window/min_n 从配置文件里抹掉）。
    """
    from smcore.strategy import adaptive_weights as aw

    assert "factor_timing" in aw._BUILTIN_DEFAULTS, "factor_timing 未注册到 _BUILTIN_DEFAULTS"
    ft = aw._BUILTIN_DEFAULTS["factor_timing"]
    assert {"enabled", "window", "min_n"} <= set(ft)
    # 缺省必须为关（不得改变现状默认行为）
    assert ft["enabled"] is False
    # _load_config 合并后 CONFIG 必须含该键（含文件覆盖）
    assert "factor_timing" in aw.CONFIG
    assert set(aw.CONFIG["factor_timing"]) >= {"enabled", "window", "min_n"}


def test_save_config_preserves_factor_timing(tmp_path, monkeypatch):
    """save_config 往返必须保留 factor_timing（回归：曾把它整块丢弃）。"""
    import json as _json

    from smcore.strategy import adaptive_weights as aw

    # monkeypatch CONFIG/_CONFIG_PATH 使其在测试后自动还原（save_config 会重绑 CONFIG）
    monkeypatch.setattr(aw, "CONFIG", {k: (dict(v) if isinstance(v, dict) else v)
                                       for k, v in aw.CONFIG.items()})
    target = tmp_path / "cfg.json"
    monkeypatch.setattr(aw, "_CONFIG_PATH", target)
    cfg = {k: (dict(v) if isinstance(v, dict) else v) for k, v in aw.CONFIG.items()}
    cfg.setdefault("factor_timing", {})["enabled"] = True
    aw.save_config(cfg)
    written = _json.loads(target.read_text(encoding="utf-8"))
    assert written["factor_timing"]["enabled"] is True
    assert written["factor_timing"]["window"] == 10
    assert written["factor_timing"]["min_n"] == 5


def test_excluded_strategies_registered():
    """2026-09-17 回归：excluded_strategies 必须注册在 _BUILTIN_DEFAULTS。

    否则 _load_config 忽略文件里的该键（黑名单静默失效），save_config 写回时整块丢弃。
    缺省必须为空列表（不破坏现状默认行为）。
    """
    from smcore.strategy import adaptive_weights as aw

    assert "excluded_strategies" in aw._BUILTIN_DEFAULTS
    assert aw._BUILTIN_DEFAULTS["excluded_strategies"] == []
    assert "excluded_strategies" in aw.CONFIG


def test_excluded_strategies_zeroed_in_allocation(monkeypatch):
    """compute_adaptive_allocation 必须把黑名单策略强制清零并重新归一化。"""
    from smcore.strategy import adaptive_weights as aw

    # 伪造 edge，避免碰真实 k 线/网络；各策略 edge 相等 ≈ 等权起点
    fake_edge = {s: {"edge": 1.0, "n": 30, "win_rate": 50, "avg": 1.0} for s in ALL_STRATEGIES}
    monkeypatch.setattr(aw, "compute_edge", lambda *a, **k: fake_edge)
    # 关掉覆盖层，隔离排除逻辑；注入黑名单
    monkeypatch.setattr(
        aw, "CONFIG",
        {**aw.CONFIG,
         "factor_timing": {**aw.CONFIG.get("factor_timing", {}), "enabled": False},
         "excluded_strategies": ["momentum", "theme"]},
    )
    _edge, w, _cash, cold = aw.compute_adaptive_allocation(min_n=1)
    assert not cold
    assert w["momentum"] == 0 and w["theme"] == 0, w
    # 其余策略吸收权重、和为 100
    assert sum(w.values()) == 100
    for s in ("boll", "relativity", "cctv"):
        assert w[s] > 0, (s, w)


def test_no_evidence_strategy_capped_to_floor(monkeypatch):
    """无业绩历史(n=0)的新策略只拿 floor 探索权重，不凭空瓜分被清零策略的额度。

    回归：fundamental 刚接入时 n=0，曾因黑名单把 theme/cctv/momentum 清零后 renormalize
    到仅剩 [boll, relativity, fundamental] 三个幸存者，被均分拿到 ~32% 无证据权重。
    门控开启后它只保留 eff_floor 的「探索权重」，释放额度全给有证据的策略。
    """
    from smcore.strategy import adaptive_weights as aw

    fake_edge = {
        "boll":       {"edge": 2.0, "n": 30, "win_rate": 55, "avg": 2.0},
        "relativity": {"edge": 1.0, "n": 30, "win_rate": 52, "avg": 1.0},
        # fundamental：刚接入，零归因历史
        "fundamental": {"edge": 0.0, "n": 0, "win_rate": None, "avg": None},
        "theme":      {"edge": 0.0, "n": 0, "win_rate": None, "avg": None},
        "cctv":       {"edge": 0.0, "n": 0, "win_rate": None, "avg": None},
        "momentum":   {"edge": 0.0, "n": 0, "win_rate": None, "avg": None},
    }
    monkeypatch.setattr(aw, "compute_edge", lambda *a, **k: fake_edge)
    monkeypatch.setattr(
        aw, "CONFIG",
        {**aw.CONFIG,
         "factor_timing": {**aw.CONFIG.get("factor_timing", {}), "enabled": False},
         "excluded_strategies": ["theme", "cctv", "momentum"],
         "exclude_no_evidence_strategies": True,
         "min_evidence_for_allocation": 1,
         "FLOOR": aw.CONFIG["FLOOR"]},
    )
    _edge, w, _cash, cold = aw.compute_adaptive_allocation(min_n=1)
    assert not cold
    # 黑名单清零
    assert w["theme"] == 0 and w["cctv"] == 0 and w["momentum"] == 0
    # fundamental 只拿 floor 探索权重（固定 ≈3%，不随幸存池放大）
    FLOOR = aw.CONFIG["FLOOR"]
    assert w["fundamental"] > 0, w
    assert w["fundamental"] <= FLOOR + 1, w
    assert w["fundamental"] >= 1, w
    # 有证据的策略吃掉释放额度
    assert w["boll"] > w["fundamental"] and w["relativity"] > w["fundamental"], w
    assert sum(w.values()) == 100


def test_no_evidence_gate_can_be_disabled(monkeypatch):
    """exclude_no_evidence_strategies=false 时退化为原行为（新策略按初稿权重参与）。

    这是用户的「即时稀释」开关：若愿承担无证据权重风险，可让刚接入的 fundamental
    立即按 softmax 初稿参与分配，而非被压到 floor。本测试仅保证该开关可正常关闭、
    不破坏求和与黑名单语义。
    """
    from smcore.strategy import adaptive_weights as aw

    fake_edge = {
        "boll":       {"edge": 2.0, "n": 30, "win_rate": 55, "avg": 2.0},
        "relativity": {"edge": 1.0, "n": 30, "win_rate": 52, "avg": 1.0},
        "fundamental": {"edge": 0.0, "n": 0, "win_rate": None, "avg": None},
        "theme":      {"edge": 0.0, "n": 0, "win_rate": None, "avg": None},
        "cctv":       {"edge": 0.0, "n": 0, "win_rate": None, "avg": None},
        "momentum":   {"edge": 0.0, "n": 0, "win_rate": None, "avg": None},
    }
    monkeypatch.setattr(aw, "compute_edge", lambda *a, **k: fake_edge)
    monkeypatch.setattr(
        aw, "CONFIG",
        {**aw.CONFIG,
         "factor_timing": {**aw.CONFIG.get("factor_timing", {}), "enabled": False},
         "excluded_strategies": ["theme", "cctv", "momentum"],
         "exclude_no_evidence_strategies": False,
         "min_evidence_for_allocation": 1,
         "FLOOR": aw.CONFIG["FLOOR"]},
    )
    _edge, w, _cash, cold = aw.compute_adaptive_allocation(min_n=1)
    assert not cold
    assert w["theme"] == 0 and w["cctv"] == 0 and w["momentum"] == 0
    assert sum(w.values()) == 100
    # 关闭门控后 fundamental 仍有正权重（不被强制压到 floor 区间外），且仍 > 0
    assert w["fundamental"] > 0, w
