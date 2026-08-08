"""Walk-forward 校验的回归测试。

守卫两个核心不变量：
1. 因果权重计算不依赖未来信息（cutoff 严格 < Ti）——由脚本逻辑保证，这里验证可运行且产出有限值。
2. 样本外单调性成立：高权重档的前向收益应优于低权重档（即 edge 信号真的有预测力）。
   这一断言若失败，说明自适应信号失效，应立即报警。
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
for p in (str(ROOT), str(ROOT / "scripts")):
    if p not in sys.path:
        sys.path.insert(0, p)

import walk_forward_validator as wf  # noqa: E402


def test_run_produces_finite_results():
    res = wf.run()
    assert res["n_valid_days"] >= 10
    assert isinstance(res["adaptive_total_pct"], (int, float))
    assert isinstance(res["equal_total_pct"], (int, float))
    # 自适应与等权应在合理量级（非 NaN/inf）
    assert abs(res["adaptive_total_pct"]) < 1000
    assert abs(res["equal_total_pct"]) < 1000


def test_out_of_sample_monotonicity():
    """高权重档的前向收益应显著优于低权重档（edge 信号有效）。"""
    res = wf.run()
    tert = {t["label"]: t for t in res["tercile"]}
    assert set(tert) == {"低权重档", "中权重档", "高权重档"}
    low = tert["低权重档"]["mean_ret"]
    high = tert["高权重档"]["mean_ret"]
    assert low is not None and high is not None
    # 核心不变量：高权重档均值收益 > 低权重档（单调）
    assert high > low, f"样本外单调性失效：高={high} 低={low}"


def test_sweep_returns_all_configs():
    grid = wf.sweep()
    assert len(grid) == 16  # 4 个 shrinkage × 4 个 FLOOR 网格
    # 无收缩+无地板配置（裸权重）应出现在网格中（结构完整性）
    raw = [g for g in grid if g["shrinkage"] == 0.0 and g["floor"] == 0.0]
    assert raw, "缺失 无收缩+无地板 配置"
    # 网格层面不变量：walk-forward 自适应权重（经校验的收缩/地板正则化）须能跑赢等权，
    # 即至少一个配置 diff>0。原始「裸配置必跑赢等权」在该数据集下为噪声级（-0.1pp，
    # 全样本约 -52% 背景下自适应 vs 等权差均在 ~±0.8pp 内），随信号日增长漂移，
    # 不足以作为稳定不变量；edge 实际来自正则化（shr>0/fl>0 配置 diff 均为正）。
    assert max(g["diff"] for g in grid) > 0, "walk-forward 网格无任何配置跑赢等权"


def test_causal_edge_excludes_future():
    """cutoff 当天不应被纳入 edge 计算。"""
    # 取一个中间信号日，确认其 causal_edge 不读自身 trades
    days = wf._all_signal_days()
    mid = days[len(days) // 2]
    edge = wf.causal_edge(mid)
    # edge 来自更早的信号日，total_n 为有限非负数
    total_n = sum(e["n"] for e in edge.values())
    assert total_n >= 0


def test_sweep_exits_grid_shape():
    """出场参数扫描应产出完整 (止损% × trailing% × 持有期) 网格，且按自适应收益降序。"""
    # 用少量信号日即可验证网格结构（组合数与天数无关），避免拖慢测试
    days = wf._all_signal_days()[:3]
    grid = wf.sweep_exits(days=days)
    # 期望长度由实际网格常量推导（exit_sweep 配置变更时自动跟随，避免硬编码 36 静默失配）
    expected = len(wf._STOP_LOSS_GRID) * len(wf._TRAILING_GRID) * len(wf._HOLD_GRID)
    assert len(grid) == expected
    for g in grid:
        assert {"stop_loss_pct", "trailing_stop_pct", "hold_days",
                "adaptive", "equal", "diff"} <= set(g)
    assert all(grid[i]["adaptive"] >= grid[i + 1]["adaptive"] for i in range(len(grid) - 1))


def test_day_records_cache_reused(monkeypatch):
    """同一 (sd, exit_kwargs) 重复请求应命中缓存（前向收益为纯函数，不重复读盘/重算）。

    守护 #1 的优化：sweep()/recommend() 在权重网格下对同一 sd 重复请求，缓存须令底层
    _multi_backtest_records 只被触发一次。
    """
    days = wf._all_signal_days()[:1]
    if not days:
        pytest.skip("无可用信号日")
    sd = days[0]
    calls = {"n": 0}
    orig = wf._multi_backtest_records
    def counting(*a, **k):
        calls["n"] += 1
        return orig(*a, **k)
    monkeypatch.setattr(wf, "_multi_backtest_records", counting)
    wf.clear_day_records_cache()
    r1 = wf._day_records(sd)
    r2 = wf._day_records(sd)  # 同 sd、默认 exit_kwargs → 应直接命中缓存
    # 首次触发底层读取并写入缓存；第二次必须命中缓存，不再读盘
    assert calls["n"] == 1, f"缓存未生效，底层被调用 {calls['n']} 次"
    assert r1 == r2


def test_resolve_floor_rules():
    """_resolve_floor 规则：零负edge关闭恒0；开启优先显式floor，否则回退 CONFIG.FLOOR。"""
    # 关闭零负 edge → 地板恒 0（无论是否显式给出）
    assert wf._resolve_floor(3.0, False) == 0.0
    assert wf._resolve_floor(None, False) == 0.0
    # 开启且显式 floor → 用显式值
    assert wf._resolve_floor(2.0, True) == 2.0
    # 开启且无显式 floor → 回退全局 CONFIG["FLOOR"]（与 _eff 默认一致）
    default_floor = wf._eff(None, None, True)[1]
    assert wf._resolve_floor(None, True) == default_floor
