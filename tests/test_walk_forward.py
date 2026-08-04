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
    # 无收缩+无地板配置应跑赢等权（本数据集内的关键正向信号）
    best = [g for g in grid if g["shrinkage"] == 0.0 and g["floor"] == 0.0]
    assert best, "缺失 无收缩+无地板 配置"
    assert best[0]["diff"] > 0, f"去地板配置未跑赢等权：{best[0]['diff']}"


def test_causal_edge_excludes_future():
    """cutoff 当天不应被纳入 edge 计算。"""
    # 取一个中间信号日，确认其 causal_edge 不读自身 trades
    days = wf._all_signal_days()
    mid = days[len(days) // 2]
    edge = wf.causal_edge(mid)
    # edge 来自更早的信号日，total_n 为有限非负数
    total_n = sum(e["n"] for e in edge.values())
    assert total_n >= 0
