from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from smcore.strategy.adaptive_weights import compute_dynamic_shrinkage, _sd


def test_strong_evidence_low_shrinkage():
    """样本大、标准差小、edge 显著 → 几乎全信自适应权重（shrinkage 接近 0）。"""
    edge = {
        "momentum": {"n": 120, "edge": 5.0, "std": 10.0},  # t = 5/(10/sqrt120) = 5.48
        "__meta__": {},
    }
    sh = compute_dynamic_shrinkage(edge)
    assert sh["momentum"] < 0.05


def test_weak_evidence_high_shrinkage():
    """证据不足 → 靠近 base（≈ 等权）但不完全等于 base 一旦有弱显著尾部。"""
    edge = {
        "boll": {"n": 3, "edge": 8.0, "std": 5.0},  # t=2.77，但由于样本极小 c 很低 → 仍高度收缩
        "cctv": {"n": 50, "edge": 0.2, "std": 9.0},  # t=0.157，不显著 → 完全 base
    }
    sh = compute_dynamic_shrinkage(edge)
    assert sh["boll"] < 0.4  # 有微弱证据，略降
    assert sh["boll"] > 0.25  # 但仍高度收缩（样本太小，几乎等权）
    assert sh["cctv"] >= 0.35  # 不显著 → 几乎完全 base（仅微小收缩）


def test_all_values_within_bounds():
    """返回全部在 [0, base]，且元数据键被忽略。"""
    edge = {
        "momentum": {"n": 200, "edge": 2.0, "std": 15.0},
        "relativity": {"n": 60, "edge": -1.0, "std": 12.0},  # 负 edge 同样有显著度
        "cctv": {"n": 2, "edge": 9.0, "std": 3.0},
        "boll": {"n": 0, "edge": 0.0, "std": 0.0},  # 无样本 → base
        "theme": {"n": 40, "edge": 0.5, "std": 2.0},
        "__meta__": {"source": "universe"},
    }
    sh = compute_dynamic_shrinkage(edge, base=0.3)
    assert "__meta__" not in sh
    for k, v in sh.items():
        assert 0.0 <= v <= 0.3, (k, v)
    # 无样本策略 → base
    assert sh["boll"] == 0.3
    # 强证据显著 → 应低于 base（负 edge 且显著时同样信）
    assert sh["relativity"] < 0.3


def test_noise_returns_base():
    """n<=1 或 std=0 → base。"""
    assert compute_dynamic_shrinkage({"a": {"n": 1, "edge": 5.0, "std": 3.0}})["a"] == 0.4
    assert compute_dynamic_shrinkage({"a": {"n": 50, "edge": 5.0, "std": 0.0}})["a"] == 0.4


def test_sd_helper():
    assert _sd([1.0]) == 0.0
    assert _sd([]) == 0.0
    assert round(_sd([2.0, 4.0]), 6) == 1.0  # 总体标准差 (均值3，方差1)