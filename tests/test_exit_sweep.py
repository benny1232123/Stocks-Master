"""P3 出场参数样本外扫描：配置驱动网格 + 落盘 + 最优组合提取。"""
from __future__ import annotations

import json
import importlib

import pytest

wf = importlib.import_module("scripts.walk_forward_validator")


def test_sweep_exits_empty_days_writes_json(tmp_path):
    # days=[] -> 各组合 adaptive 均为 0，但结构/排序/落盘必须正确（不依赖 k_data）
    out = tmp_path / "exit_sweep.json"
    rows = wf.sweep_exits(days=[], save_path=str(out))
    assert len(rows) == len(wf._STOP_LOSS_GRID) * len(wf._TRAILING_GRID) * len(wf._HOLD_GRID)
    # 按 adaptive desc 排序（相等也满足）
    assert all(rows[i]["adaptive"] >= rows[i + 1]["adaptive"] for i in range(len(rows) - 1))
    assert out.exists()
    d = json.loads(out.read_text(encoding="utf-8"))
    assert set(d.keys()) >= {"generated_at", "n_days", "grid", "rows"}
    assert d["n_days"] == 0
    assert d["grid"]["stop_loss_pct"] == list(wf._STOP_LOSS_GRID)


def test_sweep_exits_grid_is_configurable():
    # 出场扫描网格须可配置（零硬编码），改模块级网格即影响组合数
    saved = (wf._STOP_LOSS_GRID, wf._TRAILING_GRID, wf._HOLD_GRID)
    try:
        wf._STOP_LOSS_GRID = [0.05, 0.09]
        wf._TRAILING_GRID = [0.04]
        wf._HOLD_GRID = [10]
        rows = wf.sweep_exits(days=[])
        assert len(rows) == 2 * 1 * 1
    finally:
        wf._STOP_LOSS_GRID, wf._TRAILING_GRID, wf._HOLD_GRID = saved


def test_best_exit_params_returns_dict(monkeypatch):
    monkeypatch.setattr(
        wf, "sweep_exits",
        lambda *a, **k: [
            {"stop_loss_pct": 0.10, "trailing_stop_pct": 0.07, "hold_days": 14,
             "adaptive": 5.0, "equal": 3.0, "diff": 2.0},
            {"stop_loss_pct": 0.08, "trailing_stop_pct": 0.05, "hold_days": 10,
             "adaptive": 4.0, "equal": 3.0, "diff": 1.0},
        ],
    )
    monkeypatch.setattr(
        wf, "compute_adaptive_exit_params",
        lambda *a, **k: {"stop_loss_pct": 0.08, "trailing_stop_pct": 0.05, "hold_days": 10},
    )
    be = wf._best_exit_params()
    assert be is not None
    assert be["stop_loss_pct"] == 0.10 and be["hold_days"] == 14
    assert be["better_than_default"] is True
    assert be["default_row_adaptive_pct"] == 4.0


def test_best_exit_params_not_better(monkeypatch):
    # 最优组合恰好等于当前默认基线 -> better_than_default=False（不误导采纳）
    monkeypatch.setattr(
        wf, "sweep_exits",
        lambda *a, **k: [
            {"stop_loss_pct": 0.08, "trailing_stop_pct": 0.05, "hold_days": 10,
             "adaptive": 4.0, "equal": 3.0, "diff": 1.0},
        ],
    )
    monkeypatch.setattr(
        wf, "compute_adaptive_exit_params",
        lambda *a, **k: {"stop_loss_pct": 0.08, "trailing_stop_pct": 0.05, "hold_days": 10},
    )
    be = wf._best_exit_params()
    assert be["better_than_default"] is False


def test_best_exit_params_failsoft_on_error(monkeypatch):
    monkeypatch.setattr(wf, "sweep_exits", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    assert wf._best_exit_params() is None
