"""因子 IC/IR 监控单元测试（mock 掉文件读取）。"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

import factor_ic_monitor as fim


# ───────────────────────── 基础统计 ─────────────────────────
def test_spearman_perfect():
    assert fim._spearman([1, 2, 3, 4], [1, 2, 3, 4]) == pytest.approx(1.0)
    assert fim._spearman([1, 2, 3, 4], [4, 3, 2, 1]) == pytest.approx(-1.0)


def test_spearman_degenerate():
    assert fim._spearman([1, 1, 1, 1], [1, 2, 3, 4]) is None  # 常量无变化
    assert fim._spearman([1], [2]) is None                    # 样本<2
    assert fim._spearman([1, 2], [1]) is None                 # 长度不等


def test_rank_ties():
    assert fim._rank([1, 1, 2, 3]) == [1.5, 1.5, 3.0, 4.0]


# ───────────────────────── 系统级合并 IC ─────────────────────────
def _days20():
    return [f"2026{d:04d}" for d in range(1, 21)]


def _pos_picks(sd):
    # 权重 1..6 与收益 2..12 完美正相关 -> IC≈+1
    return [{"code": f"C{i}", "sources": set(), "return_pct": float(2 * i),
             "prod_weight": float(i)} for i in range(1, 7)]


def _neg_picks(sd):
    # 高权重对应低收益 -> IC≈-1
    return [{"code": f"C{i}", "sources": set(), "return_pct": float(14 - 2 * i),
             "prod_weight": float(i)} for i in range(1, 7)]


def test_system_ic_positive(monkeypatch):
    monkeypatch.setattr(fim, "_all_signal_days", _days20)
    monkeypatch.setattr(fim, "_load_day_picks", _pos_picks)
    res = fim.system_ic(window=10)
    assert res["recent_ic"] is not None
    assert res["recent_ic"] > 0
    assert res["degraded"] is False
    assert res["weak"] is False


def test_system_ic_degraded(monkeypatch):
    monkeypatch.setattr(fim, "_all_signal_days", _days20)
    monkeypatch.setattr(fim, "_load_day_picks", _neg_picks)
    res = fim.system_ic(window=10)
    assert res["recent_ic"] is not None
    assert res["recent_ic"] < 0
    assert res["degraded"] is True


def test_system_ic_no_days(monkeypatch):
    monkeypatch.setattr(fim, "_all_signal_days", lambda: [])
    res = fim.system_ic(window=10)
    assert res["recent_ic"] is None
    assert res["note"]


# ───────────────────────── 策略级信念 IC ─────────────────────────
def _days12():
    return [f"2026{d:04d}" for d in range(1, 13)]


def test_strategy_conviction_decay(monkeypatch):
    monkeypatch.setattr(fim, "_all_signal_days", _days12)

    def fake_weights(sd):
        idx = int(sd) - 20260000  # 1..12
        w = {s: 1.0 for s in fim.ALL_STRATEGIES}
        w["relativity"] = float(idx)  # 权重逐日递增
        return w, False

    monkeypatch.setattr(fim, "_weights_for_day", fake_weights)

    def fake_picks(sd):
        idx = int(sd) - 20260000
        # relativity 选中票收益随其权重递增而递减 -> 信念 IC 应显著为负
        return [{"code": "X", "sources": {"relativity"},
                 "return_pct": float(-idx), "prod_weight": 1.0}]

    monkeypatch.setattr(fim, "_load_day_picks", fake_picks)
    res = fim.strategy_conviction_ic(window=10)
    rel = res["relativity"]
    assert rel["conviction_ic"] is not None
    assert rel["conviction_ic"] < 0
    assert rel["decayed"] is True


# ───────────────────────── 汇总 ─────────────────────────
def test_analyze_no_days(monkeypatch):
    monkeypatch.setattr(fim, "_all_signal_days", lambda: [])
    res = fim.analyze()
    assert res["as_of"] is None
    assert res["alert"] is False
    assert res["system"]["recent_ic"] is None


def test_analyze_issue_body(monkeypatch):
    monkeypatch.setattr(fim, "_all_signal_days", _days20)
    monkeypatch.setattr(fim, "_load_day_picks", _neg_picks)
    res = fim.analyze(window=10)
    body = fim._format_issue_body(res)
    assert "因子 IC/IR 监控告警" in body
    assert "系统级合并 Rank-IC" in body
