"""策略衰减监控 + 纸盘模拟 的单元测试（mock 掉文件/网络读取）。"""
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

import strategy_decay_monitor as sdm
import paper_tracker as pt


# ───────────────────────── 衰减监控 ─────────────────────────
def test_analyze_flags_decay(monkeypatch):
    days = [f"202601{i:02d}" for i in range(1, 16)]  # 15 天
    monkeypatch.setattr(sdm, "_all_signal_days", lambda: days)

    def fake_edge(cutoff, window=10):
        # 近期窗口（window 较大）让 theme 偏弱 -> 衰减；boll 正常
        if window >= 10:
            return {
                "theme": {"n": 8, "avg_return": -3.0, "win_rate": 30.0, "edge": -3.0},
                "boll": {"n": 8, "avg_return": 2.0, "win_rate": 60.0, "edge": 2.0},
                "momentum": {"n": 2, "avg_return": -1.0, "win_rate": 0.0, "edge": -1.0},
            }
        return {  # 基线窗口（window 较小）也偏弱，仅用于对照
            "theme": {"n": 20, "avg_return": -1.0, "win_rate": 40.0, "edge": -1.0},
            "boll": {"n": 20, "avg_return": 3.0, "win_rate": 70.0, "edge": 3.0},
        }

    monkeypatch.setattr(sdm, "causal_edge", fake_edge)
    res = sdm.analyze(recent_window=10)
    assert "theme" in res["decayed"], "theme 近期均值<0且胜率<45 应被标记"
    assert "boll" not in res["decayed"]
    # momentum 近期样本仅 2 < MIN_N_EDGE(5)，不应误报
    assert "momentum" not in res["decayed"]
    assert res["strategies"]["theme"]["reason"]


def test_analyze_no_days(monkeypatch):
    monkeypatch.setattr(sdm, "_all_signal_days", lambda: [])
    res = sdm.analyze()
    assert res["error"]
    assert res["decayed"] == []


def test_issue_body_format(monkeypatch):
    days = [f"202601{i:02d}" for i in range(1, 16)]
    monkeypatch.setattr(sdm, "_all_signal_days", lambda: days)
    monkeypatch.setattr(sdm, "causal_edge", lambda cutoff, window=10: {
        "theme": {"n": 8, "avg_return": -3.0, "win_rate": 30.0, "edge": -3.0},
    })
    res = sdm.analyze()
    body = sdm._format_issue_body(res)
    assert "策略衰减告警" in body
    assert "theme" in body
    assert "| 策略 |" in body


# ───────────────────────── 纸盘模拟 ─────────────────────────
def _fake_kdata(prices: dict):
    df = pd.DataFrame([
        {"date": pd.to_datetime(d), "open": p, "high": p, "low": p, "close": p}
        for d, p in prices.items()
    ])
    return df


def test_stock_return_between(monkeypatch):
    # 000001: 0102 开 100, 0103 110, 0104 120 -> 段收益 +10%/+9.09%；
    # 000002: 0102 100, 0103 90, 0104 80 -> -10%/-11.11%
    def fake_kdata(code):
        if code == "000001":
            return _fake_kdata({"20260102": 100.0, "20260103": 110.0, "20260104": 120.0})
        if code == "000002":
            return _fake_kdata({"20260102": 100.0, "20260103": 90.0, "20260104": 80.0})
        return pd.DataFrame(columns=["date", "open"])

    monkeypatch.setattr(pt, "_load_cached_kdata", fake_kdata)
    r1 = pt._stock_return_between("000001", "20260101", "20260102")
    r2 = pt._stock_return_between("000002", "20260101", "20260102")
    assert abs(r1 - 10.0) < 1e-6
    assert abs(r2 - (-10.0)) < 1e-6
    # 缺失行情 -> None
    assert pt._stock_return_between("999999", "20260101", "20260102") is None


def test_max_drawdown():
    assert pt._max_drawdown([1.0, 1.2, 0.9, 1.1]) == pytest.approx(-25.0, abs=1e-6)
    assert pt._max_drawdown([1.0, 1.1, 1.2]) == pytest.approx(0.0, abs=1e-6)


def test_run_basic(monkeypatch):
    days = ["20260101", "20260102", "20260103"]
    monkeypatch.setattr(pt, "_all_signal_days", lambda: days)
    monkeypatch.setattr(pt, "_benchmark_return_between", lambda a, b: None)

    def fake_kdata(code):
        if code == "000001":
            return _fake_kdata({"20260102": 100.0, "20260103": 110.0, "20260104": 120.0})
        if code == "000002":
            return _fake_kdata({"20260102": 100.0, "20260103": 90.0, "20260104": 80.0})
        return pd.DataFrame(columns=["date", "open"])

    monkeypatch.setattr(pt, "_load_cached_kdata", fake_kdata)

    def fake_weights(sd):
        if sd == "20260101":
            return {"000001": 0.5, "000002": 0.5}
        if sd == "20260102":
            return {"000001": 1.0}
        return {}

    monkeypatch.setattr(pt, "_load_dal_weights", fake_weights)

    res = pt.run(invest_frac=1.0)
    assert res["realized_periods"] == 2
    assert res["benchmark"] == "不可用(无网络/无缓存)"
    # 段1: 0.5*10 + 0.5*(-10)=0 -> 值1.0；段2: 000001 从110到120 -> +9.09%
    assert res["final_value"] == pytest.approx(1.0909, abs=1e-3)
    assert res["total_return_pct"] == pytest.approx(9.09, abs=1e-2)
    assert res["max_drawdown_pct"] == pytest.approx(0.0, abs=1e-4)


def test_run_invest_frac_scales(monkeypatch):
    days = ["20260101", "20260102", "20260103"]
    monkeypatch.setattr(pt, "_all_signal_days", lambda: days)
    monkeypatch.setattr(pt, "_benchmark_return_between", lambda a, b: None)

    def fake_kdata(code):
        if code == "000001":
            return _fake_kdata({"20260102": 100.0, "20260103": 110.0, "20260104": 120.0})
        return pd.DataFrame(columns=["date", "open"])

    monkeypatch.setattr(pt, "_load_cached_kdata", fake_kdata)
    monkeypatch.setattr(pt, "_load_dal_weights", lambda sd: {"000001": 1.0})

    res = pt.run(invest_frac=0.5)
    # 段1 000001 +10%*0.5=+5% -> 1.05；段2 +9.09%*0.5=+4.55% -> ~1.0975
    assert res["final_value"] == pytest.approx(1.0975, abs=1e-3)
    assert res["invest_frac"] == 0.5
