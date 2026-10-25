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
    res = sdm.analyze(recent_window=10, pool_status={})
    assert "theme" in res["decayed"], "theme 近期均值<0且胜率<45 应被标记"
    assert "boll" not in res["decayed"]
    # momentum 近期样本仅 2 < MIN_N_EDGE(5)，不应误报
    assert "momentum" not in res["decayed"]
    assert res["strategies"]["theme"]["reason"]


def test_analyze_no_days(monkeypatch):
    monkeypatch.setattr(sdm, "_all_signal_days", lambda: [])
    res = sdm.analyze(pool_status={})
    assert res["error"]
    assert res["decayed"] == []


def test_analyze_zero_sample_strategy_no_crash(monkeypatch):
    """零样本策略 win_rate=None（_aggregate_excess n=0 口径）不得让监控崩。

    2026-10-13 实录：illiq20 在 20261009 口径下近期 n=0 → win_rate=None →
    main() 打印循环 TypeError，monitor.yml「策略衰减监控」步整体红，
    衰减 issue 也无法刷新。本测试钉住 analyze/打印/正文三条路径均容忍 None。
    """
    days = [f"202601{i:02d}" for i in range(1, 16)]
    monkeypatch.setattr(sdm, "_all_signal_days", lambda: days)

    def fake_edge(cutoff, window=10):
        if window >= 10:
            return {
                "theme": {"n": 8, "avg_return": -3.0, "win_rate": 30.0, "edge": -3.0},
                # n=0 → _aggregate_excess 返回 win_rate=None
                "illiq20": {"n": 0, "avg_return": 0.0, "win_rate": None, "edge": 0.0},
            }
        return {"theme": {"n": 20, "avg_return": -1.0, "win_rate": 40.0, "edge": -1.0}}

    monkeypatch.setattr(sdm, "causal_edge", fake_edge)
    res = sdm.analyze(recent_window=10, pool_status={})
    assert res["strategies"]["illiq20"]["recent_win_rate"] is None
    assert "illiq20" not in res["decayed"], "零样本不得判衰减"
    assert "theme" in res["decayed"]
    # issue 正文与打印路径都不得抛异常
    body = sdm._format_issue_body(res)
    assert "n/a" in body
    sdm._format_issue_body(res)


def test_issue_body_format(monkeypatch):
    days = [f"202601{i:02d}" for i in range(1, 16)]
    monkeypatch.setattr(sdm, "_all_signal_days", lambda: days)
    monkeypatch.setattr(sdm, "causal_edge", lambda cutoff, window=10: {
        "theme": {"n": 8, "avg_return": -3.0, "win_rate": 30.0, "edge": -3.0},
    })
    res = sdm.analyze(pool_status={})
    body = sdm._format_issue_body(res)
    assert "策略衰减告警" in body
    assert "theme" in body
    assert "| 策略 |" in body




def test_analyze_pool_status_passthrough(monkeypatch):
    """analyze() 原样透出 pool_status；零样本归因表用它区分停摆与清零。"""
    days = [f"202601{i:02d}" for i in range(1, 16)]
    monkeypatch.setattr(sdm, "_all_signal_days", lambda: days)

    def fake_edge(cutoff, window=10):
        if window >= 10:
            # illiq20 零样本（负 edge 清零后的典型形态）
            return {"illiq20": {"n": 0, "avg_return": 0.0, "win_rate": None, "edge": 0.0}}
        return {}

    monkeypatch.setattr(sdm, "causal_edge", fake_edge)
    ps = {"illiq20": {"status": "ok", "days_with_csv": 10, "days_total": 10,
                      "median_picks": 40, "latest_date": "20260115"}}
    res = sdm.analyze(recent_window=10, pool_status=ps)
    assert res["pool_status"] == ps
    assert res["zero_sample"] == ["illiq20"]
    assert "illiq20" not in res["decayed"], "零样本不得判衰减"
    body = sdm._format_issue_body(res)
    assert "候选池正常" in body
    assert "清零" in body


def test_pool_status_ok_and_stalled(monkeypatch, tmp_path):
    """候选池状态判定：多数日期有票=ok；全程无票=stalled。"""
    from smcore.config import defaults as _defaults
    from smcore.strategy.factor_types import STRATEGY_LABEL

    monkeypatch.setattr(_defaults, "STOCK_DATA_DIR", tmp_path)
    days = ["20260101", "20260102", "20260103", "20260104"]
    label = STRATEGY_LABEL["pvcorr20"]
    for d in days:
        (tmp_path / f"Stock-Selection-{label}-{d}.csv").write_text(
            "股票代码,股票名称,综合分\n000001,平安银行,100\n", encoding="utf-8")
    ps = sdm._pool_status(days)
    assert ps["pvcorr20"]["status"] == "ok"
    assert ps["pvcorr20"]["days_with_csv"] == 4
    assert ps["pvcorr20"]["median_picks"] == 1
    assert ps["pvcorr20"]["latest_date"] == "20260104"
    # illiq20 全程无文件 → stalled
    assert ps["illiq20"]["status"] == "stalled"
    assert ps["illiq20"]["days_with_csv"] == 0


def test_safe_pool_status_fail_soft(monkeypatch):
    """_pool_status 抛异常时 _safe_pool_status 返回 {}，绝不阻塞监控。"""
    def boom(_):
        raise RuntimeError("disk gone")

    monkeypatch.setattr(sdm, "_pool_status", boom)
    assert sdm._safe_pool_status(["20260101"]) == {}

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
