"""replay_current_rules 的纯函数单测（避免联网/IO）。"""
import pandas as pd
import pytest

import replay_current_rules as rp


def test_weights_from_df_skips_zero():
    df = pd.DataFrame([
        {"股票代码": "000001", "建议仓位%": 10.0},
        {"股票代码": "000002", "建议仓位%": 0.0},
        {"股票代码": "000003", "建议仓位%": 5.0},
    ])
    w = rp._weights_from_df(df)
    assert w == {"000001": 10.0, "000003": 5.0}
    assert "000002" not in w


def test_paper_curve_math(monkeypatch):
    # 固定每段 +10%，且所有票权重相同 -> 组合 +10%/段
    days = ["20260101", "20260102", "20260103"]
    monkeypatch.setattr(rp, "_stock_return_between", lambda code, a, b: 10.0)
    wmap = {"000001": 50.0, "000002": 50.0}
    by_day = {d: wmap for d in days}
    res = rp._paper_curve(by_day, days)
    # 2 段，每段 +10% -> 1.21
    assert res["final_value"] == pytest.approx(1.21, abs=1e-6)
    assert res["total_return_pct"] == pytest.approx(21.0, abs=1e-4)
    assert res["realized_periods"] == 2
    assert res["max_drawdown_pct"] == pytest.approx(0.0, abs=1e-6)
