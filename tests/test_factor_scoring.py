"""factor_scoring 单元测试：离线多因子打分的 z-score 方向与缺失安全。

因子全部离线（本地 k_data），用 monkeypatch 注入合成行情，不依赖网络。
"""
from __future__ import annotations

import pandas as pd
import pytest

import smcore.strategy.factor_scoring as fs
import smcore.strategy.risk_rules as rr


def _make_prices(closes, start="2024-01-01"):
    n = len(closes)
    dates = pd.date_range(start, periods=n, freq="D")
    opens, highs, lows = [], [], []
    for i, c in enumerate(closes):
        o = closes[i - 1] if i > 0 else c
        opens.append(o)
        highs.append(max(o, c) * 1.005)
        lows.append(min(o, c) * 0.995)
    return pd.DataFrame(
        {
            "date": [d.strftime("%Y-%m-%d") for d in dates],
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": [1e6] * n,
            "amount": [1e7] * n,
        }
    )


def _strong_up(n=80):
    return [round(10.0 * (1.005 ** i), 4) for i in range(n)]  # 强上行、低波动


def _weak_down_volatile(n=80):
    rets = [-0.03, 0.025, -0.04, 0.02, -0.02, 0.03, -0.035, 0.015] * 10
    out = [10.0]
    for i in range(1, n):
        out.append(round(out[-1] * (1 + rets[(i - 1) % len(rets)]), 4))
    return out  # 大幅下行、高波动


def _flat(n=80):
    return [round(10.0 + 0.3 * ((i % 2) * 2 - 1), 4) for i in range(n)]  # 横盘微波动


def _fake_fetch(pm: dict):
    def _f(code, start, end, **kw):
        return pm.get(str(code).strip())

    return _f


@pytest.fixture
def patched(monkeypatch):
    pm = {
        "600000": _make_prices(_strong_up()),
        "600111": _make_prices(_weak_down_volatile()),
        "600666": _make_prices(_flat()),
    }
    monkeypatch.setattr(fs, "fetch_daily_k", _fake_fetch(pm))
    return pm


def test_factor_scores_direction(patched):
    codes = ["600000", "600111", "600666"]
    params = rr.compute_factor_scoring_params()
    scores = fs.compute_factor_scores(codes, "2024-04-01", params)
    assert set(scores.keys()) == set(codes)
    # 高动量低波动票(600000) > 横盘(600666) > 低动量高波动票(600111)
    assert scores["600000"] > scores["600666"] > scores["600111"]
    # clamp 生效
    assert all(-params["max_bonus"] <= v <= params["max_bonus"] for v in scores.values())


def test_factor_scores_missing_safe(patched, monkeypatch):
    # 加入一只无行情的票（fetch 返回 None），不应抛异常，其分值记为 0
    pm = dict(patched)
    monkeypatch.setattr(fs, "fetch_daily_k", _fake_fetch(pm))
    codes = ["600000", "600111", "600666", "000999"]  # 000999 不在缓存
    params = rr.compute_factor_scoring_params()
    scores = fs.compute_factor_scores(codes, "2024-04-01", params)
    assert "000999" in scores
    assert scores["000999"] == 0.0
    # 其余票方向不变
    assert scores["600000"] > scores["600111"]


def test_factor_scoring_params_from_config():
    p = rr.compute_factor_scoring_params()
    assert "enabled" in p
    assert p["w_momentum_20"] == 1.0
    assert p["w_volatility"] == -0.4  # 低波动加分（负向权重）
    assert p["max_bonus"] == 15.0


def test_volatility_weight_sign(patched):
    """波动率权重为负：低波动票在 vol 维度应比高波动票得分高（控制其他因子一致时）。"""
    # 两只票：动量/流动性相同，仅波动率不同
    low_vol = [round(10.0 * (1.001 ** i), 4) for i in range(80)]
    high_vol = []
    v = 10.0
    for i in range(80):
        v = v * (1 + (-0.02 if i % 2 else 0.02))  # 高波动横盘
        high_vol.append(round(v, 4))
    pm = {"L": _make_prices(low_vol), "H": _make_prices(high_vol)}
    patched.clear()
    patched.update(pm)
    params = rr.compute_factor_scoring_params()
    # 只让波动率因子生效，隔离其他因子
    params = dict(params)
    params.update(
        w_momentum_20=0.0, w_momentum_60=0.0, w_rel_strength=0.0, w_liquidity=0.0
    )
    scores = fs.compute_factor_scores(["L", "H"], "2024-04-01", params)
    assert scores["L"] > scores["H"]  # 低波动得正分、高波动得负分
