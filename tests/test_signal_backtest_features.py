"""前向信号回测引擎三项收益/真实性改进的特性测试（离线、monkeypatch 数据源）。

覆盖：
  1. 波动率目标仓位(vol targeting)：同置信度下低波动票仓位 > 高波动票；关闭时约等权。
  2. 分批止盈(partial take-profit)：盈利达阈值先卖一部，余仓末批清仓 → 同一标的出现 2 笔成交。
  3. 跌停卖不出(model_limit_down)：封跌停日卖单顺延至次日，卖出日 > 跌停日。

均通过 monkeypatch smcore.data.kline.fetch_daily_k 注入合成行情，零联网、零硬编码依赖。
"""
from __future__ import annotations

from unittest import mock

import pandas as pd

import smcore.strategy.risk_rules as rr
from smcore.backtest.engine import run_forward_signal_backtest


def _fake_fetch(price_map: dict):
    def _fetch(code, start, end, **kw):
        return price_map.get(code)
    return _fetch


def _dates():
    return pd.date_range("2024-01-01", "2024-02-15", freq="D")


def _make_prices(closes: list[float], limit_down_idx: int | None = None,
                start: str = "2024-01-01") -> pd.DataFrame:
    """由收盘价序列构造日线 df（open=昨收；limit_down_idx 日封跌停：low=close=昨收*0.9）。

    start 指定行情起点；vol targeting 测试需较长历史（买入前 ≥21 根收盘才能算 20 日年化波动）。
    """
    n = len(closes)
    dates = pd.date_range(start, periods=n, freq="D")
    opens, highs, lows = [], [], []
    for i, c in enumerate(closes):
        if limit_down_idx is not None and i == limit_down_idx:
            c = (opens[-1] if opens else c) * 0.9
            closes[i] = c
            opens.append(c)
            highs.append(c)
            lows.append(c)
            continue
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


def _flat(start: float, n: int, noise: float) -> list[float]:
    out = [start]
    for _ in range(1, n):
        out.append(round(out[-1] * (1 + noise), 4))
    return out


def _signals(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _run(sig, price_map, **kw):
    """在注入合成行情的前提下运行前向回测（零联网）。"""
    with mock.patch("smcore.data.kline.fetch_daily_k", _fake_fetch(price_map)):
        return run_forward_signal_backtest(sig, **kw)


def test_vol_target_low_vol_gets_more():
    # 行情起点前移，保证买入日已有 ≥21 根收盘可算 20 日年化波动
    start = "2023-11-01"
    hist = pd.date_range(start, "2024-02-15", freq="D")
    n = len(hist)
    low = _flat(10.0, n, 0.001)
    high = [10.0]
    for i in range(1, n):
        high.append(round(high[-1] * (1 + (0.03 if i % 2 else -0.03)), 4))
    price_map = {"600000": _make_prices(low, start=start), "600111": _make_prices(high, start=start)}
    sig = _signals([
        {"日期": "2024-01-10", "代码": "600000", "来源策略": "boll"},
        {"日期": "2024-01-10", "代码": "600111", "来源策略": "boll"},
    ])

    off = _run(sig.copy(), price_map, hold_days=5, initial_capital=1_000_000,
               size_by=None, enable_exits=False, vol_target=False)
    on = _run(sig.copy(), price_map, hold_days=5, initial_capital=1_000_000,
              size_by=None, enable_exits=False, vol_target=True)

    assert len(off.trades) == 2, "equal 应成交2笔"
    assert len(on.trades) == 2, "vol 应成交2笔"
    q_off = dict(zip(off.trades["code"], off.trades["qty"]))
    q_on = dict(zip(on.trades["code"], on.trades["qty"]))
    # 两票均成交（开启/关闭都应买得进）
    assert q_off["600000"] > 0 and q_off["600111"] > 0
    # 开启时低波动票拿更多（波动率目标仓位核心效果）
    assert q_on["600000"] > q_on["600111"], f"vol开启时低波动应更多: {q_on}"
    assert q_on["600000"] >= q_off["600000"], f"低波动 vol开启应≥关闭: {q_on['600000']} vs {q_off['600000']}"


def test_partial_take_profit_splits_position():
    n = len(_dates())
    closes = [10.0]
    for i in range(1, n):
        if i <= 14:
            closes.append(round(closes[-1] * 1.01, 4))  # 温和 +1%/天
        else:
            closes.append(closes[14])  # 涨至 ~+5% 后走平：越过分批触发(4%)但未达固定止盈(6%)
    price_map = {"600200": _make_prices(closes)}
    sig = _signals([{"日期": "2024-01-10", "代码": "600200", "来源策略": "momentum"}])
    res = _run(sig.copy(), price_map, hold_days=30, initial_capital=1_000_000,
               size_by=None, enable_exits=True, vol_target=False, partial_take_profit=True,
               capital_scale=0.6)
    assert not res.trades.empty, "应有成交"
    t200 = res.trades[res.trades["code"] == "600200"]
    reasons = set(t200["exit_reason"])
    assert len(t200) >= 2, f"应拆成≥2笔: {t200.to_dict('records')}"
    assert "take_partial" in reasons, f"应有分批止盈: {reasons}"
    assert "take_partial_final" in reasons, f"应有末批清仓: {reasons}"
    first = t200.iloc[0]
    assert first["qty"] < int(t200["qty"].sum()), "首批应小于总量"


def test_limit_down_defers_sell():
    dates = _dates()
    n = len(dates)
    idx = 12
    closes = [10.0] * n
    closes[idx] = 9.0
    closes[idx + 1] = 8.95
    price_map = {"600300": _make_prices(closes, limit_down_idx=idx)}
    sig = _signals([{"日期": "2024-01-10", "代码": "600300", "来源策略": "boll", "stop_pct": 0.05}])

    off = _run(sig.copy(), price_map, hold_days=30, initial_capital=1_000_000,
               size_by=None, enable_exits=True, vol_target=False, model_limit_down=False,
               capital_scale=0.6)
    on = _run(sig.copy(), price_map, hold_days=30, initial_capital=1_000_000,
              size_by=None, enable_exits=True, vol_target=False, model_limit_down=True,
              capital_scale=0.6)

    assert not off.trades.empty and not on.trades.empty, "两种都应成交"
    t_off = off.trades[off.trades["code"] == "600300"]
    t_on = on.trades[on.trades["code"] == "600300"]
    off_sell = pd.Timestamp(t_off.iloc[0]["sell_date"])
    on_sell = pd.Timestamp(t_on.iloc[0]["sell_date"])
    ld_date = dates[idx]
    assert off_sell <= ld_date, f"关闭时跌停日即卖出: {off_sell}"
    assert on_sell > ld_date, f"开启时跌停日不可卖出、应顺延: {on_sell} > {ld_date}"


def test_risk_rules_vol_target_config_driven():
    # vol_target_scale 是纯函数（不读 enabled，enabled 由调用方判定）；配置驱动、无硬编码：
    # 改 CONFIG["vol_target"] 后 scale 随 target/min/max 变化并被 clamp。
    orig = rr.CONFIG
    try:
        rr.CONFIG = dict(orig)
        rr.CONFIG["vol_target"] = {
            "enabled": True, "target_annual_vol": 0.20, "window": 20,
            "min_scale": 0.5, "max_scale": 1.5,
        }
        # 低波动 10% → 0.20/0.10=2.0 被夹紧到 max_scale=1.5
        assert rr.vol_target_scale(0.10) == 1.5
        # 高波动 90% → 0.20/0.90=0.222 被夹紧到 min_scale=0.5
        assert rr.vol_target_scale(0.90) == 0.5
        # 命中区间：20% → 0.20/0.20=1.0（落在区间内）
        assert rr.vol_target_scale(0.20) == 1.0
        # 缺失波动 → 中性 1.0
        assert rr.vol_target_scale(None) == 1.0
    finally:
        rr.CONFIG = orig
