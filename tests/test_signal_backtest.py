"""信号回测：次日开盘撮合 + 滑点 + 持有期满卖出（无未来函数）的单元测试。

自 2026-09 起 `run_signal_backtest` 委托给 `engine.run_forward_signal_backtest`
（全项目唯一的前向信号回测引擎），本测试验证委托后的撮合语义：
- 信号日次日（第一个交易日）开盘价成交，加买滑点；
- 持有期按**买入日**起算，T+1（买入当日不卖）；
- 卖出按当日收盘减卖滑点（引擎对成交价做 3 位小数舍入）。
mock 打在数据源 `smcore.data.kline.fetch_daily_k` 上（引擎在调用时解析该名字）。
"""
import os
import sys
from datetime import date, timedelta
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from smcore.backtest.signal_backtest import run_signal_backtest

SLIP = 0.001


def _klines(start_open, n_days):
    base = date(2026, 1, 1)
    rows = []
    for i in range(n_days + 2):
        d = base + timedelta(days=i)
        rows.append(
            {
                "date": d.strftime("%Y-%m-%d"),
                "open": float(start_open + i),
                "close": float(start_open + i + 0.5),
            }
        )
    return pd.DataFrame(rows)


def test_next_day_open_buy_applies_slippage():
    """信号日 2026-01-01 应以次日(01-02)开盘价成交并加买滑点；T+1 后按收盘卖出。"""
    sig = pd.DataFrame(
        [
            {"日期": "2026-01-01", "代码": "000001", "建议买入价": 10.0},
            {"日期": "2026-01-02", "代码": "000001", "建议买入价": 10.0},
        ]
    )
    kdf = _klines(10, 6)  # 01-02 open=11, close=11.5；01-03 close=12.5

    def fake_fetch(code, start, end):
        return kdf.copy()

    with mock.patch("smcore.data.kline.fetch_daily_k", fake_fetch):
        res = run_signal_backtest(
            sig, hold_days=1, initial_capital=100000, max_positions=10, slippage=SLIP
        )

    assert not res.trades.empty, "应产生至少一笔卖出交易"
    t = res.trades.iloc[0]
    # 买入登记在处理日（信号日后第一个交易日）；成交价 = 次日开盘（11）+ 买滑点
    assert t["buy_date"] == "2026-01-02"
    assert abs(t["buy_price"] - 11 * (1 + SLIP)) < 1e-3
    # 持有期按买入日起算（T+1），满 1 个日历日后按当日收盘（12.5）− 卖滑点卖出
    assert t["sell_date"] == "2026-01-03"
    assert abs(t["sell_price"] - 12.5 * (1 - SLIP)) < 1e-3
    # 指标键存在（证明完整回测跑通）
    assert "max_drawdown" in res.summary
    assert "sharpe" in res.summary


def test_empty_signals_does_not_crash():
    res = run_signal_backtest(pd.DataFrame())
    assert "error" in res.summary
    res2 = run_signal_backtest(None)
    assert "error" in res2.summary
