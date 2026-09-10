"""基准同暴露口径单元测试（不依赖联网 / hs300 缓存）。

验证 compute_excess_vs_bench：
- 旧口径 excess = total - bench（满仓基准）
- 新口径 excess = total - bench*(1 - cash_pct/100)（同暴露）
- 市场下跌 + 有现金缓冲时，新口径不再把空仓误判为跑输基准。
"""
from __future__ import annotations

import pandas as pd
from datetime import date

from scripts.daily_backtest import compute_excess_vs_bench


def _hs(prices: dict) -> pd.Series:
    idx = pd.to_datetime(list(prices.keys()))
    return pd.Series(list(prices.values()), index=idx, name="close")


def test_market_up_excess_unchanged_when_fully_invested():
    hs = _hs({"2026-01-02": 100.0, "2026-01-16": 110.0})  # +10%
    ex = compute_excess_vs_bench(total_return=12.0, cash_pct=0.0, hs_series=hs,
                                 sd=date(2026, 1, 2), hold_days=14)
    assert ex["bench_return"] == 10.0
    assert ex["bench_return_exposed"] == 10.0
    assert ex["excess_return"] == 2.0  # 12 - 10（与旧口径一致，满仓无差）


def test_market_down_cash_buffer_no_longer_overstated():
    # 市场跌 10%，组合因含 30% 现金只跌 7%
    hs = _hs({"2026-01-02": 100.0, "2026-01-16": 90.0})  # -10%
    # 旧口径：excess = -7 - (-10) = +3 → 把「现金避险」误算成 +3 选股 alpha（高估）
    # 新口径：同暴露基准 = -10 × 0.7 = -7 → excess = -7 - (-7) = 0（避险≠alpha）
    ex = compute_excess_vs_bench(total_return=-7.0, cash_pct=30.0, hs_series=hs,
                                 sd=date(2026, 1, 2), hold_days=14)
    assert ex["bench_return_exposed"] == -7.0
    assert ex["excess_return"] == 0.0


def test_market_up_cash_drag_correctly_subtracted():
    # 市场涨 10%，组合含 30% 现金只涨 7% → 真实超额应为 -3（现金拖累），而非 +3（旧口径误判）
    hs = _hs({"2026-01-02": 100.0, "2026-01-16": 110.0})  # +10%
    ex = compute_excess_vs_bench(total_return=7.0, cash_pct=30.0, hs_series=hs,
                                 sd=date(2026, 1, 2), hold_days=14)
    # 同暴露基准 = +10 * 0.7 = +7 → excess = 7 - 7 = 0（现金拖累已计入，不虚高）
    assert ex["bench_return_exposed"] == 7.0
    assert ex["excess_return"] == 0.0  # 旧口径会算成 7-10 = -3（反而低估）


def test_no_bench_data_returns_none():
    assert compute_excess_vs_bench(5.0, 0.0, None, date(2026, 1, 2), 10) is None
