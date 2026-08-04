"""组合级回撤熔断：cash_from_drawdown 纯函数 + 跨信号日组合权益曲线重建。"""
from datetime import date

import daily_backtest  # noqa: E402  (scripts/ 经 conftest 注入 sys.path)
from daily_backtest import _PortfolioCurve, _load_equity_series  # noqa: E402
from smcore.strategy.adaptive_weights import cash_from_drawdown  # noqa: E402


# ── cash_from_drawdown 纯函数 ──
def test_cash_from_drawdown_below_threshold():
    assert cash_from_drawdown(0.0) == 0
    assert cash_from_drawdown(8.0) == 0      # 等于阈值不触发
    assert cash_from_drawdown(5.0) == 0
    assert cash_from_drawdown(None) == 0


def test_cash_from_drawdown_linear_ramp():
    # threshold=8, deep=20, cap=50 → dd=14 恰为中点 → 0.5*50=25
    assert cash_from_drawdown(14.0) == 25
    assert cash_from_drawdown(20.0) == 50    # 到达深崩阈值 → 封顶


def test_cash_from_drawdown_beyond_deep_and_custom():
    assert cash_from_drawdown(35.0) == 50    # 超过 deep → 封顶
    assert cash_from_drawdown(100.0) == 50
    # 自定义参数：threshold=5, deep=15, cap=40 → dd=10 中点 → 20
    assert cash_from_drawdown(10.0, threshold=5.0, cap=40.0, deep=15.0) == 20


# ── _PortfolioCurve 跨信号日重建 ──
def test_portfolio_curve_single_sleeve_drawdown():
    c = _PortfolioCurve()
    c.add_sleeve("A", {
        date(2026, 1, 1): 100000.0,
        date(2026, 1, 2): 110000.0,   # 峰值
        date(2026, 1, 3): 99000.0,    # (110-99)/110 = 10%
    })
    assert c.drawdown_as_of(date(2026, 1, 1)) == 0.0
    assert c.drawdown_as_of(date(2026, 1, 2)) == 0.0
    assert abs(c.drawdown_as_of(date(2026, 1, 3)) - 10.0) < 1e-6


def test_portfolio_curve_multi_sleeve_sum_and_replace():
    c = _PortfolioCurve()
    c.add_sleeve("A", {date(2026, 1, 1): 100000.0, date(2026, 1, 2): 90000.0})
    c.add_sleeve("B", {date(2026, 1, 1): 50000.0, date(2026, 1, 2): 60000.0})
    # 组合: 1/1=150000(峰值), 1/2=150000 → 无回撤
    assert c.drawdown_as_of(date(2026, 1, 2)) == 0.0
    # B 回落 → 组合 1/2 = 90000+30000=120000, 峰值150000 → (150-120)/150=20%
    c.add_sleeve("B", {date(2026, 1, 1): 50000.0, date(2026, 1, 2): 30000.0})
    assert abs(c.drawdown_as_of(date(2026, 1, 2)) - 20.0) < 1e-6


def test_portfolio_curve_causality_excludes_future_sleeve():
    # sleeve T 的权益从 T+1 才开始；用信号日 T 查询时不应包含 sleeve T 自身
    c = _PortfolioCurve()
    c.add_sleeve("T", {
        date(2026, 3, 2): 100000.0,   # T+1
        date(2026, 3, 3): 80000.0,    # T+2 深跌
    })
    # 信号日 T=2026-03-01 查询：sleeve T 在 03-01 无数据 → 曲线为空 → 回撤 0
    assert c.drawdown_as_of(date(2026, 3, 1)) == 0.0
    # 到 03-03 查询：sleeve T 已计入 → 回撤 20%
    assert abs(c.drawdown_as_of(date(2026, 3, 3)) - 20.0) < 1e-6


def test_load_equity_series_parses_date_total():
    import pandas as pd
    from pathlib import Path
    import tempfile

    df = pd.DataFrame({
        "date": ["2026-01-01", "2026-01-02"],
        "cash": [1000.0, 2000.0],
        "holding_value": [99000.0, 98000.0],
        "total": [100000.0, 100000.0],
    })
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "Multi-Backtest-20260101-equity.csv"
        df.to_csv(p, index=False, encoding="utf-8-sig")
        series = _load_equity_series(p)
    assert series[date(2026, 1, 1)] == 100000.0
    assert series[date(2026, 1, 2)] == 100000.0
    assert len(series) == 2
