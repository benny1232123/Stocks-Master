"""回测模块。

保留原有轻量 signal 回测（run_signal_backtest），并新增多策略 Backtrader 引擎
（run_multi_strategy_backtest）。两者返回结构一致（BacktestResult: summary/equity/trades）。
"""
from __future__ import annotations

from smcore.backtest.engine import run_forward_signal_backtest, run_multi_strategy_backtest
from smcore.backtest.signal_backtest import BacktestResult, run_signal_backtest

__all__ = [
    "BacktestResult",
    "run_signal_backtest",
    "run_multi_strategy_backtest",
    "run_forward_signal_backtest",
]
