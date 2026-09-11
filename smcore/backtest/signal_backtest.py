"""前向信号回测的对外入口与结果结构。

自 2026-09 起 `run_signal_backtest` 委托给 `engine.run_forward_signal_backtest`
（全项目唯一的前向信号回测实现），本模块只保留 `BacktestResult` 数据类与
旧签名兼容入口；撮合/成本/出场语义全部以 engine 为准，不再维护第二套实现。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class BacktestResult:
    summary: dict[str, Any]
    equity: pd.DataFrame
    trades: pd.DataFrame


def run_signal_backtest(
    signals: pd.DataFrame,
    hold_days: int = 5,
    initial_capital: float = 100000,
    max_positions: int = 10,
    slippage: float = 0.001,
) -> BacktestResult:
    """Run a compact long-only backtest for signal rows.

    自 2026-09 起实现**委托**给 `engine.run_forward_signal_backtest`（全项目唯一的
    前向信号回测引擎），消除多套回测口径漂移。委托后自动获得与生产一致的语义：
    信号日次日开盘买入、持有期按买入日起算、T+1、一字涨停放弃入场、超涨跌停坏 bar
    整只剔除、佣金/印花税/双边滑点。本函数仅保留旧签名（web API 兼容），固定等权、
    无主动出场（旧默认行为）；需要出场规则的调用方请直接使用主引擎。
    """
    # 延迟导入避免循环依赖（engine 顶层从本模块 import BacktestResult）
    from smcore.backtest.engine import run_forward_signal_backtest

    return run_forward_signal_backtest(
        signals,
        hold_days=int(hold_days),
        initial_capital=float(initial_capital),
        max_positions=int(max_positions),
        slippage=float(slippage),
        enable_exits=False,
        use_signal_bands=False,
        size_by=None,
    )
