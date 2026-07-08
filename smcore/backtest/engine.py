"""多策略回测引擎入口 —— 组装数据/策略/经纪商并跑 Backtrader。

对外暴露 run_multi_strategy_backtest(...)，返回与 signal_backtest.run_signal_backtest
完全相同的 BacktestResult 结构（summary/equity/trades），前端无需改动即可展示。
"""
from __future__ import annotations

from datetime import date
from typing import Any, Optional

import numpy as np
import pandas as pd

import backtrader as bt

from smcore.backtest.loader import load_index_data, load_price_data
from smcore.backtest.signal_backtest import BacktestResult
from smcore.backtest.strategies import CNCommInfo, MultiStrategy, PriceData


def _build_summary(equity_df: pd.DataFrame, trades_df: pd.DataFrame, initial_capital: float) -> dict[str, Any]:
    if equity_df.empty:
        return {"error": "回测未产生权益曲线"}
    ending = float(equity_df["total"].iloc[-1])
    total_return = round((ending / initial_capital - 1) * 100, 2)

    equity_df = equity_df.copy()
    equity_df["peak"] = equity_df["total"].cummax()
    equity_df["drawdown"] = (equity_df["total"] - equity_df["peak"]) / equity_df["peak"] * 100
    max_dd = round(float(equity_df["drawdown"].min()), 2)

    num_trades = int(len(trades_df))
    if not trades_df.empty:
        win = round(float((trades_df["return_pct"] > 0).mean() * 100), 1)
        avg = round(float(trades_df["return_pct"].mean()), 2)
    else:
        win, avg = 0.0, 0.0

    sharpe = 0.0
    if len(equity_df) > 1:
        equity_df["daily_return"] = equity_df["total"].pct_change()
        std = equity_df["daily_return"].std()
        sharpe = round(float(equity_df["daily_return"].mean() / std * np.sqrt(252)), 2) if std and std > 0 else 0.0

    return {
        "num_trades": num_trades,
        "initial_capital": float(initial_capital),
        "ending_total": round(ending, 2),
        "total_return": total_return,
        "max_drawdown": max_dd,
        "win_rate": win,
        "avg_return": avg,
        "sharpe": sharpe,
        "strategies": None,  # 由 run_multi_strategy_backtest 回填
    }


def run_multi_strategy_backtest(
    codes: list[str],
    start: date,
    end: date,
    *,
    initial_capital: float = 100000.0,
    strategies: str = "boll,relativity,theme",
    cctv_hits: Optional[dict[str, int]] = None,
    commission: bool = True,
    **kw,
) -> BacktestResult:
    """多策略 Backtrader 回测。

    Args:
        codes: 股票代码列表（任意格式，内部格式化）
        start/end: 回测区间（date）
        initial_capital: 初始资金
        strategies: 启用的策略，逗号分隔（boll/relativity/theme/cctv）
        cctv_hits: 题材命中 dict（code->命中数），启用 cctv 时生效
        commission: 是否启用 A股佣金/印花税
        **kw: 透传给 MultiStrategy 的其他参数（如 boll_period、max_hold_days）

    Returns:
        BacktestResult（summary/equity/trades）
    """
    enabled = [s.strip().lower() for s in strategies.split(",") if s.strip()]
    if not enabled:
        return BacktestResult(summary={"error": "未启用任何策略"}, equity=pd.DataFrame(), trades=pd.DataFrame())

    # 1) 加载个股行情
    from smcore.utils.code import format_stock_code

    price_dfs: dict[str, pd.DataFrame] = {}
    for raw in codes:
        code = format_stock_code(raw)
        if not code:
            continue
        df = load_price_data(code, start, end)
        if df is not None:
            price_dfs[code] = df
    if not price_dfs:
        return BacktestResult(
            summary={"error": "无可用K线数据（全部拉取失败，可能网络不可达）"},
            equity=pd.DataFrame(),
            trades=pd.DataFrame(),
        )

    # 2) 加载指数（relativity 需要）
    idx_df = None
    if "relativity" in enabled:
        idx_df = load_index_data("000001", start, end)
    relativity_active = "relativity" in enabled and idx_df is not None and not idx_df.empty
    if "relativity" in enabled and not relativity_active:
        enabled = [s for s in enabled if s != "relativity"]

    # 3) 组装 Cerebro
    cerebro = bt.Cerebro(stdstats=False)
    cerebro.broker.setcash(float(initial_capital))
    if commission:
        cerebro.broker.addcommissioninfo(CNCommInfo())

    fromdate, todate = pd.Timestamp(start), pd.Timestamp(end)
    for code, df in price_dfs.items():
        data = PriceData(
            dataname=df.set_index("date"),
            name=code,
            fromdate=fromdate,
            todate=todate,
        )
        cerebro.adddata(data)
    if idx_df is not None and not idx_df.empty:
        cerebro.adddata(
            PriceData(dataname=idx_df.set_index("date"), name="idx", fromdate=fromdate, todate=todate)
        )

    # 4) 策略
    cerebro.addstrategy(
        MultiStrategy,
        strategies=",".join(enabled),
        cctv_hits=cctv_hits or {},
        **kw,
    )
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="dd")

    # 5) 运行
    results = cerebro.run()
    strat = results[0]

    # 6) 整理结果
    equity_df = pd.DataFrame(
        [
            {"date": d.strftime("%Y-%m-%d"), "cash": round(c, 2), "holding_value": round(t - c, 2), "total": round(t, 2)}
            for d, c, t in strat.value_hist
        ]
    )
    trades_df = pd.DataFrame(strat.trades)
    summary = _build_summary(equity_df, trades_df, initial_capital)
    summary["strategies"] = ",".join(enabled)
    summary["relativity_active"] = relativity_active
    summary["data_coverage"] = {code: len(df) for code, df in price_dfs.items()}

    return BacktestResult(summary=summary, equity=equity_df, trades=trades_df)
