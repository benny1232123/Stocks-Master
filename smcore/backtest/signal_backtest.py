"""Shared helpers for lightweight signal backtesting."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd

from smcore.data.kline import fetch_daily_k


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
    """Run a compact long-only backtest for signal rows."""
    if signals is None or signals.empty:
        return BacktestResult(summary={"error": "信号文件为空"}, equity=pd.DataFrame(), trades=pd.DataFrame())

    normalized = signals.copy()
    rename_map = {"日期": "date", "代码": "code", "建议买入价": "price"}
    normalized = normalized.rename(columns=rename_map)
    if "date" not in normalized.columns or "code" not in normalized.columns:
        return BacktestResult(summary={"error": "信号文件缺少「日期」或「代码」列"}, equity=pd.DataFrame(), trades=pd.DataFrame())

    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized = normalized.dropna(subset=["date", "code"]).sort_values("date")
    if normalized.empty:
        return BacktestResult(summary={"error": "信号文件无有效行"}, equity=pd.DataFrame(), trades=pd.DataFrame())

    trades: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    cash = float(initial_capital)
    holdings: dict[str, dict[str, Any]] = {}

    unique_dates = sorted(pd.to_datetime(normalized["date"]).dt.date.unique())
    for signal_date in unique_dates:
        day_signals = normalized[pd.to_datetime(normalized["date"]).dt.date == signal_date]

        to_sell: list[str] = []
        for code, holding in holdings.items():
            if (signal_date - holding["buy_date"]).days >= hold_days:
                to_sell.append(code)

        for code in to_sell:
            holding = holdings.pop(code)
            sell_date = signal_date
            start_date = holding["buy_date"]
            end_date = sell_date + timedelta(days=5)
            sell_df = fetch_daily_k(code, start_date, end_date)
            if sell_df.empty:
                continue
            sell_df["_dt"] = pd.to_datetime(sell_df["date"])
            sell_row = sell_df[sell_df["_dt"].dt.date <= sell_date]
            if sell_row.empty:
                continue
            sell_price = float(sell_row.iloc[-1]["close"]) * (1 - slippage)
            cash += sell_price * holding["qty"]
            trades.append(
                {
                    "code": code,
                    "buy_date": holding["buy_date"].strftime("%Y-%m-%d"),
                    "sell_date": sell_date.strftime("%Y-%m-%d"),
                    "buy_price": holding["buy_price"],
                    "sell_price": sell_price,
                    "qty": holding["qty"],
                    "return_pct": round((sell_price / holding["buy_price"] - 1) * 100, 2),
                }
            )

        available_slots = max_positions - len(holdings)
        buy_candidates = day_signals[~day_signals["code"].isin(holdings.keys())]
        if available_slots > 0 and not buy_candidates.empty:
            for _, signal in buy_candidates.head(available_slots).iterrows():
                code = str(signal["code"])
                start_date = signal_date
                kdf = fetch_daily_k(code, start_date, start_date + timedelta(days=10))
                if kdf.empty:
                    continue
                kdf["_dt"] = pd.to_datetime(kdf["date"])
                next_day = kdf[kdf["_dt"].dt.date > start_date]
                if next_day.empty:
                    continue
                buy_price = float(next_day.iloc[0]["open"]) * (1 + slippage)
                per_trade = cash / available_slots if available_slots > 0 else 0
                if per_trade <= 0:
                    continue
                qty = int(per_trade / buy_price / 100) * 100
                if qty < 100:
                    continue
                cost = buy_price * qty
                if cost > cash:
                    continue
                cash -= cost
                holdings[code] = {
                    "buy_date": signal_date,
                    "buy_price": buy_price,
                    "qty": qty,
                    "capital": cost,
                }
                available_slots -= 1

        holding_value = sum(holding["capital"] for holding in holdings.values())
        equity_curve.append(
            {
                "date": signal_date.strftime("%Y-%m-%d"),
                "cash": round(cash, 2),
                "holding_value": round(holding_value, 2),
                "total": round(cash + holding_value, 2),
            }
        )

    equity_df = pd.DataFrame(equity_curve)
    trades_df = pd.DataFrame(trades)
    if equity_df.empty:
        return BacktestResult(summary={"error": "回测未产生任何权益曲线"}, equity=equity_df, trades=trades_df)

    summary: dict[str, Any] = {
        "num_trades": int(len(trades_df)),
        "initial_capital": float(initial_capital),
        "ending_total": float(equity_df["total"].iloc[-1]),
    }

    summary["total_return"] = round((summary["ending_total"] / initial_capital - 1) * 100, 2)
    equity_df["peak"] = equity_df["total"].cummax()
    equity_df["drawdown"] = (equity_df["total"] - equity_df["peak"]) / equity_df["peak"] * 100
    summary["max_drawdown"] = round(float(equity_df["drawdown"].min()), 2)

    if not trades_df.empty:
        summary["win_rate"] = round(float((trades_df["return_pct"] > 0).mean() * 100), 1)
        summary["avg_return"] = round(float(trades_df["return_pct"].mean()), 2)
    else:
        summary["win_rate"] = 0.0
        summary["avg_return"] = 0.0

    if len(equity_df) > 1:
        equity_df["daily_return"] = equity_df["total"].pct_change()
        daily_std = equity_df["daily_return"].std()
        summary["sharpe"] = round(float(equity_df["daily_return"].mean() / daily_std * np.sqrt(252)) if daily_std and daily_std > 0 else 0.0, 2)
    else:
        summary["sharpe"] = 0.0

    return BacktestResult(summary=summary, equity=equity_df, trades=trades_df)