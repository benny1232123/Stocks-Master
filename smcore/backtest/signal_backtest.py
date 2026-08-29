"""Shared helpers for lightweight signal backtesting."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Optional

import numpy as np
import pandas as pd

from smcore.data.kline import fetch_daily_k

# A 股真实交易成本（与 run_forward_signal_backtest 同口径）：
# 佣金万2.5（单笔最低5元）+ 卖出印花税千0.5；买卖另加滑点（调用方传入）。
_COMM_RATE = 0.00025
_COMM_MIN = 5.0
_STAMP_RATE = 0.0005  # 仅卖出征收


def _buy_cost(amount: float) -> float:
    return max(amount * _COMM_RATE, _COMM_MIN)


def _sell_cost(amount: float) -> float:
    return max(amount * _COMM_RATE, _COMM_MIN) + amount * _STAMP_RATE


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

    撮合语义（与旧版一致）：信号日次日开盘买入（登记在信号日）、持有满
    hold_days 个日历日的当日收盘卖出。修复点（相对旧版）：
    - 权益曲线按收盘价盯市（旧版持仓按买入成本计价，回撤/夏普/总收益全部失真）；
    - 计入 A 股佣金/印花税 + 双边滑点（旧版零成本）；
    - K 线预取一次并进程内复用（旧版每笔买卖各拉一次，慢且易被限流）；
    - 权益曲线按交易日历逐日生成（旧版仅信号日有采样点，回撤被低估）。
    """
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

    # 按信号日分组（保序），并预取全部标的 K 线（信号区间 + 持有期 + 缓冲）
    sig_by_date: dict[date, list[str]] = {}
    for _, row in normalized.iterrows():
        sd = row["date"].date()
        code = str(row["code"]).strip()
        if code:
            sig_by_date.setdefault(sd, []).append(code)

    min_sd = min(sig_by_date.keys())
    max_sd = max(sig_by_date.keys())
    fetch_end = max_sd + timedelta(days=int(hold_days) + 15)

    price_cache: dict[str, pd.DataFrame] = {}
    for code in sorted({c for cs in sig_by_date.values() for c in cs}):
        try:
            df = fetch_daily_k(code, min_sd - timedelta(days=10), fetch_end)
        except Exception:
            df = None
        if df is None or df.empty:
            continue
        df = df.copy()
        df["_dt"] = pd.to_datetime(df["date"])
        price_cache[code] = df.set_index("_dt").sort_index()

    if not price_cache:
        return BacktestResult(
            summary={"error": "无可用K线数据（全部拉取失败，可能网络不可达）"},
            equity=pd.DataFrame(),
            trades=pd.DataFrame(),
        )

    all_dates: set[date] = set()
    for df in price_cache.values():
        all_dates.update(df.index.date.tolist())
    cal = sorted(all_dates)

    # 买入调度：每个信号日 → 其后第一个交易日开盘买入（持有期从信号日起算，与旧版一致）
    buy_schedule: dict[date, list[tuple[date, str]]] = {}
    for sd, codes in sig_by_date.items():
        proc = next((d for d in cal if d > sd), None)
        if proc is not None:
            buy_schedule.setdefault(proc, []).extend((sd, c) for c in codes)

    def _px_last(code: str, d: date, col: str) -> Optional[float]:
        """code 在 ≤ d 的最近一条 K 线上的 col 值；缺失返回 None。"""
        p = price_cache.get(code)
        if p is None or p.empty:
            return None
        sub = p.loc[p.index.date <= d]
        if sub.empty:
            return None
        val = sub[col].iloc[-1]
        return None if pd.isna(val) else float(val)

    cash = float(initial_capital)
    holdings: dict[str, dict[str, Any]] = {}
    trades: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []

    for d in cal:
        # 1) 买入（次日开盘价 + 买滑点，计佣金）；等权按剩余槽位分配现金
        for sd_sig, code in buy_schedule.get(d, []):
            if code in holdings or code not in price_cache:
                continue
            available_slots = max_positions - len(holdings)
            if available_slots <= 0:
                break
            buy_price = _px_last(code, d, "open")
            if buy_price is None:
                continue
            buy_price *= 1 + slippage
            per_trade = cash / available_slots
            if per_trade <= 0:
                continue
            qty = int(per_trade / buy_price / 100) * 100
            if qty < 100:
                continue
            cost = buy_price * qty
            fee = _buy_cost(cost)
            if cost + fee > cash:
                continue
            cash -= cost + fee
            holdings[code] = {"sd": sd_sig, "buy_price": buy_price, "qty": qty}

        # 2) 持有期满卖出（收盘价 − 卖滑点，计佣金+印花税）
        to_sell = [
            c
            for c, h in holdings.items()
            if (d - h["sd"]).days >= hold_days
        ]
        for code in to_sell:
            h = holdings.pop(code)
            sell_price = _px_last(code, d, "close")
            if sell_price is None:
                sell_price = h["buy_price"]  # 缺数据兜底
            sell_price *= 1 - slippage
            proceeds = sell_price * h["qty"]
            cash += proceeds - _sell_cost(proceeds)
            trades.append(
                {
                    "code": code,
                    "buy_date": h["sd"].strftime("%Y-%m-%d"),
                    "sell_date": d.strftime("%Y-%m-%d"),
                    "buy_price": h["buy_price"],
                    "sell_price": sell_price,
                    "qty": h["qty"],
                    "return_pct": round((sell_price / h["buy_price"] - 1) * 100, 2),
                }
            )

        # 3) 按收盘价盯市（缺当日价沿用最近收盘，杜绝旧版「按成本计价」的假平稳曲线）
        holding_value = 0.0
        for code, h in holdings.items():
            close = _px_last(code, d, "close")
            holding_value += h["qty"] * (close if close is not None else h["buy_price"])
        equity_curve.append(
            {
                "date": d.strftime("%Y-%m-%d"),
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

    # 样本太短（<60 个交易日）时夏普无统计意义，输出 None 而不是年化噪声
    if len(equity_df) >= 60:
        equity_df["daily_return"] = equity_df["total"].pct_change()
        daily_std = equity_df["daily_return"].std()
        summary["sharpe"] = round(float(equity_df["daily_return"].mean() / daily_std * np.sqrt(252)) if daily_std and daily_std > 0 else 0.0, 2)
    else:
        summary["sharpe"] = None

    return BacktestResult(summary=summary, equity=equity_df, trades=trades_df)
