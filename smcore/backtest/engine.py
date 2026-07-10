"""多策略回测引擎入口 —— 组装数据/策略/经纪商并跑 Backtrader。

对外暴露 run_multi_strategy_backtest(...)，返回与 signal_backtest.run_signal_backtest
完全相同的 BacktestResult 结构（summary/equity/trades），前端无需改动即可展示。
"""
from __future__ import annotations

from datetime import date, timedelta
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


def run_forward_signal_backtest(
    signals: pd.DataFrame,
    *,
    hold_days: int = 5,
    initial_capital: float = 100000.0,
    max_positions: int = 200,
    slippage: float = 0.001,
) -> "BacktestResult":
    """前向信号回测：锁定历史某天的信号清单，从信号日起往后持有 hold_days 天，回测真实表现。

    与 run_multi_strategy_backtest（在历史区间里重跑策略引擎重新派生信号）不同，
    本函数直接使用信号清单里的标的与信号日，模拟：
      - 信号日次日开盘买入（真实往前走，不用未来数据）
      - 持有 hold_days 个日历日后卖出
      - 按交易日盯市（收盘价）生成平滑权益曲线
    语义 = 「从历史某一天开始策略 → 往后回测」。
    """
    from collections import defaultdict

    from smcore.data.kline import fetch_daily_k

    if signals is None or signals.empty:
        return BacktestResult(summary={"error": "信号文件为空"}, equity=pd.DataFrame(), trades=pd.DataFrame())

    norm = signals.copy()
    rename_map = {"日期": "date", "代码": "code", "建议买入价": "price"}
    norm = norm.rename(columns=rename_map)
    if "date" not in norm.columns or "code" not in norm.columns:
        return BacktestResult(summary={"error": "信号文件缺少「日期」或「代码」列"}, equity=pd.DataFrame(), trades=pd.DataFrame())

    norm["date"] = pd.to_datetime(norm["date"], errors="coerce")
    norm = norm.dropna(subset=["date", "code"]).sort_values("date")
    if norm.empty:
        return BacktestResult(summary={"error": "信号文件无有效行"}, equity=pd.DataFrame(), trades=pd.DataFrame())

    # 按信号日分组
    raw_by_date: dict[date, list[str]] = defaultdict(list)
    for _, row in norm.iterrows():
        raw_by_date[row["date"].date()].append(str(row["code"]).strip())

    min_sig = min(raw_by_date.keys())
    max_sig = max(raw_by_date.keys())
    # 持有期之后的缓冲区间：远端未来无 K 线数据，拉取必失败且易挂起（akshare 限流），
    # 故 end_pad 不超过「今天 + 5 天」即可覆盖真实持有期，避免无谓的远未来网络请求。
    end_pad = min(max_sig + timedelta(days=hold_days + 15), date.today() + timedelta(days=5))

    # 预拉每只标的 K 线（信号区间 + 持有期 + 缓冲），供盯市与买卖价查询
    price_cache: dict[str, pd.DataFrame] = {}
    all_dates: set[date] = set()
    for code in norm["code"].astype(str).str.strip().unique():
        df = fetch_daily_k(code, min_sig, end_pad)
        if df is not None and not df.empty:
            df = df.copy()
            df["_dt"] = pd.to_datetime(df["date"])
            price_cache[code] = df.set_index("_dt").sort_index()
            all_dates.update(df["_dt"].dt.date.tolist())

    if not price_cache:
        return BacktestResult(
            summary={"error": "无可用K线数据（全部拉取失败，可能网络不可达）"},
            equity=pd.DataFrame(),
            trades=pd.DataFrame(),
        )

    cal = sorted(all_dates)

    def _px(code: str, d: date, col: str):
        p = price_cache.get(code)
        if p is None or p.empty:
            return None
        row = p[p.index.date == d]
        if row.empty:
            return None
        val = row[col].iloc[0]
        return None if pd.isna(val) else float(val)

    # 买入调度：每个信号日 → 其「之后第一个交易日」作为买入处理日（即信号日次日开盘买入）。
    # 信号日本身可能不是交易日（周末/休市），不能直接用信号日作为交易日历中的 key。
    buy_schedule: dict[date, list[str]] = defaultdict(list)
    for sd, codes in raw_by_date.items():
        proc = None
        for d in cal:
            if d > sd:
                proc = d
                break
        if proc is not None:
            buy_schedule[proc].extend(codes)

    cash = float(initial_capital)
    holdings: dict[str, dict] = {}
    equity_curve: list[dict] = []
    trades: list[dict] = []

    for d in cal:
        # 1) 卖出到期持仓
        to_sell = [c for c, h in holdings.items() if (d - h["buy_date"]).days >= hold_days]
        for c in to_sell:
            h = holdings.pop(c)
            sell_price = _px(c, d, "close")
            if sell_price is None:
                sell_price = h["buy_price"]  # 极端缺数据兜底
            sell_price *= (1 - slippage)
            cash += sell_price * h["qty"]
            trades.append(
                {
                    "code": c,
                    "buy_date": h["buy_date"].strftime("%Y-%m-%d"),
                    "sell_date": d.strftime("%Y-%m-%d"),
                    "buy_price": round(h["buy_price"], 3),
                    "sell_price": round(sell_price, 3),
                    "qty": h["qty"],
                    "return_pct": round((sell_price / h["buy_price"] - 1) * 100, 2),
                }
            )

        # 2) 信号日次日开盘买入（处理日 = 信号日后第一个交易日，用其开盘价）
        if d in buy_schedule:
            candidates = [c for c in buy_schedule[d] if c not in holdings]
            avail = max_positions - len(holdings)
            n_buy = min(avail, len(candidates))
            per = cash / max(n_buy, 1)  # 按当日实际买入数量均分资金
            for c in candidates[:avail]:
                buy_price = _px(c, d, "open")
                if buy_price is None:
                    continue
                buy_price *= (1 + slippage)
                qty = int(per / buy_price / 100) * 100
                if qty < 100:
                    continue
                cost = buy_price * qty
                if cost > cash:
                    continue
                cash -= cost
                holdings[c] = {"buy_date": d, "buy_price": buy_price, "qty": qty}
                avail -= 1

        # 3) 按日盯市（仅对已买入持仓计价；买入前的持仓不计入）
        hv = 0.0
        for c, h in holdings.items():
            if h["buy_date"] > d:
                continue
            close = _px(c, d, "close")
            hv += h["qty"] * (close if close is not None else h["buy_price"])
        equity_curve.append(
            {
                "date": d.strftime("%Y-%m-%d"),
                "cash": round(cash, 2),
                "holding_value": round(hv, 2),
                "total": round(cash + hv, 2),
            }
        )

    equity_df = pd.DataFrame(equity_curve)
    trades_df = pd.DataFrame(trades)
    if equity_df.empty:
        return BacktestResult(summary={"error": "回测未产生任何权益曲线"}, equity=equity_df, trades=trades_df)

    summary = _build_summary(equity_df, trades_df, initial_capital)
    summary["strategies"] = None
    summary["signal_mode"] = "forward"
    return BacktestResult(summary=summary, equity=equity_df, trades=trades_df)
