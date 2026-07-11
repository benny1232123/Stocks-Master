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

# A 股真实交易成本：佣金万2.5（单笔最低5元）+ 卖出印花税千0.5
_COMM_RATE = 0.00025
_COMM_MIN = 5.0
_STAMP_RATE = 0.0005  # 仅卖出征收


def _buy_cost(amount: float) -> float:
    """买入费用（佣金，最低5元）。"""
    return max(amount * _COMM_RATE, _COMM_MIN)


def _sell_cost(amount: float) -> float:
    """卖出费用（佣金 + 印花税）。"""
    return max(amount * _COMM_RATE, _COMM_MIN) + amount * _STAMP_RATE


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
        wins = trades_df.loc[trades_df["return_pct"] > 0, "return_pct"]
        losses = trades_df.loc[trades_df["return_pct"] < 0, "return_pct"]
        avg_win = round(float(wins.mean()), 2) if len(wins) else 0.0
        avg_loss = round(float(losses.mean()), 2) if len(losses) else 0.0
        gross_profit = float(wins.sum())
        gross_loss = float(-losses.sum())
        if gross_loss > 0:
            profit_factor = round(gross_profit / gross_loss, 2)
        elif gross_profit > 0:
            profit_factor = 99.99  # 无亏损，盈亏比视作极高
        else:
            profit_factor = 0.0
    else:
        win, avg, avg_win, avg_loss, profit_factor = 0.0, 0.0, 0.0, 0.0, 0.0

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
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "profit_factor": profit_factor,
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
    enable_exits: bool = False,
    use_signal_bands: bool = False,
    stop_loss_pct: Optional[float] = None,
    take_profit_pct: Optional[float] = None,
    trailing_stop_pct: Optional[float] = None,
) -> "BacktestResult":
    """前向信号回测：锁定历史某天的信号清单，从信号日起往后持有，回测真实表现。

    与 run_multi_strategy_backtest（在历史区间里重跑策略引擎重新派生信号）不同，
    本函数直接使用信号清单里的标的与信号日，模拟：
      - 信号日次日开盘买入（真实往前走，不用未来数据）
      - 按交易日盯市（收盘价）生成平滑权益曲线

    退出规则（enable_exits=True 时生效）：
      - 止盈：优先 Boll 上轨（清单「止盈价(上轨)」列，均值回归目标），其次固定比例
      - 止损：固定比例 stop_loss_pct（Boll 下轨≈入场价、过紧易频繁假止损，不单独用作止损）
      - 移动止盈 trailing_stop_pct（从持仓期间最高收盘价回撤超阈值即离场，锁定利润）
      - 持有满 hold_days 个日历日强制平仓（兜底上限）
    默认 enable_exits=False 时行为与改动前一致（仅按持有天数平仓），保证向后兼容。
    """
    from collections import defaultdict

    from smcore.data.kline import fetch_daily_k

    if signals is None or signals.empty:
        return BacktestResult(summary={"error": "信号文件为空"}, equity=pd.DataFrame(), trades=pd.DataFrame())

    norm = signals.copy()
    rename_map = {
        "日期": "date",
        "代码": "code",
        "建议买入价": "price",
        "止损价(下轨)": "stop_price",
        "止盈价(上轨)": "take_price",
        "止损价": "stop_price",
        "止盈价": "take_price",
    }
    norm = norm.rename(columns=rename_map)
    if "date" not in norm.columns or "code" not in norm.columns:
        return BacktestResult(summary={"error": "信号文件缺少「日期」或「代码」列"}, equity=pd.DataFrame(), trades=pd.DataFrame())

    norm["date"] = pd.to_datetime(norm["date"], errors="coerce")
    norm = norm.dropna(subset=["date", "code"]).sort_values("date")
    if norm.empty:
        return BacktestResult(summary={"error": "信号文件无有效行"}, equity=pd.DataFrame(), trades=pd.DataFrame())

    def _to_f(x):
        try:
            if x is None or (isinstance(x, float) and pd.isna(x)):
                return None
            v = float(x)
            return None if pd.isna(v) else v
        except (TypeError, ValueError):
            return None

    # 按信号日分组，并记录每只标的的止损/止盈水位（来自操作清单）
    raw_by_date: dict[date, list[str]] = defaultdict(list)
    level_map: dict[tuple[date, str], tuple[Optional[float], Optional[float]]] = {}
    for _, row in norm.iterrows():
        sd = row["date"].date()
        code = str(row["code"]).strip()
        raw_by_date[sd].append(code)
        level_map[(sd, code)] = (_to_f(row.get("stop_price")), _to_f(row.get("take_price")))

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
    buy_schedule: dict[date, list[tuple[date, str]]] = defaultdict(list)
    for sd, codes in raw_by_date.items():
        proc = None
        for d in cal:
            if d > sd:
                proc = d
                break
        if proc is not None:
            for code in codes:
                buy_schedule[proc].append((sd, code))

    cash = float(initial_capital)
    holdings: dict[str, dict] = {}
    equity_curve: list[dict] = []
    trades: list[dict] = []

    for d in cal:
        # 1) 信号日次日开盘买入（处理日 = 信号日后第一个交易日，用其开盘价）
        if d in buy_schedule:
            candidates = [(sd, c) for (sd, c) in buy_schedule[d] if c not in holdings]
            avail = max_positions - len(holdings)
            n_buy = min(avail, len(candidates))
            per = cash / max(n_buy, 1)  # 按当日实际买入数量均分资金
            for sd, c in candidates[:avail]:
                buy_price = _px(c, d, "open")
                if buy_price is None:
                    continue
                buy_price *= (1 + slippage)
                qty = int(per / buy_price / 100) * 100
                if qty < 100:
                    continue
                cost = buy_price * qty
                fee = _buy_cost(cost)
                if cost + fee > cash:
                    continue
                cash -= cost + fee
                stop, take = level_map.get((sd, c), (None, None))
                holdings[c] = {
                    "buy_date": d,
                    "buy_price": buy_price,
                    "qty": qty,
                    "stop": stop,
                    "take": take,
                    "peak": buy_price,
                    "sd": sd,
                }
                avail -= 1

        # 2) 退出检查（基于当日收盘价）：止盈 / 止损 / 移动止盈 / 持有期满
        for c, h in holdings.items():
            close = _px(c, d, "close")
            if close is None:
                continue  # 缺数据则暂不处理
            h["peak"] = max(h.get("peak", close), close)
            exit_reason = None
            if enable_exits:
                # 硬止损（缺口感知）：盘中最低价触及 -stop_loss_pct 即以 min(开盘价,止损价)
                # 离场，封顶亏损≈stop_loss_pct，挡住跳空低开直接击穿收盘止损的巨亏（如单日 -23%）。
                if stop_loss_pct is not None and (d - h["buy_date"]).days >= 1:
                    low = _px(c, d, "low")
                    if low is not None:
                        hard_stop = h["buy_price"] * (1 - abs(stop_loss_pct))
                        if low <= hard_stop:
                            open_px = _px(c, d, "open")
                            exit_px = min(open_px, hard_stop) if open_px is not None else hard_stop
                            h["forced_sell_price"] = exit_px
                            exit_reason = "stop_hard"
                if exit_reason is None:
                    ret = close / h["buy_price"] - 1
                    # 止盈：优先 Boll 上轨（均值回归目标），其次固定比例
                    if use_signal_bands and h.get("take") is not None and close >= h["take"]:
                        exit_reason = "take_band"
                    elif take_profit_pct is not None and ret >= abs(take_profit_pct):
                        exit_reason = "take_pct"
                    # 止损：固定比例（Boll 下轨≈入场价、过紧易频繁假止损，不单独用作止损）
                    elif stop_loss_pct is not None and ret <= -abs(stop_loss_pct):
                        exit_reason = "stop_pct"
                    # 移动止盈：从峰值回撤锁定利润
                    elif trailing_stop_pct is not None and h["peak"] > 0 and (close / h["peak"] - 1) <= -abs(trailing_stop_pct):
                        exit_reason = "trailing"
                    # 最后防线：仅当 Boll 下轨明显低于入场价（>3%）时才用作硬止损
                    elif use_signal_bands and h.get("stop") is not None and h["stop"] < h["buy_price"] * 0.97 and close <= h["stop"]:
                        exit_reason = "stop_band"
            if exit_reason is None and (d - h["buy_date"]).days >= hold_days:
                exit_reason = "max_hold"  # 持有期满兜底
            if exit_reason is not None:
                h["exit_reason"] = exit_reason

        to_sell = [c for c, h in holdings.items() if "exit_reason" in h]
        for c in to_sell:
            h = holdings.pop(c)
            if h.get("forced_sell_price") is not None:
                sell_price = h["forced_sell_price"]
            else:
                sell_price = _px(c, d, "close")
                if sell_price is None:
                    sell_price = h["buy_price"]  # 极端缺数据兜底
            sell_price *= (1 - slippage)
            proceeds = sell_price * h["qty"]
            cash += proceeds - _sell_cost(proceeds)
            trades.append(
                {
                    "code": c,
                    "buy_date": h["buy_date"].strftime("%Y-%m-%d"),
                    "sell_date": d.strftime("%Y-%m-%d"),
                    "buy_price": round(h["buy_price"], 3),
                    "sell_price": round(sell_price, 3),
                    "qty": h["qty"],
                    "return_pct": round((sell_price / h["buy_price"] - 1) * 100, 2),
                    "exit_reason": h.get("exit_reason", "max_hold"),
                }
            )

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
