"""多策略回测引擎入口 —— 组装数据/策略/经纪商并跑 Backtrader。

对外暴露 run_multi_strategy_backtest(...)，返回与 signal_backtest.run_signal_backtest
完全相同的 BacktestResult 结构（summary/equity/trades），前端无需改动即可展示。
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Optional

import math
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
    trend_exit_ma: Optional[int] = None,
    size_by: Optional[str] = None,
    capital_scale: float = 1.0,
    vol_target: Optional[bool] = None,
    partial_take_profit: Optional[bool] = None,
    model_limit_down: Optional[bool] = None,
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
      - 趋势破位 trend_exit_ma=N：收盘价跌破 N 日均线即离场（截停在下行市里继续下跌，
        不空等硬止损；N 取 60 时 Boll 买点(近MA20、远高于MA60)不会误触发）
      - 持有满 hold_days 个日历日强制平仓（兜底上限）
    默认 enable_exits=False 时行为与改动前一致（仅按持有天数平仓），保证向后兼容。
    """
    # 出场参数 None → 自适应中性（波动率中位、震荡市 → 基线 8/6/5/60），
    # 彻底消除「None 直接禁用止损」的隐患；risk_rules 不可达时回退已验证字面量。
    try:
        from smcore.strategy.risk_rules import compute_adaptive_exit_params

        _exit = compute_adaptive_exit_params()
        stop_loss_pct = stop_loss_pct if stop_loss_pct is not None else _exit["stop_loss_pct"]
        take_profit_pct = take_profit_pct if take_profit_pct is not None else _exit["take_profit_pct"]
        trailing_stop_pct = trailing_stop_pct if trailing_stop_pct is not None else _exit["trailing_stop_pct"]
        trend_exit_ma = trend_exit_ma if trend_exit_ma is not None else _exit["trend_exit_ma"]
    except Exception:
        if stop_loss_pct is None:
            stop_loss_pct = 0.08
        if take_profit_pct is None:
            take_profit_pct = 0.06
        if trailing_stop_pct is None:
            trailing_stop_pct = 0.05
        if trend_exit_ma is None:
            trend_exit_ma = 60

    # 波动率目标仓位 / 分批止盈 / 跌停卖不出：None → 读 RISK_CONFIG（可热更新），
    # 回退内置默认；任一显式传入则以传入为准（便于脚本/测试按场景覆盖）。
    try:
        from smcore.strategy.risk_rules import (
            compute_vol_target_params,
            compute_partial_exit_params,
            compute_market_friction_params,
            vol_target_scale,
        )

        _vt_cfg = compute_vol_target_params()
        _pt_cfg = compute_partial_exit_params()
        _mf_cfg = compute_market_friction_params()
        vt_enabled = _vt_cfg["enabled"] if vol_target is None else vol_target
        pt_enabled = _pt_cfg["enabled"] if partial_take_profit is None else partial_take_profit
        ld_enabled = _mf_cfg["model_limit_down"] if model_limit_down is None else model_limit_down
        _ld_thr = _mf_cfg["limit_down_threshold"]
    except Exception:
        _vt_cfg = {"target_annual_vol": 0.30, "window": 20, "min_scale": 0.3, "max_scale": 2.0}
        _pt_cfg = {"trigger_pct": 0.04, "tranche_pct": 0.33, "trailing_tighten": 0.5, "max_tranches": 2}
        vt_enabled = bool(vol_target) if vol_target is not None else True
        pt_enabled = bool(partial_take_profit) if partial_take_profit is not None else True
        ld_enabled = bool(model_limit_down) if model_limit_down is not None else True
        _ld_thr = 0.095

        def vol_target_scale(v, p=None):
            return 1.0

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
    weight_map: dict[tuple[date, str], float] = {}
    stoppct_map: dict[tuple[date, str], Optional[float]] = {}
    strat_map: dict[tuple[date, str], str] = {}  # 来源策略 → 退出路由依据
    for _, row in norm.iterrows():
        sd = row["date"].date()
        code = str(row["code"]).strip()
        raw_by_date[sd].append(code)
        level_map[(sd, code)] = (_to_f(row.get("stop_price")), _to_f(row.get("take_price")))
        w = 1.0
        if size_by is not None:
            wv = _to_f(row.get(size_by))
            w = wv if (wv is not None and wv > 0) else 1.0
        weight_map[(sd, code)] = w
        # 逐行止损比例（波动率自适应）：输入含 stop_pct 列时按个股波动率定，否则回退全局
        sp = _to_f(row.get("stop_pct"))
        stoppct_map[(sd, code)] = sp if (sp is not None and sp > 0) else None
        # 来源策略（如 "boll" / "theme" / "Momentum" / "boll,theme"），用于退出路由
        strat_map[(sd, code)] = str(row.get("来源策略", "")).strip()

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

    def _ma_n(code: str, d: date, n: int):
        """收盘价的 N 日移动平均（截至 d，含 d）。数据不足返回 None。"""
        p = price_cache.get(code)
        if p is None or p.empty:
            return None
        sub = p.loc[p.index.date <= d, "close"]
        if len(sub) < n:
            return None
        return float(sub.iloc[-n:].mean())

    def _ann_vol(code: str, d: date, window: int) -> Optional[float]:
        """个股近 window 日年化波动率（截至 d，含 d）。数据不足返回 None。"""
        p = price_cache.get(code)
        if p is None or p.empty:
            return None
        sub = p.loc[p.index.date <= d, "close"]
        if len(sub) < window + 1:
            return None
        rets = sub.iloc[-(window + 1):].pct_change().dropna()
        if len(rets) < window:
            return None
        return float(rets.std() * math.sqrt(252))

    def _at_limit_down(code: str, d: date) -> bool:
        """近似判断当日是否封跌停（A股卖单无法成交）。

        主板±10%、科创/创业±20%；无板块信息时用保守近似：日收益 ≤ -threshold
        且收盘等于当日最低（钉在跌停板）。满足则视为封跌停、当日卖不出，顺延次日。
        """
        p = price_cache.get(code)
        if p is None or p.empty:
            return False
        rows = p.loc[p.index.date <= d]
        if len(rows) < 2:
            return False
        cur = rows.iloc[-1]
        prev = rows.iloc[-2]
        try:
            c0 = float(cur["close"])
            c1 = float(prev["close"])
            lo = float(cur["low"])
        except (TypeError, ValueError):
            return False
        if c1 <= 0 or c0 is None or c1 is None or lo is None:
            return False
        ret = c0 / c1 - 1
        pinned = abs(lo - c0) <= max(0.01, abs(c0) * 0.002)
        return ret <= -_ld_thr and pinned

    # 买入调度：每个信号日 → 其「之后第一个交易日」作为买入处理日（即信号日次日开盘买入）。
    # 信号日本身可能不是交易日（周末/休市），不能直接用信号日作为交易日历中的 key。
    buy_schedule: dict[date, list[tuple[date, str, str]]] = defaultdict(list)
    for sd, codes in raw_by_date.items():
        proc = None
        for d in cal:
            if d > sd:
                proc = d
                break
        if proc is not None:
            for code in codes:
                buy_schedule[proc].append(
                    (
                        sd,
                        code,
                        weight_map.get((sd, code), 1.0),
                        stoppct_map.get((sd, code), None),
                        strat_map.get((sd, code), ""),
                    )
                )

    cash = float(initial_capital)
    holdings: dict[str, dict] = {}
    equity_curve: list[dict] = []
    trades: list[dict] = []

    for d in cal:
        # 1) 信号日次日开盘买入（处理日 = 信号日后第一个交易日，用其开盘价）
        if d in buy_schedule:
            candidates = [(sd, c, w, sp, st) for (sd, c, w, sp, st) in buy_schedule[d] if c not in holdings]
            avail = max_positions - len(holdings)
            buyable = candidates[:avail]
            # 波动率目标仓位：仓位 ∝ 目标波动 / 个股波动20日。高波动票少买、低波动票多买，
            # 用 (scaled_w / sum_scaled) 重新分配同一笔可用资金 → 总暴露不变、内部向低波动倾斜，
            # 在不放大回撤的前提下提升收益/回撤比。scale 缺失→1.0 中性。
            _scaled = []
            _total_scaled = 0.0
            for (sd, c, w, row_stop, _st) in buyable:
                sc = 1.0
                if vt_enabled:
                    sc = vol_target_scale(_ann_vol(c, d, _vt_cfg["window"]), _vt_cfg)
                _sw = w * sc
                _scaled.append((sd, c, w, row_stop, _st, _sw))
                _total_scaled += _sw
            _total_scaled = _total_scaled or 1.0
            for sd, c, w, row_stop, _st, scaled_w in _scaled:
                buy_price = _px(c, d, "open")
                if buy_price is None:
                    continue
                buy_price *= (1 + slippage)
                # 按（波动率缩放后的）置信度权重分配资金；
                # capital_scale<1 时留现金（高波动降仓），真正减少组合暴露而非等比缩放
                per = cash * (scaled_w / _total_scaled) * max(0.0, min(1.0, capital_scale))
                qty = int(per / buy_price / 100) * 100
                if qty < 100:
                    continue
                cost = buy_price * qty
                fee = _buy_cost(cost)
                if cost + fee > cash:
                    continue
                cash -= cost + fee
                stop, take = level_map.get((sd, c), (None, None))
                # 逐行止损比例：波动率自适应（个股 vol20 定）回退全局 stop_loss_pct
                eff_stop = row_stop if row_stop is not None else stop_loss_pct
                holdings[c] = {
                    "buy_date": d,
                    "buy_price": buy_price,
                    "qty": qty,
                    "stop": stop,
                    "take": take,
                    "stop_pct": eff_stop,
                    "peak": buy_price,
                    "sd": sd,
                    "strategy": _st,
                }
                avail -= 1

        # 2) 退出检查（基于当日收盘价）：分批止盈 / 止盈 / 止损 / 移动止盈 / 持有期满
        partial_sells: list[tuple[str, int, str]] = []
        for c, h in holdings.items():
            close = _px(c, d, "close")
            if close is None:
                continue  # 缺数据则暂不处理
            h["peak"] = max(h.get("peak", close), close)
            # A股 T+1：买入当日不可卖出，统一跳过退出检查，杜绝“当天买当天卖”的假止盈亏损
            if (d - h["buy_date"]).days < 1:
                continue
            exit_reason = None
            if enable_exits:
                # 逐行止损比例：波动率自适应（个股 vol20 定）回退全局 stop_loss_pct
                row_stop = h.get("stop_pct")
                # 入场策略 → 退出路由：均值回归(boll/relativity) 不启用 MA60 趋势破位
                _strats = [
                    s.strip().lower()
                    for s in str(h.get("strategy", "")).replace("/", "，").split("，")
                    if s.strip()
                ]
                _is_mr = any(s in ("boll", "relativity") for s in _strats)
                # 分批止盈：盈利达阈值先卖一部，余仓收紧跟踪止损（最高 max_tranches 批）。
                # 末批（已达批数上限）整仓清掉；非末批仅减仓、余仓继续持有让利润奔跑。
                if (
                    pt_enabled
                    and h.get("tranches_done", 0) < _pt_cfg["max_tranches"]
                    and close is not None
                ):
                    _ret = close / h["buy_price"] - 1
                    if _ret >= _pt_cfg["trigger_pct"]:
                        _remain = h["qty"]
                        if h.get("tranches_done", 0) + 1 >= _pt_cfg["max_tranches"]:
                            exit_reason = "take_partial_final"
                        else:
                            _sq = int(_remain * _pt_cfg["tranche_pct"] / 100.0) * 100
                            _sq = max(100, min(_sq, _remain))
                            if _sq >= _remain:
                                exit_reason = "take_partial_final"
                            else:
                                h["qty"] -= _sq
                                h["tranches_done"] = h.get("tranches_done", 0) + 1
                                h["trailing_eff"] = (h.get("trailing_eff") or trailing_stop_pct) * _pt_cfg[
                                    "trailing_tighten"
                                ]
                                partial_sells.append((c, _sq, "take_partial"))
                # 硬止损（缺口感知）：盘中最低价触及 -row_stop 即以 min(开盘价,止损价)
                # 离场，封顶亏损≈row_stop，挡住跳空低开直接击穿收盘止损的巨亏（如单日 -23%）。
                if exit_reason is None and row_stop is not None:
                    low = _px(c, d, "low")
                    if low is not None:
                        hard_stop = h["buy_price"] * (1 - abs(row_stop))
                        if low <= hard_stop:
                            open_px = _px(c, d, "open")
                            exit_px = min(open_px, hard_stop) if open_px is not None else hard_stop
                            h["forced_sell_price"] = exit_px
                            exit_reason = "stop_hard"
                if exit_reason is None:
                    ret = close / h["buy_price"] - 1
                    # 止盈：均值回归目标=Boll 上轨；但仅当上轨真正高于入场价才生效。
                    # 动量/趋势票入场常高于上轨（上轨在成本下方），命中即亏损，必须剔除，
                    # 否则出现“负收益却标上轨止盈”的矛盾（实测 002258/600000 即此情形）。
                    if use_signal_bands and h.get("take") is not None and h["take"] > h["buy_price"] and close >= h["take"]:
                        exit_reason = "take_band"
                    elif take_profit_pct is not None and ret >= abs(take_profit_pct):
                        exit_reason = "take_pct"
                    # 趋势破位：仅趋势/动量策略用收盘跌破 MA60 离场；均值回归(boll/relativity)
                    # 不启用 —— relativity 实测因 MA60 破位恶化（-9.57%→-13.86%）。
                    elif (not _is_mr) and trend_exit_ma and trend_exit_ma > 0:
                        ma = _ma_n(c, d, trend_exit_ma)
                        if ma is not None and close < ma:
                            exit_reason = "trend_break"
                    # 止损：固定/自适应比例（Boll 下轨≈入场价、过紧易频繁假止损，不单独用作止损）
                    elif row_stop is not None and ret <= -abs(row_stop):
                        exit_reason = "stop_pct"
                    # 移动止盈：用余仓收紧后的 trailing_eff（若有），从峰值回撤锁定利润
                    elif (h.get("trailing_eff") or trailing_stop_pct) is not None and h["peak"] > 0 and (
                        close / h["peak"] - 1
                    ) <= -abs(h.get("trailing_eff") or trailing_stop_pct):
                        exit_reason = "trailing"
                    # 最后防线：仅当 Boll 下轨明显低于入场价（>3%）时才用作硬止损
                    elif use_signal_bands and h.get("stop") is not None and h["stop"] < h["buy_price"] * 0.97 and close <= h["stop"]:
                        exit_reason = "stop_band"
            if exit_reason is None and (d - h["buy_date"]).days >= hold_days:
                exit_reason = "max_hold"  # 持有期满兜底
            if exit_reason is not None:
                h["exit_reason"] = exit_reason

        # 2.5) 分批止盈的非末批：按当日收盘（含滑点）卖出部分仓位，余仓继续持有
        for (c, _sq, reason) in partial_sells:
            h = holdings.get(c)
            if h is None:
                continue
            sell_price = _px(c, d, "close")
            if sell_price is None:
                continue
            sell_price *= (1 - slippage)
            proceeds = sell_price * _sq
            cash += proceeds - _sell_cost(proceeds)
            trades.append(
                {
                    "code": c,
                    "buy_date": h["buy_date"].strftime("%Y-%m-%d"),
                    "sell_date": d.strftime("%Y-%m-%d"),
                    "buy_price": round(h["buy_price"], 3),
                    "sell_price": round(sell_price, 3),
                    "qty": _sq,
                    "return_pct": round((sell_price / h["buy_price"] - 1) * 100, 2),
                    "exit_reason": reason,
                }
            )

        to_sell = [c for c, h in holdings.items() if "exit_reason" in h]
        for c in to_sell:
            # 跌停卖不出：当日封跌停则卖单无法成交，清除出场意图顺延至次日（次日重新触发）
            if ld_enabled and _at_limit_down(c, d):
                _h = holdings[c]
                _h.pop("exit_reason", None)
                _h.pop("forced_sell_price", None)
                continue
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
