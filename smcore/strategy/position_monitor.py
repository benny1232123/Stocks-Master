"""纸盘组合监控器（把出场/风控引擎接入组合执行闭环）。

背景
----
回测引擎 `smcore.backtest.engine.run_forward_signal_backtest` 已实现完整的出场逻辑
（次日开盘买 / T+1 防当天卖 / 缺口感知硬止损 / 移动止盈 / MA60 趋势破位(仅非均值回归) /
持有期满 / 波动率自适应止损）。但这些逻辑此前只被「研究脚本」(measure_*/daily_backtest)
使用，**从未接入真正的组合管理闭环**：生产 `fusion → Daily-Action-List` 只把止损/止盈价
当「建议列」，而 `paper_tracker` 也只是「持有到下一信号日再平衡、中途无任何止损」。

本模块填补这一缺口：
1. `simulate_position`   —— 单只标的的出场感知前向模拟（与引擎出场规则保持一致的实现）。
2. `run_paper_with_exits`—— 对全部历史 Daily-Action-List 做「带止损」的纸盘重放，
   并与 naive（裸持有到下一信号日）口径做头对头对比，量化回撤改善。
3. `PaperPortfolio`      —— 有状态的每日纸盘执行器（持仓状态文件 + 每日盯市 + 写回已实现交易），
   供 CI/定时任务在真实每日链路里真正触发止损/止盈。**纯纸盘、不碰实盘、零网络依赖本地 k_data**。

出场规则常量默认与引擎 `daily_backtest.py` 验证过的配置一致（止损8%/止盈6%/trailing5%/MA60），
集中在 DEFAULT_EXIT 便于单一真源维护。
"""
from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from smcore.config.defaults import (
    BETA_MIN_KEEP,
    MAX_SECTOR_WEIGHT_PCT,
    PORTFOLIO_BETA_CEILING,
    STOCK_DATA_DIR,
)
from smcore.utils.code import format_stock_code

DEFAULT_EXIT = dict(
    stop_loss_pct=0.08,
    take_profit_pct=0.06,
    trailing_stop_pct=0.05,
    trend_exit_ma=60,
    hold_days=10,
    slippage=0.001,
)

# 回撤熔断：组合回撤越大，新建仓可部署现金越少（防守性降仓）。
# cash_buffer = clamp(当前回撤 / DD_FULL, 0, 1) * DD_CASH_CEILING，叠加在静态 cash_frac 之上。
DD_FULL = 0.20          # 回撤达到该比例 → 触发满额缓冲
DD_CASH_CEILING = 0.50  # 满额缓冲时额外保留的现金比例上限（与静态 cash_frac 叠加）


def _load_k_window(code: str, start: str, end: str) -> pd.DataFrame:
    """读本地前复权日线窗口（date/open/close/low/high），优先本地缓存、fail-soft。"""
    from smcore.data.kline import fetch_daily_k

    try:
        df = fetch_daily_k(code, start, end, adjust="qfq")
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    keep = [c for c in ["date", "open", "close", "low", "high"] if c in df.columns]
    if not keep:
        return pd.DataFrame()
    out = df[keep].copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    for c in ["open", "close", "low", "high"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.dropna(subset=["date"]).set_index("date").sort_index()
    return out


def _first_trading_day_after(code: str, signal_date: date, lookahead: int = 15) -> Optional[date]:
    """返回 signal_date 之后第一个有 K 线的交易日（买入处理日 = 信号日后第一个交易日）。"""
    end = (signal_date + timedelta(days=lookahead)).strftime("%Y-%m-%d")
    start = signal_date.strftime("%Y-%m-%d")
    df = _load_k_window(code, start, end)
    if df.empty:
        return None
    future = df.index[df.index.date > signal_date]
    if len(future) == 0:
        return None
    return future[0].date()


def _ma_n(close_series: pd.Series, d: pd.Timestamp, n: int) -> Optional[float]:
    sub = close_series[close_series.index <= d]
    if len(sub) < n:
        return None
    return float(sub.iloc[-n:].mean())


def simulate_position(
    code: str,
    buy_date: date,
    end_date: date,
    *,
    stop_pct: Optional[float] = None,
    take_price: Optional[float] = None,
    strategy: str = "",
    stop_loss_pct: float = DEFAULT_EXIT["stop_loss_pct"],
    take_profit_pct: float = DEFAULT_EXIT["take_profit_pct"],
    trailing_stop_pct: float = DEFAULT_EXIT["trailing_stop_pct"],
    trend_exit_ma: int = DEFAULT_EXIT["trend_exit_ma"],
    hold_days: int = DEFAULT_EXIT["hold_days"],
    slippage: float = DEFAULT_EXIT["slippage"],
) -> dict:
    """单只标的的出场感知前向模拟，与 `engine.run_forward_signal_backtest` 出场规则保持一致。

    Args:
        code: 6 位代码
        buy_date: 实际买入日（通常为信号日后第一个交易日）
        end_date: 评估截止日（如 paper_tracker 的「下一信号日」）
        stop_pct: 逐只波动率自适应止损（优先于全局 stop_loss_pct）；None 则回退全局
        take_price: Boll 上轨（均值回归止盈目标）；None 则只用固定百分比止盈
        strategy: 来源策略（boll/relativity 为均值回归，不启用 MA60 趋势破位）

    Returns:
        {"return_pct", "exit_reason", "sell_date", "buy_price", "sell_price"}
    """
    is_mr = any(s.strip().lower() in ("boll", "relativity") for s in strategy.replace("/", ",").split(",") if s.strip())
    # MA 需要回看窗口，故 K 线起点前移 trend_exit_ma+ 天
    start = (buy_date - timedelta(days=trend_exit_ma + 20)).strftime("%Y-%m-%d")
    end = (end_date + timedelta(days=2)).strftime("%Y-%m-%d")
    df = _load_k_window(code, start, end)
    if df.empty:
        return {"return_pct": 0.0, "exit_reason": "no_data", "sell_date": None, "buy_price": None, "sell_price": None}

    days = [d for d in df.index if buy_date <= d.date() <= end_date]
    if not days:
        return {"return_pct": 0.0, "exit_reason": "no_data", "sell_date": None, "buy_price": None, "sell_price": None}

    buy_row = df[df.index.date == days[0].date()]
    if buy_row.empty:
        return {"return_pct": 0.0, "exit_reason": "no_data", "sell_date": None, "buy_price": None, "sell_price": None}
    buy_price = float(buy_row["open"].iloc[0]) * (1 + slippage)
    peak = buy_price
    eff_stop = stop_pct if (stop_pct is not None and stop_pct > 0) else stop_loss_pct
    exit_reason = None
    sell_price = None
    sell_date = None

    for d in days:
        close = float(df.loc[d, "close"])
        low = float(df.loc[d, "low"]) if not pd.isna(df.loc[d, "low"]) else close
        peak = max(peak, close)
        # T+1：买入当日不检查出场（防当天买当天卖的假止损）
        if (d.date() - buy_date).days < 1:
            continue

        # 1) 缺口感知硬止损（盘中最低触及即离场，封顶亏损≈eff_stop，挡跳空低开巨亏）
        hard_stop = buy_price * (1 - abs(eff_stop))
        if low <= hard_stop:
            open_px = float(df.loc[d, "open"]) if not pd.isna(df.loc[d, "open"]) else close
            sell_price = min(open_px, hard_stop) * (1 - slippage)
            exit_reason = "stop_hard"
            sell_date = d.date()
            break

        ret = close / buy_price - 1.0
        # 2) 止盈：均值回归目标 = Boll 上轨（仅当上轨真高于成本）；否则固定百分比
        if (take_price is not None and take_price > buy_price and close >= take_price):
            exit_reason = "take_band"
        elif (take_profit_pct is not None and ret >= abs(take_profit_pct)):
            exit_reason = "take_pct"
        # 3) 趋势破位：仅趋势/动量策略用收盘跌破 MA60 离场；均值回归不启用
        elif (not is_mr) and trend_exit_ma and trend_exit_ma > 0:
            ma = _ma_n(df["close"], d, trend_exit_ma)
            if ma is not None and close < ma:
                exit_reason = "trend_break"
        # 4) 固定/自适应比例止损
        elif ret <= -abs(eff_stop):
            exit_reason = "stop_pct"
        # 5) 移动止盈：从峰值回撤锁定利润
        elif (trailing_stop_pct is not None and peak > 0
              and (close / peak - 1) <= -abs(trailing_stop_pct)):
            exit_reason = "trailing"
        # 6) 最后防线：Boll 下轨明显低于成本(>3%)时才作硬止损
        elif (take_price is not None and take_price < buy_price * 0.97 and close <= take_price):
            exit_reason = "stop_band"

        if exit_reason is not None:
            sell_price = close * (1 - slippage)
            sell_date = d.date()
            break

        # 7) 持有期满兜底
        if (d.date() - buy_date).days >= hold_days:
            exit_reason = "max_hold"
            sell_price = close * (1 - slippage)
            sell_date = d.date()
            break

    if exit_reason is None:
        # 持有到 end_date 仍未触发任何出场（如 paper_tracker 的「到下一信号日」边界）
        last_close = float(df.loc[days[-1], "close"])
        sell_price = last_close * (1 - slippage)
        sell_date = days[-1].date()
        exit_reason = "horizon"
    return_pct = (sell_price / buy_price - 1.0) * 100.0 if buy_price > 0 else 0.0
    return {"return_pct": round(return_pct, 2), "exit_reason": exit_reason,
            "sell_date": sell_date, "buy_price": round(buy_price, 3), "sell_price": round(sell_price, 3)}


def _naive_return(code: str, buy_date: date, next_signal_date: date) -> Optional[float]:
    """naive 口径：信号日后首交易日开盘买 → 下一信号日后首交易日开盘卖（与 paper_tracker 一致）。"""
    buy = _first_trading_day_after(code, buy_date)
    sell = _first_trading_day_after(code, next_signal_date)
    if buy is None or sell is None or sell <= buy:
        return None
    df = _load_k_window(code, buy.strftime("%Y-%m-%d"), sell.strftime("%Y-%m-%d"))
    if df.empty:
        return None
    b = df[df.index.date >= buy]
    s = df[df.index.date >= sell]
    if b.empty or s.empty:
        return None
    bp = float(b["open"].iloc[0])
    sp = float(s["open"].iloc[0])
    if bp <= 0:
        return None
    return (sp / bp - 1.0) * 100.0


def _load_dal(sd: str) -> Optional[pd.DataFrame]:
    p = STOCK_DATA_DIR / f"Daily-Action-List-{sd}.csv"
    if not p.exists():
        return None
    try:
        return pd.read_csv(p, encoding="utf-8-sig")
    except Exception:
        return None


def run_paper_with_exits(initial_capital: float = 1_000_000.0, invest_frac: float = 1.0,
                         **exit_kwargs) -> dict:
    """对全部历史 Daily-Action-List 做「带止损」纸盘重放，并与 naive 口径头对头对比。

    每个信号日：按清单建议金额归一化分配，从「信号日后首交易日」买入，持有到「下一信号日」，
    期间应用出场规则（止损/止盈/移动止盈/MA60/trailing/期满）。组合净值按信号日链式复利。

    Returns: 含 exit-aware 与 naive 两套 summary + equity 曲线，便于直接对比回撤。
    """
    cfg = {**DEFAULT_EXIT, **exit_kwargs}
    files = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))
    days = []
    for f in files:
        suf = f.name.replace("Daily-Action-List-", "").replace(".csv", "")
        if len(suf) == 8 and suf.isdigit():
            try:
                days.append((datetime.strptime(suf, "%Y%m%d").date(), suf))
            except ValueError:
                continue
    days.sort()
    if len(days) < 2:
        return {"error": "信号日不足 2 个，无法重放"}

    def _weights(dal: pd.DataFrame) -> dict[str, float]:
        use_col = "建议金额" if "建议金额" in dal.columns else ("建议仓位%" if "建议仓位%" in dal.columns else None)
        wmap = {}
        if use_col is None:
            return wmap
        for _, r in dal.iterrows():
            c = format_stock_code(r.get("股票代码"))
            if not c:
                continue
            try:
                v = float(r.get(use_col))
            except (TypeError, ValueError):
                continue
            if pd.notna(v) and v > 0:
                wmap[c] = v
        tot = sum(wmap.values())
        return {c: v / tot for c, v in wmap.items()} if tot > 0 else {}

    exit_value, naive_value = 1.0, 1.0
    exit_curve, naive_curve = [], []
    ex_rows, naive_rows = [], []
    for i, (sd, sd_tag) in enumerate(days):
        dal = _load_dal(sd_tag)
        if dal is None or dal.empty:
            continue
        wmap = _weights(dal)
        if not wmap:
            continue
        nxt = days[i + 1][0] if i + 1 < len(days) else (sd + timedelta(days=cfg["hold_days"]))
        ex_rets, nv_rets = [], []
        for c, w in wmap.items():
            buy = _first_trading_day_after(c, sd)
            if buy is None or buy >= nxt:
                continue
            row = dal[dal["股票代码"].apply(format_stock_code) == c]
            stop_pct = None
            take_price = None
            strat = ""
            if not row.empty:
                try:
                    sp = row.iloc[0].get("stop_pct")
                    stop_pct = float(sp) if (sp is not None and not pd.isna(sp)) else None
                except (TypeError, ValueError):
                    stop_pct = None
                try:
                    tp = row.iloc[0].get("止盈价(上轨)")
                    take_price = float(tp) if (tp is not None and not pd.isna(tp)) else None
                except (TypeError, ValueError):
                    take_price = None
                strat = str(row.iloc[0].get("来源策略", "") or "")
            ex = simulate_position(c, buy, nxt, stop_pct=stop_pct, take_price=take_price,
                                   strategy=strat, **cfg)
            nv = _naive_return(c, sd, nxt)
            if ex["return_pct"] is not None:
                ex_rets.append((w, ex["return_pct"]))
            if nv is not None:
                nv_rets.append((w, nv))
        ex_day = invest_frac * sum(w * r for w, r in ex_rets) / 100.0 if ex_rets else 0.0
        nv_day = invest_frac * sum(w * r for w, r in nv_rets) / 100.0 if nv_rets else 0.0
        exit_value *= (1 + ex_day)
        naive_value *= (1 + nv_day)
        ex_rows.append(len(ex_rets))
        naive_rows.append(len(nv_rets))
        exit_curve.append({"from": sd_tag, "value": round(exit_value, 6)})
        naive_curve.append({"from": sd_tag, "value": round(naive_value, 6)})

    def _mdd(curve):
        peak = float("-inf"); mdd = 0.0
        for v in curve:
            peak = max(peak, v["value"])
            if peak > 0:
                mdd = min(mdd, (v["value"] - peak) / peak * 100.0)
        return round(mdd, 2)

    return {
        "n_signal_days": len(days),
        "first_day": days[0][1],
        "last_day": days[-1][1],
        "invest_frac": invest_frac,
        "exit_aware": {
            "total_return_pct": round((exit_value - 1) * 100.0, 2),
            "max_drawdown_pct": _mdd(exit_curve),
            "avg_names_per_list": round(sum(ex_rows) / len(ex_rows), 1) if ex_rows else 0,
            "curve": exit_curve,
        },
        "naive": {
            "total_return_pct": round((naive_value - 1) * 100.0, 2),
            "max_drawdown_pct": _mdd(naive_curve),
            "avg_names_per_list": round(sum(naive_rows) / len(naive_rows), 1) if naive_rows else 0,
            "curve": naive_curve,
        },
    }


# ── 有状态每日纸盘执行器（供 CI/定时任务在真实每日链路里触发止损）─────────────────
STATE_PATH = STOCK_DATA_DIR / "position_monitor_state.json"


class PaperPortfolio:
    """持仓状态 + 每日盯市 + 出场触发。纯纸盘，状态落盘 JSON，零实盘风险。

    用法（每日收盘后调用一次）：
        pf = PaperPortfolio.load()
        pf.process_day(today_date, dal_path_for_today)   # 开新仓 + 盯市出场
        pf.save()
    """

    def __init__(self, initial_capital: float = 1_000_000.0, max_single_weight: float = 0.10,
                 cash_frac: float = 0.0, max_sector_weight: Optional[float] = None,
                 portfolio_beta_ceiling: Optional[float] = None, beta_min_keep: int = BETA_MIN_KEEP,
                 dd_cash_ceiling: float = DD_CASH_CEILING, dd_full: float = DD_FULL,
                 sector_resolver=None, **exit_kwargs):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.max_single_weight = max_single_weight
        self.cash_frac = cash_frac
        self.max_sector_weight = max_sector_weight if max_sector_weight is not None else (MAX_SECTOR_WEIGHT_PCT / 100.0)
        self.portfolio_beta_ceiling = portfolio_beta_ceiling if portfolio_beta_ceiling is not None else PORTFOLIO_BETA_CEILING
        self.beta_min_keep = beta_min_keep
        self.dd_cash_ceiling = dd_cash_ceiling
        self.dd_full = dd_full
        # sector_resolver(code)->行业名 或 None；为 None 时跳过行业权重再平衡（缺离线行业映射）。
        self.sector_resolver = sector_resolver
        self.exit = {**DEFAULT_EXIT, **exit_kwargs}
        self.positions: dict[str, dict] = {}   # code -> holding dict
        self.realized: list[dict] = []
        self.equity_curve: list[dict] = []

    # ── 状态持久化 ──
    def save(self, path: Path = STATE_PATH) -> None:
        payload = {
            "initial_capital": self.initial_capital,
            "cash": self.cash,
            "max_single_weight": self.max_single_weight,
            "cash_frac": self.cash_frac,
            "max_sector_weight": self.max_sector_weight,
            "portfolio_beta_ceiling": self.portfolio_beta_ceiling,
            "beta_min_keep": self.beta_min_keep,
            "dd_cash_ceiling": self.dd_cash_ceiling,
            "dd_full": self.dd_full,
            "exit": self.exit,
            "positions": self.positions,
            "realized": self.realized,
            "equity_curve": self.equity_curve,
        }
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
        except Exception:
            pass

    @classmethod
    def load(cls, path: Path = STATE_PATH) -> "PaperPortfolio":
        if not path.exists():
            return cls()
        try:
            with open(path, "r", encoding="utf-8") as f:
                d = json.load(f)
        except Exception:
            return cls()
        pf = cls(initial_capital=d.get("initial_capital", 1_000_000.0),
                 max_single_weight=d.get("max_single_weight", 0.10),
                 cash_frac=d.get("cash_frac", 0.0),
                 max_sector_weight=d.get("max_sector_weight", None),
                 portfolio_beta_ceiling=d.get("portfolio_beta_ceiling", None),
                 beta_min_keep=d.get("beta_min_keep", BETA_MIN_KEEP),
                 dd_cash_ceiling=d.get("dd_cash_ceiling", DD_CASH_CEILING),
                 dd_full=d.get("dd_full", DD_FULL),
                 **d.get("exit", {}))
        pf.cash = d.get("cash", pf.initial_capital)
        pf.positions = d.get("positions", {})
        pf.realized = d.get("realized", [])
        pf.equity_curve = d.get("equity_curve", [])
        return pf

    # ── 每日处理：开新仓 + 盯市出场 + 漂移再平衡 + 回撤熔断 ──
    def process_day(self, today: date, dal_path: Optional[Path] = None,
                    pending_signal_date: Optional[date] = None) -> dict:
        """在交易日 today 执行：①pending 信号的次日开盘建仓；②现有持仓盯市出场；
        ③持仓漂移再平衡（单名/行业/组合β 拉回上限）；④记录权益与组合回撤（熔断输入）。"""
        # 1) 建仓：信号日后第一个交易日 = today 时建仓（复用 simulate_position 的买入语义）
        if dal_path is not None and pending_signal_date is not None and self._is_first_trading_day_after(
                pending_signal_date, today):
            self._open_from_dal(dal_path, pending_signal_date, today)

        # 2) 盯市出场
        for code in list(self.positions.keys()):
            self._mark_and_exit(code, today)

        # 3) 持仓漂移再平衡（单名/β/行业权重回到上限内）
        self._rebalance(today)

        # 4) 记录权益 + 当前组合回撤（供回撤熔断与监控使用）
        hv = sum(self._market_value(h) for h in self.positions.values())
        total = self.cash + hv
        dd = self._current_drawdown(total)
        self.equity_curve.append({"date": today.strftime("%Y-%m-%d"), "total": round(total, 2),
                                   "cash": round(self.cash, 2), "holding": round(hv, 2),
                                   "drawdown_pct": round(dd * 100.0, 2)})
        return {"date": today.strftime("%Y-%m-%d"), "total": round(total, 2),
                "positions": len(self.positions), "drawdown_pct": round(dd * 100.0, 2)}

    # ── 市值 / 回撤 / 动态现金 ──
    def _market_value(self, h: dict) -> float:
        """持仓当前市值（优先股数×最新价；未跟踪股数时退化为成本×成本后涨跌幅）。"""
        qty = h.get("qty") or 0.0
        last = h.get("last_close")
        if qty > 0 and last is not None:
            return qty * last
        bp = h.get("buy_price")
        if last is not None and bp:
            return float(h.get("cost") or 0.0) * (last / bp)
        return float(h.get("cost") or 0.0)

    def _current_drawdown(self, cur_total: float) -> float:
        """组合自历史峰值的最大回撤比例（0~1）。"""
        if not self.equity_curve:
            return 0.0
        peak = max(r["total"] for r in self.equity_curve)
        peak = max(peak, cur_total)
        if peak <= 0:
            return 0.0
        return max(0.0, (peak - cur_total) / peak)

    def _effective_cash_frac(self) -> float:
        """静态 cash_frac + 回撤触发的动态缓冲（回撤越大越保守，降低新建仓部署）。"""
        if self.equity_curve:
            peak = max(r["total"] for r in self.equity_curve)
            last = self.equity_curve[-1]["total"]
            dd = max(0.0, (peak - last) / peak) if peak > 0 else 0.0
        else:
            dd = 0.0
        buffer = (min(dd / self.dd_full, 1.0) * self.dd_cash_ceiling) if self.dd_full > 0 else 0.0
        return min(self.cash_frac + buffer, 1.0)

    def _buy_open_price(self, code: str, buy_date: date) -> Optional[float]:
        """读取买入日开盘价（离线本地 k_data），用于真实盯市。缺失返回 None。"""
        s = buy_date.strftime("%Y-%m-%d")
        df = _load_k_window(code, s, s)
        if df.empty:
            return None
        row = df[df.index.date == buy_date]
        if row.empty:
            return None
        op = row["open"].iloc[0]
        return float(op) if not pd.isna(op) else None

    def _close_position(self, code: str, today: date, reason: str) -> None:
        """清掉一只持仓：市值回填现金 + 记已实现（再平衡类退出，return_pct 留 None）。"""
        h = self.positions.pop(code, None)
        if h is None:
            return
        mv = self._market_value(h)
        self.cash += mv
        self.realized.append({
            "code": code, "buy_date": h.get("buy_date"),
            "sell_date": today.strftime("%Y-%m-%d"),
            "buy_price": h.get("buy_price") or (h.get("cost") or 0.0),
            "sell_price": h.get("last_close"), "return_pct": None,
            "exit_reason": reason,
        })

    def _rebalance(self, today: date) -> dict:
        """持仓漂移再平衡：把单名/行业/组合β 权重拉回上限内。返回动作摘要。"""
        actions = {"single_trim": 0, "beta_close": 0, "sector_close": 0}
        if not self.positions:
            return actions
        total = self.cash + sum(self._market_value(h) for h in self.positions.values())
        if total <= 0:
            return actions

        # 1) 单名权重上限：超限部分赎回回现金（保留剩余暴露，不清仓）
        for code in list(self.positions.keys()):
            h = self.positions[code]
            mv = self._market_value(h)
            w = mv / total if total else 0.0
            if w > self.max_single_weight + 1e-9:
                target_mv = self.max_single_weight * total
                qty = h.get("qty") or 0.0
                if qty > 0 and mv > 0:
                    h["qty"] = qty * (target_mv / mv)
                old_cost = float(h.get("cost") or 0.0)
                new_cost = old_cost * (target_mv / mv) if mv > 0 else 0.0
                self.cash += (old_cost - new_cost)
                h["cost"] = new_cost
                actions["single_trim"] += 1

        # 2) 组合 β 上限：逐步清掉当前 β 最高持仓，直到 ≤ 上限或仅剩 beta_min_keep 只
        from smcore.strategy.position_sizing import _estimate_betas
        betas = _estimate_betas(list(self.positions.keys()), today.strftime("%Y%m%d"))

        def _port_beta() -> float:
            tot = self.cash + sum(self._market_value(h) for h in self.positions.values())
            if tot <= 0 or not self.positions:
                return 0.0
            return sum((self._market_value(h) / tot) * betas.get(c, 1.0) for c, h in self.positions.items())

        while len(self.positions) > self.beta_min_keep:
            if _port_beta() <= self.portfolio_beta_ceiling:
                break
            worst = max(self.positions.keys(), key=lambda c: betas.get(c, 1.0))
            self._close_position(worst, today, reason="rebalance_beta")
            actions["beta_close"] += 1

        # 3) 行业权重上限：按 sector_resolver 分组，超限行业清掉权重最高者
        if self.sector_resolver is not None:
            tot = self.cash + sum(self._market_value(h) for h in self.positions.values())
            sec_w: dict[str, float] = {}
            sec_codes: dict[str, list[str]] = {}
            for c, h in self.positions.items():
                sec = self.sector_resolver(c) or "__unknown__"
                mv = self._market_value(h)
                sec_w[sec] = sec_w.get(sec, 0.0) + mv
                sec_codes.setdefault(sec, []).append(c)
            for sec, codes in sec_codes.items():
                guard = 0
                while (sec_w.get(sec, 0.0) / tot > self.max_sector_weight + 1e-9
                       and len(self.positions) > 1 and guard < len(codes) + 1):
                    worst = max(codes, key=lambda c: self._market_value(self.positions[c]))
                    mv_worst = self._market_value(self.positions[worst])
                    self._close_position(worst, today, reason="rebalance_sector")
                    if worst in codes:
                        codes.remove(worst)
                    sec_w[sec] = sec_w.get(sec, 0.0) - mv_worst
                    actions["sector_close"] += 1
                    guard += 1
        return actions

    def _is_first_trading_day_after(self, signal_date: date, today: date) -> bool:
        # 简化：today 是 signal_date 之后，且本地无更早交易日（由调用方保证 today 即买入日）
        return today > signal_date

    def _open_from_dal(self, dal_path: Path, signal_date: date, buy_date: date) -> None:
        try:
            dal = pd.read_csv(dal_path, encoding="utf-8-sig")
        except Exception:
            return
        if dal.empty:
            return
        use_col = "建议金额" if "建议金额" in dal.columns else ("建议仓位%" if "建议仓位%" in dal.columns else None)
        if use_col is None:
            return
        # 回撤熔断：当前组合回撤越大，新建仓可部署现金越少（防守性降仓）。
        budget = self.cash * (1 - self._effective_cash_frac())
        spent = 0.0
        for _, r in dal.iterrows():
            code = format_stock_code(r.get("股票代码"))
            if not code or code in self.positions:
                continue
            try:
                w = float(r.get(use_col))
            except (TypeError, ValueError):
                continue
            if not pd.notna(w) or w <= 0:
                continue
            # 单名仓位上限（相对预算），并受剩余预算约束（预算内不超配）。
            alloc = min(w / 100.0, self.max_single_weight) * budget
            if spent + alloc > budget + 1e-9:
                alloc = max(0.0, budget - spent)
            if alloc <= 0:
                continue
            row = dal[dal["股票代码"].apply(format_stock_code) == code].iloc[0]
            stop_pct = None
            take_price = None
            strat = str(row.get("来源策略", "") or "")
            try:
                sp = row.get("stop_pct")
                stop_pct = float(sp) if (sp is not None and not pd.isna(sp)) else None
            except (TypeError, ValueError):
                pass
            try:
                tp = row.get("止盈价(上轨)")
                take_price = float(tp) if (tp is not None and not pd.isna(tp)) else None
            except (TypeError, ValueError):
                pass
            # 用量化买入价填充 buy_price/qty，使权益能真实盯市（不再仅现金记账近似）。
            buy_open = self._buy_open_price(code, buy_date)
            if buy_open and buy_open > 0:
                bp = buy_open * (1 + self.exit.get("slippage", 0.001))
                h_qty = alloc / bp
            else:
                bp = None
                h_qty = 0.0
            self.positions[code] = {
                "buy_date": buy_date.strftime("%Y-%m-%d"),
                "buy_price": bp,       # 实际买入价（开盘价+滑点）；None 表示本地缺 K 线，降级为成本记账
                "qty": h_qty,
                "cost": alloc,         # 计划投入现金（开仓占用，出场回收）
                "stop_pct": stop_pct,
                "take_price": take_price,
                "strategy": strat,
                "peak": bp,
                "last_close": bp,
            }
            # 开仓占用现金：已从预算扣减，避免现金与持仓市值重复计数（权益守恒）。
            self.cash -= alloc
            spent += alloc

    def _mark_and_exit(self, code: str, today: date) -> None:
        h = self.positions[code]
        res = simulate_position(code, datetime.strptime(h["buy_date"], "%Y-%m-%d").date(), today,
                                stop_pct=h.get("stop_pct"), take_price=h.get("take_price"),
                                strategy=h.get("strategy", ""), **self.exit)
        # 同步最新收盘价供权益计算
        df = _load_k_window(code, today.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
        if not df.empty:
            h["last_close"] = float(df["close"].iloc[-1])
        if res["exit_reason"] in (None, "horizon", "no_data"):
            return
        # 触发出场：记已实现交易 + 回收市值现金（纸盘近似；实盘应改用实时成交价）。
        bp = h.get("buy_price") or res.get("buy_price")
        sp = res.get("sell_price")
        if bp and sp:
            self.realized.append({
                "code": code, "buy_date": h["buy_date"], "sell_date": res["sell_date"],
                "buy_price": bp, "sell_price": sp, "return_pct": res["return_pct"],
                "exit_reason": res["exit_reason"],
            })
        # 回收市值：优先用模拟卖出价（含滑点），否则退化为当前市值，保证权益守恒。
        qty = h.get("qty") or 0.0
        mv_exit = (qty * sp) if (qty > 0 and sp) else self._market_value(h)
        self.cash += mv_exit
        self.positions.pop(code, None)
