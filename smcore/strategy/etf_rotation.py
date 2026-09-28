"""ETF 动量轮动（仅多头）——为「无做空 + 只玩主板/基金/ETF」的散户设计。

思路（Antonacci 双动量 + 相对强弱轮动）
--------------------------------------
- 相对动量：每 `rebal_days` 个交易日，按过去 `lookback` 日收益对 ETF 池排序，取前 `top_k` 只等权。
- 绝对动量（abs_filter）：只保留动量 > 0 的标的；若榜首也 ≤0 → **全部转现金**（熊市避险）。
- 纯多头、无做空、无杠杆；可执行（ETF 场内交易，跨境 ETF 支持 T+0）。
- 计交易摩擦：每次调仓按换手率计 `cost_bps`（双边）。

数据来自 `smcore.data.etf_kline.load_etf_closes`（腾讯 ifzq，前复权）。
参数全部函数入参（零硬编码），便于 walk-forward。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from smcore.data.etf_kline import load_etf_closes


def _metrics(port_ret: pd.Series) -> dict:
    """由日收益序列算权益/年化/夏普/回撤/波动。"""
    r = port_ret.dropna()
    if len(r) == 0:
        return {"ok": False}
    eq = (1.0 + r).cumprod()
    total = float(eq.iloc[-1] - 1.0)
    n_days = len(r)
    ann = float((1.0 + total) ** (252.0 / n_days) - 1.0) if n_days > 0 else 0.0
    vol = float(r.std() * np.sqrt(252))
    sharpe = float(r.mean() / r.std() * np.sqrt(252)) if r.std() > 0 else 0.0
    peak = eq.cummax()
    dd = float((eq / peak - 1.0).min())
    win = float((r > 0).mean())
    return {"ok": True, "n_days": n_days, "total_return": total, "ann_return": ann,
            "sharpe": sharpe, "ann_vol": vol, "max_drawdown": dd, "win_rate": win,
            "equity": eq}


def backtest_rotation(
    closes: pd.DataFrame,
    *,
    lookback: int = 60,
    top_k: int = 5,
    rebal_days: int = 10,
    cost_bps: float = 5.0,
    abs_filter: bool = True,
    universe: list[str] | None = None,
) -> dict:
    """日线动量轮动回测（调仓在收盘、权重次日起生效，无未来函数）。

    Returns: 指标 dict（含 equity 权益曲线、turnover 累计换手）。
    """
    codes = [c for c in (universe or list(closes.columns)) if c in closes.columns]
    px = closes[codes].ffill()
    rets = px.pct_change(fill_method=None)
    dates = px.index
    n = len(dates)
    if n <= lookback + 5:
        return {"ok": False, "reason": "样本不足"}
    port_ret = np.zeros(n)
    cur_w: dict[str, float] = {}
    total_turn = 0.0
    n_rebal = 0
    held_sum = 0
    held_n = 0
    cash_days = 0
    for t in range(lookback, n):
        # 1) 用当前持仓赚当日收益（权重在上一调仓日收盘决定，无未来函数）
        if cur_w:
            r = 0.0
            for c, w in cur_w.items():
                v = rets[c].iloc[t]
                if pd.notna(v):
                    r += w * float(v)
            port_ret[t] = r
        held_sum += len(cur_w)
        held_n += 1
        if not cur_w:
            cash_days += 1
        # 2) 调仓（收盘，次日起生效）
        if (t - lookback) % rebal_days == 0:
            mom: dict[str, float] = {}
            for c in codes:
                p0 = px[c].iloc[t - lookback]
                p1 = px[c].iloc[t]
                if pd.notna(p0) and pd.notna(p1) and p0 > 0:
                    mom[c] = float(p1 / p0 - 1.0)
            ranked = sorted(mom.items(), key=lambda x: -x[1])
            if abs_filter:
                ranked = [(c, m) for c, m in ranked if m > 0]
            sel = [c for c, _ in ranked[:top_k]]
            new_w = {c: 1.0 / len(sel) for c in sel} if sel else {}
            allc = set(new_w) | set(cur_w)
            turn = sum(abs(new_w.get(c, 0.0) - cur_w.get(c, 0.0)) for c in allc)
            port_ret[t] -= turn * cost_bps / 1e4
            total_turn += turn
            cur_w = new_w
            n_rebal += 1
    m = _metrics(pd.Series(port_ret, index=dates))
    m["turnover"] = round(total_turn, 2)
    m["n_rebal"] = n_rebal
    m["avg_hold"] = held_sum / held_n if held_n else 0.0
    m["cash_days_pct"] = round(cash_days / held_n, 3) if held_n else 0.0
    return m


def run_rotation(
    *,
    lookback: int = 60,
    top_k: int = 5,
    rebal_days: int = 10,
    cost_bps: float = 5.0,
    abs_filter: bool = True,
) -> dict:
    """加载 ETF 数据并跑一次轮动回测，附「全池等权买入持有」基准。"""
    closes = load_etf_closes()
    strat = backtest_rotation(closes, lookback=lookback, top_k=top_k,
                              rebal_days=rebal_days, cost_bps=cost_bps, abs_filter=abs_filter)
    # 基准：全池等权买入持有（同为多头，无调仓）
    base_rets = closes[list(closes.columns)].ffill().pct_change(fill_method=None).mean(axis=1)
    base = _metrics(base_rets)
    return {
        "params": dict(lookback=lookback, top_k=top_k, rebal_days=rebal_days,
                       cost_bps=cost_bps, abs_filter=abs_filter, n_etf=closes.shape[1]),
        "strategy": strat,
        "benchmark_equalweight_bh": {k: v for k, v in base.items() if k != "equity"},
    }
