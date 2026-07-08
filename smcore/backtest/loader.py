"""数据适配层 —— 把项目现有 K 线数据接入 Backtrader。

- 个股：复用 smcore.data.kline.fetch_daily_k（akshare/baostock/tdx 自动回退，海外可达）
- 指数：akshare 新浪指数日线（Relativity 策略需要），拉不到则优雅降级
"""
from __future__ import annotations

import threading
from datetime import date
from typing import Optional

import pandas as pd


def _call_with_timeout(func, timeout: float):
    """daemon 线程包裹网络调用，超时返回 None（与 kline.py 同款，避免云端挂起）。"""
    box: dict = {}

    def _run():
        try:
            box["r"] = func()
        except BaseException:
            box["e"] = True

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    worker.join(timeout)
    if worker.is_alive() or "e" in box:
        return None
    return box.get("r")


def load_price_data(
    code: str,
    start: date,
    end: date,
    *,
    min_bars: int = 30,
) -> Optional[pd.DataFrame]:
    """加载个股日 K 线（前复权），整理为 Backtrader 友好的格式。

    Returns:
        DataFrame[date, open, high, low, close, volume, amount]，按日期升序；
        数据不足或获取失败返回 None。
    """
    from smcore.data.kline import fetch_daily_k

    df = fetch_daily_k(code, start, end, adjust="qfq")
    if df is None or df.empty or len(df) < min_bars:
        return None
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    if out.empty or len(out) < min_bars:
        return None
    return out[["date", "open", "high", "low", "close", "volume", "amount"]]


def load_index_data(
    code: str = "000001",
    start: Optional[date] = None,
    end: Optional[date] = None,
    *,
    timeout: float = 30.0,
) -> Optional[pd.DataFrame]:
    """加载指数日线（默认上证指数 sh000001），用于 Relativity 相对强弱。

    指数不含成交额，amount 列置 NaN。拉取失败返回 None，调用方据此关闭 relativity。
    """
    if start is None or end is None:
        return None
    try:
        import akshare as ak
    except ImportError:
        return None

    raw = _call_with_timeout(
        lambda: ak.stock_zh_index_daily(symbol="sh" + code),
        timeout,
    )
    if raw is None or raw.empty:
        return None
    out = raw.rename(
        columns={
            "date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        }
    )
    if "amount" not in out.columns:
        out["amount"] = float("nan")
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    if out.empty:
        return None
    out = out[(out["date"].dt.date >= start) & (out["date"].dt.date <= end)]
    out = out.reset_index(drop=True)
    return out[["date", "open", "high", "low", "close", "volume", "amount"]]
