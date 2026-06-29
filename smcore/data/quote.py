"""实时行情报价 —— 双层缓存（内存 + 磁盘）。

akshare 全市场实时接口（stock_zh_a_spot_em）较慢（~80秒/5867只），
但持仓盈亏只需要持仓股的实时价。本模块拉一次全量后缓存 5 分钟，
持仓股从中过滤，后续调用秒级返回。

缓存策略：
- 内存缓存：进程内，5 分钟过期
- 磁盘缓存：pickle 文件，5 分钟过期，跨进程复用（命令行场景受益）
"""
from __future__ import annotations

import pickle
import time
from pathlib import Path
from typing import Iterable, Optional

import akshare as ak
import pandas as pd

from smcore.config.defaults import STOCK_DATA_DIR
from smcore.utils.code import format_stock_code

_CACHE_TTL = 300.0  # 5 分钟
_DISK_CACHE = STOCK_DATA_DIR / "cache" / "realtime_snapshot.pkl"

# 内存缓存
_mem_df: Optional[pd.DataFrame] = None
_mem_ts: float = 0.0


def _load_full_snapshot() -> pd.DataFrame:
    """加载全市场实时快照（内存→磁盘→网络，三级回退）。"""
    global _mem_df, _mem_ts
    now = time.time()

    # 1. 内存缓存
    if _mem_df is not None and (now - _mem_ts) < _CACHE_TTL:
        return _mem_df

    # 2. 磁盘缓存
    if _DISK_CACHE.exists():
        age = now - _DISK_CACHE.stat().st_mtime
        if age < _CACHE_TTL:
            try:
                with _DISK_CACHE.open("rb") as f:
                    df = pickle.load(f)
                if isinstance(df, pd.DataFrame) and not df.empty:
                    _mem_df = df
                    _mem_ts = _DISK_CACHE.stat().st_mtime
                    return df
            except Exception:
                pass

    # 3. 网络
    try:
        raw = ak.stock_zh_a_spot_em()
        df = raw.rename(columns={"代码": "code", "名称": "name", "最新价": "price", "涨跌幅": "pct"})
        df["code"] = df["code"].astype(str).map(format_stock_code)
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["pct"] = pd.to_numeric(df["pct"], errors="coerce")
        df = df[["code", "name", "price", "pct"]].dropna(subset=["code", "price"])

        # 写双层缓存
        _mem_df = df
        _mem_ts = now
        _DISK_CACHE.parent.mkdir(parents=True, exist_ok=True)
        with _DISK_CACHE.open("wb") as f:
            pickle.dump(df, f)
        return df
    except Exception:
        return pd.DataFrame(columns=["code", "name", "price", "pct"])


def fetch_realtime_quotes(codes: Iterable[str]) -> pd.DataFrame:
    """获取指定股票的实时报价。

    Args:
        codes: 股票代码列表（任意格式，内部标准化）

    Returns:
        DataFrame[code, name, price, pct]
    """
    codes_set = {format_stock_code(c) for c in codes if format_stock_code(c)}
    if not codes_set:
        return pd.DataFrame(columns=["code", "name", "price", "pct"])

    full = _load_full_snapshot()
    return full[full["code"].isin(codes_set)].reset_index(drop=True)


def fetch_realtime_price(code: str) -> Optional[float]:
    """获取单只股票实时价格。"""
    df = fetch_realtime_quotes([code])
    if df.empty:
        return None
    return float(df.iloc[0]["price"])


def clear_quote_cache() -> None:
    """清除内存+磁盘缓存（强制下次重拉）。"""
    global _mem_df, _mem_ts
    _mem_df = None
    _mem_ts = 0.0
    try:
        _DISK_CACHE.unlink(missing_ok=True)
    except Exception:
        pass
