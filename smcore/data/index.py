"""指数日线数据获取与指标计算 —— 上证/沪深300 等。

从 auto_notify_boll.py 巨石抽出。依赖 akshare + smcore.cache，不涉及 pipeline 执行框架。
"""
from __future__ import annotations

import pandas as pd
import akshare as ak

from smcore.cache import cache_table_name, read_cache_df, write_cache_df
from smcore.utils.code import to_ak_index_symbol


def _normalize_index_df(df: pd.DataFrame) -> pd.DataFrame:
    """规范化指数 DataFrame：提取 date/close 两列。"""
    if df is None or df.empty:
        return pd.DataFrame()

    col_map_raw = {str(c).strip(): str(c) for c in df.columns}
    date_col = col_map_raw.get("date", "")
    close_col = col_map_raw.get("close", "")

    if not date_col or not close_col:
        col_map_lc = {str(c).strip().lower(): str(c) for c in df.columns}
        date_col = col_map_lc.get("date", "")
        close_col = col_map_lc.get("close", "")

    if not date_col or not close_col:
        return pd.DataFrame()

    out_df = pd.DataFrame()
    out_df["date"] = pd.to_datetime(df[date_col], errors="coerce")
    out_df["close"] = pd.to_numeric(df[close_col], errors="coerce")
    out_df = out_df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    return out_df


def fetch_index_close_series(index_code, start_date_text, end_date_text) -> pd.DataFrame:
    """获取指数日线 close 序列（带 SQLite 缓存）。

    主源 akshare 新浪，失败回退东财。
    """
    symbol = to_ak_index_symbol(index_code)
    out = pd.DataFrame()

    cache_key = f"stock_data/index_close_{symbol}_{start_date_text}_{end_date_text}.csv"
    table_name = cache_table_name(cache_key)
    cached_df = read_cache_df(table_name)
    if not cached_df.empty:
        out = _normalize_index_df(cached_df)
    else:
        try:
            raw = ak.stock_zh_index_daily(symbol=symbol)
            out = _normalize_index_df(raw)
        except Exception:
            out = pd.DataFrame()

    if out.empty:
        try:
            raw_fallback = ak.stock_zh_index_daily(symbol=symbol)
            out = _normalize_index_df(raw_fallback)
        except Exception:
            out = pd.DataFrame()

    if out.empty:
        return out

    write_cache_df(table_name, out)

    start_dt = pd.to_datetime(start_date_text, errors="coerce")
    end_dt = pd.to_datetime(end_date_text, errors="coerce")
    if pd.notna(start_dt):
        out = out[out["date"] >= start_dt]
    if pd.notna(end_dt):
        out = out[out["date"] <= end_dt]
    return out.reset_index(drop=True)


def calc_index_metrics(index_df: pd.DataFrame) -> pd.DataFrame:
    """计算指数 5日/20日收益率与 20日波动率。"""
    if index_df is None or index_df.empty:
        return pd.DataFrame()

    out = index_df[["date", "close"]].copy().sort_values("date").reset_index(drop=True)
    out["ret_5d"] = (out["close"] / out["close"].shift(5) - 1.0) * 100.0
    out["ret_20d"] = (out["close"] / out["close"].shift(20) - 1.0) * 100.0
    daily_ret = out["close"].pct_change() * 100.0
    out["vol_20d"] = daily_ret.rolling(20).std()
    return out
