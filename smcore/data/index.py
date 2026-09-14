"""指数日线数据获取与指标计算 —— 上证/沪深300 等。

从 auto_notify_boll.py 巨石抽出。主源同花顺官方（hithink），回退 akshare；依赖 smcore.cache，
不涉及 pipeline 执行框架。

⚠️ 定位说明（2026-09-14 梳理）——本模块是**遗留脚本专用**实现，smcore 内部已无调用方，
当前仅 `Frequently-Used-Program/auto_notify_boll.py` 经 `smcore.data` 转发使用。

smcore 内的「指数数据」有两条统一入口，按用途区分：
- `smcore.strategy.market._get_index_series`：任意指数的日线 DataFrame，hithink → 新浪历史K线
  → baostock → akshare、进程内缓存 → 供 regime 判别的 `compute_market_profile` 使用；
- `smcore.strategy.regime_filter._get_hs300_close`：沪深300 **收盘 Series**、按自然日失效
  → 供 RS 过滤 / 归因 / 自适应权重 / 组合β 使用。

三者接口语义本就不同（本模块=任意区间 DataFrame + SQLite 缓存；market=全量 DataFrame +
进程缓存；regime_filter=收盘 Series + 按日失效），故**不做强行合并**。
（2026-09-14 已按「独立数据源变更任务」把三者取数**主源统一为 hithink**——只换源、不合并实现；
regime 换源属行为变更，OOS 复核待做。）
"""
from __future__ import annotations

import pandas as pd

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

    主源同花顺官方云 API（hithink，全环境统一、纯 requests），失败回退 akshare 新浪。
    """
    symbol = to_ak_index_symbol(index_code)
    out = pd.DataFrame()

    cache_key = f"stock_data/index_close_{symbol}_{start_date_text}_{end_date_text}.csv"
    table_name = cache_table_name(cache_key)
    cached_df = read_cache_df(table_name)
    if not cached_df.empty:
        out = _normalize_index_df(cached_df)
    else:
        # 首选：同花顺官方指数历史K（全环境可达；纯 requests，不依赖 akshare）
        try:
            from smcore.data import hithink as _hk

            if _hk.available() and len(symbol) > 2 and symbol[:2] in ("sh", "sz"):
                _s = pd.to_datetime(start_date_text, errors="coerce")
                _e = pd.to_datetime(end_date_text, errors="coerce")
                if pd.notna(_s) and pd.notna(_e):
                    _ths = f"{symbol[2:]}.{symbol[:2].upper()}"  # sh000300 -> 000300.SH
                    _raw = _hk.fetch_index_historical(
                        _ths, _s.strftime("%Y-%m-%d"), _e.strftime("%Y-%m-%d")
                    )
                    out = _normalize_index_df(_raw)
        except Exception:
            out = pd.DataFrame()
        if out.empty:
            from smcore.utils.ak_compat import get_ak

            ak = get_ak()  # 懒加载：CI runner 可能未安装
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
