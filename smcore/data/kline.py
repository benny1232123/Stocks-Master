"""K线数据获取 —— 单一真相源（强制前复权）。

合并自 boll-visualizer/src/core/data_fetcher.py 的 K线部分，关键改动：
- 强制前复权(qfq)：此前 Stock-Selection-Boll.py 用不复权(adjustflag=3)，
  除权除息日布林带断裂、信号失真，是"结果不可信"的头号原因。
- 统一 baostock 会话：用 core.data.session 单例，避免每只股票重复登录。
- 云端后端：环境变量 KLINE_BACKEND=akshare 时改用 akshare HTTP 接口（东财数据源），
  不依赖 baostock 登录会话，适合 GitHub Actions / SCF 等云端环境。
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from smcore.config import ADJUST_FLAG_MAP, CACHE_DIR, CSV_ENCODING, DEFAULT_ADJUST
from smcore.utils.code import format_stock_code, to_baostock_code

K_DATA_CACHE_DIR = CACHE_DIR / "k_data"
DAILY_K_COLUMNS = ["date", "open", "high", "low", "close", "volume", "amount"]


def _backend() -> str:
    """返回当前 K 线后端：baostock（本地）或 akshare（云端）。

    优先读取 KLINE_BACKEND 环境变量，未设置时自动检测 baostock 是否可用，
    不可用则自动回退到 akshare（避免云端部署报 No module named 'baostock'）。
    """
    backend = os.getenv("KLINE_BACKEND", "").strip().lower()
    if backend in ("baostock", "akshare"):
        return backend
    # 自动检测：baostock 可用则用 baostock，否则用 akshare
    try:
        import baostock as bs  # noqa: F401
        return "baostock"
    except ImportError:
        return "akshare"


def _to_date_string(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    text = str(value)
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def _to_date(value) -> date:
    return pd.to_datetime(value).date()


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=DAILY_K_COLUMNS)


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "date" not in df.columns:
        return _empty_df()
    out = df.copy()
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        if col not in out.columns:
            out[col] = pd.NA
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    if out.empty:
        return _empty_df()
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[DAILY_K_COLUMNS]


def _cache_path(code: str, adjust: str) -> Path:
    K_DATA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return K_DATA_CACHE_DIR / f"{format_stock_code(code)}_{adjust}_full.csv"


def _is_fresh(path: Path, max_age_hours: float) -> bool:
    if not path.exists():
        return False
    if max_age_hours <= 0:
        return True
    age = datetime.now().timestamp() - path.stat().st_mtime
    return age <= max_age_hours * 3600


def _slice(df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    if df.empty:
        return df
    tmp = df.copy()
    tmp["_dt"] = pd.to_datetime(tmp["date"], errors="coerce")
    mask = (tmp["_dt"].dt.date >= start) & (tmp["_dt"].dt.date <= end)
    tmp = tmp[mask].drop(columns=["_dt"])
    return _normalize(tmp)


def fetch_daily_k(
    code,
    start_date,
    end_date,
    adjust: str = DEFAULT_ADJUST,
    use_cache: bool = True,
    force_refresh: bool = False,
    max_cache_age_hours: float = 24.0,
) -> pd.DataFrame:
    """获取日 K 线（默认前复权），带文件缓存与增量合并。

    Args:
        code: 股票代码（任意格式）。
        start_date / end_date: 日期（date/datetime/字符串/YYYYMMDD 均可）。
        adjust: 复权方式 qfq(默认)/hfq/bfq。强制不传 "3"（不复权）以避免信号失真。
        use_cache / force_refresh / max_cache_age_hours: 缓存控制。
    """
    code6 = format_stock_code(code)
    if not code6:
        return _empty_df()
    adjust = str(adjust).lower()
    flag = ADJUST_FLAG_MAP.get(adjust, "2")  # 兜底前复权
    request_start = _to_date(start_date)
    request_end = _to_date(end_date)
    if request_start > request_end:
        return _empty_df()

    cache = _cache_path(code6, adjust)
    cached = pd.DataFrame()
    if use_cache and not force_refresh and cache.exists():
        try:
            cached = _normalize(pd.read_csv(cache))
        except Exception:
            cached = pd.DataFrame()

    cache_min, cache_max = None, None
    if not cached.empty:
        dt = pd.to_datetime(cached["date"], errors="coerce").dropna()
        if not dt.empty:
            cache_min, cache_max = dt.min().date(), dt.max().date()

    covers = bool(cache_min and cache_max and cache_min <= request_start and cache_max >= request_end)
    fresh = _is_fresh(cache, max_cache_age_hours)
    if covers and (fresh or request_end < date.today() - timedelta(days=1)):
        return _slice(cached, request_start, request_end)

    segments: list[tuple[date, date]] = []
    if force_refresh or cached.empty or cache_min is None:
        segments.append((request_start, request_end))
    else:
        if request_start < cache_min:
            segments.append((request_start, min(request_end, cache_min - timedelta(days=1))))
        if request_end > cache_max:
            segments.append((max(request_start, cache_max + timedelta(days=1)), request_end))
        if covers and not fresh and request_end >= date.today() - timedelta(days=1):
            segments.append((max(request_start, request_end - timedelta(days=10)), request_end))

    parts: list[pd.DataFrame] = []
    if _backend() == "akshare":
    # 云端后端：akshare HTTP 接口（新浪数据源，无需登录会话）
        for seg_start, seg_end in segments:
            if seg_start > seg_end:
                continue
            seg_df = _fetch_via_akshare(code6, seg_start, seg_end, adjust)
            if not seg_df.empty:
                parts.append(seg_df)
    else:
        # 本地后端：baostock（需登录会话）
        import baostock as bs
        from smcore.data.session import session
        with session() as ok:
            if not ok:
                return _slice(cached, request_start, request_end) if not cached.empty else _empty_df()
            for seg_start, seg_end in segments:
                if seg_start > seg_end:
                    continue
                rs = bs.query_history_k_data_plus(
                    to_baostock_code(code6),
                    "date,code,open,high,low,close,volume,amount",
                    start_date=_to_date_string(seg_start),
                    end_date=_to_date_string(seg_end),
                    frequency="d",
                    adjustflag=flag,
                )
                if rs.error_code != "0":
                    continue
                rows = []
                while rs.next():
                    rows.append(rs.get_row_data())
                if rows:
                    parts.append(pd.DataFrame(rows, columns=rs.fields))

    if cached.empty and not parts:
        return _empty_df()
    merged = _normalize(pd.concat([cached, *parts], ignore_index=True)) if (not cached.empty or parts) else _empty_df()
    if merged.empty and not cached.empty:
        merged = _normalize(cached)
    if use_cache and not merged.empty:
        merged.to_csv(cache, index=False, encoding=CSV_ENCODING)
    return _slice(merged, request_start, request_end) if not merged.empty else _empty_df()


# ── akshare 后端（云端用） ──

_AK_COL_MAP = {
    "日期": "date", "开盘": "open", "收盘": "close",
    "最高": "high", "最低": "low", "成交量": "volume", "成交额": "amount",
}


def _fetch_via_akshare(code6: str, start: date, end: date, adjust: str) -> pd.DataFrame:
    """通过 akshare 新浪接口获取 K 线（无需登录会话）。

    使用 stock_zh_a_daily（新浪数据源），不依赖东财接口。
    """
    try:
        import akshare as ak
    except ImportError:
        return pd.DataFrame()

    # 新浪格式 symbol：sh600519 / sz000001
    sina_symbol = ("sh" if code6.startswith(("5", "6", "9")) else "sz") + code6

    # akshare 复权参数：qfq/hfq/"" (空=不复权)
    ak_adjust = adjust if adjust in ("qfq", "hfq") else ""
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")

    try:
        raw = ak.stock_zh_a_daily(
            symbol=sina_symbol,
            start_date=start_str,
            end_date=end_str,
            adjust=ak_adjust,
        )
    except Exception:
        return pd.DataFrame()

    if raw is None or raw.empty:
        return pd.DataFrame()

    # stock_zh_a_daily 返回英文列名：date, open, high, low, close, volume, amount
    out = raw.copy()
    for col in DAILY_K_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    return out[DAILY_K_COLUMNS]
