"""实时行情报价 —— 同花顺官方快照为主源，纯 requests，无需 akshare。

源优先级（逐级 fail-soft 降级）：
  1. 同花顺官方快照 hithink（全环境统一；需 HITHINK_FINANCE_API_KEY + 联网）
  2. 通达信直连（本地/国内，毫秒级）
  3. 新浪财经 HTTP 按需查询（quote_sina，秒级）
  4. 全市场快照缓存（新浪源，兜底）

缓存策略：
- 内存缓存：进程内，5 分钟过期
- 磁盘缓存：pickle 文件，5 分钟过期，跨进程复用
"""
from __future__ import annotations

import pickle
import time
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from smcore.config.defaults import STOCK_DATA_DIR
from smcore.data.quote_sina import fetch_sina_quotes
from smcore.utils.code import format_stock_code

_CACHE_TTL = 300.0  # 5 分钟
_DISK_CACHE = STOCK_DATA_DIR / "cache" / "realtime_snapshot.pkl"

# 内存缓存
_mem_df: Optional[pd.DataFrame] = None
_mem_ts: float = 0.0


def _load_full_snapshot() -> pd.DataFrame:
    """加载全市场实时快照（新浪源）。

    使用 ak.stock_zh_a_spot()（新浪）替代东财接口。
    新浪源同样返回全市场数据，列名与东财版一致。
    """
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

    # 3. 网络（新浪源）
    try:
        import akshare as ak
        raw = ak.stock_zh_a_spot()
        if raw is None or raw.empty:
            return pd.DataFrame(columns=["code", "name", "price", "pct"])
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

    源优先级：同花顺官方快照（全环境统一）→ 通达信直连（本地毫秒级）→ 新浪 HTTP
    按需查询（秒级）→ 全市场快照缓存。每一级均 fail-soft，失败自动降级。

    Args:
        codes: 股票代码列表（任意格式，内部标准化）

    Returns:
        DataFrame[code, name, price, pct]
    """
    codes_set = {format_stock_code(c) for c in codes if format_stock_code(c)}
    if not codes_set:
        return pd.DataFrame(columns=["code", "name", "price", "pct"])

    # 首选：同花顺官方快照（hithink）。注意 thscode 才是权威代码（指数端点的 ticker
    # 是内部码如 1B0300，不可用）；快照端点不含名称，用离线名称索引补名（零联网）。
    try:
        from smcore.data import hithink as _hk
        if _hk.available():
            snap = _hk.fetch_snapshot(list(codes_set))
            if snap is not None and not snap.empty:
                rows = []
                for _, it in snap.iterrows():
                    c6 = format_stock_code(it.get("thscode") or "")
                    if not c6:
                        continue
                    price = it.get("last_price")
                    if price is None or pd.isna(price):
                        continue
                    pct = it.get("price_change_ratio_pct")
                    rows.append({
                        "code": c6,
                        "name": "",
                        "price": round(float(price), 2),
                        "pct": round(float(pct), 2) if pct is not None and not pd.isna(pct) else 0.0,
                    })
                if rows:
                    try:
                        from smcore.stock_names import code_to_name as _c2n
                        for r in rows:
                            r["name"] = _c2n(r["code"])
                    except Exception:
                        pass
                    return pd.DataFrame(rows)
    except Exception:
        pass

    # 其次：通达信直连（毫秒级、最稳）
    try:
        from smcore.data.tdx_client import available as tdx_available, get_client
        if tdx_available():
            q = get_client().get_realtime_quotes(list(codes_set))
            if q:
                rows = []
                for code, info in q.items():
                    price = info.get("price")
                    if price is None:
                        continue
                    pre = info.get("last_close") or 0
                    pct = ((price - pre) / pre * 100) if pre else 0.0
                    rows.append({
                        "code": code,
                        "name": info.get("name", ""),
                        "price": round(price, 2),
                        "pct": round(pct, 2),
                    })
                if rows:
                    return pd.DataFrame(rows)
    except Exception:
        pass

    # 再次：新浪HTTP按需查询（快，不拉全量）
    try:
        sina_result = fetch_sina_quotes(codes_set)
        if sina_result:
            rows = []
            for code, info in sina_result.items():
                if info.get("price") is not None:
                    pre_close = info.get("pre_close")
                    pct = ((info["price"] - pre_close) / pre_close * 100) if pre_close else 0.0
                    rows.append({
                        "code": code,
                        "name": info.get("name", ""),
                        "price": info["price"],
                        "pct": round(pct, 2),
                    })
            if rows:
                return pd.DataFrame(rows)
    except Exception:
        pass

    # 回退：全市场快照缓存
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
