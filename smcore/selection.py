"""Shared helpers for stock selection scans and candidate lookup."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pandas as pd

from smcore.cache_daily import get_daily
from smcore.data.kline import fetch_daily_k
from smcore.indicators.boll import calc_bollinger, evaluate_boll_signal
from smcore.strategy import fuse_signals, save_action_list
from smcore.utils.code import format_stock_code


def fetch_candidate_codes(price_min: float, price_max: float) -> list[str]:
    """Fetch and filter A-share codes by latest price."""
    import akshare as ak

    try:
        spot = ak.stock_zh_a_spot()
    except Exception:
        return []
    if spot is None or spot.empty:
        return []
    spot = spot[(spot["最新价"] >= price_min) & (spot["最新价"] <= price_max)]
    return [format_stock_code(code) for code in spot["代码"].tolist() if format_stock_code(code)]


def get_candidate_codes(price_min: float, price_max: float) -> tuple[list[str], str | None]:
    """Return cached candidate codes and the cache date."""
    cache_key = f"candidate_codes_{int(price_min)}_{int(price_max)}"
    codes, cache_date = get_daily(cache_key, fetch_candidate_codes, price_min, price_max)
    return codes or [], cache_date


def scan_boll_batch(
    codes: list[str],
    window: int = 20,
    k: float = 1.645,
    near_ratio: float = 1.015,
    days_back: int = 180,
    on_progress=None,
    is_cancelled=None,
) -> pd.DataFrame:
    """Scan a batch of stocks for Bollinger signals.

    Args:
        on_progress: optional callback(index, total, code, status_msg) called per stock.
        is_cancelled: optional callable() -> bool; if returns True the scan stops early.
    """
    results: list[dict[str, Any]] = []
    total = len(codes)

    for i, code in enumerate(codes):
        if is_cancelled and is_cancelled():
            break
        try:
            end_date = date.today()
            start_date = end_date - timedelta(days=days_back)
            kdf = fetch_daily_k(code, start_date, end_date)
            if kdf.empty or len(kdf) < window:
                if on_progress:
                    on_progress(i + 1, total, code, "数据不足，跳过")
                continue
            kdf = calc_bollinger(kdf, window=window, k=k)
            sig = evaluate_boll_signal(kdf, near_ratio=near_ratio)
            signal_type = sig.get("signal_type", "neutral")
            if on_progress:
                on_progress(i + 1, total, code, f"信号: {signal_type}")
            results.append(
                {
                    "代码": code,
                    "最新价": sig.get("price"),
                    "中轨": sig.get("middle"),
                    "下轨": sig.get("lower"),
                    "上轨": sig.get("upper"),
                    "信号": sig.get("signal", "无"),
                    "距下轨%": sig.get("dist_to_lower_pct"),
                    "距上轨%": sig.get("dist_to_upper_pct"),
                }
            )
        except Exception:
            if on_progress:
                on_progress(i + 1, total, code, "异常跳过")
            continue

    if not results:
        return pd.DataFrame()
    return pd.DataFrame(results).sort_values("距下轨%", ascending=True)


def run_strategy_fusion(date_yyyymmdd: str | None = None, total_capital: float = 100000.0, max_picks: int = 15) -> dict[str, Any]:
    """Run strategy fusion and persist the generated action list."""
    date_yyyymmdd = date_yyyymmdd or date.today().strftime("%Y%m%d")
    df, meta = fuse_signals(date_yyyymmdd, total_capital=total_capital, max_picks=max_picks)
    path = save_action_list(df, date_yyyymmdd) if not df.empty else None
    return {
        "date": date_yyyymmdd,
        "count": int(len(df)),
        "meta": meta,
        "saved_path": str(path) if path else None,
        "rows": df.to_dict(orient="records") if not df.empty else [],
    }