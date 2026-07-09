"""Shared helpers for stock selection scans and candidate lookup."""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pandas as pd

from smcore.cache_daily import get_daily
from smcore.data.kline import fetch_daily_k
from smcore.indicators.boll import calc_bollinger, evaluate_boll_signal
from smcore.strategies.boll import run_boll
from smcore.strategy import fuse_signals, save_action_list
from smcore.utils.code import format_stock_code


def fetch_candidate_codes(price_min: float, price_max: float) -> list[str]:
    """Fetch and filter A-share codes by latest price.

    Excludes 北交所 (920xxx), 三板/退市 (4xxx/8xxx) stocks that lack kline data.
    """
    import akshare as ak

    try:
        spot = ak.stock_zh_a_spot()
    except Exception:
        return []
    if spot is None or spot.empty:
        return []
    spot = spot[(spot["最新价"] >= price_min) & (spot["最新价"] <= price_max)]
    codes = []
    for raw_code in spot["代码"].tolist():
        code = format_stock_code(raw_code)
        if not code:
            continue
        # Skip 北交所 (920xxx), 三板/退市 (4xxx/8xxx), 科创板 (688xxx optional)
        if code.startswith("920") or code.startswith("4") or code.startswith("8"):
            continue
        codes.append(code)
    return codes


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

    若 codes 为空（如前端触发"运行完整布林选股"），则直接运行 auto-boll 多因子
    选股（资金流 + 基本面 + 重要股东 + 布林），返回与每日流水线一致的结果。

    Args:
        on_progress: optional callback(index, total, code, status_msg) called per stock.
        is_cancelled: optional callable() -> bool; if returns True the scan stops early.
    """
    # 无候选 → 运行真实 auto-boll 多因子选股（daily-pick 同款逻辑）。
    if not codes:
        if on_progress:
            on_progress(0, 1, "", "运行 auto-boll 多因子选股 ...")
        df = run_boll(k=k, near_ratio=near_ratio, days_back=days_back)
        if on_progress:
            on_progress(1, 1, "", f"完成，命中 {len(df)} 只")
        # 归一化列名，使前端"策略扫描结果"表格与轻量扫描一致显示
        if df.empty:
            return pd.DataFrame(columns=["代码", "名称", "最新价", "信号"])
        return pd.DataFrame({
            "代码": df["股票代码"].tolist(),
            "名称": df["股票名称"].tolist(),
            "最新价": df["建议买入价"].tolist(),
            "信号": ["auto-boll 多因子"] * len(df),
        })

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


def run_strategy_fusion(
    date_yyyymmdd: str | None = None,
    total_capital: float = 100000.0,
    max_picks: int = 15,
    max_stale_days: int = 3,
) -> dict[str, Any]:
    """Run strategy fusion and persist the generated action list."""
    date_yyyymmdd = date_yyyymmdd or date.today().strftime("%Y%m%d")
    df, meta = fuse_signals(
        date_yyyymmdd,
        total_capital=total_capital,
        max_picks=max_picks,
        max_stale_days=max_stale_days,
    )
    path = save_action_list(df, date_yyyymmdd) if not df.empty else None
    return {
        "date": date_yyyymmdd,
        "count": int(len(df)),
        "meta": meta,
        "saved_path": str(path) if path else None,
        "rows": df.to_dict(orient="records") if not df.empty else [],
    }