from __future__ import annotations

from datetime import date, datetime

import baostock as bs
import pandas as pd

from core.data_fetcher import fetch_daily_k_data, format_stock_code, to_baostock_code
from core.indicators import calc_bollinger, evaluate_boll_signal


def analyze_stock(
    code: str,
    start_date: str | date,
    end_date: str | date,
    window: int = 20,
    k: float = 1.645,
    near_ratio: float = 1.015,
    adjust: str = "qfq",
    stock_name: str = "",
) -> tuple[pd.DataFrame, dict[str, object]]:
    code_6 = format_stock_code(code)
    raw_df = fetch_daily_k_data(code_6, start_date=start_date, end_date=end_date, adjust=adjust)
    if raw_df.empty:
        summary = {
            "股票代码": code_6,
            "股票名称": stock_name,
            "最新收盘": None,
            "下轨": None,
            "上轨": None,
            "信号": "无数据",
            "命中策略": False,
        }
        return raw_df, summary

    boll_df = calc_bollinger(raw_df, window=window, k=k)
    signal_info = evaluate_boll_signal(boll_df, near_ratio=near_ratio)
    latest = boll_df.iloc[-1]
    summary = {
        "股票代码": code_6,
        "股票名称": stock_name,
        "最新收盘": round(float(latest["close"]), 3),
        "下轨": round(float(latest["Lower"]), 3) if pd.notna(latest["Lower"]) else None,
        "上轨": round(float(latest["Upper"]), 3) if pd.notna(latest["Upper"]) else None,
        "信号": signal_info["signal"],
        "命中策略": bool(signal_info["selected"]),
    }
    return boll_df, summary


def _to_date_string(value: str | date | datetime) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    text = str(value)
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def _result_set_to_df(result_set) -> pd.DataFrame:
    rows: list[list[str]] = []
    while result_set.next():
        rows.append(result_set.get_row_data())
    return pd.DataFrame(rows, columns=result_set.fields)


def _fetch_daily_k_data_in_session(
    code: str,
    start_date: str | date,
    end_date: str | date,
    adjust: str,
) -> pd.DataFrame:
    adjust_map = {"hfq": "1", "qfq": "2", "bfq": "3"}
    adjust_flag = adjust_map.get(str(adjust).lower(), "2")

    result_set = bs.query_history_k_data_plus(
        to_baostock_code(code),
        "date,code,open,high,low,close,volume,amount",
        start_date=_to_date_string(start_date),
        end_date=_to_date_string(end_date),
        frequency="d",
        adjustflag=adjust_flag,
    )
    if result_set.error_code != "0":
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])

    normalized = _result_set_to_df(result_set)
    if normalized.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])

    for column_name in ["open", "high", "low", "close", "volume", "amount"]:
        normalized[column_name] = pd.to_numeric(normalized[column_name], errors="coerce")

    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized = normalized.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    normalized["date"] = normalized["date"].dt.strftime("%Y-%m-%d")
    return normalized[["date", "open", "high", "low", "close", "volume", "amount"]]


def analyze_stocks(
    codes: list[str],
    start_date: str | date,
    end_date: str | date,
    window: int = 20,
    k: float = 1.645,
    near_ratio: float = 1.015,
    adjust: str = "qfq",
    code_name_map: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    code_name_map = code_name_map or {}
    rows: list[dict[str, object]] = []
    data_map: dict[str, pd.DataFrame] = {}

    login_result = bs.login()
    session_ok = login_result.error_code == "0"

    try:
        for code in codes:
            code_6 = format_stock_code(code)
            stock_name = code_name_map.get(code_6, "")

            if session_ok:
                raw_df = _fetch_daily_k_data_in_session(
                    code=code_6,
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust,
                )
            else:
                raw_df = fetch_daily_k_data(code_6, start_date=start_date, end_date=end_date, adjust=adjust)

            if raw_df.empty:
                summary = {
                    "股票代码": code_6,
                    "股票名称": stock_name,
                    "最新收盘": None,
                    "下轨": None,
                    "上轨": None,
                    "信号": "无数据",
                    "命中策略": False,
                }
                chart_df = raw_df
            else:
                chart_df = calc_bollinger(raw_df, window=window, k=k)
                signal_info = evaluate_boll_signal(chart_df, near_ratio=near_ratio)
                latest = chart_df.iloc[-1]
                summary = {
                    "股票代码": code_6,
                    "股票名称": stock_name,
                    "最新收盘": round(float(latest["close"]), 3),
                    "下轨": round(float(latest["Lower"]), 3) if pd.notna(latest["Lower"]) else None,
                    "上轨": round(float(latest["Upper"]), 3) if pd.notna(latest["Upper"]) else None,
                    "信号": signal_info["signal"],
                    "命中策略": bool(signal_info["selected"]),
                }

            rows.append(summary)
            data_map[code_6] = chart_df
    finally:
        if session_ok:
            bs.logout()

    result_df = pd.DataFrame(rows)
    if not result_df.empty:
        result_df = result_df.sort_values(by=["命中策略", "股票代码"], ascending=[False, True]).reset_index(drop=True)
    return result_df, data_map
