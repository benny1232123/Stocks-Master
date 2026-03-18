from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterable
import re

import baostock as bs
import pandas as pd


def format_stock_code(code: str | int) -> str:
    digits = "".join(ch for ch in str(code) if ch.isdigit())
    return digits.zfill(6)


def to_baostock_code(code: str | int) -> str:
    code_6 = format_stock_code(code)
    return f"sh.{code_6}" if code_6.startswith("6") else f"sz.{code_6}"


def parse_amount_text(raw_value: object) -> float:
    if raw_value is None:
        return float("nan")

    text = str(raw_value).strip().replace(",", "")
    if not text or text in {"-", "--", "nan", "None"}:
        return float("nan")

    multiplier = 1.0
    if text.endswith("亿"):
        multiplier = 1e8
        text = text[:-1]
    elif text.endswith("万"):
        multiplier = 1e4
        text = text[:-1]

    matched = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not matched:
        return float("nan")

    return float(matched.group(0)) * multiplier


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


def fetch_daily_k_data(
    code: str | int,
    start_date: str | date,
    end_date: str | date,
    adjust: str = "qfq",
) -> pd.DataFrame:
    adjust_map = {"hfq": "1", "qfq": "2", "bfq": "3"}
    adjust_flag = adjust_map.get(str(adjust).lower(), "2")

    login_result = bs.login()
    if login_result.error_code != "0":
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])

    try:
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
    finally:
        bs.logout()

    if normalized.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])

    for column_name in ["open", "high", "low", "close", "volume", "amount"]:
        normalized[column_name] = pd.to_numeric(normalized[column_name], errors="coerce")

    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized = normalized.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    normalized["date"] = normalized["date"].dt.strftime("%Y-%m-%d")
    return normalized[["date", "open", "high", "low", "close", "volume", "amount"]]


def fetch_code_name_map(codes: Iterable[str] | None = None) -> dict[str, str]:
    if not codes:
        return {}

    login_result = bs.login()
    if login_result.error_code != "0":
        return {}

    code_name_map: dict[str, str] = {}
    try:
        for code in codes:
            code_6 = format_stock_code(code)
            result_set = bs.query_stock_basic(code=to_baostock_code(code_6))
            if result_set.error_code != "0":
                continue
            data_frame = _result_set_to_df(result_set)
            if data_frame.empty:
                continue
            name_column = "code_name" if "code_name" in data_frame.columns else None
            if not name_column:
                continue
            code_name_map[code_6] = str(data_frame.iloc[0][name_column])
    finally:
        bs.logout()

    return code_name_map


def infer_report_period(anchor_date: date | datetime | str | None = None) -> tuple[int, int]:
    if anchor_date is None:
        anchor = date.today()
    else:
        anchor = pd.to_datetime(anchor_date).date()

    current_year = anchor.year
    current_month = anchor.month
    if current_month < 5:
        return current_year - 1, 3
    if current_month < 9:
        return current_year, 1
    if current_month < 11:
        return current_year, 2
    return current_year, 3


def previous_report_period(year: int, quarter: int) -> tuple[int, int]:
    if quarter <= 1:
        return year - 1, 4
    return year, quarter - 1


def fetch_all_a_share_codes(max_lookback_days: int = 10) -> list[str]:
    login_result = bs.login()
    if login_result.error_code != "0":
        return []

    try:
        raw_df = pd.DataFrame()
        for offset in range(max_lookback_days + 1):
            day = (date.today() - timedelta(days=offset)).strftime("%Y-%m-%d")
            result_set = bs.query_all_stock(day=day)
            if result_set.error_code != "0":
                continue
            temp_df = _result_set_to_df(result_set)
            if not temp_df.empty:
                raw_df = temp_df
                break

        if raw_df.empty or "code" not in raw_df.columns:
            return []

        code_series = raw_df["code"].astype(str)
        code_series = code_series[code_series.str.match(r"^(sh|sz)\.\d{6}$", na=False)]
        code_series = code_series.str[-6:]

        sh_mask = code_series.str.match(r"^(600|601|603|605|688)\d{3}$", na=False)
        sz_mask = code_series.str.match(r"^(000|001|002|003|300|301)\d{3}$", na=False)
        code_series = code_series[sh_mask | sz_mask]

        return sorted(code_series.dropna().unique().tolist())
    finally:
        bs.logout()
