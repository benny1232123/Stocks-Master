"""财报报告期推断 —— 单一真相源。

口径（采纳 data_fetcher 的稳健版本：只用披露期已结束的财报）：
- 1-4月: 去年三季报(0930)   年报披露中，用已确定的三季报
- 5-8月: 今年一季报(0331)
- 9-10月: 今年中报(0630)
- 11-12月: 今年三季报(0930)

注: Stock-Selection-Boll.py 此前 <5月 用去年年报(1231)，但年报 1-4月披露中、
未必齐全，故统一改用三季报。
"""
from __future__ import annotations

from datetime import date, datetime

_QUARTER_MONTH_DAY = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}


def _to_date(anchor) -> date:
    if anchor is None:
        return date.today()
    if isinstance(anchor, datetime):
        return anchor.date()
    if isinstance(anchor, date):
        return anchor
    return datetime.fromisoformat(str(anchor)[:10]).date()


def infer_report_period(anchor=None) -> tuple[int, int]:
    """返回 (year, quarter) 最近已披露财报期。"""
    d = _to_date(anchor)
    y, m = d.year, d.month
    if m < 5:
        return y - 1, 3
    if m < 9:
        return y, 1
    if m < 11:
        return y, 2
    return y, 3


def previous_report_period(year: int, quarter: int) -> tuple[int, int]:
    if quarter <= 1:
        return year - 1, 4
    return year, quarter - 1


def report_date_str(year: int, quarter: int) -> str:
    """季度 -> YYYYMMDD。"""
    return f"{year}{_QUARTER_MONTH_DAY[quarter]}"


def latest_report_dates(anchor=None) -> dict:
    """返回最近财报期日期：profit/holder 单日，zcfz 双日（最近 + 上一期）。"""
    y, q = infer_report_period(anchor)
    py, pq = previous_report_period(y, q)
    latest = report_date_str(y, q)
    prev = report_date_str(py, pq)
    return {"profit": latest, "holder": latest, "zcfz": [latest, prev]}
