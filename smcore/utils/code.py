"""股票代码标准化 —— 全项目唯一实现。

此前有 4 处独立的代码格式化函数（strategy_common / data_fetcher /
Stock-Selection-Boll / auto_notify_boll），行为略有差异。本模块统一为单一入口。
"""
from __future__ import annotations

import pandas as pd


def format_stock_code(code) -> str:
    """任意输入归一化为 6 位数字字符串；无数字时返回空串。"""
    digits = "".join(ch for ch in str(code) if ch.isdigit())
    return digits.zfill(6) if digits else ""


def normalize_code_series(series: pd.Series) -> pd.Series:
    """对 DataFrame 列做代码标准化。"""
    return series.astype(str).map(format_stock_code)


def to_baostock_code(code) -> str:
    """baostock 格式：sh.600519 / sz.000001。"""
    code6 = format_stock_code(code)
    if not code6:
        return ""
    return f"sh.{code6}" if code6.startswith("6") else f"sz.{code6}"


def to_ak_symbol(code) -> str:
    """akshare 个股格式：SH600519 / SZ000001（大写前缀）。"""
    code6 = format_stock_code(code)
    if not code6:
        return ""
    return f"SH{code6}" if code6.startswith("6") else f"SZ{code6}"


def to_ak_index_symbol(code) -> str:
    """akshare 指数格式：sh000001 / sz399001（小写前缀）。"""
    text = str(code or "").strip().lower().replace(".", "")
    if text.startswith(("sh", "sz")):
        return text
    if text.isdigit() and len(text) == 6:
        return ("sh" + text) if text.startswith("0") else ("sz" + text)
    return text
