"""数值与文本格式化辅助 —— 全项目统一实现。

从 auto_notify_boll.py 巨石抽出。此前这些小函数散落巨石内，无法被可视化主线复用。
均为纯函数，无外部依赖。
"""
from __future__ import annotations

from typing import Optional


def to_float(value) -> Optional[float]:
    """安全转 float，失败返回 None。"""
    try:
        return float(value)
    except Exception:
        return None


def normalize_confidence_label(raw_value) -> str:
    """置信度标签归一化：高/中/低（中文）。"""
    text = str(raw_value or "").strip().lower()
    if text in {"高", "high", "h"}:
        return "高"
    if text in {"中", "medium", "mid", "m"}:
        return "中"
    if text in {"低", "low", "l"}:
        return "低"
    return "中"


def format_yi(value) -> str:
    """金额格式化为"亿"单位字符串。"""
    num = to_float(value)
    if num is None:
        return "N/A"
    return f"{num / 1e8:.1f}亿"


def safe_pct(numerator, denominator) -> Optional[float]:
    """计算百分比变化 (numerator/denominator - 1)*100，分母为 0/None 返回 None。"""
    if denominator in (None, 0):
        return None
    return (numerator / denominator - 1.0) * 100.0


def to_percent_like(value) -> Optional[float]:
    """智能转百分比：<=1.5 视为小数（×100），否则视为已是百分比。"""
    num = to_float(value)
    if num is None:
        return None
    if num <= 1.5:
        return num * 100.0
    return num


def fmt_pct(value, digits: int = 2, signed: bool = False, na: str = "N/A") -> str:
    """百分比格式化。"""
    num = to_float(value)
    if num is None:
        return na
    sign = "+" if signed else ""
    return f"{num:{sign}.{digits}f}%"


def fmt_num(value, digits: int = 2, na: str = "N/A") -> str:
    """普通数值格式化。"""
    num = to_float(value)
    if num is None:
        return na
    return f"{num:.{digits}f}"
