"""指标层 —— re-export smcore 内核，消除第二套 Boll 实现。

本文件原含 Boll 自实现（与 Stock-Selection-Boll.py、auto_notify_boll._calc_boll_levels
共 3 套分叉），现已统一到 smcore.indicators.boll（全项目唯一实现）。
"""
from smcore.indicators.boll import calc_bollinger, evaluate_boll_signal

__all__ = ["calc_bollinger", "evaluate_boll_signal"]
