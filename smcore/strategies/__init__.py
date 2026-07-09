"""策略实现集 —— 单策略选股逻辑（Boll / 题材 / 相对强弱 / CCTV 等）。

此前各策略散落在 Frequently-Used-Program/*.py 巨石脚本里。现将其中经过实战验证的
"auto-boll" 多因子选股（资金流 + 基本面 + 重要股东 + 布林）重构为可复用模块，
供 daily-pick 工作流与后端 /api/selection/boll-scan 统一调用。
"""
from __future__ import annotations

from .boll_selection import run_boll_selection

__all__ = ["run_boll_selection"]
