"""策略实现集 —— 单策略选股逻辑（Boll / 题材 / 相对强弱 / CCTV / 动量 等）。

此前各策略散落在 Frequently-Used-Program/*.py 巨石脚本里。现已将其中经过实战验证的
策略重构为可复用模块，供 daily-pick 工作流、后端 /api/selection/* 与本地编排统一调用：
  - run_boll       (auto-boll 多因子：资金流 + 基本面 + 重要股东 + 布林)
  - run_theme      (A股短线题材：政策题材 + 动量，换手率可放宽)
  - run_relativity (A股相对强弱：资金流 + 基本面 + 股东 + 指数相对强弱)
  - run_cctv       (CCTV 新闻舆论热门板块监测)
  - run_momentum   (轻量动量/相对强度：买中期上升趋势的强势股，与买超卖的 Boll 互补)
"""
from __future__ import annotations

from .boll import run_boll
from .cctv import run_cctv
from .momentum import run_momentum
from .relativity import run_relativity
from .theme import run_theme

__all__ = ["run_boll", "run_cctv", "run_momentum", "run_relativity", "run_theme"]
