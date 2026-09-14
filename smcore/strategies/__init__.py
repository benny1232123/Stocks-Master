"""策略实现集 —— 单策略选股逻辑（Boll / 题材 / 相对强弱 / CCTV / 动量 等）。

此前各策略散落在 Frequently-Used-Program/*.py 巨石脚本里。现已将其中经过实战验证的
策略重构为可复用模块，供 daily-pick 工作流、后端 /api/selection/* 与本地编排统一调用：
  - run_boll       (auto-boll 多因子：资金流 + 基本面 + 重要股东 + 布林)
  - run_theme      (A股短线题材：政策题材 + 动量，换手率可放宽)
  - run_relativity (A股相对强弱：资金流 + 基本面 + 股东 + 指数相对强弱)
  - run_cctv       (CCTV 新闻舆论热门板块监测)
  - run_momentum   (轻量动量/相对强度：买中期上升趋势的强势股，与买超卖的 Boll 互补)

⚠️ 内存放大已修复（2026-09-14）：本 __init__ 此前「聚合导入」全部 5 个策略。
但 Python 导入任意子模块前必先执行包 __init__ —— 于是只想用 Boll 的
`from smcore.strategies.boll import run_boll`，也被迫连带加载
cctv(jieba+akshare) / momentum / relativity / theme，实测一次性拖入
akshare + jieba + bs4 + lxml + baostock（约 41MB 常驻 RSS）。
云端 512MB 免费实例上这属于纯浪费（且这些策略在精简模式下根本不可达）。

现改为 PEP 562 惰性 `__getattr__`：仅当真正以聚合形式取值
（如 `from smcore.strategies import run_cctv`）时才导入对应子模块，各子模块彼此独立。
项目内全部调用方均走子模块路径（`from smcore.strategies.boll import run_boll`），
故本改动对其行为与耗时无任何影响，仅消除「导一个拉全部」的副作用。
"""
from __future__ import annotations

import importlib

# 聚合导出名 -> 所属子模块（惰性映射，不在此处 import）
_STRATEGY_EXPORTS = {
    "run_boll": ".boll",
    "run_cctv": ".cctv",
    "run_momentum": ".momentum",
    "run_relativity": ".relativity",
    "run_theme": ".theme",
}

__all__ = list(_STRATEGY_EXPORTS)


def __getattr__(name: str):
    """PEP 562 模块级惰性属性：按需导入单个策略子模块。"""
    mod_path = _STRATEGY_EXPORTS.get(name)
    if mod_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(mod_path, __name__), name)
    globals()[name] = value  # 缓存，后续访问直接命中模块字典
    return value


def __dir__() -> list[str]:
    return sorted(__all__)
