# -*- coding: utf-8 -*-
"""导入冒烟：生产入口链必须能被 import。

存在理由（2026-09-17 回归）：680c7c3 把 `_LEGACY_WEIGHTS` 写成了 `MultiStrategy` 的**类属性**，
又在同类的**推导式**里引用它 —— 推导式有独立作用域，看不到类体命名空间 → `NameError` →
`smcore.backtest` 整条导入链断掉。而 `.github/workflows/daily-pick.yml` 里那一步是
`python scripts/daily_backtest.py || true`，崩溃被 `|| true` 吞掉，CI 依然全绿，
**回测面板静默停更**，直到跑全量 pytest 时 5 个用例无法收集才暴露。

所以这里不做「单元」测试，只做最便宜的一件事：把生产入口模块真的 import 一遍。
新增依赖/改类体作用域时，本文件会先红灯。
"""
from __future__ import annotations

import importlib

import pytest

# 生产入口链（CI 步骤直接跑到的模块 + 其必需的公共库）
ENTRY_MODULES = [
    "smcore.backtest",                 # scripts/daily_backtest.py 的第一跳
    "smcore.backtest.engine",
    "smcore.backtest.strategies",
    "smcore.strategy.fusion",
    "smcore.strategy.factor_types",
    "smcore.data.kline",
    "smcore.data.hithink",
    "smcore.data.hithink_special",
]


@pytest.mark.parametrize("modname", ENTRY_MODULES)
def test_entry_module_imports(modname):
    importlib.import_module(modname)


def test_daily_backtest_entry_imports():
    """CI 真正执行的那条命令的入口（conftest 已把 scripts/ 注入 sys.path）。"""
    importlib.import_module("daily_backtest")


def test_multi_strategy_default_weights_sane():
    """DEFAULT_WEIGHTS 必须真派生出来：覆盖全部遗留策略名、合计 1.0。"""
    from smcore.backtest.strategies import MultiStrategy
    from smcore.strategy.factor_types import RETIRED_STRATEGY_NAMES

    dw = MultiStrategy.DEFAULT_WEIGHTS
    assert set(dw) == set(RETIRED_STRATEGY_NAMES), "默认权重必须与遗留策略名同集合"
    assert abs(sum(dw.values()) - 1.0) < 1e-9, f"默认权重合计 {sum(dw.values())} ≠ 1.0"
    # 有历史权重的四个保持原值；未列出的（momentum）必须为 0
    assert dw["boll"] == 0.40 and dw["relativity"] == 0.25
    assert dw["theme"] == 0.20 and dw["cctv"] == 0.15
    assert dw.get("momentum", 0.0) == 0.0
