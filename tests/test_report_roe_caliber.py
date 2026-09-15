"""持仓日报 ROE 口径回归：报告脚本也必须用「年化后」ROE（与 analysis.py / 前端同构）。

背景：数据层 `roe` 是**年初至今累计**（THS / baostock `index_weighted_avg_roe`），
而报告脚本 `_fund_flags` 的判读阈值（0.12 / 0.08，盈利优/中/弱）与评分配置
（0.10 / 0.15 / 0.20）都是按**年度** ROE 设的 → 不年化则同一只票的报告会随财报日历
「忽优忽弱」（Q1 判「盈利弱」、年报判「盈利优」）。

本次把报告脚本所有 ROE 取用点收敛到 `_roe_val()`（= `annualize_roe(roe, roe_period)`）。
旧 v1 扁平缓存无 `roe_period`（其 roe 本就是年度值）→ 原样返回，行为不变。
"""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "notify_holdings_analysis", _ROOT / "scripts" / "notify_holdings_analysis.py")
mod = importlib.util.module_from_spec(_spec)
# 该脚本 import 时会 load_dotenv()（读仓库根 .env）→ 先快照再还原，避免污染同进程其他用例
_ENV = dict(os.environ)
try:
    _spec.loader.exec_module(mod)
finally:
    os.environ.clear()
    os.environ.update(_ENV)


def _flag_texts(fund: dict) -> str:
    return " ".join(t for t, _ in mod._fund_flags(fund))


def test_roe_val_annualizes_when_period_present():
    """002284 2026H1 累计 7.7% → 年化 15.4%（≈2025 年报 15.66%）。"""
    assert mod._roe_val({"roe": 0.077, "roe_period": "2026-06-30"}) == 0.154


def test_roe_val_passthrough_for_legacy_flat_cache():
    """旧 v1 扁平缓存无 roe_period → 原样返回（不猜），缺失 → None。"""
    assert mod._roe_val({"roe": 0.156261}) == 0.156261
    assert mod._roe_val({}) is None


def test_fund_flags_roe_uses_annualized_value():
    """Q2 累计 5.0% 年化 10.0% 应判「盈利中」，而不是拿 5.0% 判「盈利弱」。"""
    txt = _flag_texts({"roe": 0.05, "roe_period": "2026-06-30"})
    assert "盈利中" in txt and "10.0%" in txt
    assert "盈利弱" not in txt
    # 反证：同一 5.0% 若被错当年报值，就会落到「盈利弱」
    assert "盈利弱" in _flag_texts({"roe": 0.05, "roe_period": "2026-12-31"})


def test_fund_flags_legacy_flat_cache_unchanged():
    """旧缓存（年度 ROE 15.6%）→ 仍判「盈利优」，不被误年化。"""
    txt = _flag_texts({"roe": 0.156261})
    assert "盈利优" in txt and "15.6%" in txt


def test_fund_panel_md_roe_is_annualized():
    """Markdown 面板里的 ROE 也必须显示年化值（与 flags / 评分同口径）。"""
    lines = mod._fund_panel_md({"roe": 0.05, "roe_period": "2026-06-30"})
    joined = "\n".join(lines)
    assert "ROE=10.0%" in joined
