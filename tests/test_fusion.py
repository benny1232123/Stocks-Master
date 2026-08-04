"""融合层：按策略分散上限（防单策略占满名单）的单元测试。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from smcore.strategy.fusion import _apply_strategy_cap


def test_strategy_cap_limits_per_strategy():
    df = pd.DataFrame([{"代码": f"{i:06d}", "来源策略": "cctv"} for i in range(15)])
    out = _apply_strategy_cap(df, max_per=10)
    assert len(out) == 10


def test_strategy_cap_distributes_across_strategies():
    rows = [{"代码": f"c{i:06d}", "来源策略": "cctv"} for i in range(8)] + [
        {"代码": f"t{i:06d}", "来源策略": "theme"} for i in range(8)
    ]
    df = pd.DataFrame(rows)
    out = _apply_strategy_cap(df, max_per=5)
    assert len(out) == 10
    assert (out["来源策略"] == "cctv").sum() == 5
    assert (out["来源策略"] == "theme").sum() == 5


def test_multi_hit_owner_is_first_strategy():
    """多策略命中票按来源策略首策略归属（与仓位分配口径一致）。"""
    df = pd.DataFrame([{"代码": f"x{i:06d}", "来源策略": "theme/cctv"} for i in range(12)])
    out = _apply_strategy_cap(df, max_per=10)
    assert len(out) == 10


def test_strategy_cap_returns_unchanged_on_empty_or_missing_col():
    assert _apply_strategy_cap(pd.DataFrame(), 10).empty
    df = pd.DataFrame([{"代码": "000001"}])
    assert len(_apply_strategy_cap(df, 10)) == 1
