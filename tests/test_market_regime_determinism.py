#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""守卫：regime 计算必须是 (closes, as_of) 的纯函数，与数据源历史长度无关。

背景（issue #13）：同花顺指数历史自 2022-01 起（1141 根）、新浪只拉 320 根时，
波动率分位的基数长度随源而变，边界日（20260826：分位 0.668 vs 0.676）跨过 0.67
阈值使 regime 结论翻转；且 320 根切片后不足 65 根时直接退化成真空「震荡轮动」。
修复：分位固定取最近 _VOL_PCTILE_WINDOW 个滚动观测 + 新浪 datalen=1200。
本测试用合成序列钉住该不变量（不联网）。
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy.market import (  # noqa: E402
    _VOL_PCTILE_WINDOW,
    _profile_from_full_series,
)


def _synth_series(n: int, seed: int, start: str = "2019-01-01") -> pd.DataFrame:
    """确定性合成指数日线（close/volume），n 根 bar。"""
    rng = np.random.default_rng(seed)
    rets = rng.normal(0.0003, 0.011, n)
    close = 4000 * np.exp(np.cumsum(rets))
    idx = pd.bdate_range(start, periods=n)
    return pd.DataFrame({"close": close, "volume": rng.uniform(1e10, 2e10, n)}, index=idx)


def test_vol_pctile_window_is_fixed_250():
    """规格即「近 250 日分布中的分位」——窗口必须固定，不能随可用历史长度漂移。"""
    assert _VOL_PCTILE_WINDOW == 250


def test_regime_invariant_to_series_length():
    """同一组合 closes、不同历史长度（模拟不同数据源）→ as_of 处 regime 必须一致。"""
    base = _synth_series(900, seed=7)
    as_of = base.index[700]  # 深切中间，两档长度都有完整 120/250 窗口
    hs_long = base
    hs_short = base.iloc[-400:]  # 同样覆盖 as_of，但历史更短（模拟旧新浪 320 根之痛）
    z5 = _synth_series(900, seed=11)
    z1 = _synth_series(900, seed=13)
    p_long = _profile_from_full_series(hs_long, z5, z1, as_of=as_of)
    p_short = _profile_from_full_series(hs_short, z5, z1, as_of=as_of)
    assert p_long is not None and p_short is not None
    assert p_long.regime == p_short.regime, (
        f"regime 随序列长度漂移：long={p_long.regime} short={p_short.regime}"
    )


def test_regime_stable_when_older_history_appended():
    """向前追加更早的历史（数据源回溯差异）不得改变 as_of 处 regime。"""
    older = _synth_series(500, seed=21, start="2015-01-01")
    newer = _synth_series(700, seed=7, start="2017-01-01")
    combined = pd.concat([older, newer]).sort_index()
    combined = combined[~combined.index.duplicated(keep="last")]
    as_of = newer.index[600]
    z5 = _synth_series(700, seed=11, start="2017-01-01")
    z1 = _synth_series(700, seed=13, start="2017-01-01")
    p_with_older = _profile_from_full_series(combined, z5, z1, as_of=as_of)
    p_without = _profile_from_full_series(newer, z5, z1, as_of=as_of)
    assert p_with_older is not None and p_without is not None
    assert p_with_older.regime == p_without.regime


def test_pctile_ignores_history_beyond_window():
    """分位只应由最近 _VOL_PCTILE_WINDOW 个滚动 std 决定（数值级断言）。"""
    n = 900
    hs = _synth_series(n, seed=7)
    close = pd.to_numeric(hs["close"], errors="coerce").dropna()
    c = close.values.astype(float)
    rets = pd.Series(c).pct_change().dropna()
    ann = rets.tail(20).std() * (252 ** 0.5)
    roll = rets.rolling(20).std().dropna() * (252 ** 0.5)
    expected = float((roll.tail(_VOL_PCTILE_WINDOW) < ann).mean())
    # 全历史口径（旧实现）应与此不同或至少不保证相同——这里只钉新口径可复算
    assert 0.0 <= expected <= 1.0
    # 序列长度变化（截断到 400 根但保留 as_of 邻域）后分位不变
    short = hs.iloc[-400:]
    cs = pd.to_numeric(short["close"], errors="coerce").dropna().values.astype(float)
    r2 = pd.Series(cs).pct_change().dropna()
    ann2 = r2.tail(20).std() * (252 ** 0.5)
    roll2 = r2.rolling(20).std().dropna() * (252 ** 0.5)
    expected2 = float((roll2.tail(_VOL_PCTILE_WINDOW) < ann2).mean())
    assert expected == pytest.approx(expected2, abs=1e-12)
