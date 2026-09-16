"""因子引擎（smcore.strategy.factor_engine）纯函数测试。

守护：坏柱标记、坏柱回看/前向窗、基础有效域、前向收益、横截面 IC、
分位聚合——这些都是 v1 预注册回放与新挖掘脚本共用的原语，错一个两边一起错。
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pytest  # noqa: E402

from smcore.strategy import factor_engine as fe  # noqa: E402


def _mats(n_days=200, codes=("000001", "000002", "600000", "300001")):
    """确定性合成行情：每只票以不同斜率线性上涨，无缺口。"""
    idx = pd.date_range("2020-01-01", periods=n_days, freq="D")
    slope = {c: 0.001 * (i + 1) for i, c in enumerate(codes)}
    close = pd.DataFrame({c: 10.0 * (1 + slope[c]) ** np.arange(n_days) for c in codes}, index=idx)
    high = close * 1.01
    low = close * 0.99
    amount = pd.DataFrame({c: np.full(n_days, 1e8) for c in codes}, index=idx)
    return close, high, low, amount


def test_limit_series_by_board():
    s = fe.limit_series(["000001", "600000", "300123", "688001", "830001"])
    assert list(s.values) == [0.10, 0.10, 0.20, 0.20, 0.30]


def test_bad_bar_detection_flags_limit_break():
    close, _, _, _ = _mats()
    d = close.index
    # 000001 限 10%：+5% 不算坏柱，+30% 算
    # （放在最后两行，避免扰动后续收益产生额外坏柱）
    close.loc[d[-2], "000001"] = close["000001"].iloc[-3] * 1.05
    ret1, bad = fe.daily_returns_and_bad(close)
    assert bad.loc[d[-2], "000001"] == 0
    close.loc[d[-1], "000001"] = close["000001"].iloc[-2] * 1.30
    ret1, bad = fe.daily_returns_and_bad(close)
    assert bad.loc[d[-1], "000001"] == 1
    assert int(bad.values.sum()) == 1


def test_lookback_and_forward_bad_windows():
    close, _, _, _ = _mats()
    ret1, bad = fe.daily_returns_and_bad(close)
    d = close.index
    bad = pd.DataFrame(0.0, index=d, columns=close.columns)
    bad.loc[d[5], "000001"] = 1.0
    # 回看窗 3：覆盖 d5~d7
    lb = fe.lookback_bad(bad, 3)
    assert lb.loc[d[5], "000001"] == 1
    assert lb.loc[d[7], "000001"] == 1
    assert lb.loc[d[8], "000001"] == 0
    # 前向窗 2 = {t, t+1, t+2}（**含当日**，与 v1 一致）
    fb = fe.forward_bad_mask(bad, 2)
    assert fb.loc[d[2], "000001"] == 0
    assert fb.loc[d[3], "000001"] == 1
    assert fb.loc[d[5], "000001"] == 1
    assert fb.loc[d[6], "000001"] == 0


def test_lookback_bad_cache_identity():
    close, _, _, _ = _mats()
    _, bad = fe.daily_returns_and_bad(close)
    cache: dict = {}
    a = fe.lookback_bad(bad, 20, cache)
    b = fe.lookback_bad(bad, 20, cache)
    assert a is b and 20 in cache


def test_base_valid_requires_warmup():
    close, _, _, _ = _mats(n_days=200)
    bv = fe.base_valid_mask(close)
    assert not bool(bv.iloc[0].any())
    assert bool(bv.iloc[fe.WARMUP_MIN - 1].all())   # 第 60 行起通过
    assert bool(bv.iloc[-1].all())


def test_base_valid_excludes_low_price():
    close, _, _, _ = _mats(n_days=200)
    close.loc[close.index[-1], "000001"] = 1.0    # 低于 PRICE_FLOOR=2
    bv = fe.base_valid_mask(close)
    assert not bool(bv.loc[close.index[-1], "000001"])


def test_forward_return_value_and_bad_mask():
    close, _, _, _ = _mats(n_days=200)
    _, bad = fe.daily_returns_and_bad(close)
    bv = fe.base_valid_mask(close)
    fb = fe.forward_bad_mask(bad, 10)
    fwd = fe.forward_return_matrix(close, fb, bv, 10)
    d = close.index
    expected = close["000001"].iloc[150 + 10] / close["000001"].iloc[150] - 1
    assert fwd.loc[d[150], "000001"] == pytest.approx(expected)
    # 最后 10 天没有 T+10 → NaN
    assert np.isnan(fwd.iloc[-1]["000001"])


def test_cross_sectional_ic_perfect_and_inverse():
    close, high, low, amount = _mats(n_days=100, codes=("000001", "000002", "600000", "300001"))
    ret1, _ = fe.daily_returns_and_bad(close)
    bv = fe.base_valid_mask(close)
    # 构造 fwd：与 close 同向（每只票一条独立序列）→ 用 close 当 fwd
    fwd = close.copy()
    fr = fe.forward_rank_matrix(fwd)
    ic = fe.cross_sectional_ic(close, fr, min_n_day=3)
    assert ic.dropna().iloc[-1] == pytest.approx(1.0)
    ic_inv = fe.cross_sectional_ic(1.0 / close, fr, min_n_day=3)
    assert ic_inv.dropna().iloc[-1] == pytest.approx(-1.0)


def test_cross_sectional_ic_min_n_filters():
    close, _, _, _ = _mats(n_days=100)
    fr = fe.forward_rank_matrix(close)
    ic = fe.cross_sectional_ic(close, fr, min_n_day=10)   # 只有 4 列 → 全被过滤
    assert ic.dropna().empty


def test_decile_means_monotonic():
    close, _, _, _ = _mats(n_days=150)
    bv = fe.base_valid_mask(close)
    d = close.index[-20]
    fac = pd.DataFrame({c: close.loc[d, c] * (i + 1) for i, c in enumerate(close.columns)},
                       index=[d])
    fwd = pd.DataFrame({c: close.loc[d, c] * (i + 1) for i, c in enumerate(close.columns)},
                       index=[d])
    dec, used = fe.decile_means(fac, fwd, bv, [d], n_decile=4, min_n_day=4)
    assert used == 1
    assert all(dec[i] < dec[i + 1] for i in range(3))


def test_decile_means_skips_insufficient_days():
    close, _, _, _ = _mats(n_days=150)
    bv = fe.base_valid_mask(close)
    d = close.index[-20]
    fac = close.loc[[d]]
    fwd = close.loc[[d]]
    dec, used = fe.decile_means(fac, fwd, bv, [d], n_decile=10, min_n_day=10)  # 仅 4 只
    assert used == 0
    assert all(np.isnan(v) for v in dec)
