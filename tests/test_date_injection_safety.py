"""日期注入安全契约：monkeypatch 只冻「日期语义」，「计时语义」(time)必须保持真实。

这是 Round 9 补的关键不变量测试。scripts/run_strategy_for_date 通过替换
datetime.datetime / datetime.date 的 .now()/.today() 来实现历史 backfill，
但绝不可影响 time.time()（限流退避、耗时统计等计时语义必须读真实墙钟）。
"""
from __future__ import annotations

import datetime as _dt
import time
from contextlib import contextmanager

import pytest

from scripts.run_strategy_for_date import _make_frozen


@contextmanager
def _freeze(yyyymmdd: str):
    FrozenDate, FrozenDateTime, fixed_d, fixed_dt = _make_frozen(yyyymmdd)
    orig_dt, orig_date = _dt.datetime, _dt.date
    _dt.datetime, _dt.date = FrozenDateTime, FrozenDate
    try:
        yield fixed_d, fixed_dt
    finally:
        _dt.datetime, _dt.date = orig_dt, orig_date


def test_frozen_datetime_now_returns_signal_date():
    with _freeze("20260714") as (fixed_d, fixed_dt):
        assert _dt.datetime.now() == fixed_dt
        assert _dt.datetime.now().date() == fixed_d


def test_frozen_date_today_returns_signal_date():
    with _freeze("20260714") as (fixed_d, _):
        assert _dt.date.today() == fixed_d


def test_time_time_stays_real_under_freeze():
    # 关键安全契约：冻结到 2020-01-01，time.time() 仍须返回真实墙钟
    with _freeze("20200101"):
        before = time.time()
        time.sleep(0.01)
        after = time.time()
        assert after >= before                       # 仍在推进
        assert 0 <= (after - before) < 5             # 真实、正常流逝的计时
        assert _dt.datetime.now().year == 2020       # 日期确实被冻结
        assert time.time() > 1.7e9                   # 墙钟是当下，而非 2020
