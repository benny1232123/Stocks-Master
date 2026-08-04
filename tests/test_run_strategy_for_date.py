"""离线单测：日期注入机制（run_strategy_for_date + boll._compute_report_dates）。

刻意不主动 import 任何会联网拉数据的策略模块；只测纯逻辑：
  - _make_frozen：把 YYYYMMDD 正确冻结成 date / datetime（早盘 09:30），含时区透传；
  - main() 对「缺参 / 未知策略 / 缺 SIGNAL_DATE / SIGNAL_DATE 格式错」返回 2，
    且这些校验路径在 import 策略模块之前就返回，故无需联网；
  - SUPPORTED 契约：boll 走原生 today 参数，不由本脚本接管；
  - boll._compute_report_dates：backfill 时用信号日推导财报期（修复点），
    确定性、不依赖运行日；boll 无法离线导入时该用例 skip。
"""
from __future__ import annotations

import datetime as _dt
import os
import sys

import pytest

import run_strategy_for_date as rsf


def test_make_frozen_date_and_datetime():
    FrozenDate, FrozenDateTime, fixed_d, fixed_dt = rsf._make_frozen("20260714")
    assert fixed_d == _dt.date(2026, 7, 14)
    assert fixed_dt == _dt.datetime(2026, 7, 14, 9, 30, 0)
    assert FrozenDate.today() == _dt.date(2026, 7, 14)
    assert FrozenDateTime.now() == _dt.datetime(2026, 7, 14, 9, 30, 0)


def test_make_frozen_respects_tz():
    _, FrozenDateTime, _, _ = rsf._make_frozen("20260101")
    tz = _dt.timezone.utc
    now = FrozenDateTime.now(tz=tz)
    assert now.tzinfo is tz
    assert (now.year, now.month, now.day) == (2026, 1, 1)


def test_make_frozen_bad_input_raises():
    # _make_frozen 不自行校验格式（交给 main 校验），但非法串在 int() 解析时抛错。
    for bad in ("", "2026", "20261301", "20260230", "abcdefgh", "2026-07-14"):
        with pytest.raises((ValueError, IndexError)):
            rsf._make_frozen(bad)


def _run_main_with(argv, sig=None):
    orig_argv = sys.argv
    orig_env = os.environ.get("SIGNAL_DATE")
    try:
        if sig is None:
            os.environ.pop("SIGNAL_DATE", None)
        else:
            os.environ["SIGNAL_DATE"] = sig
        sys.argv = ["run_strategy_for_date.py", *argv]
        return rsf.main()
    finally:
        sys.argv = orig_argv
        if orig_env is None:
            os.environ.pop("SIGNAL_DATE", None)
        else:
            os.environ["SIGNAL_DATE"] = orig_env


def test_main_missing_strategy_returns_2():
    assert _run_main_with([]) == 2


def test_main_unknown_strategy_returns_2():
    assert _run_main_with(["not_a_strategy"], sig="20260714") == 2


def test_main_missing_signal_date_returns_2():
    assert _run_main_with(["theme"]) == 2


def test_main_bad_signal_date_returns_2():
    assert _run_main_with(["theme"], sig="2026") == 2
    assert _run_main_with(["theme"], sig="notdate") == 2


def test_supported_excludes_boll():
    # boll 走原生 today 参数，不由本脚本接管（避免双重日期机制）。
    assert "boll" not in rsf.SUPPORTED
    assert set(rsf.SUPPORTED) == {"theme", "cctv", "relativity", "momentum"}


def test_compute_report_dates_backfill():
    """boll._compute_report_dates：backfill 时用信号日推导财报期，确定性、不依赖运行日。"""
    try:
        from smcore.strategies.boll import _compute_report_dates
    except Exception as e:  # 导入失败（缺 baostock/backtrader 等）则跳过，不阻塞套件
        pytest.skip(f"boll 无法离线导入，跳过：{e}")

    # 2 月（<5）→ 上一年年报 1231
    feb = _compute_report_dates("20260201")
    assert feb["report_date_profit"] == "20251231"
    # 7 月（5<=m<9）→ 当年一季报 0331
    jul = _compute_report_dates("20260714")
    assert jul["report_date_profit"] == "20260331"
    # 同信号日用 today 推导，与运行日解耦（修复点）：两个不同月必产出不同财报期
    assert feb["report_date_profit"] != jul["report_date_profit"]
