#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A 股交易日闸门（CI gate 用；**仅标准库**，可在装依赖之前运行）。

用法::

    python3 scripts/check_trading_day.py --date 20261007

退出码（判据即退出码，CI 直接看它）::

    0    交易日       → 正常跑
    10   非交易日     → 整链空转，不告警（周末 / 法定休市）
    2    参数非法
    1    内部异常     → **按交易日继续**（fail-open，见下）

为什么用退出码而不是 stdout 字符串
-----------------------------------
stdout 上任何一行意外输出（数据源探测日志、库的 warning）都会污染字符串判据，而退出码
不受噪声影响；且 bash 里 ``cmd || RC=$?`` 与集合判断都是退出码的一等公民。

fail-open（内部异常 → 1 → 按交易日继续）
----------------------------------------
宁可退回「休市日误报一条 issue」这种可见、可关闭的旧行为，也绝不因为日历脚本自身出错
而**静默跳过真实交易日**——那会让当天信号彻底消失，且没有任何信号提示。

刻意不 ``import smcore``
-----------------------
``smcore.data.__init__`` 会拉起 pandas 等重依赖，而本脚本要在 ``pip install`` **之前**
的 gate 步骤里跑（runner 上只有系统 python3）。故按文件路径加载
``smcore/data/trading_calendar.py``，绕开包 __init__ 的副作用。
"""
from __future__ import annotations

import argparse
import importlib.util
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_CALENDAR_PATH = ROOT / "smcore" / "data" / "trading_calendar.py"

# 退出码约定（见模块 docstring）
RC_TRADING = 0
RC_INTERNAL_ERROR = 1
RC_BAD_ARGS = 2
RC_CLOSED = 10


def _load_calendar():
    """按路径加载交易日历模块，避免触发 ``smcore.data.__init__`` 的依赖链。"""
    spec = importlib.util.spec_from_file_location("_sm_trading_calendar", _CALENDAR_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载交易日历模块: {_CALENDAR_PATH}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _parse_date(text: str):
    text = (text or "").strip()
    if not text:
        return datetime.now().date()
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"无法解析日期 {text!r}（支持 YYYYMMDD / YYYY-MM-DD）")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="A 股交易日闸门（退出码 0=交易日 / 10=休市）")
    ap.add_argument("--date", default="", help="待判定日期 YYYYMMDD（默认 = 今天）")
    ap.add_argument("--quiet", action="store_true", help="只输出一行结论，不打解释性日志")
    args = ap.parse_args(argv)

    try:
        day = _parse_date(args.date)
    except ValueError as exc:
        print(f"::error::[trading-day] {exc}")
        return RC_BAD_ARGS

    try:
        cal = _load_calendar()
        notice = cal.uncovered_notice(day)
        if notice:
            print(f"::warning::[trading-day] {notice}")
        trading = bool(cal.is_trading_day(day))
        reason = "交易日" if trading else f"非交易日 —— {cal.describe_closed(day)}"
        if not args.quiet:
            print(f"[trading-day] {day} → {reason}")
            if trading:
                print(f"[trading-day] 上一交易日 {cal.prev_trading_day(day)}，"
                      f"下一交易日 {cal.next_trading_day(day)}")
        return RC_TRADING if trading else RC_CLOSED
    except Exception as exc:  # noqa: BLE001 —— fail-open：绝不因日历自身出错而跳过真实交易日
        print(f"::warning::[trading-day] 交易日闸门异常（{type(exc).__name__}: {exc}）——"
              f"按**交易日**继续（fail-open）；若因此产生误报 issue，请检查 "
              f"smcore/data/trading_calendar.py")
        return RC_INTERNAL_ERROR


if __name__ == "__main__":
    sys.exit(main())
