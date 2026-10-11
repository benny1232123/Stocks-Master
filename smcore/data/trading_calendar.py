#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A 股交易日历（**仅标准库**）——把「休市日」与「链路故障」区分开。

为什么需要它
------------
daily-pick 是「交易日 16:30 之后产信号」的管线。休市日（周末 / 法定节假日）数据源
按定义就没有当日 bar，于是链路必然这样走：

    verify_data_freshness.py 抽查样板股 → 无当日 bar → exit 1
    → 其后的 K 线刷新 / A 批 12 因子 / 操作清单 / 回测 全部 skip
    → 告警步骤（``if: always()``）判「无策略产出且无候选」→ 开 issue

2026 年国庆假期（10-01 ~ 10-07）因此连开 4 条误报（issue #10/#11/#14/#15），
2026 中秋（09-25）、2025 国庆（10-01 ~ 10-08）也各中过一次；同一原因还打红了
``daily-holdings.yml`` 的「持仓 K 线定向刷新 + 新鲜度复核」（2026-10-07）。

休市日按定义就没有信号可产，正确语义是**整链空转**，而不是报故障——本模块提供这份判据。

数据来源（逐条录入公告，不靠第三方包）
--------------------------------------
  · 2025：上证公告〔2024〕38 号
  · 2026：上证公告〔2025〕45 号 + 〔2026〕22 号（中秋/国庆）
  沪深北三所休市安排一致。若需核对：上交所官网「休市安排」页
  https://www.sse.com.cn/disclosure/dealinstruc/closed/

只录入公告给出的休市**区间**（含其中的周末）；判定时再把周末过滤掉。A 股从不在
周六/周日交易，连公告里「××日为周末休市」的调休上班日也不开市，故周末规则恒定可用。

失败方向（fail-open，重要）
---------------------------
未收录年份 / 表损坏 → **一律按交易日处理**，并由调用方打 ``::warning::``。理由：

  · 真实交易日被误判为休市 → 当天信号彻底丢失且**无声**（不可接受）；
  · 休市日被误判为交易日 → 退化成今天这条「误报 issue」（可见、可一键关闭）。

两害相权宁可误报。新增年份时在 ``HOLIDAY_RANGES`` 里补一段即可，并跑
``tests/test_trading_calendar.py`` 钉住。

⚠️ 年度维护提醒：交易所通常在**前一年 11~12 月**公布次年安排。届时未补录的年份会
fail-open，假期首日会再开一条误报 issue（issue 正文里已写明这个处置办法）。
"""
from __future__ import annotations

from datetime import date, timedelta

# 年 → 公告的休市区间 (起, 止, 名称)，含区间内的周末，判定时再过滤。
HOLIDAY_RANGES: dict[int, tuple[tuple[str, str, str], ...]] = {
    2025: (
        ("2025-01-01", "2025-01-01", "元旦"),
        ("2025-01-28", "2025-02-04", "春节"),
        ("2025-04-04", "2025-04-06", "清明节"),
        ("2025-05-01", "2025-05-05", "劳动节"),
        ("2025-05-31", "2025-06-02", "端午节"),
        ("2025-10-01", "2025-10-08", "国庆节、中秋节"),
    ),
    2026: (
        ("2026-01-01", "2026-01-03", "元旦"),
        ("2026-02-15", "2026-02-23", "春节"),
        ("2026-04-04", "2026-04-06", "清明节"),
        ("2026-05-01", "2026-05-05", "劳动节"),
        ("2026-06-19", "2026-06-21", "端午节"),
        ("2026-09-25", "2026-09-27", "中秋节"),
        ("2026-10-01", "2026-10-07", "国庆节"),
    ),
}


def _expand(ranges: tuple[tuple[str, str, str], ...]) -> dict[str, str]:
    """把休市区间展开成 ``{"YYYY-MM-DD": 名称}``，**只保留落在工作日**的日子（周末已恒为休市）。"""
    out: dict[str, str] = {}
    for start, end, label in ranges:
        d = date.fromisoformat(start)
        stop = date.fromisoformat(end)
        if stop < d:
            raise ValueError(f"休市区间起止颠倒: {start} ~ {end}")
        while d <= stop:
            if d.weekday() < 5:
                out[d.isoformat()] = label
            d += timedelta(days=1)
    return out


# 年 → {休市日(工作日): 名称}
CLOSED_DAYS: dict[int, dict[str, str]] = {y: _expand(r) for y, r in HOLIDAY_RANGES.items()}

COVERED_YEARS: frozenset[int] = frozenset(CLOSED_DAYS)


def is_covered(d: date) -> bool:
    """该日所属年份的休市安排是否已收录（未收录时判定一律 fail-open 成交易日）。"""
    return d.year in CLOSED_DAYS


def uncovered_notice(d: date) -> str | None:
    """未收录年份的提示文本；已收录返回 ``None``。供调用方打 ``::warning::``。

    做成「纯函数返回文本」而不是模块内部直接打印，是为了让本模块无副作用、
    可被 pytest 反复调用而不污染输出。
    """
    if is_covered(d):
        return None
    return (f"{d.year} 年 A 股休市安排尚未收录（{sorted(COVERED_YEARS)} 已收录）——"
            f"本次按「周一~周五皆交易日」fail-open 处理。若当日实为法定休市，"
            f"请把该年安排补进 smcore/data/trading_calendar.py: HOLIDAY_RANGES。")


def is_weekend(d: date) -> bool:
    """是否周末。A 股从不周末交易（调休上班的周末也不开市）。"""
    return d.weekday() >= 5


def is_trading_day(d: date) -> bool:
    """是否 A 股交易日。周末 → False；已收录年份的法定休市日 → False；其余 → True。"""
    if is_weekend(d):
        return False
    closed = CLOSED_DAYS.get(d.year)
    if closed is None:
        return True  # 未收录年份：fail-open（见模块 docstring）
    return d.isoformat() not in closed


def describe_closed(d: date) -> str:
    """休市原因（用于日志/issue）；若该日其实是交易日则返回「交易日」。"""
    if is_weekend(d):
        return "周六/周日周末休市"
    label = CLOSED_DAYS.get(d.year, {}).get(d.isoformat())
    if label:
        return f"{label}（法定休市）"
    if not is_covered(d):
        return f"{d.year} 年休市安排未收录，按交易日处理"
    return "交易日"


def prev_trading_day(d: date, *, include_self: bool = False) -> date:
    """``d`` 之前（含自身）最近的一个交易日。"""
    cur = d if include_self else d - timedelta(days=1)
    for _ in range(30):  # 最长连假 ≈ 8~10 天，30 天足够；防御性上限避免死循环
        if is_trading_day(cur):
            return cur
        cur -= timedelta(days=1)
    raise ValueError(f"{d} 前 30 天内未找到交易日（日历表可能损坏）")


def next_trading_day(d: date, *, include_self: bool = False) -> date:
    """``d`` 之后（含自身）最近的一个交易日。"""
    cur = d if include_self else d + timedelta(days=1)
    for _ in range(30):
        if is_trading_day(cur):
            return cur
        cur += timedelta(days=1)
    raise ValueError(f"{d} 后 30 天内未找到交易日（日历表可能损坏）")
