"""交易日历守卫（2026-10-11）——钉住「休市日不得被当成交易日跑」。

背景（事故记录）
----------------
daily-pick 是「交易日 16:30 后产信号」的管线。2026 国庆假期（10-01 ~ 10-07）整周仍是
工作日、schedule 照常触发，链路走到数据新鲜度门控时样板股没有当日 bar → 硬失败 →
后续步骤全 skip → 告警步骤（``if: always()``）判定「无策略产出且无候选」→ 连开 4 条
误报 issue（#10/#11/#14/#15）。2026 中秋 09-25、2025 国庆 10-01 ~ 10-08 同样中招。

本测试钉住修复后的不变式：
  ① 官方公告的休市日（2025 / 2026）逐日判定为非交易日；
  ② 公告里「起照常开市」的复位日必须是交易日（防止把区间多算一天）；
  ③ 周末恒为非交易日（A 股从不周末交易，调休上班的周末也不开市）；
  ④ 未收录年份 **fail-open**（按交易日处理）并给出 notice —— 绝不静默跳过真实交易日；
  ⑤ 日历表与真实 k_data 交叉验证：凡被本表标为非交易日的工作日，缓存里必然没有 bar。

再次分叉（例如有人手滑把某天从 HOLIDAY_RANGES 删掉）时这里应立刻变红。
"""
from datetime import date, timedelta
from pathlib import Path

import pytest

from smcore.data.trading_calendar import (
    CLOSED_DAYS,
    COVERED_YEARS,
    describe_closed,
    is_covered,
    is_trading_day,
    is_weekend,
    next_trading_day,
    prev_trading_day,
    uncovered_notice,
)

ROOT = Path(__file__).resolve().parents[1]


def _d(s: str) -> date:
    return date.fromisoformat(s)


# ── ① 公告休市日（工作日）必须判为非交易日 ────────────────────────────────────
_ANNOUNCED_CLOSED_2026 = [
    "2026-01-01", "2026-01-02",                                        # 元旦
    "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19", "2026-02-20", "2026-02-23",  # 春节
    "2026-04-06",                                                      # 清明
    "2026-05-01", "2026-05-04", "2026-05-05",                          # 劳动节
    "2026-06-19",                                                      # 端午
    "2026-09-25",                                                      # 中秋
    "2026-10-01", "2026-10-02", "2026-10-05", "2026-10-06", "2026-10-07",  # 国庆
]

_ANNOUNCED_OPEN_2026 = [
    "2026-01-05",   # 「1月5日(周一)起照常开市」
    "2026-02-24",   # 「2月24日(周二)起照常开市」
    "2026-04-07",   # 「4月7日(周二)起照常开市」
    "2026-05-06",   # 「5月6日(周三)起照常开市」
    "2026-06-22",   # 「6月22日(周一)起照常开市」
    "2026-09-28",   # 「9月28日(周一)起照常开市」（k_data 里有当日 bar，确实开市）
    "2026-10-08",   # 「10月8日(周四)起照常开市」
]


@pytest.mark.parametrize("day", _ANNOUNCED_CLOSED_2026)
def test_2026_announced_holidays_are_closed(day):
    assert _d(day).weekday() < 5, f"{day} 不是工作日，不该出现在本断言里"
    assert is_trading_day(_d(day)) is False


@pytest.mark.parametrize("day", _ANNOUNCED_OPEN_2026)
def test_2026_announced_reopen_days_are_trading(day):
    assert is_trading_day(_d(day)) is True, f"{day} 是公告的复市日，必须判为交易日"


# ── ② 2025 同口径 ────────────────────────────────────────────────────────────
_ANNOUNCED_CLOSED_2025 = [
    "2025-01-01",
    "2025-01-28", "2025-01-29", "2025-01-30", "2025-01-31", "2025-02-03", "2025-02-04",
    "2025-04-04",
    "2025-05-01", "2025-05-02", "2025-05-05",
    "2025-06-02",
    "2025-10-01", "2025-10-02", "2025-10-03", "2025-10-06", "2025-10-07", "2025-10-08",
]


@pytest.mark.parametrize("day", _ANNOUNCED_CLOSED_2025)
def test_2025_announced_holidays_are_closed(day):
    assert is_trading_day(_d(day)) is False


@pytest.mark.parametrize("day", ["2025-02-05", "2025-04-07", "2025-05-06", "2025-06-03", "2025-10-09"])
def test_2025_reopen_days_are_trading(day):
    assert is_trading_day(_d(day)) is True


# ── ③ 周末恒休市 ─────────────────────────────────────────────────────────────
def test_weekends_are_never_trading():
    d = date(2025, 1, 1)
    while d.year <= 2027:
        if d.weekday() >= 5:
            assert is_weekend(d) and not is_trading_day(d)
        d += timedelta(days=1)


# ── ④ 未收录年份 fail-open ──────────────────────────────────────────────────
def test_uncovered_year_fails_open():
    future = date(2027, 1, 4)  # 周一，且 2027 未收录
    assert not is_covered(future)
    assert is_trading_day(future) is True, "未收录年份必须 fail-open 成交易日，绝不静默跳过"
    assert uncovered_notice(future) and "2027" in uncovered_notice(future)
    # 周末仍然挡掉（周末规则不依赖年份表）
    assert is_trading_day(date(2027, 1, 2)) is False


def test_covered_years_and_notice_purity():
    assert COVERED_YEARS == frozenset({2025, 2026})
    covered_day = date(2026, 10, 7)
    assert is_covered(covered_day) and uncovered_notice(covered_day) is None


# ── ⑤ 与真实 k_data 交叉验证：本表标为非交易日的工作日，缓存里必然无 bar ─────────
def test_table_agrees_with_real_kline_cache():
    """数据的「缺 bar 工作日」必须是本表的子集（反向不成立：数据自身也可能有缺口）。"""
    parquet = ROOT / "stock_data" / "k_data" / "qfq_b00.parquet"
    if not parquet.exists():
        pytest.skip("本地无 k_data 缓存（CI 不跟踪该文件，属预期跳过）")
    import pandas as pd

    dates = set(pd.to_datetime(pd.read_parquet(parquet, columns=["date"])["date"]).dt.date)
    if not dates:
        pytest.skip("k_data 缓存为空")
    lo, hi = min(dates), max(dates)
    checked = 0
    d = lo
    while d <= hi:
        if d.weekday() < 5 and d not in dates and is_covered(d):
            assert not is_trading_day(d), f"{d} 在真实缓存里没有 bar，本表却判为交易日"
            checked += 1
        d += timedelta(days=1)
    assert checked > 0, "交叉验证没覆盖任何休市日，说明缓存范围或日历表有问题"


# ── 辅助函数 ─────────────────────────────────────────────────────────────────
def test_describe_and_neighbors():
    assert "国庆节" in describe_closed(_d("2026-10-07"))
    assert "周末" in describe_closed(_d("2026-10-03"))
    assert describe_closed(_d("2026-10-08")) == "交易日"
    assert prev_trading_day(_d("2026-10-08")) == _d("2026-09-30"), "国庆前最后一个交易日是 09-30"
    assert next_trading_day(_d("2026-09-30")) == _d("2026-10-08")
    # 连假跨年的边界：2025-12-31 的下一交易日是 2026-01-05（元旦 + 周末）
    assert next_trading_day(_d("2025-12-31")) == _d("2026-01-05")


def test_closed_days_table_only_weekdays():
    for year, days in CLOSED_DAYS.items():
        for iso in days:
            assert date.fromisoformat(iso).weekday() < 5, f"{iso} 是周末，不该进 CLOSED_DAYS"
            assert date.fromisoformat(iso).year == year
