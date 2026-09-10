"""基本面 Point-in-Time 选期逻辑单元测试（不依赖联网 / baostock）。

验证：
1. _select_pit_period 在 as_of 前选「已披露」的最新报告期（真实公告日优先，缺则法定大限）。
2. _extract_for_asof 对 v2 缓存：实时(as_of=None)取最新期；历史回补按 PIT 选期；
   估值快照仅在 as_of ≥ 刷新日时可用（避免未来估值泄漏）。
3. 旧扁平缓存向后兼容：实时可用、历史回补降级为 None（杜绝未来函数）。
"""
from __future__ import annotations

from datetime import date

from smcore.strategy import fundamental as F


def _v2(periods: dict, spot: dict | None = None, spot_as_of: str | None = None) -> dict:
    return {
        "_v": F.CACHE_VERSION,
        "periods": periods,
        "spot": spot or {},
        "kline_stats": {},
        "_spot_as_of": spot_as_of,
    }


def test_select_pit_period_prefers_pub_date():
    # 2025Q3 公告日 2025-10-28；2025Q4 公告日 2026-04-20
    periods = {
        "2025-09-30": {"roe": 0.10, "_pub": "2025-10-28"},
        "2025-12-31": {"roe": 0.12, "_pub": "2026-04-20"},
    }
    # as_of=2025-11-15：Q3 已公告，Q4 未公告 → 选 Q3
    sel = F._select_pit_period(periods, date(2025, 11, 15))
    assert sel is not None and sel["roe"] == 0.10
    # as_of=2026-05-01：两期都已公告 → 选最新 Q4
    sel = F._select_pit_period(periods, date(2026, 5, 1))
    assert sel["roe"] == 0.12


def test_select_pit_period_falls_back_to_statutory_lag():
    # 无 pubDate：按季末+法定滞后（Q3 约 +31 天 → 2025-10-31 可用）
    periods = {"2025-09-30": {"roe": 0.10}, "2025-12-31": {"roe": 0.12}}
    sel = F._select_pit_period(periods, date(2025, 11, 1))
    assert sel is not None and sel["roe"] == 0.10
    # 2025-10-30 仍早于 Q3 可用日 → 两期皆不可用
    assert F._select_pit_period(periods, date(2025, 10, 30)) is None


def test_extract_live_uses_latest_period():
    data = _v2({
        "2025-09-30": {"roe": 0.10, "gross_margin": 0.2},
        "2025-12-31": {"roe": 0.12, "gross_margin": 0.25},
    }, spot={"pe": 15.0, "pb": 1.5}, spot_as_of="2026-04-20")
    out = F._extract_for_asof(data, None)
    assert out["roe"] == 0.12 and out["gross_margin"] == 0.25
    assert out["pe"] == 15.0  # 实时估值可用


def test_extract_historical_pit_and_valuation_guard():
    data = _v2(
        {
            "2025-09-30": {"roe": 0.10, "gross_margin": 0.2, "_pub": "2025-10-28"},
            "2025-12-31": {"roe": 0.12, "gross_margin": 0.25, "_pub": "2026-04-20"},
        },
        spot={"pe": 15.0, "pb": 1.5},
        spot_as_of="2026-04-20",
    )
    # 2025-11-15：Q3 质量/成长可用；估值快照刷新日 2026-04-20 > as_of → 估值降级
    out = F._extract_for_asof(data, "20251115")
    assert out["roe"] == 0.10
    assert "pe" not in out and "pb" not in out
    # 2026-05-01：全部可用
    out = F._extract_for_asof(data, date(2026, 5, 1))
    assert out["roe"] == 0.12 and out["pe"] == 15.0


def test_legacy_flat_cache_backward_compat():
    flat = {"roe": 0.08, "gross_margin": 0.17}  # 旧扁平格式（无 periods）
    # 实时：原样可用
    assert F._extract_for_asof(flat, None) == flat
    # 历史回补：无报告期历史 → 降级 None（规避未来函数）
    assert F._extract_for_asof(flat, "20260101") is None


def test_fetch_fundamental_flat_cache_pit_by_mtime(monkeypatch):
    """扁平缓存过渡期：生产(信号日≥刷新日)可用，历史回补(信号日<刷新日)降级。"""
    flat = {"roe": 0.08, "gross_margin": 0.17}
    monkeypatch.setattr(F, "_load_fund_cache", lambda c: flat)
    monkeypatch.setattr(F, "_cache_mtime_date", lambda c: date(2026, 9, 1))
    # 实时/近期（2026-09-10 ≥ 刷新日）→ 原样返回，基本面不丢
    assert F.fetch_fundamental("000001", "20260910") == flat
    # 历史回补（2026-06-01 < 刷新日）→ 降级 None（规避未来函数）
    assert F.fetch_fundamental("000001", "20260601") is None


def test_build_extract_roundtrip_with_periods():
    # 构造含两期的 v2 结构，验证提取器按 as_of 选期不影响其它字段
    data = _v2({
        "2024-12-31": {"roe": 0.09, "revenue_growth": 0.05, "_pub": "2025-04-18"},
        "2025-06-30": {"roe": 0.11, "revenue_growth": 0.08, "_pub": "2025-08-29"},
    })
    out = F._extract_for_asof(data, "2025-07-01")
    # 2025-06-30 中报 2025-08-29 才公告，2025-07-01 尚不可用 → 退回 2024 年报
    assert out["roe"] == 0.09 and out["revenue_growth"] == 0.05
    # 2025-09-01（已过后中报公告日）→ 选 2025-06-30
    out = F._extract_for_asof(data, "2025-09-01")
    assert out["roe"] == 0.11 and out["revenue_growth"] == 0.08
