# -*- coding: utf-8 -*-
"""除权事件流记忆化：``classify_breaks`` 的 N 次网络调用 → 1 次。

背景（2026-09-17 实测定位）
--------------------------
``classify_breaks`` 对**每个**复权断层都调 ``corporate_action_explains``，而后者只用
``code6`` 取 ``fetch_adjustment_factors(code6)`` —— 同一只票的 N 个断层会发起 N 次
**入参完全相同**的 HTTP 请求（``_get`` 还带 retries=2）。历史遗留污染严重的票断层数可达
79 个（000019），于是单只票多出 79 次串行网络往返，成为 K 线刷新里除「全量重拉」之外
的第二大成本。

本测试锁死两件事：
  ① **每只票每个自然日只取一次**事件流（缓存命中，含失败也缓存，不重复打网络）；
  ② **记忆化不改变判定结果**（同入参同结果；可解释/不可解释的标注不变）。

全部离线：hithink 取数被替换成本地计数器，绝不触网。
"""
from __future__ import annotations

from datetime import datetime

import pytest

from smcore.data import hithink_special as hs

BREAK = {"date": "2015-01-19", "prev_close": 10.0, "close": 20.0, "ratio": 2.0}


def _breaks(n: int = 1) -> list[dict]:
    return [dict(BREAK) for _ in range(n)]


def _install(monkeypatch, events=None, boom: bool = False) -> list[str]:
    """把 hithink 的取数换成计数器，返回被调用过的代码列表（顺序保留）。"""
    calls: list[str] = []

    def _fake_fetch(code6, start=None, end=None):
        calls.append(code6)
        if boom:
            raise RuntimeError("network down")
        return list(events or [])

    monkeypatch.setattr(hs._hk, "available", lambda: True)
    monkeypatch.setattr(hs._hk, "fetch_adjustment_factors", _fake_fetch)
    return calls


@pytest.fixture(autouse=True)
def _clean_event_cache():
    hs._EVENT_CACHE.clear()
    yield
    hs._EVENT_CACHE.clear()


# ── ① 取数次数 ──────────────────────────────────────────────────────────

def test_ninety_breaks_fetch_events_once(monkeypatch):
    """核心断言：79 个断层只应发起 1 次取数（修复前是 79 次）。"""
    calls = _install(monkeypatch)
    out = hs.classify_breaks("000019", _breaks(79))
    assert len(out) == 79
    assert calls == ["000019"], f"应只取 1 次事件流，实际 {len(calls)} 次"
    assert all(b["explained_by_corporate_action"] is False for b in out)


def test_cache_shared_across_repeated_calls(monkeypatch):
    calls = _install(monkeypatch)
    hs.classify_breaks("000001", _breaks(3))
    hs.classify_breaks("000001", _breaks(7))
    assert calls == ["000001"], "同一只票的多次调用应共用缓存"


def test_distinct_codes_fetch_separately(monkeypatch):
    calls = _install(monkeypatch)
    hs.classify_breaks("000001", _breaks(2))
    hs.classify_breaks("000002", _breaks(2))
    assert calls == ["000001", "000002"], "不同代码必须各自取数"


def test_empty_breaks_make_no_call(monkeypatch):
    calls = _install(monkeypatch)
    assert hs.classify_breaks("000001", []) == []
    assert calls == [], "无断层时不应发起任何取数"


def test_break_without_date_makes_no_call(monkeypatch):
    """无日期的畸形断层不应触发取数（也走不到 corporate_action_explains 的取数分支）。"""
    calls = _install(monkeypatch)
    out = hs.classify_breaks("000001", [{"date": "", "prev_close": 10.0, "close": 20.0, "ratio": 2.0}])
    assert out[0]["explained_by_corporate_action"] is False
    assert calls == []


# ── ② 健壮性与键的作用域 ───────────────────────────────────────────────

def test_fetch_failure_is_failsoft_and_cached(monkeypatch):
    """取数抛错 → 全部标注为「不可解释」，不冒泡；且失败也进缓存，不反复打网络。"""
    calls = _install(monkeypatch, boom=True)
    out = hs.classify_breaks("000003", _breaks(5))
    assert [b["explained_by_corporate_action"] for b in out] == [False] * 5
    hs.classify_breaks("000003", _breaks(5))
    assert len(calls) == 1, "失败的取数也应缓存，避免每只票重复打网络"


def test_cache_key_is_scoped_per_day(monkeypatch):
    """键含日期：把条目改成「昨天」的键后必须重新取数（长驻进程不读陈旧事件）。"""
    calls = _install(monkeypatch)
    hs.classify_breaks("000001", _breaks(2))
    assert len(calls) == 1
    (key,) = list(hs._EVENT_CACHE)
    hs._EVENT_CACHE[("000001", "19990101")] = hs._EVENT_CACHE.pop(key)
    hs.classify_breaks("000001", _breaks(2))
    assert len(calls) == 2, "跨日后必须重取事件流"


def test_cache_is_bounded(monkeypatch):
    """全宇宙刷新（数千只）时缓存不得无界增长。"""
    calls = _install(monkeypatch)
    hs._EVENT_CACHE_MAX = 10
    try:
        for i in range(40):
            hs.classify_breaks("%06d" % i, _breaks(1))
        assert len(hs._EVENT_CACHE) <= 11, f"缓存未被裁剪: {len(hs._EVENT_CACHE)}"
    finally:
        hs._EVENT_CACHE_MAX = 2000


# ── ③ 语义不变：可解释的断层仍被判为可解释 ────────────────────────────

def test_matching_corporate_action_marks_explained(monkeypatch):
    """除权日在断层的 ±3 天内且幅度吻合 → 仍须标注为「可解释」（记忆化未破坏语义）。"""
    sh = hs._hk._SH
    ms = int(datetime(2026, 6, 23, 12, 0, tzinfo=sh).timestamp() * 1000)
    _install(monkeypatch, events=[{
        "ex_date_ms": ms, "dividend_per_share": 1.0, "per_share_bonus": 0.0,
    }])
    # prev_close=20，每股派 1 元 → expected = (1 - 1/20) / (1 + 0) = 0.95
    out = hs.classify_breaks("000004", [{"date": "2026-06-23", "prev_close": 20.0,
                                         "close": 19.0, "ratio": 0.95}])
    assert out[0]["explained_by_corporate_action"] is True


def test_distant_corporate_action_not_explained(monkeypatch):
    """除权日离断层太远（>3 天）→ 不可解释。"""
    sh = hs._hk._SH
    ms = int(datetime(2026, 1, 5, 12, 0, tzinfo=sh).timestamp() * 1000)
    _install(monkeypatch, events=[{
        "ex_date_ms": ms, "dividend_per_share": 1.0, "per_share_bonus": 0.0,
    }])
    out = hs.classify_breaks("000004", [{"date": "2026-06-23", "prev_close": 20.0,
                                         "close": 19.0, "ratio": 0.95}])
    assert out[0]["explained_by_corporate_action"] is False
