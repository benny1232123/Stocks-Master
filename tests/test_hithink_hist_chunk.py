# -*- coding: utf-8 -*-
"""hithink 个股历史 K 的分片取数：绕开服务端 ~3652 自然日跨度上限。

背景（2026-09-17 事故取证，.workbuddy/_probe_repull.txt）
------------------------------------------------------
单次请求 ``start`` 距 ``end`` > ~3652 自然日 → 服务端**整段静默返回空**（code=0 且
item=[]，不报错不截断）。而 ``kline.fetch_daily_k`` 的「复权基准守卫」发现历史断层时
会从缓存最早日**全量重拉**（``2015-01-05 ~ 今天``，>4000 天）→ 静默返空 → 回退链
**静默换源到 akshare**，把 akshare 的序列整段覆盖 hithink 缓存（实测 000019：
2844 行 → 2690 行，来源被换掉），继而断层归零、守卫误判「自愈成功」。跨源混血污染
由此产生且完全不可见。

本测试锁死分片语义：
  ① 长区间必须被切成多片，且**首尾相接、不重不漏**；
  ② 拼接按时间升序、按日去重；
  ③ 有数据之后出现空洞 → **整次返回空**（全有或全无，绝不交付截断序列）；
  ④ 开头空片（早于上市日）允许跳过；
  ⑤ 空片要重试，瞬时抖动可恢复；
  ⑥ 生产默认（1500 天）下短区间仍只发 1 次请求（常见路径零回归）。

⚠️ 分片几何：片区间是**闭区间** ``[cur, cur+CHUNK_DAYS]``，下一片从 ``chunk_end+1天``
   起，故相邻片起点相差 ``CHUNK_DAYS + 1`` 天。测试里用 ``_starts()`` 显式算，别手写。

全部离线：``_get`` 被替换成本地假实现，绝不触网。
"""
from __future__ import annotations

from datetime import date, timedelta

from smcore.data import hithink as hk

D0 = date(2020, 1, 1)


def _item(d: date, close: float = 10.0) -> dict:
    return {
        "date_ms": hk._ms(d),
        "open_price": close,
        "high_price": close,
        "low_price": close,
        "close_price": close,
        "volume": 100.0,
        "turnover": 1000.0,
    }


def _starts(n: int, chunk_days: int) -> list[date]:
    """闭区间分片下，第 i 片的起点（起点间距 = chunk_days + 1 天）。"""
    return [D0 + timedelta(days=i * (chunk_days + 1)) for i in range(n)]


def _install(monkeypatch, handler, chunk_days: int | None = 10, retry: int = 3):
    """装好假 _get；handler(start, end, nth_call_for_this_chunk) → item 列表。

    chunk_days=None 表示**不 patch**，沿用生产默认值（用于验证「短区间零回归」）。
    """
    calls: list[tuple[date, date]] = []

    if chunk_days is not None:
        monkeypatch.setattr(hk, "_HK_HIST_CHUNK_DAYS", chunk_days)
    monkeypatch.setattr(hk, "_HK_HIST_CHUNK_RETRY", retry)
    monkeypatch.setattr(hk, "to_thscode", lambda c: f"{c}.SZ")
    monkeypatch.setattr(hk.time, "sleep", lambda _s: None)

    def _fake_get(path, params=None, retries=2):
        assert path == "/api/a-share/prices/historical"
        s = date.fromisoformat(hk._ms_to_date(params["start"]))
        e = date.fromisoformat(hk._ms_to_date(params["end"]))
        nth = sum(1 for a, _b in calls if a == s)
        calls.append((s, e))
        return {"item": handler(s, e, nth)}

    monkeypatch.setattr(hk, "_get", _fake_get)
    return calls


# ── ① 切片几何 ──────────────────────────────────────────────────────────

def test_long_range_is_split_into_contiguous_chunks(monkeypatch):
    """片首尾相接、不重不漏；每片跨度 ≤ 上限。"""
    calls = _install(monkeypatch, lambda s, e, _n: [_item(s)], chunk_days=10)
    hk.fetch_historical_k("000001", D0, D0 + timedelta(days=43))
    assert [c[0] for c in calls] == _starts(4, 10), "片起点间距应为 CHUNK_DAYS+1 天"
    for i, (s, e) in enumerate(calls):
        assert (e - s).days <= 10, "单片跨度不得超过上限"
        assert (s - D0).days == i * 11, "片必须连续推进"
        assert e + timedelta(days=1) == calls[i + 1][0] if i + 1 < len(calls) else True


def test_production_default_keeps_short_range_single_request(monkeypatch):
    """生产默认 1500 天：400 天区间必须仍是 1 次请求（常见路径零回归）。"""
    calls = _install(monkeypatch, lambda s, e, _n: [_item(s)], chunk_days=None)
    hk.fetch_historical_k("000001", D0, D0 + timedelta(days=400))
    assert len(calls) == 1, f"400 天区间不应分片，实际 {len(calls)} 次"


# ── ② 拼接与去重 ────────────────────────────────────────────────────────

def test_chunks_are_concatenated_in_order(monkeypatch):
    _install(monkeypatch, lambda s, e, _n: [_item(s), _item(e)], chunk_days=10)
    # 3 片（起点 D0 / +11 / +22），每片 2 行 → 6 行
    df = hk.fetch_historical_k("000001", D0, D0 + timedelta(days=32))
    assert list(df.columns) == hk._HIST_COLUMNS
    assert df["date"].tolist() == sorted(df["date"].tolist()), "拼接结果必须按时间升序"
    assert len(df) == 6


def test_duplicate_dates_are_deduped(monkeypatch):
    """分片边界若返回重叠日，必须按日去重，不得出现重复行。"""
    def _h(s, e, _n):
        d = s + timedelta(days=1)
        return [_item(s), _item(d), _item(d)]

    _install(monkeypatch, _h, chunk_days=10)
    df = hk.fetch_historical_k("000001", D0, D0 + timedelta(days=21))
    assert df["date"].is_unique


# ── ③ 全有或全无 ────────────────────────────────────────────────────────

def test_empty_chunk_after_data_returns_empty(monkeypatch):
    """有数据之后的空洞 → 整次判失败（宁可让调用方降级，也不交截断序列）。"""
    bad_at = _starts(3, 10)[1]

    def _h(s, e, _n):
        return [] if s >= bad_at else [_item(s)]

    calls = _install(monkeypatch, _h, chunk_days=10, retry=2)
    df = hk.fetch_historical_k("000001", D0, D0 + timedelta(days=32))
    assert df.empty, "中间被挖空的序列绝不可返回"
    assert len(calls) == 3, "1 个好片 + 坏片重试 2 次"


def test_all_chunks_empty_returns_empty(monkeypatch):
    _install(monkeypatch, lambda s, e, _n: [], chunk_days=10, retry=1)
    df = hk.fetch_historical_k("000003", D0, D0 + timedelta(days=32))
    assert df.empty
    assert list(df.columns) == hk._HIST_COLUMNS


# ── ④ 开头空片（早于上市日）─────────────────────────────────────────────

def test_leading_empty_chunks_are_skipped(monkeypatch):
    """请求起点早于上市日 → 前几片为空属常态，不得判失败。"""
    listed = _starts(2, 10)[1]          # 第 2 片起点视为上市日

    def _h(s, e, _n):
        return [_item(s)] if s >= listed else []

    calls = _install(monkeypatch, _h, chunk_days=10, retry=1)
    df = hk.fetch_historical_k("000001", D0, D0 + timedelta(days=43))
    assert not df.empty, "开头空片不应导致整体判失败"
    assert len(calls) == 4, "仍要把 4 片都试一遍"
    # 前 2 片为空（早于上市日）被跳过，其余 3 片各出一行
    assert df["date"].tolist() == [str(x) for x in _starts(4, 10)[1:]]


# ── ⑤ 瞬时抖动可恢复 ────────────────────────────────────────────────────

def test_transient_empty_is_retried(monkeypatch):
    """空片多为接口抖动/限流：重试后拿到数据即成功（第 2 片首次空、重试有值）。"""
    bad_at = _starts(3, 10)[1]

    def _h(s, e, nth):
        if s == bad_at and nth == 0:
            return []
        return [_item(s)]

    calls = _install(monkeypatch, _h, chunk_days=10, retry=3)
    df = hk.fetch_historical_k("000001", D0, D0 + timedelta(days=32))
    assert not df.empty, "瞬时空片重试后应成功"
    assert len(df) == 3
    assert len(calls) == 4, "第 2 片应被请求两次"


# ── ⑥ 入参健壮性 ────────────────────────────────────────────────────────

def test_reversed_range_makes_no_request(monkeypatch):
    calls = _install(monkeypatch, lambda s, e, _n: [_item(s)])
    assert hk.fetch_historical_k("000001", D0 + timedelta(days=5), D0).empty
    assert calls == []


def test_unparsable_date_makes_no_request(monkeypatch):
    calls = _install(monkeypatch, lambda s, e, _n: [_item(s)])
    assert hk.fetch_historical_k("000001", "not-a-date", D0).empty
    assert calls == []


def test_string_dates_are_accepted(monkeypatch):
    calls = _install(monkeypatch, lambda s, e, _n: [_item(s)])
    df = hk.fetch_historical_k("000001", "2020-01-01", "2020-01-05")
    assert len(calls) == 1
    assert df["date"].tolist() == ["2020-01-01"]
