# -*- coding: utf-8 -*-
"""复权基准守卫的「窗口化」与「不丢数据」回归测试（2026-09-17 事故后定案）。

被锁死的行为：
  ① 守卫②（整段断层扫描）**只扫最近 KLINE_BREAK_SCAN_DAYS 天** —— 深历史断层是 hithink
     源头自带、重拉无解，全历史扫描会让 68.7% 的票每次刷新都白拉一轮 11 年；而窗口内的
     断层**仍然照拦**（收窄，不是关闭）。
  ② 守卫①的重拉若没拿到数据，**不得**把「旧基准缓存 + 新基准段」的拼接结果回给调用方或
     落盘（那正是 2026-08-09 的 +46% 接缝事故），要退回**单一基准**的缓存切片。
  ③ 守卫②的重拉若没拿到数据，**不得**把已经拼好的单一基准序列丢掉，要继续落盘。

硬约束：**akshare 默认不参与 K 线回退链**（KLINE_AKSHARE_FALLBACK=1 才开）。
本文件把 akshare 换成一个「一被调用就断言失败」的桩，所以任何一例碰它都会红。

全部 mock，无网络。
"""
from __future__ import annotations

import importlib
import sys
import warnings
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd

import smcore.data.kline as kl

DEEP_BREAK_DAY = "2015-06-01"     # 窗口外（深历史）
WINDOW_BREAK_DAY = "2026-06-01"   # 窗口内
EMPTY = pd.DataFrame(columns=kl.DAILY_K_COLUMNS)


def _canonical(start="2015-01-05", end="2026-12-31"):
    """内部连续、同基准、无断层的日线 dict{date: close}。"""
    dates = [d for d in pd.date_range(start, end, freq="D") if d.weekday() < 5]
    rng = np.random.default_rng(11)
    closes = 17.6 + rng.normal(0, 0.02, len(dates))
    return {d.strftime("%Y-%m-%d"): c for d, c in zip(dates, closes)}


def _frame(table, start, end, scale=1.0, spike_day=None, spike=1.46):
    rows = []
    for d in pd.date_range(start, end, freq="D"):
        if d.weekday() >= 5:
            continue
        ds = d.strftime("%Y-%m-%d")
        if ds not in table:
            continue
        c = table[ds] * scale * (spike if (spike_day and ds == spike_day) else 1.0)
        rows.append({"date": ds, "open": c, "high": c, "low": c,
                     "close": c, "volume": 1e6, "amount": 1e8})
    return pd.DataFrame(rows) if rows else EMPTY.copy()


def _seed_cache(tmp_path, table, start="2015-01-05", end="2026-08-07", **kw):
    _frame(table, start, end, **kw).to_csv(tmp_path / "600900_qfq_full.csv", index=False)


def _install_tdx(monkeypatch, table, long_range_empty=False, scale=1.0):
    """tdx 假源；long_range_empty=True 时对跨度 >3000 天的请求返空（模拟源不可用/超限）。"""
    counter = {"n": 0}

    def fake_tdx(code6, start, end, adjust):
        counter["n"] += 1
        if long_range_empty and (pd.Timestamp(end) - pd.Timestamp(start)).days > 3000:
            return EMPTY.copy()
        return _frame(table, str(start), str(end), scale=scale)

    monkeypatch.setattr(kl, "_fetch_via_tdx", fake_tdx)
    monkeypatch.setattr(kl, "_backend", lambda: "tdx")
    return counter


@contextmanager
def _no_backends(monkeypatch):
    """封死 tdx 之外的后端，保证不外溢到网络；**akshare 被碰即断言失败**。"""
    def _boom(*_a, **_k):
        raise AssertionError("akshare 默认不应参与 K 线回退链（见 KLINE_AKSHARE_FALLBACK）")

    class _FakeRs:
        error_code = "1"          # 非 "0" → _fetch_segment 视为失败返回空表
        fields: list = []

    class _FakeBS:
        @staticmethod
        def query_history_k_data_plus(*_a, **_k):
            return _FakeRs()

        @staticmethod
        def login():
            return _FakeRs()      # error_code="1" → 登录失败

        @staticmethod
        def logout():
            return None

    class _NullSess:
        def __enter__(self):
            return False          # 会话不可用 → 直接返回空表

        def __exit__(self, *_a):
            return False

    monkeypatch.setattr(kl, "_fetch_via_hithink", lambda *a, **k: EMPTY.copy())
    monkeypatch.setattr(kl, "_fetch_via_akshare", _boom)
    monkeypatch.setitem(sys.modules, "baostock", _FakeBS)
    # ⚠️ 必须按**模块对象** patch，不能用字符串目标 "smcore.data.session.session"：
    # smcore/data/__init__.py 里 re-export 了同名函数，把 `session` 子模块属性**遮蔽**了，
    # 于是 pytest 的 derive_importpath 解析 "smcore.data.session" 得到的是**函数对象**，
    # 补丁被静默打到那个函数上、模块属性纹丝不动 —— 桩形同不存在（2026-09-17 踩坑）。
    _sess_mod = importlib.import_module("smcore.data.session")
    monkeypatch.setattr(_sess_mod, "session", lambda *a, **k: _NullSess())
    yield


# ── 窗口切片本身 ────────────────────────────────────────────────────────

def test_break_scan_window_keeps_tail_plus_one_leading_bar():
    df = _frame(_canonical(), "2024-01-01", "2026-12-31")
    win = kl._break_scan_window(df, pd.Timestamp("2026-12-31").date(), 500)
    assert not win.empty and len(win) < len(df)
    cutoff = pd.Timestamp("2026-12-31") - pd.Timedelta(days=500)
    assert pd.Timestamp(win["date"].iloc[0]) < cutoff, "应多带一根前导 bar（算窗口首根的比值要用）"
    assert pd.Timestamp(win["date"].iloc[1]) >= cutoff
    assert win["date"].iloc[-1] == df["date"].iloc[-1]


def test_break_scan_window_disabled_returns_all():
    df = _frame(_canonical(), "2024-01-01", "2026-12-31")
    assert len(kl._break_scan_window(df, pd.Timestamp("2026-12-31").date(), 0)) == len(df)


def test_break_scan_window_empty_when_no_bar_in_window():
    df = _frame(_canonical(), "2024-01-01", "2024-02-01")
    assert kl._break_scan_window(df, pd.Timestamp("2026-12-31").date(), 500).empty


# ── 深历史断层不再无谓重拉；窗口内断层仍然拦 ────────────────────────────

def test_deep_history_break_does_not_trigger_repull(monkeypatch, tmp_path):
    kl.K_DATA_CACHE_DIR = Path(tmp_path)
    table = _canonical()
    _seed_cache(tmp_path, table, spike_day=DEEP_BREAK_DAY)
    counter = _install_tdx(monkeypatch, table)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = kl.fetch_daily_k("600900", "2026-08-01", "2026-12-31", adjust="qfq")

    assert not res.empty
    assert counter["n"] == 1, f"深历史断层不该触发全量重拉（实际取数 {counter['n']} 次）"


def test_same_deep_break_triggers_repull_when_scan_disabled(monkeypatch, tmp_path):
    """对照：关掉窗口（=0）就退回旧行为 → 证明上面「不重拉」确实是窗口带来的。"""
    kl.K_DATA_CACHE_DIR = Path(tmp_path)
    table = _canonical()
    _seed_cache(tmp_path, table, spike_day=DEEP_BREAK_DAY)
    counter = _install_tdx(monkeypatch, table)
    monkeypatch.setattr(kl, "KLINE_BREAK_SCAN_DAYS", 0)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kl.fetch_daily_k("600900", "2026-08-01", "2026-12-31", adjust="qfq")

    assert counter["n"] >= 2, f"全历史扫描应触发重拉（实际取数 {counter['n']} 次）"


def test_window_break_still_triggers_repull_and_heals(monkeypatch, tmp_path):
    kl.K_DATA_CACHE_DIR = Path(tmp_path)
    table = _canonical()
    _seed_cache(tmp_path, table, spike_day=WINDOW_BREAK_DAY)     # 断层在窗口内
    counter = _install_tdx(monkeypatch, table)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        kl.fetch_daily_k("600900", "2026-08-01", "2026-12-31", adjust="qfq")

    assert counter["n"] >= 2, "窗口内的断层必须继续触发重拉"
    cached = kl.read_kline_cache("600900", "qfq", base_dir=tmp_path)
    got = cached[cached["date"] == WINDOW_BREAK_DAY]["close"]
    assert len(got) == 1
    assert abs(float(got.values[0]) - table[WINDOW_BREAK_DAY]) < 0.01, "自愈后盘上不该还有尖峰"


# ── 重拉没拿到数据时不许丢数据 ──────────────────────────────────────────

def test_repull_failure_keeps_merged_series(monkeypatch, tmp_path):
    """守卫②重拉返空 → 保留已拼好的单一基准序列继续落盘，不得丢数据。"""
    kl.K_DATA_CACHE_DIR = Path(tmp_path)
    table = _canonical()
    _seed_cache(tmp_path, table, spike_day=WINDOW_BREAK_DAY)
    counter = _install_tdx(monkeypatch, table, long_range_empty=True)

    with _no_backends(monkeypatch):
        with warnings.catch_warnings(record=True) as rec:
            warnings.simplefilter("always")
            res = kl.fetch_daily_k("600900", "2026-08-01", "2026-12-31", adjust="qfq")

    assert not res.empty, "重拉失败时不得把已有数据丢掉"
    assert counter["n"] >= 2, "应确实尝试过一次全量重拉"
    joined = " ".join(str(w.message) for w in rec)
    assert "保留现有序列" in joined, joined
    cached = kl.read_kline_cache("600900", "qfq", base_dir=tmp_path)
    assert (cached["date"] == "2026-12-31").any(), "重拉失败也应把已取到的尾部落盘"


def test_drift_repull_failure_falls_back_to_single_basis_cache(monkeypatch, tmp_path):
    """守卫①重拉返空 → 退回**单一基准**的缓存切片；绝不把两套基准缝在一起回给调用方或落盘。"""
    kl.K_DATA_CACHE_DIR = Path(tmp_path)
    table = _canonical()
    _seed_cache(tmp_path, table, scale=1.454)                    # 缓存 = 1.454× 基准
    _install_tdx(monkeypatch, table, long_range_empty=True, scale=1.0)

    with _no_backends(monkeypatch):
        with warnings.catch_warnings(record=True) as rec:
            warnings.simplefilter("always")
            res = kl.fetch_daily_k("600900", "2026-08-01", "2026-12-31", adjust="qfq")

    assert not res.empty, "重拉失败时不该返回空"
    # 返回的必须是**单一基准**（全部 1.454×），而不是 1.454× 旧段 + 1.0× 新段的拼接
    ratios = res["close"].values / np.array([table[d] for d in res["date"]])
    assert np.allclose(ratios, 1.454, atol=0.02), \
        f"返回了混基准序列：{ratios.min():.3f}~{ratios.max():.3f}"
    joined = " ".join(str(w.message) for w in rec)
    assert "退回缓存切片" in joined, joined
    # 混基准内容不得落盘：缓存里最新日期仍是旧缓存的 2026-08-07
    cached = kl.read_kline_cache("600900", "qfq", base_dir=tmp_path)
    assert not (cached["date"] == "2026-12-31").any(), "混基准内容不得落盘"


# ── akshare 摘除（行为验证，不是复刻实现）──────────────────────────────

def test_akshare_not_in_default_fallback_chain(monkeypatch, tmp_path):
    """默认链上所有后端都失败 → 返回空；且 akshare 一旦被调用，_no_backends 的桩会抛断言。"""
    kl.K_DATA_CACHE_DIR = Path(tmp_path)
    _install_tdx(monkeypatch, _canonical())
    monkeypatch.setattr(kl, "_fetch_via_tdx", lambda *a, **k: EMPTY.copy())   # tdx 也失败

    with _no_backends(monkeypatch):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            res = kl.fetch_daily_k("600900", "2026-08-01", "2026-12-31",
                                   adjust="qfq", force_refresh=True)

    assert res.empty, "tdx/hithink/baostock 全失败时应返回空（而不是被 akshare 悄悄补上）"


def test_akshare_opt_in_is_used(monkeypatch, tmp_path):
    kl.K_DATA_CACHE_DIR = Path(tmp_path)
    table = _canonical()
    monkeypatch.setattr(kl, "KLINE_AKSHARE_FALLBACK", True)
    _install_tdx(monkeypatch, table)
    monkeypatch.setattr(kl, "_fetch_via_tdx", lambda *a, **k: EMPTY.copy())   # 逼到下一档
    monkeypatch.setattr(kl, "_fetch_via_hithink", lambda *a, **k: EMPTY.copy())
    called = {"n": 0}

    def fake_ak(code6, start, end, adjust):
        called["n"] += 1
        return _frame(table, str(start), str(end))

    monkeypatch.setattr(kl, "_fetch_via_akshare", fake_ak)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = kl.fetch_daily_k("600900", "2026-08-01", "2026-12-31",
                               adjust="qfq", force_refresh=True)

    assert called["n"] == 1, "显式开启后 akshare 应被使用"
    assert not res.empty
