"""k_data 复权基准守卫回归测试（2026-08-09 事故修复）。

背景：qfq 以「最新交易日」为锚，分红送转后整条历史序列被重新缩放。
「旧缓存 + 新拉段」拼接会把旧段（过期基准）和新段（新基准）缝在一起，
产生物理不可能的跳变（实测 600900 接缝 +46%）。本测试锁定：
  1. find_price_breaks 能识别物理不可能的相邻跳变；
  2. fetch_daily_k 在拼接后发现断层会全量重拉自愈，不把两套基准拼一起；
  3. 干净缓存（与源同基准）不会触发重拉（无重入/无告警风暴）。
全部用 mock 后端，不依赖网络 / tdx 终端。

关键构造：缓存与 mock 源都从「同一根 canonical 序列」派生（仅按 scale 缩放），
保证重叠交易日价格完全一致 —— 这样「干净拼接」的接缝漂移恒为 0，
守卫只在确实发生基准错位（scale 不同）时才触发重拉。
"""
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import pytest

import smcore.data.kline as kl


def _canonical(start="2015-01-05", end="2026-12-31"):
    """返回一根内部连续的日线 dict{date: close}，special 点固定为 17.619。"""
    dates = [d for d in pd.date_range(start, end, freq="D") if d.weekday() < 5]
    rng = np.random.default_rng(7)
    closes = 17.6 + rng.normal(0, 0.03, len(dates))
    table = {d.strftime("%Y-%m-%d"): c for d, c in zip(dates, closes)}
    table["2026-02-10"] = 17.619
    return table


def _frame_from_table(table, start, end, scale=1.0):
    rows = []
    for d in pd.date_range(start, end, freq="D"):
        if d.weekday() >= 5:
            continue
        ds = d.strftime("%Y-%m-%d")
        if ds in table:
            c = table[ds] * scale
            rows.append({"date": ds, "open": c, "high": c, "low": c,
                         "close": c, "volume": 1e6, "amount": 1e8})
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["date", "open", "high", "low", "close", "volume", "amount"])


def _install_mock(monkeypatch, scale=1.0):
    """把 tdx 后端替换成「从 canonical 派生、按 scale 缩放」的假源，返回调用计数器。"""
    table = _canonical()
    counter = {"n": 0}

    def fake_tdx(code6, start, end, adjust):
        counter["n"] += 1
        return _frame_from_table(table, str(start), str(end), scale=scale)

    monkeypatch.setattr(kl, "_fetch_via_tdx", fake_tdx)
    monkeypatch.setattr(kl, "_backend", lambda: "tdx")
    return counter


def test_find_price_breaks_flags_impossible_jump():
    table = _canonical()
    df = _frame_from_table(table, "2015-01-05", "2026-08-07")
    # 在中间人为制造一个 +46% 断层（主板物理不可能）。
    # 该点相对前日 +46%，其下一日的比值会反向 -31%，故会检出 >=1 个断点。
    closes = df["close"].tolist()
    bi = len(closes) // 2
    df.loc[bi, "close"] = closes[bi - 1] * 1.46
    hits = kl.find_price_breaks(df, "600900")
    assert any(abs(b["ratio"] - 1.46) < 0.02 for b in hits), hits
    assert len(hits) >= 1


def test_find_price_breaks_clean_series_no_false_positive():
    table = _canonical()
    df = _frame_from_table(table, "2015-01-05", "2026-08-07")
    assert kl.find_price_breaks(df, "600900") == []


def test_guard_self_heals_dirty_cache(monkeypatch, tmp_path):
    kl.K_DATA_CACHE_DIR = Path(tmp_path)
    counter = _install_mock(monkeypatch, scale=1.0)  # 源是干净 1x 基准

    # 写入「脏」缓存：整体 1.454 倍缩放，使 2026-02-10 = 25.62（正确应为 17.619），
    # 只覆盖到 2026-08-07，请求延伸到 2026-12-31 以进入 fetch 路径。
    table = _canonical()
    _frame_from_table(table, "2015-01-05", "2026-08-07", scale=1.454).to_csv(
        tmp_path / "600900_qfq_full.csv", index=False)

    res = kl.fetch_daily_k("600900", "2026-08-01", "2026-12-31",
                           adjust="qfq", use_cache=True, force_refresh=False)
    assert not res.empty
    cached = pd.read_csv(tmp_path / "600900_qfq_full.csv")
    v = cached[cached["date"] == "2026-02-10"]["close"].values[0]
    assert abs(v - 17.619) < 0.01, f"未自愈，仍为 {v}"
    # 接缝漂移 + 整段自洽性双重守卫至少触发一次重拉（尾段 + 全量）
    assert counter["n"] >= 2


def test_guard_does_not_repull_clean_cache(monkeypatch, tmp_path):
    kl.K_DATA_CACHE_DIR = Path(tmp_path)
    counter = _install_mock(monkeypatch, scale=1.0)

    # 干净缓存与源同基准（都来自 canonical 1x），只覆盖到 2026-08-07
    table = _canonical()
    _frame_from_table(table, "2015-01-05", "2026-08-07", scale=1.0).to_csv(
        tmp_path / "600900_qfq_full.csv", index=False)

    kl.fetch_daily_k("600900", "2026-08-01", "2026-12-31",
                     adjust="qfq", use_cache=True, force_refresh=False)
    # 同基准拼接：接缝漂移恒为 0，不应触发全量重拉（仅 1 次尾段拉取）
    assert counter["n"] == 1


def test_guard_no_infinite_recursion(monkeypatch, tmp_path):
    """源本身带合法大跳变（如停复牌）时，重拉后应接受单一源基准，不无限递归。"""
    kl.K_DATA_CACHE_DIR = Path(tmp_path)
    counter = _install_mock(monkeypatch, scale=2.0)  # 源是 2x 基准

    # 脏缓存（3x）与源（2x）不同基准，只覆盖到 2026-08-07
    table = _canonical()
    _frame_from_table(table, "2015-01-05", "2026-08-07", scale=3.0).to_csv(
        tmp_path / "600900_qfq_full.csv", index=False)

    res = kl.fetch_daily_k("600900", "2026-08-01", "2026-12-31",
                           adjust="qfq", use_cache=True, force_refresh=False)
    assert not res.empty  # 不抛 RecursionError / 不卡死
    cached = pd.read_csv(tmp_path / "600900_qfq_full.csv")
    v = cached[cached["date"] == "2026-02-10"]["close"].values[0]
    # 收敛到单一源基准（2x）
    assert abs(v - 17.619 * 2) < 0.1, f"未收敛到单一源基准：{v}"
