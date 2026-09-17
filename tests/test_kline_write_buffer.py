# -*- coding: utf-8 -*-
"""分桶写入缓冲回归测试（2026-09-17 修「写放大」）。

背景：``write_kline_cache`` 原本每写**一只票**就重写整桶（实测 5.33s/只，全宇宙
4380 只 ≈ 6.5 小时），是 CI「K 线缓存刷新」步骤 60min 超时的真因。
``kline_write_buffer()`` 让一批代码在退出时按桶只重写一次。

本测试锁定四条不变量：
  1. **默认行为不变**：无上下文时写完立刻可读（防止缓冲被误全局开启）；
  2. 上下文内**不落盘**，退出后一次性落盘且内容正确；
  3. **按桶聚合**：同桶 N 只 → 1 个 parquet 且 N 只都在；跨桶 → 各自一个文件；
  4. **异常退出也要 flush**（否则整块已取到的数据白丢），且 upsert 不会留下重复日。
"""
from __future__ import annotations

import pandas as pd
import pytest

import smcore.data.kline as kl


def _frame(n: int = 3, start: str = "2026-01-05", close: float = 10.0) -> pd.DataFrame:
    """构造 n 个连续工作日的平盘 K 线；vwap==close → 缩放巡检不会误报。"""
    dates = [d for d in pd.date_range(start, periods=n, freq="D") if d.weekday() < 5]
    return pd.DataFrame({
        "date": [d.strftime("%Y-%m-%d") for d in dates],
        "open": close, "high": close, "low": close, "close": close,
        "volume": 1e6, "amount": close * 1e6,
    })


def _bucket_files(tmp_path) -> list[str]:
    return sorted(p.name for p in tmp_path.glob("qfq_b*.parquet"))


@pytest.fixture(autouse=True)
def _clean_buffer():
    """测试前后的缓冲/开关复位，避免用例间串味。"""
    kl._BUCKET_PENDING.clear()
    kl._BUCKET_BUFFER_ENABLED = False
    yield
    kl._BUCKET_PENDING.clear()
    kl._BUCKET_BUFFER_ENABLED = False


def test_default_mode_writes_immediately(tmp_path):
    """无上下文时语义不变：写完立刻可读。"""
    kl.write_kline_cache(_frame(), "600900", "qfq", base_dir=tmp_path)
    assert _bucket_files(tmp_path) == ["qfq_b60.parquet"]
    assert len(kl.read_kline_cache("600900", "qfq", base_dir=tmp_path)) == 3


def test_buffer_defers_write_until_flush(tmp_path):
    with kl.kline_write_buffer():
        kl.write_kline_cache(_frame(), "600900", "qfq", base_dir=tmp_path)
        assert _bucket_files(tmp_path) == [], "上下文内不应落盘"
    assert _bucket_files(tmp_path) == ["qfq_b60.parquet"]
    assert len(kl.read_kline_cache("600900", "qfq", base_dir=tmp_path)) == 3


def test_buffer_groups_same_bucket_into_one_file(tmp_path):
    """核心断言：同桶 3 只 → 只重写 1 个 parquet，且 3 只都在。"""
    with kl.kline_write_buffer():
        for c in ("600001", "600002", "600003"):
            kl.write_kline_cache(_frame(), c, "qfq", base_dir=tmp_path)
    assert _bucket_files(tmp_path) == ["qfq_b60.parquet"]
    for c in ("600001", "600002", "600003"):
        assert len(kl.read_kline_cache(c, "qfq", base_dir=tmp_path)) == 3


def test_buffer_keeps_buckets_separate(tmp_path):
    with kl.kline_write_buffer():
        kl.write_kline_cache(_frame(), "600001", "qfq", base_dir=tmp_path)
        kl.write_kline_cache(_frame(), "000001", "qfq", base_dir=tmp_path)
    assert _bucket_files(tmp_path) == ["qfq_b00.parquet", "qfq_b60.parquet"]


def test_buffer_upsert_replaces_only_touched_code(tmp_path):
    """未触及的代码原样保留；同代码新行覆盖旧行，且不产生重复交易日。"""
    kl.write_kline_cache(_frame(n=4), "600100", "qfq", base_dir=tmp_path)
    with kl.kline_write_buffer():
        kl.write_kline_cache(_frame(n=5), "600100", "qfq", base_dir=tmp_path)
        kl.write_kline_cache(_frame(n=3), "600200", "qfq", base_dir=tmp_path)
    a = kl.read_kline_cache("600100", "qfq", base_dir=tmp_path)
    b = kl.read_kline_cache("600200", "qfq", base_dir=tmp_path)
    assert len(a) == 5 and a["date"].is_unique, a
    assert len(b) == 3 and b["date"].is_unique, b


def test_buffer_flushes_even_when_body_raises(tmp_path):
    """异常退出也要 flush —— 否则整块已取到的数据白丢（父进程只看到一次超时）。"""
    with pytest.raises(RuntimeError):
        with kl.kline_write_buffer():
            kl.write_kline_cache(_frame(), "600900", "qfq", base_dir=tmp_path)
            raise RuntimeError("boom")
    assert len(kl.read_kline_cache("600900", "qfq", base_dir=tmp_path)) == 3
    assert kl._BUCKET_BUFFER_ENABLED is False, "退出后缓冲开关必须复位"


def test_flush_on_empty_buffer_is_noop(tmp_path):
    with kl.kline_write_buffer():
        pass
    assert kl.flush_kline_writes() == 0


def test_buffer_nested_exit_flushes_once(tmp_path):
    """嵌套使用时内层退出即落盘、外层退出无残留（不重复写）。"""
    with kl.kline_write_buffer():
        with kl.kline_write_buffer():
            kl.write_kline_cache(_frame(), "600900", "qfq", base_dir=tmp_path)
    assert len(kl.read_kline_cache("600900", "qfq", base_dir=tmp_path)) == 3
    assert kl.flush_kline_writes() == 0
