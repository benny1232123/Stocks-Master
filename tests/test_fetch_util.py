"""fetch_data_core 行为锁：API 优先 / 本地优先 / 超时回退 / 瞬断重试。

这些行为是 boll(cctv/relativity) 三处历史 fetch_data_with_fallback 的现状，
抽取到共享骨架后必须保持等价，故用真实临时 SQLite + 假 API 锁定。
"""
import sqlite3
import time
from unittest import mock

import pandas as pd
import pytest

import smcore.data.fetch_util as fu


def _make_db(path, table, df):
    conn = sqlite3.connect(str(path))
    if df is not None:
        df.to_sql(table, conn, if_exists="replace", index=False)
    conn.close()


def test_api_first_success_writes_and_returns(tmp_path):
    db = tmp_path / "stocks_data.db"
    calls = []
    api = lambda: (calls.append(1) or pd.DataFrame({"a": [1, 2]}))
    out = fu.fetch_data_core(api, "t_xyz", db_path=str(db), prefer_local=False, timeout=None, retries=0)
    assert not out.empty
    assert calls == [1]  # API 被调用一次
    conn = sqlite3.connect(str(db))
    t = pd.read_sql("SELECT * FROM t_xyz", conn)
    conn.close()
    assert len(t) == 2  # 写回本地


def test_api_first_failure_falls_back_to_local(tmp_path):
    db = tmp_path / "stocks_data.db"
    _make_db(db, "t_xyz", pd.DataFrame({"a": [9]}))
    calls = []

    def boom(*a, **k):
        calls.append(1)
        raise ConnectionError("nope")

    out = fu.fetch_data_core(boom, "t_xyz", db_path=str(db), prefer_local=False, timeout=None, retries=0)
    assert list(out["a"]) == [9]  # 回退本地
    assert calls == [1]


def test_local_first_hits_local_without_calling_api(tmp_path):
    db = tmp_path / "stocks_data.db"
    _make_db(db, "t_xyz", pd.DataFrame({"a": [7]}))
    calls = []
    api = lambda: (calls.append(1) or pd.DataFrame({"a": [1]}))
    out = fu.fetch_data_core(api, "t_xyz", db_path=str(db), prefer_local=True, timeout=None, retries=0)
    assert list(out["a"]) == [7]
    assert calls == []  # 本地命中时不调 API


def test_local_first_miss_calls_api_and_writes(tmp_path):
    db = tmp_path / "stocks_data.db"  # 本地表缺失
    calls = []
    api = lambda: (calls.append(1) or pd.DataFrame({"a": [3]}))
    out = fu.fetch_data_core(api, "t_xyz", db_path=str(db), prefer_local=True, timeout=None, retries=0)
    assert list(out["a"]) == [3]
    assert calls == [1]
    conn = sqlite3.connect(str(db))
    t = pd.read_sql("SELECT * FROM t_xyz", conn)
    conn.close()
    assert len(t) == 1


def test_retry_on_transient_then_success(tmp_path):
    db = tmp_path / "stocks_data.db"
    state = {"n": 0}

    def flaky(*a, **k):
        state["n"] += 1
        if state["n"] < 2:
            raise OSError("transient")
        return pd.DataFrame({"a": [5]})

    out = fu.fetch_data_core(flaky, "t_xyz", db_path=str(db), prefer_local=True, timeout=None, retries=3, retry_backoff=0)
    assert list(out["a"]) == [5]
    assert state["n"] == 2  # 重试一次后成功


def test_timeout_falls_back_to_local(tmp_path):
    db = tmp_path / "stocks_data.db"
    _make_db(db, "t_xyz", pd.DataFrame({"a": [42]}))
    calls = []

    def slow(*a, **k):
        calls.append(1)
        time.sleep(2)
        return pd.DataFrame({"a": [1]})

    with mock.patch.object(fu, "_call_with_timeout", side_effect=TimeoutError("timed out")):
        out = fu.fetch_data_core(slow, "t_xyz", db_path=str(db), prefer_local=False, timeout=0.1, retries=0)
    assert list(out["a"]) == [42]  # 超时后回退本地（slow 因超时被中断，未返回）
    assert calls == []  # 超时在 API 体执行前触发


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
