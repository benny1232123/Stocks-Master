"""统一的数据获取骨架：本地 SQLite 缓存 + API 优先/本地优先 + 超时/瞬断重试。

背景
----
``boll`` / ``cctv`` / ``relativity`` 三个策略原本各自复制了一份
``fetch_data_with_fallback``，但行为已经分化：

- boll: API 优先，无超时、无重试，表名原样传入（不做 sanitize）
- cctv: API 优先，带超时（AK_API_TIMEOUT），表名经自有 sanitize
- relativity: **本地优先**，无超时，带瞬断重试，表名经 _cache_table_name

三处 DB 物理路径其实一致（均为 STOCK_DATA_DIR/stocks_data.db），但顺序 /
超时 / 重试 / 表名 sanitize 各不相同。本模块只提供"骨架"，由各策略模块
保留自己当前的 table_name 计算与参数选择后**委托调用**，从而消除复制粘贴，
同时做到「零行为变更」（调用方决定顺序与参数，本函数不做任何隐式统一）。

超时实现复用 ``smcore.data.kline._call_with_timeout``，不重复定义。
"""
from __future__ import annotations

import os
import sqlite3
import time
from typing import Callable, Optional

import pandas as pd

from smcore.data.kline import _call_with_timeout

DEFAULT_AK_TIMEOUT = float(os.getenv("AK_API_TIMEOUT", "30"))


def fetch_data_core(
    api_func: Callable,
    table_name: str,
    *args,
    db_path: str,
    prefer_local: bool = False,
    timeout: Optional[float] = None,
    retries: int = 0,
    retry_backoff: float = 2.0,
    **kwargs,
) -> pd.DataFrame:
    """统一数据获取骨架。

    参数
    ----
    api_func : 真正的 API 获取函数（akshare 等），以 ``api_func(*args, **kwargs)`` 调用。
    table_name : 已 sanitize 的 SQLite 表名（由各调用方按现状自行计算后传入）。
    db_path : SQLite 库路径（各调用方传入其当前使用的路径）。
    prefer_local : ``False``=API 优先（boll/cctv）；``True``=本地优先（relativity）。
    timeout : API 调用包裹超时（秒），``None`` 表示不超时。
    retries : API 瞬断重试次数（捕获 OSError/ConnectionError/socket.timeout）。
    retry_backoff : 重试退避基数（秒）。
    """
    try:
        import socket

        transient = (OSError, ConnectionError, socket.timeout)
    except Exception:
        transient = (OSError, ConnectionError)

    def _invoke():
        if timeout is not None:
            return _call_with_timeout(lambda: api_func(*args, **kwargs), timeout)
        return api_func(*args, **kwargs)

    def _call_api():
        # retries<=0：任何异常都吞掉返回 None，交由上层回退
        if retries <= 0:
            try:
                return _invoke()
            except Exception:
                return None
        # retries>0：仅瞬断异常重试；非瞬断异常向上抛出（由调用方外层兜底）
        for attempt in range(retries + 1):
            try:
                return _invoke()
            except transient as exc:
                if attempt < retries:
                    print(f"API连接异常，{retry_backoff}s 后重试 ({attempt + 1}/{retries}): {exc}")
                    time.sleep(retry_backoff * (attempt + 1))
                    continue
                print(f"API重试 {retries} 次仍失败: {exc}")
                return None
            except Exception:
                raise
        return None

    conn = sqlite3.connect(db_path)
    try:
        if prefer_local:
            # 本地优先：本地命中立即返回；缺失才调 API（带瞬断重试）
            try:
                df_local = pd.read_sql(f"SELECT * FROM {table_name}", conn)
                if not df_local.empty:
                    print(f"成功读取本地数据库表: {table_name}")
                    return df_local
                print(f"本地数据库表为空: {table_name}")
            except Exception:
                print(f"本地数据库缺少表: {table_name}")
            try:
                df = _call_api()
            except Exception as exc:
                print(f"API调用失败: {table_name} | {exc}")
                return pd.DataFrame()
            if isinstance(df, pd.DataFrame) and not df.empty:
                df.to_sql(table_name, conn, if_exists="replace", index=False)
                print(f"API调用成功。数据已保存至数据库表: {table_name}")
                return df
            print(f"API返回空数据: {table_name}")
            return pd.DataFrame()

        # API 优先：API 成功写回并立即返回；失败/空再读本地
        try:
            df = _call_api()
        except Exception as exc:
            print(f"API调用失败: {table_name} | {exc}")
            df = None
        if isinstance(df, pd.DataFrame) and not df.empty:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"API调用成功。数据已保存至数据库表: {table_name}")
            return df
        print(f"API返回空数据或调用失败: {table_name}，尝试回退本地数据库")
        try:
            df_local = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
            if not df_local.empty:
                print(f"回退成功，读取本地数据库表: {table_name}")
                return df_local
            print(f"本地数据库表为空: {table_name}")
        except Exception:
            print(f"本地数据库缺少表: {table_name}")
        return pd.DataFrame()
    finally:
        conn.close()
