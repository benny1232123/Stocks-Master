"""SQLite 缓存层 —— 全项目统一缓存读写。

此前巨石 auto_notify_boll.py 自带 _cache_table_name/_read_cache_df/_write_cache_df，
visualizer 侧另有文件缓存实现，形成双轨。本模块统一为单一 SQLite 缓存入口，
两条主线共用（任务5：统一缓存）。

表名规则：从缓存键生成合法 SQLite 表名（仅字母数字下划线，数字开头补 t_ 前缀）。
"""
from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pandas as pd

from .config.defaults import STOCK_DATA_DIR

DB_PATH: Path = STOCK_DATA_DIR / "stocks_data.db"


def cache_table_name(cache_key: str) -> str:
    """从缓存键生成合法 SQLite 表名。"""
    key = cache_key.replace("stock_data/", "").replace(".csv", "")
    key = re.sub(r"[^0-9a-zA-Z_]+", "_", key)
    key = re.sub(r"_+", "_", key).strip("_")
    if not key:
        key = "table"
    if key[0].isdigit():
        key = f"t_{key}"
    return key


def read_cache_df(table_name: str) -> pd.DataFrame:
    """读缓存表；表不存在或出错返回空 DataFrame。"""
    if not DB_PATH.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def write_cache_df(table_name: str, df: pd.DataFrame) -> None:
    """写缓存表（replace 模式）；空 DataFrame 静默跳过。"""
    if df is None or df.empty:
        return
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    except Exception:
        pass
    finally:
        conn.close()


def clear_cache_by_prefix(prefix: str) -> int:
    """删除表名以指定前缀开头的所有缓存表，返回删除数量。"""
    if not DB_PATH.exists():
        return 0
    conn = sqlite3.connect(DB_PATH)
    deleted = 0
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?",
            (f"{prefix}%",),
        )
        for (name,) in cur.fetchall():
            cur.execute(f'DROP TABLE IF EXISTS "{name}"')
            deleted += 1
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()
    return deleted
