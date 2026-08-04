"""每日数据持久化缓存。

一天只跑一次数据获取，结果存到 stock_data/daily_cache/ 目录。
当天没跑出来就用前一天的，页面标注实际数据日期。
"""
from __future__ import annotations

import pickle
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Optional, Tuple

import pandas as pd

from smcore.config.defaults import STOCK_DATA_DIR

CACHE_DIR = STOCK_DATA_DIR / "daily_cache"


def get_daily(
    key: str,
    fetch_func: Callable,
    *args,
    **kwargs,
) -> Tuple[Any, Optional[str]]:
    """获取每日缓存数据。

    逻辑：
    1. 今天已跑过 → 读今天的缓存文件（秒级返回）
    2. 今天没跑过 → 尝试跑一次，成功则存文件
    3. 今天跑失败 → 读最近一天的缓存文件

    Args:
        key: 缓存键名（如 "index_snapshot"）
        fetch_func: 数据获取函数
        *args, **kwargs: 传给 fetch_func 的参数

    Returns:
        (data, cache_date): 数据和缓存日期(YYYY-MM-DD)。
        全失败返回 (None, None)。
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    today = date.today().strftime("%Y-%m-%d")
    today_file = CACHE_DIR / f"{key}_{today}.pkl"

    # 1. 今天已缓存 → 直接用
    if today_file.exists():
        try:
            with open(today_file, "rb") as f:
                return pickle.load(f), today
        except Exception:
            pass

    # 2. 今天没跑过 → 尝试跑一次
    try:
        data = fetch_func(*args, **kwargs)
        # 空值（None / 空 dict / 空 list）不缓存，避免整天显示空白
        if data is not None:
            is_empty = (
                (isinstance(data, dict) and not data)
                or (isinstance(data, list) and not data)
                or (isinstance(data, pd.DataFrame) and data.empty)
            )
            if not is_empty:
                with open(today_file, "wb") as f:
                    pickle.dump(data, f)
                return data, today
    except Exception:
        pass

    # 3. 今天跑失败 → 读最近一天的
    caches = sorted(CACHE_DIR.glob(f"{key}_*.pkl"), reverse=True)
    for cache_file in caches:
        cache_date = cache_file.stem.rsplit("_", 1)[-1]
        if cache_date == today:
            continue
        try:
            with open(cache_file, "rb") as f:
                data = pickle.load(f)
            return data, cache_date
        except Exception:
            continue

    return None, None


def force_refresh(key: str) -> bool:
    """删除今天的缓存文件，强制下次重新获取。

    Returns: 是否成功删除（True=有缓存被删除）
    """
    today = date.today().strftime("%Y-%m-%d")
    today_file = CACHE_DIR / f"{key}_{today}.pkl"
    if today_file.exists():
        today_file.unlink()
        return True
    return False


def clean_old_cache(keep_days: int = 7) -> int:
    """清理超过 keep_days 天的旧缓存文件，返回清理数量。"""
    if not CACHE_DIR.exists():
        return 0
    cutoff = datetime.now().timestamp() - keep_days * 86400
    count = 0
    for f in CACHE_DIR.glob("*.pkl"):
        if f.stat().st_mtime < cutoff:
            f.unlink(missing_ok=True)
            count += 1
    return count
