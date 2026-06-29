"""baostock 会话管理 —— 单一登录点。

此前 bs.login()/bs.logout() 散落在 6+ 处（Stock-Selection-Boll / data_fetcher /
boll_strategy / auto_notify_boll...），全市场扫描时每只股票都 login/logout，
极慢且易触发连接限制。本模块提供进程级单例，确保只登录一次、全程复用。
"""
from __future__ import annotations

import atexit
import threading
from contextlib import contextmanager

import baostock as bs

_lock = threading.Lock()
_logged_in = False
_atexit_registered = False


def login() -> bool:
    """登录 baostock，已登录则复用。返回是否处于可用状态。"""
    global _logged_in, _atexit_registered
    with _lock:
        if _logged_in:
            return True
        result = bs.login()
        if result.error_code == "0":
            _logged_in = True
            if not _atexit_registered:
                atexit.register(ensure_logout)
                _atexit_registered = True
            return True
        return False


def ensure_logout() -> None:
    """显式登出（进程退出时自动调用）。"""
    global _logged_in
    with _lock:
        if _logged_in:
            try:
                bs.logout()
            except Exception:
                pass
            _logged_in = False


# 别名
logout = ensure_logout


@contextmanager
def session():
    """获取 baostock 会话上下文。

    进程内复用同一登录态，不随上下文退出登出（全市场扫描提速关键）。
    用法:
        with session() as ok:
            if not ok:
                return ...
            # 调用 bs.query_* 接口
    """
    ok = login()
    yield ok
