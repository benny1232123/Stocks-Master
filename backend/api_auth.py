"""API 鉴权核心逻辑（与 FastAPI 解耦，便于离线单测）。

安全默认（2026-09-12 反转，旧版"未配置即全放行"在公网部署下等于写接口裸奔）：
- ``API_AUTH_TOKEN`` 已配置 → 必须提供且与 token 完全一致（timing-safe 比较）。
- 未配置 → 仅放行本机回环（本地开发不受影响），公网/代理请求一律拒绝。
"""
from __future__ import annotations

import hmac
import os

# 本机回环：token 未配置时唯一放行的来源
_LOCAL_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


class ApiKeyError(ValueError):
    """未授权：缺少或错误的 API key（或未配置 token 且来源非本机）。"""


def _check_api_key(x_api_key: str | None, *, client_host: str = "") -> None:
    """校验请求携带的 API key。

    - ``API_AUTH_TOKEN`` 已配置 → key 必须与 token 完全一致，否则抛 ``ApiKeyError``。
    - 未配置 → ``client_host`` 为本机回环时放行（本地开发），否则拒绝并提示配置方法。
    """
    token = os.getenv("API_AUTH_TOKEN", "").strip()
    if token:
        # encode 后比较：compare_digest 对含非 ASCII 的 str 会抛 TypeError（→ 500 而非 401）
        if not x_api_key or not hmac.compare_digest(
            str(x_api_key).encode("utf-8"), token.encode("utf-8")
        ):
            raise ApiKeyError("Invalid or missing API key")
        return
    if client_host in _LOCAL_HOSTS:
        return
    raise ApiKeyError(
        "API_AUTH_TOKEN not configured: public access to write/heavy endpoints is denied "
        "(localhost allowed). Set API_AUTH_TOKEN and send X-API-Key to enable remote access."
    )
