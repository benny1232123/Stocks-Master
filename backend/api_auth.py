"""API 鉴权核心逻辑（与 FastAPI 解耦，便于离线单测）。

仅当环境变量 ``API_AUTH_TOKEN`` 非空时启用：调用方必须提供与 token 完全一致的
key，否则视为未授权。token 未配置时向后兼容（不鉴权），与旧部署行为一致。
"""
from __future__ import annotations

import hmac
import os


class ApiKeyError(ValueError):
    """未授权：缺少或错误的 API key。"""


def _check_api_key(x_api_key: str | None) -> None:
    """校验请求携带的 API key。

    - ``API_AUTH_TOKEN`` 未配置 → 放行（向后兼容）。
    - 已配置 → 必须提供且与 token 完全一致（timing-safe 比较），否则抛 ``ApiKeyError``。
    """
    token = os.getenv("API_AUTH_TOKEN", "").strip()
    if not token:
        return
    if not x_api_key or not hmac.compare_digest(x_api_key, token):
        raise ApiKeyError("Invalid or missing API key")
