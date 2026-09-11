"""API 鉴权核心逻辑（与 FastAPI 解耦，便于离线单测）。

安全默认（2026-09-12 反转，旧版"未配置即全放行"在公网部署下等于写接口裸奔）：
- ``API_AUTH_TOKEN`` 已配置 → 必须提供且与 token 完全一致（timing-safe 比较）。
- 未配置 → 仅放行本机回环（本地开发不受影响），公网/代理请求一律拒绝。
"""
from __future__ import annotations

import hmac
import os
from urllib.parse import urlparse

# 本机回环：token 未配置时唯一放行的来源
_LOCAL_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


class ApiKeyError(ValueError):
    """未授权：缺少或错误的 API key（或未配置 token 且来源非本机）。"""


def _same_origin(origin: str, referer: str, host: str) -> bool:
    """浏览器同源判定：Origin/Referer 的 host 与请求 Host 一致。

    浏览器对所有 POST 自动携带 Origin（同源亦然），无法被跨站页面伪造；
    curl 等非浏览器请求通常无 Origin/Referer → 按跨源处理（需 API key 或本机）。
    """
    for source in (origin, referer):
        if source:
            try:
                if urlparse(source).netloc and urlparse(source).netloc == host:
                    return True
            except Exception:
                continue
    return False


def _check_api_key(x_api_key: str | None, *, client_host: str = "",
                   origin: str = "", referer: str = "", host: str = "") -> None:
    """校验请求携带的 API key。

    - ``API_AUTH_TOKEN`` 已配置 → key 必须与 token 完全一致，否则抛 ``ApiKeyError``。
    - 未配置 → 放行三类请求：本机回环（本地开发）、**同源浏览器请求**（看板 UI 的
      POST 自动带同源 Origin，主人远程使用不受影响）、其余一律拒绝（跨站攻击页
      的 Origin 必然 ≠ 站点 Host；脚本无 Origin/Referer 同样被拒）。
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
    if _same_origin(origin, referer, host):
        return
    raise ApiKeyError(
        "API_AUTH_TOKEN not configured: cross-origin/script access to write endpoints is denied "
        "(same-origin dashboard and localhost allowed). Set API_AUTH_TOKEN + X-API-Key for API access."
    )
