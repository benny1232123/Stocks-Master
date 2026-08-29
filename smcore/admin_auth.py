"""管理员鉴权：密码登录 + HMAC 签名令牌。

安全模型
--------
- 只认环境变量 ``ADMIN_PASSWORD``。**未设置时管理端直接禁用**（安全默认），
  避免部署到公网后「没密码也能进」。
- 登录成功后签发 **HMAC-SHA256 签名令牌**（含过期时间），后续请求带
  ``X-Admin-Token`` 头。令牌**无状态**，不写服务端存储，Restart 仍有效。
- 签名密钥由 ``ADMIN_PASSWORD`` 派生 —— 改密码会立即使旧令牌失效。
- 密码比对与签名比对均用 **timing-safe** 比较，防时序侧信道。
- 连续失败登录会被**按 IP 限流**，防暴力破解。

不引入任何第三方依赖（只用标准库 hmac / hashlib / base64 / json / time）。
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import threading
import time
from typing import Any

#: 令牌默认有效期（秒）—— 8 小时，够一天内反复操作
DEFAULT_TTL = 8 * 3600

#: 限流窗口与阈值
_LOCK_WINDOW = 900.0  # 15 分钟
_MAX_FAILS = 5

_lock = threading.Lock()
_failed: dict[str, list[float]] = {}


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_decode(text: str) -> bytes:
    pad = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + pad)


def admin_password() -> str:
    """已配置的管理员密码（未配置返回空串）。"""
    return os.getenv("ADMIN_PASSWORD", "").strip()


def is_admin_enabled() -> bool:
    """管理端是否启用（= 是否配置了 ADMIN_PASSWORD）。"""
    return bool(admin_password())


def _signing_key() -> bytes:
    """由 ADMIN_PASSWORD（+ 可选 ADMIN_TOKEN_SECRET）派生签名密钥。"""
    material = f"{admin_password()}|{os.getenv('ADMIN_TOKEN_SECRET', '').strip()}"
    return hashlib.sha256(material.encode("utf-8")).digest()


# --------------------------------------------------------------------------
# 限流
# --------------------------------------------------------------------------
def _prune(now: float) -> None:
    for ip in list(_failed.keys()):
        hits = [t for t in _failed[ip] if now - t < _LOCK_WINDOW]
        if hits:
            _failed[ip] = hits
        else:
            del _failed[ip]


def is_locked_out(client_ip: str) -> bool:
    """该 IP 是否因连续失败被临时锁定。"""
    now = time.time()
    with _lock:
        _prune(now)
        return len(_failed.get(client_ip or "unknown", [])) >= _MAX_FAILS


def _record_failure(client_ip: str) -> None:
    now = time.time()
    with _lock:
        _prune(now)
        _failed.setdefault(client_ip or "unknown", []).append(now)


def _clear_failures(client_ip: str) -> None:
    with _lock:
        _failed.pop(client_ip or "unknown", None)


# --------------------------------------------------------------------------
# 密码 / 令牌
# --------------------------------------------------------------------------
def check_password(candidate: str, client_ip: str = "unknown") -> bool:
    """校验密码（timing-safe），并做失败限流。

    返回 True 表示通过。管理端未启用时恒为 False。
    """
    if not is_admin_enabled():
        return False
    if is_locked_out(client_ip):
        return False

    ok = hmac.compare_digest(str(candidate or "").encode("utf-8"), admin_password().encode("utf-8"))
    if ok:
        _clear_failures(client_ip)
    else:
        _record_failure(client_ip)
    return ok


def issue_token(ttl_seconds: int = DEFAULT_TTL) -> str:
    """签发一个有过期时间的签名令牌。"""
    now = int(time.time())
    payload = {"iat": now, "exp": now + int(ttl_seconds)}
    body = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    sig = _b64url(hmac.new(_signing_key(), body.encode("ascii"), hashlib.sha256).digest())
    return f"{body}.{sig}"


def verify_token(token: str | None) -> bool:
    """校验令牌签名与有效期。管理端未启用时恒为 False。"""
    if not is_admin_enabled() or not token:
        return False
    try:
        body, sig = str(token).rsplit(".", 1)
    except ValueError:
        return False

    expected = _b64url(hmac.new(_signing_key(), body.encode("ascii"), hashlib.sha256).digest())
    if not hmac.compare_digest(sig, expected):
        return False

    try:
        payload: dict[str, Any] = json.loads(_b64url_decode(body))
        exp = int(payload.get("exp", 0))
    except Exception:
        return False
    return exp > int(time.time())


def auth_status() -> dict[str, Any]:
    """供前端判断「管理端是否可用」（不泄露密码本身）。"""
    enabled = is_admin_enabled()
    return {
        "enabled": enabled,
        "api_auth_token_set": bool(os.getenv("API_AUTH_TOKEN", "").strip()),
        "message": (
            "管理端已启用"
            if enabled
            else "未设置 ADMIN_PASSWORD，管理端已禁用（安全默认）"
        ),
    }
