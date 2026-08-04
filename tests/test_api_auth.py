"""离线单测：API 鉴权核心 _check_api_key（与 FastAPI 解耦，仅依赖标准库）。

覆盖：未配置 token 时放行；配置后正确 key 放行、缺失/错误/空 key 拒绝；
以及 timing-safe 比较契约（hmac.compare_digest 被调用）。
"""
from __future__ import annotations

import hmac
import os
from unittest import mock

import pytest

from backend.api_auth import ApiKeyError, _check_api_key


@pytest.fixture(autouse=True)
def _no_auth_token(monkeypatch):
    """默认每个用例都不配置 API_AUTH_TOKEN，隔离环境变量影响。"""
    monkeypatch.delenv("API_AUTH_TOKEN", raising=False)
    yield


def test_no_token_allows_none():
    # 未配置 token：即使没带 key 也应放行（向后兼容旧部署）
    _check_api_key(None)  # 不应抛异常


def test_no_token_allows_any_key():
    # 未配置 token：带任意 key 也应放行
    _check_api_key("whatever")  # 不应抛异常


def test_token_set_correct_key_passes(monkeypatch):
    monkeypatch.setenv("API_AUTH_TOKEN", "secret-token")
    _check_api_key("secret-token")  # 不应抛异常


def test_token_set_missing_key_rejected(monkeypatch):
    monkeypatch.setenv("API_AUTH_TOKEN", "secret-token")
    with pytest.raises(ApiKeyError):
        _check_api_key(None)


def test_token_set_empty_key_rejected(monkeypatch):
    monkeypatch.setenv("API_AUTH_TOKEN", "secret-token")
    with pytest.raises(ApiKeyError):
        _check_api_key("")


def test_token_set_wrong_key_rejected(monkeypatch):
    monkeypatch.setenv("API_AUTH_TOKEN", "secret-token")
    with pytest.raises(ApiKeyError):
        _check_api_key("wrong-key")


def test_token_is_stripped_of_surrounding_whitespace(monkeypatch):
    # 配置 "  secret-token  " 时，精确 "secret-token" 应通过（token 被 strip）
    monkeypatch.setenv("API_AUTH_TOKEN", "  secret-token  ")
    _check_api_key("secret-token")  # 不应抛异常


def test_timing_safe_compare_is_used(monkeypatch):
    # 锁定「timing-safe 比较」契约：必须走 hmac.compare_digest，而非 == 短路比较
    monkeypatch.setenv("API_AUTH_TOKEN", "secret-token")
    captured = {}

    real_cd = hmac.compare_digest

    def _spy(a, b):
        captured["called"] = True
        captured["args"] = (a, b)
        return real_cd(a, b)

    with mock.patch("backend.api_auth.hmac.compare_digest", _spy):
        _check_api_key("secret-token")  # 正确 key -> 不抛
    assert captured.get("called") is True
    assert captured["args"] == ("secret-token", "secret-token")

    # 错误 key 也应走 compare_digest 后才拒绝
    captured.clear()
    with mock.patch("backend.api_auth.hmac.compare_digest", _spy):
        with pytest.raises(ApiKeyError):
            _check_api_key("nope")
    assert captured.get("called") is True
