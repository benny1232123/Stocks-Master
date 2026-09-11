"""离线单测：API 鉴权核心 _check_api_key（与 FastAPI 解耦，仅依赖标准库）。

安全默认（2026-09-12）：未配置 token 时仅放行本机回环，公网拒绝；
配置后正确 key 放行、缺失/错误/空 key 拒绝；timing-safe 比较契约。
"""
from __future__ import annotations

import os
from unittest import mock

import pytest

from backend.api_auth import ApiKeyError, _check_api_key


@pytest.fixture(autouse=True)
def _no_auth_token(monkeypatch):
    """默认每个用例都不配置 API_AUTH_TOKEN，隔离环境变量影响。"""
    monkeypatch.delenv("API_AUTH_TOKEN", raising=False)
    yield


def test_no_token_allows_loopback():
    # 未配置 token：本机回环放行（本地开发不受影响）
    _check_api_key(None, client_host="127.0.0.1")
    _check_api_key("whatever", client_host="::1")
    _check_api_key(None, client_host="localhost")


def test_no_token_rejects_public():
    # 未配置 token：公网/代理来源一律拒绝（安全默认，防公网裸奔）
    with pytest.raises(ApiKeyError):
        _check_api_key(None, client_host="10.2.3.4")
    with pytest.raises(ApiKeyError):
        _check_api_key("whatever", client_host="172.16.0.9")


def test_no_token_rejects_when_host_unknown():
    # 拿不到 client host（如某些代理配置）时按公网处理
    with pytest.raises(ApiKeyError):
        _check_api_key(None, client_host="")


def test_token_set_correct_key_passes(monkeypatch):
    monkeypatch.setenv("API_AUTH_TOKEN", "secret-token")
    _check_api_key("secret-token", client_host="10.0.0.1")


def test_token_set_missing_key_rejected(monkeypatch):
    monkeypatch.setenv("API_AUTH_TOKEN", "secret-token")
    with pytest.raises(ApiKeyError):
        _check_api_key(None, client_host="127.0.0.1")


def test_token_set_empty_key_rejected(monkeypatch):
    monkeypatch.setenv("API_AUTH_TOKEN", "secret-token")
    with pytest.raises(ApiKeyError):
        _check_api_key("", client_host="127.0.0.1")


def test_token_set_wrong_key_rejected(monkeypatch):
    monkeypatch.setenv("API_AUTH_TOKEN", "secret-token")
    with pytest.raises(ApiKeyError):
        _check_api_key("wrong-key", client_host="127.0.0.1")


def test_token_is_stripped_of_surrounding_whitespace(monkeypatch):
    monkeypatch.setenv("API_AUTH_TOKEN", "  secret-token  ")
    _check_api_key("secret-token", client_host="10.0.0.1")


def test_non_ascii_key_rejected_not_500(monkeypatch):
    # 非 ASCII key 不得触发 TypeError（旧版 compare_digest(str) 会 500）
    monkeypatch.setenv("API_AUTH_TOKEN", "secret-token")
    with pytest.raises(ApiKeyError):
        _check_api_key("密钥", client_host="10.0.0.1")


def test_timing_safe_compare_used(monkeypatch):
    # 契约：比较必须走 hmac.compare_digest（防时序侧信道）
    monkeypatch.setenv("API_AUTH_TOKEN", "secret-token")
    with mock.patch("backend.api_auth.hmac.compare_digest", wraps=__import__("hmac").compare_digest) as spy:
        _check_api_key("secret-token", client_host="10.0.0.1")
    assert spy.called
