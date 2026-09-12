"""回归守卫：看板首屏依赖的 GET 端点必须返回 JSON（而非 SPA 兜底的 index.html）。

背景（2026-09-12 事故）：/api/portfolio 的路由装饰器丢失后，请求落入
spa_fallback 返回 index.html（200 + HTML），前端 p.json() 解析失败 →
每次看板加载都报「后端未启动」——服务在线、数据正常，唯独这一个端点坏，
且无任何服务端报错，持续数日无人察觉。本测试确保所有前端依赖的
GET 端点永远返回 JSON content-type。
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient  # noqa: E402

from backend.main import app  # noqa: E402

# 前端 loadDashboard 及概览页依赖的读端点（全部应为 JSON）
DASHBOARD_ENDPOINTS = [
    "/api/dashboard",
    "/api/artifacts/daily-action-list",
    "/api/artifacts/daily-action-list/full",
    "/api/backtests/latest",
    "/api/portfolio",
    "/api/backtests/daily-summary",
]


def test_dashboard_endpoints_return_json():
    client = TestClient(app)
    for path in DASHBOARD_ENDPOINTS:
        r = client.get(path)
        ct = r.headers.get("content-type", "")
        assert r.status_code == 200, f"{path} → HTTP {r.status_code}"
        assert "application/json" in ct, (
            f"{path} 返回了非 JSON（content-type={ct!r}）——路由装饰器可能丢失，"
            f"请求落入了 SPA 兜底返回 index.html"
        )
        # JSON 且非 HTML 伪装：能解析且首字符不是 '<'
        body = r.text.lstrip()
        assert body.startswith(("{", "[")), f"{path} 响应体不是 JSON 结构"
