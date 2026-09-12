#!/usr/bin/env python3
"""导出看板首屏静态数据快照 → stock_data/web_data/*.json（数据静态化，2026-09-12）。

数据静态化方案的核心：看板首屏的 4 个读端点由每日 CI（或本地/连夜任务）预生成
JSON 快照，后端读端点「快照优先、缺失回退实时」——网站首屏从「跨洋实时拉数据
（分钟级、常超时）」变成「读本地文件（毫秒级）」，且 Render 部署清空文件系统后
快照随仓库恢复。

端点 → 文件映射（与前端 loadDashboard 的四个请求一一对应，前端零改动）：
  /api/dashboard                      → dashboard.json
  /api/artifacts/daily-action-list    → artifacts.json      （path 字段脱敏为文件名）
  /api/artifacts/daily-action-list/full → daily_full.json
  /api/backtests/latest               → backtests_latest.json

用法：python scripts/export_web_data.py
每个端点独立容错（失败跳过，后端自动回退实时路径）。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient  # noqa: E402

from smcore.artifacts import STOCK_DATA_DIR  # noqa: E402

OUT_DIR = STOCK_DATA_DIR / "web_data"

# (端点, 输出文件名, 是否脱敏 path 字段为文件名)
ENDPOINTS = [
    ("/api/dashboard", "dashboard.json", False),
    ("/api/artifacts/daily-action-list", "artifacts.json", True),
    ("/api/artifacts/daily-action-list/full", "daily_full.json", False),
    ("/api/backtests/latest", "backtests_latest.json", False),
    ("/api/portfolio", "portfolio.json", False),
    ("/api/backtests/daily-summary", "daily_summary.json", False),
]


def _sanitize(obj):
    """artifacts.latest.path 是生成机器上的绝对路径——替换为纯文件名（前端只用 name）。"""
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k == "path" and isinstance(v, str):
                out[k] = Path(v).name
            else:
                out[k] = _sanitize(v)
        return out
    if isinstance(obj, list):
        return [_sanitize(x) for x in obj]
    return obj


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    from backend.main import app

    client = TestClient(app)
    ok = 0
    for path, name, sanitize in ENDPOINTS:
        try:
            r = client.get(path)
            if r.status_code != 200:
                print(f"[export] 跳过 {path}（HTTP {r.status_code}）")
                continue
            data = r.json()
            if sanitize:
                data = _sanitize(data)
            (OUT_DIR / name).write_text(
                json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8"
            )
            ok += 1
            print(f"[export] {path} → {name}（{len(json.dumps(data)) // 1024}KB）")
        except Exception as exc:
            print(f"[export] WARN: {path} 导出失败（{exc!r}），后端将回退实时路径")
    print(f"[export] 完成 {ok}/{len(ENDPOINTS)} → {OUT_DIR.relative_to(ROOT)}")
    return 0 if ok else 1


if __name__ == "__main__":
    import json

    sys.exit(main())
