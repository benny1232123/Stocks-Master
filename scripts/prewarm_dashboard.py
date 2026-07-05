"""Prewarm dashboard caches for the backend and UI."""
from __future__ import annotations

import sys
from pathlib import Path

# 路径设置
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from smcore.dashboard import prewarm_dashboard_cache


def main():
    print("🔥 预热看板缓存...")
    results = prewarm_dashboard_cache(keep_days=7)
    for key, value in results.items():
        print(f"  ✓ {key}: {value}")
    print("✅ 预热完成")


if __name__ == "__main__":
    main()
