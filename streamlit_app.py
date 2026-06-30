"""Streamlit 可视化入口 — 正确方式：用 subprocess 启动 streamlit。

直接用 `python streamlit_app.py` 运行即可，
脚本会自动找到 app.py 并启动 streamlit 服务。
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
APP_PY = REPO_ROOT / "Frequently-Used-Program" / "boll-visualizer" / "src" / "app.py"

if not APP_PY.exists():
    print(f"错误：找不到 {APP_PY}")
    sys.exit(1)

cmd = [
    sys.executable, "-m", "streamlit", "run",
    str(APP_PY),
    "--server.port", "8520",
    "--server.address", "0.0.0.0",
]
print(f"启动可视化：{' '.join(cmd)}")
subprocess.run(cmd)
