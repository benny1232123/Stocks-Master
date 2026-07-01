"""Streamlit 可视化入口 — 同时支持本地运行和 Streamlit Cloud 部署。

本地运行：  python streamlit_app.py
Cloud 部署入口： Frequently-Used-Program/boll-visualizer/src/app.py
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
APP_SRC = REPO_ROOT / "Frequently-Used-Program" / "boll-visualizer" / "src"

# 把仓库根和 visualizer src 加入 path，使 import 能找到 smcore 和 app
for p in (REPO_ROOT, APP_SRC):
    s = str(p)
    if s not in sys.path:
        sys.path.insert(0, s)

# 直接把 app.py 当成模块跑 —— streamlit 会通过 streamlit run 启动，
# 这里只是确保 import 路径正确。实际运行方式：
#   本地：python -m streamlit run streamlit_app.py
#   Cloud：入口设为 Frequently-Used-Program/boll-visualizer/src/app.py
#
# 为了让 python streamlit_app.py 也能直接启动，用 subprocess 调 streamlit：
if __name__ == "__main__":
    import subprocess

    cmd = [
        sys.executable, "-m", "streamlit", "run",
        str(APP_SRC / "app.py"),
        "--server.port", "8520",
        "--server.address", "0.0.0.0",
    ]
    print(f"启动可视化：{' '.join(cmd)}")
    subprocess.run(cmd)
