from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
APP_SRC = REPO_ROOT / "Frequently-Used-Program" / "boll-visualizer" / "src"

# 仓库根加入 path，使 smcore 共享内核可被 visualizer 引用
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(APP_SRC) not in sys.path:
    sys.path.insert(0, str(APP_SRC))

from app import main


if __name__ == "__main__":
    main()
