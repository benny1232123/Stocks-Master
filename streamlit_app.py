from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
APP_SRC = REPO_ROOT / "Frequently-Used-Program" / "boll-visualizer" / "src"

if str(APP_SRC) not in sys.path:
    sys.path.insert(0, str(APP_SRC))

from app import main


if __name__ == "__main__":
    main()
