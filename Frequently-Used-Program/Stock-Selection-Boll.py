# auto-boll 多因子选股的真实逻辑已迁入 smcore/strategies/boll_selection.py。
# 本文件仅作为兼容入口：auto_notify_boll.py 与 daily-pick.yml 仍调 `python Stock-Selection-Boll.py`，
# 实际执行委托 smcore 统一实现，避免逻辑分散、保证单一真相源。
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategies.boll_selection import run_boll_selection

if __name__ == "__main__":
    run_boll_selection()
