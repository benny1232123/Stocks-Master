import sys
from pathlib import Path

# 让 tests/ 既能 import smcore（项目根），也能 import daily_backtest（scripts/ 非包）
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))
