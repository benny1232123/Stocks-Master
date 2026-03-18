from datetime import date
from pathlib import Path
import math
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from core.data_fetcher import infer_report_period, parse_amount_text, previous_report_period


def test_parse_amount_text_units() -> None:
    assert parse_amount_text("3.14亿") == 314000000.0
    assert parse_amount_text("-18.84万") == -188400.0
    assert math.isnan(parse_amount_text("-"))


def test_infer_report_period() -> None:
    assert infer_report_period(date(2026, 3, 4)) == (2025, 3)
    assert infer_report_period(date(2026, 6, 1)) == (2026, 1)
    assert infer_report_period(date(2026, 9, 15)) == (2026, 2)
    assert infer_report_period(date(2026, 12, 1)) == (2026, 3)


def test_previous_report_period() -> None:
    assert previous_report_period(2026, 3) == (2026, 2)
    assert previous_report_period(2026, 1) == (2025, 4)
