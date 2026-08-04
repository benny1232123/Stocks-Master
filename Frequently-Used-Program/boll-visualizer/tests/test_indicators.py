from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from core.indicators import calc_bollinger, evaluate_boll_signal


def test_calc_bollinger_adds_expected_columns() -> None:
    df = pd.DataFrame({"close": list(range(1, 41))})
    out = calc_bollinger(df, window=20, k=1.645)
    assert {"MA", "STD", "Upper", "Lower"}.issubset(out.columns)
    assert out["MA"].iloc[-1] > 0


def test_evaluate_boll_signal_oversold() -> None:
    df = pd.DataFrame([{"close": 9.0, "Lower": 10.0, "Upper": 12.0}])
    result = evaluate_boll_signal(df)
    assert result["signal_type"] == "oversold"
    assert result["selected"] is True


def test_evaluate_boll_signal_insufficient() -> None:
    df = pd.DataFrame([{"close": 10.0, "Lower": None, "Upper": 12.0}])
    result = evaluate_boll_signal(df)
    assert result["signal_type"] == "insufficient"
    assert result["selected"] is False


def test_evaluate_boll_signal_suppress_continuous_oversold() -> None:
    df = pd.DataFrame(
        [
            {"close": 9.0, "Lower": 10.0, "Upper": 12.0},
            {"close": 8.8, "Lower": 10.0, "Upper": 12.0},
        ]
    )
    result = evaluate_boll_signal(df)
    assert result["signal_type"] == "oversold_continuous"
    assert result["selected"] is False
