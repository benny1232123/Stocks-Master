from pathlib import Path
import importlib
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

_backtester = importlib.import_module("core.backtester")
backtest_boll_signals = _backtester.backtest_boll_signals
classify_boll_signals = _backtester.classify_boll_signals


def _mock_boll_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=8, freq="D").strftime("%Y-%m-%d"),
            "close": [10.5, 9.0, 10.2, 9.2, 10.4, 10.8, 11.0, 10.9],
            "Lower": [9.5] * 8,
            "Upper": [11.5] * 8,
        }
    )


def test_classify_boll_signals_marks_selected_rows() -> None:
    df = _mock_boll_df()
    out = classify_boll_signals(df, near_ratio=1.015)

    assert "signal_type" in out.columns
    assert "selected" in out.columns
    assert out["selected"].sum() == 2
    assert set(out[out["selected"]]["signal_type"].tolist()) == {"oversold"}


def test_backtest_boll_signals_summary_values() -> None:
    df = _mock_boll_df()
    summary_df, details_df = backtest_boll_signals(df, horizons=(2,), near_ratio=1.015)

    assert not summary_df.empty
    assert len(details_df) == 2

    row = summary_df.iloc[0]
    assert row["持有天数"] == 2
    assert row["信号样本"] == 2
    assert row["有效样本"] == 2
    assert row["胜率(%)"] == 100.0
    assert abs(float(row["平均最大回撤(%)"])) < 1e-6


def test_backtest_boll_signals_handles_insufficient_lookahead() -> None:
    df = _mock_boll_df()
    summary_df, _details_df = backtest_boll_signals(df, horizons=(5,), near_ratio=1.015)

    row = summary_df.iloc[0]
    assert row["信号样本"] == 2
    assert row["有效样本"] == 1


def test_classify_boll_signals_suppress_consecutive_selected() -> None:
    df = pd.DataFrame(
        {
            "date": pd.date_range("2026-01-01", periods=6, freq="D").strftime("%Y-%m-%d"),
            "close": [9.0, 8.9, 8.8, 10.4, 10.6, 10.8],
            "Lower": [9.5] * 6,
            "Upper": [11.5] * 6,
        }
    )

    deduped = classify_boll_signals(df)
    raw = classify_boll_signals(df, suppress_consecutive_selected=False)

    assert int(deduped["selected"].sum()) == 1
    assert int(raw["selected"].sum()) == 3
