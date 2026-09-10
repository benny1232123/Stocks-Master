import sys
import pytest
import pandas as pd
from datetime import date

sys.path.insert(0, ".")
from smcore.backtest.continuous import build_continuous_curve, compute_metrics


def test_build_continuous_curve_basic():
    sleeves = {
        "a": {date(2026, 1, 5): 1000.0, date(2026, 1, 6): 1100.0},
        "b": {date(2026, 1, 5): 1000.0, date(2026, 1, 6): 900.0},
    }
    s = build_continuous_curve(sleeves, normalize=False)
    assert s.index.is_monotonic_increasing
    assert s.index[0].isoformat() == "2026-01-05"
    assert s.loc[date(2026, 1, 6)] == pytest.approx(1000.0)

    nav = build_continuous_curve(sleeves, normalize=True)
    assert nav.iloc[0] == pytest.approx(1.0)
    assert nav.iloc[-1] == pytest.approx(1.0)


def test_compute_metrics_basic():
    nav = pd.Series([1.0, 1.1, 0.99], index=pd.date_range("2026-01-01", periods=3))
    m = compute_metrics(nav)
    assert m["total_return_pct"] == pytest.approx(-1.0)
    assert m["max_drawdown_pct"] < 0
    assert m["win_rate_pct"] == pytest.approx(50.0)
    assert m["n_days"] == 2


def test_compute_metrics_insufficient():
    m = compute_metrics(pd.Series([1.0]))
    assert m == {"error": "insufficient_data"}