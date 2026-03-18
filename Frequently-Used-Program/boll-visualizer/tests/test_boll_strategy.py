from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import core.boll_strategy as strategy


def _mock_history() -> pd.DataFrame:
    dates = pd.date_range("2026-01-01", periods=40, freq="D").strftime("%Y-%m-%d")
    closes = [10 + i * 0.1 for i in range(39)] + [8.5]
    return pd.DataFrame(
        {
            "date": dates,
            "open": closes,
            "high": [c + 0.3 for c in closes],
            "low": [c - 0.3 for c in closes],
            "close": closes,
            "volume": [1000] * len(closes),
            "amount": [100000] * len(closes),
        }
    )


def test_analyze_stock_returns_summary(monkeypatch) -> None:
    monkeypatch.setattr(strategy, "fetch_daily_k_data", lambda *args, **kwargs: _mock_history())

    chart_df, summary = strategy.analyze_stock(
        code="600000",
        start_date="2026-01-01",
        end_date="2026-03-01",
        window=20,
        k=1.645,
        near_ratio=1.015,
        adjust="qfq",
        stock_name="浦发银行",
    )

    assert not chart_df.empty
    assert summary["股票代码"] == "600000"
    assert "信号" in summary
    assert "命中策略" in summary


def test_analyze_stock_handles_empty(monkeypatch) -> None:
    monkeypatch.setattr(strategy, "fetch_daily_k_data", lambda *args, **kwargs: pd.DataFrame())

    chart_df, summary = strategy.analyze_stock(
        code="000001",
        start_date="2026-01-01",
        end_date="2026-03-01",
    )

    assert chart_df.empty
    assert summary["信号"] == "无数据"
    assert summary["命中策略"] is False
