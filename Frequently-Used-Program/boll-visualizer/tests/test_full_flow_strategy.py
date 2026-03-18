from pathlib import Path
import importlib
import sys
from types import SimpleNamespace

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

strategy = importlib.import_module("core.full_flow_strategy")


def _mock_k_df() -> pd.DataFrame:
    dates = pd.date_range("2026-01-01", periods=40, freq="D").strftime("%Y-%m-%d")
    closes = [10 + i * 0.05 for i in range(39)] + [8.8]
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


def test_retry_fetch_dataframe_retries_on_empty() -> None:
    calls = {"count": 0}

    def _action() -> pd.DataFrame:
        calls["count"] += 1
        if calls["count"] == 1:
            return pd.DataFrame()
        return pd.DataFrame({"x": [1]})

    out = strategy._retry_fetch_dataframe(
        _action,
        max_retries=2,
        backoff_seconds=0.0,
        rate_limiter=None,
    )

    assert calls["count"] == 2
    assert not out.empty


def test_analyze_stocks_full_flow_parallel_and_retry(monkeypatch) -> None:
    codes = ["600000", "600001"]

    monkeypatch.setattr(strategy.bs, "login", lambda: SimpleNamespace(error_code="0", error_msg=""))
    monkeypatch.setattr(strategy.bs, "logout", lambda: None)

    monkeypatch.setattr(
        strategy,
        "fetch_code_name_map",
        lambda *args, **kwargs: {"600000": "A", "600001": "B"},
    )

    def _fake_fund_flow_union(*args, **kwargs):
        periods = kwargs.get("periods", ("3日排行", "5日排行", "10日排行"))
        return set(codes), {period: set(codes) for period in periods}

    monkeypatch.setattr(strategy, "_fetch_fund_flow_union", _fake_fund_flow_union)

    monkeypatch.setattr(
        strategy,
        "_evaluate_fundamental",
        lambda *args, **kwargs: {
            "fundamental_pass": True,
            "debt_ratio_percent": 45.0,
            "debt_pass": True,
            "net_profit": 1.0,
            "profit_pass": True,
            "cfo_to_np": 1.1,
            "cash_pass": True,
            "forecast_eps_mean": 0.8,
            "yoy_ni": 0.2,
            "forecast_pass": True,
        },
    )

    monkeypatch.setattr(strategy, "_check_important_shareholder", lambda *args, **kwargs: (True, "ok"))
    monkeypatch.setattr(
        strategy,
        "evaluate_boll_signal",
        lambda *args, **kwargs: {"signal": "超卖：收盘价低于下轨", "selected": True, "signal_type": "oversold"},
    )

    attempts: dict[str, int] = {}

    def _fake_fetch_daily_k_data(code: str, *args, **kwargs) -> pd.DataFrame:
        attempts[code] = attempts.get(code, 0) + 1
        if code == "600000" and attempts[code] == 1:
            raise RuntimeError("temporary network issue")
        return _mock_k_df()

    monkeypatch.setattr(strategy, "fetch_daily_k_data", _fake_fetch_daily_k_data)

    result_df, data_map, flow_stats = strategy.analyze_stocks_full_flow(
        codes=codes,
        start_date="2026-01-01",
        end_date="2026-03-01",
        max_workers=2,
        max_retries=1,
        retry_backoff_seconds=0.0,
        request_interval_seconds=0.0,
    )

    assert len(result_df) == 2
    assert attempts["600000"] == 2
    assert set(data_map.keys()) == set(codes)
    assert int(flow_stats.get("Boll命中", 0)) == 2
    assert result_df["命中策略"].astype(bool).all()
