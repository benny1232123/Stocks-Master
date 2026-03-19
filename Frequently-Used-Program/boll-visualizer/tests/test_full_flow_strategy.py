from pathlib import Path
import importlib
import sys
import time
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


def test_analyze_stocks_full_flow_fast_mode_skips_non_flow_fundamental(monkeypatch) -> None:
    codes = ["600000", "600001", "600002"]

    monkeypatch.setattr(strategy.bs, "login", lambda: SimpleNamespace(error_code="0", error_msg=""))
    monkeypatch.setattr(strategy.bs, "logout", lambda: None)

    monkeypatch.setattr(
        strategy,
        "fetch_code_name_map",
        lambda *args, **kwargs: {"600000": "A", "600001": "B", "600002": "C"},
    )

    monkeypatch.setattr(
        strategy,
        "_fetch_fund_flow_union",
        lambda *args, **kwargs: ({"600000"}, {"3日排行": {"600000"}, "5日排行": set(), "10日排行": set()}),
    )

    calls: list[tuple[str, bool]] = []

    def _fake_fundamental(*args, **kwargs):
        code = str(kwargs.get("code", ""))
        calls.append((code, bool(kwargs.get("use_profit_forecast", True))))
        return {
            "fundamental_pass": True,
            "debt_ratio_percent": 40.0,
            "debt_pass": True,
            "net_profit": 1.0,
            "profit_pass": True,
            "cfo_to_np": 1.2,
            "cash_pass": True,
            "forecast_eps_mean": None,
            "yoy_ni": 1.0,
            "forecast_pass": True,
        }

    monkeypatch.setattr(strategy, "_evaluate_fundamental", _fake_fundamental)
    monkeypatch.setattr(strategy, "_check_important_shareholder", lambda *args, **kwargs: (True, "ok"))
    monkeypatch.setattr(
        strategy,
        "evaluate_boll_signal",
        lambda *args, **kwargs: {"signal": "中性", "selected": False, "signal_type": "neutral"},
    )
    monkeypatch.setattr(strategy, "fetch_daily_k_data", lambda *args, **kwargs: _mock_k_df())

    result_df, _data_map, _flow_stats = strategy.analyze_stocks_full_flow(
        codes=codes,
        start_date="2026-01-01",
        end_date="2026-03-01",
        max_workers=2,
        max_retries=0,
        retry_backoff_seconds=0.0,
        request_interval_seconds=0.0,
        fast_mode=True,
    )

    assert calls == [("600000", False)]
    assert len(result_df) == 3

    row_non_flow = result_df[result_df["股票代码"].astype(str) == "600001"].iloc[0]
    assert not bool(row_non_flow["资金流通过"])
    assert not bool(row_non_flow["前置汇合通过"])


def test_evaluate_boll_candidates_parallel_timeout(monkeypatch) -> None:
    codes = ["600000", "600001"]

    monkeypatch.setattr(strategy, "BOLL_STAGE_TIMEOUT_MIN_SECONDS", 0.01)
    monkeypatch.setattr(strategy, "BOLL_STAGE_TIMEOUT_PER_CODE_SECONDS", 0.01)

    def _fake_eval(code: str, *args, **kwargs):
        if code == "600001":
            time.sleep(0.2)
        return (
            code,
            pd.DataFrame(),
            {
                "signal": "测试",
                "signal_type": "neutral",
                "selected": False,
                "latest_close": None,
                "latest_lower": None,
                "latest_upper": None,
            },
        )

    monkeypatch.setattr(strategy, "_evaluate_boll_candidate", _fake_eval)

    signal_map, data_map = strategy._evaluate_boll_candidates_parallel(
        codes=codes,
        start_date="2026-01-01",
        end_date="2026-03-01",
        window=20,
        k=1.645,
        near_ratio=1.015,
        adjust="qfq",
        use_cache=True,
        force_refresh=False,
        cache_max_age_hours=24.0,
        max_workers=2,
        max_retries=0,
        retry_backoff_seconds=0.0,
        rate_limiter=None,
        progress_callback=None,
    )

    assert "600000" in signal_map
    assert "600001" in signal_map
    assert signal_map["600001"]["signal_type"] == "fetch_timeout"
    assert data_map == {}
