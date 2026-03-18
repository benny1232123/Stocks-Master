from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from threading import Lock
import time
from typing import Callable, TypeVar

import akshare as ak
import baostock as bs
import pandas as pd

from core.data_fetcher import (
    fetch_code_name_map,
    fetch_daily_k_data,
    fetch_fund_flow_snapshot,
    format_stock_code,
    infer_report_period,
    previous_report_period,
    to_baostock_code,
)
from core.indicators import calc_bollinger, evaluate_boll_signal
from utils.config import DEFAULT_FUND_FLOW_PERIODS, IMPORTANT_SHAREHOLDER_TYPES, IMPORTANT_SHAREHOLDERS


ProgressCallback = Callable[[str, int, int, str], None]
T = TypeVar("T")


class _RateLimiter:
    def __init__(self, request_interval_seconds: float = 0.0) -> None:
        self.interval_seconds = max(0.0, float(request_interval_seconds))
        self._lock = Lock()
        self._next_allowed = 0.0

    def wait(self) -> None:
        if self.interval_seconds <= 0:
            return

        while True:
            with self._lock:
                now = time.monotonic()
                if now >= self._next_allowed:
                    self._next_allowed = now + self.interval_seconds
                    return
                sleep_seconds = self._next_allowed - now
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)


def _retry_action(
    action: Callable[[], T],
    max_retries: int,
    backoff_seconds: float,
    rate_limiter: _RateLimiter | None = None,
) -> T:
    retries = max(0, int(max_retries))
    base_sleep = max(0.0, float(backoff_seconds))
    last_error: Exception | None = None

    for attempt in range(retries + 1):
        try:
            if rate_limiter is not None:
                rate_limiter.wait()
            return action()
        except Exception as error:
            last_error = error
            if attempt >= retries:
                break
            if base_sleep > 0:
                time.sleep(base_sleep * (2**attempt))

    if last_error is not None:
        raise last_error
    raise RuntimeError("重试失败")


def _retry_fetch_dataframe(
    action: Callable[[], pd.DataFrame],
    max_retries: int,
    backoff_seconds: float,
    rate_limiter: _RateLimiter | None = None,
) -> pd.DataFrame:
    retries = max(0, int(max_retries))
    base_sleep = max(0.0, float(backoff_seconds))
    last_df = pd.DataFrame()

    for attempt in range(retries + 1):
        try:
            if rate_limiter is not None:
                rate_limiter.wait()
            current_df = action()
        except Exception:
            current_df = pd.DataFrame()

        if isinstance(current_df, pd.DataFrame):
            last_df = current_df
            if not current_df.empty:
                return current_df

        if attempt >= retries:
            break
        if base_sleep > 0:
            time.sleep(base_sleep * (2**attempt))

    return last_df


def _to_float(raw_value: object) -> float | None:
    if raw_value is None:
        return None
    text = str(raw_value).strip().replace(",", "")
    if not text or text in {"-", "--", "nan", "None"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _result_set_to_df(result_set) -> pd.DataFrame:
    rows: list[list[str]] = []
    while result_set.next():
        rows.append(result_set.get_row_data())
    return pd.DataFrame(rows, columns=result_set.fields)


def _fetch_positive_fund_flow_codes(
    period_symbol: str,
    price_upper_limit: float,
    use_cache: bool,
    force_refresh: bool,
    cache_max_age_hours: float,
    max_retries: int,
    retry_backoff_seconds: float,
    rate_limiter: _RateLimiter | None,
) -> set[str]:
    fund_df = _retry_fetch_dataframe(
        lambda: fetch_fund_flow_snapshot(
            period_symbol=period_symbol,
            use_cache=use_cache,
            force_refresh=force_refresh,
            max_cache_age_hours=cache_max_age_hours,
        ),
        max_retries=max_retries,
        backoff_seconds=retry_backoff_seconds,
        rate_limiter=rate_limiter,
    )
    if fund_df.empty:
        return set()

    filtered = fund_df[(fund_df["net_inflow"] > 0) & (fund_df["latest_price"] < float(price_upper_limit))]
    return set(filtered["code"].dropna().astype(str).tolist())


def _fetch_fund_flow_union(
    price_upper_limit: float,
    periods: tuple[str, ...],
    use_cache: bool,
    force_refresh: bool,
    cache_max_age_hours: float,
    max_workers: int,
    max_retries: int,
    retry_backoff_seconds: float,
    rate_limiter: _RateLimiter | None,
    progress_callback: ProgressCallback | None = None,
) -> tuple[set[str], dict[str, set[str]]]:
    period_code_map: dict[str, set[str]] = {}
    total = len(periods)
    if total == 0:
        return set(), {}

    worker_count = max(1, min(int(max_workers), total))
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                _fetch_positive_fund_flow_codes,
                period_symbol,
                price_upper_limit,
                use_cache,
                force_refresh,
                cache_max_age_hours,
                max_retries,
                retry_backoff_seconds,
                rate_limiter,
            ): period_symbol
            for period_symbol in periods
        }

        for done_count, future in enumerate(as_completed(future_map), start=1):
            period_symbol = future_map[future]
            try:
                period_code_map[period_symbol] = future.result()
            except Exception:
                period_code_map[period_symbol] = set()
            if progress_callback is not None:
                progress_callback("fund_flow", done_count, total, f"资金流榜单处理中：{period_symbol}")

    period_code_map = {period: period_code_map.get(period, set()) for period in periods}

    union_codes: set[str] = set()
    for code_set in period_code_map.values():
        union_codes |= code_set

    return union_codes, period_code_map


def _query_financial_with_fallback(
    code: str,
    query_func,
    periods: list[tuple[int, int]],
    max_retries: int,
    retry_backoff_seconds: float,
    rate_limiter: _RateLimiter | None,
) -> pd.DataFrame:
    bs_code = to_baostock_code(code)
    retries = max(0, int(max_retries))
    base_sleep = max(0.0, float(retry_backoff_seconds))

    for year, quarter in periods:
        result_set = None
        for attempt in range(retries + 1):
            try:
                if rate_limiter is not None:
                    rate_limiter.wait()
                result_set = query_func(code=bs_code, year=year, quarter=quarter)
            except Exception:
                result_set = None

            if result_set is not None and getattr(result_set, "error_code", "1") == "0":
                break
            if attempt >= retries:
                break
            if base_sleep > 0:
                time.sleep(base_sleep * (2**attempt))

        if result_set is None or result_set.error_code != "0":
            continue
        data_frame = _result_set_to_df(result_set)
        if not data_frame.empty:
            return data_frame
    return pd.DataFrame()


def _calc_liability_ratio_percent(balance_df: pd.DataFrame) -> float | None:
    if balance_df.empty or "liabilityToAsset" not in balance_df.columns:
        return None

    ratio_raw = _to_float(balance_df.iloc[-1].get("liabilityToAsset"))
    if ratio_raw is None:
        return None
    if ratio_raw <= 1:
        return ratio_raw * 10000
    return ratio_raw


def _fetch_forecast_eps_mean(
    code: str,
    current_year: int,
    max_retries: int,
    retry_backoff_seconds: float,
    rate_limiter: _RateLimiter | None,
) -> float | None:
    try:
        forecast_df = _retry_action(
            lambda: ak.stock_profit_forecast_ths(symbol=code),
            max_retries=max_retries,
            backoff_seconds=retry_backoff_seconds,
            rate_limiter=rate_limiter,
        )
    except Exception:
        return None

    if not isinstance(forecast_df, pd.DataFrame) or forecast_df.empty or forecast_df.shape[1] < 4:
        return None

    temp = forecast_df.copy()
    temp["forecast_year"] = pd.to_numeric(temp.iloc[:, 0], errors="coerce")
    temp["forecast_eps_mean"] = pd.to_numeric(temp.iloc[:, 3], errors="coerce")
    temp = temp.dropna(subset=["forecast_year", "forecast_eps_mean"]).sort_values("forecast_year")
    if temp.empty:
        return None

    exact_match = temp[temp["forecast_year"] == current_year]
    if not exact_match.empty:
        return float(exact_match.iloc[0]["forecast_eps_mean"])

    future_match = temp[temp["forecast_year"] > current_year]
    if not future_match.empty:
        return float(future_match.iloc[0]["forecast_eps_mean"])

    return float(temp.iloc[-1]["forecast_eps_mean"])


def _evaluate_fundamental(
    code: str,
    periods: list[tuple[int, int]],
    debt_asset_ratio_limit: float,
    current_year: int,
    max_retries: int,
    retry_backoff_seconds: float,
    rate_limiter: _RateLimiter | None,
) -> dict[str, object]:
    balance_df = _query_financial_with_fallback(
        code,
        bs.query_balance_data,
        periods,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        rate_limiter=rate_limiter,
    )
    profit_df = _query_financial_with_fallback(
        code,
        bs.query_profit_data,
        periods,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        rate_limiter=rate_limiter,
    )
    cash_df = _query_financial_with_fallback(
        code,
        bs.query_cash_flow_data,
        periods,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        rate_limiter=rate_limiter,
    )
    growth_df = _query_financial_with_fallback(
        code,
        bs.query_growth_data,
        periods,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        rate_limiter=rate_limiter,
    )

    debt_ratio_percent = _calc_liability_ratio_percent(balance_df)
    debt_pass = debt_ratio_percent is not None and debt_ratio_percent < float(debt_asset_ratio_limit)

    net_profit = None
    if not profit_df.empty and "netProfit" in profit_df.columns:
        net_profit = _to_float(profit_df.iloc[-1].get("netProfit"))
    profit_pass = net_profit is not None and net_profit > 0

    cfo_to_np = None
    cfo_to_or = None
    if not cash_df.empty:
        if "CFOToNP" in cash_df.columns:
            cfo_to_np = _to_float(cash_df.iloc[-1].get("CFOToNP"))
        if "CFOToOR" in cash_df.columns:
            cfo_to_or = _to_float(cash_df.iloc[-1].get("CFOToOR"))
    cash_pass = bool((cfo_to_np is not None and cfo_to_np > 0) or (cfo_to_or is not None and cfo_to_or > 0))

    yoy_ni = None
    if not growth_df.empty and "YOYNI" in growth_df.columns:
        yoy_ni = _to_float(growth_df.iloc[-1].get("YOYNI"))

    forecast_eps_mean = _fetch_forecast_eps_mean(
        code,
        current_year=current_year,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        rate_limiter=rate_limiter,
    )
    forecast_pass = bool(
        (forecast_eps_mean is not None and forecast_eps_mean > 0)
        or (yoy_ni is not None and yoy_ni > 0)
    )

    fundamental_pass = bool(debt_pass and profit_pass and cash_pass and forecast_pass)
    return {
        "fundamental_pass": fundamental_pass,
        "debt_ratio_percent": debt_ratio_percent,
        "debt_pass": debt_pass,
        "net_profit": net_profit,
        "profit_pass": profit_pass,
        "cfo_to_np": cfo_to_np,
        "cash_pass": cash_pass,
        "forecast_eps_mean": forecast_eps_mean,
        "yoy_ni": yoy_ni,
        "forecast_pass": forecast_pass,
    }


def _check_important_shareholder(
    code: str,
    important_shareholders: tuple[str, ...],
    important_holder_types: tuple[str, ...],
    max_retries: int,
    retry_backoff_seconds: float,
    rate_limiter: _RateLimiter | None,
) -> tuple[bool, str]:
    try:
        holder_df = _retry_action(
            lambda: ak.stock_circulate_stock_holder(symbol=code),
            max_retries=max_retries,
            backoff_seconds=retry_backoff_seconds,
            rate_limiter=rate_limiter,
        )
    except Exception:
        return True, "股东接口异常，默认保留"

    if not isinstance(holder_df, pd.DataFrame) or holder_df.empty or holder_df.shape[1] < 7:
        return True, "股东数据为空，默认保留"

    dates = pd.to_datetime(holder_df.iloc[:, 0], errors="coerce")
    if dates.isna().all():
        return True, "股东日期异常，默认保留"

    latest_date = dates.max()
    latest_df = holder_df[dates == latest_date].copy()
    latest_df["rank"] = pd.to_numeric(latest_df.iloc[:, 2], errors="coerce")
    latest_df = latest_df.sort_values("rank").head(5)

    top_names = latest_df.iloc[:, 3].astype(str).tolist()
    top_types = latest_df.iloc[:, 6].astype(str).tolist()

    for important_holder in important_shareholders:
        if any(important_holder in holder_name for holder_name in top_names):
            return True, f"命中重点股东：{important_holder}"

    for holder_type in important_holder_types:
        if any(holder_type in stock_type for stock_type in top_types):
            return True, f"命中股东性质：{holder_type}"

    return False, "未命中重点股东"


def _score_full_flow_result(
    flow_pass: bool,
    debt_pass: bool,
    profit_pass: bool,
    cash_pass: bool,
    forecast_pass: bool,
    shareholder_pass: bool,
    signal_type: str,
    hit: bool,
) -> tuple[int, str, str]:
    score = 0
    reasons: list[str] = []

    if flow_pass:
        score += 20
        reasons.append("资金流通过")
    else:
        reasons.append("资金流未通过")

    if debt_pass:
        score += 10
    else:
        reasons.append("资产负债率未通过")

    if profit_pass:
        score += 15
    else:
        reasons.append("净利润未通过")

    if cash_pass:
        score += 10
    else:
        reasons.append("现金流未通过")

    if forecast_pass:
        score += 10
    else:
        reasons.append("盈利预期未通过")

    if shareholder_pass:
        score += 15
        reasons.append("重点股东加分")
    else:
        reasons.append("重点股东未命中")

    if hit:
        score += 20
        reasons.append("Boll命中")
    elif signal_type == "oversold_continuous":
        score += 4
        reasons.append("连续低于下轨，已抑制重复触发")
    elif signal_type == "near_lower":
        score += 12
        reasons.append("接近下轨")
    elif signal_type == "oversold":
        score += 16
        reasons.append("低于下轨")
    elif signal_type == "neutral":
        score += 6
        reasons.append("布林中性")
    elif signal_type == "near_upper":
        score += 4
        reasons.append("接近上轨")
    elif signal_type == "overbought":
        score += 2
        reasons.append("高于上轨")
    elif signal_type == "insufficient":
        reasons.append("K线样本不足")
    elif signal_type == "empty_k":
        reasons.append("K线数据为空")
    elif signal_type == "fetch_error":
        reasons.append("K线请求失败")
    else:
        reasons.append("未进入Boll")

    score = int(max(0, min(100, score)))
    if score >= 85:
        grade = "A"
        conclusion = "优先关注"
    elif score >= 70:
        grade = "B"
        conclusion = "可持续跟踪"
    elif score >= 55:
        grade = "C"
        conclusion = "中性观察"
    else:
        grade = "D"
        conclusion = "暂不优先"

    return score, grade, f"{conclusion}：{'；'.join(reasons)}"


def _evaluate_boll_candidate(
    code: str,
    start_date: str | date,
    end_date: str | date,
    window: int,
    k: float,
    near_ratio: float,
    adjust: str,
    use_cache: bool,
    force_refresh: bool,
    cache_max_age_hours: float,
    max_retries: int,
    retry_backoff_seconds: float,
    rate_limiter: _RateLimiter | None,
) -> tuple[str, pd.DataFrame, dict[str, object]]:
    k_df = _retry_fetch_dataframe(
        lambda: fetch_daily_k_data(
            code=code,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
            use_cache=use_cache,
            force_refresh=force_refresh,
            max_cache_age_hours=cache_max_age_hours,
        ),
        max_retries=max_retries,
        backoff_seconds=retry_backoff_seconds,
        rate_limiter=rate_limiter,
    )

    if k_df.empty:
        return (
            code,
            pd.DataFrame(),
            {
                "signal": "K线数据为空",
                "signal_type": "empty_k",
                "selected": False,
                "latest_close": None,
                "latest_lower": None,
                "latest_upper": None,
            },
        )

    boll_df = calc_bollinger(k_df, window=window, k=k)
    signal_info = evaluate_boll_signal(boll_df, near_ratio=near_ratio)
    latest = boll_df.iloc[-1]

    return (
        code,
        boll_df,
        {
            "signal": str(signal_info["signal"]),
            "signal_type": str(signal_info.get("signal_type", "unknown")),
            "selected": bool(signal_info["selected"]),
            "latest_close": float(latest["close"]) if pd.notna(latest["close"]) else None,
            "latest_lower": float(latest["Lower"]) if pd.notna(latest["Lower"]) else None,
            "latest_upper": float(latest["Upper"]) if pd.notna(latest["Upper"]) else None,
        },
    )


def _evaluate_boll_candidates_parallel(
    codes: list[str],
    start_date: str | date,
    end_date: str | date,
    window: int,
    k: float,
    near_ratio: float,
    adjust: str,
    use_cache: bool,
    force_refresh: bool,
    cache_max_age_hours: float,
    max_workers: int,
    max_retries: int,
    retry_backoff_seconds: float,
    rate_limiter: _RateLimiter | None,
) -> tuple[dict[str, dict[str, object]], dict[str, pd.DataFrame]]:
    if not codes:
        return {}, {}

    worker_count = max(1, min(int(max_workers), len(codes)))
    signal_map: dict[str, dict[str, object]] = {}
    data_map: dict[str, pd.DataFrame] = {}

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                _evaluate_boll_candidate,
                code,
                start_date,
                end_date,
                window,
                k,
                near_ratio,
                adjust,
                use_cache,
                force_refresh,
                cache_max_age_hours,
                max_retries,
                retry_backoff_seconds,
                rate_limiter,
            ): code
            for code in codes
        }

        for future in as_completed(future_map):
            code = future_map[future]
            try:
                code_key, boll_df, signal_info = future.result()
            except Exception:
                code_key = code
                boll_df = pd.DataFrame()
                signal_info = {
                    "signal": "K线请求失败",
                    "signal_type": "fetch_error",
                    "selected": False,
                    "latest_close": None,
                    "latest_lower": None,
                    "latest_upper": None,
                }

            signal_map[code_key] = signal_info
            if not boll_df.empty:
                data_map[code_key] = boll_df

    return signal_map, data_map


def analyze_stocks_full_flow(
    codes: list[str],
    start_date: str | date,
    end_date: str | date,
    window: int = 20,
    k: float = 1.645,
    near_ratio: float = 1.015,
    adjust: str = "qfq",
    price_upper_limit: float = 35.0,
    debt_asset_ratio_limit: float = 70.0,
    exclude_gem_sci: bool = True,
    fund_flow_periods: tuple[str, ...] = DEFAULT_FUND_FLOW_PERIODS,
    important_shareholders: tuple[str, ...] = IMPORTANT_SHAREHOLDERS,
    important_holder_types: tuple[str, ...] = IMPORTANT_SHAREHOLDER_TYPES,
    use_cache: bool = True,
    force_refresh: bool = False,
    cache_max_age_hours: float = 24.0,
    max_workers: int = 4,
    max_retries: int = 2,
    retry_backoff_seconds: float = 0.5,
    request_interval_seconds: float = 0.0,
    progress_callback: ProgressCallback | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], dict[str, int | float]]:
    normalized_codes = [format_stock_code(code) for code in codes]
    normalized_codes = list(dict.fromkeys(normalized_codes))

    if exclude_gem_sci:
        universe_codes = [
            code
            for code in normalized_codes
            if not (str(code).startswith("30") or str(code).startswith("688"))
        ]
    else:
        universe_codes = normalized_codes

    code_name_map = fetch_code_name_map(
        universe_codes,
        use_cache=use_cache,
        force_refresh=force_refresh,
        max_cache_age_hours=cache_max_age_hours,
    )

    if progress_callback is not None:
        progress_callback("init", 0, len(universe_codes), "初始化筛选任务")

    safe_max_workers = max(1, int(max_workers))
    safe_max_retries = max(0, int(max_retries))
    safe_retry_backoff = max(0.0, float(retry_backoff_seconds))
    network_limiter = _RateLimiter(request_interval_seconds=request_interval_seconds)

    flow_union_codes, fund_flow_map = _fetch_fund_flow_union(
        price_upper_limit=float(price_upper_limit),
        periods=fund_flow_periods,
        use_cache=use_cache,
        force_refresh=force_refresh,
        cache_max_age_hours=cache_max_age_hours,
        max_workers=safe_max_workers,
        max_retries=safe_max_retries,
        retry_backoff_seconds=safe_retry_backoff,
        rate_limiter=network_limiter,
        progress_callback=progress_callback,
    )
    fund_flow_pass_codes = set(universe_codes) & flow_union_codes

    period_year, period_quarter = infer_report_period(end_date)
    fallback_year, fallback_quarter = previous_report_period(period_year, period_quarter)
    report_periods = [(period_year, period_quarter), (fallback_year, fallback_quarter)]

    data_map: dict[str, pd.DataFrame] = {}
    rows: list[dict[str, object]] = []
    fundamental_pass_codes: set[str] = set()
    shareholder_pass_codes: set[str] = set()
    boll_selected_codes: set[str] = set()

    login_result = bs.login()
    if login_result.error_code != "0":
        raise RuntimeError(f"baostock 登录失败: {login_result.error_msg}")

    try:
        total_codes = len(universe_codes)
        update_step = 1 if total_codes <= 500 else 10
        for index, code in enumerate(universe_codes, start=1):
            if progress_callback is not None and (index % update_step == 0 or index == total_codes):
                progress_callback("evaluate", index, total_codes, f"逐股评估中：{code}")

            stock_name = code_name_map.get(code, "")
            flow_pass = code in fund_flow_pass_codes
            period_hits = [period for period, code_set in fund_flow_map.items() if code in code_set]
            if period_hits:
                flow_note = f"通过：{','.join(period_hits)}净流入>0 且最新价<{float(price_upper_limit):g}"
            else:
                flow_note = f"未通过：3/5/10日均未满足净流入>0 且最新价<{float(price_upper_limit):g}"

            debt_ratio_percent = None
            debt_pass = False
            net_profit = None
            profit_pass = False
            cfo_to_np = None
            cash_pass = False
            forecast_eps_mean = None
            yoy_ni = None
            forecast_pass = False
            fundamental_pass = False
            shareholder_pass = False
            shareholder_note = "未执行"
            boll_signal = "未执行"
            signal_type = "not_run"
            latest_close = None
            latest_lower = None
            latest_upper = None
            hit = False

            fundamental_info = _evaluate_fundamental(
                code=code,
                periods=report_periods,
                debt_asset_ratio_limit=float(debt_asset_ratio_limit),
                current_year=pd.to_datetime(end_date).year,
                max_retries=safe_max_retries,
                retry_backoff_seconds=safe_retry_backoff,
                rate_limiter=network_limiter,
            )
            debt_ratio_percent = fundamental_info["debt_ratio_percent"]
            debt_pass = bool(fundamental_info["debt_pass"])
            net_profit = fundamental_info["net_profit"]
            profit_pass = bool(fundamental_info["profit_pass"])
            cfo_to_np = fundamental_info["cfo_to_np"]
            cash_pass = bool(fundamental_info["cash_pass"])
            forecast_eps_mean = fundamental_info["forecast_eps_mean"]
            yoy_ni = fundamental_info["yoy_ni"]
            forecast_pass = bool(fundamental_info["forecast_pass"])
            fundamental_pass = bool(fundamental_info["fundamental_pass"])

            if fundamental_pass:
                fundamental_pass_codes.add(code)

            prefilter_pass = bool(flow_pass and fundamental_pass)
            if prefilter_pass:
                shareholder_pass, shareholder_note = _check_important_shareholder(
                    code=code,
                    important_shareholders=important_shareholders,
                    important_holder_types=important_holder_types,
                    max_retries=safe_max_retries,
                    retry_backoff_seconds=safe_retry_backoff,
                    rate_limiter=network_limiter,
                )
            else:
                if not flow_pass and not fundamental_pass:
                    shareholder_note = "未进入股东环节：资金流与基本面均未通过"
                elif not flow_pass:
                    shareholder_note = "未进入股东环节：资金流未通过"
                else:
                    shareholder_note = "未进入股东环节：基本面未通过"
                boll_signal = "未进入 Boll"

            if shareholder_pass:
                shareholder_pass_codes.add(code)
                boll_signal = "待并发评估"
                signal_type = "pending"
            elif prefilter_pass:
                boll_signal = "股东未通过，未进入 Boll"

            rows.append(
                {
                    "股票代码": code,
                    "股票名称": stock_name,
                    "资金流通过": flow_pass,
                    "资金流说明": flow_note,
                    "基本面通过": fundamental_pass,
                    "前置汇合通过": prefilter_pass,
                    "资产负债率(%)": round(debt_ratio_percent, 3) if debt_ratio_percent is not None else None,
                    "资产负债率通过": debt_pass,
                    "净利润": round(net_profit, 3) if net_profit is not None else None,
                    "净利润通过": profit_pass,
                    "CFO/净利润": round(cfo_to_np, 4) if cfo_to_np is not None else None,
                    "现金流通过": cash_pass,
                    "预测EPS均值": round(forecast_eps_mean, 4) if forecast_eps_mean is not None else None,
                    "YOY净利润": round(yoy_ni, 4) if yoy_ni is not None else None,
                    "盈利预期通过": forecast_pass,
                    "重要股东通过": shareholder_pass,
                    "股东说明": shareholder_note,
                    "最新收盘": round(latest_close, 3) if latest_close is not None else None,
                    "下轨": round(latest_lower, 3) if latest_lower is not None else None,
                    "上轨": round(latest_upper, 3) if latest_upper is not None else None,
                    "信号": boll_signal,
                    "信号类型": signal_type,
                    "综合评分": 0,
                    "评分等级": "D",
                    "评分说明": "待计算",
                    "命中策略": hit,
                }
            )
    finally:
        bs.logout()

    boll_candidates = [str(row["股票代码"]) for row in rows if bool(row.get("重要股东通过", False))]
    boll_signal_map, boll_data_map = _evaluate_boll_candidates_parallel(
        codes=boll_candidates,
        start_date=start_date,
        end_date=end_date,
        window=window,
        k=k,
        near_ratio=near_ratio,
        adjust=adjust,
        use_cache=use_cache,
        force_refresh=force_refresh,
        cache_max_age_hours=cache_max_age_hours,
        max_workers=safe_max_workers,
        max_retries=safe_max_retries,
        retry_backoff_seconds=safe_retry_backoff,
        rate_limiter=network_limiter,
    )
    data_map.update(boll_data_map)

    for row in rows:
        code = str(row["股票代码"])
        if bool(row.get("重要股东通过", False)):
            boll_info = boll_signal_map.get(code)
            if boll_info is None:
                boll_info = {
                    "signal": "K线请求失败",
                    "signal_type": "fetch_error",
                    "selected": False,
                    "latest_close": None,
                    "latest_lower": None,
                    "latest_upper": None,
                }

            row["信号"] = str(boll_info.get("signal", "K线数据为空"))
            row["信号类型"] = str(boll_info.get("signal_type", "empty_k"))
            row["命中策略"] = bool(boll_info.get("selected", False))
            latest_close = boll_info.get("latest_close")
            latest_lower = boll_info.get("latest_lower")
            latest_upper = boll_info.get("latest_upper")
            row["最新收盘"] = round(float(latest_close), 3) if latest_close is not None else None
            row["下轨"] = round(float(latest_lower), 3) if latest_lower is not None else None
            row["上轨"] = round(float(latest_upper), 3) if latest_upper is not None else None

            if bool(row.get("命中策略", False)):
                boll_selected_codes.add(code)

        score, score_grade, score_note = _score_full_flow_result(
            flow_pass=bool(row.get("资金流通过", False)),
            debt_pass=bool(row.get("资产负债率通过", False)),
            profit_pass=bool(row.get("净利润通过", False)),
            cash_pass=bool(row.get("现金流通过", False)),
            forecast_pass=bool(row.get("盈利预期通过", False)),
            shareholder_pass=bool(row.get("重要股东通过", False)),
            signal_type=str(row.get("信号类型", "not_run")),
            hit=bool(row.get("命中策略", False)),
        )
        row["综合评分"] = score
        row["评分等级"] = score_grade
        row["评分说明"] = score_note

    result_df = pd.DataFrame(rows)
    if not result_df.empty:
        result_df = result_df.sort_values(
            by=["命中策略", "综合评分", "股票代码"],
            ascending=[False, False, True],
        ).reset_index(drop=True)

    flow_stats = {
        "输入代码数": len(normalized_codes),
        "板块过滤后": len(universe_codes),
        "资金流通过": len(fund_flow_pass_codes),
        "基本面通过": len(fundamental_pass_codes),
        "前置汇合通过": len(fund_flow_pass_codes & fundamental_pass_codes),
        "股东通过": len(shareholder_pass_codes),
        "Boll命中": len(boll_selected_codes),
        "3日资金命中": len(set(universe_codes) & fund_flow_map.get("3日排行", set())),
        "5日资金命中": len(set(universe_codes) & fund_flow_map.get("5日排行", set())),
        "10日资金命中": len(set(universe_codes) & fund_flow_map.get("10日排行", set())),
    }
    if not result_df.empty and "综合评分" in result_df.columns:
        flow_stats["平均评分"] = round(float(result_df["综合评分"].mean()), 2)
        flow_stats["A档数量"] = int((result_df["评分等级"] == "A").sum())

    if progress_callback is not None:
        progress_callback("done", len(universe_codes), len(universe_codes), "全流程分析完成")

    return result_df, data_map, flow_stats
