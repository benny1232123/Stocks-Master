from __future__ import annotations

from datetime import date, datetime

import akshare as ak
import baostock as bs
import pandas as pd

from core.data_fetcher import (
    format_stock_code,
    infer_report_period,
    parse_amount_text,
    previous_report_period,
    to_baostock_code,
)
from core.indicators import calc_bollinger, evaluate_boll_signal
from utils.config import DEFAULT_FUND_FLOW_PERIODS, IMPORTANT_SHAREHOLDER_TYPES, IMPORTANT_SHAREHOLDERS


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


def _to_date_string(value: str | date | datetime) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    text = str(value)
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def _fetch_code_name_map_bs(codes: list[str]) -> dict[str, str]:
    code_name_map: dict[str, str] = {}
    for code in codes:
        result_set = bs.query_stock_basic(code=to_baostock_code(code))
        if result_set.error_code != "0":
            continue
        basic_df = _result_set_to_df(result_set)
        if basic_df.empty or "code_name" not in basic_df.columns:
            continue
        code_name_map[code] = str(basic_df.iloc[0]["code_name"])
    return code_name_map


def _fetch_positive_fund_flow_codes(period_symbol: str, price_upper_limit: float) -> set[str]:
    try:
        fund_df = ak.stock_fund_flow_individual(symbol=period_symbol)
    except Exception:
        return set()

    if fund_df is None or fund_df.empty or fund_df.shape[1] < 7:
        return set()

    normalized = fund_df.copy()
    normalized["code"] = normalized.iloc[:, 1].map(format_stock_code)
    normalized["latest_price"] = pd.to_numeric(normalized.iloc[:, 3], errors="coerce")
    normalized["net_inflow"] = normalized.iloc[:, 6].map(parse_amount_text)

    filtered = normalized[
        (normalized["net_inflow"] > 0)
        & (normalized["latest_price"] < float(price_upper_limit))
    ]
    return set(filtered["code"].dropna().astype(str).tolist())


def _fetch_fund_flow_union(
    price_upper_limit: float,
    periods: tuple[str, ...],
) -> tuple[set[str], dict[str, set[str]]]:
    period_code_map: dict[str, set[str]] = {}
    for period_symbol in periods:
        period_code_map[period_symbol] = _fetch_positive_fund_flow_codes(period_symbol, price_upper_limit)

    union_codes: set[str] = set()
    for code_set in period_code_map.values():
        union_codes |= code_set

    return union_codes, period_code_map


def _query_financial_with_fallback(code: str, query_func, periods: list[tuple[int, int]]) -> pd.DataFrame:
    bs_code = to_baostock_code(code)
    for year, quarter in periods:
        result_set = query_func(code=bs_code, year=year, quarter=quarter)
        if result_set.error_code != "0":
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


def _fetch_forecast_eps_mean(code: str, current_year: int) -> float | None:
    try:
        forecast_df = ak.stock_profit_forecast_ths(symbol=code)
    except Exception:
        return None

    if forecast_df is None or forecast_df.empty or forecast_df.shape[1] < 4:
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
) -> dict[str, object]:
    balance_df = _query_financial_with_fallback(code, bs.query_balance_data, periods)
    profit_df = _query_financial_with_fallback(code, bs.query_profit_data, periods)
    cash_df = _query_financial_with_fallback(code, bs.query_cash_flow_data, periods)
    growth_df = _query_financial_with_fallback(code, bs.query_growth_data, periods)

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

    forecast_eps_mean = _fetch_forecast_eps_mean(code, current_year=current_year)
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
) -> tuple[bool, str]:
    try:
        holder_df = ak.stock_circulate_stock_holder(symbol=code)
    except Exception:
        return True, "股东接口异常，默认保留"

    if holder_df is None or holder_df.empty or holder_df.shape[1] < 7:
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


def _fetch_daily_k_data_in_session(
    code: str,
    start_date: str | date,
    end_date: str | date,
    adjust: str,
) -> pd.DataFrame:
    adjust_map = {"hfq": "1", "qfq": "2", "bfq": "3"}
    adjust_flag = adjust_map.get(str(adjust).lower(), "2")

    result_set = bs.query_history_k_data_plus(
        to_baostock_code(code),
        "date,code,open,high,low,close,volume,amount",
        start_date=_to_date_string(start_date),
        end_date=_to_date_string(end_date),
        frequency="d",
        adjustflag=adjust_flag,
    )
    if result_set.error_code != "0":
        return pd.DataFrame()

    price_df = _result_set_to_df(result_set)
    if price_df.empty:
        return price_df

    for column_name in ["open", "high", "low", "close", "volume", "amount"]:
        price_df[column_name] = pd.to_numeric(price_df[column_name], errors="coerce")
    price_df["date"] = pd.to_datetime(price_df["date"], errors="coerce")
    price_df = price_df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    price_df["date"] = price_df["date"].dt.strftime("%Y-%m-%d")
    return price_df


def analyze_stocks_full_flow(
    codes: list[str],
    start_date: str | date,
    end_date: str | date,
    window: int = 20,
    k: float = 1.645,
    near_ratio: float = 1.015,
    adjust: str = "qfq",
    price_upper_limit: float = 30.0,
    debt_asset_ratio_limit: float = 70.0,
    exclude_gem_sci: bool = True,
    fund_flow_periods: tuple[str, ...] = DEFAULT_FUND_FLOW_PERIODS,
    important_shareholders: tuple[str, ...] = IMPORTANT_SHAREHOLDERS,
    important_holder_types: tuple[str, ...] = IMPORTANT_SHAREHOLDER_TYPES,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], dict[str, int]]:
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

    flow_union_codes, fund_flow_map = _fetch_fund_flow_union(
        price_upper_limit=float(price_upper_limit),
        periods=fund_flow_periods,
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
        code_name_map = _fetch_code_name_map_bs(universe_codes)

        for code in universe_codes:
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
            latest_close = None
            latest_lower = None
            latest_upper = None
            hit = False

            fundamental_info = _evaluate_fundamental(
                code=code,
                periods=report_periods,
                debt_asset_ratio_limit=float(debt_asset_ratio_limit),
                current_year=pd.to_datetime(end_date).year,
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
                k_df = _fetch_daily_k_data_in_session(
                    code=code,
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust,
                )
                if not k_df.empty:
                    boll_df = calc_bollinger(k_df, window=window, k=k)
                    signal_info = evaluate_boll_signal(boll_df, near_ratio=near_ratio)
                    data_map[code] = boll_df
                    boll_signal = str(signal_info["signal"])
                    hit = bool(signal_info["selected"])
                    latest = boll_df.iloc[-1]
                    latest_close = float(latest["close"]) if pd.notna(latest["close"]) else None
                    latest_lower = float(latest["Lower"]) if pd.notna(latest["Lower"]) else None
                    latest_upper = float(latest["Upper"]) if pd.notna(latest["Upper"]) else None
                    if hit:
                        boll_selected_codes.add(code)
                else:
                    boll_signal = "K线数据为空"
            elif prefilter_pass:
                boll_signal = "股东未通过，未进入 Boll"

            rows.append(
                {
                    "股票代码": code,
                    "股票名称": stock_name,
                    "资金流通过": flow_pass,
                    "资金流说明": flow_note,
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
                    "命中策略": hit,
                }
            )
    finally:
        bs.logout()

    result_df = pd.DataFrame(rows)
    if not result_df.empty:
        result_df = result_df.sort_values(by=["命中策略", "股票代码"], ascending=[False, True]).reset_index(drop=True)

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
    return result_df, data_map, flow_stats
