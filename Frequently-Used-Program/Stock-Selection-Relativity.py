from __future__ import annotations

import argparse
import os
import re
import socket
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import akshare as ak
import baostock as bs
import pandas as pd

from strategy_common import format_stock_code, load_checkpoint_df, normalize_code_series, save_checkpoint_df


ROOT_DIR = Path(__file__).resolve().parents[1]
STOCK_DATA_DIR = ROOT_DIR / "stock_data"
CHECKPOINT_DIR = STOCK_DATA_DIR / "checkpoints"
DB_PATH = STOCK_DATA_DIR / "stocks_data.db"

PRICE_UPPER_LIMIT = 30.0
PRICE_LOWER_LIMIT = 5.0
DEBT_ASSET_RATIO_LIMIT = 70.0

RS_INDEX_CODE = "sh.000001"
RS_LOOKBACK_DAYS = 100
RS_MIN_OVERLAP_DAYS = 30
RS_UP_TOL = -0.025
RS_DOWN_OUTPERF = 0.0
RS_MIN_UP_RATIO = 0.6
RS_MIN_DOWN_RATIO = 0.7
RS_MIN_UP_DAYS = 5
RS_MIN_DOWN_DAYS = 5

IMPORTANT_SHAREHOLDERS = [
    "香港中央结算有限公司",
    "中央汇金资产管理有限公司",
    "中央汇金投资有限责任公司",
    "香港中央结算（代理人）有限公司",
    "中国证券金融股份有限公司",
]
IMPORTANT_SHAREHOLDER_TYPES = ["社保基金"]

DEFAULT_SLEEP_SECONDS = 0.0
DEFAULT_MAX_WORKERS = max(4, min(16, (os.cpu_count() or 8)))
DEFAULT_HOLDER_MAX_WORKERS = DEFAULT_MAX_WORKERS
CHECKPOINT_SAVE_EVERY = 20
RS_CHECKPOINT_PASS_COL = "是否通过"
DEFAULT_BS_TIMEOUT_SECONDS = float(os.getenv("BS_REQUEST_TIMEOUT_SECONDS", "15"))
DEFAULT_BS_REQUEST_INTERVAL_SECONDS = float(os.getenv("BS_REQUEST_INTERVAL_SECONDS", "0.05"))
DEFAULT_BS_MAX_RETRIES = int(os.getenv("BS_MAX_RETRIES", "2"))


_BS_RATE_LIMIT_LOCK = threading.Lock()
_BS_NEXT_ALLOWED_AT = 0.0


# pandas 显示设置
pd.set_option("display.unicode.ambiguous_as_wide", True)
pd.set_option("display.unicode.east_asian_width", True)
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="A股相对强弱策略（资金流 + 基本面 + 股东 + 指数相对强弱）")
    parser.add_argument("--price-upper-limit", type=float, default=PRICE_UPPER_LIMIT, help="股价上限")
    parser.add_argument("--price-lower-limit", type=float, default=PRICE_LOWER_LIMIT, help="股价下限")
    parser.add_argument("--debt-asset-ratio-limit", type=float, default=DEBT_ASSET_RATIO_LIMIT, help="资产负债率上限")
    parser.add_argument("--index-code", default=RS_INDEX_CODE, help="对比指数代码（baostock）")
    parser.add_argument("--rs-lookback-days", type=int, default=RS_LOOKBACK_DAYS, help="相对强弱回看天数")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS, help="相对强弱评估并发数，默认自适应CPU(4~16)")
    parser.add_argument("--holder-max-workers", type=int, default=DEFAULT_HOLDER_MAX_WORKERS, help="股东筛选并发数")
    parser.add_argument("--min-down-ratio", type=float, default=RS_MIN_DOWN_RATIO, help="抗跌满足率下限，支持 0~1 或 0~100")
    parser.add_argument("--resume", action="store_true", help="启用相对强弱阶段断点续跑")
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS, help="慢接口调用间隔秒数，默认0")
    parser.add_argument("--disable-rs", action="store_true", help="关闭相对强弱筛选，仅输出前置候选")
    parser.add_argument("--seed-csv", default="", help="复用已有候选CSV（如当日Boll结果），跳过前置资金流/基本面/股东筛选")
    parser.add_argument("--bs-timeout-seconds", type=float, default=DEFAULT_BS_TIMEOUT_SECONDS, help="baostock请求超时秒数")
    parser.add_argument("--bs-request-interval-seconds", type=float, default=DEFAULT_BS_REQUEST_INTERVAL_SECONDS, help="baostock请求最小间隔秒数")
    parser.add_argument("--bs-max-retries", type=int, default=DEFAULT_BS_MAX_RETRIES, help="baostock请求失败重试次数")
    return parser.parse_args()


def _throttle_bs_request(interval_seconds: float) -> None:
    interval = max(0.0, float(interval_seconds))
    if interval <= 0:
        return

    global _BS_NEXT_ALLOWED_AT
    with _BS_RATE_LIMIT_LOCK:
        now = time.time()
        if now < _BS_NEXT_ALLOWED_AT:
            time.sleep(_BS_NEXT_ALLOWED_AT - now)
            now = time.time()
        _BS_NEXT_ALLOWED_AT = now + interval


def _latest_trade_day_bs(today_text: str, lookback_days: int, request_interval_seconds: float, max_retries: int) -> str:
    try:
        start_text = (datetime.strptime(today_text, "%Y-%m-%d") - timedelta(days=max(lookback_days, 30))).strftime("%Y-%m-%d")
    except Exception:
        return today_text

    retries = max(0, int(max_retries))
    rs = None
    for attempt in range(retries + 1):
        _throttle_bs_request(request_interval_seconds)
        rs = bs.query_trade_dates(start_date=start_text, end_date=today_text)
        if rs is not None and rs.error_code == "0":
            break
        if attempt < retries:
            time.sleep(min(0.2 * (attempt + 1), 1.0))

    if rs is None or rs.error_code != "0":
        return today_text

    latest_trade_day = None
    while rs.next():
        row = rs.get_row_data()
        if not row or len(row) < 2:
            continue
        trade_date, is_trading = row[0], row[1]
        if is_trading == "1":
            latest_trade_day = trade_date

    return latest_trade_day or today_text


def add_market_prefix(code) -> str:
    formatted_code = format_stock_code(code)
    return f"sh{formatted_code}" if formatted_code.startswith("6") else f"sz{formatted_code}"


def add_market_prefix_dotted(code) -> str:
    formatted_code = format_stock_code(code)
    return f"sh.{formatted_code}" if formatted_code.startswith("6") else f"sz.{formatted_code}"


def convert_fund_flow(value):
    if isinstance(value, str):
        if "亿" in value:
            return float(value.replace("亿", "")) * 1e8
        if "万" in value:
            return float(value.replace("万", "")) * 1e4
        if value == "-":
            return 0.0
        return float(value)
    return value


def _cache_table_name(cache_key: str) -> str:
    key = cache_key.replace("stock_data/", "").replace(".csv", "")
    return re.sub(r"[^0-9a-zA-Z_]+", "_", key)


def fetch_data_with_fallback(api_func, cache_key: str, *args, **kwargs) -> pd.DataFrame:
    """优先读取 sqlite 缓存，缺失时再调用 API 并写回本地缓存。"""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    table_name = _cache_table_name(cache_key)

    conn = sqlite3.connect(DB_PATH)
    try:
        try:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            if not df.empty:
                print(f"成功读取本地数据库表: {table_name}")
                return df
            print(f"本地数据库表为空: {table_name}，准备调用 API 补充")
        except Exception:
            print(f"本地数据库缺少表: {table_name}，准备调用 API 补充")

        df = api_func(*args, **kwargs)
        if isinstance(df, pd.DataFrame) and not df.empty:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"API调用成功。数据已保存至数据库表: {table_name}")
            return df

        print(f"API返回空数据: {table_name}")
        return pd.DataFrame()
    except Exception as exc:
        print(f"本地读取和API调用都失败: {exc}")
        return pd.DataFrame()
    finally:
        conn.close()


def resolve_report_dates(now: datetime) -> tuple[str, str, list[str], int]:
    current_year = now.year
    last_year = current_year - 1
    current_month = now.month

    if current_month < 5:
        report_date_profit = f"{last_year}0930"
        report_date_holder = f"{last_year}0930"
    elif current_month < 9:
        report_date_profit = f"{current_year}0331"
        report_date_holder = f"{current_year}0331"
    elif current_month < 11:
        report_date_profit = f"{current_year}0630"
        report_date_holder = f"{current_year}0630"
    else:
        report_date_profit = f"{current_year}0930"
        report_date_holder = f"{current_year}0930"

    if current_month < 5:
        zcfz_dates = [f"{last_year}0930", f"{last_year}0630"]
    elif current_month < 9:
        zcfz_dates = [f"{current_year}0331", f"{last_year}1231"]
    elif current_month < 11:
        zcfz_dates = [f"{current_year}0630", f"{current_year}0331"]
    else:
        zcfz_dates = [f"{current_year}0930", f"{current_year}0630"]

    return report_date_profit, report_date_holder, zcfz_dates, current_year


def get_fund_flow_codes(price_upper_limit: float, price_lower_limit: float, sleep_seconds: float) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    period_map = [("3日排行", "3"), ("5日排行", "5"), ("10日排行", "10")]

    for period, period_name in period_map:
        df = fetch_data_with_fallback(
            ak.stock_fund_flow_individual,
            f"stock_data/{period_name}-days-positive-funds.csv",
            symbol=period,
        )
        if not df.empty:
            df["资金流入净额"] = df["资金流入净额"].apply(convert_fund_flow)
            df["最新价"] = pd.to_numeric(df["最新价"], errors="coerce")
            positive_df = df[
                (df["资金流入净额"] > 0)
                & (df["最新价"] < price_upper_limit)
                & (df["最新价"] >= price_lower_limit)
            ]
            out[f"{period_name}d"] = positive_df["股票代码"].apply(format_stock_code).tolist()
        else:
            out[f"{period_name}d"] = []
        time.sleep(max(sleep_seconds, 0.0))
    return out


def get_fundamental_codes(
    debt_asset_ratio_limit: float,
    report_date_profit: str,
    zcfz_dates: list[str],
    current_year: int,
    sleep_seconds: float,
) -> tuple[list[str], list[str], list[str], list[str]]:
    zcfz_codes_list: list[str] = []
    for date_str in zcfz_dates:
        s_zcfz_df = fetch_data_with_fallback(
            ak.stock_zcfz_em,
            f"stock_data/stock_zcfz_em_{date_str}.csv",
            date=date_str,
        )
        if not s_zcfz_df.empty:
            s_good_zcfz_df = s_zcfz_df[pd.to_numeric(s_zcfz_df["资产负债率"], errors="coerce") < debt_asset_ratio_limit]
            zcfz_codes_list.extend(s_good_zcfz_df["股票代码"].apply(format_stock_code).tolist())
        time.sleep(max(sleep_seconds, 0.0))
    zcfz_codes = list(set(zcfz_codes_list))

    profit_df = fetch_data_with_fallback(
        ak.stock_lrb_em,
        f"stock_data/stock_lrb_em_{report_date_profit}.csv",
        date=report_date_profit,
    )
    profit_codes: list[str] = []
    if not profit_df.empty:
        good_profit_df = profit_df[pd.to_numeric(profit_df["净利润"], errors="coerce") > 0]
        profit_codes = good_profit_df["股票代码"].apply(format_stock_code).tolist()
    time.sleep(max(sleep_seconds, 0.0))

    cashflow_df = fetch_data_with_fallback(
        ak.stock_xjll_em,
        f"stock_data/stock_xjll_em_{report_date_profit}.csv",
        date=report_date_profit,
    )
    cashflow_codes: list[str] = []
    if not cashflow_df.empty:
        good_cashflow_df = cashflow_df[pd.to_numeric(cashflow_df["经营性现金流-现金流量净额"], errors="coerce") > 0]
        cashflow_codes = good_cashflow_df["股票代码"].apply(format_stock_code).tolist()
    time.sleep(max(sleep_seconds, 0.0))

    profit_forecast_df = fetch_data_with_fallback(
        ak.stock_profit_forecast_em,
        "stock_data/stock_profit_forecast_em.csv",
    )
    profit_forecast_codes: list[str] = []
    if not profit_forecast_df.empty:
        forecast_col = f"{current_year}预测每股收益"
        if forecast_col in profit_forecast_df.columns:
            good_profit_forecast_df = profit_forecast_df[pd.to_numeric(profit_forecast_df[forecast_col], errors="coerce") > 0]
            code_col = "代码" if "代码" in good_profit_forecast_df.columns else "股票代码"
            if code_col in good_profit_forecast_df.columns:
                profit_forecast_codes = good_profit_forecast_df[code_col].apply(format_stock_code).tolist()
        else:
            print(f"'{forecast_col}' not found in profit forecast data.")
    time.sleep(max(sleep_seconds, 0.0))

    return cashflow_codes, profit_codes, zcfz_codes, profit_forecast_codes


def build_candidate_codes(
    cashflow_codes: list[str],
    profit_codes: list[str],
    zcfz_codes: list[str],
    profit_forecast_codes: list[str],
    fund_flow_codes: dict[str, list[str]],
) -> list[str]:
    print("\n各条件股票数量:")
    print(f"  现金流: {len(cashflow_codes)}")
    print(f"  利润表: {len(profit_codes)}")
    print(f"  资产负债率: {len(zcfz_codes)}")
    print(f"  盈利预测: {len(profit_forecast_codes)}")
    print(f"  3日资金: {len(fund_flow_codes.get('3d', []))}")
    print(f"  5日资金: {len(fund_flow_codes.get('5d', []))}")
    print(f"  10日资金: {len(fund_flow_codes.get('10d', []))}")

    fundamental_intersection = (
        set(cashflow_codes) & set(profit_codes) & set(zcfz_codes) & set(profit_forecast_codes)
    )
    print(f"基本面条件交集: {len(fundamental_intersection)}")

    set_3d = set(fund_flow_codes.get("3d", []))
    set_5d = set(fund_flow_codes.get("5d", []))
    set_10d = set(fund_flow_codes.get("10d", []))
    fund_flow_union = set_3d | set_5d | set_10d
    print(f"资金流向条件(至少满足一个)交集: {len(fund_flow_union)}")

    common_codes_set = fundamental_intersection & fund_flow_union
    print(f"所有条件交集后: {len(common_codes_set)}")

    filtered_codes = [code for code in common_codes_set if not (str(code).startswith("30") or str(code).startswith("688"))]
    print(f"排除创业板和科创板后: {len(filtered_codes)}")
    return sorted(filtered_codes)


def load_seed_candidates(seed_csv: str) -> tuple[list[str], dict[str, str]]:
    path = Path(seed_csv)
    if not path.is_absolute():
        path = (ROOT_DIR / path).resolve()
    if not path.exists():
        print(f"[seed] 文件不存在: {path}")
        return [], {}

    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception as exc:
        print(f"[seed] 读取失败: {path} | {exc}")
        return [], {}

    if df.empty:
        print(f"[seed] 文件为空: {path}")
        return [], {}

    code_col = "股票代码" if "股票代码" in df.columns else ("code" if "code" in df.columns else "")
    if not code_col:
        print(f"[seed] 缺少代码列(股票代码/code): {path}")
        return [], {}

    work = df.copy()
    work["_code"] = normalize_code_series(work[code_col])
    work = work[work["_code"].astype(str).str.len() == 6]
    work = work.drop_duplicates(subset=["_code"], keep="first")

    name_col = "股票名称" if "股票名称" in work.columns else ("name" if "name" in work.columns else "")
    name_map: dict[str, str] = {}
    if name_col:
        name_map = dict(zip(work["_code"].astype(str).tolist(), work[name_col].astype(str).fillna("").tolist()))

    codes = work["_code"].astype(str).tolist()
    print(f"[seed] 复用候选: {len(codes)} | {path}")
    return codes, name_map


def get_code_name_map() -> dict[str, str]:
    code_name_map: dict[str, str] = {}
    try:
        code_name_df = fetch_data_with_fallback(
            ak.stock_info_a_code_name,
            "stock_data/stock_info_a_code_name.csv",
        )
        if not code_name_df.empty and {"code", "name"}.issubset(code_name_df.columns):
            tmp = code_name_df.copy()
            tmp["code"] = tmp["code"].apply(format_stock_code)
            code_name_map = dict(zip(tmp["code"], tmp["name"]))
    except Exception as exc:
        print(f"获取股票名称映射失败: {exc}（将仅输出股票代码）")
    return code_name_map


def get_code_name_map_from_cache() -> dict[str, str]:
    """仅从本地sqlite缓存读取股票代码-名称映射，不触发远端API调用。"""
    if not DB_PATH.exists():
        return {}

    table_name = _cache_table_name("stock_data/stock_info_a_code_name.csv")
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql(f"SELECT code, name FROM {table_name}", conn)
        if df.empty:
            return {}
        df["code"] = df["code"].apply(format_stock_code)
        df["name"] = df["name"].astype(str).replace("nan", "").fillna("")
        return dict(zip(df["code"], df["name"]))
    except Exception:
        return {}
    finally:
        conn.close()


def _print_progress(stage: str, done: int, total: int, *, passed: int | None = None) -> None:
    pct = 100.0 if total <= 0 else (done * 100.0 / total)
    if passed is None:
        print(f"[{stage}进度] {done}/{total} ({pct:.1f}%)")
    else:
        print(f"[{stage}进度] {done}/{total} ({pct:.1f}%) | 当前通过={passed}")


def _evaluate_shareholder_single(code: str, report_date_holder: str) -> tuple[str, bool, str]:
    code_fmt = format_stock_code(code)
    try:
        new_code = add_market_prefix(code_fmt)
        share_holders_df = ak.stock_gdfx_free_top_10_em(symbol=new_code, date=report_date_holder)

        has_important = False
        if not share_holders_df.empty:
            top5_names = share_holders_df["股东名称"].head(5).astype(str).tolist()
            top5_types = share_holders_df["股东性质"].head(5).astype(str).tolist()

            if any(any(imp in name for name in top5_names) for imp in IMPORTANT_SHAREHOLDERS):
                has_important = True
            if (not has_important) and any(
                any(imp_type in t for t in top5_types) for imp_type in IMPORTANT_SHAREHOLDER_TYPES
            ):
                has_important = True

        if has_important:
            return code_fmt, True, f"{code_fmt}：大股东持股稳定，符合条件"
        return code_fmt, False, f"{code_fmt}：无重要股东持股"
    except Exception as exc:
        return code_fmt, True, f"获取 {code_fmt} 流通股东数据时出错: {exc}. 默认保留该股票。"


def filter_by_shareholders(
    candidate_codes: list[str],
    report_date_holder: str,
    sleep_seconds: float,
    *,
    max_workers: int,
) -> list[str]:
    final_candidate_codes: list[str] = []
    if not candidate_codes:
        print("没有候选股票进行流通股东分析")
        return final_candidate_codes

    total = len(candidate_codes)
    processed = 0
    passed = 0

    if max_workers <= 1:
        for code in candidate_codes:
            code_fmt, keep, msg = _evaluate_shareholder_single(code, report_date_holder)
            print(msg)
            if keep:
                final_candidate_codes.append(code_fmt)
                passed += 1
            processed += 1
            _print_progress("股东筛选", processed, total, passed=passed)
            time.sleep(max(sleep_seconds, 0.0))
        return final_candidate_codes

    print(f"[股东筛选] 已启用并发评估 workers={max_workers}")
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {
            ex.submit(_evaluate_shareholder_single, code, report_date_holder): format_stock_code(code)
            for code in candidate_codes
        }
        for fut in as_completed(futures):
            code_fmt = futures[fut]
            try:
                _code_fmt, keep, msg = fut.result()
                print(msg)
                if keep:
                    final_candidate_codes.append(code_fmt)
                    passed += 1
            except Exception as exc:
                print(f"获取 {code_fmt} 流通股东数据时出错: {exc}. 默认保留该股票。")
                final_candidate_codes.append(code_fmt)
                passed += 1
            processed += 1
            _print_progress("股东筛选", processed, total, passed=passed)
            time.sleep(max(sleep_seconds, 0.0))

    return final_candidate_codes


def fetch_bs_daily_close(
    code_bs: str,
    start_date: str,
    end_date: str,
    *,
    request_interval_seconds: float,
    max_retries: int,
) -> pd.DataFrame:
    retries = max(0, int(max_retries))
    data_list = []
    rs_fields = []
    last_error_code = ""
    last_error_msg = ""

    for attempt in range(retries + 1):
        _throttle_bs_request(request_interval_seconds)
        rs = bs.query_history_k_data_plus(
            code_bs,
            "date,close,tradestatus",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2",
        )
        if rs is None or rs.error_code != "0":
            if rs is not None:
                last_error_code = str(getattr(rs, "error_code", "") or "")
                last_error_msg = str(getattr(rs, "error_msg", "") or "")
            if attempt < retries:
                time.sleep(min(0.2 * (attempt + 1), 1.0))
            continue

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        rs_fields = list(getattr(rs, "fields", []) or [])
        if data_list and rs_fields:
            break

        if attempt < retries:
            time.sleep(min(0.2 * (attempt + 1), 1.0))

    if not data_list or not rs_fields:
        if last_error_code or last_error_msg:
            print(
                f"[相对强弱] baostock日线为空 {code_bs} {start_date}~{end_date} | "
                f"error_code={last_error_code} error_msg={last_error_msg}"
            )
        return pd.DataFrame()

    df = pd.DataFrame(data_list, columns=rs_fields)
    if df.empty:
        return df

    raw_close_df = df[["date", "close"]].copy() if {"date", "close"}.issubset(df.columns) else pd.DataFrame()

    if "tradestatus" in df.columns:
        active_df = df[df["tradestatus"].astype(str) == "1"]
        if not active_df.empty:
            df = active_df
        else:
            # 某些日期区间会出现 tradestatus 异常值，回退到原始行情避免误判为 empty_close。
            print(f"[相对强弱] tradestatus过滤后为空，回退原始收盘数据: {code_bs}")

    df = df[["date", "close"]].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "close"]).sort_values("date")

    if df.empty and not raw_close_df.empty:
        raw_close_df["date"] = pd.to_datetime(raw_close_df["date"], errors="coerce")
        raw_close_df["close"] = pd.to_numeric(raw_close_df["close"], errors="coerce")
        raw_close_df = raw_close_df.dropna(subset=["date", "close"]).sort_values("date")
        if not raw_close_df.empty:
            print(f"[相对强弱] 使用原始收盘回退成功: {code_bs} rows={len(raw_close_df)}")
            return raw_close_df

    return df


def _to_daily_ret(df_close: pd.DataFrame, col_name: str) -> pd.DataFrame:
    if df_close is None or df_close.empty:
        return pd.DataFrame()
    t = df_close[["date", "close"]].copy()
    t["ret"] = t["close"].pct_change()
    t = t.dropna(subset=["ret"])
    return t[["date"]].assign(**{col_name: t["ret"].values})


def relative_strength_pass(
    stock_close_df: pd.DataFrame,
    index_close_df: pd.DataFrame,
    *,
    min_overlap_days: int,
    up_tol: float,
    down_outperf: float,
    min_up_ratio: float,
    min_down_ratio: float,
    min_up_days: int,
    min_down_days: int,
) -> tuple[bool, dict]:
    s = _to_daily_ret(stock_close_df, "sret")
    i = _to_daily_ret(index_close_df, "iret")
    if s.empty or i.empty:
        return False, {"reason": "empty_ret"}

    m = pd.merge(s, i, on="date", how="inner")
    if len(m) < min_overlap_days:
        return False, {"reason": "overlap_too_small", "overlap_days": int(len(m))}

    up_mask = m["iret"] > 0
    down_mask = m["iret"] < 0
    up_days = int(up_mask.sum())
    down_days = int(down_mask.sum())

    stock_up_days = int((m["sret"] > 0).sum())
    stock_down_days = int((m["sret"] < 0).sum())

    if up_days < min_up_days or down_days < min_down_days:
        return False, {
            "reason": "insufficient_up_or_down_days",
            "up_days": up_days,
            "down_days": down_days,
            "stock_up_days": stock_up_days,
            "stock_down_days": stock_down_days,
            "overlap_days": int(len(m)),
        }

    up_ok = int((m.loc[up_mask, "sret"] >= up_tol).sum())
    down_ok = int(((m.loc[down_mask, "sret"] - m.loc[down_mask, "iret"]) >= down_outperf).sum())

    up_ratio = up_ok / up_days if up_days else 0.0
    down_ratio = down_ok / down_days if down_days else 0.0

    passed = (up_ratio >= min_up_ratio) and (down_ratio >= min_down_ratio)
    return passed, {
        "overlap_days": int(len(m)),
        "up_days": up_days,
        "down_days": down_days,
        "stock_up_days": stock_up_days,
        "stock_down_days": stock_down_days,
        "up_ratio": float(up_ratio),
        "down_ratio": float(down_ratio),
    }


def _checkpoint_path(today_text: str, index_code: str, lookback_days: int) -> Path:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    idx = re.sub(r"[^0-9a-zA-Z]+", "", str(index_code or "").lower()) or "index"
    lb = max(int(lookback_days), 1)
    return CHECKPOINT_DIR / f"Stock-Selection-Relativity-Checkpoint-{today_text}-{idx}-lb{lb}.csv"


def _load_rs_checkpoint(path: Path) -> pd.DataFrame:
    return load_checkpoint_df(path)


def _save_rs_checkpoint(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    save_checkpoint_df(path, pd.DataFrame(rows))


def _is_retryable_fail_reason(reason: str) -> bool:
    text = str(reason or "").strip().lower()
    if not text:
        return False
    retryable_tokens = [
        "empty_close",
        "exception",
        "query_failed",
        "timeout",
        "network",
        "connect",
    ]
    return any(token in text for token in retryable_tokens)


def _evaluate_single_code(
    code: str,
    stock_name: str,
    index_close_df: pd.DataFrame,
    start_date: str,
    end_date: str,
    price_lower_limit: float,
    price_upper_limit: float,
    min_down_ratio: float,
    bs_request_interval_seconds: float,
    bs_max_retries: int,
) -> tuple[str, bool, dict]:
    code_bs = add_market_prefix_dotted(code)
    stock_close_df = fetch_bs_daily_close(
        code_bs,
        start_date=start_date,
        end_date=end_date,
        request_interval_seconds=bs_request_interval_seconds,
        max_retries=bs_max_retries,
    )
    if stock_close_df.empty:
        return format_stock_code(code), False, {
            "reason": "empty_close",
            "code_bs": code_bs,
            "start_date": start_date,
            "end_date": end_date,
        }

    latest_close = float(stock_close_df["close"].iloc[-1])
    if latest_close < price_lower_limit:
        return format_stock_code(code), False, {"reason": "below_min_price", "latest_close": latest_close}
    if latest_close > price_upper_limit:
        return format_stock_code(code), False, {"reason": "above_max_price", "latest_close": latest_close}

    passed, stats = relative_strength_pass(
        stock_close_df,
        index_close_df,
        min_overlap_days=RS_MIN_OVERLAP_DAYS,
        up_tol=RS_UP_TOL,
        down_outperf=RS_DOWN_OUTPERF,
        min_up_ratio=RS_MIN_UP_RATIO,
        min_down_ratio=min_down_ratio,
        min_up_days=RS_MIN_UP_DAYS,
        min_down_days=RS_MIN_DOWN_DAYS,
    )
    meta = {"code": format_stock_code(code), "name": stock_name, **stats}
    return format_stock_code(code), passed, meta


def run_relative_strength(
    final_candidate_codes: list[str],
    code_name_map: dict[str, str],
    index_code: str,
    lookback_days: int,
    today_text: str,
    *,
    max_workers: int,
    resume: bool,
    price_lower_limit: float,
    price_upper_limit: float,
    min_down_ratio: float,
    bs_request_interval_seconds: float,
    bs_max_retries: int,
) -> list[dict]:
    if not final_candidate_codes:
        return []

    checkpoint = _checkpoint_path(today_text, index_code, lookback_days)
    checkpoint_df = _load_rs_checkpoint(checkpoint) if resume else pd.DataFrame()
    if not checkpoint_df.empty and RS_CHECKPOINT_PASS_COL in checkpoint_df.columns:
        normalized_codes = normalize_code_series(checkpoint_df["股票代码"])
        retryable_codes: set[str] = set()
        if "原因" in checkpoint_df.columns:
            fail_mask = checkpoint_df[RS_CHECKPOINT_PASS_COL].fillna(0).astype(int) == 0
            fail_df = checkpoint_df[fail_mask].copy()
            if not fail_df.empty:
                fail_df["_code"] = normalize_code_series(fail_df["股票代码"])
                retryable_codes = {
                    row["_code"]
                    for _, row in fail_df.iterrows()
                    if _is_retryable_fail_reason(row.get("原因", ""))
                }

        done_codes = {c for c in normalized_codes.tolist() if c not in retryable_codes}
        selected_rows = checkpoint_df[checkpoint_df[RS_CHECKPOINT_PASS_COL] == 1].copy().to_dict(orient="records")

        if retryable_codes:
            print(f"[相对强弱] 断点续跑将重试临时失败股票: {len(retryable_codes)}")
    else:
        selected_rows = checkpoint_df.to_dict(orient="records") if not checkpoint_df.empty else []
        done_codes = set(normalize_code_series(checkpoint_df["股票代码"]).tolist()) if not checkpoint_df.empty else set()

    # 新格式检查点记录所有已处理股票（含失败），提升断点续跑速度。
    checkpoint_rows: list[dict] = checkpoint_df.to_dict(orient="records") if not checkpoint_df.empty else []

    pending_codes = [c for c in final_candidate_codes if format_stock_code(c) not in done_codes]
    total_all = len(final_candidate_codes)
    processed = len(done_codes)
    if resume and done_codes:
        print(f"[相对强弱] 检查点已完成: {len(done_codes)}，待处理: {len(pending_codes)}")
        _print_progress("相对强弱", processed, total_all, passed=len(selected_rows))

    now_text = datetime.now().strftime("%Y-%m-%d")
    end_date = _latest_trade_day_bs(
        now_text,
        lookback_days=max(int(lookback_days), 45),
        request_interval_seconds=bs_request_interval_seconds,
        max_retries=bs_max_retries,
    )
    start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=max(int(lookback_days), 10))).strftime("%Y-%m-%d")

    index_close_df = fetch_bs_daily_close(
        index_code,
        start_date=start_date,
        end_date=end_date,
        request_interval_seconds=bs_request_interval_seconds,
        max_retries=bs_max_retries,
    )
    if index_close_df.empty:
        print(f"[相对强弱] 指数数据为空：{index_code}，无法执行相对强弱筛选。")
        return selected_rows

    if max_workers > 1:
        print(f"[相对强弱] 已启用并发评估 workers={max_workers}（实验特性）")

    def row_from_meta(meta: dict) -> dict:
        return {
            "股票代码": meta["code"],
            "股票名称": meta.get("name", ""),
            RS_CHECKPOINT_PASS_COL: 1,
            "上涨满足率": meta.get("up_ratio"),
            "抗跌满足率": meta.get("down_ratio"),
            "对齐交易日": meta.get("overlap_days"),
            "指数上涨日数(对齐后)": meta.get("up_days"),
            "指数下跌日数(对齐后)": meta.get("down_days"),
            "个股上涨日数(对齐后)": meta.get("stock_up_days"),
            "个股下跌日数(对齐后)": meta.get("stock_down_days"),
        }

    def fail_row_from_meta(code_fmt: str, stock_name: str, meta: dict) -> dict:
        return {
            "股票代码": code_fmt,
            "股票名称": stock_name,
            RS_CHECKPOINT_PASS_COL: 0,
            "原因": str(meta.get("reason", "not_passed")),
        }

    pending_processed = 0
    checkpoint_dirty = False

    def maybe_flush_checkpoint(force: bool = False) -> None:
        nonlocal checkpoint_dirty
        if not resume:
            return
        if not checkpoint_dirty:
            return
        if force or (pending_processed > 0 and pending_processed % CHECKPOINT_SAVE_EVERY == 0):
            _save_rs_checkpoint(checkpoint, checkpoint_rows)
            checkpoint_dirty = False

    if max_workers <= 1:
        for code in pending_codes:
            stock_name = code_name_map.get(format_stock_code(code), "")
            code_fmt, passed, meta = _evaluate_single_code(
                code,
                stock_name,
                index_close_df,
                start_date,
                end_date,
                price_lower_limit,
                price_upper_limit,
                min_down_ratio,
                bs_request_interval_seconds,
                bs_max_retries,
            )
            if passed:
                print(
                    f"[相对强弱] PASS {code_fmt} {stock_name} | "
                    f"上涨满足率={meta.get('up_ratio', 0):.2f} 抗跌满足率={meta.get('down_ratio', 0):.2f} "
                    f"对齐交易日={meta.get('overlap_days', 0)}"
                )
                pass_row = row_from_meta(meta)
                selected_rows.append(pass_row)
                checkpoint_rows.append(pass_row)
                checkpoint_dirty = True
            else:
                print(f"[相对强弱] FAIL {code_fmt} {stock_name} | {meta}")
                checkpoint_rows.append(fail_row_from_meta(code_fmt, stock_name, meta))
                checkpoint_dirty = True

            processed += 1
            pending_processed += 1
            _print_progress("相对强弱", processed, total_all, passed=len(selected_rows))
            maybe_flush_checkpoint()
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {}
            for code in pending_codes:
                code_fmt = format_stock_code(code)
                stock_name = code_name_map.get(code_fmt, "")
                fut = ex.submit(
                    _evaluate_single_code,
                    code,
                    stock_name,
                    index_close_df,
                    start_date,
                    end_date,
                    price_lower_limit,
                    price_upper_limit,
                    min_down_ratio,
                    bs_request_interval_seconds,
                    bs_max_retries,
                )
                futures[fut] = (code_fmt, stock_name)

            for fut in as_completed(futures):
                code_fmt, stock_name = futures[fut]
                try:
                    _code_fmt, passed, meta = fut.result()
                    if passed:
                        print(
                            f"[相对强弱] PASS {code_fmt} {stock_name} | "
                            f"上涨满足率={meta.get('up_ratio', 0):.2f} 抗跌满足率={meta.get('down_ratio', 0):.2f} "
                            f"对齐交易日={meta.get('overlap_days', 0)}"
                        )
                        pass_row = row_from_meta(meta)
                        selected_rows.append(pass_row)
                        checkpoint_rows.append(pass_row)
                        checkpoint_dirty = True
                    else:
                        print(f"[相对强弱] FAIL {code_fmt} {stock_name} | {meta}")
                        checkpoint_rows.append(fail_row_from_meta(code_fmt, stock_name, meta))
                        checkpoint_dirty = True
                except Exception as exc:
                    print(f"[相对强弱] FAIL {code_fmt} {stock_name} | 评估异常: {exc}")
                    checkpoint_rows.append(
                        {
                            "股票代码": code_fmt,
                            "股票名称": stock_name,
                            RS_CHECKPOINT_PASS_COL: 0,
                            "原因": f"exception: {exc}",
                        }
                    )
                    checkpoint_dirty = True

                processed += 1
                pending_processed += 1
                _print_progress("相对强弱", processed, total_all, passed=len(selected_rows))
                maybe_flush_checkpoint()

    maybe_flush_checkpoint(force=True)

    return selected_rows


def print_param_warnings() -> None:
    if abs(RS_UP_TOL) >= 0.2:
        print(f"[参数警告] RS_UP_TOL={RS_UP_TOL} 看起来过大（日收益率小数制：0.01=1%）。")
    if abs(RS_DOWN_OUTPERF) >= 0.2:
        print(f"[参数警告] RS_DOWN_OUTPERF={RS_DOWN_OUTPERF} 看起来过大（日收益率小数制：0.01=1%）。")


def main() -> None:
    args = parse_args()
    socket.setdefaulttimeout(max(3.0, float(args.bs_timeout_seconds)))
    STOCK_DATA_DIR.mkdir(parents=True, exist_ok=True)

    print_param_warnings()

    now = datetime.now()
    today_text = now.strftime("%Y%m%d")
    report_date_profit, report_date_holder, zcfz_dates, current_year = resolve_report_dates(now)

    print(
        f"参数: {args.price_lower_limit}<=price<{args.price_upper_limit}, debt<{args.debt_asset_ratio_limit}, "
        f"index={args.index_code}, lookback={args.rs_lookback_days}, "
        f"workers={max(1, int(args.max_workers))}, holder_workers={max(1, int(args.holder_max_workers))}, "
        f"min_down_ratio={args.min_down_ratio}, resume={bool(args.resume)}, disable_rs={bool(args.disable_rs)}, seed_csv={bool(args.seed_csv)}, "
        f"bs_timeout={args.bs_timeout_seconds}s, bs_interval={args.bs_request_interval_seconds}s, bs_retries={args.bs_max_retries}"
    )

    min_down_ratio = float(args.min_down_ratio)
    if min_down_ratio > 1.5:
        min_down_ratio = min_down_ratio / 100.0
    min_down_ratio = max(0.0, min(min_down_ratio, 1.0))

    seed_codes: list[str] = []
    seed_name_map: dict[str, str] = {}
    if args.seed_csv:
        seed_codes, seed_name_map = load_seed_candidates(args.seed_csv)

    if seed_codes:
        print("[前置筛选] 已启用 seed-csv，跳过资金流/基本面/股东筛选。")
        final_candidate_codes = seed_codes
        code_name_map = dict(seed_name_map)
    else:
        fund_flow_codes = get_fund_flow_codes(args.price_upper_limit, args.price_lower_limit, args.sleep_seconds)
        cashflow_codes, profit_codes, zcfz_codes, profit_forecast_codes = get_fundamental_codes(
            args.debt_asset_ratio_limit,
            report_date_profit,
            zcfz_dates,
            current_year,
            args.sleep_seconds,
        )

        candidate_codes = build_candidate_codes(
            cashflow_codes,
            profit_codes,
            zcfz_codes,
            profit_forecast_codes,
            fund_flow_codes,
        )

        final_candidate_codes = filter_by_shareholders(
            candidate_codes,
            report_date_holder,
            args.sleep_seconds,
            max_workers=max(1, int(args.holder_max_workers)),
        )
        code_name_map = get_code_name_map()

    if final_candidate_codes and any(not code_name_map.get(format_stock_code(c), "") for c in final_candidate_codes):
        # seed模式优先只读本地缓存，避免重复触发远端接口调用。
        fallback_map = get_code_name_map_from_cache() if seed_codes else get_code_name_map()
        if fallback_map:
            for k, v in fallback_map.items():
                if k not in code_name_map or not code_name_map.get(k):
                    code_name_map[k] = v

    selected_rows: list[dict] = []
    lg = bs.login()
    print("login respond error_code:" + lg.error_code)
    print("login respond  error_msg:" + lg.error_msg)
    try:
        if args.disable_rs:
            print("[相对强弱] 已关闭，仅输出股东过滤后的候选。")
            for code in final_candidate_codes:
                selected_rows.append(
                    {
                        "股票代码": format_stock_code(code),
                        "股票名称": code_name_map.get(format_stock_code(code), ""),
                        "上涨满足率": None,
                        "抗跌满足率": None,
                        "对齐交易日": None,
                        "指数上涨日数(对齐后)": None,
                        "指数下跌日数(对齐后)": None,
                        "个股上涨日数(对齐后)": None,
                        "个股下跌日数(对齐后)": None,
                    }
                )
        else:
            selected_rows = run_relative_strength(
                final_candidate_codes,
                code_name_map,
                args.index_code,
                args.rs_lookback_days,
                today_text,
                max_workers=max(1, int(args.max_workers)),
                resume=bool(args.resume),
                price_lower_limit=float(args.price_lower_limit),
                price_upper_limit=float(args.price_upper_limit),
                min_down_ratio=min_down_ratio,
                bs_request_interval_seconds=float(args.bs_request_interval_seconds),
                bs_max_retries=int(args.bs_max_retries),
            )
    finally:
        bs.logout()

    if selected_rows:
        out_df = pd.DataFrame(selected_rows)
        if RS_CHECKPOINT_PASS_COL in out_df.columns:
            out_df = out_df.drop(columns=[RS_CHECKPOINT_PASS_COL], errors="ignore")
        if "抗跌满足率" in out_df.columns and "上涨满足率" in out_df.columns:
            out_df = out_df.sort_values(["抗跌满足率", "上涨满足率"], ascending=False, na_position="last")
        out_path = STOCK_DATA_DIR / f"Stock-Selection-Relativity-{today_text}.csv"
        out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"\n{out_path.name} 文件已保存")
        print(out_df)
    else:
        print("\n没有选出符合相对强弱策略的股票。")


if __name__ == "__main__":
    main()
