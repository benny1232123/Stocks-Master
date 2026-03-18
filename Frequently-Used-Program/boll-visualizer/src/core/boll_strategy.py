from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Callable

import baostock as bs
import pandas as pd

from core.data_fetcher import fetch_daily_k_data, fetch_daily_k_data_in_session, format_stock_code
from core.indicators import calc_bollinger, evaluate_boll_signal


ProgressCallback = Callable[[str, int, int, str], None]


def _empty_summary(code_6: str, stock_name: str, signal: str = "无数据") -> dict[str, object]:
    return {
        "股票代码": code_6,
        "股票名称": stock_name,
        "最新收盘": None,
        "下轨": None,
        "上轨": None,
        "信号": signal,
        "命中策略": False,
    }


def _summary_from_chart(
    code_6: str,
    stock_name: str,
    chart_df: pd.DataFrame,
    signal_info: dict[str, object],
) -> dict[str, object]:
    latest = chart_df.iloc[-1]
    return {
        "股票代码": code_6,
        "股票名称": stock_name,
        "最新收盘": round(float(latest["close"]), 3),
        "下轨": round(float(latest["Lower"]), 3) if pd.notna(latest["Lower"]) else None,
        "上轨": round(float(latest["Upper"]), 3) if pd.notna(latest["Upper"]) else None,
        "信号": signal_info["signal"],
        "命中策略": bool(signal_info["selected"]),
    }


def _analyze_single_code(
    code: str,
    start_date: str | date,
    end_date: str | date,
    window: int,
    k: float,
    near_ratio: float,
    adjust: str,
    stock_name: str,
    use_cache: bool,
    force_refresh: bool,
    cache_max_age_hours: float,
) -> tuple[str, dict[str, object], pd.DataFrame]:
    code_6 = format_stock_code(code)
    raw_df = fetch_daily_k_data(
        code_6,
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
        use_cache=use_cache,
        force_refresh=force_refresh,
        max_cache_age_hours=cache_max_age_hours,
    )

    if raw_df.empty:
        return code_6, _empty_summary(code_6, stock_name, signal="无数据"), raw_df

    chart_df = calc_bollinger(raw_df, window=window, k=k)
    signal_info = evaluate_boll_signal(chart_df, near_ratio=near_ratio)
    return code_6, _summary_from_chart(code_6, stock_name, chart_df, signal_info), chart_df


def analyze_stock(
    code: str,
    start_date: str | date,
    end_date: str | date,
    window: int = 20,
    k: float = 1.645,
    near_ratio: float = 1.015,
    adjust: str = "qfq",
    stock_name: str = "",
    use_cache: bool = True,
    force_refresh: bool = False,
    cache_max_age_hours: float = 24.0,
) -> tuple[pd.DataFrame, dict[str, object]]:
    code_6, summary, chart_df = _analyze_single_code(
        code=code,
        start_date=start_date,
        end_date=end_date,
        window=window,
        k=k,
        near_ratio=near_ratio,
        adjust=adjust,
        stock_name=stock_name,
        use_cache=use_cache,
        force_refresh=force_refresh,
        cache_max_age_hours=cache_max_age_hours,
    )
    return chart_df, summary


def analyze_stocks(
    codes: list[str],
    start_date: str | date,
    end_date: str | date,
    window: int = 20,
    k: float = 1.645,
    near_ratio: float = 1.015,
    adjust: str = "qfq",
    code_name_map: dict[str, str] | None = None,
    use_cache: bool = True,
    force_refresh: bool = False,
    cache_max_age_hours: float = 24.0,
    max_workers: int = 1,
    retain_all_charts: bool = True,
    progress_callback: ProgressCallback | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    code_name_map = code_name_map or {}
    rows: list[dict[str, object]] = []
    data_map: dict[str, pd.DataFrame] = {}
    total_codes = len(codes)
    worker_count = max(1, int(max_workers))

    if progress_callback is not None:
        progress_callback("init", 0, total_codes, "初始化 Boll 任务")

    update_step = 1 if total_codes <= 500 else 10

    if worker_count <= 1:
        login_result = bs.login()
        session_ok = login_result.error_code == "0"

        try:
            for index, code in enumerate(codes, start=1):
                if progress_callback is not None and (index % update_step == 0 or index == total_codes):
                    progress_callback("evaluate", index, total_codes, f"仅Boll处理中：{code}")

                code_6 = format_stock_code(code)
                stock_name = code_name_map.get(code_6, "")

                if session_ok:
                    raw_df = fetch_daily_k_data_in_session(
                        code=code_6,
                        start_date=start_date,
                        end_date=end_date,
                        adjust=adjust,
                        use_cache=use_cache,
                        force_refresh=force_refresh,
                        max_cache_age_hours=cache_max_age_hours,
                    )
                    if raw_df.empty:
                        summary = _empty_summary(code_6, stock_name, signal="无数据")
                        chart_df = raw_df
                    else:
                        chart_df = calc_bollinger(raw_df, window=window, k=k)
                        signal_info = evaluate_boll_signal(chart_df, near_ratio=near_ratio)
                        summary = _summary_from_chart(code_6, stock_name, chart_df, signal_info)
                else:
                    code_6, summary, chart_df = _analyze_single_code(
                        code=code,
                        start_date=start_date,
                        end_date=end_date,
                        window=window,
                        k=k,
                        near_ratio=near_ratio,
                        adjust=adjust,
                        stock_name=stock_name,
                        use_cache=use_cache,
                        force_refresh=force_refresh,
                        cache_max_age_hours=cache_max_age_hours,
                    )

                rows.append(summary)
                if retain_all_charts or bool(summary.get("命中策略", False)):
                    data_map[code_6] = chart_df
        finally:
            if session_ok:
                bs.logout()
    else:
        with ThreadPoolExecutor(max_workers=min(worker_count, max(1, total_codes))) as executor:
            future_map = {
                executor.submit(
                    _analyze_single_code,
                    code,
                    start_date,
                    end_date,
                    window,
                    k,
                    near_ratio,
                    adjust,
                    code_name_map.get(format_stock_code(code), ""),
                    use_cache,
                    force_refresh,
                    cache_max_age_hours,
                ): code
                for code in codes
            }

            for done_count, future in enumerate(as_completed(future_map), start=1):
                input_code = future_map[future]
                try:
                    code_6, summary, chart_df = future.result()
                except Exception:
                    code_6 = format_stock_code(input_code)
                    summary = _empty_summary(code_6, code_name_map.get(code_6, ""), signal="数据获取异常")
                    chart_df = pd.DataFrame()

                rows.append(summary)
                if retain_all_charts or bool(summary.get("命中策略", False)):
                    data_map[code_6] = chart_df

                if progress_callback is not None and (done_count % update_step == 0 or done_count == total_codes):
                    progress_callback("evaluate", done_count, total_codes, f"仅Boll处理中：{code_6}")

    result_df = pd.DataFrame(rows)
    if not result_df.empty:
        result_df = result_df.sort_values(by=["命中策略", "股票代码"], ascending=[False, True]).reset_index(drop=True)

    if progress_callback is not None:
        progress_callback("done", total_codes, total_codes, "仅Boll分析完成")

    return result_df, data_map
