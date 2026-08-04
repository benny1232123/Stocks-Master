from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from core.indicators import calc_bollinger


DEFAULT_BACKTEST_HORIZONS = (5, 10, 20)


def _normalize_horizons(horizons: tuple[int, ...] | list[int] | None) -> list[int]:
    if not horizons:
        return list(DEFAULT_BACKTEST_HORIZONS)
    cleaned = sorted({int(item) for item in horizons if int(item) > 0})
    return cleaned or list(DEFAULT_BACKTEST_HORIZONS)


def _to_date_text(value: object) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if pd.isna(value):
        return ""
    return str(value)


def classify_boll_signals(
    boll_df: pd.DataFrame,
    near_ratio: float = 1.015,
    upper_near_ratio: float = 0.985,
    suppress_consecutive_selected: bool = True,
) -> pd.DataFrame:
    if boll_df is None or boll_df.empty:
        return pd.DataFrame(columns=["date", "close", "Lower", "Upper", "signal_type", "selected_raw", "selected"])

    required_columns = {"close", "Lower", "Upper"}
    if not required_columns.issubset(boll_df.columns):
        raise ValueError("输入数据缺少 backtest 所需列: close/Lower/Upper")

    out = boll_df.copy()
    if "date" not in out.columns:
        out["date"] = out.index.astype(str)

    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out["Lower"] = pd.to_numeric(out["Lower"], errors="coerce")
    out["Upper"] = pd.to_numeric(out["Upper"], errors="coerce")
    out = out.dropna(subset=["close"]).reset_index(drop=True)

    out["signal_type"] = "insufficient"
    out["selected_raw"] = False
    out["selected"] = False

    valid_mask = out["Lower"].notna() & out["Upper"].notna()
    oversold_mask = valid_mask & (out["close"] < out["Lower"])
    near_lower_mask = valid_mask & (~oversold_mask) & (out["close"] <= out["Lower"] * float(near_ratio))
    overbought_mask = valid_mask & (~oversold_mask) & (~near_lower_mask) & (out["close"] > out["Upper"])
    near_upper_mask = (
        valid_mask
        & (~oversold_mask)
        & (~near_lower_mask)
        & (~overbought_mask)
        & (out["close"] >= out["Upper"] * float(upper_near_ratio))
    )
    neutral_mask = valid_mask & (~oversold_mask) & (~near_lower_mask) & (~overbought_mask) & (~near_upper_mask)

    out.loc[oversold_mask, "signal_type"] = "oversold"
    out.loc[near_lower_mask, "signal_type"] = "near_lower"
    out.loc[overbought_mask, "signal_type"] = "overbought"
    out.loc[near_upper_mask, "signal_type"] = "near_upper"
    out.loc[neutral_mask, "signal_type"] = "neutral"
    out.loc[oversold_mask | near_lower_mask, "selected_raw"] = True

    if suppress_consecutive_selected:
        previous_selected = out["selected_raw"].shift(1, fill_value=False).astype(bool)
        out["selected"] = out["selected_raw"].astype(bool) & (~previous_selected)
    else:
        out["selected"] = out["selected_raw"].astype(bool)

    return out


def backtest_boll_signals(
    boll_df: pd.DataFrame,
    horizons: tuple[int, ...] | list[int] | None = None,
    near_ratio: float = 1.015,
    upper_near_ratio: float = 0.985,
    suppress_consecutive_selected: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    signal_df = classify_boll_signals(
        boll_df,
        near_ratio=near_ratio,
        upper_near_ratio=upper_near_ratio,
        suppress_consecutive_selected=suppress_consecutive_selected,
    )
    horizon_list = _normalize_horizons(horizons)

    if signal_df.empty:
        empty_summary = pd.DataFrame(
            columns=[
                "持有天数",
                "信号样本",
                "有效样本",
                "胜率(%)",
                "平均收益(%)",
                "中位收益(%)",
                "平均最大回撤(%)",
            ]
        )
        empty_details = pd.DataFrame(
            columns=[
                "信号日期",
                "信号类型",
                "入场价",
                "持有天数",
                "离场价",
                "区间收益(%)",
                "区间最大回撤(%)",
            ]
        )
        return empty_summary, empty_details

    selected_indexes = signal_df.index[signal_df["selected"]].tolist()
    close_series = signal_df["close"].astype(float)

    details_rows: list[dict[str, object]] = []
    for index in selected_indexes:
        entry_price = float(close_series.iloc[index])
        signal_type = str(signal_df.iloc[index].get("signal_type", ""))
        signal_date = _to_date_text(signal_df.iloc[index].get("date", ""))

        if entry_price <= 0:
            continue

        for horizon in horizon_list:
            exit_index = index + int(horizon)
            if exit_index >= len(signal_df):
                continue

            exit_price = float(close_series.iloc[exit_index])
            forward_return = exit_price / entry_price - 1.0

            lookahead_window = close_series.iloc[index + 1 : exit_index + 1]
            if lookahead_window.empty:
                max_drawdown = 0.0
            else:
                min_relative = float(lookahead_window.min() / entry_price - 1.0)
                max_drawdown = min(min_relative, 0.0)

            details_rows.append(
                {
                    "信号日期": signal_date,
                    "信号类型": signal_type,
                    "入场价": round(entry_price, 4),
                    "持有天数": int(horizon),
                    "离场价": round(exit_price, 4),
                    "区间收益(%)": round(forward_return * 100, 3),
                    "区间最大回撤(%)": round(max_drawdown * 100, 3),
                }
            )

    details_df = pd.DataFrame(details_rows)

    summary_rows: list[dict[str, object]] = []
    signal_count = len(selected_indexes)
    for horizon in horizon_list:
        if details_df.empty:
            horizon_df = pd.DataFrame()
        else:
            horizon_df = details_df[details_df["持有天数"] == int(horizon)]

        valid_count = int(len(horizon_df))
        if valid_count == 0:
            win_rate = 0.0
            avg_return = 0.0
            median_return = 0.0
            avg_drawdown = 0.0
        else:
            returns = horizon_df["区间收益(%)"].astype(float)
            drawdowns = horizon_df["区间最大回撤(%)"].astype(float)
            win_rate = float((returns > 0).mean() * 100)
            avg_return = float(returns.mean())
            median_return = float(returns.median())
            avg_drawdown = float(drawdowns.mean())

        summary_rows.append(
            {
                "持有天数": int(horizon),
                "信号样本": signal_count,
                "有效样本": valid_count,
                "胜率(%)": round(win_rate, 2),
                "平均收益(%)": round(avg_return, 3),
                "中位收益(%)": round(median_return, 3),
                "平均最大回撤(%)": round(avg_drawdown, 3),
            }
        )

    summary_df = pd.DataFrame(summary_rows)
    return summary_df, details_df


def backtest_from_k_data(
    k_df: pd.DataFrame,
    window: int = 20,
    k: float = 1.645,
    near_ratio: float = 1.015,
    upper_near_ratio: float = 0.985,
    horizons: tuple[int, ...] | list[int] | None = None,
    suppress_consecutive_selected: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    boll_df = calc_bollinger(k_df, window=window, k=k)
    summary_df, details_df = backtest_boll_signals(
        boll_df,
        horizons=horizons,
        near_ratio=near_ratio,
        upper_near_ratio=upper_near_ratio,
        suppress_consecutive_selected=suppress_consecutive_selected,
    )
    return boll_df, summary_df, details_df
