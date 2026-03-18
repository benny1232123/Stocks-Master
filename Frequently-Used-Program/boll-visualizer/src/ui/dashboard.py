from __future__ import annotations

import pandas as pd
import streamlit as st


def parse_codes_input(text: str) -> list[str]:
    separators = [",", "，", "\n", "\t", " ", "；", ";"]
    normalized = text
    for sep in separators[1:]:
        normalized = normalized.replace(sep, separators[0])

    raw_codes = [item.strip() for item in normalized.split(separators[0]) if item.strip()]
    result: list[str] = []
    seen: set[str] = set()
    for code in raw_codes:
        digits = "".join(ch for ch in code if ch.isdigit()).zfill(6)
        if len(digits) != 6:
            continue
        if digits in seen:
            continue
        seen.add(digits)
        result.append(digits)
    return result


def render_overview_metrics(result_df: pd.DataFrame) -> None:
    total = int(len(result_df))
    hit = int(result_df["命中策略"].sum()) if (not result_df.empty and "命中策略" in result_df.columns) else 0
    hit_ratio = f"{(hit / total * 100):.1f}%" if total else "0.0%"

    has_score = (not result_df.empty) and ("综合评分" in result_df.columns)
    avg_score = round(float(result_df["综合评分"].mean()), 2) if has_score else None

    if has_score:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("分析股票数", total)
        col2.metric("命中策略数", hit)
        col3.metric("命中率", hit_ratio)
        col4.metric("平均评分", avg_score)
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("分析股票数", total)
        col2.metric("命中策略数", hit)
        col3.metric("命中率", hit_ratio)


def to_export_csv_bytes(result_df: pd.DataFrame) -> bytes:
    if result_df.empty:
        return b""
    return result_df.to_csv(index=False).encode("utf-8-sig")
