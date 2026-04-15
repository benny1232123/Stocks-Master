from __future__ import annotations

from pathlib import Path

import pandas as pd


def format_stock_code(code) -> str:
    if isinstance(code, str):
        digits = "".join(ch for ch in code if ch.isdigit())
        return digits.zfill(6)
    if isinstance(code, int):
        return f"{code:06d}"
    text = str(code)
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else text


def normalize_code_series(series: pd.Series) -> pd.Series:
    return series.astype(str).apply(format_stock_code)


def load_checkpoint_df(checkpoint_path: Path) -> pd.DataFrame:
    if not checkpoint_path.exists():
        return pd.DataFrame()
    try:
        checkpoint_df = pd.read_csv(checkpoint_path, encoding="utf-8-sig")
        if "股票代码" in checkpoint_df.columns:
            checkpoint_df["股票代码"] = normalize_code_series(checkpoint_df["股票代码"])
        return checkpoint_df
    except Exception:
        return pd.DataFrame()


def save_checkpoint_df(checkpoint_path: Path, checkpoint_df: pd.DataFrame) -> None:
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint_df.to_csv(checkpoint_path, index=False, encoding="utf-8-sig")


def merge_result_rows(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    *,
    code_col: str = "股票代码",
    sort_cols: list[str] | None = None,
) -> pd.DataFrame:
    if existing_df.empty:
        merged = new_df.copy()
    elif new_df.empty:
        merged = existing_df.copy()
    else:
        merged = pd.concat([existing_df, new_df], ignore_index=True)

    if merged.empty:
        return merged

    if code_col in merged.columns:
        merged[code_col] = normalize_code_series(merged[code_col])
        merged = merged.drop_duplicates(subset=[code_col], keep="last")

    if sort_cols:
        present_sort_cols = [col for col in sort_cols if col in merged.columns]
        if present_sort_cols:
            ascending = [False if col in {"命中策略", "综合评分"} else True for col in present_sort_cols]
            merged = merged.sort_values(by=present_sort_cols, ascending=ascending)

    return merged.reset_index(drop=True)
