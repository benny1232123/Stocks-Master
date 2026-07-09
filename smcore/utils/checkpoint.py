"""断点续跑 CSV 读写工具 —— 由 Frequently-Used-Program/strategy_common.py 提炼而来。

此前各选股脚本散落自己的 checkpoint 读写逻辑，本模块统一为单一入口，
供 smcore/strategies/relativity.py 等复用。
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from smcore.utils.code import normalize_code_series


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
