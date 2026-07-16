"""Helpers for locating generated artifact files under stock_data/ and archive/."""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STOCK_DATA_DIR = PROJECT_ROOT / "stock_data"


@dataclass(frozen=True)
class ArtifactFile:
    name: str
    path: str
    modified_at: float


def _candidate_paths(pattern: str) -> Iterable[Path]:
    yield from STOCK_DATA_DIR.glob(pattern)
    archive_dir = STOCK_DATA_DIR / "archive"
    if archive_dir.exists():
        yield from archive_dir.rglob(pattern)


def _extract_date_tag(name: str) -> str | None:
    """从文件名提取 YYYYMMDD 日期标签（如 Daily-Action-List-20260709.csv → 20260709）。"""
    m = re.search(r"(\d{8})", name)
    return m.group(1) if m else None


def find_latest_file(pattern: str) -> ArtifactFile | None:
    """Find the newest file matching a glob pattern under stock_data/ and archive/.

    排序规则：文件名含 YYYYMMDD 日期标签的，按日期降序优先（如 Daily-Action-List 的日常日报，
    git 拉取后 mtime 会被重置，必须按日期而非 mtime 选最新）；不含日期的按 mtime 降序（保持原行为）。
    """
    candidates: list[tuple[int, str, float, Path]] = []
    for path in _candidate_paths(pattern):
        try:
            mtime = path.stat().st_mtime
        except FileNotFoundError:
            continue
        date_tag = _extract_date_tag(path.name)
        # 优先级 1=含日期（按日期字符串降序），0=无日期（按 mtime 字符串降序）
        priority = 1 if date_tag else 0
        sort_key = date_tag if date_tag else f"{mtime:018.6f}"
        candidates.append((priority, sort_key, mtime, path))

    if not candidates:
        return None

    # reverse=True：含日期者(priority=1)恒优先；同组内日期/ mtime 降序
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    _, _, mtime, latest_path = candidates[0]

    return ArtifactFile(
        name=latest_path.name,
        path=str(latest_path.relative_to(PROJECT_ROOT)),
        modified_at=mtime,
    )


def find_latest_file_any(patterns: Iterable[str]) -> ArtifactFile | None:
    """Find the newest file across several glob patterns."""
    latest: ArtifactFile | None = None
    for pattern in patterns:
        candidate = find_latest_file(pattern)
        if candidate is None:
            continue
        if latest is None or candidate.modified_at > latest.modified_at:
            latest = candidate
    return latest


def preview_csv(path: str, limit: int = 20) -> dict:
    """Read a small CSV preview for the frontend."""
    csv_path = PROJECT_ROOT / path
    if not csv_path.exists():
        return {"rows": [], "columns": []}

    frame = pd.read_csv(csv_path)
    if frame.empty:
        return {"rows": [], "columns": frame.columns.tolist()}

    # 股票代码列归一化：与 read_csv_file 保持一致，避免前端 Hero 预览表
    # 显示 566 而非 000566（pandas 把 '000566' 推断成 int 丢前导零）。
    from smcore.utils.code import format_stock_code

    for col in frame.columns:
        if "股票代码" in col or col == "代码":
            frame[col] = frame[col].map(lambda x: format_stock_code(x))

    return {
        "columns": frame.columns.tolist(),
        "rows": frame.head(limit).to_dict(orient="records"),
    }


def read_csv_file(path: str) -> pd.DataFrame:
    """Read a CSV file relative to the project root."""
    csv_path = PROJECT_ROOT / path
    if not csv_path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()
    # 股票代码列：pandas 会把 '000915' 推断成 int 915 丢前导零，导致下游
    # fetch_daily_k('915') 拉不到数据、前端显示成 915。统一归一为 6 位字符串。
    from smcore.utils.code import format_stock_code

    for col in df.columns:
        if "股票代码" in col or col == "代码":
            df[col] = df[col].map(lambda x: format_stock_code(x))
    return df