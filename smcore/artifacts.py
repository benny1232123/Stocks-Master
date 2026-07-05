"""Helpers for locating generated artifact files under stock_data/ and archive/."""
from __future__ import annotations

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


def find_latest_file(pattern: str) -> ArtifactFile | None:
    """Find the newest file matching a glob pattern under stock_data/ and archive/."""
    latest_path: Path | None = None
    latest_mtime = -1.0

    for path in _candidate_paths(pattern):
        try:
            mtime = path.stat().st_mtime
        except FileNotFoundError:
            continue
        if mtime > latest_mtime:
            latest_path = path
            latest_mtime = mtime

    if latest_path is None:
        return None

    return ArtifactFile(
        name=latest_path.name,
        path=str(latest_path.relative_to(PROJECT_ROOT)),
        modified_at=latest_mtime,
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
        return pd.read_csv(csv_path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()