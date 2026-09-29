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


def _csv_has_data_row(path: Path) -> bool:
    """轻量判断 CSV 是否含至少一行数据（只读头部 4KB，数换行数）。

    只有一行 = 仅表头（或空文件）→ False。带 BOM 的 utf-8 不影响换行计数。
    """
    try:
        with open(path, "rb") as f:
            head = f.read(4096)
        if len(head) < 4096:
            # CRLF/LF/CR 统一成 LF 再数行，否则表头一行的 CRLF 会被 \r+\n 数成 2 行
            lines = head.replace(b"\r\n", b"\n").replace(b"\r", b"\n").count(b"\n")
            return lines > 1
        return True  # 头部即超 4KB，必然远超一行
    except OSError:
        return True  # 读不了就别拦，按旧行为放行


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

    # 空CSV不配当"最新"（2026-09-29）：archive/ 里的备份可能含只有表头的空清单
    # （如 pre_menu_swap_20260927/DAL/Daily-Action-List-20260925.csv，周六空跑产物），
    # 按日期排序它会压过真实的最新文件，端点/快照把"最新日报"选成空文件 —— 与
    # /api/backtests/latest 的"空快照自我毒化"同型。故按新→旧跳过无数据行的空CSV，
    # 全部为空时退回旧行为（返回最新那个），不改变"找不到"语义。
    for _, _, mtime, latest_path in candidates:
        if latest_path.suffix.lower() == ".csv" and not _csv_has_data_row(latest_path):
            continue
        return ArtifactFile(
            name=latest_path.name,
            path=str(latest_path.relative_to(PROJECT_ROOT)),
            modified_at=mtime,
        )

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