from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re

ROOT_DIR = Path(__file__).resolve().parent.parent
STOCK_DATA_DIR = ROOT_DIR / "stock_data"
INDEX_PATH = STOCK_DATA_DIR / "INDEX.md"

DATE_SUFFIX_RE = re.compile(r"^(?P<prefix>.+)-(?P<date>\d{8})\.(?P<ext>[A-Za-z0-9]+)$")
NEWS_RE = re.compile(r"^(?P<date>\d{8})_news\.(?P<ext>[A-Za-z0-9]+)$")


@dataclass
class DataFile:
    path: Path
    name: str
    size: int
    mtime: datetime
    prefix: str
    date_str: str | None


def _human_size(size: int) -> str:
    units = ["B", "KB", "MB", "GB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f}{unit}" if unit != "B" else f"{int(value)}B"
        value /= 1024
    return f"{size}B"


def _parse_file(path: Path) -> DataFile:
    name = path.name
    stat = path.stat()
    mtime = datetime.fromtimestamp(stat.st_mtime)

    prefix = "misc"
    date_str = None

    m = DATE_SUFFIX_RE.match(name)
    if m:
        prefix = m.group("prefix")
        date_str = m.group("date")
    else:
        m = NEWS_RE.match(name)
        if m:
            prefix = "news"
            date_str = m.group("date")
        else:
            prefix = path.stem

    return DataFile(
        path=path,
        name=name,
        size=stat.st_size,
        mtime=mtime,
        prefix=prefix,
        date_str=date_str,
    )


def _collect_files() -> list[DataFile]:
    files = []
    for path in STOCK_DATA_DIR.iterdir():
        if path.is_file() and path.name != "INDEX.md":
            files.append(_parse_file(path))
    return files


def _latest_by_prefix(files: list[DataFile]) -> dict[str, DataFile]:
    grouped: dict[str, list[DataFile]] = defaultdict(list)
    for item in files:
        grouped[item.prefix].append(item)

    latest: dict[str, DataFile] = {}
    for prefix, items in grouped.items():
        items.sort(
            key=lambda x: (
                x.date_str or "00000000",
                x.mtime,
                x.name,
            ),
            reverse=True,
        )
        latest[prefix] = items[0]
    return latest


def _folder_stats() -> list[tuple[str, int, int]]:
    rows: list[tuple[str, int, int]] = []
    for path in sorted(STOCK_DATA_DIR.iterdir(), key=lambda p: p.name.lower()):
        if not path.is_dir():
            continue
        total_size = 0
        count = 0
        for file in path.rglob("*"):
            if file.is_file():
                count += 1
                total_size += file.stat().st_size
        rows.append((path.name, count, total_size))
    return rows


def _build_markdown(files: list[DataFile], top_n_dates: int) -> str:
    now = datetime.now()
    today = now.strftime("%Y%m%d")
    latest = _latest_by_prefix(files)

    by_date: dict[str, list[DataFile]] = defaultdict(list)
    for item in files:
        if item.date_str:
            by_date[item.date_str].append(item)

    date_keys = sorted(by_date.keys(), reverse=True)
    today_files = sorted(by_date.get(today, []), key=lambda x: x.name)

    lines: list[str] = []
    lines.append("# stock_data 快速索引")
    lines.append("")
    lines.append(f"- 更新时间: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- 顶层文件数: {len(files)}")
    lines.append("")

    lines.append("## 今日新增")
    lines.append("")
    if not today_files:
        lines.append(f"- 今日({today})暂无日期命名文件。")
    else:
        for item in today_files:
            lines.append(f"- {item.name} ({_human_size(item.size)})")
    lines.append("")

    lines.append("## 每类最新文件")
    lines.append("")
    lines.append("| 数据类 | 最新文件 | 日期 | 大小 |")
    lines.append("|---|---|---:|---:|")
    for prefix in sorted(latest.keys(), key=lambda s: s.lower()):
        item = latest[prefix]
        date_show = item.date_str if item.date_str else "-"
        lines.append(
            f"| {prefix} | {item.name} | {date_show} | {_human_size(item.size)} |"
        )
    lines.append("")

    lines.append(f"## 最近 {top_n_dates} 天文件")
    lines.append("")
    if not date_keys:
        lines.append("- 暂无日期命名文件。")
    else:
        for date_key in date_keys[:top_n_dates]:
            lines.append(f"### {date_key}")
            for item in sorted(by_date[date_key], key=lambda x: x.name):
                lines.append(f"- {item.name} ({_human_size(item.size)})")
            lines.append("")

    lines.append("## 子目录统计")
    lines.append("")
    folder_rows = _folder_stats()
    if not folder_rows:
        lines.append("- 无子目录。")
    else:
        lines.append("| 子目录 | 文件数 | 总大小 |")
        lines.append("|---|---:|---:|")
        for folder_name, count, total_size in folder_rows:
            lines.append(f"| {folder_name} | {count} | {_human_size(total_size)} |")
    lines.append("")

    lines.append("## 快速建议")
    lines.append("")
    lines.append("- 先看‘今日新增’，确认今天是否已产出核心结果。")
    lines.append("- 再看‘每类最新文件’，快速定位你要的那一类数据。")
    lines.append("- 如果空间增长明显，优先看 `plots/` 与 `auto_logs/`。")

    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a readable index for stock_data.")
    parser.add_argument(
        "--top-dates",
        type=int,
        default=7,
        help="How many recent dates to display (default: 7).",
    )
    args = parser.parse_args()

    if not STOCK_DATA_DIR.exists() or not STOCK_DATA_DIR.is_dir():
        print(f"stock_data not found: {STOCK_DATA_DIR}")
        return 1

    files = _collect_files()
    markdown = _build_markdown(files, top_n_dates=max(1, args.top_dates))
    INDEX_PATH.write_text(markdown, encoding="utf-8")

    print(f"[OK] Index written: {INDEX_PATH}")
    print(f"[INFO] Top-level files indexed: {len(files)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
