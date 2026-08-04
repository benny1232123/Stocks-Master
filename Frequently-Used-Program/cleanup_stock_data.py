import argparse
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
STOCK_DATA_DIR = ROOT_DIR / "stock_data"


@dataclass(frozen=True)
class CleanupRule:
    regex: re.Pattern
    date_format: str = "%Y%m%d"


DATE_RULES = [
    CleanupRule(re.compile(r"^Stock-Selection-Boll-([0-9]{8})\.csv$")),
    CleanupRule(re.compile(r"^Stock-Selection-Boll-All-([0-9]{8})\.csv$")),
    CleanupRule(re.compile(r"^Stock-Selection-Boll-All-Hits-([0-9]{8})\.csv$")),
    CleanupRule(re.compile(r"^CCTV-Hot-Sectors-([0-9]{8})\.(csv|md)$")),
    CleanupRule(re.compile(r"^CCTV-Sector-News-Matched-([0-9]{8})\.csv$")),
    CleanupRule(re.compile(r"^CCTV-Emerging-Keywords-([0-9]{8})\.csv$")),
    CleanupRule(re.compile(r"^CCTV-Emerging-Keyword-Suggestions-([0-9]{8})\.csv$")),
    CleanupRule(re.compile(r"^CCTV-Quality-Metrics-([0-9]{8})\.csv$")),
    CleanupRule(re.compile(r"^CCTV-Sector-Stock-Pool-([0-9]{8})\.csv$")),
    CleanupRule(re.compile(r"^CCTV-Backtest-([0-9]{8})\.csv$")),
    CleanupRule(re.compile(r"^([0-9]{8})_news\.csv$")),
]


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Clean historical files under stock_data by retention days.",
    )
    parser.add_argument(
        "--keep-days",
        type=int,
        default=30,
        help="Retention days for date-named files under stock_data (default: 30).",
    )
    parser.add_argument(
        "--log-keep-days",
        type=int,
        default=30,
        help="Retention days for files under stock_data/auto_logs (default: 30).",
    )
    parser.add_argument(
        "--plots-keep-days",
        type=int,
        default=30,
        help="Retention days for files under stock_data/plots by mtime (default: 30).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print files that would be deleted.",
    )
    return parser.parse_args()


def _safe_days(days, fallback):
    return days if days >= 0 else fallback


def _extract_date_from_name(name):
    for rule in DATE_RULES:
        match = rule.regex.match(name)
        if not match:
            continue
        date_text = match.group(1)
        try:
            return datetime.strptime(date_text, rule.date_format).date()
        except ValueError:
            return None
    return None


def _remove_file(path, dry_run):
    size = path.stat().st_size if path.exists() else 0
    if dry_run:
        print(f"[DRY-RUN] delete: {path}")
        return size

    try:
        path.unlink()
        print(f"[DELETED] {path}")
        return size
    except FileNotFoundError:
        return 0
    except OSError as exc:
        print(f"[SKIP] {path} ({exc})")
        return 0


def _cleanup_dated_files(data_dir, keep_days, dry_run):
    cutoff = datetime.now().date() - timedelta(days=keep_days)
    deleted_count = 0
    freed_bytes = 0

    for file_path in data_dir.iterdir():
        if not file_path.is_file():
            continue

        file_date = _extract_date_from_name(file_path.name)
        if file_date is None:
            continue

        if file_date < cutoff:
            freed_bytes += _remove_file(file_path, dry_run)
            deleted_count += 1

    return deleted_count, freed_bytes


def _cleanup_by_mtime(folder, keep_days, dry_run):
    if not folder.exists() or not folder.is_dir():
        return 0, 0

    cutoff_dt = datetime.now() - timedelta(days=keep_days)
    deleted_count = 0
    freed_bytes = 0

    for path in folder.rglob("*"):
        if not path.is_file():
            continue

        file_mtime = datetime.fromtimestamp(path.stat().st_mtime)
        if file_mtime < cutoff_dt:
            freed_bytes += _remove_file(path, dry_run)
            deleted_count += 1

    return deleted_count, freed_bytes


def _format_bytes(byte_count):
    if byte_count < 1024:
        return f"{byte_count} B"
    if byte_count < 1024 * 1024:
        return f"{byte_count / 1024:.2f} KB"
    return f"{byte_count / (1024 * 1024):.2f} MB"


def main():
    args = _parse_args()

    keep_days = _safe_days(args.keep_days, 30)
    log_keep_days = _safe_days(args.log_keep_days, keep_days)
    plots_keep_days = _safe_days(args.plots_keep_days, keep_days)

    if not STOCK_DATA_DIR.exists() or not STOCK_DATA_DIR.is_dir():
        print(f"stock_data not found: {STOCK_DATA_DIR}")
        return 1

    print("[cleanup] start")
    print(
        "[cleanup] retention => "
        f"dated:{keep_days}d, auto_logs:{log_keep_days}d, plots:{plots_keep_days}d"
    )

    total_count = 0
    total_bytes = 0

    count, size = _cleanup_dated_files(STOCK_DATA_DIR, keep_days, args.dry_run)
    print(f"[cleanup] dated files removed: {count}")
    total_count += count
    total_bytes += size

    auto_logs_dir = STOCK_DATA_DIR / "auto_logs"
    count, size = _cleanup_by_mtime(auto_logs_dir, log_keep_days, args.dry_run)
    print(f"[cleanup] auto_logs removed: {count}")
    total_count += count
    total_bytes += size

    plots_dir = STOCK_DATA_DIR / "plots"
    count, size = _cleanup_by_mtime(plots_dir, plots_keep_days, args.dry_run)
    print(f"[cleanup] plots removed: {count}")
    total_count += count
    total_bytes += size

    print(
        f"[cleanup] done, removed {total_count} files, reclaimed {_format_bytes(total_bytes)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
