import argparse
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
STOCK_DATA_DIR = ROOT_DIR / "stock_data"
ARCHIVE_ROOT_DIR = STOCK_DATA_DIR / "archive"


@dataclass(frozen=True)
class ArchiveRule:
    regex: re.Pattern
    category: str
    date_format: str = "%Y%m%d"


DATE_RULES = [
    ArchiveRule(re.compile(r"^Stock-Selection-Boll-([0-9]{8})\.csv$"), "boll"),
    ArchiveRule(re.compile(r"^Stock-Selection-Boll-All-([0-9]{8})\.csv$"), "boll"),
    ArchiveRule(re.compile(r"^Stock-Selection-Boll-All-Hits-([0-9]{8})\.csv$"), "boll"),
    ArchiveRule(re.compile(r"^Stock-Selection-Ashare-Theme-Turnover-([0-9]{8})\.csv$"), "theme"),
    ArchiveRule(re.compile(r"^Stock-Selection-Relativity-([0-9]{8})\.csv$"), "relativity"),
    ArchiveRule(re.compile(r"^Stock-Selection-Shared-Seed-([0-9]{8})\.csv$"), "seed"),
    ArchiveRule(re.compile(r"^CCTV-Hot-Sectors-([0-9]{8})\.(csv|md)$"), "cctv"),
    ArchiveRule(re.compile(r"^CCTV-Extra-News-([0-9]{8})\.csv$"), "cctv"),
    ArchiveRule(re.compile(r"^CCTV-Sector-News-Matched-([0-9]{8})\.csv$"), "cctv"),
    ArchiveRule(re.compile(r"^CCTV-Emerging-Keywords-([0-9]{8})\.csv$"), "cctv"),
    ArchiveRule(re.compile(r"^CCTV-Emerging-Keyword-Suggestions-([0-9]{8})\.csv$"), "cctv"),
    ArchiveRule(re.compile(r"^CCTV-Quality-Metrics-([0-9]{8})\.csv$"), "cctv"),
    ArchiveRule(re.compile(r"^CCTV-Sector-Stock-Pool-([0-9]{8})\.csv$"), "cctv"),
    ArchiveRule(re.compile(r"^CCTV-Backtest-([0-9]{8})\.csv$"), "cctv"),
    ArchiveRule(re.compile(r"^Signal-Backtest-UI-([0-9]{8})-[0-9A-Za-z_-]+\.csv$"), "backtest"),
    ArchiveRule(re.compile(r"^Trade-Backtest-UI-([0-9]{8})-[0-9A-Za-z_-]+\.csv$"), "backtest"),
    ArchiveRule(re.compile(r"^([0-9]{8})_news\.csv$"), "news"),
]

UNMATCHED_ROOT_CSV_SKIP = {
    "my_trades.template.csv",
}


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Archive old date-named files under stock_data to stock_data/archive.",
    )
    parser.add_argument(
        "--keep-root-days",
        type=int,
        default=7,
        help="Keep recent date files in stock_data root for this many days (default: 7).",
    )
    parser.add_argument(
        "--archive-keep-days",
        type=int,
        default=365,
        help="Delete archived date files older than this many days (default: 365).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print actions without moving/deleting files.",
    )
    parser.add_argument(
        "--secondary-level",
        action="store_true",
        default=True,
        help="Use archive/YYYYMM/<category>/file layout (default: enabled).",
    )
    parser.add_argument(
        "--no-secondary-level",
        action="store_false",
        dest="secondary_level",
        help="Disable secondary layout and keep archive/YYYYMM/file layout.",
    )
    parser.add_argument(
        "--archive-all-root-dated",
        action="store_true",
        help="Archive all date-named files in stock_data root, including today.",
    )
    parser.add_argument(
        "--archive-unmatched-root-csv",
        action="store_true",
        help="Also archive unmatched CSV files in stock_data root to archive/YYYYMM/misc.",
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


def _resolve_category(name):
    for rule in DATE_RULES:
        if rule.regex.match(name):
            return rule.category
    return "misc"


def _format_bytes(byte_count):
    if byte_count < 1024:
        return f"{byte_count} B"
    if byte_count < 1024 * 1024:
        return f"{byte_count / 1024:.2f} KB"
    return f"{byte_count / (1024 * 1024):.2f} MB"


def _build_archive_target(file_path, file_date, secondary_level):
    month_dir = ARCHIVE_ROOT_DIR / file_date.strftime("%Y%m")
    if secondary_level:
        month_dir = month_dir / _resolve_category(file_path.name)
    return month_dir / file_path.name


def _move_to_archive(file_path, dry_run, secondary_level):
    file_date = _extract_date_from_name(file_path.name)
    if file_date is None:
        return False, 0

    target_path = _build_archive_target(file_path, file_date, secondary_level)
    month_dir = target_path.parent
    size = file_path.stat().st_size

    if dry_run:
        print(f"[DRY-RUN] move: {file_path} -> {target_path}")
        return True, size

    month_dir.mkdir(parents=True, exist_ok=True)

    if target_path.exists():
        src_size = file_path.stat().st_size
        dst_size = target_path.stat().st_size
        if src_size == dst_size:
            file_path.unlink()
            print(f"[SKIP-MOVED] same target exists, removed source: {file_path}")
            return True, src_size

        stem = target_path.stem
        suffix = target_path.suffix
        ts = datetime.now().strftime("%H%M%S")
        target_path = target_path.with_name(f"{stem}-{ts}{suffix}")

    shutil.move(str(file_path), str(target_path))
    print(f"[ARCHIVED] {file_path} -> {target_path}")
    return True, size


def _archive_old_root_files(data_dir, keep_root_days, dry_run, secondary_level):
    cutoff = datetime.now().date() - timedelta(days=keep_root_days)
    moved_count = 0
    moved_bytes = 0

    for file_path in data_dir.iterdir():
        if not file_path.is_file():
            continue

        file_date = _extract_date_from_name(file_path.name)
        if file_date is None:
            continue

        if file_date < cutoff:
            moved, size = _move_to_archive(file_path, dry_run, secondary_level)
            if moved:
                moved_count += 1
                moved_bytes += size

    return moved_count, moved_bytes


def _archive_all_root_dated_files(data_dir, dry_run, secondary_level):
    moved_count = 0
    moved_bytes = 0

    for file_path in data_dir.iterdir():
        if not file_path.is_file():
            continue

        file_date = _extract_date_from_name(file_path.name)
        if file_date is None:
            continue

        moved, size = _move_to_archive(file_path, dry_run, secondary_level)
        if moved:
            moved_count += 1
            moved_bytes += size

    return moved_count, moved_bytes


def _archive_unmatched_root_csv_files(data_dir, dry_run):
    moved_count = 0
    moved_bytes = 0
    month_dir = ARCHIVE_ROOT_DIR / datetime.now().strftime("%Y%m") / "misc"

    for file_path in data_dir.iterdir():
        if not file_path.is_file():
            continue
        if file_path.suffix.lower() != ".csv":
            continue

        name = file_path.name
        if name in UNMATCHED_ROOT_CSV_SKIP:
            continue
        if _extract_date_from_name(name) is not None:
            continue

        size = file_path.stat().st_size
        target_path = month_dir / name
        if dry_run:
            print(f"[DRY-RUN] move unmatched csv: {file_path} -> {target_path}")
            moved_count += 1
            moved_bytes += size
            continue

        month_dir.mkdir(parents=True, exist_ok=True)
        if target_path.exists():
            src_size = file_path.stat().st_size
            dst_size = target_path.stat().st_size
            if src_size == dst_size:
                file_path.unlink()
                print(f"[SKIP-MOVED] same unmatched target exists, removed source: {file_path}")
                moved_count += 1
                moved_bytes += src_size
                continue

            stem = target_path.stem
            suffix = target_path.suffix
            ts = datetime.now().strftime("%H%M%S")
            target_path = target_path.with_name(f"{stem}-{ts}{suffix}")

        shutil.move(str(file_path), str(target_path))
        print(f"[ARCHIVED-UNMATCHED] {file_path} -> {target_path}")
        moved_count += 1
        moved_bytes += size

    return moved_count, moved_bytes


def _organize_existing_archive(archive_root, dry_run, secondary_level):
    if not secondary_level:
        return 0, 0

    if not archive_root.exists() or not archive_root.is_dir():
        return 0, 0

    moved_count = 0
    moved_bytes = 0

    for file_path in archive_root.rglob("*"):
        if not file_path.is_file():
            continue

        rel_parts = file_path.relative_to(archive_root).parts
        file_date = _extract_date_from_name(file_path.name)
        if file_date is None:
            continue

        month_name = file_date.strftime("%Y%m")
        if rel_parts and rel_parts[0] != month_name:
            continue

        expected = _build_archive_target(file_path, file_date, secondary_level)
        if file_path.resolve() == expected.resolve():
            continue

        size = file_path.stat().st_size
        if dry_run:
            print(f"[DRY-RUN] organize: {file_path} -> {expected}")
            moved_count += 1
            moved_bytes += size
            continue

        expected.parent.mkdir(parents=True, exist_ok=True)
        if expected.exists():
            src_size = file_path.stat().st_size
            dst_size = expected.stat().st_size
            if src_size == dst_size:
                file_path.unlink()
                print(f"[ORGANIZE-SKIP] same target exists, removed source: {file_path}")
                moved_count += 1
                moved_bytes += src_size
                continue

            stem = expected.stem
            suffix = expected.suffix
            ts = datetime.now().strftime("%H%M%S")
            expected = expected.with_name(f"{stem}-{ts}{suffix}")

        shutil.move(str(file_path), str(expected))
        print(f"[ORGANIZED] {file_path} -> {expected}")
        moved_count += 1
        moved_bytes += size

    # Remove empty month/category directories for readability.
    for dir_path in sorted(archive_root.rglob("*"), key=lambda p: len(p.parts), reverse=True):
        if dir_path.is_dir() and not any(dir_path.iterdir()):
            if dry_run:
                print(f"[DRY-RUN] rmdir empty: {dir_path}")
            else:
                dir_path.rmdir()

    return moved_count, moved_bytes


def _delete_archived_expired(archive_root, archive_keep_days, dry_run):
    if not archive_root.exists() or not archive_root.is_dir():
        return 0, 0

    cutoff = datetime.now().date() - timedelta(days=archive_keep_days)
    deleted_count = 0
    deleted_bytes = 0

    for file_path in archive_root.rglob("*"):
        if not file_path.is_file():
            continue

        file_date = _extract_date_from_name(file_path.name)
        if file_date is None:
            continue

        if file_date < cutoff:
            size = file_path.stat().st_size
            if dry_run:
                print(f"[DRY-RUN] delete archived: {file_path}")
            else:
                file_path.unlink()
                print(f"[DELETED] archived expired: {file_path}")
            deleted_count += 1
            deleted_bytes += size

    return deleted_count, deleted_bytes


def main():
    args = _parse_args()

    keep_root_days = _safe_days(args.keep_root_days, 7)
    archive_keep_days = _safe_days(args.archive_keep_days, 365)

    if not STOCK_DATA_DIR.exists() or not STOCK_DATA_DIR.is_dir():
        print(f"stock_data not found: {STOCK_DATA_DIR}")
        return 1

    print("[archive] start")
    print(
        "[archive] policy => "
        f"keep-root:{keep_root_days}d, archive-keep:{archive_keep_days}d, "
        f"secondary-level:{'on' if args.secondary_level else 'off'}, "
        f"archive-all-root-dated:{'on' if args.archive_all_root_dated else 'off'}, "
        f"archive-unmatched-root-csv:{'on' if args.archive_unmatched_root_csv else 'off'}"
    )

    organized_count, organized_bytes = _organize_existing_archive(
        ARCHIVE_ROOT_DIR,
        args.dry_run,
        args.secondary_level,
    )
    print(f"[archive] existing archive organized: {organized_count}")

    if args.archive_all_root_dated:
        moved_count, moved_bytes = _archive_all_root_dated_files(
            STOCK_DATA_DIR,
            args.dry_run,
            args.secondary_level,
        )
    else:
        moved_count, moved_bytes = _archive_old_root_files(
            STOCK_DATA_DIR,
            keep_root_days,
            args.dry_run,
            args.secondary_level,
        )
    print(f"[archive] root files archived: {moved_count}")

    unmatched_count = 0
    unmatched_bytes = 0
    if args.archive_unmatched_root_csv:
        unmatched_count, unmatched_bytes = _archive_unmatched_root_csv_files(
            STOCK_DATA_DIR,
            args.dry_run,
        )
    print(f"[archive] unmatched root csv archived: {unmatched_count}")

    deleted_count, deleted_bytes = _delete_archived_expired(
        ARCHIVE_ROOT_DIR,
        archive_keep_days,
        args.dry_run,
    )
    print(f"[archive] expired archive files removed: {deleted_count}")

    print(
        "[archive] done, "
        f"organized {organized_count} files ({_format_bytes(organized_bytes)}), "
        f"archived {moved_count} files ({_format_bytes(moved_bytes)}), "
        f"archived-unmatched {unmatched_count} files ({_format_bytes(unmatched_bytes)}), "
        f"deleted {deleted_count} files ({_format_bytes(deleted_bytes)})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
