from __future__ import annotations

import argparse
import zipfile
from datetime import datetime, timedelta
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
STOCK_DATA_DIR = ROOT_DIR / "stock_data"
COMPRESSED_DIR = STOCK_DATA_DIR / "compressed"


TARGETS = {
    "auto_logs": {"rel_dir": "auto_logs", "keep_days": 30},
    "plots": {"rel_dir": "plots", "keep_days": 30},
    "ui_uploads": {"rel_dir": "ui_uploads", "keep_days": 30},
    "checkpoints": {"rel_dir": "checkpoints", "keep_days": 180},
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compress cold stock_data folders into zip files.")
    parser.add_argument("--auto-logs-keep-days", type=int, default=30, help="auto_logs 保留天数，默认 30")
    parser.add_argument("--plots-keep-days", type=int, default=30, help="plots 保留天数，默认 30")
    parser.add_argument("--ui-uploads-keep-days", type=int, default=30, help="ui_uploads 保留天数，默认 30")
    parser.add_argument("--checkpoints-keep-days", type=int, default=180, help="checkpoints 保留天数，默认 180")
    parser.add_argument("--dry-run", action="store_true", help="仅预览，不真正压缩或删除")
    return parser.parse_args()


def _safe_days(value: int, fallback: int) -> int:
    return value if value >= 0 else fallback


def _format_bytes(byte_count: int) -> str:
    if byte_count < 1024:
        return f"{byte_count} B"
    if byte_count < 1024 * 1024:
        return f"{byte_count / 1024:.2f} KB"
    return f"{byte_count / (1024 * 1024):.2f} MB"


def _iter_candidates(target_dir: Path, cutoff_dt: datetime):
    for path in target_dir.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() == ".zip":
            continue
        file_mtime = datetime.fromtimestamp(path.stat().st_mtime)
        if file_mtime < cutoff_dt:
            yield path


def _prune_empty_dirs(start_dir: Path, stop_dir: Path) -> None:
    current = start_dir
    while current != stop_dir and current.exists():
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent


def _compress_target(rel_name: str, keep_days: int, dry_run: bool) -> tuple[int, int, int]:
    target_dir = STOCK_DATA_DIR / rel_name
    if not target_dir.exists() or not target_dir.is_dir():
        return 0, 0, 0

    cutoff_dt = datetime.now() - timedelta(days=keep_days)
    candidates = list(_iter_candidates(target_dir, cutoff_dt))
    if not candidates:
        return 0, 0, 0

    COMPRESSED_DIR.mkdir(parents=True, exist_ok=True)
    zip_name = f"{rel_name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.zip"
    zip_path = COMPRESSED_DIR / zip_name

    total_size = sum(path.stat().st_size for path in candidates)
    if dry_run:
        print(f"[DRY-RUN] {rel_name}: {len(candidates)} files -> {zip_path} ({_format_bytes(total_size)})")
        return len(candidates), total_size, 0

    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for path in candidates:
            zf.write(path, arcname=path.relative_to(STOCK_DATA_DIR).as_posix())

    removed_count = 0
    removed_bytes = 0
    for path in candidates:
        removed_bytes += path.stat().st_size
        path.unlink()
        removed_count += 1

    # 只清理空目录，不动目标目录本身。
    for child_dir in sorted([p for p in target_dir.rglob("*") if p.is_dir()], key=lambda p: len(p.parts), reverse=True):
        _prune_empty_dirs(child_dir, target_dir)

    print(f"[COMPRESSED] {rel_name}: {removed_count} files -> {zip_path} ({_format_bytes(removed_bytes)})")
    return removed_count, removed_bytes, zip_path.stat().st_size


def main() -> int:
    args = _parse_args()

    if not STOCK_DATA_DIR.exists() or not STOCK_DATA_DIR.is_dir():
        print(f"stock_data not found: {STOCK_DATA_DIR}")
        return 1

    keep_map = {
        "auto_logs": _safe_days(args.auto_logs_keep_days, TARGETS["auto_logs"]["keep_days"]),
        "plots": _safe_days(args.plots_keep_days, TARGETS["plots"]["keep_days"]),
        "ui_uploads": _safe_days(args.ui_uploads_keep_days, TARGETS["ui_uploads"]["keep_days"]),
        "checkpoints": _safe_days(args.checkpoints_keep_days, TARGETS["checkpoints"]["keep_days"]),
    }

    print("[compress] start")
    print(
        "[compress] retention => "
        f"auto_logs:{keep_map['auto_logs']}d, "
        f"plots:{keep_map['plots']}d, "
        f"ui_uploads:{keep_map['ui_uploads']}d, "
        f"checkpoints:{keep_map['checkpoints']}d"
    )

    total_files = 0
    total_input_bytes = 0
    total_zip_bytes = 0

    for rel_name in ["auto_logs", "plots", "ui_uploads", "checkpoints"]:
        count, input_bytes, zip_bytes = _compress_target(rel_name, keep_map[rel_name], args.dry_run)
        total_files += count
        total_input_bytes += input_bytes
        total_zip_bytes += zip_bytes

    print(
        f"[compress] done, packed {total_files} files, "
        f"raw {_format_bytes(total_input_bytes)}, zip {_format_bytes(total_zip_bytes)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())