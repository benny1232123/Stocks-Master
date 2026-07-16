"""用权威 A 股全量名单（沪深交易所官方上市列表，受证监会监管）补齐所有数据 CSV 中缺失的股票名称。

用法：
    python scripts/backfill_stock_names.py [--dry-run]

行为：
1. 强制用 akshare 全市场 `stock_info_a_code_name()` 刷新 stock_data/stock_info_a_code_name.csv
   （即证监体系下的全 A 股代码↔名称权威映射）。
2. 扫描以下数据文件，凡「股票名称」列为 nan/空/-- 且能从权威映射查到代码者，就地补上名称：
   - stock_data/Daily-Action-List-*.csv
   - stock_data/Stock-Selection-*.csv
   - stock_data/CCTV-Sector-Stock-Pool-*.csv
   - stock_data/archive/**/Stock-Selection-*.csv
   - stock_data/archive/**/CCTV-Sector-Stock-Pool-*.csv
3. 统计并打印补了多少只、还剩多少只查不到（极少数新股/退市股会落到 fallback）。
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from smcore.strategy.fusion import (  # noqa: E402
    _build_stock_name_cache_from_akshare,
    _normalize_name,
)
from smcore.utils.code import format_stock_code  # noqa: E402

STOCK_DATA = ROOT / "stock_data"
NAME_COLS = ("股票名称", "名称")
CODE_COLS = ("股票代码", "代码", "code")

GLOBS = [
    "Daily-Action-List-*.csv",
    "Stock-Selection-*.csv",
    "CCTV-Sector-Stock-Pool-*.csv",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="只统计不写盘")
    args = ap.parse_args()

    print("[1/3] 刷新权威 A 股名单缓存（stock_info_a_code_name.csv）…")
    if not _build_stock_name_cache_from_akshare(STOCK_DATA / "stock_info_a_code_name.csv"):
        print("  ⚠️ akshare 拉取失败，将沿用已有缓存文件")
    # 重新加载映射
    from smcore.strategy.fusion import _get_stock_name_map, _stock_name_cache
    import smcore.strategy.fusion as fusion_mod
    fusion_mod._stock_name_cache = None  # 强制重建
    name_map = _get_stock_name_map()
    print(f"  权威映射共 {len(name_map)} 只")

    files: list[Path] = []
    for g in GLOBS:
        files += list(STOCK_DATA.glob(g))
        files += list(STOCK_DATA.rglob("archive/**/" + g))
    # 去重
    files = sorted(set(files))
    print(f"[2/3] 扫描到 {len(files)} 个 CSV 文件")

    total_filled = 0
    total_remaining = 0
    per_file: list[tuple[str, int, int]] = []

    for p in files:
        filled, remaining = _fill_one(p, name_map, dry_run=args.dry_run)
        if filled or remaining:
            per_file.append((str(p.relative_to(ROOT)), filled, remaining))
            total_filled += filled
            total_remaining += remaining

    print(f"[3/3] 完成。共补齐 {total_filled} 处名称，仍缺 {total_remaining} 处：")
    for rel, f, r in per_file:
        flag = "✓" if f else "·"
        print(f"  {flag} {rel}: 补 {f} / 仍缺 {r}")
    if args.dry_run:
        print("(dry-run：未写盘)")
    return 0


def _fill_one(path: Path, name_map: dict, *, dry_run: bool) -> tuple[int, int]:
    rows = []
    fields = None
    name_col = code_col = None
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames
        if not fields:
            return 0, 0
        name_col = next((c for c in fields if c in NAME_COLS), None)
        code_col = next((c for c in fields if c in CODE_COLS), None)
        if not name_col or not code_col:
            return 0, 0
        for row in reader:
            rows.append(row)

    filled = 0
    remaining = 0
    for row in rows:
        code = format_stock_code(row.get(code_col, ""))
        cur = _normalize_name(row.get(name_col, ""))
        if cur:
            continue
        if code and code in name_map:
            row[name_col] = name_map[code]
            filled += 1
        else:
            remaining += 1

    if filled and not dry_run:
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(rows)
    return filled, remaining


if __name__ == "__main__":
    raise SystemExit(main())
