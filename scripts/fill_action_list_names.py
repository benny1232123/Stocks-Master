#!/usr/bin/env python3
"""补全 Daily-Action-List-*.csv 中缺失的「股票名称」列。

背景：历史回填的后期批次（约 06-28 起）因 akshare 名称映射接口抖动，
写出的 CSV 股票名称为空。本脚本用 akshare.stock_info_a_code_name 一次性拉取
全市场代码→名称映射，回填所有 Daily-Action-List 中名称为空的行（原地修改）。
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from smcore.artifacts import STOCK_DATA_DIR
from smcore.utils.code import format_stock_code

NAME_COL = "股票名称"


def load_name_map() -> dict[str, str]:
    import akshare as ak

    df = ak.stock_info_a_code_name()
    if df is None or df.empty or {"code", "name"}.issubset(df.columns) is False:
        # 容错：尝试常见列名
        if df is not None and not df.empty and len(df.columns) >= 2:
            df = df.rename(columns={df.columns[0]: "code", df.columns[1]: "name"})
        else:
            return {}
    out: dict[str, str] = {}
    for raw_code, raw_name in zip(df["code"].tolist(), df["name"].tolist()):
        c = format_stock_code(str(raw_code))
        if c and raw_name:
            out[c] = str(raw_name).strip()
    return out


def main() -> int:
    name_map = load_name_map()
    if not name_map:
        print("[fill_names] 名称映射为空，退出")
        return 1
    print(f"[fill_names] 名称映射规模: {len(name_map)}")

    files = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))
    filled_total = 0
    for f in files:
        rows = []
        changed = False
        with open(f, encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            fieldnames = reader.fieldnames or []
            if NAME_COL not in fieldnames:
                fieldnames = fieldnames + [NAME_COL]
            for row in reader:
                if NAME_COL not in row or not (row.get(NAME_COL) or "").strip():
                    c = format_stock_code(str(row.get("股票代码", "")))
                    nm = name_map.get(c)
                    if nm:
                        row[NAME_COL] = nm
                        changed = True
                        filled_total += 1
                rows.append(row)
        if changed:
            with open(f, "w", encoding="utf-8-sig", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            print(f"  → {f.name}: 补全 {sum(1 for r in rows if r.get(NAME_COL))} 行名称")
    print(f"[fill_names] 共补全 {filled_total} 处名称")
    return 0


if __name__ == "__main__":
    sys.exit(main())
