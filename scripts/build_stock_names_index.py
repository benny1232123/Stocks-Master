"""重建股票名称索引 JSON（smcore/data/stock_names_index.json）。

数据源：本地 SQLite（stock_data/stocks_data.db 的 ak_stock_info_a_code_name）优先，
没有再回退 akshare。

用法：
    python scripts/build_stock_names_index.py

建议：本地每月 / 新股上市后跑一次，让 Render 上的 JSON 保持最新。
"""
from __future__ import annotations

import json
import sqlite3
import sys
import unicodedata
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUT_PATH = PROJECT_ROOT / "smcore" / "data" / "stock_names_index.json"
DB_PATH = PROJECT_ROOT / "stock_data" / "stocks_data.db"
TABLE = "ak_stock_info_a_code_name"


def normalize(s: str) -> str:
    return "".join(unicodedata.normalize("NFKC", s).split()).upper()


def load_from_sqlite() -> list[tuple[str, str]]:
    if not DB_PATH.exists():
        return []
    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    try:
        cur = con.cursor()
        cur.execute(f"SELECT code, name FROM {TABLE}")
        rows = cur.fetchall()
    finally:
        con.close()
    return [(str(c).strip(), str(n).strip()) for c, n in rows if c and n]


def load_from_akshare() -> list[tuple[str, str]]:
    try:
        import akshare as ak
    except ImportError:
        print("[FATAL] 未安装 akshare，无法联网拉全表；请先 pip install akshare", file=sys.stderr)
        sys.exit(1)
    df = ak.stock_info_a_code_name()
    if df is None or df.empty:
        return []
    return [
        (str(r["code"]).strip(), str(r["name"]).strip())
        for _, r in df.iterrows()
        if str(r.get("code", "")).strip()
    ]


def main() -> int:
    rows = load_from_sqlite()
    src = "本地 SQLite"
    if not rows:
        print("本地 SQLite 缺，回退 akshare...")
        rows = load_from_akshare()
        src = "akshare"
    if not rows:
        print("[FATAL] 无数据源可加载", file=sys.stderr)
        return 1

    code_to_name: dict[str, str] = {}
    norm_buckets: dict[str, list[str]] = {}
    for code, name in rows:
        code_to_name[code] = name
        norm_buckets.setdefault(normalize(name), []).append(code)
    # 同名多 code 保留列表，单 code 简化为字符串
    name_to_code = {k: (v[0] if len(v) == 1 else v) for k, v in norm_buckets.items()}

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "_meta": {
            "source": src,
            "count_codes": len(code_to_name),
            "count_names": len(name_to_code),
        },
        "code_to_name": code_to_name,
        "name_to_code": name_to_code,
    }
    OUT_PATH.write_text(
        json.dumps(data, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    print(f"[OK] {OUT_PATH} | {OUT_PATH.stat().st_size//1024} KB | "
          f"{len(code_to_name)} codes / {len(name_to_code)} names (from {src})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
