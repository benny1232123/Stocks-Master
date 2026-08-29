"""股票「名称 ↔ 代码」索引（离线优先）。

用途
----
管理端录入持仓时，用户更习惯输入**股票名称**（如「贵州茅台」）而不是 6 位代码。
本模块提供名称 → 代码的解析。

数据来源（按优先级）
--------------------
1. 本地 SQLite ``stock_data/stocks_data.db`` 的 ``ak_stock_info_a_code_name`` 表
   （约 5500 只，**离线可用**，沙箱/无网络时最稳）。
2. 网络可用时回退 akshare ``ak.stock_info_a_code_name()``。

归一化
------
A股名称含全角字符与空格（如 ``'万  科Ａ'``、``'ＴＣＬ科技'``），直接比对会失败。
统一做：``NFKC`` 归一化（全角→半角）+ 去所有空白 + 转大写。
这样用户输入「万科A」「万科Ａ」「万 科 A」都能命中。
"""
from __future__ import annotations

import sqlite3
import threading
import unicodedata
from pathlib import Path
from typing import Any

from smcore.config.defaults import STOCK_DATA_DIR
from smcore.utils.code import format_stock_code

_DB_PATH = STOCK_DATA_DIR / "stocks_data.db"
_TABLE = "ak_stock_info_a_code_name"

_lock = threading.Lock()
_code_to_name: dict[str, str] | None = None
_norm_to_codes: dict[str, list[str]] | None = None


def normalize(text: str) -> str:
    """归一化股票名称：NFKC（全角→半角）+ 去空白 + 大写。"""
    s = unicodedata.normalize("NFKC", str(text or ""))
    return "".join(s.split()).upper()


def _load_from_sqlite() -> list[tuple[str, str]]:
    """从本地 SQLite 读 (code, name)。表不存在/读取失败返回空列表。"""
    if not _DB_PATH.exists():
        return []
    try:
        con = sqlite3.connect(f"file:{_DB_PATH}?mode=ro", uri=True)
        try:
            cur = con.cursor()
            cur.execute(f"SELECT code, name FROM {_TABLE}")
            rows = cur.fetchall()
        finally:
            con.close()
        return [(str(c).strip(), str(n).strip()) for c, n in rows if c and n]
    except Exception:
        return []


def _load_from_akshare() -> list[tuple[str, str]]:
    """联网回退：akshare 拉全量 A 股代码名称表。"""
    try:
        import akshare as ak

        df = ak.stock_info_a_code_name()
        if df is None or df.empty:
            return []
        return [
            (str(r["code"]).strip(), str(r["name"]).strip())
            for _, r in df.iterrows()
            if str(r.get("code", "")).strip()
        ]
    except Exception:
        return []


def _ensure_loaded() -> None:
    """懒加载索引（只加载一次，线程安全）。"""
    global _code_to_name, _norm_to_codes
    if _code_to_name is not None:
        return
    with _lock:
        if _code_to_name is not None:
            return

        rows = _load_from_sqlite()
        if not rows:
            rows = _load_from_akshare()

        code_to_name: dict[str, str] = {}
        norm_to_codes: dict[str, list[str]] = {}
        for raw_code, raw_name in rows:
            code = format_stock_code(raw_code)
            if not code:
                continue
            code_to_name.setdefault(code, raw_name)
            key = normalize(raw_name)
            if key:
                norm_to_codes.setdefault(key, []).append(code)

        _code_to_name = code_to_name
        _norm_to_codes = norm_to_codes


def reload() -> None:
    """强制重建索引（数据源更新后调用）。"""
    global _code_to_name, _norm_to_codes
    with _lock:
        _code_to_name = None
        _norm_to_codes = None
    _ensure_loaded()


def code_to_name(code: str) -> str:
    """代码 → 名称；查不到返回空串。"""
    _ensure_loaded()
    return (_code_to_name or {}).get(format_stock_code(code), "")


def is_known_code(code: str) -> bool:
    """该代码是否在索引中（索引可能不完整，仅作参考，不作为硬性校验）。"""
    _ensure_loaded()
    return format_stock_code(code) in (_code_to_name or {})


def lookup_candidates(query: str, limit: int = 10) -> list[dict[str, str]]:
    """按名称或代码查候选，返回 ``[{"code": "600519", "name": "贵州茅台"}, ...]``。

    匹配顺序：代码精确 → 名称精确 → 名称前缀 → 名称包含。
    """
    _ensure_loaded()
    q = str(query or "").strip()
    if not q:
        return []

    code_to_name = _code_to_name or {}
    norm_to_codes = _norm_to_codes or {}

    # 1) 输入本身就是 6 位代码
    code = format_stock_code(q)
    if code and code.isdigit() and len(code) == 6:
        return [{"code": code, "name": code_to_name.get(code, "")}]

    key = normalize(q)
    if not key:
        return []

    # 2) 名称精确匹配
    exact = norm_to_codes.get(key, [])
    if exact:
        return [{"code": c, "name": code_to_name.get(c, "")} for c in exact[:limit]]

    # 3) 前缀匹配
    prefixed = [c for k, codes in norm_to_codes.items() if k.startswith(key) for c in codes]
    if prefixed:
        return [{"code": c, "name": code_to_name.get(c, "")} for c in prefixed[:limit]]

    # 4) 包含匹配
    contained = [c for k, codes in norm_to_codes.items() if key in k for c in codes]
    return [{"code": c, "name": code_to_name.get(c, "")} for c in contained[:limit]]


def resolve(query: str) -> dict[str, Any]:
    """把用户输入解析为代码。

    返回 ``{"ok": bool, "code": str, "name": str, "candidates": [...]}``。
    - 唯一命中 → ``ok=True``
    - 多个候选 → ``ok=False`` 且带 ``candidates``（调用方应让用户确认）
    - 查不到 → ``ok=False`` 且 ``candidates`` 为空
    """
    cands = lookup_candidates(query, limit=10)
    if not cands:
        return {"ok": False, "code": "", "name": "", "candidates": []}
    # 名称精确匹配时可能有重名，用归一化后的名称再判一次唯一性
    if len(cands) > 1:
        first_name = normalize(cands[0]["name"])
        if all(normalize(c["name"]) == first_name for c in cands):
            return {"ok": False, "code": "", "name": cands[0]["name"], "candidates": cands}
    return {
        "ok": True,
        "code": cands[0]["code"],
        "name": cands[0]["name"],
        "candidates": cands,
    }
