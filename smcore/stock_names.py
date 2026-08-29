"""股票「名称 ↔ 代码」索引（离线优先）。

用途
----
管理端录入持仓时，用户更习惯输入**股票名称**（如「贵州茅台」）而不是 6 位代码。
本模块提供名称 → 代码的解析。

数据来源（按优先级）
--------------------
1. **随代码提交的 JSON 索引** ``smcore/data/stock_names_index.json``
   （约 5500 只 A 股，含全称/简称同表，**完全离线，Render / 沙箱 / 无网络都能用**）。
2. 本地 SQLite ``stock_data/stocks_data.db`` 的 ``ak_stock_info_a_code_name`` 表
   （本地开发有，但 Render 没有；用作 JSON 的本地补充）。
3. 联网回退：akshare ``ak.stock_info_a_code_name()`` 全表。
4. 运行时单名兜底：上面都查不到时，临时调一次 akshare 全表并搜索。
   （新上市/JSON 漏收录的股会触发一次，之后缓存住。）

归一化
------
A股名称含全角字符与空格（如 ``'万  科Ａ'``、``'ＴＣＬ科技'``），直接比对会失败。
统一做：``NFKC`` 归一化（全角→半角）+ 去所有空白 + 转大写。
这样用户输入「万科A」「万科Ａ」「万 科 A」都能命中。

维护
----
JSON 文件可通过 ``python scripts/build_stock_names_index.py`` 重建（从本地 SQLite 或
akshare 全表导出），新上市/更名时跑一下就行。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import unicodedata
from pathlib import Path
from typing import Any

from smcore.config.defaults import STOCK_DATA_DIR
from smcore.utils.code import format_stock_code

#: 随代码提交的主索引（Render / 沙箱 / 无网络都可用）
_JSON_PATH = Path(__file__).resolve().parent / "data" / "stock_names_index.json"
#: 本地 SQLite（dev 补充）
_DB_PATH = STOCK_DATA_DIR / "stocks_data.db"
_TABLE = "ak_stock_info_a_code_name"

_lock = threading.Lock()
_code_to_name: dict[str, str] | None = None
_norm_to_codes: dict[str, list[str]] | None = None


def normalize(text: str) -> str:
    """归一化股票名称：NFKC（全角→半角）+ 去空白 + 大写。"""
    s = unicodedata.normalize("NFKC", str(text or ""))
    return "".join(s.split()).upper()


def _parse_rows(rows: list[tuple[str, str]]) -> tuple[dict[str, str], dict[str, list[str]]]:
    code_to_name: dict[str, str] = {}
    norm_to_codes: dict[str, list[str]] = {}
    for raw_code, raw_name in rows:
        code = format_stock_code(raw_code)
        if not code:
            continue
        code_to_name.setdefault(code, str(raw_name).strip())
        key = normalize(raw_name)
        if key:
            norm_to_codes.setdefault(key, []).append(code)
    return code_to_name, norm_to_codes


def _load_from_json() -> list[tuple[str, str]]:
    """读随代码提交的 JSON 索引。文件不存在或格式不对返回空。"""
    if not _JSON_PATH.exists():
        return []
    try:
        with _JSON_PATH.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        code_to_name = data.get("code_to_name") or {}
        out: list[tuple[str, str]] = []
        for code, name in code_to_name.items():
            if code and name:
                out.append((str(code), str(name)))
        return out
    except Exception:
        return []


def _load_from_sqlite() -> list[tuple[str, str]]:
    """本地 SQLite 读 (code, name)。"""
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


def _load_from_akshare_full() -> list[tuple[str, str]]:
    """联网全表加载 akshare。"""
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


_akshare_full_cache: list[tuple[str, str]] | None = None
_akshare_full_lock = threading.Lock()


def _load_from_akshare_full_with_timeout(timeout: float = 12.0) -> list[tuple[str, str]]:
    """akshare 全表加载加**超时守卫**（独立线程 + join）。

    原因：akshare 联网拉全表可能 30s+，在 Render 上会拖垮请求（网关 504，
    前端表现为「请求失败」）。超时直接放弃，返回空列表交给兜底链。
    """
    result: list[tuple[str, str]] = []

    def runner() -> None:
        try:
            result.extend(_load_from_akshare_full())
        except Exception:
            pass

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    t.join(timeout)
    return result


def _ensure_akshare_full() -> list[tuple[str, str]]:
    """懒加载 akshare 全表并缓存（每个进程一次），用于单名兜底。"""
    global _akshare_full_cache
    if _akshare_full_cache is not None:
        return _akshare_full_cache
    with _akshare_full_lock:
        if _akshare_full_cache is not None:
            return _akshare_full_cache
        _akshare_full_cache = _load_from_akshare_full_with_timeout()
        return _akshare_full_cache


def _ensure_loaded() -> None:
    """懒加载主索引（JSON > SQLite > akshare 全表）。"""
    global _code_to_name, _norm_to_codes
    if _code_to_name is not None:
        return
    with _lock:
        if _code_to_name is not None:
            return

        rows = _load_from_json()
        if not rows:
            rows = _load_from_sqlite()
        if not rows:
            rows = _load_from_akshare_full()

        _code_to_name, _norm_to_codes = _parse_rows(rows)


def reload() -> None:
    """强制重建主索引。"""
    global _code_to_name, _norm_to_codes
    with _lock:
        _code_to_name = None
        _norm_to_codes = None
    _ensure_loaded()


def _online_single_name(query: str) -> dict[str, str] | None:
    """主索引查不到时，临时拉一次 akshare 全表找该名（每进程只拉一次）。"""
    rows = _ensure_akshare_full()
    code_to_name, norm_to_codes = _parse_rows(rows)
    key = normalize(query)
    if not key:
        return None
    # 把临时解析结果回填到主索引（无需锁——只是补充查询）
    if _code_to_name is not None and _norm_to_codes is not None:
        for c, names in code_to_name.items():
            _code_to_name.setdefault(c, names)
        for n, codes in norm_to_codes.items():
            _norm_to_codes.setdefault(n, list(codes))
    exact = norm_to_codes.get(key, [])
    if len(exact) == 1:
        return {"code": exact[0], "name": code_to_name.get(exact[0], "")}
    if len(exact) > 1:
        return None  # 歧义，让调用方走候选路径
    return None


def code_to_name(code: str) -> str:
    """代码 → 名称；查不到返回空串。"""
    _ensure_loaded()
    return (_code_to_name or {}).get(format_stock_code(code), "")


def is_known_code(code: str) -> bool:
    """该代码是否在索引中（索引可能不完整，仅作参考，不作为硬性校验）。"""
    _ensure_loaded()
    return format_stock_code(code) in (_code_to_name or {})


def lookup_candidates(query: str, limit: int = 10) -> list[dict[str, str]]:
    """按名称或代码查候选。

    匹配顺序：代码精确 → 主索引名称精确 → 主索引前缀 → 主索引包含 → 单名兜底（akshare）。
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
    if contained:
        return [{"code": c, "name": code_to_name.get(c, "")} for c in contained[:limit]]

    # 5) 兜底：拉一次 akshare 全表（每进程首次）找该名
    try:
        online = _online_single_name(q)
    except Exception:
        online = None
    if online:
        return [online]

    return []


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
