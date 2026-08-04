"""股票名称兜底查询。

把 fusion.py 里的「代码→名称」缓存与查询逻辑抽到独立模块，便于单测与复用。
名称真身（缓存 dict、baostock 登录态）定义在本模块，fusion.py 仅做兼容重新导出。
"""
from __future__ import annotations

import pandas as pd
from pathlib import Path
from typing import Optional

from smcore.config.defaults import STOCK_DATA_DIR
from smcore.utils.code import format_stock_code

# ── 股票名称兜底映射（当所有策略 CSV 都缺 股票名称 时使用）─────────────
_stock_name_cache: Optional[dict] = None
# baostock 登录态复用
_bs_name_logged_in = False

# ── 名称归一化：把 pandas 写出的 "nan" / "None" / "--" 统一视为缺失 ──
_INVALID_NAMES = {"nan", "none", "null", "--", "", "na", "nat"}


def _normalize_name(raw: str) -> str:
    """将 CSV 中可能出现的无效名称归一化为空串。"""
    s = (raw or "").strip()
    return "" if s.lower() in _INVALID_NAMES else s


def _build_stock_name_cache_from_akshare(path: Path) -> bool:
    """用 akshare 全市场代码→名称映射构建本地缓存文件。成功返回 True。"""
    try:
        import akshare as ak

        df = ak.stock_info_a_code_name()
        if df is None or df.empty:
            return False
        df = df.copy()
        df["code"] = df["code"].astype(str).str.zfill(6)
        # 清洗名称：去除内部多余空格（如 "万 科Ａ" -> "万科Ａ"），保留全角Ａ/Ｂ
        df["name"] = (
            df["name"].astype(str).str.replace(r"\s+", "", regex=True).str.strip()
        )
        df = df[df["name"].str.len() > 0]
        df.to_csv(path, index=False, encoding="utf-8-sig")
        for _, r in df.iterrows():
            c = format_stock_code(str(r.get("code", "")).strip())
            n = str(r.get("name", "")).strip()
            if c and n and c not in _stock_name_cache:
                _stock_name_cache[c] = n
        return True
    except Exception as exc:
        print(f"[name-cache] akshare 构建失败：{type(exc).__name__}: {exc}")
        return False


def _get_stock_name_map() -> dict:
    """返回 {code(6位): name} 映射，优先从 stock_info_a_code_name.csv 缓存读取；
    文件缺失时自动用 akshare 全市场映射构建（覆盖全部 A 股，避免 baostock 兜底查不到）。"""
    global _stock_name_cache
    if _stock_name_cache is not None:
        return _stock_name_cache
    _stock_name_cache = {}
    p = STOCK_DATA_DIR / "stock_info_a_code_name.csv"
    if p.exists():
        try:
            df = pd.read_csv(p, encoding="utf-8-sig", dtype=str)
            code_col = next((c for c in df.columns if c in ("code", "代码", "股票代码")), None)
            name_col = next((c for c in df.columns if c in ("name", "名称", "股票名称")), None)
            if code_col and name_col:
                for _, r in df.iterrows():
                    c = format_stock_code(str(r[code_col]).strip())
                    n = str(r[name_col]).strip()
                    if c and n and c not in _stock_name_cache:
                        _stock_name_cache[c] = n
        except Exception:
            pass
    else:
        # 缓存文件缺失 → 用 akshare 一次性构建并持久化（之后直接读文件，秒级）
        if _build_stock_name_cache_from_akshare(p):
            print(f"[name-cache] 已从 akshare 构建 {len(_stock_name_cache)} 只股票名称缓存")
    return _stock_name_cache


def lookup_stock_name(code: str) -> str:
    """查询单只股票名称（缓存 → akshare 单只 → baostock 兜底），找不到返回空串。"""
    c6 = format_stock_code(code)
    if not c6:
        return ""
    # 1) 已有缓存（含 stock_info_a_code_name.csv 或 akshare 全市场构建结果）
    m = _get_stock_name_map()
    if c6 in m:
        return m[c6]
    # 2) akshare 单只实时兜底（覆盖新股等未入全市场列表的代码）
    try:
        import akshare as ak

        # stock_individual_info_em 返回 item/value 两列，其中「股票简称」即名称
        info = ak.stock_individual_info_em(symbol=c6)
        if info is not None and not info.empty:
            name_row = info[info["item"].astype(str).str.contains("简称|名称", na=False)]
            if not name_row.empty:
                found = str(name_row["value"].iloc[0]).strip()
                if found:
                    _stock_name_cache[c6] = found  # 写回缓存
                    return found
    except Exception:
        pass
    # 3) baostock 兜底（仅一次登录，~0.3s/只）
    global _bs_name_logged_in
    try:
        import baostock as bs

        bs_code = f"sh.{c6}" if c6[0] == "6" else f"sz.{c6}"
        if not _bs_name_logged_in:
            lg = bs.login()
            if getattr(lg, "error_code", "1") != "0":
                return ""
            _bs_name_logged_in = True
        rs = bs.query_stock_basic(code=bs_code, code_name="")
        found = ""
        while rs.next():
            row = rs.get_row_data()
            if len(row) >= 2 and row[1]:
                found = str(row[1]).strip()
                break
        if found:
            _stock_name_cache[c6] = found  # 写回缓存
        return found
    except Exception:
        return ""
