"""基本面 / 估值 / 资金流因子数据提供（联网优先 + 本地缓存 + 缺失降级）。

设计原则（与 factor_scoring 一致的「配置驱动 + 离线安全」范式）：
- 联网失败时**绝不抛异常**，返回 None；因子层据此将该因子贡献置 0（中性降级），
  绝不因数据缺失而中断生产选股或回测。
- 本地缓存目录 stock_data/fundamental_cache/：
    spot_snapshot.csv : 全 A 快照（市盈率-动态/市净率/市销率/总市值/流通市值/换手率），
                        来自 stock_zh_a_spot_em，一次拉全市场，覆盖「估值」因子；
    {code}.json       : 个股「质量」(ROE/销售毛利率/营收增长) + 「资金流」(主力 20 日净流入)。
- scripts/refresh_fundamentals.py 在**有网环境**运行以填充缓存；生产运行时优先读缓存，
  仅当缓存缺失/过期才尝试联网。这样回测/选股在离线沙箱也能跑（因子降级为 0）。

注意：本沙箱网络对 akshare 数据源（eastmoney/sina）受限，在线拉取会 ConnectionError，
属预期降级路径；在用户真实机器 / CI（有网）上即可正常填充缓存并激活因子。
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

try:
    from smcore.config.defaults import PROJECT_ROOT
except Exception:  # pragma: no cover
    PROJECT_ROOT = Path(__file__).resolve().parents[2]

CACHE_DIR = PROJECT_ROOT / "stock_data" / "fundamental_cache"
SPOT_FILE = CACHE_DIR / "spot_snapshot.csv"
CACHE_TTL_DAYS = int(__import__("os").environ.get("FUND_TTL_DAYS", "30"))


def _norm_code(code: str) -> str:
    s = str(code).strip().lstrip("0")
    # 保留 6 位标准代码（去前缀）
    raw = str(code).strip()
    return raw[-6:] if raw[-6:].isdigit() else raw


def _cache_age_days(path: Path) -> float:
    try:
        return (datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)).days
    except Exception:
        return 1e9


# ───────────────────────── 估值（全 A 快照） ─────────────────────────
def _load_spot_cache() -> Optional[pd.DataFrame]:
    if not SPOT_FILE.exists():
        return None
    if _cache_age_days(SPOT_FILE) > CACHE_TTL_DAYS:
        return None
    try:
        df = pd.read_csv(SPOT_FILE, dtype={"代码": str})
        df["代码"] = df["代码"].astype(str).str.zfill(6)
        return df
    except Exception:
        return None


def _fetch_spot_online() -> Optional[pd.DataFrame]:
    """联网拉全 A 快照；失败返回 None（缓存不会被覆盖）。"""
    try:
        import akshare as ak
    except Exception:
        return None
    try:
        df = ak.stock_zh_a_spot_em()
    except Exception:
        return None
    if df is None or df.empty:
        return None
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        df.to_csv(SPOT_FILE, index=False, encoding="utf-8-sig")
    except Exception:
        pass
    return df


def get_valuation(code: str, *, force: bool = False) -> Optional[dict]:
    """返回 {pe, pb, ps, mkt_cap, float_mkt_cap, turnover}；缺失/异常返回 None。"""
    code6 = _norm_code(code)
    df = _load_spot_cache() if not force else _fetch_spot_online()
    if df is None and not force:
        df = _fetch_spot_online()
    if df is None or "代码" not in df.columns:
        return None
    row = df[df["代码"] == code6]
    if row.empty:
        return None
    r = row.iloc[0]
    out: dict = {}
    for src, dst in (("市盈率-动态", "pe"), ("市净率", "pb"), ("市销率", "ps"),
                     ("总市值", "mkt_cap"), ("流通市值", "float_mkt_cap"), ("换手率", "turnover")):
        if src in r.index:
            try:
                v = float(r[src])
                if pd.notna(v):
                    out[dst] = v
            except (TypeError, ValueError):
                pass
    return out or None


# ───────────────────────── 质量 + 资金流（个股） ─────────────────────────
def _fund_cache_file(code: str) -> Path:
    return CACHE_DIR / f"{_norm_code(code)}.json"


def _load_fund_cache(code: str) -> Optional[dict]:
    p = _fund_cache_file(code)
    if not p.exists() or _cache_age_days(p) > CACHE_TTL_DAYS:
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_fund_cache(code: str, data: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        _fund_cache_file(code).write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def _fetch_quality_online(code: str) -> Optional[dict]:
    try:
        import akshare as ak
    except Exception:
        return None
    try:
        d = ak.stock_financial_analysis_indicator(symbol=_norm_code(code))
    except Exception:
        return None
    if d is None or d.empty:
        return None
    # 取最新一期（末行）
    r = d.iloc[-1]
    out: dict = {}
    for col, dst in (("净资产收益率(%)", "roe"), ("销售毛利率(%)", "gross_margin"),
                     ("主营业务收入增长率(%)", "revenue_growth"), ("营业收入增长率(%)", "revenue_growth")):
        if col in r.index:
            try:
                v = float(r[col])
                if pd.notna(v):
                    out.setdefault(dst, v)
            except (TypeError, ValueError):
                pass
    return out or None


def _fetch_fundflow_online(code: str, as_of) -> Optional[float]:
    try:
        import akshare as ak
    except Exception:
        return None
    code6 = _norm_code(code)
    market = "sh" if code6.startswith(("6", "9")) else "sz"
    try:
        d = ak.stock_individual_fund_flow(stock=code6, market=market)
    except Exception:
        return None
    if d is None or d.empty:
        return None
    # 主力净流入-净额 近 20 日均值
    col = "主力净流入-净额" if "主力净流入-净额" in d.columns else d.columns[-3]
    try:
        s = pd.to_numeric(d[col], errors="coerce").dropna().tail(20)
        return float(s.mean()) if len(s) else None
    except Exception:
        return None


def fetch_fundamental(code: str, as_of=None, *, force: bool = False) -> Optional[dict]:
    """合并返回单只票的基本面因子原始值：
        {roe, gross_margin, revenue_growth, pe, pb, ps, mkt_cap, float_mkt_cap,
         turnover, main_inflow_20}
    任一子块缺失则其字段为 None（因子层据此降级）。全部缺失返回 None。
    """
    code6 = _norm_code(code)
    cached = None if force else _load_fund_cache(code6)
    if cached is not None:
        return cached

    out: dict = {}
    q = _fetch_quality_online(code6)
    if q:
        out.update(q)
    ff = _fetch_fundflow_online(code6, as_of)
    if ff is not None:
        out["main_inflow_20"] = ff
    val = get_valuation(code6, force=force)
    if val:
        out.update(val)
    if not out:
        return None
    _save_fund_cache(code6, out)
    return out


def fetch_fundamentals_batch(codes, as_of=None, *, force: bool = False) -> dict:
    """批量：返回 {code: fundamental_dict|None}。逐个拉取，互不波及。"""
    out: dict = {}
    for c in codes:
        try:
            out[str(c).strip()] = fetch_fundamental(str(c).strip(), as_of, force=force)
        except Exception:
            out[str(c).strip()] = None
    return out


def refresh_all(codes, as_of=None) -> int:
    """强制刷新缓存（scripts/refresh_fundamentals.py 调用）。返回成功填充的票数。"""
    n = 0
    for c in codes:
        try:
            if fetch_fundamental(str(c).strip(), as_of, force=True):
                n += 1
        except Exception:
            pass
    return n
