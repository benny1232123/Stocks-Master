"""基本面 / 估值 / 资金流因子数据提供（**不依赖东方财富**，fail-soft + 本地缓存）。

数据源（全部为项目内已验证可达的稳定源，替代原 akshare 东方财富 em 接口——
em 接口在当前/沙箱网络下间歇性 ConnectionError，不可用）：
- **估值**(PE/PB/总市值)：腾讯行情 qt.gtimg.cn（海外/本地均稳定，单请求批量）。
- **质量**(ROE/毛利率) / **成长**(营收增长)：baostock query_profit_data / query_growth_data。
- **换手率** / **资金流量价代理**(近20日成交额均值)：baostock 日线 K 线（含 turn/amount）。

设计原则（与 factor_scoring 一致的「配置驱动 + 离线安全」范式）：
- 任何数据源失败**绝不抛异常**，返回 None；因子层据此将该因子贡献置 0（中性降级）。
- 本地缓存目录 stock_data/fundamental_cache/：
    spot_snapshot.csv : 腾讯全样本估值快照（PE/PB/总市值），覆盖「估值」因子；
    {code}.json       : 个股「质量」(ROE/毛利率/营收增长) + 「换手率」+ 「资金流量价代理」。
- scripts/refresh_fundamentals.py 在**有网环境**运行以填充缓存；生产运行时优先读缓存，
  仅当缓存缺失/过期才尝试联网。回测/选股在离线环境也能跑（因子降级为 0）。
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

# 腾讯行情字段索引（~ 分隔）：1=名称 3=现价 34=PE(TTM) 39=PB 45=总市值(万元)
_TX_HOST = "http://qt.gtimg.cn"
_TX_PE_IDX = 34
_TX_PB_IDX = 39
_TX_MKT_CAP_IDX = 45  # 单位：万元

# 延迟导入 requests（可选依赖，CI 缺时降级）
_tx_req: object | None = None


def _get_tx_requests():
    global _tx_req
    if _tx_req is None:
        try:
            import requests as _r
            _tx_req = _r
        except ImportError:
            _tx_req = False
    return _tx_req if _tx_req is not False else None


def _norm_code(code: str) -> str:
    raw = str(code).strip()
    return raw[-6:] if raw[-6:].isdigit() else raw


def _cache_age_days(path: Path) -> float:
    try:
        return (datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)).days
    except Exception:
        return 1e9


def _to_tx_symbol(code6: str) -> str:
    """6 位代码转腾讯格式（sh/sz 前缀）。"""
    if not code6.isdigit() or len(code6) != 6:
        return ""
    return ("sh" if code6.startswith(("5", "6", "9")) else "sz") + code6


# ───────────────────────── 估值（腾讯行情快照） ─────────────────────────
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


def _fetch_spot_online(codes: Optional[list] = None) -> Optional[pd.DataFrame]:
    """批量拉腾讯行情取 PE/PB/总市值；失败返回 None。

    Args:
        codes: 指定代码列表（refresh 时传候选股，精准填充）；为 None 则回退全量 k_data 样本。
    """
    req = _get_tx_requests()
    if req is None:
        return None
    # 样本：优先用指定 codes；否则用 k_data 缓存里真实出现过的代码（即历史候选股）
    sample: list[str] = list(codes) if codes else []
    if not sample:
        try:
            import glob
            kdir = PROJECT_ROOT / "stock_data" / "k_data"
            sample = [p.name.split("_")[0] for p in kdir.glob("*_qfq_full.csv")]
        except Exception:
            sample = []
    if not sample:
        return None

    syms = [_to_tx_symbol(c) for c in sample if _to_tx_symbol(c)]
    out: dict[str, dict] = {}
    # 分批（每批 ~80 只）避免 URL 过长
    for i in range(0, len(syms), 80):
        batch = syms[i:i + 80]
        try:
            r = req.get(f"{_TX_HOST}/q={','.join(batch)}", timeout=12,
                        headers={"User-Agent": "Mozilla/5.0"})
            r.encoding = "gbk"
            text = r.text
        except Exception:
            continue
        for line in text.strip().splitlines():
            if "=" not in line:
                continue
            raw = line.split("=", 1)[1].strip().strip('"')
            f = raw.split("~")
            if len(f) < _TX_MKT_CAP_IDX + 1:
                continue
            code6 = f[2].strip() if len(f) > 2 else ""
            if not code6.isdigit() or len(code6) != 6:
                continue
            try:
                pe = float(f[_TX_PE_IDX]) if f[_TX_PE_IDX].strip() else None
                pb = float(f[_TX_PB_IDX]) if f[_TX_PB_IDX].strip() else None
                mkt = float(f[_TX_MKT_CAP_IDX]) if f[_TX_MKT_CAP_IDX].strip() else None
            except (ValueError, IndexError):
                continue
            # 合理性过滤：异常值降级为 None，避免污染因子
            pe_ok = pe is not None and 0 < pe < 300
            pb_ok = pb is not None and 0 < pb < 50
            mkt_ok = mkt is not None and mkt > 0
            if not (pe_ok or pb_ok or mkt_ok):
                continue
            out[code6] = {
                "pe": round(pe, 2) if pe_ok else None,
                "pb": round(pb, 2) if pb_ok else None,
                # 腾讯市值字段单位：亿元（茅台~16357亿=1.6万亿，符合常识）
                "mkt_cap": round(mkt, 2) if mkt_ok else None,
            }
    if not out:
        return None
    df = pd.DataFrame.from_dict(out, orient="index").reset_index()
    df = df.rename(columns={"index": "代码"})
    df["代码"] = df["代码"].astype(str).str.zfill(6)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        df.to_csv(SPOT_FILE, index=False, encoding="utf-8-sig")
    except Exception:
        pass
    return df


def get_valuation(code: str, *, force: bool = False) -> Optional[dict]:
    """返回 {pe, pb, mkt_cap}；缺失/异常返回 None。ps 腾讯无→不再提供。"""
    code6 = _norm_code(code)
    df = _load_spot_cache() if not force else _fetch_spot_online([code6])
    if df is None and not force:
        df = _fetch_spot_online([code6])
    if df is None or "代码" not in df.columns:
        return None
    row = df[df["代码"] == code6]
    if row.empty:
        return None
    r = row.iloc[0]
    out: dict = {}
    for src, dst in (("pe", "pe"), ("pb", "pb"), ("mkt_cap", "mkt_cap")):
        if src in r.index:
            try:
                v = float(r[src])
                if pd.notna(v) and v > 0:
                    out[dst] = v
            except (TypeError, ValueError):
                pass
    return out or None


# ───────────────────────── 质量 + 成长（baostock） ─────────────────────────
def _bs_login() -> bool:
    try:
        from smcore.data.session import login
        return login()
    except Exception:
        return False


def _fetch_profit_baostock(code6: str) -> Optional[dict]:
    """baostock 盈利能力 → ROE + 毛利率。取最近一期。"""
    try:
        import baostock as bs
        if not _bs_login():
            return None
        # 拉近 4 年各季度，取最新非空
        for year in (2025, 2024, 2023, 2022):
            for q in (4, 3, 2, 1):
                rs = bs.query_profit_data(code=f"sh.{code6}" if code6.startswith(("6", "9"))
                                          else f"sz.{code6}", year=year, quarter=q)
                if rs is None or rs.error_code != "0":
                    continue
                rows = []
                while rs.next():
                    rows.append(rs.get_row_data())
                if not rows or not rs.fields:
                    continue
                rec = dict(zip(rs.fields, rows[0]))
                try:
                    roe = float(rec.get("roeAvg", "")) if rec.get("roeAvg") else None
                    gm = float(rec.get("gpMargin", "")) if rec.get("gpMargin") else None
                except (TypeError, ValueError):
                    roe = gm = None
                if roe is None and gm is None:
                    continue
                return {"roe": roe, "gross_margin": gm}
    except Exception:
        return None
    return None


def _fetch_growth_baostock(code6: str) -> Optional[dict]:
    """baostock 成长能力 → 营收增长。取最近一期。"""
    try:
        import baostock as bs
        if not _bs_login():
            return None
        for year in (2025, 2024, 2023, 2022):
            for q in (4, 3, 2, 1):
                rs = bs.query_growth_data(code=f"sh.{code6}" if code6.startswith(("6", "9"))
                                          else f"sz.{code6}", year=year, quarter=q)
                if rs is None or rs.error_code != "0":
                    continue
                rows = []
                while rs.next():
                    rows.append(rs.get_row_data())
                if not rows or not rs.fields:
                    continue
                rec = dict(zip(rs.fields, rows[0]))
                try:
                    rg = float(rec.get("YSTZ", "")) if rec.get("YSTZ") else None  # 营业收入同比增长率
                except (TypeError, ValueError):
                    rg = None
                if rg is None:
                    continue
                return {"revenue_growth": rg}
    except Exception:
        return None
    return None


def _fetch_kline_stats_baostock(code6: str, as_of=None) -> Optional[dict]:
    """baostock 日线 → 换手率(turn)最新值 + 近20日成交额均值(资金流量价代理)。

    返回 {turnover, amount_20}；amount_20 单位元。
    """
    try:
        import baostock as bs
        if not _bs_login():
            return None
        end = (as_of or datetime.now()).strftime("%Y-%m-%d")
        start = (as_of or datetime.now()) - timedelta(days=120)
        rs = bs.query_history_k_data_plus(
            f"sh.{code6}" if code6.startswith(("6", "9")) else f"sz.{code6}",
            "date,turn,amount",
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end,
            frequency="d",
            adjustflag="2",
        )
        if rs is None or rs.error_code != "0":
            return None
        rows = []
        while rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=["date", "turn", "amount"])
        df["turn"] = pd.to_numeric(df["turn"], errors="coerce")
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        turn = df["turn"].dropna()
        amt = df["amount"].dropna().tail(20)
        out: dict = {}
        if not turn.empty:
            out["turnover"] = float(turn.iloc[-1])
        if len(amt) >= 5:
            out["amount_20"] = float(amt.mean())
        return out or None
    except Exception:
        return None


# ───────────────────────── 合并 fetch ─────────────────────────
def fetch_fundamental(code: str, as_of=None, *, force: bool = False) -> Optional[dict]:
    """合并返回单只票的基本面因子原始值：
        {roe, gross_margin, revenue_growth, pe, pb, mkt_cap, turnover, amount_20}
    任一子块缺失则其字段为 None（因子层据此降级）。全部缺失返回 None。
    """
    code6 = _norm_code(code)
    cached = None if force else _load_fund_cache(code6)
    if cached is not None:
        return cached

    out: dict = {}
    q = _fetch_profit_baostock(code6)
    if q:
        out.update(q)
    g = _fetch_growth_baostock(code6)
    if g:
        out.update(g)
    val = get_valuation(code6, force=force)
    if val:
        out.update(val)
    ks = _fetch_kline_stats_baostock(code6, as_of)
    if ks:
        out.update(ks)
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


# ───────────────────────── 缓存读写（个股 JSON） ─────────────────────────
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
