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
from datetime import date, datetime, timedelta
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

# ───────────────────────── Point-in-Time (PIT) 纪律 ─────────────────────────
# 历史回补时绝不能使用「信号日之后才公告」的财报（未来函数）。缓存按报告期存历史，
# 取数时按「真实公告日 pubDate（最优） 或 法定披露截止日」对齐到 as_of。
# 这是基本面因子进入选股/回测前的最后一道防泄漏闸——价格因子已在 factor_scoring
# 内按 end=as_of 因果切片，此处补齐基本面因子。
CACHE_VERSION = 2

# 法定披露截止日相对报告期季末的滞后天数（A股惯例，仅当缺 pubDate 时回退使用）：
# 一季报/中报/三季报/年报 大限约 4-30 / 8-31 / 10-31 / 次年4-30。配置可由
# risk_config.json 的 fundamental_pit.disclosure_lag_days 覆盖。
_DISCLOSURE_LAG_DAYS = {"03-31": 30, "06-30": 62, "09-30": 31, "12-31": 120}


def _load_pit_cfg() -> dict:
    """从 risk_config.json 读取 fundamental_pit 配置（缺省回退内置常量）。"""
    try:
        cfg_path = PROJECT_ROOT / "smcore" / "strategy" / "risk_config.json"
        cfg = json.loads(Path(cfg_path).read_text(encoding="utf-8"))
        pit = cfg.get("fundamental_pit")
        if isinstance(pit, dict):
            return pit
    except Exception:
        pass
    return {}


def _parse_date_str(s) -> Optional[date]:
    if not s:
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def _pit_lag_days(period_end: str) -> Optional[int]:
    """季末 MM-DD → 法定披露滞后天数（配置优先，内置常量回退）。"""
    mmdd = period_end[5:10] if len(period_end) >= 10 else period_end
    lags = (_load_pit_cfg().get("disclosure_lag_days") or _DISCLOSURE_LAG_DAYS)
    if mmdd in lags:
        try:
            return int(lags[mmdd])
        except (TypeError, ValueError):
            return None
    return None


def _period_available_date(period_end: str, pub_date: Optional[str]) -> Optional[date]:
    """报告期可用日期：优先真实公告日；缺则按法定披露大限推算。"""
    if _load_pit_cfg().get("prefer_pub_date", True):
        pd_ = _parse_date_str(pub_date)
        if pd_ is not None:
            return pd_
    lag = _pit_lag_days(period_end)
    pe = _parse_date_str(period_end)
    if pe is None:
        return None
    if lag is None:
        return pe  # 无滞后信息则退化为报告期当日（保守下限）
    return pe + timedelta(days=lag)


def _select_pit_period(periods: dict, as_of: date) -> Optional[dict]:
    """在 periods{报告期: 记录(含 _pub)} 中选 as_of 前已披露的最新一期。"""
    best_end = None
    best = None
    for pe, rec in periods.items():
        if not isinstance(rec, dict):
            continue
        avail = _period_available_date(pe, rec.get("_pub"))
        if avail is None or avail > as_of:
            continue
        if best_end is None or pe > best_end:
            best_end = pe
            best = rec
    return best


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
            from smcore.data.kline import list_kline_codes
            sample = list_kline_codes()
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
        # 合并进已有快照：单只补取时不覆盖全市场估值（修复原覆盖写盘隐患）
        if SPOT_FILE.exists():
            try:
                old = pd.read_csv(SPOT_FILE, dtype={"代码": str})
                old["代码"] = old["代码"].astype(str).str.zfill(6)
                df = pd.concat([old, df], ignore_index=True).drop_duplicates(subset=["代码"], keep="last")
            except Exception:
                pass
        df.to_csv(SPOT_FILE, index=False, encoding="utf-8-sig")
    except Exception:
        pass
    return df


def get_valuation(code: str, *, force: bool = False) -> Optional[dict]:
    """返回 {pe, pb, mkt_cap}；缺失/异常返回 None。ps 腾讯无→不再提供。

    快照存在但本票未覆盖时，单只补取腾讯估值并合并写盘（不破坏其他票）。
    """
    code6 = _norm_code(code)
    df = _load_spot_cache() if not force else None
    if df is None:
        df = _fetch_spot_online([code6])
    else:
        present = False
        try:
            present = code6 in set(df["代码"].astype(str).str.zfill(6).tolist())
        except Exception:
            present = False
        if not present:
            fresh = _fetch_spot_online([code6])
            if fresh is not None:
                df = fresh
    out: dict = {}
    if df is not None and "代码" in df.columns:
        row = df[df["代码"] == code6]
        if not row.empty:
            r = row.iloc[0]
            for src, dst in (("pe", "pe"), ("pb", "pb"), ("mkt_cap", "mkt_cap")):
                if src in r.index:
                    try:
                        v = float(r[src])
                        if pd.notna(v) and v > 0:
                            out[dst] = v
                    except (TypeError, ValueError):
                        pass
    # 同花顺估值（如配置 Key）增强：pe/pb/ps/pcf 优先用 THS（比腾讯多 PS/PCF 两口径），
    # mkt_cap 腾讯无对应字段，保留腾讯值（THS 估值端点不含市值）。无 Key 时整体跳过。
    hk = _fetch_valuation_hithink(code6)
    if hk:
        for k in ("pe", "pb", "ps", "pcf"):
            if hk.get(k) is not None:
                out[k] = hk[k]
    return out or None


def _fetch_valuation_hithink(code6: str) -> Optional[dict]:
    """同花顺估值快照 → {pe, pb, ps, pcf}（pe=pe_ttm, pb=pb_mrq）。fail-soft。"""
    from smcore.data import hithink as _hk

    if not _hk.available():
        return None
    v = _hk.fetch_valuation([code6])
    return v.get(code6) if v else None


# ───────────────────────── 质量 + 成长（baostock） ─────────────────────────
def _bs_login() -> bool:
    try:
        from smcore.data.session import login
        return login()
    except Exception:
        return False


def _fetch_profit_growth_periods_baostock(code6: str) -> dict:
    """baostock 盈利 + 成长 → 按报告期归集的 {statDate: {roe, gross_margin, revenue_growth, _pub}}。

    拉取全部历史报告期（不取最新一期），供 PIT 按 as_of 选期。优先用真实公告日 pubDate；
    缺 pubDate 时按法定披露大限兜底。fail-soft：任何异常返回空 dict。
    """
    result: dict[str, dict] = {}
    try:
        import baostock as bs
        if not _bs_login():
            return result
        sym = ("sh." if code6.startswith(("6", "9")) else "sz.") + code6
        # 盈利（ROE / 毛利率）
        for year in range(2020, datetime.now().year + 1):
            for q in (4, 3, 2, 1):
                rs = bs.query_profit_data(code=sym, year=year, quarter=q)
                if rs is None or rs.error_code != "0":
                    continue
                while rs.next():
                    rec = dict(zip(rs.fields, rs.get_row_data()))
                    pe = rec.get("statDate")
                    if not pe:
                        continue
                    d = result.setdefault(pe, {})
                    if rec.get("pubDate"):
                        d["_pub"] = rec["pubDate"]
                    try:
                        if rec.get("roeAvg"):
                            d["roe"] = float(rec["roeAvg"])
                        if rec.get("gpMargin"):
                            d["gross_margin"] = float(rec["gpMargin"])
                    except (TypeError, ValueError):
                        pass
        # 成长（营收同比增长）
        for year in range(2020, datetime.now().year + 1):
            for q in (4, 3, 2, 1):
                rs = bs.query_growth_data(code=sym, year=year, quarter=q)
                if rs is None or rs.error_code != "0":
                    continue
                while rs.next():
                    rec = dict(zip(rs.fields, rs.get_row_data()))
                    pe = rec.get("statDate")
                    if not pe:
                        continue
                    d = result.setdefault(pe, {})
                    if rec.get("pubDate"):
                        d["_pub"] = rec["pubDate"]
                    try:
                        if rec.get("YSTZ"):
                            d["revenue_growth"] = float(rec["YSTZ"])
                    except (TypeError, ValueError):
                        pass
    except Exception:
        return result
    return result


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


# ───────────────────────── 合并 fetch（PIT 合规） ─────────────────────────
def fetch_fundamental(code: str, as_of=None, *, force: bool = False,
                      offline: bool = False) -> Optional[dict]:
    """合并返回单只票的基本面因子原始值（**Point-in-Time 合规**）：
        {roe, gross_margin, revenue_growth, pe, pb, mkt_cap, turnover, amount_20}
    任一子块缺失则其字段为 None（因子层据此降级）。全部缺失返回 None。

    PIT 行为：
    - as_of 为 None（实时/刷新）：用缓存中最新一期报告 + 最新估值快照。
    - as_of 给定（历史回补）：质量/成长取「as_of 前已公告」的最新报告期；缺失该期则降级 None。
      估值快照仅当 as_of ≥ 快照刷新日时可用，否则降级 None（绝不用未来估值污染历史）。

    offline=True：**cache-only 模式**——缓存未命中直接返回 None，绝不联网补取。
      供每日持仓报告等「须离线确定」的批量路径使用（海外 runner 拉 baostock/hithink 常超时，
      缺缓存持仓会白挂 12s 且静默降级，离线模式让缺失变成可预检、可告警的确定性状态）。
    """
    code6 = _norm_code(code)
    cached = None if force else _load_fund_cache(code6)
    if cached is not None:
        # 旧扁平缓存(无 periods)：仅当信号日不早于缓存刷新日时 PIT 有效（实时/近期），
        # 历史回补早于刷新日则降级为 None（规避未来函数）。v2 缓存走 _extract_for_asof 精选期。
        if "periods" not in cached and as_of is not None:
            mtime = _cache_mtime_date(code6)
            a = _parse_date_str(as_of)
            if mtime is not None and a is not None and a < mtime:
                return None
            return cached
        return _extract_for_asof(cached, as_of)
    if offline:
        return None
    built = _build_fundamental_online(code6, as_of)
    if built:
        _save_fund_cache(code6, built)
    return _extract_for_asof(built, as_of)


def fund_cache_exists(code: str) -> bool:
    """报告/批量路径用：判断该票本地基本面缓存是否可用（**不联网**）。

    命中条件与 fetch_fundamental(offline=True) 完全一致（缓存文件存在且未超 TTL）。
    供每日报告预检「哪些持仓缺缓存」，避免缺缓存持仓静默显示「暂无基本面数据」。
    """
    try:
        return _load_fund_cache(_norm_code(code)) is not None
    except Exception:
        return False


def _build_fundamental_online(code6: str, as_of=None) -> Optional[dict]:
    """联网构建 v2 缓存结构：{periods, spot, kline_stats, _spot_as_of}。fail-soft。"""
    periods = _fetch_profit_growth_periods_baostock(code6)
    val = get_valuation(code6)
    ks = _fetch_kline_stats_baostock(code6, as_of)
    if not periods and not val and not ks:
        return None
    return {
        "_v": CACHE_VERSION,
        "periods": periods,
        "spot": val or {},
        "kline_stats": ks or {},
        "_spot_as_of": datetime.now().strftime("%Y-%m-%d"),
    }


def _extract_for_asof(data: Optional[dict], as_of=None) -> Optional[dict]:
    """从 v2 缓存结构按 as_of 提取扁平基本面 dict；旧扁平缓存(无 periods)保守降级。"""
    if not data or not isinstance(data, dict):
        return None
    if "periods" not in data:
        # 旧扁平格式：无报告期历史 → 仅实时(as_of=None)可用；历史回补视为不可用（规避未来函数）
        return data if as_of is None else None
    as_of_d = _parse_date_str(as_of)
    out: dict = {}
    # 质量/成长：PIT 选期
    periods = data.get("periods") or {}
    if as_of_d is None:
        if periods:
            latest = max(periods.keys())
            rec = periods[latest] or {}
            for k in ("roe", "gross_margin", "revenue_growth"):
                if rec.get(k) is not None:
                    out[k] = rec[k]
    else:
        pit = _select_pit_period(periods, as_of_d)
        if pit:
            for k in ("roe", "gross_margin", "revenue_growth"):
                if pit.get(k) is not None:
                    out[k] = pit[k]
    # 估值：最新快照仅在 as_of ≥ 快照刷新日时可用（保守，避免用未来估值）
    spot = data.get("spot") or {}
    spot_as_of = _parse_date_str(data.get("_spot_as_of"))
    if spot and (as_of_d is None or spot_as_of is None or as_of_d >= spot_as_of):
        for k in ("pe", "pb", "mkt_cap"):
            if spot.get(k) is not None:
                out[k] = spot[k]
    # 资金流：换手/成交额来自刷新日 K 线，历史回补近似沿用（慢变量，后续可改本地 K 线重算）
    ks = data.get("kline_stats") or {}
    if ks:
        for k in ("turnover", "amount_20"):
            if ks.get(k) is not None:
                out[k] = ks[k]
    return out or None


def fetch_fundamentals_batch(codes, as_of=None, *, force: bool = False) -> dict:
    """批量：返回 {code: fundamental_dict|None}。逐个拉取，互不波及。"""
    out: dict = {}
    for c in codes:
        try:
            out[str(c).strip()] = fetch_fundamental(str(c).strip(), as_of, force=force)
        except Exception:
            out[str(c).strip()] = None
    return out


def fetch_fundamental_hithink(code: str, report: str = "") -> Optional[dict]:
    """同花顺官方五类财务指标（growth/profitability/solvency/operation/cash-flow）。

    定位：作为 fundamental 的**可选第四源**（现有三源：腾讯估值 + baostock 质量/成长/换手）。
    report: "yyyy-1"~"yyyy-4"（如 "2025-4"）；空则跳过（需显式期号，避免误用过期报告）。
    返回 {index_id: value} 或 None。fail-soft：需 HITHINK_FINANCE_API_KEY + 联网。
    注意：同花顺估值(PE/PB/PS/PC)端点尚未在契约中暴露，现有估值仍走腾讯 qt.gtimg.cn。
    """
    from smcore.data import hithink as _hk

    if not _hk.available() or not report:
        return None
    ind = _hk.fetch_indicators(code, report)
    return ind or None


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


def _cache_mtime_date(code: str) -> Optional[date]:
    """缓存文件刷新日（mtime）→ date，用于扁平缓存的 PIT 有效性判断。"""
    p = _fund_cache_file(code)
    if not p.exists():
        return None
    try:
        return datetime.fromtimestamp(p.stat().st_mtime).date()
    except Exception:
        return None


def _load_fund_cache(code: str) -> Optional[dict]:
    p = _fund_cache_file(code)
    if not p.exists() or _cache_age_days(p) > CACHE_TTL_DAYS:
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    # v2（含 periods）或旧扁平格式均原样返回，由 _extract_for_asof 解析
    return data if isinstance(data, dict) else None


def _save_fund_cache(code: str, data: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        # v2 合并：保留历史已存报告期，避免重复刷新覆盖旧期
        p = _fund_cache_file(code)
        if p.exists():
            try:
                old = json.loads(p.read_text(encoding="utf-8"))
                if (isinstance(old, dict) and "periods" in old
                        and isinstance(data, dict) and "periods" in data):
                    merged = dict(old.get("periods", {}))
                    merged.update(data.get("periods", {}))
                    data = {
                        "_v": CACHE_VERSION,
                        "periods": merged,
                        "spot": data.get("spot") or old.get("spot"),
                        "kline_stats": data.get("kline_stats") or old.get("kline_stats"),
                        "_spot_as_of": data.get("_spot_as_of") or old.get("_spot_as_of"),
                    }
            except Exception:
                pass
        p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass
