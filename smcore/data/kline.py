"""K线数据获取 —— 单一真相源（强制前复权）。

合并自 boll-visualizer/src/core/data_fetcher.py 的 K线部分，关键改动：
- 强制前复权(qfq)：此前 Boll 选股用不复权(adjustflag=3)，
  除权除息日布林带断裂、信号失真，是"结果不可信"的头号原因。
- 统一 baostock 会话：用 core.data.session 单例，避免每只股票重复登录。
- 云端后端：环境变量 KLINE_BACKEND=akshare 时改用 akshare HTTP 接口（东财数据源），
  不依赖 baostock 登录会话，适合 GitHub Actions / SCF 等云端环境。
"""
from __future__ import annotations

import os
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from smcore.config import ADJUST_FLAG_MAP, CACHE_DIR, CSV_ENCODING, DEFAULT_ADJUST, STOCK_DATA_DIR
from smcore.utils.code import format_stock_code, to_baostock_code

# K 线缓存单独放在 stock_data/k_data/（受追踪、随仓库提交），
# 不放在 stock_data/cache/ 下（该目录被 .gitignore 整目录忽略，会导致云端每次冷启动重抓）。
K_DATA_CACHE_DIR = STOCK_DATA_DIR / "k_data"
DAILY_K_COLUMNS = ["date", "open", "high", "low", "close", "volume", "amount"]

# ── 复权基准漂移守卫 ──
# 前复权价以「最新交易日」为锚：此后一旦发生分红送转，整条历史序列都会被重新缩放。
# 因此「把新拉的几天直接拼到旧缓存后面」在物理上就是错的 —— 旧段停留在过期基准、
# 新段用新基准，接缝处出现断层（实测 600900 长江电力接缝单日 +46%，物理不可能）。
# 断层不只毒化回测成交价，更会静默毒化所有跨接缝的回看指标（MA/布林/动量/相对强度）。
# 守卫：增量拉取强制与缓存重叠若干日，比对重叠日收盘；偏差超容差即判定基准漂移，
# 丢弃缓存全量重拉。容差取 0.5%（小于最低分红率，又大于浮点/数据源舍入噪声）。
KLINE_OVERLAP_DAYS = int(os.getenv("KLINE_OVERLAP_DAYS", "7"))
KLINE_DRIFT_TOL = float(os.getenv("KLINE_DRIFT_TOL", "0.005"))

# 重叠检测只能发现「接缝处」漂移；若污染位于缓存中段（历史某次追加留下的），
# 接缝可能完全一致而中段仍是坏的。因此落盘前还要做整段自洽性检查：
# 相邻交易日跳变一旦超过该板块涨跌停上限，就是非市场行为 —— 只可能是复权错误。
# 板块涨跌停随交易所规则固定，非策略超参；留环境变量仅为便于测试与规则变更。
PRICE_LIMIT_MAIN = float(os.getenv("KLINE_LIMIT_MAIN", "10"))      # 主板 60/00/002
PRICE_LIMIT_GROWTH = float(os.getenv("KLINE_LIMIT_GROWTH", "20"))  # 创业板 300/301、科创 688
PRICE_LIMIT_BJ = float(os.getenv("KLINE_LIMIT_BJ", "30"))          # 北交所 8/4
PRICE_LIMIT_MARGIN = float(os.getenv("KLINE_LIMIT_MARGIN", "1.15"))  # 余量，吸收停复牌等边缘情形
JUMP_SKIP_HEAD_BARS = int(os.getenv("KLINE_JUMP_SKIP_HEAD", "10"))   # 新股上市初期不设涨跌幅限制

# 同花顺复权因子事件流对 qfq 守卫的交叉校验（默认开；无 Key/异常自动跳过，不改动重拉行为）
HITHINK_QFQ_CHECK = os.getenv("HITHINK_QFQ_CHECK", "1") == "1"


def price_limit_ratio(code6: str) -> float:
    """该股单日价格变动的物理上限（比值，如 1.115）。超过即非市场行为。"""
    c = str(code6)
    if c.startswith(("300", "301", "688")):
        pct = PRICE_LIMIT_GROWTH
    elif c.startswith(("8", "4")):
        pct = PRICE_LIMIT_BJ
    else:
        pct = PRICE_LIMIT_MAIN
    return 1 + pct * PRICE_LIMIT_MARGIN / 100.0


def find_price_breaks(df: pd.DataFrame, code6: str) -> list[dict]:
    """扫描收盘价序列中的复权断层，返回断层点列表（正常序列返回 []）。"""
    if df is None or len(df) < 2 or "close" not in df.columns:
        return []
    close = pd.to_numeric(df["close"], errors="coerce").reset_index(drop=True)
    dates = df["date"].reset_index(drop=True) if "date" in df.columns else close.index.to_series()
    up = price_limit_ratio(code6)
    ratio = close / close.shift(1)
    hits = ratio[(ratio > up) | (ratio < 1 / up)]
    return [
        {
            "date": str(dates.iloc[i]),
            "prev_close": round(float(close.iloc[i - 1]), 4),
            "close": round(float(close.iloc[i]), 4),
            "ratio": round(float(ratio.iloc[i]), 4),
        }
        for i in hits.index
        if i >= JUMP_SKIP_HEAD_BARS and pd.notna(ratio.iloc[i])
    ]


def _backend() -> str:
    """返回当前 K 线后端：tdx（最快）> baostock（本地）> akshare（云端兜底）。

    优先读取 KLINE_BACKEND 环境变量（可强制 tdx/baostock/akshare）；
    未设置时自动检测：通达信可用则优先（毫秒级、直连券商、最稳），
    否则 baostock，再否则 akshare。
    """
    backend = os.getenv("KLINE_BACKEND", "").strip().lower()
    if backend in ("tdx", "baostock", "akshare", "hithink"):
        return backend
    # 自动检测优先级：tdx(本地直连,毫秒级) > hithink(官方云API,有Key且联网) > baostock > akshare
    # GitHub Actions 等海外/无终端环境 tdx 不可用，hithink 有 Key 时自动成为云端最快首选。
    try:
        from smcore.data.tdx_client import available as tdx_available
        if tdx_available():
            return "tdx"
    except Exception:
        pass
    try:
        from smcore.data import hithink as _hk
        if _hk.available():
            return "hithink"
    except Exception:
        pass
    try:
        import baostock as bs  # noqa: F401
        return "baostock"
    except ImportError:
        return "akshare"


def _to_date_string(value) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    text = str(value)
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def _to_date(value) -> date:
    return pd.to_datetime(value).date()


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=DAILY_K_COLUMNS)


def _call_with_timeout(func, timeout: float):
    """在 daemon 线程中执行 func，超时（挂起）则返回 None 而非永久阻塞。

    用于包裹 akshare 等无内置超时的网络调用，保证云端流水线「不会挂」。
    """
    box: dict = {}

    def _run():
        try:
            box["r"] = func()
        except BaseException:
            box["e"] = True

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    worker.join(timeout)
    if worker.is_alive() or "e" in box:
        return None
    return box.get("r")


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "date" not in df.columns:
        return _empty_df()
    out = df.copy()
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        if col not in out.columns:
            out[col] = pd.NA
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date", "close"]).sort_values("date")
    # 同一交易日只保留最后一条：concat(缓存, 新拉段) 时重叠日必然重复，
    # 保留后者（新拉的）才是最新复权基准。此前缺失去重会让重叠日残留两行。
    out = out.drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    if out.empty:
        return _empty_df()
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out[DAILY_K_COLUMNS]


def _cache_path(code: str, adjust: str) -> Path:
    K_DATA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return K_DATA_CACHE_DIR / f"{format_stock_code(code)}_{adjust}_full.csv"


def _is_fresh(path: Path, max_age_hours: float) -> bool:
    if not path.exists():
        return False
    if max_age_hours <= 0:
        return True
    age = datetime.now().timestamp() - path.stat().st_mtime
    return age <= max_age_hours * 3600


def _detect_adjust_drift(cached: pd.DataFrame, fresh: pd.DataFrame) -> float:
    """比对缓存与新拉数据在重叠交易日上的收盘价，返回最大相对偏差。

    返回 -1.0 表示无重叠、无法判定（调用方按「不阻断」处理）。
    """
    if cached is None or cached.empty or fresh is None or fresh.empty:
        return -1.0
    new = _normalize(fresh)
    if new.empty:
        return -1.0
    merged = cached[["date", "close"]].merge(
        new[["date", "close"]], on="date", suffixes=("_old", "_new")
    )
    if merged.empty:
        return -1.0
    old_c = pd.to_numeric(merged["close_old"], errors="coerce")
    new_c = pd.to_numeric(merged["close_new"], errors="coerce")
    ok = (old_c > 0) & (new_c > 0)
    if not ok.any():
        return -1.0
    return float(((new_c[ok] - old_c[ok]).abs() / old_c[ok]).max())


def _slice(df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    if df.empty:
        return df
    tmp = df.copy()
    tmp["_dt"] = pd.to_datetime(tmp["date"], errors="coerce")
    mask = (tmp["_dt"].dt.date >= start) & (tmp["_dt"].dt.date <= end)
    tmp = tmp[mask].drop(columns=["_dt"])
    return _normalize(tmp)


def fetch_daily_k(
    code,
    start_date,
    end_date,
    adjust: str = DEFAULT_ADJUST,
    use_cache: bool = True,
    force_refresh: bool = False,
    max_cache_age_hours: float = 24.0,
    _no_retry: bool = False,
) -> pd.DataFrame:
    """获取日 K 线（默认前复权），带文件缓存与增量合并。

    Args:
        code: 股票代码（任意格式）。
        start_date / end_date: 日期（date/datetime/字符串/YYYYMMDD 均可）。
        adjust: 复权方式 qfq(默认)/hfq/bfq。强制不传 "3"（不复权）以避免信号失真。
        use_cache / force_refresh / max_cache_age_hours: 缓存控制。
    """
    code6 = format_stock_code(code)
    if not code6:
        return _empty_df()
    adjust = str(adjust).lower()
    flag = ADJUST_FLAG_MAP.get(adjust, "2")  # 兜底前复权
    request_start = _to_date(start_date)
    request_end = _to_date(end_date)
    if request_start > request_end:
        return _empty_df()

    cache = _cache_path(code6, adjust)
    cached = pd.DataFrame()
    if use_cache and not force_refresh and cache.exists():
        try:
            cached = _normalize(pd.read_csv(cache))
        except Exception:
            cached = pd.DataFrame()

    cache_min, cache_max = None, None
    if not cached.empty:
        dt = pd.to_datetime(cached["date"], errors="coerce").dropna()
        if not dt.empty:
            cache_min, cache_max = dt.min().date(), dt.max().date()

    covers = bool(cache_min and cache_max and cache_min <= request_start and cache_max >= request_end)
    fresh = _is_fresh(cache, max_cache_age_hours)
    if covers and (fresh or request_end < date.today() - timedelta(days=1)):
        return _slice(cached, request_start, request_end)

    segments: list[tuple[date, date]] = []
    if force_refresh or cached.empty or cache_min is None:
        segments.append((request_start, request_end))
    else:
        # 前导缺口：缓存起点之前的请求区间（极少触发，缓存通常从上市起覆盖）
        if request_start < cache_min:
            segments.append((request_start, min(request_end, cache_min - timedelta(days=1))))
        # 尾部缺口：缓存终点之后的请求区间（每个交易日新增的部分）。
        # 起点强制前移 KLINE_OVERLAP_DAYS 与缓存重叠，重叠段用于复权基准漂移检测；
        # 重叠行会在 _normalize 去重时被新数据覆盖，不会产生重复行。
        if request_end > cache_max:
            segments.append((cache_max - timedelta(days=KLINE_OVERLAP_DAYS), request_end))

    parts: list[pd.DataFrame] = []

    def _fetch_segment(seg_start: date, seg_end: date, backend: str) -> pd.DataFrame:
        if seg_start > seg_end:
            return pd.DataFrame()
        if backend == "tdx":
            return _fetch_via_tdx(code6, seg_start, seg_end, adjust)
        if backend == "akshare":
            return _fetch_via_akshare(code6, seg_start, seg_end, adjust)
        if backend == "hithink":
            return _fetch_via_hithink(code6, seg_start, seg_end, adjust)
        # baostock
        import baostock as bs
        from smcore.data.session import session
        with session() as ok:
            if not ok:
                return pd.DataFrame()
            rs = bs.query_history_k_data_plus(
                to_baostock_code(code6),
                "date,code,open,high,low,close,volume,amount",
                start_date=_to_date_string(seg_start),
                end_date=_to_date_string(seg_end),
                frequency="d",
                adjustflag=flag,
            )
            if rs.error_code != "0":
                return pd.DataFrame()
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())
            return pd.DataFrame(rows, columns=rs.fields) if rows else pd.DataFrame()

    # 后端优先级：首选 tdx（最快最稳），失败自动回退 akshare → baostock
    preferred = _backend()
    fallback_chain = [preferred] + [b for b in ("tdx", "akshare", "baostock") if b != preferred]
    for backend in fallback_chain:
        parts = []
        for seg_start, seg_end in segments:
            seg_df = _fetch_segment(seg_start, seg_end, backend)
            if not seg_df.empty:
                parts.append(seg_df)
        if parts or not cached.empty:
            break  # 拿到数据或用缓存即可，不再回退

    if cached.empty and not parts:
        return _empty_df()

    # ── 复权基准守卫 ──
    # 两层防御，确保落盘数据「单一复权基准、物理自洽」：
    #  ① 接缝漂移检测：缓存与新段在重叠交易日的收盘偏差 > 容差，说明缓存基准已过期，
    #     丢弃缓存、从最早日全量重拉（保留历史区间）。
    #  ② 整段自洽性检查（落盘前）：扫整条 merged 序列，相邻交易日跳变超该板块涨跌停
    #     即物理不可能（只可能是复权错误）——含缓存中段的历史污染（接缝一致也抓不到）。
    # 注意：重拉调用传入 force_refresh=True，本守卫的 `not force_refresh` 条件使其不会重入，
    # 因此不会无限递归；重拉得到的是单一源全量数据，至少内部自洽（真有跳变则告警接受）。
    if parts and not cached.empty and not force_refresh:
        drift = _detect_adjust_drift(cached, pd.concat(parts, ignore_index=True))
        if drift > KLINE_DRIFT_TOL:
            return fetch_daily_k(
                code6,
                min(request_start, cache_min or request_start),
                request_end,
                adjust=adjust,
                use_cache=use_cache,
                force_refresh=True,
                max_cache_age_hours=max_cache_age_hours,
                _no_retry=True,
            )

    merged = _normalize(pd.concat([cached, *parts], ignore_index=True)) if (not cached.empty or parts) else _empty_df()
    if merged.empty and not cached.empty:
        merged = _normalize(cached)

    # ② 整段自洽性检查：任何非市场跳变都触发全量重拉
    if not merged.empty:
        breaks = find_price_breaks(merged, code6)
        if breaks and HITHINK_QFQ_CHECK:
            # 交叉校验：断层若由真实分红/送股解释，则属「缓存基准过期需重拉」的预期事件；
            # 无法解释的断层疑似真实数据错误，单独告警（仍触发下方全量重拉，安全不变）。
            try:
                from smcore.data import hithink as _hk
                if _hk.available():
                    from smcore.data.hithink_special import classify_breaks
                    breaks = classify_breaks(code6, breaks)
                    _unexplained = [b for b in breaks if not b.get("explained_by_corporate_action")]
                    if _unexplained:
                        import warnings as _w
                        _w.warn(
                            f"[kline] {code6} {len(_unexplained)} 处复权断层无法用分红/送股解释"
                            f"（疑似真实数据错误，将触发全量重拉）；可解释={len(breaks) - len(_unexplained)}"
                        )
            except Exception:
                pass
        if breaks:
            if (not force_refresh) and (not _no_retry):
                return fetch_daily_k(
                    code6,
                    min(request_start, cache_min or request_start),
                    request_end,
                    adjust=adjust,
                    use_cache=use_cache,
                    force_refresh=True,
                    max_cache_age_hours=max_cache_age_hours,
                    _no_retry=True,
                )
            import warnings
            warnings.warn(
                f"[kline] {code6} {len(breaks)} 处复权跳变未被自愈，已用单次全量拉取覆盖"
                f"（可能是真实除权/停复牌导致的合法大跳变）。"
            )

    if use_cache and not merged.empty:
        merged.to_csv(cache, index=False, encoding=CSV_ENCODING)
    return _slice(merged, request_start, request_end) if not merged.empty else _empty_df()


# ── 通达信后端（高速主源）──

def _fetch_via_tdx(code6: str, start: date, end: date, adjust: str) -> pd.DataFrame:
    """通过通达信直连获取 K 线（自带前复权，毫秒级）。失败返回空。"""
    try:
        from smcore.data.tdx_client import get_client
        df = get_client().get_daily_k(code6, start, end, adjust)
        if df is None or df.empty:
            return pd.DataFrame()
        return df[DAILY_K_COLUMNS]
    except Exception:
        return pd.DataFrame()


# ── 同花顺官方 Financial-API 后端（云端，需 HITHINK_FINANCE_API_KEY）──

def _fetch_via_hithink(code6: str, start: date, end: date, adjust: str) -> pd.DataFrame:
    """通过同花顺官方 Financial-API 获取历史日 K（云端，需 API Key）。

    复用 hithink.fetch_historical_k，返回 kline.py 规范列，由 _normalize 统一。
    Key 缺失或失败返回空（fail-soft，交给回退链）。
    """
    try:
        from smcore.data import hithink as _hk
        if not _hk.available():
            return pd.DataFrame()
        return _hk.fetch_historical_k(code6, start, end, adjust)
    except Exception:
        return pd.DataFrame()


# ── akshare 后端（云端用） ──

_AK_COL_MAP = {
    "日期": "date", "开盘": "open", "收盘": "close",
    "最高": "high", "最低": "low", "成交量": "volume", "成交额": "amount",
}


def _fetch_via_akshare(code6: str, start: date, end: date, adjust: str) -> pd.DataFrame:
    """通过 akshare 新浪接口获取 K 线（无需登录会话）。

    使用 stock_zh_a_daily（新浪数据源），不依赖东财接口。
    """
    try:
        import akshare as ak
    except ImportError:
        return pd.DataFrame()

    # 新浪格式 symbol：sh600519 / sz000001
    sina_symbol = ("sh" if code6.startswith(("5", "6", "9")) else "sz") + code6

    # akshare 复权参数：qfq/hfq/"" (空=不复权)
    ak_adjust = adjust if adjust in ("qfq", "hfq") else ""
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")

    # 云端环境 akshare 偶发挂起/瞬断：超时(30s) + 重试(2 次) 兜底，保证「不会挂」
    raw = None
    for attempt in range(2):
        raw = _call_with_timeout(
            lambda: ak.stock_zh_a_daily(
                symbol=sina_symbol,
                start_date=start_str,
                end_date=end_str,
                adjust=ak_adjust,
            ),
            30,
        )
        if raw is not None and not raw.empty:
            break
        if attempt < 1:
            time.sleep(1.0)

    if raw is None or raw.empty:
        return pd.DataFrame()

    # stock_zh_a_daily 返回英文列名：date, open, high, low, close, volume, amount
    out = raw.copy()
    for col in DAILY_K_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    return out[DAILY_K_COLUMNS]
