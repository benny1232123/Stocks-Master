"""同花顺官方金融数据服务 (HiThink-Tech/Financial-API) 适配层。

官方仓库: https://github.com/HiThink-Tech/Financial-API
Base URL: https://fuyao.aicubes.cn
鉴权: HTTP Header `X-api-key`，值取环境变量 HITHINK_FINANCE_API_KEY
响应信封: {"code":0,"message":...,"request_id":...,"data":{...}}，HTTP 恒 200，业务错误看 code
  (2001 未认证 / 2003 权限不足)
时间: 毫秒级 Unix 时间戳，时区 Asia/Shanghai

设计约定（与本项目现有 baostock/akshare 后端一致）:
- 配置驱动: 仅当 HITHINK_FINANCE_API_KEY 非空时 available()；缺失时所有函数 fail-soft 返回 None/空。
- 零新依赖: 复用 requirements 中已有的 requests。
- 不破坏现有源: 本模块独立，kline.py 仅在 KLINE_BACKEND=hithink 时调用。
- 复权口径: 历史K线 adjust 支持 forward(前复权)/backward(后复权)/none(不复权)；
  本项目强制前复权，调用方传 qfq → forward。

注意: 估值快照(PE/PB/PS/PC) 在已抓取契约(llms-full.txt)中未暴露端点，
fetch_valuation 暂返回 None，待持有 Key 后实测补充（现有估值仍走腾讯 qt.gtimg.cn）。
"""
from __future__ import annotations

import os
import time
from datetime import date, datetime, timezone, timedelta

import pandas as pd
import requests

from smcore.utils.code import to_thscode, format_stock_code

def _base() -> str:
    return os.getenv("HITHINK_BASE", "https://fuyao.aicubes.cn").rstrip("/")


def _api_key() -> str:
    return (os.getenv("HITHINK_FINANCE_API_KEY") or "").strip()


def _timeout() -> float:
    try:
        return float(os.getenv("HITHINK_TIMEOUT", "20"))
    except (TypeError, ValueError):
        return 20.0
_SH = timezone(timedelta(hours=8))  # Asia/Shanghai，避免引入 pytz

# 本项目 adjust(qfq/hfq/bfq) → 同花顺 adjust(forward/backward/none)
_ADJ_MAP = {
    "qfq": "forward",
    "hfq": "backward",
    "bfq": "none",
    "forward": "forward",
    "backward": "backward",
    "none": "none",
}


def available() -> bool:
    """API Key 是否已配置（决定是否启用本后端）。"""
    return bool(_api_key())


def _ms(d) -> int:
    """date / datetime / 'YYYY-MM-DD' 字符串 → 上海时区毫秒戳。"""
    if isinstance(d, str):
        d = datetime.strptime(d, "%Y-%m-%d").date()
    elif isinstance(d, datetime):
        d = d.date()
    return int(datetime(d.year, d.month, d.day, tzinfo=_SH).timestamp() * 1000)


def _ms_to_date(ms) -> str:
    if not ms:
        return ""
    return datetime.fromtimestamp(int(ms) / 1000, tz=_SH).strftime("%Y-%m-%d")


def _num(v):
    try:
        if v in (None, "", "None", "null"):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _get(path: str, params: dict | None = None, retries: int = 2):
    """GET 并解包 data；任何失败/业务错误返回 None（fail-soft）。"""
    if not _api_key():
        return None
    url = _base() + path
    headers = {"X-api-key": _api_key()}
    for _ in range(retries + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=_timeout())
            body = r.json()
            if body.get("code") != 0:
                return None
            return body.get("data")
        except Exception:
            time.sleep(1)
    return None


# ───────────────────────── K 线 ─────────────────────────

# ⚠️ 个股历史 K 的**单次请求跨度上限**（2026-09-14 实测；见 skills/hithink-finance-api §限制2）：
#    ``start`` 距 ``end`` > **~3652 自然日** → 服务端**整段静默返回空**（code=0 且 item=[]），
#    既不报错也不截断。留足余量取 1500 天。
#    （本文件此前写「个股无此限制」是**错的**，已按实测更正。）
#
# 为什么必须在本函数内分片（2026-09-17 事故取证，见 .workbuddy/_probe_repull.txt）：
#    ``kline.fetch_daily_k`` 的「复权基准守卫」发现历史断层时会**从缓存最早日全量重拉**，
#    即请求 ``2015-01-05 ~ 今天``（>4000 天）。不分片 ⇒ hithink 静默返空 ⇒ 回退链
#    **静默换源到 akshare**，让 akshare 的序列**整段覆盖** hithink 缓存（实测 000019：
#    2844 行 → 2690 行，数据来源被换掉），随后断层归零、守卫误判「自愈成功」并静默接受。
#    跨源混血污染由此产生，且完全不可见。
_HK_HIST_CHUNK_DAYS = int(os.getenv("HITHINK_HIST_CHUNK_DAYS", "1500"))
_HK_HIST_CHUNK_RETRY = int(os.getenv("HITHINK_HIST_CHUNK_RETRY", "3"))

_HIST_COLUMNS = ["date", "open", "high", "low", "close", "volume", "amount"]


def _hist_row(it: dict) -> dict:
    return {
        "date": _ms_to_date(it.get("date_ms")),
        "open": _num(it.get("open_price")),
        "high": _num(it.get("high_price")),
        "low": _num(it.get("low_price")),
        "close": _num(it.get("close_price")),
        "volume": _num(it.get("volume")),
        "amount": _num(it.get("turnover")),
    }


def fetch_historical_k(code, start: date, end: date, adjust: str = "qfq") -> pd.DataFrame:
    """历史日 K，返回 kline.py 规范列: date,open,high,low,close,volume,amount。

    adjust: qfq(前复权,默认)/hfq/bfq。

    内部按 ``_HK_HIST_CHUNK_DAYS`` 分片请求（绕开服务端跨度上限）后拼接、按日去重。
    分片语义是**全有或全无**：一旦已经开始拿到数据，任一分片重试后仍为空 → 整次返回空。
    宁可让调用方降级到其它后端，也不交出一条中途被挖空的序列 —— 截断序列看起来
    「正常且全为正价」，仅靠「非空 + 正价」判据检不出（2026-09-14 实测 002533 /
    300189 / 600644 / 603033 因此丢过 2023-03 之后的全部数据）。
    例外：**开头**的空分片（请求起点早于上市日）允许跳过，只要后面有数据。
    """
    ts = to_thscode(code)
    if not ts:
        return pd.DataFrame()
    adj = _ADJ_MAP.get(str(adjust).lower(), "forward")
    try:
        s_d, e_d = _as_date(start), _as_date(end)
    except Exception:
        return pd.DataFrame()
    if s_d > e_d:
        return pd.DataFrame()

    attempts = max(1, _HK_HIST_CHUNK_RETRY)
    rows: list = []
    seen_data = False
    cur = s_d
    while cur <= e_d:
        chunk_end = min(cur + timedelta(days=_HK_HIST_CHUNK_DAYS), e_d)
        items: list = []
        for attempt in range(attempts):
            data = _get(
                "/api/a-share/prices/historical",
                {"thscode": ts, "interval": "1d",
                 "start": _ms(cur), "end": _ms(chunk_end), "adjust": adj},
            )
            items = (data or {}).get("item") or []
            if items:
                break
            if attempt + 1 < attempts:
                time.sleep(0.5 * (attempt + 1))
        if items:
            rows.extend(_hist_row(it) for it in items if it.get("date_ms"))
            seen_data = True
        elif seen_data:
            # 有数据之后出现空洞 = 接口抖动/限流（或跨度仍超限）→ 不可交付截断序列
            import sys as _sys
            print(
                f"[hithink] WARN: {ts} {cur}~{chunk_end} 分片重试 {attempts} 次仍为空"
                f"（接口抖动/限流）→ 本次整体判失败，交调用方降级到其它后端",
                file=_sys.stderr,
            )
            return pd.DataFrame()
        # 否则：开头的空分片 = 请求起点早于上市日，正常跳过
        cur = chunk_end + timedelta(days=1)

    if not rows:
        return pd.DataFrame(columns=_HIST_COLUMNS)
    return pd.DataFrame(rows, columns=_HIST_COLUMNS).drop_duplicates(
        subset=["date"], keep="last"
    ).reset_index(drop=True)


# ───────────────────────── 行情快照 ─────────────────────────
def fetch_snapshot(codes) -> pd.DataFrame:
    """最新行情快照；codes 可 6位/ths；返回 thscode,last_price,... 的 DataFrame。"""
    if not _api_key():
        return pd.DataFrame()
    if isinstance(codes, (list, tuple, set)):
        ts_list = [to_thscode(c) for c in codes]
    else:
        ts_list = [to_thscode(codes)]
    ts_list = [t for t in ts_list if t]
    if not ts_list:
        return pd.DataFrame()
    data = _get("/api/a-share/prices/snapshot", {"thscodes": ",".join(ts_list)})
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data.get("item") or [])


# ───────────────────────── 财务报表与指标 ─────────────────────────
def fetch_indicators(code, report: str = "") -> dict:
    """五类财务指标（growth/profitability/solvency/operation/cash-flow）。

    report: "yyyy-1"~"yyyy-4"（如 "2025-4"）。返回 {index_id: float_or_None}。
    """
    ts = to_thscode(code)
    if not ts or not report:
        return {}
    data = _get("/api/a-share/financials/indicators", {"thscode": ts, "report": report})
    out: dict = {}
    if not data:
        return out
    for ab in data.get("abilities") or []:
        for ind in ab.get("indicators") or []:
            vid = ind.get("index_id")
            if vid:
                out[vid] = _num(ind.get("value"))
    return out


def fetch_income_statements(code, period: str = "annual", limit: int = 4) -> list:
    ts = to_thscode(code)
    if not ts:
        return []
    data = _get("/api/a-share/financials/income-statements", {"thscode": ts, "period": period, "limit": limit})
    return data.get("item") or [] if data else []


def fetch_balance_sheets(code, period: str = "annual", limit: int = 4) -> list:
    ts = to_thscode(code)
    if not ts:
        return []
    data = _get("/api/a-share/financials/balance-sheets", {"thscode": ts, "period": period, "limit": limit})
    return data.get("item") or [] if data else []


def fetch_cash_flow_statements(code, period: str = "annual", limit: int = 4) -> list:
    ts = to_thscode(code)
    if not ts:
        return []
    data = _get("/api/a-share/financials/cash-flow-statements", {"thscode": ts, "period": period, "limit": limit})
    return data.get("item") or [] if data else []


def fetch_valuation(codes) -> dict:
    """A 股估值快照（批量）→ {code6: {pe, pb, ps, pcf, pe_mrq}}。

    GET /api/a-share/valuations/snapshot?thscodes=...，单次 ≤100 只（服务端硬上限）。
    返回字段：pe_ttm/pe_mrq/pb_mrq/ps_ttm/pcf_ttm。本系统映射 pe=pe_ttm、pb=pb_mrq。
    **不含市值**（mkt_cap 仍走腾讯 qt.gtimg.cn）。fail-soft：无 Key/失败返回 {}。
    """
    if not _api_key():
        return {}
    if isinstance(codes, (str, int)):
        codes = [str(codes)]
    ts_list = [to_thscode(c) for c in codes]
    ts_list = [t for t in ts_list if t]
    if not ts_list:
        return {}
    out: dict = {}
    for i in range(0, len(ts_list), 100):
        batch = ts_list[i : i + 100]
        data = _get("/api/a-share/valuations/snapshot", {"thscodes": ",".join(batch)})
        if not data:
            continue
        for it in data.get("item") or []:
            c6 = format_stock_code(it.get("ticker") or "")
            if not c6:
                continue
            out[c6] = {
                "pe": _num(it.get("pe_ttm")),
                "pe_mrq": _num(it.get("pe_mrq")),
                "pb": _num(it.get("pb_mrq")),
                "ps": _num(it.get("ps_ttm")),
                "pcf": _num(it.get("pcf_ttm")),
            }
    return out


# ───────────────────────── 特色数据（本项目原本缺失） ─────────────────────────
def _special(path: str, params: dict | None = None) -> list:
    data = _get(path, params)
    if not data:
        return []
    return data.get("item") or []


def limit_up_pool(trade_date: str | None = None) -> list:
    return _special("/api/a-share/special-data/limit-up-pool", {"date": trade_date} if trade_date else None)


def limit_down_pool(trade_date: str | None = None) -> list:
    return _special("/api/a-share/special-data/limit-down-pool", {"date": trade_date} if trade_date else None)


def limit_break_pool(trade_date: str | None = None) -> list:
    return _special("/api/a-share/special-data/limit-break-pool", {"date": trade_date} if trade_date else None)


def limit_up_ladder() -> list:
    return _special("/api/a-share/special-data/limit-up-ladder")


def dragon_tiger_list(trade_date: str | None = None, board_type: str = "all") -> list:
    params = {"board_type": board_type}
    if trade_date:
        params["date"] = trade_date
    return _special("/api/a-share/special-data/dragon-tiger-list", params)


def skyrocket_list() -> list:
    return _special("/api/a-share/special-data/skyrocket-list")


def hot_stock_list() -> list:
    return _special("/api/a-share/special-data/hot-stock-list")


# ───────────────────────── 板块 / 指数（theme 策略增量） ─────────────────────────
def concept_list(tag: str = "cn_concept") -> list:
    """同花顺概念板块目录；tag=cn_concept/region/tszs/industry。"""
    data = _get("/api/a-share-index/catalog/ths-index-list", {"tag": tag})
    return data.get("item") or [] if data else []


def concept_stocks(thscode: str) -> list:
    """板块成分股；thscode 为板块 thscode（如概念板块的 thscode）。"""
    data = _get("/api/a-share-index/constituents/ths-stock-list", {"thscode": thscode})
    return data.get("item") or [] if data else []


# ───────────────────────── 标的检索 / 日历 ─────────────────────────
def search_ticker(q: str, limit: int = 10) -> list:
    data = _get("/api/meta/tickers/search", {"q": q, "limit": limit})
    return data.get("item") or [] if data else []


def trading_days() -> list:
    data = _get("/api/a-share/calendar/trading-days")
    return data.get("item") or [] if data else []


# ───────────────────────── 个股异动原因（事件催化增强） ─────────────────────────
def anomaly_list(tag_codes: str | None = None) -> list:
    """当日个股异动原因列表；tag_codes=逗号分隔(LIMIT_UP/SHARP_RISE/RAPID_RALLY/...)，空=全部。"""
    params = {"tag_codes": tag_codes} if tag_codes else None
    return _special("/api/a-share/special-data/anomaly-analysis-list", params)


def anomaly_stock(thscodes) -> list:
    """按 thscode 批量查当日异动原因（≤50）。thscodes 可 6位/ths/list。返回含 keyword_list 催化剂标签。"""
    if isinstance(thscodes, (list, tuple, set)):
        ts = [to_thscode(c) for c in thscodes]
    else:
        ts = [to_thscode(thscodes)]
    ts = [t for t in ts if t]
    if not ts:
        return []
    return _special("/api/a-share/special-data/anomaly-analysis-stock", {"thscodes": ",".join(ts)})


# ───────────────────────── 指数历史K + 快照（板块动量源） ─────────────────────────
# ⚠️ 服务端硬限制（2026-09-14 实测）：指数历史 K 端点**只覆盖近期一段历史**（约自 2022-01 起 /
# 近 ~1126 根），且**单次请求的起点若早于该可用窗口，整段直接静默返回空**（code=0 + item=[]，
# 不报错、不截断）。实测：2023-05-31~今(799根) ✅ / 2020-01-01~2022-01-20(仅486根) ❌空 /
# 单次 2020-01-01~今 ❌空 —— 可见瓶颈是**起点日期**而非根数，故不能靠"缩短区间"规避，
# 必须分片：早于窗口的片返回空、窗口内的片正常，拼接后即得完整可用序列。
# 注意：**个股**历史 K 端点（/api/a-share/prices/historical）的限制**与指数不同但同样存在**
# —— 单次跨度 > ~3652 自然日即静默返空（见 fetch_historical_k 上方的实测记录与事故背景）。
# 本行此前写「个股无此限制」是错的，已于 2026-09-17 更正；个股侧现由
# _HK_HIST_CHUNK_DAYS=1500 分片，指数侧用下面的 750。
_INDEX_HIST_CHUNK_DAYS = 750


def _as_date(d) -> date:
    """date / datetime / 'YYYY-MM-DD' → date。"""
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    return datetime.strptime(str(d)[:10], "%Y-%m-%d").date()


def fetch_index_historical(thscode, start: date, end: date, interval: str = "1d") -> pd.DataFrame:
    """板块/行业/标准指数历史日 K。thscode 为指数 thscode（如 886042.TI / 000001.SH / 881101.TI）。

    内部按 _INDEX_HIST_CHUNK_DAYS 分片请求（绕开服务端单次根数上限）后拼接、按日去重。
    """
    ts = str(thscode).strip().upper()
    if not ts:
        return pd.DataFrame()
    try:
        s_d, e_d = _as_date(start), _as_date(end)
    except Exception:
        return pd.DataFrame()
    if s_d > e_d:
        return pd.DataFrame()
    rows: list = []
    n_chunks = 0
    failed_chunks = 0
    cur = s_d
    while cur <= e_d:
        n_chunks += 1
        chunk_end = min(cur + timedelta(days=_INDEX_HIST_CHUNK_DAYS), e_d)
        data = _get(
            "/api/a-share-index/prices/historical",
            {"thscode": ts, "interval": interval, "start": _ms(cur), "end": _ms(chunk_end)},
        )
        items = (data or {}).get("item") or []
        if not items:
            # 首个分片为空＝请求起点早于服务端可用历史（实测指数历史约只覆盖近 ~1126 根），属常态；
            # 其余分片为空＝接口抖动/限流（实测 000905 曾只取到前 3 片、末值停留在 6 个月前）。
            if n_chunks > 1:
                failed_chunks += 1
                import sys as _sys
                print(
                    f"[hithink] WARN: 指数 {ts} {cur}~{chunk_end} 返回空 item（接口抖动/限流）",
                    file=_sys.stderr,
                )
        rows.extend(
            {
                "date": _ms_to_date(it.get("date_ms")),
                "open": _num(it.get("open_price")),
                "high": _num(it.get("high_price")),
                "low": _num(it.get("low_price")),
                "close": _num(it.get("close_price")),
                "volume": _num(it.get("volume")),
                "amount": _num(it.get("turnover")),
            }
            for it in items
            if it.get("date_ms")
        )
        cur = chunk_end + timedelta(days=1)
    # 全有或全无：任一分片缺失即整体判失败，返回空让调用方降级到新浪/baostock。
    # 截断的指数序列比"取不到"更危险——它会被当成最新数据静默用于 regime / 相对强度基准。
    if failed_chunks:
        import sys as _sys
        print(
            f"[hithink] WARN: 指数 {ts} {s_d}~{e_d} 有 {failed_chunks}/{n_chunks} 分片缺失，"
            f"按整体失败处理（调用方应降级）",
            file=_sys.stderr,
        )
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])
    if not rows:
        import sys as _sys
        print(
            f"[hithink] WARN: 指数 {ts} {s_d}~{e_d} 全部分片返回空（接口不可达或该区间无数据）",
            file=_sys.stderr,
        )
    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume", "amount"])
    if df.empty:
        return df
    df = df[df["date"].astype(bool)]
    return df.drop_duplicates(subset="date", keep="last").sort_values("date").reset_index(drop=True)


def fetch_index_snapshot(thscodes) -> pd.DataFrame:
    """指数最新行情快照；thscodes 可 6位/ths/list。"""
    if isinstance(thscodes, (list, tuple, set)):
        ts = [str(t).strip().upper() for t in thscodes]
    else:
        ts = [str(thscodes).strip().upper()]
    ts = [t for t in ts if t]
    if not ts:
        return pd.DataFrame()
    data = _get("/api/a-share-index/prices/snapshot", {"thscodes": ",".join(ts)})
    if not data:
        return pd.DataFrame()
    return pd.DataFrame(data.get("item") or [])


# ───────────────────────── 复权因子事件流（qfq 守卫加固） ─────────────────────────
def fetch_adjustment_factors(thscode, start: str | None = None, end: str | None = None) -> list:
    """复权因子事件流（现金分红 + 送股）。thscode 单只；start/end 为 YYYY-MM-DD（可选）。"""
    ts = to_thscode(thscode)
    if not ts:
        return []
    params: dict = {"thscode": ts}
    if start:
        params["from"] = start
    if end:
        params["end"] = end
    data = _get("/api/a-share/corporate-actions/adjustment-factors", params)
    return data.get("item") or [] if data else []


def derive_qfq_factor(events: list, as_of: date) -> float | None:
    """由复权因子事件流推导截至 as_of 的前复权累计调整系数（价格比，不含 as_of 当日除权）。

    前复权口径：历史价格 = 当日价格 / 累计因子。因子 = Π(1 + 每股分红/前收盘 + 每股送股)。
    这里以「除权日 < as_of」的事件累乘近似（忽略精确前收，用于守卫交叉校验的容差比较）。
    无事件返回 None（不约束）。
    """
    if not events:
        return None
    cum = 1.0
    any_event = False
    for ev in events:
        ex_ms = ev.get("ex_date_ms")
        if not ex_ms:
            continue
        ex_d = _ms_to_date(ex_ms)
        if not ex_d:
            continue
        # 仅计入 as_of 之前的除权事件
        try:
            if _to_date(ex_d) >= as_of:
                continue
        except Exception:
            continue
        div = _num(ev.get("dividend_per_share")) or 0.0
        bonus = _num(ev.get("per_share_bonus")) or 0.0
        # 简化：以(分红+送股*近似价)无法得精确前收，这里用(1+送股比例)代表股本扩张，
        # 分红影响在本次近似中并入（守卫仅做容差级别交叉校验）。
        factor = 1.0 + bonus
        if factor <= 0:
            continue
        cum *= factor
        any_event = True
    return cum if any_event else None
