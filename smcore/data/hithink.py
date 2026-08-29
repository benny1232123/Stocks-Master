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

_BASE = os.getenv("HITHINK_BASE", "https://fuyao.aicubes.cn").rstrip("/")
_API_KEY = (os.getenv("HITHINK_FINANCE_API_KEY") or "").strip()
_TIMEOUT = float(os.getenv("HITHINK_TIMEOUT", "20"))
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
    return bool(_API_KEY)


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
    if not _API_KEY:
        return None
    url = _BASE + path
    headers = {"X-api-key": _API_KEY}
    for _ in range(retries + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=_TIMEOUT)
            body = r.json()
            if body.get("code") != 0:
                return None
            return body.get("data")
        except Exception:
            time.sleep(1)
    return None


# ───────────────────────── K 线 ─────────────────────────
def fetch_historical_k(code, start: date, end: date, adjust: str = "qfq") -> pd.DataFrame:
    """历史日 K，返回 kline.py 规范列: date,open,high,low,close,volume,amount。

    adjust: qfq(前复权,默认)/hfq/bfq。
    """
    ts = to_thscode(code)
    if not ts:
        return pd.DataFrame()
    adj = _ADJ_MAP.get(str(adjust).lower(), "forward")
    data = _get(
        "/api/a-share/prices/historical",
        {"thscode": ts, "interval": "1d", "start": _ms(start), "end": _ms(end), "adjust": adj},
    )
    if not data:
        return pd.DataFrame()
    items = data.get("item") or []
    rows = [
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
    ]
    return pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume", "amount"])


# ───────────────────────── 行情快照 ─────────────────────────
def fetch_snapshot(codes) -> pd.DataFrame:
    """最新行情快照；codes 可 6位/ths；返回 thscode,last_price,... 的 DataFrame。"""
    if not _API_KEY:
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


def fetch_valuation(code) -> dict | None:
    """估值快照（PE/PB/PS/PC）。

    注意: 已抓取契约(llms-full.txt)未暴露估值端点，暂返回 None。
    待持有 Key 后实测补充；现有估值仍走腾讯 qt.gtimg.cn（fundamental.py）。
    """
    return None


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
