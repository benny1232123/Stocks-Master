"""ETF 日线抓取（腾讯 ifzq，前复权）。

为什么单独一个模块
------------------
本地 `smcore.data.kline.fetch_daily_k` 的 baostock / tdx / hithink 后端**均不覆盖 ETF**
（实测 `fetch_daily_k("510300")` 返回空）。ETF 前复权日线改走**腾讯行情**接口：
  https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=<sym>,day,<start>,<end>,<count>,qfq
返回 qfqday 行 [date, open, close, high, low, volume]；本模块只取 date/close（配对交易用）。

落盘为宽表 `stock_data/etf_data/etf_qfq_close.parquet`（index=date, columns=code），供 arb_pairs 读取。
fail-soft：单只失败跳过，不抛。
"""
from __future__ import annotations

import json
import time
import urllib.request
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from smcore.config.defaults import STOCK_DATA_DIR

ETF_CACHE_DIR = Path(STOCK_DATA_DIR) / "etf_data"
ETF_CLOSE_PARQUET = ETF_CACHE_DIR / "etf_qfq_close.parquet"

# 精选流动 ETF 池（宽基 + 行业/主题 + 跨境 + 商品/债）——用户「只玩主板+基金+ETF」。
# 无效/退市代码会在抓取时 fail-soft 跳过，实际以取到的为准。
ETF_UNIVERSE: dict[str, str] = {
    # 宽基
    "510300": "沪深300ETF", "510500": "中证500ETF", "510050": "上证50ETF",
    "159915": "创业板ETF", "588000": "科创50ETF", "512100": "中证1000ETF",
    "159901": "深100ETF", "510880": "红利ETF", "512090": "MSCI中国A股ETF",
    "510310": "沪深300ETF易方达", "159919": "沪深300ETF嘉实", "510330": "沪深300ETF华夏",
    "510180": "上证180ETF", "159949": "创业板50ETF", "159845": "中证1000ETF广发",
    "512510": "中证500ETF华泰", "159922": "中证500ETF嘉实", "515180": "红利ETF易方达",
    "515080": "中证红利ETF", "588080": "科创板50ETF", "159781": "科创创业50ETF",
    # 金融/地产
    "512880": "证券ETF", "512800": "银行ETF", "512000": "券商ETF", "512900": "证券ETF南方",
    "512070": "证券保险ETF", "512200": "房地产ETF", "159940": "金融地产ETF",
    # 消费
    "512690": "酒ETF", "159928": "消费ETF", "512010": "医药ETF", "512170": "医疗ETF",
    "159929": "医药ETF", "159839": "医药ETF国泰", "159865": "养殖ETF", "159766": "旅游ETF",
    "159825": "农业ETF", "515170": "食品饮料ETF", "159843": "食品饮料ETF",
    # 科技/成长
    "515030": "新能源车ETF", "512480": "半导体ETF", "515000": "科技ETF", "512720": "计算机ETF",
    "159995": "芯片ETF", "512760": "芯片ETF国泰", "515050": "5GETF", "159994": "5GETF银华",
    "515230": "软件ETF", "159852": "软件ETF招商", "512930": "人工智能ETF", "159819": "人工智能ETF",
    "159869": "游戏ETF", "516010": "游戏ETF国泰", "512980": "传媒ETF", "515880": "通信ETF",
    "512660": "军工ETF", "512710": "军工龙头ETF", "512670": "国防ETF",
    "515790": "光伏ETF", "516160": "新能源ETF", "515700": "新能车ETF", "159755": "电池ETF",
    # 周期/其他行业
    "512400": "有色金属ETF", "515220": "煤炭ETF", "515210": "钢铁ETF", "516110": "汽车ETF",
    "512580": "环保ETF", "516970": "基建ETF", "159611": "电力ETF",
    # 跨境
    "513100": "纳指ETF", "513500": "标普500ETF", "513050": "中概互联ETF",
    "159920": "恒生ETF", "513180": "恒生科技ETF", "513330": "恒生互联网ETF",
    "513060": "恒生医疗ETF", "159941": "纳指ETF广发", "513300": "纳斯达克ETF",
    "513520": "日经ETF", "513030": "德国ETF", "513080": "法国ETF",
    # 商品
    "518880": "黄金ETF", "159934": "黄金ETF易方达", "159937": "黄金ETF博时",
    "159981": "能源化工ETF", "159985": "豆粕ETF",
    # 债 / 可转债
    "511010": "国债ETF", "511260": "十年国债ETF", "511380": "可转债ETF", "511180": "上证可转债ETF",
}

_TENCENT = ("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
            "?param={sym},day,{start},{end},{count},qfq")


def _tencent_symbol(code: str) -> str:
    """ETF 代码 → 腾讯符号（5/6/9 开头 sh，其余 sz；ETF 为 51/56/58→sh，15/16→sz）。"""
    c = str(code).strip().split(".")[0].lstrip("sh").lstrip("sz")
    c = str(code).strip()[-6:]
    return ("sh" if c.startswith(("5", "6", "9")) else "sz") + c


def fetch_etf_daily(code: str, start: str = "2024-01-01", end: str = "2030-01-01",
                    count: int = 900) -> Optional[pd.DataFrame]:
    """取单只 ETF 前复权日线，返回 DataFrame[date, close]（date 为 YYYY-MM-DD 字符串）。失败返回 None。"""
    sym = _tencent_symbol(code)
    url = _TENCENT.format(sym=sym, start=start, end=end, count=count)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            d = json.loads(r.read().decode("utf-8", "replace"))
        node = (d.get("data") or {}).get(sym) or {}
        rows = node.get("qfqday") or node.get("day") or []
        if not rows:
            return None
        recs = [{"date": r[0], "close": float(r[2])} for r in rows if len(r) >= 3 and r[2]]
        df = pd.DataFrame(recs).dropna().drop_duplicates("date")
        return df if len(df) else None
    except Exception:
        return None


def fetch_etf_closes(codes: Optional[Iterable[str]] = None, start: str = "2024-01-01",
                     sleep: float = 0.25, save: bool = True) -> pd.DataFrame:
    """批量取 ETF close 宽表（index=date, columns=code）。单只失败跳过。"""
    codes = list(codes) if codes is not None else list(ETF_UNIVERSE.keys())
    series: dict[str, pd.Series] = {}
    for i, c in enumerate(codes):
        df = fetch_etf_daily(c, start=start)
        if df is not None and len(df) > 60:
            s = df.copy()
            s["date"] = pd.to_datetime(s["date"])
            series[str(c)] = s.set_index("date")["close"]
        time.sleep(sleep)
    if not series:
        return pd.DataFrame()
    wide = pd.DataFrame(series).sort_index()
    wide = wide.ffill()
    if save:
        ETF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        wide.to_parquet(ETF_CLOSE_PARQUET)
    return wide


def load_etf_closes(codes: Optional[Iterable[str]] = None,
                    refresh: bool = False) -> pd.DataFrame:
    """读取 ETF close 宽表；无缓存或 refresh 时联网抓取。"""
    if not refresh and ETF_CLOSE_PARQUET.exists():
        try:
            wide = pd.read_parquet(ETF_CLOSE_PARQUET, use_threads=False)
            if codes is not None:
                keep = [c for c in codes if c in wide.columns]
                wide = wide[keep]
            return wide
        except Exception:
            pass
    return fetch_etf_closes(codes)
