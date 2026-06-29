"""新浪财经实时行情 —— 轻量 HTTP 接口，无需 akshare。

专为 SCF（云函数）设计：只依赖 requests（SCF 预装），不拉全市场快照，
按需查询指定股票，单次请求秒级返回。

接口：http://hq.sinajs.cn/list=sh600519,sz000001
返回：var hq_str_sh600519="名称,今开,昨收,最新价,最高,最低,...";

注意：需带 Referer: https://finance.sina.com.cn 头，否则被拒。
"""
from __future__ import annotations

import logging
import re
from typing import Iterable, Optional

import requests

logger = logging.getLogger("smcore.data.quote_sina")

_SINA_URL = "http://hq.sinajs.cn/list="
_SINA_HEADERS = {"Referer": "https://finance.sina.com.cn"}


def _to_sina_symbol(code: str) -> str:
    """6 位代码转新浪格式（sh/sz 前缀）。"""
    code = str(code).strip()
    digits = "".join(c for c in code if c.isdigit())
    if len(digits) != 6:
        return ""
    return ("sh" if digits.startswith(("5", "6", "9")) else "sz") + digits


def fetch_sina_quotes(codes: Iterable[str]) -> dict:
    """批量获取实时报价。

    Returns:
        {code: {"name", "price", "open", "pre_close", "high", "low"}}
    """
    symbols = []
    code_map = {}
    for c in codes:
        sym = _to_sina_symbol(c)
        if sym:
            symbols.append(sym)
            code_map[sym] = "".join(ch for ch in c if ch.isdigit()).zfill(6)

    if not symbols:
        return {}

    result = {}
    try:
        resp = requests.get(
            _SINA_URL + ",".join(symbols),
            headers=_SINA_HEADERS,
            timeout=8,
        )
        resp.encoding = "gbk"
        text = resp.text
    except Exception as e:
        logger.warning("新浪行情请求失败: %s", e)
        return {}

    # 解析 var hq_str_sh600519="贵州茅台,1194.96,...";
    for line in text.strip().split("\n"):
        m = re.match(r'var hq_str_(\w+)="([^"]*)"', line.strip())
        if not m:
            continue
        symbol = m.group(1)
        fields = m.group(2).split(",")
        if len(fields) < 6:
            continue
        code = code_map.get(symbol, "")
        if not code:
            continue
        try:
            result[code] = {
                "name": fields[0],
                "open": float(fields[1]) if fields[1] else None,
                "pre_close": float(fields[2]) if fields[2] else None,
                "price": float(fields[3]) if fields[3] else None,
                "high": float(fields[4]) if fields[4] else None,
                "low": float(fields[5]) if fields[5] else None,
            }
        except (ValueError, IndexError):
            continue

    return result


def fetch_sina_price(code: str) -> Optional[float]:
    """获取单只股票实时价格。"""
    quotes = fetch_sina_quotes([code])
    info = quotes.get("".join(ch for ch in code if ch.isdigit()).zfill(6))
    return info["price"] if info else None
