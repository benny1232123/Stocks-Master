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


# ── 指数行情 ──

_INDEX_PREFIX = {
    "000": "sh",  # 上证系列指数（000001 上证指数、000300 沪深300、000688 科创50、000016 上证50）
    "880": "sh",  # 上证系列
}


def _to_sina_index_symbol(code: str) -> str:
    """6 位指数代码转新浪格式（sh/sz 前缀）。

    规则：
    - 000/880 开头 → sh（上证指数系列）
    - 399 开头 → sz（深证指数系列）
    - 已带 sh/sz 前缀的直接用
    """
    text = str(code).strip().lower().replace(".", "")
    if text.startswith(("sh", "sz")):
        return text
    digits = "".join(c for c in text if c.isdigit())
    if len(digits) != 6:
        return ""
    prefix = _INDEX_PREFIX.get(digits[:3])
    if prefix:
        return prefix + digits
    if digits.startswith("399"):
        return "sz" + digits
    if digits.startswith(("5", "6", "9")):
        return "sh" + digits
    return "sz" + digits


def fetch_sina_index_quotes(codes: Iterable[str]) -> dict:
    """批量获取指数实时行情。

    Args:
        codes: 指数代码列表（000001/399001/sh000001 等格式均可）

    Returns:
        {code6: {"name", "price", "open", "pre_close", "high", "low"}}
    """
    symbols = []
    code_map = {}
    for c in codes:
        sym = _to_sina_index_symbol(c)
        if sym:
            symbols.append(sym)
            code_map[sym] = "".join(ch for ch in str(c) if ch.isdigit()).zfill(6)

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
        logger.warning("新浪指数行情请求失败: %s", e)
        return {}

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


def fetch_sina_index_price(code: str) -> Optional[float]:
    """获取单个指数最新价。"""
    quotes = fetch_sina_index_quotes([code])
    info = quotes.get("".join(ch for ch in str(code) if ch.isdigit()).zfill(6))
    return info["price"] if info else None


def fetch_sina_market_breadth() -> Optional[dict]:
    """从上证综指(sh000001)扩展字段获取涨跌家数。

    新浪指数接口对上证综指返回的额外字段：
    field[6] = 上涨家数
    field[7] = 平盘家数
    field[8] = 下跌家数
    单次 HTTP 请求，秒级返回，不依赖全市场快照。
    """
    try:
        resp = requests.get(
            _SINA_URL + "sh000001",
            headers=_SINA_HEADERS,
            timeout=10,
        )
        resp.encoding = "gbk"
        m = re.search(r'var hq_str_sh000001="([^"]*)"', resp.text)
        if not m:
            return None
        fields = m.group(1).split(",")
        if len(fields) < 9:
            return None
        up = int(float(fields[6]))
        flat = int(float(fields[7]))
        down = int(float(fields[8]))
        total = up + flat + down
        if total == 0:
            return None
        return {
            "上涨": up,
            "下跌": down,
            "平盘": flat,
            "总数": total,
            "上涨比例": round(up / total * 100, 1),
        }
    except Exception as e:
        logger.warning("新浪涨跌家数获取失败: %s", e)
        return None
