"""外部市场数据获取与风险评估 —— 美股 / 汇率 / 期货。

此前内嵌在 auto_notify_boll.py(3306行巨石)，无法被可视化主线复用。
本模块提供纯数据获取 + 评估函数，依赖仅 akshare，无内部状态。
"""
from __future__ import annotations

import math

import akshare as ak


def safe_float(val, default=None):
    """安全转 float，过滤 None / NaN / 非数值。"""
    try:
        v = float(val)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except Exception:
        return default


# ── 数据获取 ──

def fetch_us_market_data():
    """获取美股三大指数最近行情，返回 dict with keys: sp500, nasdaq, dow."""
    indices = {
        "sp500": ".INX",
        "nasdaq": ".IXIC",
        "dow": ".DJI",
    }
    result = {}
    for key, symbol in indices.items():
        try:
            df = ak.index_us_stock_sina(symbol=symbol)
            if df is None or df.empty:
                continue
            df = df.tail(25)
            last = df.iloc[-1]
            prev = df.iloc[-2] if len(df) >= 2 else last
            close = safe_float(last.get("close"))
            prev_close = safe_float(prev.get("close"))
            ret_1d = ((close - prev_close) / prev_close * 100) if (close and prev_close) else None
            close_5d = safe_float(df.iloc[-6].get("close")) if len(df) >= 6 else None
            close_20d = safe_float(df.iloc[-21].get("close")) if len(df) >= 21 else None
            ret_5d = ((close - close_5d) / close_5d * 100) if (close and close_5d) else None
            ret_20d = ((close - close_20d) / close_20d * 100) if (close and close_20d) else None
            result[key] = {
                "close": close,
                "ret_1d": ret_1d,
                "ret_5d": ret_5d,
                "ret_20d": ret_20d,
            }
        except Exception:
            continue
    return result


def fetch_fx_data():
    """获取关键汇率数据，返回 dict with keys: usdcny, eurusd etc."""
    try:
        df = ak.fx_spot_quote()
        if df is None or df.empty:
            return {}
    except Exception:
        return {}

    result = {}
    all_nan = True
    for _, row in df.iterrows():
        pair = str(row.iloc[0]).strip()
        buy = safe_float(row.iloc[1]) if len(row) > 1 else None
        sell = safe_float(row.iloc[2]) if len(row) > 2 else None
        if buy is not None and not (isinstance(buy, float) and math.isnan(buy)):
            all_nan = False
        mid = None
        if buy and sell and not (math.isnan(buy) if isinstance(buy, float) else False) and not (math.isnan(sell) if isinstance(sell, float) else False):
            mid = (buy + sell) / 2
        elif buy and not (isinstance(buy, float) and math.isnan(buy)):
            mid = buy
        elif sell and not (isinstance(sell, float) and math.isnan(sell)):
            mid = sell
        if pair == "USD/CNY":
            result["usdcny"] = mid
        elif pair == "EUR/CNY":
            result["eurcny"] = mid
        elif pair == "100JPY/CNY":
            result["jpycny"] = mid
        elif pair == "GBP/CNY":
            result["gbpcny"] = mid
        elif pair == "CNY/KRW":
            result["cnykrw"] = mid
    if all_nan:
        return {}
    return result


def fetch_futures_data():
    """获取关键期货品种最近行情，返回 dict with keys: crude_oil, gold, copper."""
    futures_map = {
        "crude_oil": "CL",
        "gold": "GC",
        "copper": "HG",
    }
    result = {}
    for key, symbol in futures_map.items():
        try:
            df = ak.futures_foreign_hist(symbol=symbol)
            if df is None or df.empty:
                continue
            df = df.tail(25)
            last = df.iloc[-1]
            prev = df.iloc[-2] if len(df) >= 2 else last
            close = safe_float(last.get("close"))
            prev_close = safe_float(prev.get("close"))
            ret_1d = ((close - prev_close) / prev_close * 100) if (close and prev_close) else None
            close_5d = safe_float(df.iloc[-6].get("close")) if len(df) >= 6 else None
            close_20d = safe_float(df.iloc[-21].get("close")) if len(df) >= 21 else None
            ret_5d = ((close - close_5d) / close_5d * 100) if (close and close_5d) else None
            ret_20d = ((close - close_20d) / close_20d * 100) if (close and close_20d) else None
            result[key] = {
                "close": close,
                "ret_1d": ret_1d,
                "ret_5d": ret_5d,
                "ret_20d": ret_20d,
            }
        except Exception:
            continue
    return result


# ── 风险评估 ──

def assess_us_market_risk(us_data):
    """根据美股表现评估风险等级，返回 (level, reason)。"""
    if not us_data:
        return "low", ""
    risks = []
    for name, info in us_data.items():
        label = {"sp500": "标普500", "nasdaq": "纳斯达克", "dow": "道琼斯"}.get(name, name)
        ret_1d = info.get("ret_1d")
        ret_5d = info.get("ret_5d")
        ret_20d = info.get("ret_20d")
        if ret_1d is not None and ret_1d <= -3:
            risks.append(f"{label}单日跌{ret_1d:.1f}%")
        elif ret_5d is not None and ret_5d <= -5:
            risks.append(f"{label}5日跌{ret_5d:.1f}%")
        elif ret_20d is not None and ret_20d <= -10:
            risks.append(f"{label}20日跌{ret_20d:.1f}%")
    high_count = sum(1 for r in risks if "跌" in r and (("-5%" in r) or ("-10%" in r) or ("-3%" in r)))
    if high_count >= 2:
        return "high", "; ".join(risks)
    if high_count == 1:
        return "medium", "; ".join(risks)
    return "low", ""


def assess_fx_risk(fx_data):
    """根据汇率变动评估风险，返回 (level, reason)。"""
    if not fx_data:
        return "low", ""
    usdcny = fx_data.get("usdcny")
    if usdcny is None or (isinstance(usdcny, float) and math.isnan(usdcny)):
        return "low", ""
    # 人民币大幅贬值为风险信号
    if usdcny >= 7.3:
        return "high", f"美元/人民币={usdcny:.4f}，人民币显著贬值"
    if usdcny >= 7.1:
        return "medium", f"美元/人民币={usdcny:.4f}，人民币偏弱"
    return "low", ""


def assess_futures_risk(futures_data):
    """根据期货表现评估风险，返回 (level, reason)。"""
    if not futures_data:
        return "low", ""
    risks = []
    crude = futures_data.get("crude_oil")
    gold = futures_data.get("gold")
    if crude and crude.get("ret_5d") is not None and crude["ret_5d"] <= -10:
        risks.append(f"原油5日跌{crude['ret_5d']:.1f}%")
    if gold and gold.get("ret_5d") is not None and gold["ret_5d"] >= 5:
        risks.append(f"黄金5日涨{gold['ret_5d']:.1f}%（避险情绪升温）")
    if crude and crude.get("ret_1d") is not None and crude["ret_1d"] <= -5:
        risks.append(f"原油单日跌{crude['ret_1d']:.1f}%")
    if gold and gold.get("ret_1d") is not None and gold["ret_1d"] >= 3:
        risks.append(f"黄金单日涨{gold['ret_1d']:.1f}%")
    high_count = len([r for r in risks if "跌" in r or "涨" in r])
    if high_count >= 2:
        return "high", "; ".join(risks)
    if high_count == 1:
        return "medium", "; ".join(risks)
    return "low", ""
