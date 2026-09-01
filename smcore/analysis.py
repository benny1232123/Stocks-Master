"""Shared helpers for single-stock technical analysis."""
from __future__ import annotations

import threading
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd

from smcore.config.defaults import RECOMMENDATION_CONFIG
from smcore.data.kline import fetch_daily_k
from smcore.indicators.boll import calc_bollinger, evaluate_boll_signal


def calc_ma(close: pd.Series, periods: list[int] | None = None) -> pd.DataFrame:
    """Calculate moving averages for a close-price series."""
    periods = periods or [5, 10, 20, 60]
    frame = pd.DataFrame(index=close.index)
    for period in periods:
        frame[f"MA{period}"] = close.rolling(window=period).mean()
    return frame


def calc_macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """Calculate MACD indicators."""
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    hist = 2 * (dif - dea)
    return pd.DataFrame({"DIF": dif, "DEA": dea, "MACD": hist}, index=close.index)


def calc_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Calculate RSI."""
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(span=period, adjust=False).mean()
    avg_loss = loss.ewm(span=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def calc_kdj(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 9, m1: int = 3, m2: int = 3) -> pd.DataFrame:
    """Calculate KDJ."""
    lowest = low.rolling(window=n).min()
    highest = high.rolling(window=n).max()
    rsv = ((close - lowest) / (highest - lowest).replace(0, np.nan)) * 100
    k = rsv.ewm(span=m1, adjust=False).mean()
    d = k.ewm(span=m2, adjust=False).mean()
    j = 3 * k - 2 * d
    return pd.DataFrame({"K": k, "D": d, "J": j}, index=close.index)


def build_stock_analysis(
    code: str,
    window: int = 20,
    k: float = 1.645,
    days_back: int = 180,
    as_of: date | None = None,
    with_fundamentals: bool = True,
) -> dict[str, Any]:
    """Build a JSON-friendly technical analysis snapshot for a stock.

    as_of: 指定截至日期（默认今天），用于历史回看；历史日期仅取缓存 K 线，不联网。
    with_fundamentals: 是否补取基本面（历史回看建议关掉以提速）。
    """
    end_date = as_of or date.today()
    start_date = end_date - timedelta(days=days_back)
    kdf = fetch_daily_k(code, start_date, end_date)
    if kdf.empty:
        return {"code": code, "error": "未获取到K线数据"}

    kdf = calc_bollinger(kdf, window=window, k=k)
    signal_info = evaluate_boll_signal(kdf, near_ratio=1.015)

    plot_df = kdf.copy()
    for column in ["close", "open", "high", "low", "volume", "MA", "Upper", "Lower"]:
        if column in plot_df.columns:
            plot_df[column] = pd.to_numeric(plot_df[column], errors="coerce")
    plot_df["date"] = pd.to_datetime(plot_df["date"], errors="coerce")
    plot_df = plot_df.dropna(subset=["close"])

    ma_df = calc_ma(plot_df["close"])
    macd_df = calc_macd(plot_df["close"])
    rsi_series = calc_rsi(plot_df["close"])
    kdj_df = calc_kdj(plot_df["high"], plot_df["low"], plot_df["close"])

    latest = plot_df.iloc[-1]
    last_n = min(120, len(plot_df))

    payload: dict[str, Any] = {
        "code": code,
        "window": window,
        "k": k,
        "days_back": days_back,
        "signal": signal_info,
        "latest": {
            "date": latest["date"].strftime("%Y-%m-%d") if pd.notna(latest["date"]) else None,
            "close": float(latest["close"]),
            "lower": float(latest["Lower"]) if pd.notna(latest.get("Lower")) else None,
            "upper": float(latest["Upper"]) if pd.notna(latest.get("Upper")) else None,
            "middle": float(latest["MA"]) if pd.notna(latest.get("MA")) else None,
            "rsi": float(rsi_series.iloc[-1]) if not pd.isna(rsi_series.iloc[-1]) else None,
            # MACD
            "dif": float(macd_df["DIF"].iloc[-1]) if not pd.isna(macd_df["DIF"].iloc[-1]) else None,
            "dea": float(macd_df["DEA"].iloc[-1]) if not pd.isna(macd_df["DEA"].iloc[-1]) else None,
            "macd_hist": float(macd_df["MACD"].iloc[-1]) if not pd.isna(macd_df["MACD"].iloc[-1]) else None,
            # KDJ
            "k_val": float(kdj_df["K"].iloc[-1]) if not pd.isna(kdj_df["K"].iloc[-1]) else None,
            "d_val": float(kdj_df["D"].iloc[-1]) if not pd.isna(kdj_df["D"].iloc[-1]) else None,
            "j_val": float(kdj_df["J"].iloc[-1]) if not pd.isna(kdj_df["J"].iloc[-1]) else None,
            # MA
            "ma5": float(ma_df["MA5"].iloc[-1]) if not pd.isna(ma_df["MA5"].iloc[-1]) else None,
            "ma10": float(ma_df["MA10"].iloc[-1]) if not pd.isna(ma_df["MA10"].iloc[-1]) else None,
            "ma20": float(ma_df["MA20"].iloc[-1]) if not pd.isna(ma_df["MA20"].iloc[-1]) else None,
            "ma60": float(ma_df["MA60"].iloc[-1]) if not pd.isna(ma_df["MA60"].iloc[-1]) else None,
        },
        "metrics": {
            "latest_close": float(latest["close"]),
            "dist_to_lower_pct": signal_info.get("dist_to_lower_pct"),
            "dist_to_upper_pct": signal_info.get("dist_to_upper_pct"),
            "signal_text": signal_info.get("signal"),
            "bandwidth": signal_info.get("bandwidth"),
        },
        "series": {
            "rows": plot_df.tail(last_n).assign(
                MA5=ma_df.get("MA5"),
                MA10=ma_df.get("MA10"),
                MA20=ma_df.get("MA20"),
                MA60=ma_df.get("MA60"),
                DIF=macd_df.get("DIF"),
                DEA=macd_df.get("DEA"),
                MACD=macd_df.get("MACD"),
                RSI=rsi_series,
                K=kdj_df.get("K"),
                D=kdj_df.get("D"),
                J=kdj_df.get("J"),
            ).replace({pd.NA: None, np.nan: None}).to_dict(orient="records")
        },
        # ── 基本面 / 资金面（缓存优先，未命中实时补取并写回缓存）──
        "fundamentals": _build_fundamentals(code) if with_fundamentals else None,
    }
    return payload


def recommendation_from_analysis(
    analysis: dict, cfg: dict | None = None
) -> dict[str, object]:
    """由「技术面 + 基本面 + 资金面」三维度综合给出持仓建议。

    技术面：均线趋势(MA20/MA60)、MACD 柱、布林带位置/买卖信号、RSI、KDJ(J)。
    基本面：估值(PE/PB 绝对阈值)、质量(ROE/毛利率)。
    资金面：换手率活跃度。

    每个维度先算「面归一分」= 面净分 / Σ|该面权重| ∈ [-1, +1]，
    再按 face_weights 加权得综合分 ∈ [-1, +1]，按 action_thresholds 分五档：
        加仓 / 持有偏多 / 持有观望 / 减仓偏空 / 减仓
    数据缺失的维度自动从加权中剔除，避免无数据面把综合分稀释向 0。
    返回 {action, score, reason, drivers, faces}。
    权重与阈值全部来自 smcore.config.defaults.RECOMMENDATION_CONFIG（可传参覆盖）。
    """
    def _num(v: object) -> float | None:
        try:
            return None if v is None else float(v)
        except (TypeError, ValueError):
            return None

    def _thr(d: dict, key: str, default: float) -> float:
        v = _num(d.get(key))
        return default if v is None else v

    cfg = cfg or RECOMMENDATION_CONFIG
    if analysis.get("error"):
        return {
            "action": "未知",
            "score": 0.0,
            "reason": "分析失败",
            "drivers": [],
            "faces": {},
        }

    latest = analysis.get("latest", {}) or {}
    metrics = analysis.get("metrics", {}) or {}
    signal_info = analysis.get("signal", {}) or {}
    fund = analysis.get("fundamentals")
    if not isinstance(fund, dict) or fund.get("error"):
        fund = {}

    w_t = cfg.get("technical", {}) or {}
    w_f = cfg.get("fundamental", {}) or {}
    w_c = cfg.get("capital", {}) or {}
    face_weights = cfg.get("face_weights", {}) or {}

    close = latest.get("close")
    up = latest.get("upper")
    lo = latest.get("lower")
    ma20 = latest.get("ma20")
    ma60 = latest.get("ma60")
    rsi = latest.get("rsi")
    hist = latest.get("macd_hist")
    j = latest.get("j_val")
    sig = metrics.get("signal_text") or latest.get("signal_text") or ""
    sig_selected = bool(signal_info.get("selected"))

    faces: dict[str, list[tuple[float, str]]] = {
        "technical": [],
        "fundamental": [],
        "capital": [],
    }

    def _add(face: str, weight: float, label: str, direction: int = 1) -> None:
        """direction: +1 按 weight 原符号计入，-1 取反。"""
        faces[face].append((float(weight) * direction, label))

    # ── 技术面 ──
    if cfg.get("enable_technical", True):
        if close is not None and ma20 is not None and ma60 is not None:
            if close > ma20 > ma60:
                _add("technical", _thr(w_t, "ma_trend", 2.0), "多头排列(价>MA20>MA60)", 1)
            elif close < ma20 < ma60:
                _add("technical", _thr(w_t, "ma_trend", 2.0), "空头排列(价<MA20<MA60)", -1)
            else:
                _add("technical", 0.0, "均线缠绕震荡")
        if hist is not None:
            red = hist > 0
            _add(
                "technical",
                _thr(w_t, "macd_hist", 1.0),
                "MACD红柱" if red else "MACD绿柱",
                1 if red else -1,
            )
        if close is not None and up and lo and up > lo:
            if close >= up * 0.995:
                _add("technical", _thr(w_t, "boll_upper", 2.0), "触及/突破布林上轨(超买)", -1)
            elif close <= lo * 1.005:
                _add("technical", _thr(w_t, "boll_lower", 2.0), "触及/跌破布林下轨(超卖)", 1)
            elif "near_upper" in sig or "overbought" in sig:
                _add("technical", _thr(w_t, "boll_near_upper", 1.5), "高位接近上轨", -1)
            elif sig_selected or any(
                kw in sig for kw in ("near_lower", "mid_pullback", "squeeze", "oversold")
            ):
                _add("technical", _thr(w_t, "boll_buy", 1.5), "布林买点信号", 1)
        if rsi is not None:
            if rsi > 70:
                _add("technical", _thr(w_t, "rsi_overbought", 1.0), f"RSI超买({rsi:.0f})", -1)
            elif rsi < 30:
                _add("technical", _thr(w_t, "rsi_oversold", 1.0), f"RSI超卖({rsi:.0f})", 1)
        if j is not None:
            if j > 100:
                _add("technical", _thr(w_t, "kdj_overbought", 1.0), f"KDJ超买(J={j:.0f})", -1)
            elif j < 0:
                _add("technical", _thr(w_t, "kdj_oversold", 1.0), f"KDJ超卖(J={j:.0f})", 1)

    # ── 基本面：估值(PE/PB) + 质量(ROE/毛利率) ──
    th = cfg.get("thresholds", {}) or {}
    pe = _num(fund.get("pe"))
    pb = _num(fund.get("pb"))
    # baostock 的 roeAvg / gpMargin 是小数(0.156 → 15.6%)，统一 ×100 转百分数后再比对阈值，
    # 与报告展示层(roe*100)口径保持一致。turnover 本身已是百分数，不转换。
    _roe_raw = _num(fund.get("roe"))
    _gm_raw = _num(fund.get("gross_margin"))
    roe = _roe_raw * 100 if _roe_raw is not None else None
    gm = _gm_raw * 100 if _gm_raw is not None else None
    turnover = _num(fund.get("turnover"))

    if cfg.get("enable_fundamental", True) and fund:
        if pb is not None:
            if pb < 1:
                _add("fundamental", _thr(w_f, "pb_break", 1.5), f"破净·估值低(PB {pb:.2f})", 1)
            elif pb > _thr(th, "pb_high_cap", 8.0):
                _add("fundamental", _thr(w_f, "pb_high", 1.0), f"PB偏高({pb:.1f})", -1)
        if pe is not None:
            if pe < _thr(th, "pe_low_cap", 15.0):
                _add("fundamental", _thr(w_f, "pe_low", 1.0), f"PE偏低({pe:.0f})", 1)
            elif pe > _thr(th, "pe_high_cap", 60.0):
                _add("fundamental", _thr(w_f, "pe_high", 1.5), f"PE偏高({pe:.0f})", -1)
        if roe is not None:
            if roe >= _thr(th, "roe_good_floor", 12.0):
                _add("fundamental", _thr(w_f, "roe_good", 1.0), f"ROE优({roe:.0f}%)", 1)
            elif roe < _thr(th, "roe_weak_cap", 5.0):
                _add("fundamental", _thr(w_f, "roe_weak", 1.0), f"ROE弱({roe:.0f}%)", -1)
        if gm is not None:
            if gm >= _thr(th, "margin_good_floor", 30.0):
                _add("fundamental", _thr(w_f, "margin_good", 0.8), f"毛利高({gm:.0f}%)", 1)
            elif gm < _thr(th, "margin_weak_cap", 15.0):
                _add("fundamental", _thr(w_f, "margin_weak", 0.8), f"毛利低({gm:.0f}%)", -1)

    # ── 资金面：换手率活跃度 ──
    if cfg.get("enable_capital", True) and fund and turnover is not None:
        if turnover >= _thr(th, "turnover_active_floor", 3.0):
            _add("capital", _thr(w_c, "turnover_active", 0.8), f"交投活跃(换手{turnover:.1f}%)", 1)
        elif turnover < _thr(th, "turnover_thin_cap", 0.5):
            _add("capital", _thr(w_c, "turnover_thin", 0.8), f"交投清淡(换手{turnover:.1f}%)", -1)

    # ── 汇总：面归一分 → 按面权重加权得综合分 ──
    available = {
        "technical": cfg.get("enable_technical", True) and bool(faces["technical"]),
        "fundamental": bool(faces["fundamental"]),
        "capital": bool(faces["capital"]),
    }
    max_abs = {
        "technical": sum(abs(float(w)) for w in w_t.values()),
        "fundamental": sum(abs(float(w)) for w in w_f.values()),
        "capital": sum(abs(float(w)) for w in w_c.values()),
    }

    face_norm: dict[str, float] = {}
    weighted_sum = 0.0
    weight_sum = 0.0
    for face in ("technical", "fundamental", "capital"):
        if not available.get(face):
            continue
        raw = sum(w for w, _ in faces[face])
        denom = max_abs.get(face) or 0.0
        norm = (raw / denom) if denom > 0 else 0.0
        face_norm[face] = round(norm, 3)
        fw = float(face_weights.get(face, 1.0))
        weighted_sum += fw * norm
        weight_sum += fw
    score = (weighted_sum / weight_sum) if weight_sum else 0.0

    at = cfg.get("action_thresholds", {}) or {}
    if score >= _thr(at, "add", 0.25):
        action = "加仓"
    elif score >= _thr(at, "bullish", 0.08):
        action = "持有偏多"
    elif score > _thr(at, "neutral", -0.08):
        action = "持有观望"
    elif score > _thr(at, "bearish", -0.25):
        action = "减仓偏空"
    else:
        action = "减仓"

    # ── 理由：每个面取最强驱动，保证三维度都被反映 ──
    parts: list[str] = []
    for face in ("technical", "fundamental", "capital"):
        drivers = [e for e in faces[face] if e[0] != 0]
        if not drivers:
            continue
        parts.append(max(drivers, key=lambda x: abs(x[0]))[1])
    reason = "，".join(parts[:3]) if parts else "无明显多空信号"

    return {
        "action": action,
        "score": round(score, 3),
        "reason": reason,
        "drivers": [lbl for entries in faces.values() for _, lbl in entries],
        "faces": face_norm,
    }


def _build_fundamentals(code: str, timeout: float = 12.0) -> dict | None:
    """汇总个股基本面 + 资金面快照，供前端做综合分析。

    优先读本地缓存；未命中时通过 smcore.strategy.fundamental.fetch_fundamental
    实时联网补取（腾讯估值 + baostock 质量/成长/换手）并写回缓存。
    全程在后台线程执行 + 超时保护，避免慢网络拖垮技术面分析响应；
    超时/失败返回 None（前端显示「暂无基本面数据」提示，不影响技术面）。
    """
    try:
        from smcore.strategy.fundamental import fetch_fundamental
    except Exception:
        return None

    result: dict | None = None

    def _run() -> None:
        nonlocal result
        try:
            result = fetch_fundamental(code)
        except Exception:
            result = None

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    # 超时则放弃本次补取（技术面已就绪返回）；残留线程会在后台完成并写缓存，供下次命中
    return result