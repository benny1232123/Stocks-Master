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

    ⚠️ 2026-09-01 与前端 ComprehensivePanel（frontend/src/App.jsx）完全对齐：
    三面均为 0-100 分制，同一因子、同一分段阈值、同一综合权重、同一档位语义，
    保证「日报/后端算出的三维打分 = 网站展示的三维评分」。

    技术面：RSI / MACD(金叉红柱·死叉绿柱) / KDJ(J 极值·K-D) / 均线(MA5-10-20 排列) /
            布林(破下轨·近下轨·近上轨) 五组信号累加 techS → techScore = clamp(50+techS*6, 0, 100)。
    基本面：PE / PB / ROE / 毛利率 / 营收增长 5 因子分段打分取平均（缺失因子给 missing 分）。
    资金面：20 日成交额日均(亿) + 换手率(%) 2 因子分段打分取平均。
    综合分：有基本面时 total = round(tech*0.40 + fund*0.35 + cap*0.25)，否则 = techScore。
    档位：rating 五档（推荐关注/偏积极/中性观望/偏谨慎/回避），action 由 rating 映射
          （加仓/持有偏多/持有观望/减仓偏空/减仓）。

    返回 {action, score, reason, drivers, faces, rating, cls}：
    - score: 综合分 0-100；faces: {technical, fundamental, capital} 各面 0-100
    - drivers: 全部因子明细（技术信号 + 估值/质量/资金点评）
    - reason: 自然语言理由（按前端 verdict 逻辑 + 最强驱动）
    - cls: 各面档位 {technical, fundamental, capital} ∈ good/bad/neutral
    权重与阈值全部来自 smcore.config.defaults.RECOMMENDATION_CONFIG（可传参覆盖）。
    """
    def _num(v: object) -> float | None:
        try:
            return None if v is None else float(v)
        except (TypeError, ValueError):
            return None

    cfg = cfg or RECOMMENDATION_CONFIG
    if analysis.get("error"):
        return {
            "action": "未知", "score": 0, "reason": "分析失败",
            "drivers": [], "faces": {"technical": 0, "fundamental": 0, "capital": 0},
            "rating": "中性观望", "cls": {},
        }

    latest = analysis.get("latest", {}) or {}
    metrics = analysis.get("metrics", {}) or {}
    fund = analysis.get("fundamentals")
    hasF = isinstance(fund, dict) and not fund.get("error")
    fund = fund if hasF else {}

    w_t = cfg.get("technical", {}) or {}
    w_f = cfg.get("fundamental", {}) or {}
    w_c = cfg.get("capital", {}) or {}
    missing_f = float(w_f.get("missing", 50))
    missing_c = float(w_c.get("missing", 50))

    def _seg_score(segments: list, value: float | None) -> tuple[float, str | None]:
        """按前端分段（gt/lt 逐条命中）返回 (score, label)；value=None → missing 分。"""
        if value is None:
            return missing_f, None
        for s in segments:
            if "gt" in s and value > float(s["gt"]):
                return float(s["score"]), s.get("label")
            if "lt" in s and value < float(s["lt"]):
                return float(s["score"]), s.get("label")
        return float(segments[-1]["score"]), segments[-1].get("label")

    # ── 技术面（与前端 ComprehensivePanel 同构）──
    rsi, dif, dea = _num(latest.get("rsi")), _num(latest.get("dif")), _num(latest.get("dea"))
    macdH, kV, dV, jV = (
        _num(latest.get("macd_hist")), _num(latest.get("k_val")),
        _num(latest.get("d_val")), _num(latest.get("j_val")),
    )
    close, lower, upper = _num(latest.get("close")), _num(latest.get("lower")), _num(latest.get("upper"))
    ma5, ma10, ma20 = _num(latest.get("ma5")), _num(latest.get("ma10")), _num(latest.get("ma20"))
    distLo = _num(metrics.get("dist_to_lower_pct"))
    distHi = _num(metrics.get("dist_to_upper_pct"))

    tech_detail: list[tuple[float, str, str]] = []  # (score, label, side)
    techS = 0.0
    if cfg.get("enable_technical", True):
        if rsi is not None:
            hit = next((s for s in w_t.get("rsi", [])
                        if ("gt" in s and rsi > float(s["gt"])) or ("lt" in s and rsi < float(s["lt"]))), None)
            if hit:
                techS += float(hit["score"])
                tech_detail.append((float(hit["score"]), f"RSI·{hit.get('label','')}", "bear" if hit["score"] < 0 else "bull"))
        if dif is not None and dea is not None and macdH is not None:
            if dif > dea and macdH > 0:
                techS += float(w_t.get("macd_golden_red", 2))
                tech_detail.append((float(w_t.get("macd_golden_red", 2)), "MACD·金叉红柱", "bull"))
            elif dif < dea and macdH < 0:
                techS += float(w_t.get("macd_dead_green", -2))
                tech_detail.append((float(w_t.get("macd_dead_green", -2)), "MACD·死叉绿柱", "bear"))
        if kV is not None and dV is not None:
            if jV is not None and jV > float(w_t.get("kdj_j_over", 100)):
                s = float(w_t.get("kdj_j_over_score", -2))
                techS += s; tech_detail.append((s, "KDJ·极端超买", "bear"))
            elif jV is not None and jV < float(w_t.get("kdj_j_under", 0)):
                s = float(w_t.get("kdj_j_under_score", 2))
                techS += s; tech_detail.append((s, "KDJ·极端超卖", "bull"))
            elif kV > dV:
                techS += float(w_t.get("kdj_k_gt_d", 1))
                tech_detail.append((float(w_t.get("kdj_k_gt_d", 1)), "KDJ·金叉", "bull"))
            elif kV < dV:
                techS += float(w_t.get("kdj_k_lt_d", -1))
                tech_detail.append((float(w_t.get("kdj_k_lt_d", -1)), "KDJ·死叉", "bear"))
        if ma5 is not None and ma10 is not None and ma20 is not None:
            if ma5 > ma10 > ma20:
                techS += float(w_t.get("ma_bull", 2)); tech_detail.append((float(w_t.get("ma_bull", 2)), "均线·多头排列", "bull"))
            elif ma5 < ma10 < ma20:
                techS += float(w_t.get("ma_bear", -2)); tech_detail.append((float(w_t.get("ma_bear", -2)), "均线·空头排列", "bear"))
            elif ma5 > ma20:
                techS += float(w_t.get("ma5_gt_ma20", 1)); tech_detail.append((float(w_t.get("ma5_gt_ma20", 1)), "均线·短期偏强", "bull"))
            elif ma5 < ma20:
                techS += float(w_t.get("ma5_lt_ma20", -1)); tech_detail.append((float(w_t.get("ma5_lt_ma20", -1)), "均线·短期偏弱", "bear"))
        if close is not None and lower is not None:
            if close < lower:
                techS += float(w_t.get("boll_below_lower", 1)); tech_detail.append((float(w_t.get("boll_below_lower", 1)), "布林·破下轨", "bull"))
            elif distLo is not None and distLo < float(w_t.get("boll_near_lower_dist", 2.0)):
                techS += float(w_t.get("boll_near_lower", 1)); tech_detail.append((float(w_t.get("boll_near_lower", 1)), "布林·近下轨", "bull"))
            elif distHi is not None and distHi > float(w_t.get("boll_near_upper_dist", -2.0)):
                techS += float(w_t.get("boll_near_upper", -1)); tech_detail.append((float(w_t.get("boll_near_upper", -1)), "布林·近上轨", "bear"))

    tech_base = float(cfg.get("tech_base", 50))
    tech_step = float(cfg.get("tech_step", 6))
    techScore = max(0.0, min(100.0, tech_base + techS * tech_step))
    t_cls_cfg = cfg.get("technical_cls", {}) or {}
    techCls = "good" if techScore >= float(t_cls_cfg.get("good", 70)) else "bad" if techScore <= float(t_cls_cfg.get("bad", 30)) else "neutral"

    # ── 基本面（与前端 ComprehensivePanel 同构）──
    pe, pb = _num(fund.get("pe")), _num(fund.get("pb"))
    roe, gm = _num(fund.get("roe")), _num(fund.get("gross_margin"))
    rg = _num(fund.get("revenue_growth"))
    fund_detail: list[str] = []
    fund_scores: list[float] = []
    if cfg.get("enable_fundamental", True) and hasF:
        for key, value, unit in (("pe", pe, "PE"), ("pb", pb, "PB"), ("roe", roe, "ROE"),
                                 ("gm", gm, "毛利"), ("rg", rg, "营收增长")):
            segments = w_f.get(key)
            if not segments:
                continue
            sc, label = _seg_score(segments, value)
            fund_scores.append(sc)
            if value is not None and label:
                fmt = f"{value:.1f}" if unit in ("PE", "PB") else f"{value*100:.0f}%"
                fund_detail.append(f"{unit}{fmt}·{label}")
    fundScore = round(sum(fund_scores) / len(fund_scores)) if fund_scores else 0.0
    fc_cls = cfg.get("fund_cap_cls", {}) or {}
    fundCls = "good" if fundScore >= float(fc_cls.get("good", 65)) else "bad" if fundScore < float(fc_cls.get("bad", 45)) else "neutral"

    # ── 资金面（与前端 ComprehensivePanel 同构）──
    amt, to = _num(fund.get("amount_20")), _num(fund.get("turnover"))
    cap_detail: list[str] = []
    cap_scores: list[float] = []
    if cfg.get("enable_capital", True) and hasF:
        liq_segs = w_c.get("liq_amt")
        daily_amt = (amt / 20 / 1e8) if amt is not None else None
        if liq_segs:
            sc, label = _seg_score(liq_segs, daily_amt)
            cap_scores.append(sc)
            if daily_amt is not None and label:
                cap_detail.append(f"日均成交{daily_amt:.2f}亿·{label}")
        to_segs = w_c.get("turnover")
        if to_segs:
            sc, label = _seg_score(to_segs, to)
            cap_scores.append(sc)
            if to is not None and label:
                cap_detail.append(f"换手{to:.1f}%·{label}")
    capScore = round(sum(cap_scores) / len(cap_scores)) if cap_scores else 0.0
    capCls = "good" if capScore >= float(fc_cls.get("good", 65)) else "bad" if capScore < float(fc_cls.get("bad", 45)) else "neutral"

    # ── 综合分 + 档位（与前端 ComprehensivePanel 同构）──
    fw = cfg.get("face_weights", {}) or {}
    if hasF:
        total = round(
            techScore * float(fw.get("technical", 0.40))
            + fundScore * float(fw.get("fundamental", 0.35))
            + capScore * float(fw.get("capital", 0.25))
        )
    else:
        total = round(techScore)
    rating = "中性观望"
    for r_cfg in cfg.get("rating", []):
        if r_cfg.get("gte") is not None and total >= float(r_cfg["gte"]):
            rating = r_cfg["label"]
            break
        if r_cfg.get("gte") is None:
            rating = r_cfg["label"]
    action = (cfg.get("action_map", {}) or {}).get(rating, "持有观望")

    # ── 理由（前端 verdict 逻辑 + 各面最强驱动）──
    if not hasF:
        reason = "当前标的暂无基本面/资金面缓存，研判仅基于技术面"
    elif techCls == "good" and fundScore >= 60 and capScore >= 55:
        reason = "三维共振偏多：技术走强、基本面扎实、资金活跃"
    elif techCls == "bad" and fundScore < 55:
        reason = "技术与基本面双弱，风险偏高"
    elif techCls == "good" and fundScore < 55:
        reason = "技术面偏多但基本面一般"
    elif techCls != "good" and fundScore >= 65 and capScore >= 55:
        reason = "基本面优质、资金认可，技术震荡"
    else:
        reason = "多空因素交织，建议结合仓位管理观望"
    drivers = [lbl for _, lbl, _ in tech_detail] + fund_detail + cap_detail
    if drivers:
        reason += "；" + "，".join(drivers[:4])

    return {
        "action": action,
        "score": int(total),
        "reason": reason,
        "drivers": drivers,
        "faces": {"technical": int(techScore), "fundamental": int(fundScore), "capital": int(capScore)},
        "rating": rating,
        "cls": {"technical": techCls, "fundamental": fundCls, "capital": capCls},
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