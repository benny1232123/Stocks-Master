from __future__ import annotations

"""A股动量 / 相对强度策略（smcore 多策略体系第 5 个），轻量实现。

设计定位（与现有四策略互补）：
- Boll   = 买「超卖 / 近下轨」的弱势反转
- CCTV   = 买「舆情热门」板块
- Theme  = 买「题材 + 短线动量」
- Relativity = 买「指数相对强弱（抗跌/跟涨）」
- Momentum = 买「中期上升趋势的强势股」（20/60 日收益 + MA20 上行 + 近高点）

实现要点（轻量、可控）：
1. 用新浪 `ak.stock_zh_a_spot()` 一次拉全市场快照做廉价预筛（价格/流动性/当日涨幅），
   避免对全市场逐只拉 K 线。默认不用东财接口（东财接口常不可用），仅在
   MOMENTUM_USE_EASTMONEY=1 时回退尝试东财快照（含 60日涨跌幅/换手率 更丰富）。
2. 仅对预筛后的 Top 候选拉前复权 K 线，确认 20 日收益、MA20 斜率、距 20 日高点距离。
3. 严格排除创业板(30x)/科创板(688x)，价格 5~30，与 Relativity 边界一致。
4. 仅选处于上升趋势（20日收益>0 且 MA20 上行）的票，作为「买强不买弱」维度。

输出 stock_data/Stock-Selection-Momentum-{today}.csv
(股票代码, 股票名称, 建议买入价, 动量分, 20日收益%, 60日收益%, MA20斜率%, 距20日高点%)
"""
import argparse
import concurrent.futures as cf
import os
import socket
import time
from datetime import datetime, timedelta

import akshare as ak
import pandas as pd

from smcore.config.defaults import STOCK_DATA_DIR
from smcore.data.kline import fetch_daily_k
from smcore.utils.code import format_stock_code


def _load_momentum_config():
    """从 risk_config.json 读取动量策略参数（消除模块级魔数）。"""
    try:
        import json
        _cfg_path = STOCK_DATA_DIR.parent / "smcore" / "strategy" / "risk_config.json"
        with open(_cfg_path, "r", encoding="utf-8") as _f:
            _cfg = json.load(_f)
        return _cfg.get("momentum", {})
    except Exception:
        return {}


# —— 预筛参数（默认值仅作 fallback；正常全部走 config）——
_MCFG = _load_momentum_config()
PRICE_UPPER_LIMIT = float(_MCFG.get("price_upper_limit", 50.0))
PRICE_LOWER_LIMIT = float(_MCFG.get("price_lower_limit", 5.0))
MIN_TURNOVER = float(_MCFG.get("min_turnover", 2e8))
MIN_TURNOVER_RATE = float(_MCFG.get("min_turnover_rate", 1.0))
MIN_60D_RETURN = float(_MCFG.get("min_60d_return", 0.0))
MAX_CANDIDATES = int(_MCFG.get("max_candidates", 80))
TOP_N = int(_MCFG.get("top_n", 30))

# 动量评分权重
W_RET20 = float(_MCFG.get("w_ret20", 0.40))
W_RET60 = float(_MCFG.get("w_ret60", 0.30))
W_MA20_SLOPE = float(_MCFG.get("w_ma20_slope", 0.30))

# 打分校正（原 +5/-8 魔数已改为与分数量纲对齐的合理值）
VOL_CONFIRM_BONUS = float(_MCFG.get("vol_confirm_bonus", 2.0))
NEAR_HIGH_PENALTY = float(_MCFG.get("near_high_penalty", 3.0))
NEAR_HIGH_THRESHOLD = float(_MCFG.get("near_high_threshold", 0.02))
FAR_FROM_HIGH_THRESHOLD = float(_MCFG.get("far_from_high_threshold", -0.18))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="A股动量/相对强度策略（轻量）")
    p.add_argument("--price-upper-limit", type=float, default=PRICE_UPPER_LIMIT)
    p.add_argument("--price-lower-limit", type=float, default=PRICE_LOWER_LIMIT)
    p.add_argument("--min-turnover", type=float, default=MIN_TURNOVER)
    p.add_argument("--min-turnover-rate", type=float, default=MIN_TURNOVER_RATE)
    p.add_argument("--min-60d-return", type=float, default=MIN_60D_RETURN)
    p.add_argument("--max-candidates", type=int, default=MAX_CANDIDATES)
    p.add_argument("--top-n", type=int, default=TOP_N)
    p.add_argument("--sleep-seconds", type=float, default=0.05)
    p.add_argument("--date", default=None,
                   help="信号日 YYYY-MM-DD（历史重放用；默认今天）。K线窗口锚定该日，"
                        "避免 datetime.now() 造成的时点错配/前视偏差。")
    return p.parse_args()


def _fetch_spot_sina() -> pd.DataFrame | None:
    """新浪全市场快照（东财-free，沙箱/海外均可达）。列: 代码,名称,最新价,涨跌幅,成交额...

    新浪接口偶发返回空 df（瞬断），故重试 3 次；仍为空则返回 None 由上层降级。
    """
    for attempt in range(3):
        try:
            df = ak.stock_zh_a_spot()
            if df is not None and not df.empty:
                return df
            print(f"[动量] 新浪快照第 {attempt + 1} 次返回空，重试...")
        except Exception as exc:  # noqa: BLE001
            print(f"[动量] 新浪快照第 {attempt + 1} 次失败（{type(exc).__name__}），重试...")
        time.sleep(2)
    print("[动量] 新浪快照重试后仍为空")
    return None


def _fetch_spot_em() -> pd.DataFrame | None:
    """东财全市场快照（列更丰富：含 60日涨跌幅/换手率）。仅在 MOMENTUM_USE_EASTMONEY=1 时尝试，
    用线程超时(15s)包裹避免东财接口挂起拖垮整策略。"""
    try:
        with cf.ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(ak.stock_zh_a_spot_em)
            return fut.result(timeout=15)
    except Exception as exc:  # noqa: BLE001
        print(f"[动量] 东财快照失败（{type(exc).__name__}），跳过")
        return None


def _fetch_spot() -> pd.DataFrame:
    """拉全市场快照：默认新浪（东财-free）；显式开关下回退东财。"""
    df = _fetch_spot_sina()
    if (df is None or df.empty) and os.getenv("MOMENTUM_USE_EASTMONEY") == "1":
        df = _fetch_spot_em()
    return df if df is not None else pd.DataFrame()


def _momentum_metrics(code: str, sleep_seconds: float, min_60d: float = MIN_60D_RETURN, as_of_date: str | None = None) -> dict | None:
    # K线窗口锚定信号日（as_of_date），默认今天；避免 datetime.now() 造成的时点错配/前视偏差，
    # 使策略可被历史诚实重放（backfill / PositionMonitor 传信号日即可）。
    if as_of_date:
        try:
            end_dt = datetime.strptime(as_of_date, "%Y-%m-%d")
        except (ValueError, TypeError):
            end_dt = datetime.now()
    else:
        end_dt = datetime.now()
    end = end_dt.strftime("%Y-%m-%d")
    start = (end_dt - timedelta(days=80)).strftime("%Y-%m-%d")
    time.sleep(max(sleep_seconds, 0.0))
    df = fetch_daily_k(code, start, end, adjust="qfq")
    if df is None or len(df) < 25:
        return None
    close = pd.to_numeric(df["close"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    vol = pd.to_numeric(df["volume"], errors="coerce")
    if close.isna().any() or len(close) < 25:
        return None
    last = float(close.iloc[-1])
    if last <= 0:
        return None
    # 必须至少 61 根才能可靠计算 60 日收益；不足则剔除（不再退回窗口起点 close[0]，避免失真评分）。
    if len(close) <= 61:
        return None
    try:
        ret20 = last / float(close.iloc[-21]) - 1
        ret60 = last / float(close.iloc[-61]) - 1
    except (IndexError, ZeroDivisionError):
        return None
    if ret60 < min_60d:
        return None  # 必须 60 日收益达标（中期已走强；新浪快照无 60日列时由此处把关）
    if ret20 <= 0:
        return None  # 必须 20 日收益为正（上升趋势）
    # MA20 斜率：当前 MA20 vs 20 个交易日前 MA20
    ma20_now = float(close.tail(20).mean())
    if len(close) >= 40:
        ma20_prev = float(close.iloc[-40:-20].mean())
    else:
        ma20_prev = float(close.iloc[0])
    ma20_slope = (ma20_now - ma20_prev) / ma20_prev if ma20_prev > 0 else 0.0
    if ma20_slope <= 0:
        return None  # MA20 必须上行
    high20 = float(high.tail(20).max())
    dist_from_high = last / high20 - 1 if high20 > 0 else 0.0
    # 量能确认：近 5 日均量 > 近 20 日均量
    vol_confirm = bool(vol.tail(5).mean() > vol.tail(20).mean() * 1.05) if len(vol) >= 20 else False
    return {
        "ret20": ret20,
        "ret60": ret60,
        "ma20_slope": ma20_slope,
        "dist_from_high": dist_from_high,
        "vol_confirm": vol_confirm,
        "last": last,
    }


def _write_empty_momentum(date_str: str, reason: str) -> None:
    """写空动量标记文件，让 fusion 能区分「跑了但为空」vs「没跑过」。"""
    out_path = STOCK_DATA_DIR / f"Stock-Selection-Momentum-{date_str}.csv"
    pd.DataFrame(columns=["股票代码", "股票名称", "建议买入价", "动量分",
                          "20日收益%", "60日收益%", "MA20斜率%", "距20日高点%"]).to_csv(
        out_path, index=False, encoding="utf-8-sig"
    )
    print(f"[动量] 产出=0（{reason}）→ 已写入空文件 {out_path.name}")


def run_momentum() -> None:
    args = parse_args()
    STOCK_DATA_DIR.mkdir(parents=True, exist_ok=True)
    # 支持历史重放：--date 指定信号日（YYYY-MM-DD），否则默认今天。
    if args.date:
        try:
            today_text = datetime.strptime(args.date, "%Y-%m-%d").strftime("%Y%m%d")
        except (ValueError, TypeError):
            today_text = datetime.now().strftime("%Y%m%d")
    else:
        today_text = datetime.now().strftime("%Y%m%d")

    print(f"[动量] 拉取全市场快照（新浪，东财-free）...")
    try:
        spot = _fetch_spot()
    except Exception as exc:
        print(f"[动量] 快照拉取失败（{type(exc).__name__}），跳过本策略: {exc}")
        _write_empty_momentum(today_text, f"快照拉取失败: {exc}")
        return
    if spot is None or spot.empty:
        print("[动量] 快照为空，退出")
        _write_empty_momentum(today_text, "快照为空")
        return
    spot = spot.copy()
    spot["代码"] = spot["代码"].astype(str).str.zfill(6)
    spot["最新价"] = pd.to_numeric(spot.get("最新价"), errors="coerce")
    spot["成交额"] = pd.to_numeric(spot.get("成交额"), errors="coerce")
    # 新浪快照列: 代码,名称,最新价,涨跌幅,成交额...（无 换手率 / 60日涨跌幅）
    # 东财快照(可选)额外含 换手率 / 60日涨跌幅 —— 自适应启用
    has_turn = "换手率" in spot.columns
    has_60 = "60日涨跌幅" in spot.columns
    if has_turn:
        spot["换手率"] = pd.to_numeric(spot.get("换手率"), errors="coerce")
    if has_60:
        spot["60日涨跌幅"] = pd.to_numeric(spot.get("60日涨跌幅"), errors="coerce")

    # 预筛
    mask = (
        ~spot["代码"].str.startswith("30")
        & ~spot["代码"].str.startswith("688")
        & (spot["最新价"] >= args.price_lower_limit)
        & (spot["最新价"] <= args.price_upper_limit)
        & (spot["成交额"] >= args.min_turnover)
    )
    if has_60:
        mask &= (spot["60日涨跌幅"] >= args.min_60d_return)
    if has_turn:
        mask &= (spot["换手率"] >= args.min_turnover_rate)
    cand = spot[mask].copy()
    if cand.empty:
        print("[动量] 预筛后无候选")
        _write_empty_momentum(today_text, "预筛后无候选")
        return
    # 排序：东财快照按 60日涨幅，新浪快照按当日涨幅（廉价强度代理）；取 Top 候选拉 K 线确认
    sort_col = "60日涨跌幅" if has_60 else "涨跌幅"
    cand = cand.sort_values(sort_col, ascending=False).head(args.max_candidates)
    print(f"[动量] 预筛候选 {len(cand)} 只（快照源={'东财' if has_60 else '新浪'}），开始拉 K 线确认动量 ...")

    rows = []
    done = 0
    total = len(cand)
    for _, r in cand.iterrows():
        code = format_stock_code(r["代码"])
        name = str(r.get("名称", "") or "")
        m = _momentum_metrics(code, args.sleep_seconds, args.min_60d_return, as_of_date=args.date)
        done += 1
        if m is None:
            continue
        score = (
            m["ret20"] * 100 * W_RET20
            + m["ret60"] * 100 * W_RET60
            + m["ma20_slope"] * 100 * W_MA20_SLOPE
        )
        if m["vol_confirm"]:
            score += VOL_CONFIRM_BONUS
        # 距高点过近视为追高略降分；过远视为转弱剔除（阈值均走 config）
        if m["dist_from_high"] > NEAR_HIGH_THRESHOLD:
            score -= NEAR_HIGH_PENALTY
        if m["dist_from_high"] < FAR_FROM_HIGH_THRESHOLD:
            continue
        rows.append({
            "股票代码": code,
            "股票名称": name,
            "建议买入价": round(m["last"], 2),
            "动量分": round(score, 2),
            "20日收益%": round(m["ret20"] * 100, 2),
            "60日收益%": round(m["ret60"] * 100, 2),
            "MA20斜率%": round(m["ma20_slope"] * 100, 2),
            "距20日高点%": round(m["dist_from_high"] * 100, 2),
        })
        print(f"[动量] PASS {code} {name} | 动量分={score:.1f} 20日={m['ret20']*100:.1f}% MA20斜率={m['ma20_slope']*100:.1f}%")

    if not rows:
        print("[动量] 无符合动量条件的股票")
        _write_empty_momentum(today_text, "K线确认后无符合条件的股票")
        return

    out = pd.DataFrame(rows).sort_values("动量分", ascending=False).head(args.top_n)
    out_path = STOCK_DATA_DIR / f"Stock-Selection-Momentum-{today_text}.csv"
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n{out_path.name} 已保存，共 {len(out)} 只")
    print(out.to_string(index=False))


if __name__ == "__main__":
    run_momentum()
