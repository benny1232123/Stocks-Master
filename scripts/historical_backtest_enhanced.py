"""历史长周期回测（增强入场信号版）。

在 `historical_backtest.py`（代理信号版）基础上，对入场信号加两个
学界与实战均验证过的「超卖反转确认因子」，筛选掉「假超卖 / 阴跌不止」：

    1) 成交量确认：当日成交量 >= 5日均量 × VOL_CONFIRM_MULT（默认 1.3）
       —— 放量见底，排除无量阴跌（无量下跌往往还没到底）
    2) RSI(14) 超卖强化：RSI < RSI_OS（默认 35）
       —— 不只是「偏低」，是真超卖区，过滤掉「仅仅是弱势盘整」的伪信号

其余与 historical_backtest.py 完全一致：
    - 收盘价 <= 布林下轨×1.015（超卖/近下轨，对应策略1 boll）
    - 20日相对强弱 >= 0（不弱于指数，对应策略4 relativity）
    - 当日成交额 >= ¥1亿（流动性，对应 fusion 内联过滤）
    - 出场 / 仓位**完全复用线上引擎** `run_forward_signal_backtest`

输出独立命名（Enhanced-*），与 Historical-* / Multi-* 都隔离，便于对照：
    stock_data/Enhanced-Backtest-{YYYYMMDD}-summary.csv / -equity.csv / -trades.csv
    stock_data/Enhanced-Backtest-ALL-summary.csv（聚合）
"""
from __future__ import annotations

import os
import sys
import time
import glob
from datetime import date, timedelta

import numpy as np
import pandas as pd

# ── 复用线上回测引擎与工具 ──
from smcore.data import kline as kline_mod
from smcore.backtest import run_forward_signal_backtest
from scripts.daily_backtest import (
    format_stock_code,
    compute_market_profile,
    TOP_N,
    VOL_STOP_MULT,
)
from smcore.strategy.risk_rules import compute_adaptive_exit_params
from smcore.strategy.adaptive_weights import cash_from_volatility, cash_from_regime

HIST_START = os.environ.get("HIST_START", "2026-01-01")
HIST_END = os.environ.get("HIST_END", date.today().strftime("%Y-%m-%d"))
HOLD_DAYS = int(os.environ.get("HOLD_DAYS", "5"))
TOP_N = int(os.environ.get("HIST_TOP_N", str(TOP_N)))
INTERVAL = float(os.environ.get("PREPULL_INTERVAL", "0.3"))
MIN_AMOUNT = float(os.environ.get("BACKTEST_MIN_AMOUNT", "100000000"))  # ¥1亿
MAX_CODES = int(os.environ.get("HIST_MAX_CODES", "0"))  # 0=全量；测试时设小值做 pilot
BOLL_WIN = 20
BOLL_K = 1.645
RSI_WIN = 14
RSI_OS = float(os.environ.get("RSI_OS", "35"))            # 超卖阈值
VOL_CONFIRM_MULT = float(os.environ.get("VOL_CONFIRM_MULT", "1.3"))  # 放量确认倍数
OUT_PREFIX = os.environ.get("OUT_PREFIX", "Enhanced-Backtest")        # 输出前缀：同入场不同出场做对照用
STRAT_TAG = os.environ.get("STRAT_TAG", "historical-enhanced")        # 写入 summary 的 strategies 标签

from smcore.artifacts import STOCK_DATA_DIR


def _load_all_codes() -> list[str]:
    """从权威名单缓存加载全 A 股代码（与 fusion 同源）。"""
    cache = STOCK_DATA_DIR / "stock_info_a_code_name.csv"
    if not cache.exists():
        from smcore.strategy.fusion import _get_stock_name_map
        _get_stock_name_map()
    df = pd.read_csv(cache, dtype=str)
    codes = [format_stock_code(c) for c in df["code"].dropna() if format_stock_code(c)]
    return sorted(set(codes))


def _boll_lower_upper(close: pd.Series) -> tuple[float, float]:
    """返回 (下轨, 上轨)，样本不足返回 (nan, nan)。"""
    if len(close) < BOLL_WIN:
        return (float("nan"), float("nan"))
    ma = close.mean()
    sd = close.std(ddof=0)
    lower = ma - BOLL_K * sd
    upper = ma + BOLL_K * sd
    return (float(lower), float(upper))


def _rsi(close: pd.Series, n: int = RSI_WIN) -> float:
    """Wilder RSI。样本不足返回 nan。"""
    if len(close) < n + 1:
        return float("nan")
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(n).mean()
    loss = (-delta.clip(upper=0)).rolling(n).mean()
    rs = gain / loss.replace(0, float("nan"))
    rsi = 100 - 100 / (1 + rs)
    v = rsi.iloc[-1]
    return float(v)


def _run_one_day(sd, sd_tag, local, idx_ret20, market_profile, hold_days,
                 top_n, min_amount, all_summaries):
    """对单个历史交易日生成【增强】代理信号并回测、落盘。"""
    sd_ts = pd.Timestamp(sd)

    # 候选：布林下轨超卖 + 相对强弱 + 流动性 + 【增强】成交量确认 + RSI 超卖
    cands = []
    for code, df in local.items():
        if sd_ts not in df.index:
            continue
        row = df.loc[sd_ts]
        close = float(row["close"])
        amt = float(row.get("amount", 0) or 0)
        if amt < min_amount:
            continue
        win = df.loc[:sd_ts].tail(BOLL_WIN)
        lower, _ = _boll_lower_upper(win["close"])
        if not (lower == lower):  # nan 检查
            continue
        if not (close <= lower * 1.015):  # 超卖/近下轨（与线上 boll 策略一致）
            continue
        # ── 增强过滤 1：成交量确认（放量见底，排除无量阴跌）──
        vol = float(row.get("volume", 0) or 0)
        vol_ma5 = float(win["volume"].tail(5).mean())
        if vol < vol_ma5 * VOL_CONFIRM_MULT:
            continue
        # ── 增强过滤 2：RSI(14) 超卖强化 ──
        rsi = _rsi(win["close"])
        if rsi == rsi and rsi >= RSI_OS:  # 非负 nan 且未进入超卖区
            continue
        idx_r = float(idx_ret20.loc[sd_ts]) if sd_ts in idx_ret20.index else 0.0
        ret20 = float(win["close"].pct_change(BOLL_WIN).iloc[-1])
        rs = ret20 - idx_r
        if rs < 0:  # 弱于指数则不要
            continue
        cands.append((code, close, lower, rs))

    if not cands:
        # 无信号日也落一个空 summary，避免反复重算
        pd.DataFrame([{
            "date": sd_tag, "num_trades": 0, "total_return": 0.0,
            "max_drawdown": 0.0, "win_rate": 0.0, "sharpe": 0.0,
            "strategies": STRAT_TAG, "signal_mode": "forward",
            "hold_days": hold_days, "signal_start": sd.strftime("%Y-%m-%d"),
            "signal_end": sd.strftime("%Y-%m-%d"),
            "exit_mode": "boll_upper_take+take6%+trailing5%+MA60break",
        }]).to_csv(
            STOCK_DATA_DIR / f"{OUT_PREFIX}-{sd_tag}-summary.csv",
            index=False, encoding="utf-8-sig")
        return

    cands.sort(key=lambda x: x[3], reverse=True)
    cands = cands[:top_n]
    codes_d = [c[0] for c in cands]
    # 上轨用于 use_signal_bands 止盈
    upper_map = {}
    for code in codes_d:
        df = local[code]
        w = df.loc[:sd_ts].tail(BOLL_WIN)["close"]
        _, up = _boll_lower_upper(w)
        upper_map[code] = up
    sub = pd.DataFrame({
        "日期": [sd.strftime("%Y-%m-%d")] * len(codes_d),
        "代码": codes_d,
        "建议买入价": [c[1] for c in cands],
        "止盈价(上轨)": [upper_map.get(c[0], float("nan")) for c in cands],
        "综合评分": [round(c[2] * 100, 2) for c in cands],
    })

    capital_scale = 1.0
    if market_profile is not None:
        _cash = cash_from_volatility(getattr(market_profile, "volatility_pctile", None))
        _cash = cash_from_regime(getattr(market_profile, "regime", None), _cash)
        capital_scale = max(0.0, 1.0 - _cash / 100.0)

    _exit = compute_adaptive_exit_params(market_profile, regime=getattr(market_profile, "regime", None))
    result = run_forward_signal_backtest(
        sub, hold_days=hold_days, initial_capital=100000.0,
        max_positions=200, enable_exits=True, use_signal_bands=True,
        stop_loss_pct=_exit["stop_loss_pct"], take_profit_pct=_exit["take_profit_pct"],
        trailing_stop_pct=_exit["trailing_stop_pct"], trend_exit_ma=_exit["trend_exit_ma"],
        size_by="综合评分", capital_scale=capital_scale,
    )
    if result.summary.get("error"):
        return

    summary = dict(result.summary)
    summary["date"] = sd_tag
    summary["run_date"] = date.today().strftime("%Y%m%d")
    summary["signal_start"] = sd.strftime("%Y-%m-%d")
    summary["signal_end"] = sd.strftime("%Y-%m-%d")
    summary["hold_days"] = hold_days
    summary["exit_mode"] = "boll_upper_take+take6%+trailing5%+MA60break"
    summary["size_mode"] = "conviction(综合评分)"
    summary["vol_mode"] = f"scaled_stop+pos{capital_scale}"
    summary["capital_scale"] = round(capital_scale, 2)
    summary["signals_days"] = 1
    summary["codes_count"] = len(sub)
    summary["strategies"] = STRAT_TAG
    summary["start"] = sd.strftime("%Y-%m-%d")
    summary["end"] = (sd + timedelta(days=hold_days)).strftime("%Y-%m-%d")

    base = STOCK_DATA_DIR / f"{OUT_PREFIX}-{sd_tag}"
    pd.DataFrame([summary]).to_csv(f"{base}-summary.csv", index=False, encoding="utf-8-sig")
    eq = result.equity.copy()
    eq["peak"] = eq["total"].cummax()
    eq["drawdown"] = (eq["total"] - eq["peak"]) / eq["peak"] * 100
    eq.to_csv(f"{base}-equity.csv", index=False, encoding="utf-8-sig")
    result.trades.to_csv(f"{base}-trades.csv", index=False, encoding="utf-8-sig")
    all_summaries.append(summary)


def main() -> int:
    start = pd.to_datetime(HIST_START).date()
    end = pd.to_datetime(HIST_END).date()
    today = date.today()
    if end > today:
        end = today
    t0 = time.time()
    print(f"[增强回测] 区间 {start} ~ {end} HOLD={HOLD_DAYS}d TOP_N={TOP_N} "
          f"MIN_AMT={MIN_AMOUNT/1e8:.0f}亿 RSI<{RSI_OS} VOL>={VOL_CONFIRM_MULT}xMA5",
          flush=True)

    # ── 进程内 K 线缓存（跨股票去重）──
    _orig = kline_mod.fetch_daily_k
    _kcache: dict = {}

    def _cached(code, s, e, *a, **k):
        key = (str(code), str(s), str(e), k.get("adjust", "qfq"))
        if key not in _kcache:
            _kcache[key] = _orig(code, s, e, *a, **k)
        return _kcache[key]

    kline_mod.fetch_daily_k = _cached

    # 沪深300 序列（计算 20 日相对强弱基准）
    idx_df = _orig("sh.000300", start - timedelta(days=120), end, adjust="qfq")
    idx_df = idx_df.copy()
    idx_df["_dt"] = pd.to_datetime(idx_df["date"])
    idx_df = idx_df.sort_values("_dt").set_index("_dt")
    idx_ret20 = idx_df["close"].pct_change(BOLL_WIN)

    # ── 预拉全量 K 线 ──
    codes = _load_all_codes()
    if MAX_CODES and MAX_CODES > 0:
        codes = codes[:MAX_CODES]
        print(f"[pilot] HIST_MAX_CODES={MAX_CODES}，仅用前 {len(codes)} 只做验证", flush=True)
    global_start = start - timedelta(days=120)  # 给布林窗口 + 20日收益留头
    print(f"[预拉K线] {len(codes)} 只, 范围 {global_start} ~ {end}, 串行(间隔 {INTERVAL}s)", flush=True)
    t_pre = time.time()
    ok = 0
    local: dict[str, pd.DataFrame] = {}
    for i, code in enumerate(codes):
        df = None
        for _attempt in range(2):  # 失败重试 1 次，扛网络抖动
            try:
                df = _orig(code, global_start, end, adjust="qfq")
                break
            except Exception:
                if _attempt == 0:
                    time.sleep(1.0)
                else:
                    df = None
        if df is not None and not df.empty:
            try:
                d = df.copy()
                d["_dt"] = pd.to_datetime(d["date"])
                d = d.sort_values("_dt").set_index("_dt")
                local[code] = d
                ok += 1
            except Exception:
                pass
        if (i + 1) % 50 == 0 or (i + 1) == len(codes):
            el = time.time() - t_pre
            eta = (el / (i + 1)) * (len(codes) - i - 1)
            print(f"  [预拉 {i+1}/{len(codes)}] 成功 {ok} 已用 {el:.0f}s 剩余 {eta:.0f}s", flush=True)
        if INTERVAL > 0 and i + 1 < len(codes):
            time.sleep(INTERVAL)

    # ── 逐交易日生成增强代理信号并回测 ──
    all_dates = sorted({d for df in local.values() for d in df.index})
    trade_dates = [d.date() for d in all_dates if start <= d.date() <= end]
    print(f"[信号日] {len(trade_dates)} 个交易日待回测", flush=True)

    market_profile = None
    try:
        market_profile = compute_market_profile()
    except Exception as e:
        print(f"[warn] 市场仪表盘失败：{e}")

    completed = 0
    skipped = 0
    all_summaries: list[dict] = []
    for sd in trade_dates:
        sd_tag = sd.strftime("%Y%m%d")
        sum_path = STOCK_DATA_DIR / f"{OUT_PREFIX}-{sd_tag}-summary.csv"
        if sum_path.exists():
            skipped += 1
            continue
        try:
            _run_one_day(sd, sd_tag, local, idx_ret20, market_profile,
                          HOLD_DAYS, TOP_N, MIN_AMOUNT, all_summaries)
            if sum_path.exists():
                completed += 1
                if completed % 5 == 0:
                    print(f"  [进度] 已完成 {completed} 个信号日 (跳过 {skipped})", flush=True)
        except Exception as e:
            print(f"  [!] {sd} 异常跳过：{e}", flush=True)
            continue

    # ── 聚合全部增强回测 ──
    files = sorted(glob.glob(str(STOCK_DATA_DIR / f"{OUT_PREFIX}-*-summary.csv")), reverse=True)
    rows = []
    for f in files:
        try:
            rows.append(pd.read_csv(f).to_dict("records")[0])
        except Exception:
            continue
    if rows:
        pd.DataFrame(rows).to_csv(
            STOCK_DATA_DIR / f"{OUT_PREFIX}-ALL-summary.csv",
            index=False, encoding="utf-8-sig")

    el = time.time() - t0
    print(f"\n[增强回测] 完成：新回测 {completed} 天, 跳过已有 {skipped} 天, "
          f"聚合 {len(rows)} 天, 耗时 {el:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
