"""出场参数敏感性扫描（固定代理入场信号）。

思路
----
入场信号（布林下轨超卖 + 20日相对强弱 + ¥1亿流动性）**只生成一次**，
之后对每个候选参数组合只改「出场/仓位」参数重跑 `run_forward_signal_backtest`，
从而在「入场完全相同」的前提下，干净地隔离出场参数的影响。

K 线走进程内缓存（底层仍是 smcore 磁盘缓存 stock_data/k_data/*.csv，已全量就位），
因此重算几乎无网络开销。

输出
----
- stock_data/Param-Scan-results.csv    （每组参数的聚合指标）
- 终端打印每组结果的对比表
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
MAX_CODES = int(os.environ.get("HIST_MAX_CODES", "1500"))  # 扫描用子集，相对比较足矣
BOLL_WIN = 20
BOLL_K = 1.645

from smcore.artifacts import STOCK_DATA_DIR


def _load_all_codes() -> list[str]:
    cache = STOCK_DATA_DIR / "stock_info_a_code_name.csv"
    if not cache.exists():
        from smcore.strategy.fusion import _get_stock_name_map
        _get_stock_name_map()
    df = pd.read_csv(cache, dtype=str)
    codes = [format_stock_code(c) for c in df["code"].dropna() if format_stock_code(c)]
    return sorted(set(codes))


def _boll_lower_upper(close: pd.Series) -> tuple[float, float]:
    if len(close) < BOLL_WIN:
        return (float("nan"), float("nan"))
    ma = close.mean()
    sd = close.std(ddof=0)
    lower = ma - BOLL_K * sd
    upper = ma + BOLL_K * sd
    return (float(lower), float(upper))


def _gen_candidates(local, idx_ret20, market_profile, hold_days, top_n, min_amount):
    """生成全部信号日的候选信号（只调用一次），返回 list[(sd, sd_tag, sub_df, capital_scale)]。"""
    all_dates = sorted({d for df in local.values() for d in df.index})
    start = pd.to_datetime(HIST_START).date()
    end = pd.to_datetime(HIST_END).date()
    trade_dates = [d.date() for d in all_dates if start <= d.date() <= end]

    caps = 1.0
    if market_profile is not None:
        _cash = cash_from_volatility(getattr(market_profile, "volatility_pctile", None))
        _cash = cash_from_regime(getattr(market_profile, "regime", None), _cash)
        caps = max(0.0, 1.0 - _cash / 100.0)

    out = []
    for sd in trade_dates:
        sd_ts = pd.Timestamp(sd)
        cands = []
        for code, df in local.items():
            if sd_ts not in df.index:
                continue
            row = df.loc[sd_ts]
            close = float(row["close"])
            amt = float(row.get("amount", 0) or 0)
            if amt < min_amount:
                continue
            win = df.loc[:sd_ts].tail(BOLL_WIN)["close"]
            lower, _ = _boll_lower_upper(win)
            if not (lower == lower):
                continue
            if not (close <= lower * 1.015):
                continue
            idx_r = float(idx_ret20.loc[sd_ts]) if sd_ts in idx_ret20.index else 0.0
            ret20 = float(win.pct_change(BOLL_WIN).iloc[-1])
            rs = ret20 - idx_r
            if rs < 0:
                continue
            cands.append((code, close, lower, rs))

        if not cands:
            continue
        cands.sort(key=lambda x: x[3], reverse=True)
        cands = cands[:top_n]
        codes_d = [c[0] for c in cands]
        upper_map = {}
        for code in codes_d:
            w = local[code].loc[:sd_ts].tail(BOLL_WIN)["close"]
            _, up = _boll_lower_upper(w)
            upper_map[code] = up
        sub = pd.DataFrame({
            "日期": [sd.strftime("%Y-%m-%d")] * len(codes_d),
            "代码": codes_d,
            "建议买入价": [c[1] for c in cands],
            "止盈价(上轨)": [upper_map.get(c[0], float("nan")) for c in cands],
            "综合评分": [round(c[2] * 100, 2) for c in cands],
        })
        out.append((sd, sd.strftime("%Y%m%d"), sub, caps))
    return out


# ── 待扫描的参数组合（均相对 baseline 改一个维度，便于归因）──
SCAN = [
    ("baseline",        dict(hold_days=5,  stop_loss_pct=0.08, take_profit_pct=0.06, trailing_stop_pct=0.05, trend_exit_ma=60)),
    ("hold10",          dict(hold_days=10, stop_loss_pct=0.08, take_profit_pct=0.06, trailing_stop_pct=0.05, trend_exit_ma=60)),
    ("hold20",          dict(hold_days=20, stop_loss_pct=0.08, take_profit_pct=0.06, trailing_stop_pct=0.05, trend_exit_ma=60)),
    ("trail15",         dict(hold_days=5,  stop_loss_pct=0.08, take_profit_pct=0.06, trailing_stop_pct=0.15, trend_exit_ma=60)),
    ("trail20",         dict(hold_days=5,  stop_loss_pct=0.08, take_profit_pct=0.06, trailing_stop_pct=0.20, trend_exit_ma=60)),
    ("stoploss05",      dict(hold_days=5,  stop_loss_pct=0.05, take_profit_pct=0.06, trailing_stop_pct=0.05, trend_exit_ma=60)),
    ("notrendexit",     dict(hold_days=5,  stop_loss_pct=0.08, take_profit_pct=0.06, trailing_stop_pct=0.05, trend_exit_ma=0)),
]


def _aggregate(per_day_summaries):
    if not per_day_summaries:
        return dict(days=0, avg_return=0.0, median_return=0.0, win_days=0.0,
                    win_rate=0.0, avg_drawdown=0.0, sharpe=0.0, total_trades=0)
    t = pd.DataFrame(per_day_summaries)
    t = t[t["num_trades"] > 0]
    if t.empty:
        return dict(days=0, avg_return=0.0, median_return=0.0, win_days=0.0,
                    win_rate=0.0, avg_drawdown=0.0, sharpe=0.0, total_trades=0)
    return dict(
        days=len(t),
        avg_return=round(float(t["total_return"].mean()), 3),
        median_return=round(float(t["total_return"].median()), 3),
        win_days=int((t["total_return"] > 0).sum()),
        win_days_pct=round(float((t["total_return"] > 0).mean() * 100), 1),
        win_rate=round(float(t["win_rate"].mean()), 1),
        avg_drawdown=round(float(t["max_drawdown"].mean()), 3),
        sharpe=round(float(t["sharpe"].mean()), 3),
        total_trades=int(t["num_trades"].sum()),
    )


def main() -> int:
    start = pd.to_datetime(HIST_START).date()
    end = pd.to_datetime(HIST_END).date()
    today = date.today()
    if end > today:
        end = today
    t0 = time.time()
    print(f"[参数扫描] 区间 {start}~{end} TOP_N={TOP_N} MIN_AMT={MIN_AMOUNT/1e8:.0f}亿 MAX_CODES={MAX_CODES}", flush=True)

    # ── 进程内 K 线缓存（底层走磁盘缓存，已全量就位）──
    _orig = kline_mod.fetch_daily_k
    _kcache: dict = {}

    def _cached(code, s, e, *a, **k):
        key = (str(code), str(s), str(e), k.get("adjust", "qfq"))
        if key not in _kcache:
            _kcache[key] = _orig(code, s, e, *a, **k)
        return _kcache[key]

    kline_mod.fetch_daily_k = _cached

    idx_df = _orig("sh.000300", start - timedelta(days=120), end, adjust="qfq")
    idx_df = idx_df.copy()
    idx_df["_dt"] = pd.to_datetime(idx_df["date"])
    idx_df = idx_df.sort_values("_dt").set_index("_dt")
    idx_ret20 = idx_df["close"].pct_change(BOLL_WIN)

    codes = _load_all_codes()
    if MAX_CODES and MAX_CODES > 0:
        codes = codes[:MAX_CODES]
    global_start = start - timedelta(days=120)
    print(f"[预拉K线] {len(codes)} 只, 范围 {global_start}~{end}", flush=True)
    t_pre = time.time()
    ok = 0
    local: dict[str, pd.DataFrame] = {}
    for i, code in enumerate(codes):
        df = None
        for _a in range(2):
            try:
                df = _orig(code, global_start, end, adjust="qfq")
                break
            except Exception:
                if _a == 0:
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
        if (i + 1) % 100 == 0 or (i + 1) == len(codes):
            el = time.time() - t_pre
            eta = (el / (i + 1)) * (len(codes) - i - 1)
            print(f"  [预拉 {i+1}/{len(codes)}] 成功 {ok} 已用 {el:.0f}s 剩余 {eta:.0f}s", flush=True)
        if INTERVAL > 0 and i + 1 < len(codes):
            time.sleep(INTERVAL)

    market_profile = None
    try:
        market_profile = compute_market_profile()
    except Exception as e:
        print(f"[warn] 市场仪表盘失败：{e}")

    print(f"[候选] 生成代理信号（仅一次）...", flush=True)
    candidates = _gen_candidates(local, idx_ret20, market_profile, HOLD_DAYS, TOP_N, MIN_AMOUNT)
    print(f"[候选] 共 {len(candidates)} 个有信号日", flush=True)

    results = []
    for name, kw in SCAN:
        tc = time.time()
        per_day = []
        for sd, sd_tag, sub, caps in candidates:
            try:
                res = run_forward_signal_backtest(
                    sub, hold_days=kw["hold_days"], initial_capital=100000.0,
                    max_positions=200, enable_exits=True, use_signal_bands=True,
                    stop_loss_pct=kw["stop_loss_pct"], take_profit_pct=kw["take_profit_pct"],
                    trailing_stop_pct=kw["trailing_stop_pct"], trend_exit_ma=kw["trend_exit_ma"],
                    size_by="综合评分", capital_scale=caps,
                )
                if res.summary.get("error"):
                    continue
                s = dict(res.summary)
                s["date"] = sd_tag
                per_day.append(s)
            except Exception as e:
                print(f"  [!] {sd_tag} {name} 异常跳过：{e}", flush=True)
                continue
        agg = _aggregate(per_day)
        agg["param_set"] = name
        agg["kwargs"] = str(kw)
        results.append(agg)
        el = time.time() - tc
        print(f"[{name}] 天数={agg['days']} 平均={agg['avg_return']:+.2f}% "
              f"正收益日={agg['win_days_pct']:.0f}% 夏普={agg['sharpe']:.2f} "
              f"回撤={agg['avg_drawdown']:.2f}% 成交={agg['total_trades']} ({el:.0f}s)", flush=True)

    if results:
        out_df = pd.DataFrame(results)[
            ["param_set", "days", "total_trades", "avg_return", "median_return",
             "win_days", "win_days_pct", "win_rate", "avg_drawdown", "sharpe", "kwargs"]
        ]
        out_df.to_csv(STOCK_DATA_DIR / "Param-Scan-results.csv", index=False, encoding="utf-8-sig")
        print("\n[参数扫描] 完成，结果已写入 stock_data/Param-Scan-results.csv")
        print(out_df.to_string(index=False))

    print(f"\n[总耗时] {time.time()-t0:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
