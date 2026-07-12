"""在「已 RS 过滤 + 流动性门槛(¥1亿)」的改进宇宙上，再测一组信号质量过滤器，
找下一个能抬升每信号期望的 alpha。口径与 measure_signal_quality.py 一致。

测试过滤器（均叠加在 RS+liquidity 之上）：
- BASE        : RS(TOL=0.03) + 流动性(¥1亿) 基线
- GAPDN       : 剔除信号日跳空低开 >3% 的票（恐慌/续跌风险）
- MINPRICE5   : 剔除信号日收盘 <¥5 的低价股（退市/操纵/难出场）
- GAPDN+PRICE : 两者叠加
- VC15        : 要求信号日成交量 > 20日均量×1.5（放量确认，缩量假信号剔除）
- VC15+GAP+PR : 三者叠加

仅做研究，不改生产文件。
"""
from __future__ import annotations

import glob
import os
import random
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

os.environ.setdefault("KLINE_BACKEND", "baostock")

from smcore.backtest.engine import run_forward_signal_backtest
import smcore.data.kline as kline_mod
from smcore.strategy.fusion import _passes_relative_strength_filter, _index_20d_return
from smcore.utils.code import format_stock_code

STUDY_DIR = "stock_data"
TODAY = date.today()
RS_TOL = 0.03
MIN_AMOUNT = 1e8  # 已定稿流动性门槛 ¥1亿
SAMPLE_N = 700
RNG_SEED = 20260626

CURRENT_EXIT = dict(stop_loss_pct=0.08, take_profit_pct=0.06, trailing_stop_pct=0.05, trend_exit_ma=60)
HOLD = 10


def _signal_date_from_name(path: str) -> date | None:
    name = os.path.basename(path)
    suffix = name.replace("Daily-Action-List-", "").replace(".csv", "")
    if len(suffix) == 8 and suffix.isdigit():
        return date(int(suffix[:4]), int(suffix[4:6]), int(suffix[6:8]))
    return None


def _stock_20d_return(code: str, sd: date, cached_fetch) -> float | None:
    end = sd.strftime("%Y-%m-%d")
    start = (sd - timedelta(days=45)).strftime("%Y-%m-%d")
    df = cached_fetch(code, start, end)
    if df is None or len(df) < 22:
        return None
    close = pd.to_numeric(df["close"], errors="coerce").dropna()
    if len(close) < 22:
        return None
    prev = close.iloc[-22]
    if prev == 0 or pd.isna(prev):
        return None
    return float(close.iloc[-1]) / float(prev) - 1


def _signal_day_features(code: str, sd: date, cached_fetch) -> dict | None:
    """返回信号日特征：缺口%、收盘价、量比。取不到返回 None。"""
    sd_str = sd.strftime("%Y-%m-%d")
    start = (sd - timedelta(days=25)).strftime("%Y-%m-%d")
    df = cached_fetch(code, start, sd_str)
    if df is None or len(df) < 2:
        return None
    close = pd.to_numeric(df["close"], errors="coerce")
    open_ = pd.to_numeric(df["open"], errors="coerce")
    vol = pd.to_numeric(df["volume"], errors="coerce")
    if close.dropna().empty or open_.dropna().empty or vol.dropna().empty:
        return None
    # 取信号日当天（最后一行）与上一行
    last = df.iloc[-1]
    prev = df.iloc[-2]
    sig_close = float(pd.to_numeric(last["close"], errors="coerce"))
    sig_open = float(pd.to_numeric(last["open"], errors="coerce"))
    prev_close = float(pd.to_numeric(prev["close"], errors="coerce"))
    if sig_close <= 0 or prev_close <= 0:
        return None
    gap = sig_open / prev_close - 1.0
    # 量比：信号日量 / 前20日均量（排除信号日自身）
    win = vol.iloc[:-1].tail(20)
    avg_vol = float(win.mean()) if len(win) >= 5 and not pd.isna(win.mean()) else float(vol.iloc[-1])
    vol_ratio = float(vol.iloc[-1]) / avg_vol if avg_vol > 0 else 1.0
    return {"gap": gap, "close": sig_close, "vol_ratio": vol_ratio}


def main() -> int:
    files = sorted(glob.glob(os.path.join(STUDY_DIR, "Daily-Action-List-*.csv")))
    study = []
    for f in files:
        sd = _signal_date_from_name(f)
        if sd and (TODAY - sd).days >= 15:
            study.append((sd, f))
    if not study:
        print("无已完成窗口的信号日可供测量")
        return 1
    print(f"测量信号日数: {len(study)}  ({study[0][0]} ~ {study[-1][0]})")

    universe: list[dict] = []
    unique_codes: set[str] = set()
    for sd, f in study:
        df = pd.read_csv(f)
        sd_str = sd.strftime("%Y-%m-%d")
        for _, r in df.iterrows():
            code = format_stock_code(r.get("股票代码", ""))
            if not code:
                continue
            unique_codes.add(code)
            universe.append({
                "日期": sd_str, "代码": code,
                "综合评分": float(r.get("综合评分", 0) or 0),
                "来源策略": str(r.get("来源策略", "") or ""),
            })
    print(f"候选总数: {len(universe)}  唯一标的: {len(unique_codes)}")

    _rng = random.Random(RNG_SEED)
    _sampled = set(_rng.sample(sorted(unique_codes), min(SAMPLE_N, len(unique_codes))))
    universe = [r for r in universe if r["代码"] in _sampled]
    unique_codes = _sampled
    print(f"[采样] 取 {len(unique_codes)} 只唯一标的（种子={RNG_SEED}）")

    earliest = min(sd for sd, _ in study) - timedelta(days=60)
    latest = max(sd for sd, _ in study) + timedelta(days=30)
    cache: dict[str, pd.DataFrame] = {}

    def cached_fetch(code6, start, end, adjust="qfq"):
        if code6 not in cache or cache[code6] is None or cache[code6].empty:
            cache[code6] = kline_mod.fetch_daily_k(code6, earliest, latest, adjust)
        df = cache[code6]
        if df is None or df.empty:
            return df
        mask = (df["date"] >= str(start)) & (df["date"] <= str(end))
        return df.loc[mask].copy()

    print("预拉 K 线 ...")
    t0 = time.time()
    done = 0
    for code in sorted(unique_codes):
        cached_fetch(code, earliest, latest)
        done += 1
        if done % 50 == 0:
            print(f"  已拉 {done}/{len(unique_codes)}  ({(time.time()-t0):.0f}s)")
    print(f"  K线预拉完成 {(time.time()-t0):.0f}s")

    kline_mod.fetch_daily_k = cached_fetch
    import smcore.backtest.engine as eng_mod
    eng_mod.fetch_daily_k = cached_fetch

    # 构建 RS + 流动性 基线宇宙，并预计算每个信号日特征
    improved: list[dict] = []
    for rec in universe:
        sd = datetime.strptime(rec["日期"], "%Y-%m-%d").date()
        hit = [s.strip() for s in rec["来源策略"].split("/") if s.strip()]
        stock_ret = _stock_20d_return(rec["代码"], sd, cached_fetch)
        index_ret = _index_20d_return(sd.strftime("%Y%m%d"))
        if not _passes_relative_strength_filter(hit, stock_ret, index_ret, tol=RS_TOL):
            continue
        # 流动性门槛：取信号日成交额
        feat = _signal_day_features(rec["代码"], sd, cached_fetch)
        if feat is None:
            continue
        # 取信号日成交额（amount 列）
        sd_str = rec["日期"]
        kdf = cached_fetch(rec["代码"], sd_str, sd_str)
        amt = None
        if kdf is not None and not kdf.empty:
            a = pd.to_numeric(kdf.iloc[-1].get("amount"), errors="coerce")
            amt = float(a) if not pd.isna(a) else None
        if amt is None or amt < MIN_AMOUNT:
            continue
        rec["gap"] = feat["gap"]
        rec["close"] = feat["close"]
        rec["vol_ratio"] = feat["vol_ratio"]
        improved.append(rec)
    print(f"RS+流动性 保留 {len(improved)}  (剔除 {len(universe)-len(improved)})")

    universe_by_date: dict[str, list[dict]] = defaultdict(list)
    for rec in improved:
        universe_by_date[rec["日期"]].append(rec)

    def backtest_rows(rows: list[dict]) -> dict | None:
        if not rows:
            return None
        sdf = pd.DataFrame(rows)
        res = run_forward_signal_backtest(
            sdf, hold_days=HOLD, initial_capital=20_000_000.0, max_positions=500,
            slippage=0.001, enable_exits=True, use_signal_bands=True,
            stop_loss_pct=CURRENT_EXIT["stop_loss_pct"], take_profit_pct=CURRENT_EXIT["take_profit_pct"],
            trailing_stop_pct=CURRENT_EXIT["trailing_stop_pct"], trend_exit_ma=CURRENT_EXIT["trend_exit_ma"],
        )
        return res.summary if "error" not in res.summary else None

    def passes(rec, kind: str) -> bool:
        if kind == "BASE":
            return True
        if kind == "GAPDN":
            return rec["gap"] >= -0.03
        if kind == "MINPRICE5":
            return rec["close"] >= 5.0
        if kind == "GAPDN+PRICE":
            return rec["gap"] >= -0.03 and rec["close"] >= 5.0
        if kind == "VC15":
            return rec["vol_ratio"] >= 1.5
        if kind == "VC15+GAP+PR":
            return rec["vol_ratio"] >= 1.5 and rec["gap"] >= -0.03 and rec["close"] >= 5.0
        return True

    KINDS = ["BASE", "GAPDN", "MINPRICE5", "GAPDN+PRICE", "VC15", "VC15+GAP+PR"]
    print(f"\n[信号质量过滤器] 基线=RS+流动性  hold={HOLD} 出场=当前生产配置")
    print(f"{'过滤器':<14s} {'保留':>5s} {'平均收益%':>9s} {'胜率%':>7s} {'盈亏比':>7s} {'交易':>6s} {'Δ收益':>8s}")
    base_s = backtest_rows([r for rs in universe_by_date.values() for r in rs])
    if base_s is None:
        print("基线回测失败")
        return 1
    print(f"{'BASE':<14s} {len(improved):>5d} {base_s['avg_return']:>9.2f} {base_s['win_rate']:>7.1f} "
          f"{base_s['profit_factor']:>7.2f} {base_s['num_trades']:>6d} {'—':>8s}")
    for kind in KINDS[1:]:
        rows = [r for r in improved if passes(r, kind)]
        s = backtest_rows(rows)
        if s is None:
            print(f"{kind:<14s} {len(rows):>5d}  (回测失败/空集)")
            continue
        delta = s["avg_return"] - base_s["avg_return"]
        print(f"{kind:<14s} {len(rows):>5d} {s['avg_return']:>9.2f} {s['win_rate']:>7.1f} "
              f"{s['profit_factor']:>7.2f} {s['num_trades']:>6d} {delta:>+8.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
