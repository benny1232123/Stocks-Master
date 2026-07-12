"""在「RS + 流动性门槛(¥1亿)」宇宙上，测 置信度加权仓位 是否改善组合风险收益。
当前引擎对每日候选等权分仓；若按置信度(综合评分 / 跑赢大盘幅度 / 成交额)加权，
高确定性信号多给仓位，理论上改善风险调整后收益。

权重方案：
- EQUAL    : 等权（size_by=None，当前生产行为）
- SCORE    : 按 综合评分 加权
- RS_MARGIN: 按 个股20日收益−沪深300收益(跑赢幅度) 加权（clip≥0）
- LIQ_LOG  : 按 log(成交额) 加权

指标看 组合总收益(total_return)/夏普/回撤（加权改变资金分配，看组合层而非每信号）。

仅做研究，不改生产文件（引擎 size_by 支持已在 measure 前合入）。
"""
from __future__ import annotations

import glob
import math
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
MIN_AMOUNT = 1e8
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


def main() -> int:
    files = sorted(glob.glob(os.path.join(STUDY_DIR, "Daily-Action-List-*.csv")))
    study = []
    for f in files:
        sd = _signal_date_from_name(f)
        if sd and (TODAY - sd).days >= 15:
            study.append((sd, f))
    if not study:
        print("无已完成窗口的信号日")
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
            universe.append({"日期": sd_str, "代码": code,
                             "综合评分": float(r.get("综合评分", 0) or 0),
                             "来源策略": str(r.get("来源策略", "") or "")})
    print(f"候选总数: {len(universe)}  唯一标的: {len(unique_codes)}")

    _rng = random.Random(RNG_SEED)
    _sampled = set(_rng.sample(sorted(unique_codes), min(SAMPLE_N, len(unique_codes))))
    universe = [r for r in universe if r["代码"] in _sampled]
    unique_codes = _sampled
    print(f"[采样] 取 {len(unique_codes)} 只（种子={RNG_SEED}）")

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
        if done % 100 == 0:
            print(f"  已拉 {done}/{len(unique_codes)}  ({(time.time()-t0):.0f}s)")
    print(f"  K线预拉完成 {(time.time()-t0):.0f}s")

    kline_mod.fetch_daily_k = cached_fetch
    import smcore.backtest.engine as eng_mod
    eng_mod.fetch_daily_k = cached_fetch

    improved: list[dict] = []
    for rec in universe:
        sd = datetime.strptime(rec["日期"], "%Y-%m-%d").date()
        hit = [s.strip() for s in rec["来源策略"].split("/") if s.strip()]
        stock_ret = _stock_20d_return(rec["代码"], sd, cached_fetch)
        index_ret = _index_20d_return(sd.strftime("%Y%m%d"))
        if not _passes_relative_strength_filter(hit, stock_ret, index_ret, tol=RS_TOL):
            continue
        sd_str = rec["日期"]
        kdf = cached_fetch(rec["代码"], sd_str, sd_str)
        amt = None
        if kdf is not None and not kdf.empty:
            a = pd.to_numeric(kdf.iloc[-1].get("amount"), errors="coerce")
            amt = float(a) if not pd.isna(a) else None
        if amt is None or amt < MIN_AMOUNT:
            continue
        rec["stock_ret20"] = stock_ret if stock_ret is not None else 0.0
        rec["index_ret20"] = index_ret if index_ret is not None else 0.0
        rec["amount"] = amt
        rec["score"] = rec["综合评分"]
        improved.append(rec)
    print(f"RS+流动性 保留 {len(improved)}")

    def build_df(rows: list[dict], weight_col: str | None) -> pd.DataFrame:
        out = []
        for r in rows:
            out.append({
                "日期": r["日期"], "代码": r["代码"],
                "stop_price": None, "take_price": None,
                "weight": float(r.get(weight_col, 1.0)) if weight_col else 1.0,
            })
        return pd.DataFrame(out)

    def run_sized(rows: list[dict], weight_col: str | None):
        if not rows:
            return None
        sdf = build_df(rows, weight_col)
        res = run_forward_signal_backtest(
            sdf, hold_days=HOLD, initial_capital=20_000_000.0, max_positions=500,
            slippage=0.001, enable_exits=True, use_signal_bands=True,
            stop_loss_pct=CURRENT_EXIT["stop_loss_pct"], take_profit_pct=CURRENT_EXIT["take_profit_pct"],
            trailing_stop_pct=CURRENT_EXIT["trailing_stop_pct"], trend_exit_ma=CURRENT_EXIT["trend_exit_ma"],
            size_by=("weight" if weight_col else None),
        )
        return res.summary if "error" not in res.summary else None

    print(f"\n[置信度加权] 基线=RS+流动性  hold={HOLD}")
    print(f"{'方案':<10s} {'总收益%':>8s} {'每信号%':>8s} {'胜率%':>7s} {'夏普':>6s} {'回撤%':>7s} {'盈亏比':>7s} {'交易':>6s}")
    schemes = {
        "EQUAL": None,
        "SCORE": "score",
        "RS_MARGIN": "rs_margin",
        "LIQ_LOG": "liq_log",
    }
    # 预计算每个 rec 的权重列
    for r in improved:
        r["rs_margin"] = max(r["stock_ret20"] - r["index_ret20"], 0.0)
        r["liq_log"] = math.log(max(r["amount"], 1.0)) if r["amount"] > 0 else 0.0
    base = run_sized(improved, None)
    if base is None:
        print("基线回测失败")
        return 1
    print(f"{'EQUAL':<10s} {base['total_return']:>8.2f} {base['avg_return']:>8.2f} {base['win_rate']:>7.1f} "
          f"{base['sharpe']:>6.2f} {base['max_drawdown']:>7.2f} {base['profit_factor']:>7.2f} {base['num_trades']:>6d}")
    for name, col in schemes.items():
        if name == "EQUAL":
            continue
        s = run_sized(improved, col)
        if s is None:
            print(f"{name:<10s}  (回测失败)")
            continue
        dtr = s["total_return"] - base["total_return"]
        dsh = s["sharpe"] - base["sharpe"]
        print(f"{name:<10s} {s['total_return']:>8.2f} {s['avg_return']:>8.2f} {s['win_rate']:>7.1f} "
              f"{s['sharpe']:>6.2f} {s['max_drawdown']:>7.2f} {s['profit_factor']:>7.2f} {s['num_trades']:>6d}  "
              f"Δ总收益{dtr:+.2f} Δ夏普{dsh:+.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
