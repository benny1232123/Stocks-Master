"""在「已 RS 过滤」的候选宇宙上，头对头测试「信号质量过滤器」，定位能真正抬升每信号期望的 alpha。

过滤器（均叠加在 RS TOL=0.03 宇宙之上，互相独立、不冲突任何策略逻辑）：
- VC  (量能确认)    : 信号日成交量 >= 1.5 × 前 20 日均值（机构建仓/资金介入信号）
- RSR (相对强度排名): 同信号日内按 20 日相对收益排前 50%（在已胜出大盘的票里再集中最强者）
- LIQ (流动性门槛)  : 信号日成交额位于同信号日前 50%（剔除流动性差的票，避免难出场/庄股陷阱）

每组用「按信号日独立回测」(A_current 出场, 持有10日) 比较 平均收益/胜率/盈亏比/交易数。
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
RS_TOL = 0.03  # 已定稿默认
SAMPLE_N = 700
SEED = 20260626
VOL_RATIO_TH = 1.5  # 量能确认阈值


def _signal_date_from_name(path: str) -> date | None:
    name = os.path.basename(path)
    suffix = name.replace("Daily-Action-List-", "").replace(".csv", "")
    if len(suffix) == 8 and suffix.isdigit():
        return date(int(suffix[:4]), int(suffix[4:6]), int(suffix[6:8]))
    return None


def _stock_20d_return(df: pd.DataFrame, sd: date) -> float | None:
    """df 已切片到 [sd-45, sd]。返回信号日前 ~20 交易日收益。"""
    if df is None or len(df) < 22:
        return None
    close = pd.to_numeric(df["close"], errors="coerce").dropna()
    if len(close) < 22:
        return None
    prev = close.iloc[-22]
    if prev == 0 or pd.isna(prev):
        return None
    return float(close.iloc[-1]) / float(prev) - 1


def _signal_day_metrics(df: pd.DataFrame) -> dict | None:
    """从切片到 [sd-45, sd] 的 K 线取信号日量能指标。"""
    if df is None or len(df) < 22:
        return None
    close = pd.to_numeric(df["close"], errors="coerce")
    volume = pd.to_numeric(df["volume"], errors="coerce")
    amount = pd.to_numeric(df["amount"], errors="coerce")
    if close.isna().all() or volume.isna().all():
        return None
    vol_today = float(volume.iloc[-1])
    # 前 20 个交易日均值（不含信号日）
    prior = volume.iloc[-22:-2]
    prior = prior[~prior.isna()]
    if len(prior) < 10:
        return None
    vol_avg = float(prior.mean())
    vol_ratio = (vol_today / vol_avg) if vol_avg > 0 else None
    amt_today = float(amount.iloc[-1]) if not amount.isna().all() else float(vol_today * close.iloc[-1])
    return {"vol_ratio": vol_ratio, "amount": amt_today, "close": float(close.iloc[-1])}


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

    _rng = random.Random(SEED)
    _sampled = set(_rng.sample(sorted(unique_codes), min(SAMPLE_N, len(unique_codes))))
    universe = [r for r in universe if r["代码"] in _sampled]
    unique_codes = _sampled
    print(f"[采样] 取 {len(unique_codes)} 只唯一标的（代表性样本，种子={SEED}）")

    earliest = min(sd for sd, _ in study) - timedelta(days=60)
    latest = max(sd for sd, _ in study) + timedelta(days=30)
    cache: dict[str, pd.DataFrame] = {}

    def cached_fetch(code6, start, end, adjust="qfq"):
        if code6 not in cache or cache[code6] is None or (code6 in cache and cache[code6].empty):
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

    # RS 过滤（TOL=0.03）→ 改进宇宙，并附信号日量能/RS 指标
    improved: list[dict] = []
    for rec in universe:
        sd = datetime.strptime(rec["日期"], "%Y-%m-%d").date()
        hit = [s.strip() for s in rec["来源策略"].split("/") if s.strip()]
        sub = cached_fetch(rec["代码"], (sd - timedelta(days=45)).strftime("%Y-%m-%d"), sd.strftime("%Y-%m-%d"))
        stock_ret = _stock_20d_return(sub, sd)
        index_ret = _index_20d_return(sd.strftime("%Y%m%d"))
        if _passes_relative_strength_filter(hit, stock_ret, index_ret, tol=RS_TOL):
            m = _signal_day_metrics(sub)
            rec["rs_20d"] = stock_ret
            rec["vol_ratio"] = m["vol_ratio"] if m else None
            rec["amount"] = m["amount"] if m else None
            improved.append(rec)
    print(f"RS 过滤(TOL={RS_TOL}) 剔除 {len(universe)-len(improved)} → 保留 {len(improved)}")

    by_date: dict[str, list[dict]] = defaultdict(list)
    for rec in improved:
        by_date[rec["日期"]].append(rec)

    # 流动性门槛：同信号日内按 amount 排前 50% 作为「流动性合格」
    amt_th_by_date: dict[str, float] = {}
    all_amts = [r["amount"] for r in improved if r["amount"] is not None]
    if all_amts:
        s = pd.Series(all_amts)
        print(f"[流动性分布] amount 中位数={s.median():.0f} p25={s.quantile(.25):.0f} p75={s.quantile(.75):.0f} "
              f"(单位同 baostock amount)")
    for d, rows in by_date.items():
        amts = [r["amount"] for r in rows if r["amount"] is not None]
        amt_th_by_date[d] = (pd.Series(amts).median() if amts else float("inf"))

    def filt(rec, kind: str) -> bool:
        if kind == "baseline":
            return True
        if kind == "VC":
            return rec["vol_ratio"] is not None and rec["vol_ratio"] >= VOL_RATIO_TH
        if kind == "RSR":
            # 同信号日内 rs_20d 前 50%
            if rec["rs_20d"] is None:
                return False
            d = rec["日期"]
            rs_vals = sorted([r["rs_20d"] for r in by_date[d] if r["rs_20d"] is not None])
            if not rs_vals:
                return False
            med = rs_vals[len(rs_vals) // 2]
            return rec["rs_20d"] >= med
        if kind == "LIQ":
            return rec["amount"] is not None and rec["amount"] >= amt_th_by_date.get(rec["日期"], float("inf"))
        if kind == "LIQ1e8":
            return rec["amount"] is not None and rec["amount"] >= 1e8
        if kind == "LIQ5e7":
            return rec["amount"] is not None and rec["amount"] >= 5e7
        if kind == "VC+LIQ":
            return filt(rec, "VC") and filt(rec, "LIQ")
        if kind == "VC+RSR":
            return filt(rec, "VC") and filt(rec, "RSR")
        if kind == "VC+RSR+LIQ":
            return filt(rec, "VC") and filt(rec, "RSR") and filt(rec, "LIQ")
        return False

    def backtest_rows(rows: list[dict]) -> dict | None:
        if not rows:
            return None
        sdf = pd.DataFrame(rows)
        res = run_forward_signal_backtest(
            sdf, hold_days=10, initial_capital=20_000_000.0, max_positions=500,
            slippage=0.001, enable_exits=True, use_signal_bands=True,
            stop_loss_pct=0.08, take_profit_pct=0.06, trailing_stop_pct=0.05, trend_exit_ma=60,
        )
        return res.summary if "error" not in res.summary else None

    KINDS = ["baseline", "LIQ", "LIQ1e8", "LIQ5e7", "VC", "RSR", "VC+LIQ"]
    print(f"\n[信号质量过滤器] 改进宇宙 × 过滤器（A_current 出场, 持有10日）")
    print(f"{'过滤器':<12s} {'保留':>5s} {'平均收益%':>9s} {'胜率%':>7s} {'盈亏比':>7s} {'交易':>6s}")
    results = {}
    for kind in KINDS:
        rows = [r for r in improved if filt(r, kind)]
        s = backtest_rows(rows)
        if s is None:
            print(f"{kind:<12s} {len(rows):>5d}  (回测失败)")
            continue
        results[kind] = s
        print(f"{kind:<12s} {len(rows):>5d} {s['avg_return']:>9.2f} {s['win_rate']:>7.1f} "
              f"{s['profit_factor']:>7.2f} {s['num_trades']:>6d}")
    if "baseline" in results:
        base = results["baseline"]
        print(f"\n[相对基线 Δ] (基线 均{base['avg_return']:.2f}% 胜{base['win_rate']:.1f}% 盈亏比{base['profit_factor']:.2f})")
        for kind in KINDS:
            if kind == "baseline" or kind not in results:
                continue
            s = results[kind]
            print(f"  {kind:<12s} Δ收益={s['avg_return']-base['avg_return']:>+6.2f}  "
                  f"Δ胜率={s['win_rate']-base['win_rate']:>+5.1f}  Δ盈亏比={s['profit_factor']-base['profit_factor']:>+5.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
