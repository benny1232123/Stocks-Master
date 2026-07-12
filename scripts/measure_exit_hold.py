"""在「已 RS 过滤」的候选宇宙上，扫描 持有天数 × 出场配置 组合，定位最优出场方案。

做法（与 measure_rs_filter.py 同源、复用其预拉与 RS 过滤逻辑）：
- 读全部已完成 10 日窗口的 Daily-Action-List，组成候选宇宙；
- 先用已验证的 RS 过滤（TOL=0.03）剔除跑输大盘的票 → 得到「改进宇宙」；
- 在改进宇宙上，对 (hold_days, exit_variant) 网格做前向回测；
- 对比各组合的 平均收益/胜率/回撤/盈亏比/交易数，挑出最优出场配置。

仅做研究，不改生产文件。
"""
from __future__ import annotations

import glob
import os
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

# 出场配置网格
HOLD_DAYS_LIST = [5, 10, 15, 20]
VARIANTS = {
    "A_current":   dict(stop_loss_pct=0.08, take_profit_pct=0.06, trailing_stop_pct=0.05, trend_exit_ma=60),
    "B_no_fixed_tp": dict(stop_loss_pct=0.08, take_profit_pct=None, trailing_stop_pct=0.05, trend_exit_ma=60),
    "C_no_ma60":   dict(stop_loss_pct=0.08, take_profit_pct=0.06, trailing_stop_pct=0.05, trend_exit_ma=None),
    "D_run_free":  dict(stop_loss_pct=0.08, take_profit_pct=None, trailing_stop_pct=0.08, trend_exit_ma=None),
    "E_tight_stop": dict(stop_loss_pct=0.06, take_profit_pct=0.06, trailing_stop_pct=0.05, trend_exit_ma=60),
}


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
                "建议买入价": r.get("建议买入价"),
                "止损价(下轨)": r.get("止损价(下轨)"),
                "止盈价(上轨)": r.get("止盈价(上轨)"),
            })
    print(f"候选总数: {len(universe)}  唯一标的: {len(unique_codes)}")

    # 采样：全宇宙 1719 只重新拉 K 线需 ~690s，超过前台 600s 上限。
    # 出场/持有配置是「统一施加」，相对排名不依赖全样本，取代表性子集即可。
    # 固定随机种子保证可复现。
    SAMPLE_N = 700
    import random
    _rng = random.Random(20260626)
    _sampled = set(_rng.sample(sorted(unique_codes), min(SAMPLE_N, len(unique_codes))))
    universe = [r for r in universe if r["代码"] in _sampled]
    unique_codes = _sampled
    print(f"[采样] 取 {len(unique_codes)} 只唯一标的做出场/持有扫描（代表性样本，种子=20260626）")

    earliest = min(sd for sd, _ in study) - timedelta(days=60)
    # 对齐已缓存窗口（measure_rs_filter 拉过 max(sd)+30 = 2026-07-26），
    # 否则 covers 检查失败→全部重拉网络→超过前台超时。持有期最大20天所需
    # 数据(≤2026-07-16)在缓存内，回测不受影响。
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

    # RS 过滤（TOL=0.03）→ 改进宇宙
    improved: list[dict] = []
    for rec in universe:
        sd = datetime.strptime(rec["日期"], "%Y-%m-%d").date()
        hit = [s.strip() for s in rec["来源策略"].split("/") if s.strip()]
        stock_ret = _stock_20d_return(rec["代码"], sd, cached_fetch)
        index_ret = _index_20d_return(sd.strftime("%Y%m%d"))
        if _passes_relative_strength_filter(hit, stock_ret, index_ret, tol=RS_TOL):
            improved.append(rec)
    print(f"RS 过滤(TOL={RS_TOL}) 剔除 {len(universe)-len(improved)} → 保留 {len(improved)}")

    universe_by_date: dict[str, list[dict]] = defaultdict(list)
    for rec in improved:
        universe_by_date[rec["日期"]].append(rec)

    def backtest_rows(rows: list[dict], hold_days: int, variant: dict) -> dict | None:
        if not rows:
            return None
        sdf = pd.DataFrame(rows)
        res = run_forward_signal_backtest(
            sdf, hold_days=hold_days, initial_capital=20_000_000.0, max_positions=500,
            slippage=0.001, enable_exits=True, use_signal_bands=True,
            stop_loss_pct=variant["stop_loss_pct"], take_profit_pct=variant["take_profit_pct"],
            trailing_stop_pct=variant["trailing_stop_pct"], trend_exit_ma=variant["trend_exit_ma"],
        )
        return res.summary if "error" not in res.summary else None

    print(f"\n[网格扫描] 改进宇宙 × 持有天数 × 出场配置")
    print(f"{'variant':<14s} {'hold':>4s} {'平均收益%':>9s} {'胜率%':>7s} {'回撤%':>7s} {'盈亏比':>7s} {'交易':>6s}")
    best = None
    for vname, variant in VARIANTS.items():
        for hd in HOLD_DAYS_LIST:
            rows = [r for rs in universe_by_date.values() for r in rs]
            s = backtest_rows(rows, hd, variant)
            if s is None:
                print(f"{vname:<14s} {hd:>4d}  (回测失败)")
                continue
            tag = f"{vname}(tp={variant['take_profit_pct']},trail={variant['trailing_stop_pct']},ma={variant['trend_exit_ma']})"
            print(f"{vname:<14s} {hd:>4d} {s['avg_return']:>9.2f} {s['win_rate']:>7.1f} "
                  f"{s['max_drawdown']:>7.2f} {s['profit_factor']:>7.2f} {s['num_trades']:>6d}")
            score = s["avg_return"]  # 以平均收益为主要优化目标
            if best is None or score > best[0]:
                best = (score, vname, hd, s)
    if best:
        print(f"\n[最优] {best[1]} hold={best[2]} 平均收益={best[3]['avg_return']:.2f}% "
              f"胜率={best[3]['win_rate']:.1f}% 回撤={best[3]['max_drawdown']:.2f}% 盈亏比={best[3]['profit_factor']:.2f} 交易={best[3]['num_trades']}")

    # 6) 按策略分组混合出场：动量保留 MA60，非动量(均值回归类)关 MA60
    def is_momentum(rec):
        return "momentum" in [t.strip().lower() for t in rec["来源策略"].split("/") if t.strip()]

    mom_rows = [r for r in improved if is_momentum(r)]
    non_rows = [r for r in improved if not is_momentum(r)]
    sm = backtest_rows(mom_rows, 10, VARIANTS["A_current"])   # 动量: MA60 开
    sn = backtest_rows(non_rows, 10, VARIANTS["C_no_ma60"])   # 非动量: MA60 关
    print(f"\n[混合出场] 动量子集={len(mom_rows)}笔 非动量子集={len(non_rows)}笔")
    if sm and sn:
        n = sm["num_trades"] + sn["num_trades"]
        comb_avg = (sm["avg_return"] * sm["num_trades"] + sn["avg_return"] * sn["num_trades"]) / n
        comb_win = (sm["win_rate"] * sm["num_trades"] + sn["win_rate"] * sn["num_trades"]) / n
        print(f"  动量(MA60开,均{sm['avg_return']:.2f}%) + 非动量(MA60关,均{sn['avg_return']:.2f}%)")
        print(f"  => 组合 平均收益={comb_avg:.2f}% 胜率={comb_win:.1f}% 交易={n}")
    else:
        if sm:
            print(f"  仅动量可用: 均{sm['avg_return']:.2f}% 胜率{sm['win_rate']:.1f}% 交易{sm['num_trades']}")
        if sn:
            print(f"  仅非动量可用: 均{sn['avg_return']:.2f}% 胜率{sn['win_rate']:.1f}% 交易{sn['num_trades']}")

    # 7) 市场择时：仅沪深300 在 MA60 上方时的信号日才交易（防御性跳过下跌市）
    from smcore.strategy.fusion import _get_hs300_close
    hs = _get_hs300_close()
    up_rows, down_rows = [], []
    if hs is not None:
        hs = hs.sort_index()
        hs_ma60 = hs.rolling(60).mean()
        for r in improved:
            sd = datetime.strptime(r["日期"], "%Y-%m-%d").date()
            ts = pd.Timestamp(sd)
            avail = hs_ma60.index[hs_ma60.index <= ts]
            if len(avail) == 0:
                continue
            last = avail[-1]
            if not pd.isna(hs_ma60.loc[last]) and hs.loc[last] > hs_ma60.loc[last]:
                up_rows.append(r)
            else:
                down_rows.append(r)
        su = backtest_rows(up_rows, 10, VARIANTS["A_current"])
        sdwn = backtest_rows(down_rows, 10, VARIANTS["A_current"])
        print(f"\n[市场择时] 上行市信号日={len(up_rows)}笔 下行市信号日={len(down_rows)}笔")
        if su:
            print(f"  仅上行市(沪深300>MA60): 平均收益={su['avg_return']:.2f}% 胜率={su['win_rate']:.1f}% "
                  f"盈亏比={su['profit_factor']:.2f} 交易={su['num_trades']}")
        if sdwn:
            print(f"  仅下行市(沪深300<MA60): 平均收益={sdwn['avg_return']:.2f}% 胜率={sdwn['win_rate']:.1f}% "
                  f"盈亏比={sdwn['profit_factor']:.2f} 交易={sdwn['num_trades']}")
        if su and sdwn:
            print(f"  => 若跳过下行市只做上行市，组合平均收益从 {best[3]['avg_return']:.2f}% 变为 {su['avg_return']:.2f}%")
        elif su and not sdwn:
            print(f"  => 全部信号日都在上行市，市场闸门无影响")
        elif sdwn and not su:
            print(f"  => 全部信号日都在下行市！市场闸门应跳过整个周期（当前闸门未触发=缺陷）")
    else:
        print("\n[市场择时] 沪深300 序列不可用，跳过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
