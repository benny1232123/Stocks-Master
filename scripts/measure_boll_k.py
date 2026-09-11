#!/usr/bin/env python3
"""布林 k 的分 regime 敏感性测量（为 k_by_vol_regime 配置提供数据）。

方法：对历史 DAL 中来源含 boll 的每一次信号（code, signal_date），
  1. 用本地 K 线重算信号日各 k ∈ {1.5, 1.645, 2.0} 下的布林下轨；
  2. 判定该 k 下「近下轨/破下轨」入场条件是否触发（close ≤ lower × near_ratio）；
  3. 前向收益 = 次日开盘 → +12 个交易日收盘（与生产持有期一致口径）；
  4. 信号日按沪深300 20 日年化波动率分位切成 low/mid/high 三档；
  5. 输出 (k × regime) 的触发数 / 均值前向收益 / 胜率 → 填 k_by_vol_regime 的依据。

判定纪律：某 regime 下某 k 的触发数 < 30 视为样本不足，不建议采用；
只有「均值差 ≥ 0.3pp 且触发数足够」才建议该 regime 换 k。
输出 stock_data/measure_reports/boll_k.json。纯本地数据、离线可跑。
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

from smcore.artifacts import STOCK_DATA_DIR
from smcore.data.kline import fetch_daily_k
from smcore.strategy.regime_filter import _get_hs300_close

K_GRID = [1.5, 1.645, 2.0]
WINDOW = 20
NEAR_RATIO = 1.015          # 近下轨判定（与生产 near_ratio 一致）
HOLD_BARS = 12              # 前向持有（交易日 bar）
MIN_SAMPLES = 30            # 单格最少触发数
OUT_DIR = STOCK_DATA_DIR / "measure_reports"
REGILES = ["low", "mid", "high"]


def vol_regime_series() -> pd.Series | None:
    """沪深300 20 日年化波动率 → 全历史分位 → low/mid/high 三档（按日期索引）。"""
    s = _get_hs300_close()
    if s is None or len(s) < 260:
        return None
    s = s.sort_index()
    ret = s.pct_change()
    vol = ret.rolling(20).std() * np.sqrt(252)
    pct = vol.rolling(250, min_periods=120).apply(lambda x: (x <= x.iloc[-1]).mean(), raw=False)
    lab = pd.cut(pct, bins=[-0.01, 0.33, 0.66, 1.01], labels=REGILES)
    return lab.dropna()


def main() -> int:
    t0 = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) 收集 boll 来源的历史信号（code, 信号日）
    rows: list[tuple[str, pd.Timestamp]] = []
    for dal in sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv")):
        tag = dal.stem.replace("Daily-Action-List-", "")
        if not tag.isdigit():
            continue
        try:
            df = pd.read_csv(dal, encoding="utf-8-sig", dtype=str)
        except Exception:
            continue
        if df.empty or "股票代码" not in df.columns:
            continue
        strat = df.get("来源策略", pd.Series([""] * len(df))).fillna("").str.lower()
        sel = df[strat.str.contains("boll")]
        if sel.empty:
            continue
        dts = pd.to_datetime(tag, format="%Y%m%d", errors="coerce")
        if pd.isna(dts):
            continue
        for code in sel["股票代码"].dropna():
            rows.append((str(code).strip(), dts))
    rows = sorted(set(rows))
    print(f"boll 历史信号 {len(rows)} 条")
    if not rows:
        return 1

    regime = vol_regime_series()
    if regime is None:
        print("WARN: 波动率 regime 序列不可得（指数数据不足），按无分档统计")
    trade_cal: dict[str, pd.DatetimeIndex] = {}

    # (k, regime_label) → [前向收益]
    rets: dict[tuple[float, str], list[float]] = {(k, r): [] for k in K_GRID for r in REGILES + ["all"]}
    triggered: dict[tuple[float, str], int] = {(k, r): 0 for k in K_GRID for r in REGILES + ["all"]}
    done = 0
    cache: dict[str, pd.DataFrame] = {}

    for code, sd in rows:
        df = cache.get(code)
        if df is None:
            try:
                df = fetch_daily_k(code, (sd - pd.Timedelta(days=200)).strftime("%Y-%m-%d"),
                                   (sd + pd.Timedelta(days=60)).strftime("%Y-%m-%d"), adjust="qfq")
            except Exception:
                df = None
            if df is None or df.empty:
                df = pd.DataFrame()
            cache[code] = df
        if df.empty:
            continue
        dts = pd.to_datetime(df["date"], errors="coerce")
        df = df.assign(_dt=dts).dropna(subset=["_dt"]).set_index("_dt").sort_index()
        pos = df.index.searchsorted(sd, side="right")  # 信号日次日（买入 bar）
        if pos < 1 or pos + HOLD_BARS >= len(df):
            continue
        window = df["close"].iloc[max(0, pos - WINDOW):pos]
        if len(window) < WINDOW:
            continue
        mean = float(window.mean())
        std = float(window.std(ddof=0))
        if std <= 0:
            continue
        close_at = float(df["close"].iloc[pos - 1])  # 信号日收盘
        entry = float(df["open"].iloc[pos])
        exitp = float(df["close"].iloc[pos + HOLD_BARS])
        if entry <= 0 or exitp != exitp:
            continue
        fwd = exitp / entry - 1

        r_label = "all"
        if regime is not None:
            labs = regime[regime.index <= sd]
            if len(labs):
                r_label = str(labs.iloc[-1])

        done += 1
        for k in K_GRID:
            lower = mean - k * std
            if close_at <= lower * NEAR_RATIO:  # 触发近下轨/破下轨入场
                rets[(k, r_label)].append(fwd)
                rets[(k, "all")].append(fwd)
        if done % 100 == 0:
            print(f"  已处理 {done}/{len(rows)}")

    # 汇总
    report: dict = {
        "k_grid": K_GRID, "near_ratio": NEAR_RATIO, "hold_bars": HOLD_BARS,
        "signals_processed": done,
        "min_samples": MIN_SAMPLES,
        "grid": {},
    }
    print("\n════ k × 波动 regime（近下轨触发 → 次日开盘买持有 12 bar）════")
    print(f"{'regime':>6} {'k':>6} {'触发数':>7} {'均值%':>8} {'胜率%':>8}")
    for r in REGILES + ["all"]:
        for k in K_GRID:
            xs = rets[(k, r)]
            n = len(xs)
            avg = float(np.mean(xs) * 100) if n else None
            win = float(np.mean([x > 0 for x in xs]) * 100) if n else None
            report["grid"][f"{r}|{k}"] = {
                "n": n,
                "avg_return_pct": round(avg, 3) if avg is not None else None,
                "win_rate_pct": round(win, 1) if win is not None else None,
                "enough_samples": n >= MIN_SAMPLES,
            }
            print(f"{r:>6} {k:>6} {n:>7} {avg if avg is not None else float('nan'):>8.3f} {win if win is not None else float('nan'):>8.1f}")

    # 建议：每 regime 内比较 k 与基线 1.645
    suggestion: dict[str, object] = {}
    for r in REGILES:
        base = report["grid"].get(f"{r}|1.645", {})
        best_k, best_avg, ok = 1.645, None, False
        for k in K_GRID:
            g = report["grid"][f"{r}|{k}"]
            if not g["enough_samples"] or g["avg_return_pct"] is None:
                continue
            if best_avg is None or g["avg_return_pct"] > best_avg:
                best_k, best_avg, ok = k, g["avg_return_pct"], True
        base_avg = base.get("avg_return_pct")
        if ok and base_avg is not None and best_k != 1.645 and best_avg - base_avg >= 0.3:
            suggestion[r] = {"k": best_k, "avg_diff_pp": round(best_avg - base_avg, 2),
                             "note": "均值优势 ≥0.3pp 且样本足够，可试点配置 k_by_vol_regime"}
        else:
            suggestion[r] = {"k": 1.645, "note": "无稳定优势，维持 1.645"}
    report["suggestion"] = suggestion
    print("\n════ 建议汇总（均值优势 ≥0.3pp 且触发数达标才建议换 k）════")
    for r, s in suggestion.items():
        print(f"  {r:>5}: k = {s['k']}  {s['note']}")

    out = OUT_DIR / "boll_k.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n报告已写 {out}（耗时 {time.time() - t0:.0f}s）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
