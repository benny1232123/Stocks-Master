#!/usr/bin/env python3
"""价格带 5~30 元静态过滤的验证测量（全样本价格桶前向收益）。

背景：价格带 5~30 是全系统最大的静态宇宙过滤器，从未被验证。
本脚本从本地 K 线缓存抽样全市场股票，按历史各月月初的价格分桶，
统计桶内「次日开盘买入持有 12 个交易日」的前向收益，回答：
  30 元以上的股票是否系统性更差（过滤合理）还是被误杀（过滤过严）？

注意口径：测的是**全市场无条件收益**，不是"策略选中后的收益"——
它回答"这个价格段整体有没有 alpha"，不能完全替代"策略在不设价格上限时
会不会选得更好"。结论用于校准价格带上限，不用于直接改策略。

输出 stock_data/measure_reports/price_band.json。纯本地数据、离线可跑。
"""
from __future__ import annotations

import json
import random
import sys
import time
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd

from smcore.artifacts import STOCK_DATA_DIR
from smcore.data.kline import list_kline_codes, read_kline_cache

SAMPLE_N = 1200          # 抽样股票数（0=全量；种子固定可复现）
HOLD_BARS = 12           # 前向持有（交易日 bar）
BANDS = [(0, 5), (5, 10), (10, 20), (20, 30), (30, 50), (50, 100), (100, 1e9)]
BAND_LABELS = ["<5", "5-10", "10-20", "20-30", "30-50", "50-100", ">100"]
OUT_DIR = STOCK_DATA_DIR / "measure_reports"


def signal_dates(index_range: pd.DatetimeIndex) -> list[pd.Timestamp]:
    """每月第一个可用交易日作为信号日。"""
    if len(index_range) == 0:
        return []
    s = pd.Series(index_range, index=index_range)
    monthly = s.groupby([index_range.year, index_range.month]).first()
    return list(monthly)


def main() -> int:
    t0 = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    codes = list_kline_codes()
    if not codes:
        print("本地 K 线缓存为空")
        return 1
    random.seed(20260912)
    if SAMPLE_N and len(codes) > SAMPLE_N:
        codes = random.sample(codes, SAMPLE_N)
    print(f"抽样 {len(codes)} 只本地缓存股票")

    # 先读一只确定全局日期轴
    per_code: dict[str, pd.DataFrame] = {}
    all_idx: set[pd.Timestamp] = set()
    for i, code in enumerate(codes):
        try:
            df = read_kline_cache(code, base_dir=STOCK_DATA_DIR / "k_data")
        except Exception:
            continue
        if df is None or df.empty or "date" not in df.columns or "close" not in df.columns:
            continue
        dts = pd.to_datetime(df["date"], errors="coerce")
        df = df.assign(_dt=dts).dropna(subset=["_dt"]).set_index("_dt").sort_index()
        if len(df) < HOLD_BARS + 5:
            continue
        per_code[code] = df
        all_idx.update(df.index)
        if (i + 1) % 200 == 0:
            print(f"  已读 {i + 1}/{len(codes)}")
    if not per_code:
        print("无有效 K 线")
        return 1

    idx = pd.DatetimeIndex(sorted(all_idx))
    # 信号日范围：留出前向窗口，取近 24 个月
    end = idx[-1] - pd.Timedelta(days=HOLD_BARS * 1.6)
    start = max(idx[0], end - pd.DateOffset(months=24))
    sdates = [d for d in signal_dates(idx[(idx >= start) & (idx <= end)])]
    print(f"信号日 {len(sdates)} 个（{sdates[0].date()} ~ {sdates[-1].date()}）")

    pos = {c: df.index for c, df in per_code.items()}
    stats: dict[str, dict] = {lab: {"n": 0, "sum_ret": 0.0, "wins": 0} for lab in BAND_LABELS}
    per_date_band: dict[str, dict] = {}

    for sd in sdates:
        next_positions: dict[str, tuple[float, float]] = {}
        for code, df in per_code.items():
            pos_idx = pos[code]
            i = pos_idx.searchsorted(sd, side="right")  # 信号日次日
            if i >= len(df) or i + HOLD_BARS >= len(df):
                continue
            if (df.index[i] - sd).days > 10:  # 停牌跨月：次日太远，跳过
                continue
            entry = float(df["open"].iloc[i])
            exitp = float(df["close"].iloc[i + HOLD_BARS])
            if entry <= 0 or exitp != exitp:
                continue
            next_positions[code] = (entry, exitp)
        for code, (entry, exitp) in next_positions.items():
            band = next(lab for lab, (lo, hi) in zip(BAND_LABELS, BANDS) if lo <= entry < hi)
            ret = exitp / entry - 1
            stats[band]["n"] += 1
            stats[band]["sum_ret"] += ret
            stats[band]["wins"] += 1 if ret > 0 else 0
            per_date_band.setdefault(str(sd.date()), {}).setdefault(band, []).append(ret)

    report: dict = {
        "sample_codes": len(per_code),
        "signal_dates": [str(d.date()) for d in sdates],
        "hold_bars": HOLD_BARS,
        "bands": {},
    }
    print("\n════ 价格桶 × 次日开盘买持有 12 bar（全市场无条件）════")
    print(f"{'桶':>8} {'n':>8} {'均值%':>8} {'胜率%':>8}")
    for lab in BAND_LABELS:
        st = stats[lab]
        n = st["n"]
        avg = (st["sum_ret"] / n * 100) if n else None
        win = (st["wins"] / n * 100) if n else None
        report["bands"][lab] = {
            "n": n,
            "avg_return_pct": round(avg, 3) if avg is not None else None,
            "win_rate_pct": round(win, 1) if win is not None else None,
        }
        print(f"{lab:>8} {n:>8} {avg if avg is not None else float('nan'):>8.3f} {win if win is not None else float('nan'):>8.1f}")

    # 结论：30+ 桶 vs 5-30 桶
    b30 = report["bands"]["30-50"]
    b50 = report["bands"]["50-100"]
    in_band = [report["bands"][l] for l in ("5-10", "10-20", "20-30")]
    in_n = sum(b["n"] for b in in_band)
    in_avg = sum(b["avg_return_pct"] * b["n"] for b in in_band if b["avg_return_pct"] is not None) / in_n if in_n else None
    out_bands = [b for b in (b30, b50, report["bands"][">100"]) if b["n"] > 0]
    out_n = sum(b["n"] for b in out_bands)
    out_avg = (sum(b["avg_return_pct"] * b["n"] for b in out_bands) / out_n) if out_n and all(b["avg_return_pct"] is not None for b in out_bands) else None
    verdict = "样本不足，无法下结论"
    if in_avg is not None and out_avg is not None:
        diff = out_avg - in_avg
        if diff < -0.3:
            verdict = f"带外(≥30元)均值比带内低 {abs(diff):.2f}pp/期 → 现行 30 元上限**合理**"
        elif diff > 0.3:
            verdict = f"带外(≥30元)均值比带内高 {diff:.2f}pp/期 → 上限可能在**误杀强势价格段**，建议做策略级复验"
        else:
            verdict = f"带内外差异 {diff:+.2f}pp/期 → 不显著，上限影响中性"
    report["verdict"] = verdict
    print(f"\n结论: {verdict}")

    out = OUT_DIR / "price_band.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"报告已写 {out}（耗时 {time.time() - t0:.0f}s）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
