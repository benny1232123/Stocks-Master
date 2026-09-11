#!/usr/bin/env python3
"""按策略族测量最优持有期（训练/验证分离，回答「持有天数该不该分档、怎么分」）。

方法（对应重构文档 §17/§31 纪律）：
  1. 读全部历史 Daily-Action-List，按「来源策略」把信号拆成 5 个策略桶
     （一行命中多策略 → 进入每个命中桶，衡量"该族独挑"的组合）；
  2. 每桶 × hold 网格，用**当前引擎**（次日开盘买/T+1/自适应止损止盈）跑全期前向回测；
  3. 按信号日排序切 70% 训练 / 30% 验证：训练段挑最优 hold，验证段只做验收；
  4. 只有「验证段均值 ≥ 当前全局 12 日在验证段的表现」时才给出分档建议。

输出 stock_data/measure_reports/hold_by_family.json + 控制台表格。纯读历史 + 本地 K 线。
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd

from smcore.artifacts import STOCK_DATA_DIR
from smcore.backtest.engine import run_forward_signal_backtest

STRATEGIES = ["boll", "theme", "relativity", "momentum", "cctv"]
HOLD_GRID = [5, 7, 10, 12, 14, 20]
CURRENT_GLOBAL_HOLD = 12
TRAIN_FRAC = 0.7
OUT_DIR = STOCK_DATA_DIR / "measure_reports"


def load_signals() -> pd.DataFrame:
    frames = []
    for dal in sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv")):
        tag = dal.stem.replace("Daily-Action-List-", "")
        if not tag.isdigit():
            continue
        try:
            df = pd.read_csv(dal, encoding="utf-8-sig", dtype=str)
        except Exception:
            continue
        if df.empty or "股票代码" not in df.columns or "日期" not in df.columns:
            continue
        df = df[["日期", "股票代码", "来源策略"]].copy()
        df["来源策略"] = df.get("来源策略", "").fillna("")
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["日期", "代码"])
    out = pd.concat(frames, ignore_index=True)
    out = out.rename(columns={"股票代码": "代码"})
    out["日期"] = pd.to_datetime(out["日期"], errors="coerce")
    out = out.dropna(subset=["日期", "代码"]).reset_index(drop=True)
    return out


def run_bucket(signals: pd.DataFrame, hold: int) -> pd.DataFrame | None:
    sub = pd.DataFrame({
        "日期": signals["日期"].dt.strftime("%Y-%m-%d"),
        "代码": signals["代码"],
    })
    try:
        res = run_forward_signal_backtest(
            sub, hold_days=hold, initial_capital=1_000_000, max_positions=200,
            enable_exits=True, use_signal_bands=False, size_by=None,
        )
    except Exception as exc:
        print(f"    引擎异常: {exc}")
        return None
    if res.trades is None or res.trades.empty:
        return None
    t = res.trades.copy()
    t["buy_date"] = pd.to_datetime(t["buy_date"])
    return t


def split_stats(trades: pd.DataFrame, cut: pd.Timestamp) -> dict:
    def _stats(x: pd.DataFrame) -> dict:
        if x.empty:
            return {"n": 0, "avg_return": None, "win_rate": None}
        return {
            "n": int(len(x)),
            "avg_return": round(float(x["return_pct"].mean()), 3),
            "win_rate": round(float((x["return_pct"] > 0).mean() * 100), 1),
        }
    tr = trades[trades["buy_date"] < cut]
    va = trades[trades["buy_date"] >= cut]
    return {"train": _stats(tr), "validation": _stats(va)}


def main() -> int:
    t0 = time.time()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    all_sig = load_signals()
    if all_sig.empty:
        print("无可用历史清单")
        return 1
    dates = sorted(all_sig["日期"].dt.date.unique())
    cut_idx = int(len(dates) * TRAIN_FRAC)
    cut = pd.Timestamp(dates[min(cut_idx, len(dates) - 1)])
    print(f"信号日 {dates[0]} ~ {dates[-1]}（{len(dates)} 天），训练/验证切分点 = {cut}")

    report: dict = {"cut_date": str(cut.date()), "hold_grid": HOLD_GRID, "buckets": {}}
    for strat in STRATEGIES:
        sig = all_sig[all_sig["来源策略"].fillna("").str.lower().str.contains(strat)]
        if sig.empty:
            continue
        # 同日同码去重（多策略命中会在多桶重复出现，桶内唯一）
        sig = sig.drop_duplicates(subset=["日期", "代码"])
        print(f"\n[{strat}] 信号 {len(sig)} 行（{sig['日期'].dt.date.nunique()} 天）")
        bucket: dict = {"n_signals": int(len(sig))}
        for hold in HOLD_GRID:
            trades = run_bucket(sig, hold)
            stats = split_stats(trades, cut) if trades is not None else {"train": {"n": 0}, "validation": {"n": 0}}
            bucket[str(hold)] = stats
            print(f"  hold={hold:>2}: 训练 {stats['train']} | 验证 {stats['validation']}")
            time.sleep(0.5)

        # 训练段挑最优（均值收益），验证段验收
        valid_holds = [h for h in HOLD_GRID if bucket[str(h)]["train"]["n"] >= 10]
        if valid_holds:
            best_tr = max(valid_holds, key=lambda h: bucket[str(h)]["train"]["avg_return"] or -9e9)
            val_best = bucket[str(best_tr)]["validation"]["avg_return"]
            val_cur = bucket[str(CURRENT_GLOBAL_HOLD)]["validation"]["avg_return"]
            cur_in_train = bucket[str(CURRENT_GLOBAL_HOLD)]["train"]["avg_return"]
            confirm = (val_best is not None and val_cur is not None and val_best >= val_cur)
            bucket["recommendation"] = {
                "train_best_hold": best_tr,
                "validation_mean_at_best": val_best,
                "validation_mean_at_current12": val_cur,
                "validated": bool(confirm and best_tr != CURRENT_GLOBAL_HOLD),
                "suggested_hold": best_tr if (confirm and best_tr != CURRENT_GLOBAL_HOLD) else CURRENT_GLOBAL_HOLD,
                "note": "训练段最优且验证段不劣于全局 12 日才建议分档；否则维持全局",
            }
            if cur_in_train is not None:
                bucket["recommendation"]["train_mean_at_current12"] = cur_in_train
        bucket["recommendation"] = bucket.get("recommendation", {"note": "样本不足，维持全局"})
        report["buckets"][strat] = bucket

    out = OUT_DIR / "hold_by_family.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n报告已写 {out}（耗时 {time.time() - t0:.0f}s）")

    print("\n════ 建议汇总（仅验证通过才建议分档）════")
    for strat, b in report["buckets"].items():
        rec = b.get("recommendation", {})
        print(f"  {strat:<11s} 建议 hold = {rec.get('suggested_hold', CURRENT_GLOBAL_HOLD)}"
              f"（validated={rec.get('validated', False)}）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
