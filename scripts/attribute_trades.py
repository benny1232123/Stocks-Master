#!/usr/bin/env python3
"""离线交易归因 —— 对已落盘的 Multi-Backtest-*-trades.csv 做多维归因，零联网秒级。

回答「+1.45% 的单笔毛收益从哪来、到哪去」：
- 来源策略：每笔交易按信号日 DAL 的「来源策略」归因（多策略命中计入每桶）
- 出场原因：exit_reason 桶（分批止盈的两批分别统计）
- 价格段 / 综合评分段 / 持有天数 / 止损宽度
- 每桶输出 n / 胜率 / 毛均值 / 费后均值（费=佣金万2.5min5双边+印花税千0.5，滑点已在成交价内）

用法：python scripts/attribute_trades.py [--out stock_data/Attribution-latest.md]
"""
from __future__ import annotations

import argparse
import glob
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from smcore.artifacts import STOCK_DATA_DIR  # noqa: E402

_COMM = 0.00025
_COMM_MIN = 5.0
_STAMP = 0.0005


def _fee_drag(buy_price: float, qty: int) -> float:
    """双边费用占买入成本的百分比（滑点已含在成交价里，此处只算佣金+印花税）。"""
    buy_amt = buy_price * qty
    sell_amt = buy_price * qty  # 近似：用买入额估卖出额
    fee_buy = max(buy_amt * _COMM, _COMM_MIN)
    fee_sell = max(sell_amt * _COMM, _COMM_MIN) + sell_amt * _STAMP
    return (fee_buy + fee_sell) / buy_amt * 100


def load() -> pd.DataFrame:
    rows = []
    for f in sorted(glob.glob(str(STOCK_DATA_DIR / "Multi-Backtest-*-trades.csv"))):
        m = re.search(r"Multi-Backtest-(\d{8})-trades\.csv$", f)
        if not m:
            continue
        tag = m.group(1)
        try:
            t = pd.read_csv(f, encoding="utf-8-sig")
        except Exception:
            continue
        if t.empty:
            continue
        # 信号日 DAL 归因表
        dal_path = STOCK_DATA_DIR / f"Daily-Action-List-{tag}.csv"
        meta: dict[str, dict] = {}
        if dal_path.exists():
            try:
                d = pd.read_csv(dal_path, encoding="utf-8-sig", dtype={"股票代码": str})
                for _, r in d.iterrows():
                    code = str(r.get("股票代码", "")).strip().zfill(6)
                    meta[code] = {
                        "来源策略": str(r.get("来源策略", "")),
                        "综合评分": pd.to_numeric(r.get("综合评分"), errors="coerce"),
                        "dal买入价": pd.to_numeric(r.get("建议买入价"), errors="coerce"),
                        "dal止损价": pd.to_numeric(r.get("止损价(下轨)"), errors="coerce"),
                        "dal止盈价": pd.to_numeric(r.get("止盈价(上轨)"), errors="coerce"),
                        "因子分": pd.to_numeric(r.get("因子分"), errors="coerce"),
                    }
            except Exception:
                pass
        for _, r in t.iterrows():
            code = str(r.get("code", "")).strip().zfill(6)
            m_ = meta.get(code, {})
            buy = pd.to_numeric(r.get("buy_price"), errors="coerce")
            qty = pd.to_numeric(r.get("qty"), errors="coerce")
            sd = pd.to_datetime(tag, format="%Y%m%d")
            bd = pd.to_datetime(r.get("buy_date"), errors="coerce")
            sdl = pd.to_datetime(r.get("sell_date"), errors="coerce")
            stop = m_.get("dal止损价")
            rows.append({
                "tag": tag,
                "code": code,
                "return_pct": pd.to_numeric(r.get("return_pct"), errors="coerce"),
                "exit_reason": r.get("exit_reason", "?"),
                "buy_price": buy,
                "qty": qty,
                "fee_drag": _fee_drag(float(buy), int(qty)) if pd.notna(buy) and pd.notna(qty) and qty > 0 else None,
                "strategies": m_.get("来源策略", ""),
                "score": m_.get("综合评分"),
                "factor": m_.get("因子分"),
                "stop_width": (buy - stop) / buy * 100 if pd.notna(buy) and stop is not None and pd.notna(stop) and buy > 0 else None,
                "hold_days": (sdl - sd).days if pd.notna(sdl) else None,
                "month": sd.strftime("%Y-%m"),
            })
    return pd.DataFrame(rows)


def _bucket_stats(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """按 key 分组统计：n/胜率/毛均值/费后均值/中位/最差。多策略列支持 '/' 拆分。"""
    out = []
    if key == "strategies":
        # 多策略命中：拆到每个策略桶；无匹配归 unknown
        exploded = []
        for _, r in df.iterrows():
            parts = [p.strip().lower() for p in str(r["strategies"]).split("/") if p.strip()]
            for p in parts or ["unknown"]:
                exploded.append({**r.to_dict(), "bucket": p})
        g = pd.DataFrame(exploded).groupby("bucket")["return_pct"]
    else:
        g = df.groupby(key)["return_pct"]
    for name, s in g:
        s = s.dropna()
        if s.empty:
            continue
        cost = df.loc[s.index, "fee_drag"].astype(float).mean() if key != "strategies" else \
            pd.DataFrame(exploded).groupby("bucket")["fee_drag"].mean().get(name)
        out.append({
            "bucket": name, "n": len(s),
            "win%": round((s > 0).mean() * 100, 1),
            "gross%": round(s.mean(), 2),
            "net%": round(s.mean() - (cost or 0.35), 2),
            "med%": round(s.median(), 2),
            "worst%": round(s.min(), 2),
        })
    return pd.DataFrame(out).sort_values("net%", ascending=False)


def _print_table(title: str, stats: pd.DataFrame) -> None:
    print(f"\n── {title} " + "─" * max(1, 50 - len(title)))
    if stats.empty:
        print("  (无样本)")
        return
    print(stats.to_string(index=False))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=None, help="同时把报告写到该 markdown 路径")
    args = ap.parse_args()

    df = load()
    if df.empty:
        print("无可归因交易（Multi-Backtest-*-trades.csv 缺失或为空）")
        return 1
    df = df.dropna(subset=["return_pct"])
    net_all = df["return_pct"].mean() - df["fee_drag"].astype(float).mean()
    print(f"样本：{len(df)} 笔（{df['tag'].min()} ~ {df['tag'].max()}），"
          f"毛均值 {df['return_pct'].mean():.2f}%，费后均值 {net_all:.2f}%，"
          f"胜率 {(df['return_pct'] > 0).mean() * 100:.1f}%")

    lines = [f"# 交易归因报告（{datetime.now().strftime('%Y-%m-%d')}）\n",
             f"样本 {len(df)} 笔，毛均值 {df['return_pct'].mean():.2f}%，费后均值 {net_all:.2f}%\n"]

    def emit(title: str, stats: pd.DataFrame, key: str):
        _print_table(title, stats)
        if args.out and not stats.empty:
            lines.append(f"\n## {title}\n")
            lines.append(stats.to_markdown(index=False))

    emit("按来源策略（多命中拆分计入）", _bucket_stats(df, "strategies"), "strategies")
    emit("按出场原因", _bucket_stats(df, "exit_reason"), "exit_reason")

    p = df.dropna(subset=["buy_price"]).copy()
    p["价格段"] = pd.cut(p["buy_price"], [0, 8, 15, 25, 50, 1e9],
                        labels=["<8", "8-15", "15-25", "25-50", ">50"])
    emit("按买入价格段", _bucket_stats(p, "价格段"), "价格段")

    s = df.dropna(subset=["score"]).copy()
    s["评分段"] = pd.cut(s["score"], [0, 25, 30, 35, 40, 100],
                        labels=["<25", "25-30", "30-35", "35-40", ">=40"])
    emit("按综合评分段", _bucket_stats(s, "评分段"), "评分段")

    h = df.dropna(subset=["hold_days"]).copy()
    h["持有段"] = pd.cut(h["hold_days"], [0, 5, 8, 11, 15, 99],
                        labels=["<=5", "6-8", "9-11", "12-15", ">15"])
    emit("按实际持有天数", _bucket_stats(h, "持有段"), "持有段")

    w = df.dropna(subset=["stop_width"]).copy()
    w["止损宽度"] = pd.cut(w["stop_width"], [0, 3, 6, 9, 100],
                          labels=["<3%", "3-6%", "6-9%", ">=9%"])
    emit("按止损宽度(买入价距下轨)", _bucket_stats(w, "止损宽度"), "止损宽度")

    emit("按月份（regime 敏感性）", _bucket_stats(df, "month"), "month")

    # 策略 × 出场 交叉：每桶平均收益，找出「哪类信号被哪种出场毁掉」
    cross_rows = []
    exploded = []
    for _, r in df.iterrows():
        parts = [pp.strip().lower() for pp in str(r["strategies"]).split("/") if pp.strip()]
        for pp in parts or ["unknown"]:
            exploded.append({"bucket": pp, "exit_reason": r["exit_reason"], "return_pct": r["return_pct"]})
    ex = pd.DataFrame(exploded)
    if not ex.empty:
        piv = ex.pivot_table(index="bucket", columns="exit_reason", values="return_pct", aggfunc="mean").round(2)
        cnt = ex.pivot_table(index="bucket", columns="exit_reason", values="return_pct", aggfunc="count")
        print("\n── 策略×出场 平均收益%（括号内为笔数） " + "─" * 30)
        print(piv.to_string())
        print("\n笔数：")
        print(cnt.to_string())
        if args.out:
            lines.append("\n## 策略×出场 平均收益%\n")
            lines.append(piv.to_markdown())
            lines.append("\n\n## 策略×出场 笔数\n")
            lines.append(cnt.to_markdown())

    if args.out:
        Path(args.out).write_text("\n".join(lines), encoding="utf-8")
        print(f"\n[已写出] {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
