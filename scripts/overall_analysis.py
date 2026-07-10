"""总体策略信号分析：聚合近一个月所有 Daily-Action-List 与回测结果。

输出：
  - 每日信号数、策略分布
  - 跨日共识度最高的标的（多次命中）
  - 前向回测汇总（收益/回撤/胜率/夏普）
  - 市场风格（Boll超卖 vs 相对强弱占比）
用法：python scripts/overall_analysis.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STOCK_DATA_DIR = PROJECT_ROOT / "stock_data"


def load_all_lists() -> tuple[pd.DataFrame, list[str]]:
    rows = []
    dates = []
    for p in sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv")):
        d = p.stem.replace("Daily-Action-List-", "")
        dates.append(d)
        try:
            df = pd.read_csv(p, encoding="utf-8-sig")
        except Exception:
            continue
        df["_date"] = d
        rows.append(df)
    if not rows:
        return pd.DataFrame(), dates
    return pd.concat(rows, ignore_index=True), dates


def load_backtests() -> pd.DataFrame:
    rows = []
    for p in sorted(STOCK_DATA_DIR.glob("Multi-Backtest-*-summary.csv")):
        d = p.stem.replace("Multi-Backtest-", "").replace("-summary", "")
        try:
            df = pd.read_csv(p, encoding="utf-8-sig")
        except Exception:
            continue
        df["_date"] = d
        rows.append(df)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def main() -> int:
    all_df, dates = load_all_lists()
    bt = load_backtests()

    print("=" * 60)
    print("📊 近一个月策略信号总体分析")
    print("=" * 60)
    if all_df.empty:
        print("无日报数据")
        return 0

    print(f"\n【覆盖范围】")
    print(f"  交易日数: {len(dates)}  ({dates[0]} ~ {dates[-1]})")
    print(f"  信号总数: {len(all_df)} 条")

    # 每日信号数
    per_day = all_df.groupby("_date").size()
    print(f"\n【每日信号数】")
    for d, n in per_day.items():
        print(f"  {d}: {n} 只")

    # 策略分布
    print(f"\n【策略命中分布】")
    strat = all_df["来源策略"].value_counts()
    for name, cnt in strat.items():
        print(f"  {name}: {cnt} 次 ({cnt/len(all_df)*100:.1f}%)")

    # 共识标的（跨多个信号日出现）
    print(f"\n【跨日共识度最高的标的（多次命中 = 强信号）】")
    code_grp = all_df.groupby("股票代码")
    consensus = []
    for code, g in code_grp:
        days = g["_date"].nunique()
        if days >= 2:
            name = g["股票名称"].iloc[0]
            strats = "/".join(sorted(set("/".join(g["来源策略"]).split("/"))))
            consensus.append((days, code, name, strats, len(g)))
    consensus.sort(reverse=True)
    if not consensus:
        print("  (无跨日重复标的)")
    for days, code, name, strats, hits in consensus[:15]:
        print(f"  {code} {name} · 命中 {days} 天 · 策略[{strats}] · 累计 {hits} 次")

    # 前向回测汇总
    if not bt.empty:
        print(f"\n【前向信号回测汇总（{len(bt)} 个信号日）】")
        tot_ret = pd.to_numeric(bt["total_return"], errors="coerce")
        dd = pd.to_numeric(bt["max_drawdown"], errors="coerce")
        wr = pd.to_numeric(bt["win_rate"], errors="coerce")
        sh = pd.to_numeric(bt["sharpe"], errors="coerce")
        nt = pd.to_numeric(bt["num_trades"], errors="coerce")
        print(f"  平均总收益: {tot_ret.mean():.2f}%  (中位 {tot_ret.median():.2f}%)")
        print(f"  平均最大回撤: {dd.mean():.2f}%  (中位 {dd.median():.2f}%)")
        print(f"  平均胜率: {wr.mean():.1f}%  (中位 {wr.median():.1f}%)")
        print(f"  平均夏普: {sh.mean():.2f}  (中位 {sh.median():.2f})")
        print(f"  平均交易数: {nt.mean():.1f} 笔")
        print(f"  正收益天数: {(tot_ret > 0).sum()}/{len(bt)}")
        print(f"\n  最优/最差信号日:")
        bt2 = bt.copy()
        bt2["ret"] = tot_ret
        best = bt2.loc[bt2["ret"].idxmax()]
        worst = bt2.loc[bt2["ret"].idxmin()]
        print(f"    最佳 {best['_date']}: {best['ret']:.2f}% 回撤 {best['max_drawdown']}% 胜率 {best['win_rate']}%")
        print(f"    最差 {worst['_date']}: {worst['ret']:.2f}% 回撤 {worst['max_drawdown']}% 胜率 {worst['win_rate']}%")

    print("\n" + "=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
