#!/usr/bin/env python3
"""组合连续权益口径报告（20260905 新增，参考 vectorbt/qlib 的单曲线评估口径）。

背景：Multi-Backtest-{date}-summary.csv 是「每个信号日独立 10 万」的事件研究口径，
AGGREGATE 拼接后的日均收益/夏普是「对 82 个 sleeve 求均值再平均」，不是组合的资金
曲线语义。本报告把全部 sleeve 的权益曲线按日历日叠加成一条组合曲线（每个信号日
按固定规模开一组仓位、滚动重叠），计算连续口径的累计收益/年化/夏普/最大回撤，
并给出同期沪深300 对照（qlib 式 excess return）。

用法：python scripts/portfolio_curve_report.py
输出：stock_data/Portfolio-Curve-latest.csv（date, portfolio_equity, active_sleeves,
      hs300_norm）+ 控制台摘要。
"""
from __future__ import annotations

import argparse
import glob
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from smcore.artifacts import STOCK_DATA_DIR  # noqa: E402
from smcore.strategy.regime_filter import _get_hs300_close  # noqa: E402

_RISK_FREE = 0.015  # 年化无风险利率（夏普扣减），A 股口径取 1.5%


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(STOCK_DATA_DIR / "Portfolio-Curve-latest.csv"))
    args = ap.parse_args()

    sleeves = {}
    for f in glob.glob(str(STOCK_DATA_DIR / "Multi-Backtest-*-equity.csv")):
        m = re.search(r"Multi-Backtest-(\d{8})-equity", f)
        try:
            eq = pd.read_csv(f, encoding="utf-8-sig")
        except Exception:
            continue
        if eq.empty or "total" not in eq.columns or "date" not in eq.columns:
            continue
        eq["date"] = pd.to_datetime(eq["date"])
        # 裁剪到该 sleeve 的实际持仓窗口：部分 equity 文件带指标预热期（信号日前
        # 只是空仓 100k，个别文件甚至含多年历史），不裁剪会把组合曲线起点拖偏。
        sig = pd.Timestamp(m.group(1))
        try:
            hold = int(pd.read_csv(f.replace("-equity", "-summary"), encoding="utf-8-sig").iloc[0]["hold_days"])
        except Exception:
            hold = 12
        eq = eq[(eq["date"] >= sig) & (eq["date"] <= sig + pd.Timedelta(days=hold + 7))]
        if eq.empty:
            continue
        sleeves[m.group(1)] = eq.set_index("date")["total"]
    if not sleeves:
        print("未找到 Multi-Backtest-*-equity.csv，先跑 daily_backtest")
        return 1

    # 正确的组合口径：每条 sleeve 归一为「日收益率序列」，组合日收益 = 当日在场
    # sleeve 的等权平均，再累乘成曲线。直接对权益绝对值求和会把新信号日的资金
    # 注入当成收益、把 sleeve 到期退出算成回撤（已实测得出年化 +952% 的荒谬值）。
    all_rets = pd.DataFrame({tag: s.sort_index().pct_change() for tag, s in sleeves.items()})
    port_ret = all_rets.mean(axis=1).fillna(0.0)  # mean 自动跳过当日不在场的 sleeve
    active = all_rets.notna().sum(axis=1)
    port = (1 + port_ret).cumprod()

    total_ret = float(port.iloc[-1] / port.iloc[0] - 1)
    days = (port.index[-1] - port.index[0]).days
    years = max(days / 365.25, 1e-9)
    ann = (1 + total_ret) ** (1 / years) - 1
    rf_daily = _RISK_FREE / 252
    std = port_ret.std()
    sharpe = float((port_ret.mean() - rf_daily) / std * np.sqrt(252)) if std and std > 0 else 0.0
    peak = port.cummax()
    max_dd = float((port / peak - 1).min())

    print(f"组合连续口径（{len(sleeves)} 个 sleeve，{port.index[0].date()} ~ {port.index[-1].date()}，"
          f"日均在场 sleeve {active.mean():.1f} 个）：")
    print(f"  累计收益 {total_ret*100:+.2f}% | 年化 {ann*100:+.2f}% | 夏普(扣rf) {sharpe:+.3f} | 最大回撤 {max_dd*100:.2f}%")

    # 同期 HS300 对照（qlib 式 excess）
    hs = _get_hs300_close()
    if hs is not None and len(hs) > 1:
        hs2 = hs[(hs.index >= port.index[0]) & (hs.index <= port.index[-1])]
        if len(hs2) > 1:
            bench_ret = hs2.iloc[-1] / hs2.iloc[0] - 1
            brets = hs2.pct_change().dropna()
            bsharpe = float((brets.mean() - rf_daily) / brets.std() * np.sqrt(252)) if brets.std() > 0 else 0.0
            print(f"  同期沪深300: 累计 {bench_ret*100:+.2f}% | 超额 {(total_ret-bench_ret)*100:+.2f}pp | 基准夏普 {bsharpe:+.3f}")
            hs_norm = (hs2 / hs2.iloc[0]).reindex(port.index).ffill()
        else:
            hs_norm = None
    else:
        hs_norm = None
        print("  （HS300 数据不可用，跳过基准对照）")

    out = pd.DataFrame({"portfolio_equity": port, "active_sleeves": active})
    if hs_norm is not None:
        out["hs300_norm"] = hs_norm
    out.index.name = "date"
    out.to_csv(args.out, encoding="utf-8-sig")
    print(f"[已写出] {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
