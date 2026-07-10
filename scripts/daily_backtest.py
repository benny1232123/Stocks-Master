#!/usr/bin/env python3
"""每日全策略清单 → 多策略 Backtrader 回测（供 GitHub Actions daily-pick 调用）。

读取最新的 Daily-Action-List-*.csv，提取股票代码与来源策略，
用近 90 天 K 线跑 run_multi_strategy_backtest，结果存为
stock_data/Multi-Backtest-{date}-{summary,equity,trades}.csv。
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

# 确保项目根（scripts/ 的父目录）在 sys.path，便于 `python scripts/daily_backtest.py` 直接运行
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from smcore.artifacts import find_latest_file, PROJECT_ROOT, STOCK_DATA_DIR
from smcore.backtest import run_multi_strategy_backtest

STRAT_MAP = {
    "boll": "boll",
    "relativity": "relativity",
    "theme": "theme",
    "cctv": "cctv",
}


def derive_strategies(source_series: pd.Series) -> str:
    """从来源策略列（如 'Boll/Relativity/Theme'）解析启用的策略集合。"""
    enabled = set()
    for raw in source_series.dropna():
        for part in str(raw).split("/"):
            key = part.strip().lower()
            if key in STRAT_MAP:
                enabled.add(STRAT_MAP[key])
    if not enabled:
        enabled = {"boll", "relativity", "theme"}
    return ",".join(sorted(enabled))


def main() -> int:
    today = date.today()
    latest = find_latest_file("Daily-Action-List-*.csv")
    if latest is None:
        print("未找到 Daily-Action-List CSV，跳过每日回测")
        return 0

    df = pd.read_csv(PROJECT_ROOT / latest.path, encoding="utf-8-sig")
    if df.empty or "股票代码" not in df.columns:
        print("操作清单为空或缺少股票代码列，跳过每日回测")
        return 0

    codes = [str(c).strip() for c in df["股票代码"].dropna().tolist() if str(c).strip()]
    codes = codes[:3000]
    if not codes:
        print("清单无有效股票代码，跳过每日回测")
        return 0

    strategies = derive_strategies(df.get("来源策略", pd.Series(dtype=str)))
    start = today - timedelta(days=90)
    end = today

    print(f"每日回测：{len(codes)} 只股票，区间 {start}~{end}，策略 {strategies}")
    result = run_multi_strategy_backtest(
        codes, start, end,
        initial_capital=100000.0,
        strategies=strategies,
    )

    if result.summary.get("error"):
        print(f"回测未产生有效结果：{result.summary.get('error')}")
        return 0

    date_tag = today.strftime("%Y%m%d")
    base = STOCK_DATA_DIR / f"Multi-Backtest-{date_tag}"

    summary = dict(result.summary)
    summary.pop("data_coverage", None)  # 嵌套结构，不写入单行 CSV
    summary["date"] = date_tag
    summary["codes_count"] = len(codes)
    summary["strategies"] = strategies
    summary["start"] = start.strftime("%Y-%m-%d")
    summary["end"] = end.strftime("%Y-%m-%d")

    pd.DataFrame([summary]).to_csv(f"{base}-summary.csv", index=False, encoding="utf-8-sig")

    # 补算回撤列（引擎返回的 equity 仅含 date/cash/holding_value/total），供前端回撤图展示
    equity = result.equity.copy()
    equity["peak"] = equity["total"].cummax()
    equity["drawdown"] = (equity["total"] - equity["peak"]) / equity["peak"] * 100
    equity.to_csv(f"{base}-equity.csv", index=False, encoding="utf-8-sig")

    result.trades.to_csv(f"{base}-trades.csv", index=False, encoding="utf-8-sig")

    print(f"回测完成：{summary.get('num_trades', 0)} 笔，总收益 {summary.get('total_return')}%，"
          f"最大回撤 {summary.get('max_drawdown')}%，胜率 {summary.get('win_rate')}%")
    print(f"已保存：{base}-summary.csv / -equity.csv / -trades.csv")
    return 0


if __name__ == "__main__":
    sys.exit(main())
