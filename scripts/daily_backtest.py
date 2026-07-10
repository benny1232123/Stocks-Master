#!/usr/bin/env python3
"""每日全策略清单 → 前向信号回测（供 GitHub Actions daily-pick 调用）。

语义：锁定历史上某一天产生的 Daily-Action-List 信号清单（signal_date），
从该信号日的次日开盘买入、持有 HOLD_DAYS 天后卖出，回测这段「往后」的真实表现。
即「从历史某天开始策略 → 往后回测」，而非在过去 N 天里重跑策略引擎重新派生信号。

逻辑：
1. 找最近的若干个「信号日 + 持有期 ≤ 今天」的 Daily-Action-List（前向窗口已走完，数据真实）。
2. 合并为信号序列喂给 run_forward_signal_backtest（按日盯市）。
3. 结果存 stock_data/Multi-Backtest-{信号日}-{summary,equity,trades}.csv，
   命名带信号日，便于追溯「这是哪天的信号、往后持有 N 天的真实结果」。
"""
from __future__ import annotations

import os
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

# 确保项目根（scripts/ 的父目录）在 sys.path，便于 `python scripts/daily_backtest.py` 直接运行
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from smcore.artifacts import PROJECT_ROOT, STOCK_DATA_DIR
from smcore.backtest import run_forward_signal_backtest

STRAT_MAP = {
    "boll": "boll",
    "relativity": "relativity",
    "theme": "theme",
    "cctv": "cctv",
}


def _parse_signal_date(name: str) -> date | None:
    m = re.search(r"(\d{8})", name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d").date()
    except ValueError:
        return None


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


def collect_eligible_lists(hold_days: int, max_lists: int) -> list[tuple[Path, date]]:
    """返回 (路径, 信号日) 列表，仅含「信号日 + hold_days <= 今天」的清单（前向窗口已走完）。"""
    cands: list[tuple[Path, date]] = []
    for path in STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"):
        sd = _parse_signal_date(path.name)
        if sd is None:
            continue
        if sd + timedelta(days=hold_days) <= date.today():
            cands.append((path, sd))
    # 最新信号日优先
    cands.sort(key=lambda x: x[1], reverse=True)
    if not cands:
        # 回退：若没有任何清单走完持有期（如刚启动），取最新清单做部分前向回测
        for path in STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"):
            sd = _parse_signal_date(path.name)
            if sd is not None:
                cands.append((path, sd))
        cands.sort(key=lambda x: x[1], reverse=True)
    return cands[:max_lists]


def main() -> int:
    hold_days = int(os.environ.get("HOLD_DAYS", "10"))
    max_lists = int(os.environ.get("MAX_LISTS", "20"))
    today = date.today()

    lists = collect_eligible_lists(hold_days, max_lists)
    if not lists:
        print("未找到 Daily-Action-List CSV，跳过每日回测")
        return 0

    frames: list[pd.DataFrame] = []
    for path, sd in lists:
        df = pd.read_csv(path, encoding="utf-8-sig")
        if df.empty or "股票代码" not in df.columns:
            continue
        codes = [str(c).strip() for c in df["股票代码"].dropna().tolist() if str(c).strip()]
        if not codes:
            continue
        sub = pd.DataFrame({"日期": [sd.strftime("%Y-%m-%d")] * len(codes), "代码": codes})
        if "建议买入价" in df.columns:
            sub["建议买入价"] = df["建议买入价"].values[: len(codes)]
        frames.append(sub)

    if not frames:
        print("候选清单均无有效股票代码，跳过每日回测")
        return 0

    signals = pd.concat(frames, ignore_index=True)
    # 同一代码同一信号日只保留一次
    signals = signals.drop_duplicates(subset=["日期", "代码"])

    # 汇总来源策略（用于展示）
    src_frames = []
    for path, _ in lists:
        d = pd.read_csv(path, encoding="utf-8-sig")
        if "来源策略" in d.columns:
            src_frames.append(d["来源策略"])
    strategies = derive_strategies(pd.concat(src_frames, ignore_index=True)) if src_frames else "boll,relativity,theme"

    sig_start = lists[-1][1].strftime("%Y-%m-%d")
    sig_end = lists[0][1].strftime("%Y-%m-%d")
    print(f"每日前向回测：{len(lists)} 个历史信号日，共 {len(signals)} 条信号，"
          f"信号区间 {sig_start}~{sig_end}，持有 {hold_days} 天往后回测")
    result = run_forward_signal_backtest(
        signals,
        hold_days=hold_days,
        initial_capital=100000.0,
        max_positions=200,
    )

    if result.summary.get("error"):
        print(f"回测未产生有效结果：{result.summary.get('error')}")
        return 0

    # 命名带信号日（最新信号日），便于追溯
    date_tag = lists[0][1].strftime("%Y%m%d")
    base = STOCK_DATA_DIR / f"Multi-Backtest-{date_tag}"

    summary = dict(result.summary)
    summary.pop("data_coverage", None)
    summary["date"] = date_tag
    summary["run_date"] = today.strftime("%Y%m%d")
    summary["signal_start"] = sig_start
    summary["signal_end"] = sig_end
    summary["hold_days"] = hold_days
    summary["signals_days"] = len(lists)
    summary["codes_count"] = len(signals)
    summary["strategies"] = strategies
    summary["start"] = sig_start
    summary["end"] = (lists[0][1] + timedelta(days=hold_days)).strftime("%Y-%m-%d")

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
