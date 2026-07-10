#!/usr/bin/env python3
"""每日全策略清单 → 前向信号回测（供 GitHub Actions daily-pick 调用）。

语义：对每个历史信号日（默认近 LOOKBACK_DAYS=30 天）独立做「前向信号回测」——
锁定该信号日产生的 Daily-Action-List，从信号日次日开盘买入、持有
min(HOLD_DAYS, 距今天数) 天后卖出，回测这段「往后」的真实表现。
即「从历史某天开始策略 → 往后回测」，而非在过去 N 天里重跑策略引擎重新派生信号。

每天 CI 运行时：
- 对「窗口已走完」（信号日 + 持有期 ≤ 今天）的清单做完整 HOLD_DAYS 天回测；
- 对「窗口未走完」的近期信号（如最近几天）做部分前向回测（持有到今天），
  结果随日期延长逐步更新，直到持有期满；
最终形成「从一个月前每天开始」的一系列前向回测，每天都在滚动积累。

每个信号日独立存档：stock_data/Multi-Backtest-{信号日}-{summary,equity,trades}.csv，
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

# 回测只取综合评分最高的前 TOP_N 只，避免信号过多把资金摊成数百个迷你仓位、
# 导致权益曲线近乎水平（「曲线不动」）。TOP_N 个等权仓位每只约 initial/TOP_N，曲线才能看出涨跌。
TOP_N = 30


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


def collect_eligible_lists(lookback_days: int) -> list[tuple[Path, date]]:
    """返回 (路径, 信号日) 列表，仅含「信号日 >= 今天 - lookback_days」的清单（旧→新排序）。"""
    cutoff = date.today() - timedelta(days=lookback_days)
    cands: list[tuple[Path, date]] = []
    for path in STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"):
        sd = _parse_signal_date(path.name)
        if sd is None:
            continue
        if sd >= cutoff:
            cands.append((path, sd))
    cands.sort(key=lambda x: x[1])
    return cands


def _backtest_one(path: Path, sd: date, hold_days: int) -> dict | None:
    """对单个信号日做前向回测并落盘，返回摘要信息；无有效结果返回 None。"""
    df = pd.read_csv(path, encoding="utf-8-sig")
    if df.empty or "股票代码" not in df.columns:
        return None
    # 按综合评分取前 TOP_N，避免信号过多导致仓位被摊薄、权益曲线近乎不动
    if "综合评分" in df.columns:
        df = df.copy()
        df["_s"] = pd.to_numeric(df["综合评分"], errors="coerce")
        df = df.sort_values("_s", ascending=False)
        if len(df) > TOP_N:
            df = df.head(TOP_N)
        df = df.drop(columns=["_s"])
    codes = [str(c).strip() for c in df["股票代码"].dropna().tolist() if str(c).strip()]
    if not codes:
        return None

    sub = pd.DataFrame({"日期": [sd.strftime("%Y-%m-%d")] * len(codes), "代码": codes})
    if "建议买入价" in df.columns:
        sub["建议买入价"] = df["建议买入价"].values[: len(codes)]

    strategies = derive_strategies(df["来源策略"]) if "来源策略" in df.columns else "boll,relativity,theme"

    result = run_forward_signal_backtest(
        sub,
        hold_days=hold_days,
        initial_capital=100000.0,
        max_positions=200,
    )
    if result.summary.get("error"):
        return None

    date_tag = sd.strftime("%Y%m%d")
    base = STOCK_DATA_DIR / f"Multi-Backtest-{date_tag}"

    summary = dict(result.summary)
    summary.pop("data_coverage", None)
    summary["date"] = date_tag
    summary["run_date"] = date.today().strftime("%Y%m%d")
    summary["signal_start"] = sd.strftime("%Y-%m-%d")
    summary["signal_end"] = sd.strftime("%Y-%m-%d")
    summary["hold_days"] = hold_days
    summary["signals_days"] = 1
    summary["codes_count"] = len(sub)
    summary["strategies"] = strategies
    summary["start"] = sd.strftime("%Y-%m-%d")
    summary["end"] = (sd + timedelta(days=hold_days)).strftime("%Y-%m-%d")

    pd.DataFrame([summary]).to_csv(f"{base}-summary.csv", index=False, encoding="utf-8-sig")

    # 补算回撤列（引擎返回的 equity 仅含 date/cash/holding_value/total），供前端回撤图展示
    equity = result.equity.copy()
    equity["peak"] = equity["total"].cummax()
    equity["drawdown"] = (equity["total"] - equity["peak"]) / equity["peak"] * 100
    equity.to_csv(f"{base}-equity.csv", index=False, encoding="utf-8-sig")

    result.trades.to_csv(f"{base}-trades.csv", index=False, encoding="utf-8-sig")

    return {
        "date_tag": date_tag,
        "summary": summary,
        "num_trades": int(summary.get("num_trades", 0)),
    }


def main() -> int:
    hold_days = int(os.environ.get("HOLD_DAYS", "10"))
    lookback_days = int(os.environ.get("LOOKBACK_DAYS", "30"))
    today = date.today()

    lists = collect_eligible_lists(lookback_days)
    if not lists:
        print(f"未找到近 {lookback_days} 天的 Daily-Action-List CSV，跳过每日回测")
        return 0

    generated: list[dict] = []
    for path, sd in lists:
        days_since = (today - sd).days
        actual_hold = min(hold_days, days_since)
        if actual_hold < 1:
            # 当天的信号至少需持有 1 天，跳过（明天 CI 会自动纳入）
            continue
        print(f"[回测] 信号日 {sd} · 距今天 {days_since} 天 · 实际持有 {actual_hold} 天")
        res = _backtest_one(path, sd, actual_hold)
        if res is None:
            print("  → 无有效结果（信号为空或 K 线拉取失败）")
            continue
        generated.append(res)
        print(f"  → 总收益 {res['summary'].get('total_return')}%，"
              f"回撤 {res['summary'].get('max_drawdown')}%，{res['num_trades']} 笔")

    if not generated:
        print("没有生成任何每日回测结果（可能历史清单为空）")
        return 0

    print(f"\n完成：共生成 {len(generated)} 份前向信号回测，信号日范围 "
          f"{generated[0]['date_tag']} ~ {generated[-1]['date_tag']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
