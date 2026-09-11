#!/usr/bin/env python3
"""数据新鲜度硬校验（fail fast）——信号日当日 bar 必须已存在。

背景（2026-09-12 复审）：CI 在北京时间 16:30 起跑，akshare/baostock 的当日日线
存在更新延迟；此前链路里"数据没到"会静默降级为用 D-1 收盘数据出"信号日=今天"
的清单，产出文件照常落库，缺陷不以任何红色形式暴露，持续污染前向回测档案。

本脚本在五策略之前运行：
  - 任一流动性样本股的 K 线已含信号日 bar → 通过（exit 0）；
  - 全部样本都缺信号日 bar → 打 GitHub ::error:: 注解并 exit 1（触发失败告警）。

逃生口：ALLOW_STALE_DATA=1（手动补跑旧日期 / 明确接受陈旧数据时设置）。
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# 样本股：两只流动性最好的主板票（任一新鲜即认为数据源已更新当日 bar）
SAMPLE_CODES = ("600000", "000001")


def _beijing_today():
    from smcore.data.kline import _now_cn

    return _now_cn().date()


def _latest_bar_date(code: str, signal_date):
    from smcore.data.kline import fetch_daily_k

    try:
        df = fetch_daily_k(code, (signal_date - timedelta(days=10)).strftime("%Y-%m-%d"),
                           signal_date.strftime("%Y-%m-%d"))
    except Exception as exc:
        print(f"  {code}: 拉取失败（{type(exc).__name__}: {exc}）")
        return None
    if df is None or df.empty or "date" not in df.columns:
        print(f"  {code}: 无 K 线数据")
        return None
    dates = __import__("pandas").to_datetime(df["date"], errors="coerce").dropna()
    if dates.empty:
        return None
    return dates.max().date()


def main() -> int:
    parser = argparse.ArgumentParser(description="信号日数据新鲜度硬校验")
    parser.add_argument("--signal-date", default="", help="信号日 YYYYMMDD（默认 = 北京今天）")
    args = parser.parse_args()

    today = _beijing_today()
    if args.signal_date.strip():
        from datetime import datetime

        signal_date = datetime.strptime(args.signal_date.strip(), "%Y%m%d").date()
    else:
        signal_date = today

    if os.getenv("ALLOW_STALE_DATA", "").strip() == "1":
        print("[freshness] ALLOW_STALE_DATA=1 → 跳过新鲜度校验（手动补跑模式）")
        return 0
    if signal_date < today:
        # 补跑历史日：只要求该日之前有数据即可，不做"当日"要求
        print(f"[freshness] 信号日 {signal_date} 早于今天，跳过当日 bar 校验")
        return 0

    print(f"[freshness] 校验信号日 {signal_date} 的日 K 是否已发布（样本：{', '.join(SAMPLE_CODES)}）...")
    for code in SAMPLE_CODES:
        latest = _latest_bar_date(code, signal_date)
        print(f"  {code}: 最新 bar = {latest}")
        if latest is not None and latest >= signal_date:
            print(f"[freshness] ✅ 通过：数据源已含信号日 {signal_date} 的 bar")
            return 0

    print("::error::数据新鲜度校验失败：所有样本股都缺信号日 %s 的日 K——" % signal_date)
    print("::error::此时跑选股会用 D-1 收盘数据出『信号日=今天』的清单（静默污染前向档案）。")
    print("::error::建议：① 等数据源更新后重跑；② 或设 ALLOW_STALE_DATA=1 明确接受陈旧数据。")
    return 1


if __name__ == "__main__":
    sys.exit(main())
