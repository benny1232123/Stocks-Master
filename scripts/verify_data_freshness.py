#!/usr/bin/env python3
"""数据新鲜度校验——两层职责，都在 K 线刷新步骤之前跑。

背景（2026-09-12 复审）：CI 在北京时间 16:30 起跑，akshare/baostock 的当日日线
存在更新延迟；此前链路里"数据没到"会静默降级为用 D-1 收盘数据出"信号日=今天"
的清单，产出文件照常落库，缺陷不以任何红色形式暴露，持续污染前向回测档案。

本脚本两层（⚠️ 2026-09-17 A 批空产出事故后拆分，别再混为一谈）：

  ① **缓存截面健康度**（离线，永远打印，只 `::warning::` 不退出）
     —— 报告「**上一交易日**」在 `stock_data/k_data` 缓存里的有效截面 N/total
     （`factor_engine.signal_day_coverage`）。看上一交易日而非今天：今天的数据本就
     要等下一步的全宇宙刷新去拉，报"今天覆盖度低"是噪声；但上一交易日理应由昨天那次
     运行补齐，连它都塌了就说明刷新链路已失效。
  ② **数据源是否已发布当日 bar**（网络抽查 2 只流动样本股；fail fast + exit 1）
     —— 目的只是"别在上游还没出数据时白跑 ~60min 全宇宙刷新"，因此判据宽容
     （任一样本有当日 bar 即通过）。
     ⚠️ 这一层**不能**用来判断"能不能出票"——它只看 2 只票，会 4363/4380 → 1/4380
     的崩塌判成"正常"（这正是事故成因）。权威门控是刷新**之后**的
     `scripts/prepull_klines.py --min-codes`（数真实截面）。

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


# 上一交易日缓存截面的健康门槛。与 scripts/prepull_klines.py 的 --min-codes 默认值一致。
# 取绝对阈值而非比例：正常交易日截面是 4300+，塌陷时是几十，1000 有很大余量。
PREV_DAY_MIN_CODES = 1000


def _prev_day_coverage(signal_date):
    """上一交易日的**缓存截面**覆盖度（离线，无网络）。返回 ``(date, n, total)`` 或 ``None``。

    为什么要看"上一交易日"而不是"今天"：本脚本跑在 K 线刷新步骤**之前**，今天的数据
    本来就还没拉（那正是下一步要做的），所以"今天覆盖度低"是正常的、报了也是噪声。
    但**上一交易日**理应由昨天那次运行的刷新补齐——若连它都塌了，说明刷新链路已失效。
    这正是 2026-09-17 事故的形态：缓存 4363/4380（0914）→ 35 → 19 → 1，连续三天无声崩塌。
    """
    from smcore.strategy import factor_engine as fe

    load_start = (signal_date - timedelta(days=120)).strftime("%Y-%m-%d")
    try:
        close = fe.load_matrices(cols=("close",), load_start=load_start)["close"]
    except Exception as exc:
        print(f"  [缓存截面] 矩阵读取失败：{type(exc).__name__}: {exc}")
        return None
    if close.empty:
        return None
    idx = [d for d in close.index if d.date() < signal_date]
    if not idx:
        return None
    prev = idx[-1]
    n, total = fe.signal_day_coverage(close, prev.strftime("%Y%m%d"))
    return prev.date(), n, total


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
    # ① 缓存上一交易日截面健康度（离线；永远打印，只告警不退出）——
    #    把"缓存是不是在悄悄塌"变成每次运行日志里都看得见的数字（2026-09-17 事故就是
    #    因为没人看得见 4363 → 35 → 19 → 1 这条曲线）。
    #    放在「早于今天就跳过」之前：补跑历史日时这条同样有意义。
    cov = _prev_day_coverage(signal_date)
    if cov is not None:
        prev_d, n_cov, total = cov
        print(f"[freshness] 缓存上一交易日 {prev_d} 截面：{n_cov}/{total}")
        if n_cov < PREV_DAY_MIN_CODES:
            print(f"::warning::上一交易日 {prev_d} 的缓存截面仅 {n_cov}/{total}，"
                  f"低于 {PREV_DAY_MIN_CODES}——K 线刷新链路可能已失效（A 批因子会算空表）。"
                  f"请检查 daily-pick.yml 的「K 线缓存刷新」步骤（scripts/prepull_klines.py）。")

    if signal_date < today:
        # 补跑历史日：只要求该日之前有数据即可，不做"当日"要求
        print(f"[freshness] 信号日 {signal_date} 早于今天，跳过当日 bar 校验")
        return 0

    # ② 数据源是否已发布当日 bar（网络抽查；fail fast，避免白跑 ~60min 的全宇宙刷新）
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
