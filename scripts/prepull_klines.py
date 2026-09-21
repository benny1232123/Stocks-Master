#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""全宇宙 K 线缓存刷新（补齐到信号日）——A 批因子生成的前置数据步骤。

事故背景（2026-09-17）
----------------------
``scripts/zoo_factor_strategy.py`` 按其设计**只读**本地 ``stock_data/k_data/`` 缓存
（离线可跑、PIT 安全）。该缓存此前是被 5 个遗留策略步骤「顺带」刷新的——尤其
``smcore/strategies/momentum.py`` 会遍历全宇宙 ``fetch_daily_k(code, '2015-01-01', now)``。
2026-09-17 这些遗留步骤退役后，缓存不再被刷新：信号日有效截面从 4363 只塌到 1 只 →
A 批 12 因子全部 0 票（只写出表头空表）→ ``Daily-Action-List`` 缺失，而 CI 仍报 success。

本脚本把这份「顺带刷新」提为**显式步骤**，并在末尾做覆盖度门控——
**把静默失败变成红色构建**。

做法
----
1. 取 k_data 现有宇宙（``list_kline_codes``）——与因子矩阵的 universe 一致；
2. 按 ``--chunk-size`` 分块，每块起**独立子进程**逐只 ``fetch_daily_k`` 补尾部缺口
   （子进程隔离：单只/单块崩溃不影响其余；与 ``scripts/warmup_missing_klines.py``
   同一思路，但那里只补「整只缺失」的代码，不修陈旧尾部）；
3. 用 ``factor_engine.load_matrices`` 读回矩阵，统计信号日那一行的非空代码数；
   低于 ``--min-codes`` → ``::error::`` + exit 1（触发失败告警）。

⚠️ 为什么不用「每只一个子进程」：4380 只 × 解释器启动开销 ≈ 1–2 小时，CI 跑不完；
分块（默认 100 只/块 ≈ 44 个子进程）在隔离性与总耗时之间取平衡。

⚠️ **写放大才是超时的真因**（2026-09-17 实测定位，见 ``.workbuddy/_bench_kline.txt``）：
``k_data`` 只有 **7 个分桶**（最大 ``qfq_b00.parquet`` 91.8MB / 3.70M 行），而
``write_kline_cache`` 每写**一只票**都要 ``read_parquet(整桶) → concat → sort →
to_parquet(整桶, zstd)``，实测 **5.33s/只** → 4380 只 ≈ **6.5 小时**。
相比之下网络取数只有 0.17–1.07s/只（≈36min）。故 ``_run_chunk`` 整块包在
``kline_write_buffer()`` 里 —— 每块（100 只 ≈ 1 个桶）只重写一次桶。

用法::

    python scripts/prepull_klines.py --date 20260917
    python scripts/prepull_klines.py --date 20260917 --min-codes 1000 --lookback-days 400
    python scripts/prepull_klines.py --date 20260917 --codes 600000,000001   # 调试：只跑指定代码
    python scripts/prepull_klines.py --date 20260921 --from-holdings         # 持仓日报：只刷当前持仓
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

KDATA = ROOT / "stock_data" / "k_data"


def _held_codes() -> list[str]:
    """当前 FIFO 开仓持仓的代码（去重保序）。读取失败返回 []（不抛）。

    持仓日报（daily-holdings.yml）用它做**定向刷新**：只补当前持仓那几只票的 K 线，
    而不是全宇宙 4380 只（后者 ~50min，持仓通常 <20 只 → 秒级）。
    """
    try:
        from smcore.holdings import compute_fifo_positions, load_trades

        pos_df, _closed = compute_fifo_positions(load_trades())
        if pos_df is None or pos_df.empty or "代码" not in pos_df.columns:
            return []
        return list(
            dict.fromkeys(str(c).strip() for c in pos_df["代码"].tolist() if str(c).strip())
        )
    except Exception as exc:
        print(f"[prepull] 读取持仓失败（{type(exc).__name__}: {exc}）", file=sys.stderr)
        return []


def _stale_codes(codes: list[str], date_str: str) -> list[str]:
    """返回缓存尾部 bar **早于** date_str 的代码（含完全无数据者）。

    这是持仓日报的「新鲜度门控」：定向刷新后逐只复核，确保报告用的不是 D-1 旧价
    （旧实现无此校验，抓到旧 bar 时 RSI/MACD/布林/现价全部失真且无声）。
    """
    import pandas as pd

    from smcore.data.kline import fetch_daily_k

    sig = datetime.strptime(date_str, "%Y%m%d").date()
    start = (sig - timedelta(days=30)).strftime("%Y-%m-%d")
    end = sig.strftime("%Y-%m-%d")
    stale: list[str] = []
    for c in codes:
        try:
            df = fetch_daily_k(c, start, end)
            if df is None or df.empty or "date" not in df.columns:
                stale.append(c)
                continue
            mx = pd.to_datetime(df["date"], errors="coerce").dropna()
            if mx.empty or mx.max().date() < sig:
                stale.append(c)
        except Exception:
            stale.append(c)
    return stale


# ── 子进程 worker：跑一个分块 ─────────────────────────────────────────────

def _run_chunk(codes: list[str], start: str, end: str) -> int:
    """在**当前进程**内逐只刷新（由父进程以子进程方式调起）。

    整块包在 ``kline_write_buffer()`` 里：退出时按**桶**一次性 upsert，而不是每只票
    各自重写整桶（见模块 docstring 的「写放大」说明）。codes 已排序，同块通常落在
    同一个桶 → 整块只需一次重写。异常时上下文也会 flush，不丢已取到的数据。
    """
    from smcore.data.kline import fetch_daily_k, kline_write_buffer

    ok = fail = 0
    t0 = time.time()
    with kline_write_buffer():
        for c in codes:
            try:
                fetch_daily_k(c, start, end, adjust="qfq")
                ok += 1
            except Exception as exc:  # 单只失败不拖垮整块
                fail += 1
                print(f"  [chunk] {c} 失败（{type(exc).__name__}: {exc}）", file=sys.stderr)
    print(f"[chunk] ok={ok} fail={fail} 用时 {time.time() - t0:.1f}s", flush=True)
    return 0


# ── 覆盖度：信号日那一行的非空代码数（= A 批因子实际能看到的截面）────────

def _coverage(date_str: str, load_start: str) -> tuple[int, int]:
    """信号日有效截面——复用 factor_engine 的权威度量（与 verify_data_freshness 同口径）。"""
    from smcore.strategy import factor_engine as fe

    return fe.load_signal_day_coverage(date_str, load_start)


def main() -> int:
    ap = argparse.ArgumentParser(description="全宇宙 K 线缓存刷新（A 批因子前置）")
    ap.add_argument("--date", default=datetime.now().strftime("%Y%m%d"), help="信号日 YYYYMMDD")
    ap.add_argument("--lookback-days", type=int, default=400,
                    help="请求窗口回溯自然日数（够覆盖最长因子窗 120 交易日即可，默认 400）")
    ap.add_argument("--chunk-size", type=int, default=100, help="每个子进程处理的代码数")
    ap.add_argument("--timeout", type=int, default=900, help="单个分块的超时秒数")
    ap.add_argument("--min-codes", type=int, default=1000,
                    help="信号日有效截面下限；低于此值报错退出（默认 1000，远高于引擎 MIN_N_DAY=300）")
    ap.add_argument("--codes", default="", help="调试：逗号分隔，只刷新这些代码（跳过门控）")
    ap.add_argument("--from-holdings", action="store_true",
                    help="持仓日报：只刷新当前 FIFO 持仓，并逐只复核信号日 bar（不跑全宇宙覆盖度门控）")
    ap.add_argument("--_chunk", default="", help=argparse.SUPPRESS)   # 内部：子进程分块
    args = ap.parse_args()

    try:
        sig = datetime.strptime(args.date, "%Y%m%d")
    except ValueError:
        print(f"[prepull] 非法日期 {args.date}")
        return 2
    start = (sig - timedelta(days=int(args.lookback_days))).strftime("%Y-%m-%d")
    end = sig.strftime("%Y-%m-%d")

    # 子进程分块模式
    if args._chunk:
        return _run_chunk([c for c in args._chunk.split(",") if c], start, end)

    if args.codes:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
        print(f"[prepull] 调试模式：{len(codes)} 只，窗口 {start} ~ {end}", flush=True)
        _run_chunk(codes, start, end)
        n, total = _coverage(args.date, start)
        print(f"[prepull] 覆盖度 {n}/{total}（调试模式不做门控）")
        return 0

    # ── 持仓日报定向模式（daily-holdings.yml）──────────────────────────────
    # 只刷当前持仓（通常 <20 只，秒级），不做全宇宙覆盖度门控（那不是本场景的判据）；
    # 改为**逐只**复核信号日 bar，把「抓到旧 bar 却照常出信号」变成显式失败/告警。
    if args.from_holdings:
        codes = _held_codes()
        if not codes:
            print("[prepull] 当前无持仓 → 无需刷新 K 线")
            return 0
        print(f"[prepull] 持仓定向刷新：{len(codes)} 只，窗口 {start} ~ {end}", flush=True)
        _run_chunk(codes, start, end)
        stale = _stale_codes(codes, args.date)
        if not stale:
            print(f"[prepull] ✅ {len(codes)} 只持仓均含信号日 {args.date} 的 bar，可供报告使用。")
            return 0
        print(f"[prepull] 信号日 {args.date} 缓存仍缺当日 bar 的持仓：{', '.join(stale)}")
        if len(stale) == len(codes):
            print("::error::[prepull] 全部持仓都缺信号日 bar —— 数据源未更新或刷新链失效；"
                  "继续出报告会用 D-1 旧价（RSI/MACD/布林/现价全失真）故直接失败。")
            print("::error::排查：① KLINE_BACKEND/密钥是否可用；② 数据源当日 bar 是否已发布；"
                  "③ 若非交易日（周末/节假日）可设 ALLOW_STALE_DATA=1 或跳过本步。")
            return 1
        print(f"::warning::[prepull] {len(stale)}/{len(codes)} 只持仓缺信号日 bar"
              f"（停牌/退市/次新等），报告对这几只将使用最新可得 bar。")
        return 0

    from smcore.data.kline import list_kline_codes

    codes = [c for c in list_kline_codes(base_dir=KDATA) if c]
    if not codes:
        print("::error::[prepull] k_data 宇宙为空——无法刷新（缓存文件缺失或全部读失败）")
        return 1

    chunks = [codes[i:i + args.chunk_size] for i in range(0, len(codes), args.chunk_size)]
    print(f"[prepull] 信号日 {args.date}，宇宙 {len(codes)} 只 → {len(chunks)} 块，"
          f"窗口 {start} ~ {end}", flush=True)

    dead_chunks = 0
    for i, ch in enumerate(chunks, 1):
        try:
            r = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), "--date", args.date,
                 "--lookback-days", str(args.lookback_days), "--_chunk", ",".join(ch)],
                capture_output=True, text=True, timeout=args.timeout, cwd=str(ROOT),
            )
            rc = r.returncode
        except subprocess.TimeoutExpired:
            rc, r = -1, None
            print(f"  [{i}/{len(chunks)}] 超时（>{args.timeout}s），该块剩余代码跳过", flush=True)
        if rc != 0:
            dead_chunks += 1
        if i % 10 == 0 or i == len(chunks):
            print(f"  [{i}/{len(chunks)}] 完成（坏块 {dead_chunks}）", flush=True)

    n, total = _coverage(args.date, start)
    print(f"[prepull] 信号日 {args.date} 有效截面 {n}/{total} 只"
          f"（坏块 {dead_chunks}/{len(chunks)}），门槛 {args.min_codes}")

    if n < int(args.min_codes):
        print(f"::error::[prepull] 信号日 {args.date} 有效截面仅 {n} 只，低于门槛 {args.min_codes}——"
              f"A 批因子会全部产空表并让当日清单缺失，故直接失败。")
        print("::error::排查：① KLINE_BACKEND/密钥是否可用；② 数据源当日 bar 是否已发布；"
              "③ 上游 k_data 缓存是否损坏。")
        return 1
    print(f"[prepull] ✅ 通过：{n} 只已含信号日 bar，可供 A 批因子计算。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
