#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""warmup_missing_klines.py 的单批拉取工作单元（由其以子进程调用）。

用法：python _warmup_batch.py 000001,000002,...
在 kline_write_buffer() 内逐只 fetch_daily_k（2015 至今，qfq），退出时按桶一次落盘。
单只失败只 print 告警不抛异常（父进程按「实际可读代码数」统计成败，不依赖本进程 rc）。
"""
from __future__ import annotations

import datetime
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from smcore.data.kline import fetch_daily_k, kline_write_buffer  # noqa: E402


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: _warmup_batch.py code[,code...]", file=sys.stderr)
        return 2
    codes = [c.strip() for c in sys.argv[1].split(",") if c.strip()]
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    n_ok = 0
    with kline_write_buffer():
        for c in codes:
            try:
                df = fetch_daily_k(c, "2015-01-01", today, adjust="qfq")
                rows = 0 if df is None or df.empty else len(df)
                if rows:
                    n_ok += 1
                else:
                    print(f"[warmup-batch] WARN {c}: 0 行", file=sys.stderr)
            except Exception as exc:  # noqa: BLE001
                print(f"[warmup-batch] WARN {c}: {type(exc).__name__}:{exc}", file=sys.stderr)
    print(f"[warmup-batch] got {n_ok}/{len(codes)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
