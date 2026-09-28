#!/usr/bin/env python3
"""补拉缺失的 k_data 缓存：按批子进程拉取（网络崩溃只损失单批，可重试）。

背景：fetch_daily_k 在数据源不稳定时会无声硬崩（原生层，无 traceback），此前
批量预热因此反复失败。子进程隔离 = 单只代码崩溃不影响整体。

批量模式（--batch N，默认 20）：每批在一个子进程内用 kline_write_buffer() 聚合
写入 —— 桶重写次数从「每只一次」（O(n²) IO，1700 只 ≈ 2.5h）降到「每批一次」
（网络成为瓶颈）。崩溃隔离从单只降为单批，重跑同命令即可断点续补（已落盘的
批次不再被识别为缺失）。
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
KDATA = ROOT / "stock_data" / "k_data"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, default=20,
                    help="每批代码数（kline_write_buffer 聚合落盘；1 = 旧行为逐只拉）")
    args = ap.parse_args()
    batch = max(1, args.batch)

    from smcore.strategies.momentum import _load_all_codes

    codes = [c for c, _n in _load_all_codes()
             if not c.startswith("30") and not c.startswith("688")
             # 北交所(920xxx)：数据源阶梯(baostock/akshare 主通道)不提供北交所日线，
             # fetch 恒返回空 —— 不剔除会被永远计为 fail（2026-09-26 实测 322/322 全为此类）
             and not c.startswith("920")]
    from smcore.data.kline import list_kline_codes
    have = set(list_kline_codes(base_dir=KDATA))
    missing = [c for c in codes if c not in have]
    print(f"宇宙 {len(codes)} 只，缺缓存 {len(missing)} 只（batch={batch}）", flush=True)
    batch_script = str(ROOT / "scripts" / "_warmup_batch.py")
    ok = fail = 0
    retries = 2
    for i in range(0, len(missing), batch):
        chunk = missing[i:i + batch]
        done = False
        for attempt in range(1, retries + 1):
            try:
                r = subprocess.run([sys.executable, batch_script, ",".join(chunk)],
                                   capture_output=True, text=True,
                                   timeout=90 * len(chunk) + 60, cwd=str(ROOT))
                done = r.returncode == 0
                if not done:
                    print(f"  DEBUG rc={r.returncode} cwd={ROOT} script={batch_script}", flush=True)
                    if r.stdout:
                        print("  stdout tail: " + r.stdout[-300:].replace("\n", " | "), flush=True)
                    if r.stderr:
                        print("  stderr tail: " + r.stderr[-300:].replace("\n", " | "), flush=True)
                    if not r.stdout and not r.stderr:
                        print("  DEBUG: 子进程无任何输出即退出（疑似原生层硬崩）", flush=True)
            except subprocess.TimeoutExpired:
                done = False
            if done:
                break
            print(f"  批 {i//batch+1} attempt {attempt} 失败，重试…", flush=True)
            time.sleep(5)
        # 落盘校验以实际可读代码为准（子进程 rc=0 不代表每只都取到）
        from smcore.data.kline import list_kline_codes as _lk
        have_now = set(_lk(base_dir=KDATA))
        got = sum(1 for c in chunk if c in have_now)
        ok += got
        fail += len(chunk) - got
        if (i // batch) % 5 == 0 or i + batch >= len(missing):
            print(f"  [{min(i+batch, len(missing))}/{len(missing)}] ok={ok} fail={fail}", flush=True)
        time.sleep(10)  # 批间小歇：连拉 1500+ 只后源端会限流导致原生层硬崩（2026-09-26 实测）
    print(f"[warmup] 完成：成功 {ok} 失败 {fail}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
