#!/usr/bin/env python3
"""补拉缺失的 k_data 缓存：每只代码用独立子进程拉取（网络崩溃只损失单只，可重试）。

背景：fetch_daily_k 在数据源不稳定时会无声硬崩（原生层，无 traceback），此前
批量预热因此反复失败。子进程隔离 = 单只代码崩溃不影响整体。
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
KDATA = ROOT / "stock_data" / "k_data"


def main() -> int:
    from smcore.strategies.momentum import _load_all_codes

    codes = [c for c, _n in _load_all_codes()
             if not c.startswith("30") and not c.startswith("688")]
    from smcore.data.kline import list_kline_codes
    have = set(list_kline_codes(base_dir=KDATA))
    missing = [c for c in codes if c not in have]
    print(f"宇宙 {len(codes)} 只，缺缓存 {len(missing)} 只")
    fetch_snippet = (
        "import sys; sys.path.insert(0, %r); "
        "from smcore.data.kline import fetch_daily_k; "
        "fetch_daily_k(sys.argv[1], '2015-01-01', __import__('datetime').datetime.now().strftime('%%Y-%%m-%%d'), adjust='qfq')"
        % str(ROOT)
    )
    ok = fail = 0
    for i, c in enumerate(missing, 1):
        try:
            r = subprocess.run([sys.executable, "-c", fetch_snippet, c],
                               capture_output=True, text=True, timeout=90, cwd=str(ROOT))
            ok += r.returncode == 0
            fail += r.returncode != 0
        except subprocess.TimeoutExpired:
            fail += 1
        if i % 50 == 0:
            print(f"  [{i}/{len(missing)}] ok={ok} fail={fail}", flush=True)
    print(f"[warmup] 完成：成功 {ok} 失败 {fail}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
