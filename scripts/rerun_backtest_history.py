#!/usr/bin/env python3
"""一次性全量重回测：用最新 DAL 重新生成所有历史信号日的 Multi-Backtest 结果。

硬化点：
- socket.setdefaulttimeout(45)：防止 akshare 单次请求挂死整轮。
- 删除旧 Multi-Backtest-*-{summary,trades,equity}.csv，强制 daily_backtest 的
  _skip_completed 不再跳过已走完窗口的信号日，使全部历史日都用「新 DAL」重新回测。
- LOOKBACK_DAYS=200：覆盖全部 38 个信号日（DAL 跨度 20260610~20260731）。
- 通过 runpy 以 __main__ 身份执行 daily_backtest.py，复用其预拉/内联过滤/回测逻辑。
"""
import glob
import os
import runpy
import socket
import sys
from pathlib import Path

socket.setdefaulttimeout(45)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# 本地重回测：用通达信(TDX,毫秒级)拉 K 线；TDX 不可达时 fetch_daily_k 自动回退 akshare。
# 切勿强制 akshare —— 那会让每只票 ~6s，全量重回测会拖到十几小时。
try:
    from smcore.data.tdx_client import get_client as _tdx_get
    _tdx_get()
    print("[info] TDX 可达，使用本地高速后端", flush=True)
except Exception:
    print("[info] TDX 不可达，回退 akshare", flush=True)

os.environ["LOOKBACK_DAYS"] = "200"
os.environ["HOLD_DAYS"] = "10"
os.environ["BACKTEST_MIN_STRATEGIES"] = "2"
os.environ["PREPULL_INTERVAL"] = "0.2"
os.environ["BACKTEST_INLINE_FILTER"] = "1"

from smcore.artifacts import STOCK_DATA_DIR

# 强制重算：清掉旧 Multi-Backtest 产物（可重新生成，属中间结果）
removed = 0
for ext in ("summary", "trades", "equity"):
    for f in glob.glob(str(STOCK_DATA_DIR / f"Multi-Backtest-*-{ext}.csv")):
        try:
            os.remove(f)
            removed += 1
        except OSError:
            pass
print(f"[清理] 删除旧 Multi-Backtest 产物 {removed} 个，强制全量重回测", flush=True)

if __name__ == "__main__":
    runpy.run_path(
        str(ROOT / "scripts" / "daily_backtest.py"), run_name="__main__"
    )
    sys.exit(0)
