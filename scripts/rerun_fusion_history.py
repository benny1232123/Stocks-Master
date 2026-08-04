#!/usr/bin/env python3
"""一次性全量重融合（硬化 + 落盘日志版）。

- socket.setdefaulttimeout(45) 防单请求挂死；akshare 自带 30s 超时 + 重试，不会死锁。
- 进度同时写 stdout(-u flush) 与 .workbuddy/rerun_fusion.log，便于后台任务记录丢失时仍能监控。
- k_data 已预热(2642+ 缓存)时单日秒级完成；幂等覆盖旧 DAL。
"""
import glob
import os
import socket
import sys
import time
from datetime import datetime, date
from pathlib import Path

socket.setdefaulttimeout(45)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
# 本地重融合：优先用通达信(TDX,毫秒级)；TDX 不可达时 fetch_daily_k 自动回退 akshare。
# 注意：不要强制 akshare —— 那会让每只要 ~6s，全量重融合会拖到十几小时。
try:
    from smcore.data.tdx_client import get_client as _tdx_get
    _tdx_get()
    print("[info] TDX 可达，使用本地高速后端", flush=True)
except Exception:
    print("[info] TDX 不可达，回退 akshare", flush=True)

LOG = ROOT / ".workbuddy" / "rerun_fusion.log"
LOG.parent.mkdir(parents=True, exist_ok=True)


def log(msg: str) -> None:
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass


from smcore.strategy.fusion import fuse_signals, save_action_list  # noqa: E402
from smcore.config.defaults import STOCK_DATA_DIR  # noqa: E402


def main() -> None:
    dal_files = sorted(glob.glob(str(STOCK_DATA_DIR / "Daily-Action-List-*.csv")))
    tags = [
        os.path.basename(f).replace("Daily-Action-List-", "").replace(".csv", "")
        for f in dal_files
    ]
    log(f"=== 全量重融合 {len(tags)} 个信号日（全自适应逻辑）===")
    today = datetime.combine(date.today(), datetime.min.time()).timestamp()
    force = "--force" in sys.argv
    t0 = time.time()
    ok = fail = skipped = 0
    for i, tag in enumerate(tags):
        dal = STOCK_DATA_DIR / f"Daily-Action-List-{tag}.csv"
        # 幂等：今天已经重融合过的天跳过（除非 --force）
        if not force and dal.exists() and dal.stat().st_mtime >= today:
            skipped += 1
            log(f"  [{i + 1}/{len(tags)}] {tag} -> 跳过(今日已融合)")
            continue
        day_t0 = time.time()
        try:
            df, msg = fuse_signals(tag)
            if df is not None and not df.empty:
                save_action_list(df, tag)
                ok += 1
                status = f"{len(df)}只"
            else:
                # 新逻辑下该日无候选 → 删除旧 DAL，使回测正确排除该日（不残留旧选股）
                old = STOCK_DATA_DIR / f"Daily-Action-List-{tag}.csv"
                if old.exists():
                    old.unlink()
                ok += 1
                status = "0只(删旧DAL)"
        except Exception as e:  # noqa: BLE001
            fail += 1
            status = f"ERR:{type(e).__name__}:{str(e)[:80]}"
        cost = time.time() - day_t0
        elapsed = time.time() - t0
        eta = elapsed / (i + 1) * (len(tags) - i - 1)
        log(
            f"  [{i + 1}/{len(tags)}] {tag} -> {status}  本日{cost:.1f}s 累计{elapsed:.0f}s ETA{eta:.0f}s"
        )
    log(f"融合完成: {ok}成功 {fail}失败 {skipped}跳过 耗时{time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
