#!/usr/bin/env python3
"""一次性工具（2026-09-05）：用修复后的 CCTV 流水线重建历史池。

背景：消息面修复（联播快讯拆分 / 时政过滤 / 情感负权重 / 申万词叠加门控）只影响
此后每日运行，历史 CCTV-Sector-Stock-Pool 仍是旧口径，融合与回测学的是脏数据。
本脚本分三步：
  1) 预取缺失的 {date}_news.csv（akshare news_cctv，4 线程，温和限速）；
  2) 备份 stock_data 根目录全部 CCTV-*.csv → stock_data/archive/pre_rebuild_20260905/；
  3) 对目标日期升序逐日回放 cctv 策略（SIGNAL_DATE + run_strategy_for_date.py 子进程，
     失败重试 1 次），保证 enrich_with_prev_change 的「较上一期」沿新链路计算。

用法：python scripts/rebuild_cctv_history.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
DATA = ROOT / "stock_data"
ARCHIVE = DATA / "archive" / "pre_rebuild_20260905"
LOG = ROOT / ".workbuddy" / "rebuild_cctv_20260905.log"


def dates_of(pattern: str) -> set[str]:
    out = set()
    for p in DATA.glob(pattern):
        m = re.search(r"(\d{8})", p.name)
        if m:
            out.add(m.group(1))
    return out


def target_dates() -> list[str]:
    """重建范围 = 已有池的日期 ∪ 有 Daily-Action-List 但当年 cctv 失败的日期。"""
    pool = dates_of("CCTV-Sector-Stock-Pool-*.csv")
    dal = dates_of("Daily-Action-List-*.csv")
    return sorted(pool | dal)


def prefetch_news(dates: list[str]) -> list[str]:
    """缺本地新闻缓存的日期先抓取并落盘（与 fetch_cctv_news 同样的规范化+保存）。"""
    import akshare as ak
    from smcore.strategies.cctv import _normalize_news_df

    missing = [d for d in dates if not (DATA / f"{d}_news.csv").exists()]
    if not missing:
        print(f"[prefetch] 全部 {len(dates)} 日均有本地新闻缓存", flush=True)
        return []

    def one(ds: str) -> tuple[str, str]:
        try:
            df = ak.news_cctv(date=ds)
            df, raw = _normalize_news_df(df)
            if df is None or df.empty:
                return ds, "empty"
            df.to_csv(DATA / f"{ds}_news.csv", index=False, encoding="utf-8-sig")
            return ds, f"ok({len(df)}条)"
        except Exception as e:  # noqa: BLE001
            return ds, f"fail:{type(e).__name__}:{str(e)[:60]}"

    print(f"[prefetch] 预取 {len(missing)} 日新闻缓存…", flush=True)
    fails = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(one, ds): ds for ds in missing}
        for i, fut in enumerate(as_completed(futs), 1):
            ds, msg = fut.result()
            print(f"  [{i}/{len(missing)}] {ds} -> {msg}", flush=True)
            if msg.startswith(("fail", "empty")):
                fails.append(ds)
            time.sleep(0.3)
    print(f"[prefetch] 完成，失败/空 {len(fails)} 日", flush=True)
    return fails


def backup_cctv_files() -> int:
    ARCHIVE.mkdir(parents=True, exist_ok=True)
    files = sorted(DATA.glob("CCTV-*.csv")) + sorted(DATA.glob("CCTV-*.md"))
    n = 0
    for p in files:
        dst = ARCHIVE / p.name
        if not dst.exists():
            shutil_copy(p, dst)
            n += 1
    print(f"[backup] 已备份 {n} 个 CCTV 产物 → {ARCHIVE.relative_to(ROOT)}", flush=True)
    return n


def shutil_copy(src: Path, dst: Path) -> None:
    import shutil
    shutil.copy2(src, dst)


def replay_cctv(dates: list[str]) -> tuple[list[str], list[str]]:
    """逐日升序回放 cctv 策略。返回 (成功, 失败) 日期列表。"""
    ok, fail = [], []
    total = len(dates)
    t0 = time.time()
    for i, ds in enumerate(dates, 1):
        env = dict(os.environ, SIGNAL_DATE=ds, MPLBACKEND="Agg")
        cmd = [sys.executable, str(ROOT / "scripts" / "run_strategy_for_date.py"), "cctv"]
        tag = f"[{i}/{total}] {ds}"
        for attempt in (1, 2):
            try:
                r = subprocess.run(cmd, env=env, capture_output=True, text=True,
                                   cwd=str(ROOT), timeout=300)
                rc = r.returncode
            except Exception as e:  # noqa: BLE001
                rc, r = 1, None
                print(f"{tag} attempt{attempt} EXC {type(e).__name__}:{e}", flush=True)
            if rc == 0:
                pool = DATA / f"CCTV-Sector-Stock-Pool-{ds}.csv"
                n = 0
                if pool.exists():
                    try:
                        n = len(pd.read_csv(pool, encoding="utf-8-sig"))
                    except Exception:
                        n = -1
                ok.append(ds)
                print(f"{tag} ok 池={n}只 ({time.time()-t0:.0f}s)", flush=True)
                break
            tail = ""
            if r is not None:
                tail = (r.stderr or r.stdout or "")[-300:].replace("\n", " | ")
            if attempt == 1:
                print(f"{tag} attempt1 rc={rc}，重试… {tail[:120]}", flush=True)
                time.sleep(3)
            else:
                fail.append(ds)
                print(f"{tag} FAIL rc={rc} {tail}", flush=True)
    print(f"[replay] ok={len(ok)} fail={len(fail)} 耗时 {time.time()-t0:.0f}s", flush=True)
    if fail:
        print(f"[replay] 失败日期: {fail}", flush=True)
    return ok, fail


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    dates = target_dates()
    print(f"[plan] 目标 {len(dates)} 日: {dates[0]}~{dates[-1]}", flush=True)
    if args.dry_run:
        for d in dates:
            print("  ", d, flush=True)
        return 0

    LOG.parent.mkdir(parents=True, exist_ok=True)
    fails_prefetch = prefetch_news(dates)
    backup_cctv_files()
    ok, fail = replay_cctv(dates)
    if fails_prefetch:
        print(f"[note] 新闻预取失败日期（其池可能回退前一日或为空）: {fails_prefetch}", flush=True)
    print("[done] CCTV 历史重建完成", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
