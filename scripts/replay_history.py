#!/usr/bin/env python3
"""历史回放驱动：把融合+回测口径延伸回过去一年（Phase 1 默认周频抽样 ≈ 52 日）。

分三阶段，每阶段都可断点续跑（结果落 .workbuddy/replay_results/，已完成的日期跳过）：
  replay   : 对每个信号日回放 5 策略 → 生成 stock_data/Stock-Selection-*-{date}.csv
  fuse     : 融合 → 生成 stock_data/Daily-Action-List-{date}.csv
  backtest : 跑 daily_backtest（LOOKBACK_DAYS 覆盖整年；其内置 _skip_completed 续跑）

设计要点（沿用已验证的坑修复）：
  - 回放阶段用【子进程隔离】调 run_strategy_for_date.py(4 冻结策略) + run_boll(today=)，
    每进程退出自动释放句柄；经核查 5 策略均不依赖 TDX，故无跨进程泄漏风险。
  - 融合/回测阶段用【单进程 + 线程超时】包裹（TDX 在单进程内复用，避免 subprocess 的
    跨进程 k_data 句柄泄漏 → PermissionError[Errno 13]）。中和 flaky 的 baostock/em，
    保留 TDX(本地行情) + 新浪 regime；基本面走缓存(沙箱/本机 em 不可用)。
  - 单策略候选日由 daily_backtest 的 BACKTEST_MIN_STRATEGIES=2 自动跳过。

用法：
  python scripts/replay_history.py --phase all --cadence weekly --start 20250808 --end 20260808
  python scripts/replay_history.py --phase replay --dry-run          # 只看将回放哪些日
  python scripts/replay_history.py --phase all --limit 3             # 先冒烟 3 日
  python scripts/replay_history.py --phase backtest                 # 仅补跑回测(续跑)
"""
from __future__ import annotations

import argparse
import gc
import json
import os
import runpy
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from smcore.config.defaults import STOCK_DATA_DIR  # noqa: E402

RESULT_DIR = ROOT / ".workbuddy" / "replay_results"
RESULT_DIR.mkdir(parents=True, exist_ok=True)

STRATS_FROZEN = ("theme", "cctv", "relativity", "momentum")
# 必须与 smcore/strategy/picks_loader.py 中 _load_*_picks 读取的精确文件名一致
# （fusion 按 {pattern}-{date}.csv 精确匹配，仅缺失时 3 日内回退，无前视偏差）。
STRAT_FILE = {
    "boll": "Stock-Selection-Boll-{date}.csv",
    "relativity": "Stock-Selection-Relativity-{date}.csv",
    "theme": "Stock-Selection-Ashare-Theme-Turnover-{date}.csv",
    "cctv": "CCTV-Sector-Stock-Pool-{date}.csv",
    "momentum": "Stock-Selection-Momentum-{date}.csv",
}
REPLAY_TIMEOUT = 300       # 单策略回放子进程超时(秒)
FUSE_TIMEOUT = 240         # 单日融合线程超时(秒)
LOOKBACK_DAYS = 400        # 覆盖一整年(>=252 交易日 + 缓冲)


# ───────────────────────────── 交易日历 ─────────────────────────────
def build_calendar(start: str, end: str, cadence: str) -> list[str]:
    """返回 YYYYMMDD 列表。cadence=daily 用工作日, weekly 用每周五(均不含未来日)。"""
    freq = "B" if cadence == "daily" else "W-FRI"
    days = pd.date_range(start, end, freq=freq)
    today = pd.Timestamp(datetime.now().date())
    out = []
    for d in days:
        if d > today:
            continue
        out.append(d.strftime("%Y%m%d"))
    return out


# ───────────────────────────── 回放阶段 ─────────────────────────────
def _stock_selection_exists(date: str) -> bool:
    return all(
        (STOCK_DATA_DIR / f.format(date=date)).exists() for f in STRAT_FILE.values()
    )


def _replay_one(date: str) -> tuple[bool, str]:
    """子进程隔离回放 5 策略。返回 (ok, msg)。"""
    log = []
    # 4 个冻结策略：复用 run_strategy_for_date.py 的 datetime 冻结机制
    for strat in STRATS_FROZEN:
        env = dict(os.environ, SIGNAL_DATE=date, MPLBACKEND="Agg")
        try:
            r = subprocess.run(
                [sys.executable, str(ROOT / "scripts" / "run_strategy_for_date.py"), strat],
                env=env, timeout=REPLAY_TIMEOUT, capture_output=True, text=True,
                cwd=str(ROOT),
            )
            if r.returncode != 0:
                log.append(f"{strat}:rc={r.returncode}:{_tail(r.stderr or r.stdout)}")
        except subprocess.TimeoutExpired:
            return False, f"{strat}:timeout>{REPLAY_TIMEOUT}s"
        except Exception as e:  # noqa: BLE001
            return False, f"{strat}:{type(e).__name__}:{e}"
    # boll：原生 today 参数，子进程隔离
    try:
        r = subprocess.run(
            [sys.executable, "-c",
             "import sys; sys.path.insert(0, %r); "
             "from smcore.strategies.boll import run_boll; run_boll(today='%s')"
             % (str(ROOT), date)],
            env=dict(os.environ, MPLBACKEND="Agg"),
            timeout=REPLAY_TIMEOUT, capture_output=True, text=True, cwd=str(ROOT),
        )
        if r.returncode != 0:
            log.append(f"boll:rc={r.returncode}:{_tail(r.stderr or r.stdout)}")
    except subprocess.TimeoutExpired:
        return False, "boll:timeout>%ds" % REPLAY_TIMEOUT
    except Exception as e:  # noqa: BLE001
        return False, f"boll:{type(e).__name__}:{e}"
    if log:
        return False, "; ".join(log)
    return True, "5 策略回放完成"


def _tail(s: str, n: int = 200) -> str:
    return (s or "")[-n:].replace("\n", " ").strip()


# ───────────────────────────── 融合阶段 ─────────────────────────────
def _setup_neutralized_imports():
    """在 import smcore 融合/回测前中和 flaky 数据源，复用 rerun_fusion_offline 模式。"""
    os.environ.setdefault("SECTOR_MAP_ONDEMAND", "0")
    import smcore.data.session as _session_mod
    import smcore.strategy.fundamental as _fundamental
    import smcore.strategy.name_lookup as _name_lookup_mod
    import smcore.strategy.sectors as _sectors_mod
    import smcore.strategies.boll as _boll_mod
    import smcore.strategies.relativity as _rel_mod
    import smcore.strategies.theme as _theme_mod
    from smcore.utils.code import format_stock_code
    from smcore.strategy import fusion as _fusion
    from smcore.strategy.fusion import fuse_signals, save_action_list

    for _m in (_session_mod, _fundamental, _name_lookup_mod, _sectors_mod,
              _boll_mod, _rel_mod, _theme_mod):
        try:
            _m.login = lambda: False  # type: ignore[assignment]
        except Exception:
            pass
    # 基本面：纯缓存，miss→None(不联网补拉)
    def _cache_only(codes, *a, **k):
        out = {}
        for c in codes:
            out[str(c).strip()] = _fundamental._load_fund_cache(str(c).strip())
        return out
    _fundamental.fetch_fundamentals_batch = _cache_only
    return _fusion, fuse_signals, save_action_list, format_stock_code


def _fuse_one(date: str) -> dict:
    _fusion, fuse_signals, save_action_list, _ = _setup_neutralized_imports()
    try:
        df, msg = fuse_signals(date, total_capital=100000, max_picks=15,
                               fetch_levels=True)
        if df is not None and not df.empty:
            save_action_list(df, date, placeholder_when_empty=True)
            return {"date": date, "status": "ok", "n": int(len(df)),
                    "msg": (msg or "")[:80]}
        save_action_list(df if df is not None else pd.DataFrame(),
                         date, placeholder_when_empty=True)
        return {"date": date, "status": "empty", "n": 0, "msg": (msg or "")[:80]}
    except Exception as e:  # noqa: BLE001
        return {"date": date, "status": "error",
                "msg": f"{type(e).__name__}:{str(e)[:120]}"}


def _fuse_run(date: str) -> dict:
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            return ex.submit(_fuse_one, date).result(timeout=FUSE_TIMEOUT)
    except FuturesTimeout:
        return {"date": date, "status": "timeout",
                "msg": f"超过 {FUSE_TIMEOUT}s 挂死，跳过"}
    except Exception as e:  # noqa: BLE001
        return {"date": date, "status": "error",
                "msg": f"{type(e).__name__}:{str(e)[:120]}"}


def _dal_ok(date: str) -> bool:
    p = STOCK_DATA_DIR / f"Daily-Action-List-{date}.csv"
    if not p.exists():
        return False
    try:
        d = pd.read_csv(p, encoding="utf-8-sig")
        return len(d) > 0
    except Exception:
        return False


# ───────────────────────────── 回测阶段 ─────────────────────────────
def _backtest_all() -> None:
    os.environ["LOOKBACK_DAYS"] = str(LOOKBACK_DAYS)
    os.environ["HOLD_DAYS"] = os.environ.get("HOLD_DAYS", "10")
    os.environ["BACKTEST_MIN_STRATEGIES"] = "2"
    os.environ["PREPULL_INTERVAL"] = "0.2"
    os.environ["BACKTEST_INLINE_FILTER"] = "1"
    socket.setdefaulttimeout(45)
    # 中和 baostock(回测路径 sina 偶发慢时回退 baostock 会挂死)
    try:
        import baostock as bs
        bs.login = lambda: False  # type: ignore[assignment]
    except Exception:
        pass
    runpy.run_path(str(ROOT / "scripts" / "daily_backtest.py"), run_name="__main__")


# ───────────────────────────── 编排 ─────────────────────────────
def _write_result(phase: str, date: str, res: dict) -> None:
    try:
        (RESULT_DIR / phase).mkdir(parents=True, exist_ok=True)
        (RESULT_DIR / phase / f"{date}.json").write_text(
            json.dumps(res, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def run_phase(phase: str, dates: list[str]) -> None:
    if phase in ("replay", "all"):
        print(f"=== [replay] {len(dates)} 日 ===", flush=True)
        t0 = time.time()
        ok = skip = fail = 0
        for i, d in enumerate(dates):
            if _dal_ok(d) or _stock_selection_exists(d):
                print(f"  [{i+1}/{len(dates)}] {d} -> skip(已融合/已回放)", flush=True)
                skip += 1
                continue
            ok_flag, msg = _replay_one(d)
            if ok_flag:
                ok += 1
                _write_result("replay", d, {"status": "ok"})
            else:
                fail += 1
                _write_result("replay", d, {"status": "error", "msg": msg})
            print(f"  [{i+1}/{len(dates)}] {d} -> {'ok' if ok_flag else 'FAIL'} "
                  f"{msg[:60]} ({time.time()-t0:.0f}s)", flush=True)
        print(f"[replay] ok={ok} skip={skip} fail={fail} 耗时 {time.time()-t0:.1f}s",
              flush=True)

    if phase in ("fuse", "all"):
        print(f"=== [fuse] {len(dates)} 日 ===", flush=True)
        t0 = time.time()
        ok = skip = empty = timeout = fail = 0
        for i, d in enumerate(dates):
            if _dal_ok(d):
                print(f"  [{i+1}/{len(dates)}] {d} -> skip(已融合)", flush=True)
                skip += 1
                continue
            res = _fuse_run(d)
            st = res.get("status")
            if st == "ok":
                ok += 1
            elif st == "empty":
                empty += 1
            elif st == "timeout":
                timeout += 1
            else:
                fail += 1
            _write_result("fuse", d, res)
            gc.collect()
            print(f"  [{i+1}/{len(dates)}] {d} -> {st} n={res.get('n')} "
                  f"{res.get('msg','')[:60]} ({time.time()-t0:.0f}s)", flush=True)
        print(f"[fuse] ok={ok} empty={empty} timeout={timeout} fail={fail} "
              f"耗时 {time.time()-t0:.1f}s", flush=True)

    if phase in ("backtest", "all"):
        print(f"=== [backtest] LOOKBACK_DAYS={LOOKBACK_DAYS}（含 _skip_completed 续跑）===",
              flush=True)
        _backtest_all()
        print("[backtest] 完成", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="历史回放驱动(融合+回测延伸回过去一年)")
    ap.add_argument("--phase", choices=["replay", "fuse", "backtest", "all"],
                    default="all")
    ap.add_argument("--cadence", choices=["daily", "weekly"], default="weekly")
    ap.add_argument("--start", default=None, help="YYYYMMDD，默认 end-1年")
    ap.add_argument("--end", default=None, help="YYYYMMDD，默认今天")
    ap.add_argument("--limit", type=int, default=0, help="最多处理前 N 个日(冒烟用)")
    ap.add_argument("--dry-run", action="store_true", help="只打印日历不执行")
    args = ap.parse_args()

    end = args.end or datetime.now().strftime("%Y%m%d")
    start = args.start or (datetime.now() - pd.DateOffset(years=1)).strftime("%Y%m%d")
    dates = build_calendar(start, end, args.cadence)
    print(f"日历({args.cadence}): {len(dates)} 日，范围 {dates[0]}~{dates[-1]}", flush=True)
    if args.dry_run:
        for d in dates:
            print("  ", d)
        return 0
    if args.limit:
        dates = dates[: args.limit]
        print(f"--limit {args.limit} → 处理前 {len(dates)} 日", flush=True)

    run_phase(args.phase, dates)
    return 0


if __name__ == "__main__":
    sys.exit(main())
