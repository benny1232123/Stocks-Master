"""连续组合回测：按时间顺序串起全部历史信号日，生成一条连续净值曲线。

用法:
    python scripts/continuous_backtest.py --variant baseline [--limit N]
输出:
    stock_data/strategy_improve/continuous_{variant}_equity.csv
    stock_data/strategy_improve/continuous_{variant}_metrics.csv

说明:
    - 复用 daily_backtest._backtest_one 内核，输出隔离到 strategy_improve/，不污染生产 Multi-Backtest-*
    - portfolio_curve=None：关闭组合级回撤熔断，让 A/B 变体差异只来自权重/风控/现金逻辑本身
    - K 线进程内缓存注入与 daily_backtest.main() 一致
"""
import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

import smcore.strategy.adaptive_weights as aw_mod
from smcore.artifacts import STOCK_DATA_DIR
from smcore.backtest.continuous import build_continuous_curve, compute_metrics
from scripts.daily_backtest import (
    _backtest_one,
    _collect_all_candidate_codes,
    _filter_incomplete,
    _load_equity_series,
    _parse_signal_date,
)
import smcore.data.kline as kline_mod

# 变体预设：覆盖 adaptive_weights.CONFIG（进程内生效，跑完进程退出即恢复）。
# 四形态（stage1）：baseline=现状；v_fixed0=去FLOOR+固定shrink0；
# v_dyn=动态shrinkage；v_dyn_floor0=动态+FLOOR0。
VARIANT_PRESETS = {
    "baseline": {},
    "v_fixed0": {
        "aw": {"FLOOR": 0.0, "shrinkage_dynamic": False, "shrinkage": 0.0},
    },
    "v_dyn": {
        "aw": {"shrinkage_dynamic": True},
    },
    "v_dyn_floor0": {
        "aw": {"FLOOR": 0.0, "shrinkage_dynamic": True},
    },
}


def _apply_variant(name: str):
    overrides = VARIANT_PRESETS.get(name, {})
    for k, v in overrides.get("aw", {}).items():
        aw_mod.CONFIG[k] = v


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--variant", default="baseline")
    ap.add_argument("--limit", type=int, default=0, help="只跑最近 N 个信号日（0=全部）")
    args = ap.parse_args()

    outdir = STOCK_DATA_DIR / "strategy_improve"
    (outdir / "backtests").mkdir(parents=True, exist_ok=True)

    _apply_variant(args.variant)

    lists = [(p, _parse_signal_date(p.name)) for p in sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))]
    lists = [x for x in lists if x[1] is not None]
    # 剔除策略残缺的信号日（与生产 daily_backtest 同口径）
    lists, _ = _filter_incomplete(lists, int(os.environ.get("BACKTEST_MIN_STRATEGIES", "2")))
    if args.limit:
        lists = lists[-args.limit:]
    if not lists:
        print("[continuous] 无有效信号日，退出")
        return 1
    print(f"[continuous:{args.variant}] 信号日 {len(lists)} 个: {lists[0][1]} ~ {lists[-1][1]}")

    # ── K 线进程内缓存注入（与 daily_backtest.main 一致）──
    _orig_fetch = kline_mod.fetch_daily_k
    _kcache: dict = {}

    def _cached_fetch(code, start, end, *a, **k):
        key = (str(code), str(start), str(end), k.get("adjust", "qfq"))
        if key not in _kcache:
            _kcache[key] = _orig_fetch(code, start, end, *a, **k)
        return _kcache[key]

    kline_mod.fetch_daily_k = _cached_fetch

    # ── 预拉全量 K 线（温和间隔，保护上游）──
    all_codes = _collect_all_candidate_codes(lists)
    if all_codes:
        from datetime import timedelta

        today = pd.Timestamp.today().date()
        global_start = min(sd for _, sd in lists) - timedelta(days=120)
        global_end = today
        _interval = float(os.environ.get("PREPULL_INTERVAL", "0.3"))
        print(f"[预拉K线] 共 {len(all_codes)} 只，范围 {global_start} ~ {global_end}, 串行(间隔 {_interval}s)")
        t_pre = time.time()
        ok_cnt = 0
        for i, code in enumerate(sorted(all_codes)):
            try:
                df = kline_mod.fetch_daily_k(code, global_start, global_end, adjust="qfq")
                if df is not None and not df.empty:
                    ok_cnt += 1
            except Exception:
                pass
            if (i + 1) % 10 == 0 or (i + 1) == len(all_codes):
                elapsed = time.time() - t_pre
                print(f"  [预拉 {i+1}/{len(all_codes)}] ({100*(i+1)/len(all_codes):.0f}%) "
                      f"已用 {elapsed:.0f}s 成功{ok_cnt}", flush=True)
            if _interval > 0 and i + 1 < len(all_codes):
                time.sleep(_interval)
        print(f"[预拉K线] 完成: {ok_cnt}/{len(all_codes)} 只成功, 耗时 {time.time()-t_pre:.0f}s")

    # ── 逐信号日回测 ──
    hold_days = int(os.environ.get("HOLD_DAYS", "12"))
    sleeves: dict[str, dict[str, float]] = {}
    for i, (path, sd) in enumerate(lists, 1):
        try:
            _backtest_one(path, sd, hold_days, out_dir=outdir / "backtests")
        except Exception as e:
            print(f"  [{i}/{len(lists)}] {sd:%Y%m%d} 失败: {e}", flush=True)
            continue
        tag = f"{sd:%Y%m%d}"
        eq = _load_equity_series(outdir / "backtests" / f"Multi-Backtest-{tag}-equity.csv")
        if eq:
            sleeves[tag] = eq
        n_trades = None
        try:
            import csv
            with open(outdir / "backtests" / f"Multi-Backtest-{tag}-summary.csv", encoding="utf-8-sig") as f:
                rows = list(csv.DictReader(f))
            if rows:
                n_trades = rows[0].get("num_trades", "?")
        except Exception:
            pass
        print(f"  [{i}/{len(lists)}] {tag} ok sleeve日={len(eq)} 笔数={n_trades}", flush=True)

    if not sleeves:
        print("[continuous] 无有效 sleeve，退出")
        return 1

    nav = build_continuous_curve(sleeves)
    nav.index = pd.DatetimeIndex(nav.index)
    metrics = compute_metrics(nav)

    nav.to_csv(outdir / f"continuous_{args.variant}_equity.csv", header=["nav"])
    _flat = {k: v for k, v in metrics.items() if k != "monthly"}
    pd.DataFrame([_flat]).to_csv(outdir / f"continuous_{args.variant}_metrics.csv", index=False)
    pd.DataFrame({"date": nav.index.strftime("%Y-%m-%d"), "nav": nav.values}).to_csv(
        outdir / f"continuous_{args.variant}_dates.csv", index=False)
    print(f"[ok] {args.variant} 年化={_flat.get('annual_return_pct')}% "
          f"回撤={_flat.get('max_drawdown_pct')}% 夏普={_flat.get('sharpe')} 胜率={_flat.get('win_rate_pct')}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())