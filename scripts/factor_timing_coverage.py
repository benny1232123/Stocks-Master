#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""因子开关覆盖度体检（factor_timing coverage diagnostic）.

`scripts/verify_adaptive_weights.py` 只看**某一个信号日**的 mask 结果；本脚本回答一个
它答不了的问题：**在全部信号日上，每个策略到底被清零多少次？保留的那些天，究竟是
「信念 IC 显著为正」还是「样本不足/无定义 → 默认保留」？**

更深入一层（2026-09-17 新增 `--sweep`）：**检验力够不够？** 全期 `keep_evidenced`
全为 0，可能只是因为 window=10 太短、DAL 贡献稀疏（momentum 近 10 日只在 3 天有票 →
n 永远 < min_n=5，根本来不及被评估）。`--sweep` 把窗口拉到 10/20/30，固定 min_n/z，
只变窗口宽度，看 n 与 keep_evidenced 是否随之出现——这正是「扩窗能否让动量依证据被清零」的
实证检验（详见 2026-09-17 讨论：扩窗是唯一不改代码就让动量被合规排除的杠杆）。

口径与生产一致：window/min_n/z 读 `adaptive_weights.CONFIG["factor_timing"]`；
信念 IC 点来自 `factor_timing.conviction_points()`（与验证器同源，防分叉）。
`--sweep` 只**报告**，不改任何配置；3 个窗口 = 3 次观测，若据此翻转开关须按 n_trials=3
调高 t 临界（本脚本不自动翻转）。

用法（在仓库根）::

    python scripts/factor_timing_coverage.py             # 配置窗口单点体检
    python scripts/factor_timing_coverage.py --sweep      # 窗口 10/20/30 对比
    python scripts/factor_timing_coverage.py --window 30  # 指定窗口
    python scripts/factor_timing_coverage.py --json OUT  # 另存 JSON
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from smcore.strategy import adaptive_weights as aw  # noqa: E402
from smcore.strategy import factor_timing as ft  # noqa: E402

VERDICTS = ("keep_evidenced", "keep_insufficient", "zero")


def _verdict(s: str, pts: dict, prev_set: set[str], min_n: int, z: float) -> tuple[str, int]:
    """单策略在给定窗口内的判定 + n。"""
    series = [(w, r) for (d, w, r) in pts.get(s, []) if d in prev_set]
    n = len(series)
    ic = ft.spearman_ic([w for w, _ in series], [r for _, r in series]) if n >= 3 else None
    crit = (z / (n - 1) ** 0.5) if n > 1 else None
    if n < min_n or ic is None:
        v = "keep_insufficient"
    elif ic > 0 and abs(ic) >= crit:
        v = "keep_evidenced"
    else:
        v = "zero"
    return v, n


def evaluate(window: int, days: list[str], all_pts: dict, min_n: int, z: float):
    """返回 (cov, ns, eval_days)。cov[strategy][verdict]=天数；ns 为每信号日 n 列表；
    eval_days[strategy]=n>=min_n 的天数（真正被评估过的天数）。"""
    cov = {s: {v: 0 for v in VERDICTS} for s in aw.ALL_STRATEGIES}
    ns = {s: [] for s in aw.ALL_STRATEGIES}
    eval_days = {s: 0 for s in aw.ALL_STRATEGIES}
    for sd in days:
        prev = [d for d in days if d < sd][-window:]
        prev_set = set(prev)
        for s in aw.ALL_STRATEGIES:
            v, n = _verdict(s, all_pts, prev_set, min_n, z)
            cov[s][v] += 1
            ns[s].append(n)
            if n >= min_n:
                eval_days[s] += 1
    return cov, ns, eval_days


def _mean(xs: list[int]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _print_window(window, cov, ns, eval_days, min_n):
    print(f"### window={window}  (min_n={min_n})")
    print(f"{'strategy':<12}{'evi':>5}{'insuf':>7}{'zero':>7}{'avg_n':>8}{'days_n>=min':>13}")
    tot_evi = 0
    for s in aw.ALL_STRATEGIES:
        c = cov[s]
        tot_evi += c["keep_evidenced"]
        print(
            f"{s:<12}{c['keep_evidenced']:>5}{c['keep_insufficient']:>7}{c['zero']:>7}"
            f"{_mean(ns[s]):>8.2f}{eval_days[s]:>13}"
        )
    n_days = len(ns[aw.ALL_STRATEGIES[0]])
    print(f"  => 全期 keep_evidenced 合计 = {tot_evi} （共 {len(aw.ALL_STRATEGIES)*n_days} 个判定）")
    print()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", type=int, default=None, help="指定窗口（默认读 config）")
    ap.add_argument("--sweep", action="store_true", help="跑 window=10/20/30 对比")
    ap.add_argument("--windows", type=str, default="10,20,30", help="--sweep 的窗口列表，逗号分隔")
    ap.add_argument("--json", metavar="OUT", help="另存完整结果到 JSON")
    ap.add_argument("--tail", type=int, default=0, help="单点模式下打印最近 N 个信号日明细")
    args = ap.parse_args()

    ftc = aw.CONFIG.get("factor_timing") or {}
    min_n = int(ftc.get("min_n", ft.DEFAULT_MIN_N))
    z = float(ftc.get("z", ft.DEFAULT_Z))
    enabled = ft.is_enabled()

    days = ft._signal_days()
    if not days:
        print("无信号日（stock_data/Daily-Action-List-*.csv 缺失）")
        return 1

    all_pts = ft.conviction_points()  # 只算一次，逐窗口复用

    cfg_window = int(ftc.get("window", ft.DEFAULT_WINDOW))
    if args.window:
        windows = [args.window]
    elif args.sweep:
        windows = [int(x) for x in args.windows.split(",")]
    else:
        windows = [cfg_window]

    out: dict = {
        "signal_days": {"n": len(days), "first": days[0], "last": days[-1]},
        "config": {"enabled": enabled, "min_n": min_n, "z": z, "cfg_window": cfg_window},
        "windows": {},
    }

    print(f"# 因子开关覆盖度体检  (signal_days={len(days)}, {days[0]}~{days[-1]})")
    print(f"# enabled={enabled}  min_n={min_n}  z={z}  cfg_window={cfg_window}")
    print()

    single = len(windows) == 1
    for window in windows:
        cov, ns, eval_days = evaluate(window, days, all_pts, min_n, z)
        out["windows"][str(window)] = {
            "coverage": cov,
            "mean_n": {s: round(_mean(ns[s]), 3) for s in aw.ALL_STRATEGIES},
            "days_evaluated": eval_days,
        }
        _print_window(window, cov, ns, eval_days, min_n)

        if args.tail > 0 and single:
            tail_rows = []
            for sd in days[-args.tail:]:
                prev = [d for d in days if d < sd][-window:]
                prev_set = set(prev)
                row = {}
                for s in aw.ALL_STRATEGIES:
                    v, n = _verdict(s, all_pts, prev_set, min_n, z)
                    pairs = [(w, r) for (d, w, r) in all_pts.get(s, []) if d in prev_set]
                    ic = ft.spearman_ic([w for w, _ in pairs], [r for _, r in pairs]) if n >= 3 else None
                    crit = (z / (n - 1) ** 0.5) if n > 1 else None
                    row[s] = {"verdict": v, "n": n, "ic": None if ic is None else round(ic, 4),
                              "crit": None if crit is None else round(crit, 4)}
                tail_rows.append({"signal_date": sd, **row})
            print(f"## 最近 {len(tail_rows)} 个信号日明细 (window={window})")
            for r in tail_rows:
                print(f"\n[{r['signal_date']}]")
                for s in aw.ALL_STRATEGIES:
                    d = r[s]
                    print(f"  {s:<12} n={d['n']:<3} ic={str(d['ic']):<8} crit={str(d['crit']):<8} -> {d['verdict']}")

    if args.json:
        Path(args.json).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n已写入 {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
