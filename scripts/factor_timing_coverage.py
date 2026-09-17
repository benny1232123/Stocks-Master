#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""因子开关覆盖度体检（factor_timing coverage diagnostic）.

`scripts/verify_adaptive_weights.py` 只看**某一个信号日**的 mask 结果；本脚本回答一个
它答不了的问题：**在全部信号日上，每个策略到底被清零多少次？保留的那些天，究竟是
「信念 IC 显著为正」还是「样本不足/无定义 → 默认保留」？**

背景（2026-09-16）：生产权重 = boll 73 / relativity 14 / momentum 13 / theme 0 / cctv 0。
momentum 表面上有 13%，但它在融合层的 LOO 检验里不过门（+1.16pp / t=1.08）。本脚本
给出的解释是：**那 13% 没有任何证据支撑，纯粹是「无证据不判失效」的保守默认值。**

用法（在仓库根）::

    python scripts/factor_timing_coverage.py            # 打印全部信号日的覆盖分布
    python scripts/factor_timing_coverage.py --json OUT # 另存 JSON

口径与生产一致：window/min_n/z 读 `adaptive_weights.CONFIG["factor_timing"]`；
信念 IC 点来自 `factor_timing.conviction_points()`（与验证器同源，防分叉）。
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


def _verdict(s: str, pts: dict, prev_set: set[str], min_n: int, z: float) -> tuple[str, dict]:
    """单策略在给定窗口内的判定 + 明细。"""
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
    return v, {
        "n": n,
        "ic": None if ic is None else round(ic, 4),
        "crit": None if crit is None else round(crit, 4),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", metavar="OUT", help="另存完整结果到 JSON")
    ap.add_argument("--tail", type=int, default=5, help="打印最近 N 个信号日的明细（默认 5）")
    args = ap.parse_args()

    ftc = aw.CONFIG.get("factor_timing") or {}
    window = int(ftc.get("window", ft.DEFAULT_WINDOW))
    min_n = int(ftc.get("min_n", ft.DEFAULT_MIN_N))
    z = float(ftc.get("z", ft.DEFAULT_Z))
    enabled = ft.is_enabled()

    days = ft._signal_days()
    if not days:
        print("无信号日（stock_data/Daily-Action-List-*.csv 缺失）")
        return 1

    # 信念 IC 点只算一次（全历史口径较重），逐日按窗口切片复用
    all_pts = ft.conviction_points()

    cov: dict[str, dict[str, int]] = {s: {v: 0 for v in VERDICTS} for s in aw.ALL_STRATEGIES}
    tail_rows: list[tuple[str, dict]] = []
    for sd in days:
        prev = [d for d in days if d < sd][-window:]
        prev_set = set(prev)
        row = {}
        for s in aw.ALL_STRATEGIES:
            v, info = _verdict(s, all_pts, prev_set, min_n, z)
            cov[s][v] += 1
            row[s] = {"verdict": v, **info}
        if len(tail_rows) < args.tail:
            tail_rows.append((sd, row))
        else:
            tail_rows.pop(0)
            tail_rows.append((sd, row))

    print(f"# 因子开关覆盖度体检  (signal_days={len(days)}, {days[0]}~{days[-1]})")
    print(f"# enabled={enabled}  window={window}  min_n={min_n}  z={z}")
    print()
    print("## 全期判定分布（keep_evidenced=IC 显著为正 / keep_insufficient=样本不足默认保留 / zero=清零）")
    print(f"{'strategy':<12}{'keep_evi':>10}{'keep_insuf':>12}{'zero':>8}{'zero%':>8}")
    for s in aw.ALL_STRATEGIES:
        c = cov[s]
        total = max(1, sum(c.values()))
        print(
            f"{s:<12}{c['keep_evidenced']:>10}{c['keep_insufficient']:>12}"
            f"{c['zero']:>8}{c['zero'] / total * 100:>7.1f}%"
        )
    print()
    print(f"## 最近 {len(tail_rows)} 个信号日明细")
    for sd, row in tail_rows:
        print(f"\n[{sd}]")
        for s in aw.ALL_STRATEGIES:
            r = row[s]
            print(
                f"  {s:<12} n={r['n']:<3} ic={str(r['ic']):<8} crit={str(r['crit']):<8} → {r['verdict']}"
            )

    if args.json:
        payload = {
            "signal_days": {"n": len(days), "first": days[0], "last": days[-1]},
            "config": {"enabled": enabled, "window": window, "min_n": min_n, "z": z},
            "coverage": cov,
            "tail": [{"signal_date": sd, **row} for sd, row in tail_rows],
        }
        Path(args.json).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n已写入 {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
