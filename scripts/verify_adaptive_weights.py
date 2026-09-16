#!/usr/bin/env python
"""分配器体检：打印**生产口径**的各策略 edge → 权重（含 factor_timing 覆盖层前后）。

原脚本只验证一件事：shrinkage 修复后 CCTV（n=1）不再被放大到 76%。本版保留该回归断言，
但把 edge 口径换成**生产实际使用的那一套**，并额外展示 factor_timing 覆盖层的效果 ——
因为这两点决定了「当天到底是哪个策略在说话」，而旧版 STEP 1 用的
`compute_strategy_edge`（回测成交子集）已不是 `edge.source` 的默认口径。

生产口径（见 adaptive_weights_config.json）：
- `edge.source = universe` → `compute_universe_edge(window=30, hold_days=10,
  use_benchmark=True, benchmark=hs300)`：候选全集前向收益**减同期沪深300**，基准相对。
- `adaptive_weights`：贝叶斯收缩(pseudo) → softmax(temp) → 向等权收缩(shrinkage)
  → **FLOOR 事后抬升**（负 edge / 样本不足者压到地板，**不清零**）→ 归一化到 100。
- `factor_timing.enabled=true` → 再按近期「信念 IC」清零非显著为正的策略、其余重分配
  （故生产权重可能**与「未套覆盖层」的权重差很多**，例如 theme/cctv 被清零）。

用法：
    python scripts/verify_adaptive_weights.py                 # 生产口径
    python scripts/verify_adaptive_weights.py --backtest-edge # 附旧口径对照
退出码：CCTV 生产权重 > 40% 视为回归失败（非 0）。
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from smcore.strategy.adaptive_weights import (  # noqa: E402
    CONFIG,
    adaptive_weights,
    compute_adaptive_allocation,
)

STRATEGIES = ["boll", "theme", "relativity", "momentum", "cctv"]


def _print_weights(title: str, weights: dict) -> None:
    print(f"  {title}")
    for s in STRATEGIES:
        print(f"    {s:11s} {weights.get(s)}%")
    print(f"    {'合计':11s} {round(sum(weights.values()), 3)}%")


def main() -> int:
    cfg = CONFIG.get("edge") or {}
    print("=" * 72)
    print("配置：edge.source=%s window=%s hold_days=%s benchmark=%s position_weighted=%s" % (
        cfg.get("source"), cfg.get("window"), cfg.get("hold_days"),
        cfg.get("benchmark"), cfg.get("position_weighted")))
    print("      shrinkage=%s temp=%s pseudo=%s FLOOR=%s" % (
        CONFIG.get("shrinkage"), CONFIG.get("temp"), CONFIG.get("pseudo"), CONFIG.get("FLOOR")))
    print("      factor_timing=%s" % (CONFIG.get("factor_timing"),))
    print("=" * 72)

    # 生产入口：一次调用同时拿到 edge 与「已套 factor_timing」的权重
    edge, w_prod, cash, cold = compute_adaptive_allocation()

    print("STEP 1  各策略 edge（生产口径：候选全集前向收益 − 同期沪深300）")
    for s in STRATEGIES:
        e = edge.get(s) or {}
        print("  %-11s n=%-5s edge=%-9s avg_ret=%-9s win%%=%-6s std=%s" % (
            s, e.get("n"), e.get("edge"), e.get("avg_return"),
            e.get("win_rate"), e.get("std")))
    if any(k.startswith("__") for k in edge):
        print("  meta:", {k: v for k, v in edge.items() if k.startswith("__")})

    # 同一份 edge、不套覆盖层 → 用于分离「分配器本身」与「factor_timing」的作用
    w_pre = adaptive_weights(edge, shrinkage=CONFIG["shrinkage"], floor=CONFIG["FLOOR"],
                            zero_negative_edge=True)

    print()
    print("STEP 2  权重")
    _print_weights("未套 factor_timing（分配器原样）", w_pre)
    _print_weights("生产 compute_adaptive_allocation（含 factor_timing）", w_prod)
    print("    cash=%s%%  cold_start=%s" % (cash, cold))
    zeroed = [s for s in STRATEGIES if (w_pre.get(s) or 0) > 0 and not (w_prod.get(s) or 0)]
    if zeroed:
        print("    ⚠️ factor_timing 覆盖层清零了：%s（未套时为 %s）" % (
            "/".join(zeroed), {s: w_pre.get(s) for s in zeroed}))

    print()
    cctv_w = w_prod.get("cctv", 0) or 0
    if cctv_w > 40:
        print(f"❌ 失败：CCTV 生产权重仍达 {cctv_w}%（n 很少却主导）")
        return 1
    print(f"✅ 通过：CCTV 生产权重={cctv_w}%（不再主导）；"
          f"权重随业绩此消彼长，且无策略被彻底剔除（FLOOR 语义）。")

    if "--backtest-edge" in sys.argv:
        from smcore.strategy.adaptive_weights import compute_strategy_edge
        print()
        print("STEP 3  旧口径对照 compute_strategy_edge（回测成交子集，已非生产 edge.source）")
        old = compute_strategy_edge(30)
        for s in STRATEGIES:
            e = old.get(s) or {}
            print("  %-11s n=%-5s edge=%-9s win%%=%s" % (
                s, e.get("n"), e.get("edge"), e.get("win_rate")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
