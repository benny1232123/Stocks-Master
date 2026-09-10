"""四形态权重 A/B 对比（阶段1验收）。

对同一批样本外信号日，用 walk_forward_validator 的因果机制分别跑：
  - baseline       : 生产现状（shrinkage=0.4 常数, FLOOR=3.0）
  - v_fixed0       : shrinkage=0, FLOOR=0（等权化自适应权重，无地板保底）
  - v_dyn          : 动态收缩（按证据强度逐策略 shrinkage）+ FLOOR=3.0
  - v_dyn_floor0   : 动态收缩 + FLOOR=0

产出：
  - 控制台对比表（自适应累计/等权累计/差值/胜率/单调性三档/regime分层）
  - stock_data/strategy_improve/weight_variant_compare.json

用法：python scripts/compare_weight_variants.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from smcore.artifacts import STOCK_DATA_DIR
from smcore.strategy import adaptive_weights as aw

sys.path.insert(0, str(ROOT / "scripts"))
import walk_forward_validator as wfv  # noqa: E402


VARIANTS = [
    ("baseline",       {"shrinkage": None, "floor": None, "dynamic": False}),
    ("v_fixed0",       {"shrinkage": 0.0,  "floor": 0.0,  "dynamic": False}),
    ("v_dyn",          {"shrinkage": None, "floor": None, "dynamic": True}),
    ("v_dyn_floor0",   {"shrinkage": None, "floor": 0.0,  "dynamic": True}),
]

ZERO_NEGATIVE_EDGE = True


def _run_variant(name: str, cfg: dict) -> dict:
    saved = {k: aw.CONFIG.get(k) for k in ("FLOOR", "shrinkage_dynamic")}
    try:
        if cfg["dynamic"]:
            aw.CONFIG["shrinkage_dynamic"] = True
        res = wfv.run(shrinkage=cfg["shrinkage"], floor=cfg["floor"],
                      zero_negative_edge=ZERO_NEGATIVE_EDGE,
                      dynamic=cfg["dynamic"])
    finally:
        for k, v in saved.items():
            if v is None:
                aw.CONFIG.pop(k, None)
            else:
                aw.CONFIG[k] = v
    return res


def _fmt_row(name: str, r: dict) -> dict:
    terc = r["tercile"]
    return {
        "variant": name,
        "n_days": r["n_days"],
        "n_valid_days": r["n_valid_days"],
        "adaptive_total_pct": r["adaptive_total_pct"],
        "equal_total_pct": r["equal_total_pct"],
        "diff_pct": round(r["adaptive_total_pct"] - r["equal_total_pct"], 2),
        "adaptive_win_rate": r["adaptive_win_rate"],
        "equal_win_rate": r["equal_win_rate"],
        "tercile": [{k: t[k] for k in ("label", "n", "mean_ret", "win_rate")} for t in terc],
        "regime_table": r["regime_table"],
    }


def main() -> int:
    results = {}
    for name, cfg in VARIANTS:
        print(f"\n>>> 正在跑 {name} ...", flush=True)
        results[name] = _fmt_row(name, _run_variant(name, cfg))

    print("\n" + "=" * 76)
    print("四形态权重 A/B 对比（样本外，自适应 vs 等权，同票对比）")
    print("=" * 76)
    headers = ["variant", "n_valid", "自适应累计%", "等权累计%", "差值pp", "自适应胜率%", "等权胜率%"]
    print(f"{headers[0]:>12} {headers[1]:>8} {headers[2]:>10} {headers[3]:>10} "
          f"{headers[4]:>8} {headers[5]:>10} {headers[6]:>10}")
    for name, r in results.items():
        print(f"{name:>12} {r['n_valid_days']:>8} {r['adaptive_total_pct']:>+10.2f} "
              f"{r['equal_total_pct']:>+10.2f} {r['diff_pct']:>+8.2f} "
              f"{r['adaptive_win_rate']:>10} {r['equal_win_rate']:>10}")

    print("\n单调性三档（低/中/高权重档 样本外均值收益）：")
    for name, r in results.items():
        ts = " | ".join(f"{t['label']}:{t['mean_ret']:+.3f}% (n={t['n']})" for t in r["tercile"])
        print(f"  {name:>14}: {ts}")

    print("\n按市场状态分层（diff = 自适应累计 - 等权累计, pp）:")
    all_regimes = sorted({rg for r in results.values() for rg in r["regime_table"]})
    if all_regimes:
        print(f"  {'variant':>14}", "".join(f"{rg:>14}" for rg in all_regimes))
        for name, r in results.items():
            cells = "".join(f"{r['regime_table'].get(rg, {}).get('diff_pct', 0):>+13.2f}p"
                            for rg in all_regimes)
            print(f"  {name:>14}{cells}")
    else:
        print("  （无 regime 分层数据）")

    out_dir = STOCK_DATA_DIR / "strategy_improve"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "weight_variant_compare.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump({"variants": results}, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n对比结果已写：{out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())