#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""预注册门控实验：检验「仓位加权 edge 聚合」（edge.position_weighted）是否稳健优于等权口径。

背景
----
因子归因 OOS（factor_oos_cctv_vs_fusion.md）显示：CCTV（事件·舆情）在「仓位加权(pw)」口径下
3/3 窗口跑赢融合组合，但在「日加权(dw)」口径下跑输（CCTV 交易集中在少数信号日、单日回撤大）。
这提示当前 edge 聚合用的**等权平均（dw）**稀释了 CCTV 这类「高资本效率」因子的真实贡献。

本脚本把「按 pw 贡献降权非 CCTV」落地为一个**纯数据驱动的旋钮 edge.position_weighted**，
并经 walk-forward 稳健门控决定是否写回配置（绝不手动改权重）：
- 对 position_weighted ∈ {False（现状）, True（提案）} 两种模式各跑一次 walk-forward OOS；
- 套用与月度重验一致的稳健门（improve_pp≥2 ∧ 单调 ∧ 前后半稳定 ∧ 显著 ∧ 跨 regime 稳健）；
- 仅当 True 模式 robust=True 且严格优于 False 模式（自适应累计更高、改进幅度更大）时，
  判定「开启」；否则「维持关闭」并输出原因。

默认 dry-run（只出报告、不碰配置）；`--apply` 且判定开启时才写回 adaptive_weights_config.json。

数据源与口径：完全复用 walk_forward_validator 的因果 walk-forward（权重由过去决定、收益用未来检验），
仅把 edge 聚合口径在两种模式间切换，OOS 收益计算（自适应 vs 等权，同票对比）保持一致。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
for p in (str(ROOT), str(ROOT / "scripts")):
    if p not in sys.path:
        sys.path.insert(0, p)

import walk_forward_validator as wf  # noqa: E402
from smcore.strategy.adaptive_weights import CONFIG, save_config, STOCK_DATA_DIR  # noqa: E402
from smcore.strategy.significance import significance_report, sharpe_ratio  # noqa: E402

try:
    from smcore.strategy.risk_rules import CONFIG as RISK_CONFIG  # noqa: E402
except Exception:
    RISK_CONFIG = {}

MIN_IMPROVE_PP = 2.0  # 与 walk_forward_validator 一致


def _cum(rows: list[dict], key: str) -> float:
    acc = 1.0
    for r in rows:
        v = r.get(key)
        if v is not None:
            acc *= (1 + v / 100.0)
    return (acc - 1) * 100


def _gate(res: dict) -> dict:
    """套用稳健门，返回各守卫与 robust 判定。res 来自 wf.run()。"""
    valid = [r for r in res["rows"] if not r.get("skipped") and r.get("adaptive_ret") is not None]
    adaptive = [r["adaptive_ret"] for r in valid]
    equal = [r["equal_ret"] for r in valid]
    improve_pp = round(res["adaptive_total_pct"] - res["equal_total_pct"], 2)

    tert = res["tercile"]
    monotonic = (
        tert[2]["mean_ret"] is not None and tert[0]["mean_ret"] is not None
        and tert[2]["mean_ret"] > tert[0]["mean_ret"]
    )

    # 前后半段稳定性：两段自适应累计收益均 > 等权累计
    half = max(1, len(valid) // 2)
    first_ok = _cum(valid[:half], "adaptive_ret") > _cum(valid[:half], "equal_ret")
    second_ok = _cum(valid[half:], "adaptive_ret") > _cum(valid[half:], "equal_ret")
    stable = bool(first_ok and second_ok)

    # 统计显著性守卫
    _sig_cfg = (RISK_CONFIG or {}).get("calibration_significance", {})
    _sig_enabled = bool(_sig_cfg.get("enabled", True))
    sig = significance_report(
        adaptive, n_trials=16,
        sr_benchmark=(sharpe_ratio(equal) or 0.0),
        significance=float(_sig_cfg.get("significance", 0.05)),
        min_t_stat=float(_sig_cfg.get("min_t_stat", 3.0)),
    )
    significant = (not _sig_enabled) or bool(sig.get("significant"))

    # 跨 regime 稳健
    _rr_cfg = (RISK_CONFIG or {}).get("regime_robustness", {})
    _rr_enabled = bool(_rr_cfg.get("enabled", True))
    _gate_regime = wf._regime_robust_gate(
        res.get("regime_table", {}), enabled=_rr_enabled,
        min_regimes=int(_rr_cfg.get("min_regimes", 2)),
        min_days_per_regime=int(_rr_cfg.get("min_days_per_regime", 3)),
    )
    regime_robust = _gate_regime["robust"]

    robust = (
        improve_pp >= MIN_IMPROVE_PP and monotonic and stable and significant and regime_robust
    )
    return {
        "improve_pp": improve_pp,
        "monotonic": monotonic,
        "stable": stable,
        "significant": significant,
        "regime_robust": regime_robust,
        "robust": robust,
        "adaptive_total_pct": res["adaptive_total_pct"],
        "equal_total_pct": res["equal_total_pct"],
        "adaptive_win_rate": res["adaptive_win_rate"],
        "n_valid_days": res["n_valid_days"],
        "regime_table": res.get("regime_table", {}),
        "checks": {
            "min_improve_pp": MIN_IMPROVE_PP,
            "improve_ok": improve_pp >= MIN_IMPROVE_PP,
            "monotonic": monotonic,
            "stable_first_half": bool(first_ok),
            "stable_second_half": bool(second_ok),
            "stable_ok": stable,
            "regime_diverse": _gate_regime["diverse"],
            "regime_beat_count": _gate_regime["beat"],
            "regime_robust_ok": regime_robust,
            "significant": significant,
        },
        "sig": sig,
    }


def evaluate(mode: bool) -> dict:
    res = wf.run(position_weighted=mode)
    return {"mode": mode, "res": res, "gate": _gate(res)}


def decide(uw: dict, pw: dict) -> dict:
    pw_g, uw_g = pw["gate"], uw["gate"]
    turn_on = (
        pw_g["robust"]
        and pw["res"]["adaptive_total_pct"] > uw["res"]["adaptive_total_pct"]
        and pw_g["improve_pp"] >= uw_g["improve_pp"]
    )
    reasons = []
    if not pw_g["robust"]:
        reasons.append(
            f"pw 模式未通过稳健门（robust={pw_g['robust']}）："
            f"improve={pw_g['improve_pp']}pp(需≥{MIN_IMPROVE_PP}) monotonic={pw_g['monotonic']} "
            f"stable={pw_g['stable']} significant={pw_g['significant']} regime_robust={pw_g['regime_robust']}"
        )
    if pw_g["robust"] and pw["res"]["adaptive_total_pct"] <= uw["res"]["adaptive_total_pct"]:
        reasons.append("pw 模式虽稳健但自适应累计未严格优于现状")
    if pw_g["robust"] and pw_g["improve_pp"] < uw_g["improve_pp"]:
        reasons.append("pw 模式改进幅度未优于现状")
    if not reasons:
        reasons.append("pw 模式稳健且严格优于现状 → 建议开启 edge.position_weighted")
    return {"turn_on": turn_on, "reasons": reasons}


def _regime_lines(table: dict) -> list[str]:
    if not table:
        return ["（无市场状态分层数据）"]
    out = []
    for rg, d in sorted(table.items(), key=lambda x: -x[1]["diff_pct"]):
        flag = "✓跑赢" if d["diff_pct"] > 0 else "✗跑输"
        out.append(
            f"  {rg:>6}  n={d['n_days']:>3}  自适应={d['adaptive_pct']:+.2f}%  "
            f"等权={d['equal_pct']:+.2f}%  差值={d['diff_pct']:+.2f}%  {flag}"
        )
    return out


def _md(uw: dict, pw: dict, dec: dict) -> str:
    verdict = "开启 edge.position_weighted" if dec["turn_on"] else "维持关闭（现状等权聚合）"
    lines = [
        "# 仓位加权 edge 聚合 — walk-forward 预注册门控实验",
        "",
        f"- 判定：**{verdict}**",
        f"- 现状(等权)      : 自适应={uw['res']['adaptive_total_pct']:+.2f}%  等权={uw['res']['equal_total_pct']:+.2f}%  "
        f"robust={uw['gate']['robust']}  (有效信号日 {uw['res']['n_valid_days']})",
        f"- 提案(仓位加权)  : 自适应={pw['res']['adaptive_total_pct']:+.2f}%  等权={pw['res']['equal_total_pct']:+.2f}%  "
        f"robust={pw['gate']['robust']}  (有效信号日 {pw['res']['n_valid_days']})",
        "",
        "## 稳健门各守卫",
        "",
        "| 守卫 | 现状(等权) | 提案(仓位加权) |",
        "|---|---|---|",
        f"| 改进幅度 improve_pp（需≥{MIN_IMPROVE_PP}pp） | {uw['gate']['improve_pp']:+.2f} | {pw['gate']['improve_pp']:+.2f} |",
        f"| 样本外单调性 high>low | {uw['gate']['monotonic']} | {pw['gate']['monotonic']} |",
        f"| 前后半段稳定 | {uw['gate']['stable']} | {pw['gate']['stable']} |",
        f"| 统计显著 | {uw['gate']['significant']} | {pw['gate']['significant']} |",
        f"| 跨 regime 稳健 | {uw['gate']['regime_robust']} | {pw['gate']['regime_robust']} |",
        f"| **robust** | {uw['gate']['robust']} | {pw['gate']['robust']} |",
        "",
        "## 按市场状态分层（自适应 vs 等权，样本外累计）",
        "",
        "### 现状(等权)",
        *_regime_lines(uw["gate"]["regime_table"]),
        "",
        "### 提案(仓位加权)",
        *_regime_lines(pw["gate"]["regime_table"]),
        "",
        "## 判定理由",
        "",
    ]
    for r in dec["reasons"]:
        lines.append(f"- {r}")
    lines += [
        "",
        "> 机制：edge.position_weighted 开启后，compute_universe_edge 把每只候选票的前向超额收益"
        "按 DAL「建议仓位%」加权再聚合到各策略 edge（替代等权平均），使权重决策对齐"
        "「每元资本的边际贡献」。零硬编码、纯数据驱动，与因子归因 OOS 同源口径。",
        "> 门控纪律：仅当提案模式 robust=True 且严格优于现状时才允许写回配置；否则维持关闭，"
        "交由下一轮月度 walk-forward 重验复核。",
    ]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="判定开启时才写回 adaptive_weights_config.json（默认 dry-run）")
    ap.add_argument("--out",
                    default=str(STOCK_DATA_DIR / "factor_ic_replay" / "pw_edge_walk_forward.md"))
    args = ap.parse_args()

    uw = evaluate(False)
    pw = evaluate(True)
    dec = decide(uw, pw)
    md = _md(uw, pw, dec)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md, encoding="utf-8")

    print("=" * 64)
    print("仓位加权 edge 聚合 — walk-forward 预注册门控实验")
    print("=" * 64)
    print(f"现状(等权)      : 自适应={uw['res']['adaptive_total_pct']:+.2f}%  "
          f"等权={uw['res']['equal_total_pct']:+.2f}%  robust={uw['gate']['robust']}")
    print(f"提案(仓位加权)  : 自适应={pw['res']['adaptive_total_pct']:+.2f}%  "
          f"等权={pw['res']['equal_total_pct']:+.2f}%  robust={pw['gate']['robust']}")
    print(f"判定            : {'开启' if dec['turn_on'] else '维持关闭'}")
    for r in dec["reasons"]:
        print(f"  - {r}")
    print(f"\n报告已写：{out}")

    if args.apply and dec["turn_on"]:
        cfg = dict(CONFIG)
        cfg.setdefault("edge", {})["position_weighted"] = True
        save_config(cfg)
        print("✓ 已写回 adaptive_weights_config.json：edge.position_weighted=true")
    elif args.apply and not dec["turn_on"]:
        print("✗ 未达门控，未改动配置。")
    else:
        print("（dry-run：未改动配置；加 --apply 可在判定开启时写回）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
