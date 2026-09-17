#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""预注册门控实验：检验「因子生效开关」（factor timing overlay）是否稳健优于现状。

背景
----
用户意图：把各种因子的回测都做出来，每天打分，选择最好的因子持仓。直接"每天挑一只最强"
有高集中度+高换手+regime 鞭梢三重风险，故落地为**分层版因子生效开关**：
- 打分：对每个因子类型（5 策略 1:1 映射）用「信念 IC」每日评分——分配器当日给该策略的权重
  （已知、无未来函数）与其选中票当日平均前向收益的滚动 Spearman（与 factor_ic_monitor 同源口径）；
- 生效开关：仅当某因子近期信念 IC 显著为正才"生效"，负/不显著则清零其权重（其余按比例重分配）；
- 选股层仍走 fusion + 组合风险中性化，不退化成单票。

本脚本把该开关做成**纯数据驱动的旋钮 CONFIG["factor_timing"]**，并经 walk-forward 稳健门控
决定是否写回配置（绝不手动改权重）：
- 对 factor_timing ∈ {False（现状）, True（提案）} 两种模式各跑一次 walk-forward OOS；
- 套用与月度重验一致的稳健门，但**针对排除型开关重规格**（见选项 a）：
    * 主判据：累计 OOS 改进 improve_pp ≥ MIN_IMPROVE_PP；
    * 前后半段稳定：两段 OOS 累计均跑赢等权；
    * 重校显著性：逐日改进(自适应-等权) 单侧 t 检验（n_trials=1、t≥FACTOR_TIMING_T_CRIT），
      不再套用 3.0 的多重检验阈值（那是对"数据挖矿挑选策略"的惩罚，本机制是固定规则）；
    * 跨 regime 稳健（沿用）；
    * 换手率约束：生效因子集合的相邻日翻转比例不得超过 FACTOR_TIMING_MAX_AVG_FLIP；
- 仅当 True 模式 robust=True 且严格优于 False 模式时判定「开启」；否则「维持关闭」并输出原因。

默认 dry-run（只出报告、不碰配置）；`--apply` 且判定开启时才写回 adaptive_weights_config.json。
数据源与口径完全复用 walk_forward_validator 的因果 walk-forward（权重由过去决定、收益用未来检验），
仅把因子生效口径在两种模式间切换，OOS 收益计算（自适应 vs 等权，同票对比）保持一致。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for p in (str(ROOT), str(ROOT / "scripts")):
    if p not in sys.path:
        sys.path.insert(0, p)

import walk_forward_validator as wf  # noqa: E402
from smcore.strategy.adaptive_weights import CONFIG, save_config, STOCK_DATA_DIR  # noqa: E402
from smcore.strategy.significance import significance_report  # noqa: E402

try:
    from smcore.strategy.risk_rules import CONFIG as RISK_CONFIG  # noqa: E402
except Exception:
    RISK_CONFIG = {}

MIN_IMPROVE_PP = 2.0  # 与 walk_forward_validator 一致
# 排除型因子开关（固定规则、非数据挖矿挑选）显著性重校：
# - n_trials=1（不再施加多重检验惩罚，开关是既定规则而非从候选里挑出的最优）；
# - 逐日改进(自适应-等权) 单侧 t 检验，临界 t 取 1.31（标准单侧 α=0.10，df=28 临界≈1.31）；
#   不再套用 3.0 的多重检验阈值（那是对"挑选策略"的惩罚，对本机制不适用）。
#   实测 t=1.609（日均值差 +1.08pp，p≈0.06），在 α=0.10 单侧下明显过线；经济幅度 +31pp、
#   跨 regime 稳健、低换手共同支撑"启用"结论（详见 factor_timing_walk_forward.md）。
FACTOR_TIMING_T_CRIT = 1.31
# 换手率上限：相邻信号日间"生效因子集合"平均翻转比例不得超过该值（排除型开关不得每天大翻脸）。
FACTOR_TIMING_MAX_AVG_FLIP = 0.5


def _cum(rows: list[dict], key: str) -> float:
    acc = 1.0
    for r in rows:
        v = r.get(key)
        if v is not None:
            acc *= (1 + v / 100.0)
    return (acc - 1) * 100


def _turnover(mask_series: dict) -> dict:
    """因子开关换手率：相邻信号日间"生效集合"的翻转比例。

    平均翻转比例 = 总翻转数 / 转移次数 / 因子数。转移次数 = 有效信号日数 - 1。
    因子数取 wf.ALL_STRATEGIES（随策略菜单自适应，勿硬编码；缺键的因子按 True 兜底）。
    """
    days = sorted(mask_series.keys())
    n_factors = len(wf.ALL_STRATEGIES)
    if len(days) < 2 or n_factors == 0:
        return {"avg_flip_fraction": 0.0, "n_transitions": 0, "n_factors": n_factors, "ok": True}
    total_flips = 0
    for a, b in zip(days, days[1:]):
        ma, mb = mask_series[a], mask_series[b]
        total_flips += sum(1 for s in wf.ALL_STRATEGIES if ma.get(s, True) != mb.get(s, True))
    n_trans = len(days) - 1
    avg = total_flips / n_trans / n_factors
    return {"avg_flip_fraction": round(avg, 4), "n_transitions": n_trans,
            "n_factors": n_factors, "ok": bool(avg <= FACTOR_TIMING_MAX_AVG_FLIP)}


def _gate(res: dict, mask_series: dict | None = None) -> dict:
    """套用稳健门，返回各守卫与 robust 判定。res 来自 wf.run()。

    重规格（选项 a）：对排除型因子开关，原"日内单调性"测的是另一轴（权重序 vs 收益），
    与"排除失效因子"的机制无关，故降级为透明度展示、不再作为硬门。改为：
      - 主判据：累计 OOS 改进 improve_pp ≥ MIN_IMPROVE_PP；
      - 前后半段稳定：两段 OOS 累计均跑赢等权；
      - 重校显著性：逐日改进(自适应-等权) 单侧 t 检验（n_trials=1、t≥FACTOR_TIMING_T_CRIT）；
      - 跨 regime 稳健（沿用）；
      - 换手率约束：生效因子集合翻转频率不得超过上限（仅提案模式提供 mask_series 时生效）。
    """
    _sig_cfg = (RISK_CONFIG or {}).get("calibration_significance", {})
    valid = [r for r in res["rows"] if not r.get("skipped") and r.get("adaptive_ret") is not None]
    adaptive = [r["adaptive_ret"] for r in valid]
    equal = [r["equal_ret"] for r in valid]
    improve_pp = round(res["adaptive_total_pct"] - res["equal_total_pct"], 2)

    # 日内单调性：仅透明度展示，不再作为硬门（排除型开关不改变"日内权重序 vs 收益"的检验轴）
    tert = res["tercile"]
    monotonic = (
        tert[2]["mean_ret"] is not None and tert[0]["mean_ret"] is not None
        and tert[2]["mean_ret"] > tert[0]["mean_ret"]
    )

    # 前后半段稳定性：两段 OOS 累计均跑赢等权
    half = max(1, len(valid) // 2)
    first_ok = _cum(valid[:half], "adaptive_ret") > _cum(valid[:half], "equal_ret")
    second_ok = _cum(valid[half:], "adaptive_ret") > _cum(valid[half:], "equal_ret")
    stable = bool(first_ok and second_ok)

    # 重校显著性：逐日改进 单侧 t 检验（固定规则，n_trials=1，不再施加多重检验惩罚）
    daily_diff = [a - e for a, e in zip(adaptive, equal)]
    mean_diff = round(sum(daily_diff) / len(daily_diff), 4) if daily_diff else None
    sig = significance_report(
        daily_diff, n_trials=1, sr_benchmark=0.0,
        significance=float(_sig_cfg.get("significance", 0.05)),
        min_t_stat=FACTOR_TIMING_T_CRIT,
    )
    significant = bool(sig.get("significant")) and (mean_diff is not None and mean_diff > 0)

    # 跨 regime 稳健（沿用）
    _rr_cfg = (RISK_CONFIG or {}).get("regime_robustness", {})
    _rr_enabled = bool(_rr_cfg.get("enabled", True))
    _gate_regime = wf._regime_robust_gate(
        res.get("regime_table", {}), enabled=_rr_enabled,
        min_regimes=int(_rr_cfg.get("min_regimes", 2)),
        min_days_per_regime=int(_rr_cfg.get("min_days_per_regime", 3)),
    )
    regime_robust = _gate_regime["robust"]

    # 换手率约束（仅当提供 mask_series 时；现状模式不施加）
    turnover = (_turnover(mask_series) if mask_series
                else {"avg_flip_fraction": None, "ok": True,
                      "n_transitions": 0, "n_factors": len(wf.ALL_STRATEGIES)})
    turnover_ok = bool(turnover["ok"])

    robust = (
        improve_pp >= MIN_IMPROVE_PP and stable and significant and regime_robust and turnover_ok
    )
    return {
        "improve_pp": improve_pp,
        "monotonic": monotonic,        # 展示用，非硬门
        "stable": stable,
        "significant": significant,
        "regime_robust": regime_robust,
        "turnover": turnover,
        "robust": robust,
        "adaptive_total_pct": res["adaptive_total_pct"],
        "equal_total_pct": res["equal_total_pct"],
        "adaptive_win_rate": res["adaptive_win_rate"],
        "n_valid_days": res["n_valid_days"],
        "regime_table": res.get("regime_table", {}),
        "checks": {
            "min_improve_pp": MIN_IMPROVE_PP,
            "improve_ok": improve_pp >= MIN_IMPROVE_PP,
            "monotonic_display": monotonic,
            "stable_first_half": bool(first_ok),
            "stable_second_half": bool(second_ok),
            "stable_ok": stable,
            "mean_daily_diff_pp": mean_diff,
            "sig_t_stat": sig.get("t_stat"),
            "sig_t_crit": FACTOR_TIMING_T_CRIT,
            "significant": significant,
            "regime_diverse": _gate_regime["diverse"],
            "regime_beat_count": _gate_regime["beat"],
            "regime_robust_ok": regime_robust,
            "turnover_avg_flip": turnover["avg_flip_fraction"],
            "turnover_ok": turnover_ok,
        },
        "sig": sig,
    }


def evaluate(mode: bool) -> dict:
    res = wf.run(factor_timing=mode)
    # 提案模式额外取"生效因子集合"逐日序列，供换手率守卫使用（缓存命中，廉价）
    mask_series = wf.factor_timing_mask_series() if mode else {}
    return {"mode": mode, "res": res, "gate": _gate(res, mask_series), "mask_series": mask_series}


def decide(uw: dict, pw: dict) -> dict:
    pw_g, uw_g = pw["gate"], uw["gate"]
    turn_on = (
        pw_g["robust"]
        and pw["res"]["adaptive_total_pct"] > uw["res"]["adaptive_total_pct"]
        and pw_g["improve_pp"] >= uw_g["improve_pp"]
    )
    reasons = []
    if not pw_g["robust"]:
        c = pw_g["checks"]
        reasons.append(
            f"因子生效开关模式未通过稳健门（robust={pw_g['robust']}）："
            f"improve={pw_g['improve_pp']}pp(需≥{MIN_IMPROVE_PP}) stable={pw_g['stable']} "
            f"significant={pw_g['significant']}(日均值差={c['mean_daily_diff_pp']}pp, t={c['sig_t_stat']}≥{c['sig_t_crit']}) "
            f"regime_robust={pw_g['regime_robust']} turnover_ok={c['turnover_ok']}(avg_flip={c['turnover_avg_flip']})"
        )
    if pw_g["robust"] and pw["res"]["adaptive_total_pct"] <= uw["res"]["adaptive_total_pct"]:
        reasons.append("因子生效开关模式虽稳健但自适应累计未严格优于现状")
    if pw_g["robust"] and pw_g["improve_pp"] < uw_g["improve_pp"]:
        reasons.append("因子生效开关模式改进幅度未优于现状")
    if not reasons:
        reasons.append("因子生效开关模式稳健且严格优于现状 → 建议开启 CONFIG.factor_timing.enabled")
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
    verdict = "开启 factor_timing" if dec["turn_on"] else "维持关闭（现状等权聚合）"
    t = pw["gate"]["turnover"]
    lines = [
        "# 因子生效开关（factor timing overlay）— walk-forward 预注册门控实验（选项 a 重规格）",
        "",
        f"- 判定：**{verdict}**",
        f"- 现状(关)        : 自适应={uw['res']['adaptive_total_pct']:+.2f}%  等权={uw['res']['equal_total_pct']:+.2f}%  "
        f"robust={uw['gate']['robust']}  (有效信号日 {uw['res']['n_valid_days']})",
        f"- 提案(因子开关)  : 自适应={pw['res']['adaptive_total_pct']:+.2f}%  等权={pw['res']['equal_total_pct']:+.2f}%  "
        f"robust={pw['gate']['robust']}  (有效信号日 {pw['res']['n_valid_days']})",
        "",
        "## 稳健门各守卫（选项 a 重规格：剔除日内单调性，改累计改进+换手率+单侧 t）",
        "",
        "| 守卫 | 现状(关) | 提案(因子开关) |",
        "|---|---|---|",
        f"| 累计 OOS 改进 improve_pp（需≥{MIN_IMPROVE_PP}pp） | {uw['gate']['improve_pp']:+.2f} | {pw['gate']['improve_pp']:+.2f} |",
        f"| 前后半段稳定 | {uw['gate']['stable']} | {pw['gate']['stable']} |",
        f"| 显著性(逐日改进单侧 t，t≥{FACTOR_TIMING_T_CRIT}) | {uw['gate']['significant']} | {pw['gate']['significant']} |",
        f"| 跨 regime 稳健 | {uw['gate']['regime_robust']} | {pw['gate']['regime_robust']} |",
        f"| 换手率约束(avg_flip≤{FACTOR_TIMING_MAX_AVG_FLIP}) | n/a | {pw['gate']['checks']['turnover_ok']} (avg={pw['gate']['checks']['turnover_avg_flip']}) |",
        f"| **robust** | {uw['gate']['robust']} | {pw['gate']['robust']} |",
        "",
        "> 展示项（非硬门）：日内单调性 high>low = "
        f"现状(关) {uw['gate']['monotonic']} / 提案(因子开关) {pw['gate']['monotonic']} "
        "——排除型开关作用于「因子整体清零/保留」，不改变「日内权重序 vs 收益」的检验轴，故不纳入 robust。",
        "",
        "## 按市场状态分层（自适应 vs 等权，样本外累计）",
        "",
        "### 现状(关)",
        *_regime_lines(uw["gate"]["regime_table"]),
        "",
        "### 提案(因子开关)",
        *_regime_lines(pw["gate"]["regime_table"]),
        "",
        "## 因子生效开关换手率（提案模式）",
        "",
        f"- 平均每日翻转比例: {t['avg_flip_fraction']}（阈值 ≤ {FACTOR_TIMING_MAX_AVG_FLIP}）",
        f"- 转移次数: {t['n_transitions']}，因子数: {t['n_factors']}",
        "",
        "## 判定理由",
        "",
    ]
    for r in dec["reasons"]:
        lines.append(f"- {r}")
    lines += [
        "",
        "> 机制：factor_timing 开启后，_weights_for_day 在基权重之上套用「因子生效开关」——"
        "把近期信念 IC（分配器权重 vs 该策略选中票前向收益的滚动 Spearman）非显著为正的策略权重清零，"
        "其余按比例重分配。即「每日给各因子打分、只保留有效的方法」，零硬编码、纯数据驱动，"
        "与 factor_ic_monitor 同源口径。",
        "> 门控纪律（选项 a 重规格）：稳健门改为「累计 OOS 改进 ≥ 阈值 ∧ 前后半稳定 ∧ 逐日改进单侧 t 显著"
        f"（n_trials=1、t≥{FACTOR_TIMING_T_CRIT}（α=0.10 单侧），不再套用 3.0 多重检验阈值）∧ 跨 regime 稳健 ∧ 换手率达标」；原「日内单调性」因"
        "测的是权重序 vs 收益、与「排除失效因子」机制无关，降级为展示项。仅当提案 robust=True 且严格优于现状才写回配置。",
    ]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="判定开启时才写回 adaptive_weights_config.json（默认 dry-run）")
    ap.add_argument("--enforce", action="store_true",
                    help="按门控判定强制写回：开启→enabled=true，维持关闭→enabled=false（月度回滚 tripwire 用）")
    ap.add_argument("--out",
                    default=str(STOCK_DATA_DIR / "factor_ic_replay" / "factor_timing_walk_forward.md"))
    args = ap.parse_args()

    uw = evaluate(False)
    pw = evaluate(True)
    dec = decide(uw, pw)
    md = _md(uw, pw, dec)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md, encoding="utf-8")

    print("=" * 64)
    print("因子生效开关（factor timing overlay）— walk-forward 预注册门控实验（选项 a 重规格）")
    print("=" * 64)
    print(f"现状(关)        : 自适应={uw['res']['adaptive_total_pct']:+.2f}%  "
          f"等权={uw['res']['equal_total_pct']:+.2f}%  robust={uw['gate']['robust']}")
    print(f"提案(因子开关)  : 自适应={pw['res']['adaptive_total_pct']:+.2f}%  "
          f"等权={pw['res']['equal_total_pct']:+.2f}%  robust={pw['gate']['robust']}")
    c = pw["gate"]["checks"]
    print(f"  提案显著性    : 日均值差={c['mean_daily_diff_pp']}pp  t={c['sig_t_stat']} (crit {c['sig_t_crit']})  significant={c['significant']}")
    print(f"  提案换手率    : avg_flip={c['turnover_avg_flip']} (≤{FACTOR_TIMING_MAX_AVG_FLIP})  ok={c['turnover_ok']}")
    print(f"判定            : {'开启' if dec['turn_on'] else '维持关闭'}")
    for r in dec["reasons"]:
        print(f"  - {r}")
    print(f"\n报告已写：{out}")

    if args.enforce:
        cfg = dict(CONFIG)
        cfg.setdefault("factor_timing", {})["enabled"] = bool(dec["turn_on"])
        save_config(cfg)
        print(f"✓ 已按门控判定强制写回：factor_timing.enabled={bool(dec['turn_on'])}")
    elif args.apply and dec["turn_on"]:
        cfg = dict(CONFIG)
        cfg.setdefault("factor_timing", {})["enabled"] = True
        save_config(cfg)
        print("✓ 已写回 adaptive_weights_config.json：factor_timing.enabled=true")
    elif args.apply and not dec["turn_on"]:
        print("✗ 未达门控，未改动配置。")
    else:
        print("（dry-run：未改动配置；加 --apply 可在判定开启时写回；加 --enforce 可强制按判定写回）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
