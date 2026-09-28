#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""预注册 OOS A/B：ML 叠加因子接入融合评分（fuse_integration）是否改善最终清单选择。

为什么做这个：
- ML 叠加因子研究闸门已激活（2026-09-26，54 折 Rank-IC 0.189），但「激活≠接入」——
  接入融合评分需另立 OOS 门控。本脚本先声明假设/指标/闸门再出数，避免数据窥探。
- 接入开关 fuse_integration.enabled 默认 False；本脚本是「是否允许置 True」的证据来源。

预注册声明（先写死，运行时先打印，再算数）：
- 数据口径：全部回放信号日的 Daily-Action-List（生产最终清单 = 当日候选池，票池/守卫
  与生产一致）；前向收益 = walk_forward_validator 同款「次开盘买 → 持有10日开盘卖」
  （本地 k_data，零联网、零成本模型）。
- 假设 H1：在接入门控放行日，「综合评分 + ML分」重排选出的前一半票的等权前向收益
  高于「综合评分」单独重排的前一半（GATED 轨道）。
- 切分口径：中位切分（K = len(pool)//2，len<10 跳过）——隔离 ML 对既有候选池**排序**的
  边际改善，不重跑 fuse 全管线（caps/sizing 二阶效应不在本对比内，结论为方向性）。
- 三轨：A（综合评分排序）/ ON（无条件叠加 ML 分）/ GATED（仅当因果接入门控放行才叠加）。
- 接入门控（被测对象本身）：ml_factors.json 研究闸门激活 + 近20折 IC>0（因果折过滤：
  只统计 label 已实现于信号日之前的折）——与生产 evaluate_fuse_gate 完全同源。
- 决策闸门（全部满足才建议开启 enabled，且开启后须再跑一次全量 replay 确认）：
  improve_pp ≥ 2 ∧ 前后半段稳定 ∧ 逐日差异单侧 t ≥ 1.31（n_trials=1，非挖矿）
  ∧ regime 分层（≥2 个 regime 改善为正，各 n≥3）∧ 门控翻转率 ≤ 0.5。
- 诚实约束：训练/评估全用本地历史数据；ML 分在早期信号日因 min_train_days=60 中性
  （该日三轨同票，如实计入对比）。

用法：
    python scripts/run_ml_fuse_ab.py [--out stock_data/ml_factors_fuse_ab.md]
                                     [--limit N]（调试：只跑最近 N 个信号日）
"""
from __future__ import annotations

import argparse
import functools
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
for p in (str(ROOT), str(ROOT / "scripts")):
    if p not in sys.path:
        sys.path.insert(0, p)

from smcore.config.defaults import STOCK_DATA_DIR  # noqa: E402
from smcore.strategy import ml_factors as ml  # noqa: E402
from smcore.strategy.risk_rules import CONFIG  # noqa: E402
from smcore.strategy.significance import significance_report  # noqa: E402
from walk_forward_validator import (  # noqa: E402
    WF_HOLD_DAYS,
    _all_signal_days,
    _forward_return_from_kdata,
    _regime_as_of,
)

T_CRIT = 1.31            # 单侧 α=0.10（n_trials=1，预注册规则非数据挖矿）
MIN_IMPROVE_PP = 2.0     # 与 walk-forward recommend / gate_candidate 同源
MAX_FLIP = 0.5           # 门控翻转率上限（GATED 生效日占比 1-x）

PRE_REGISTRATION = """
════════════════════════════════════════════════════════════════
预注册声明（先打印，再算数 —— 防数据窥探）
════════════════════════════════════════════════════════════════
H1：接入门控放行日，「综合评分+ML分」中位切分前半的等权前向收益
    > 「综合评分」单独中位切分前半（GATED 轨道 vs A 轨道）。
口径：回放信号日 DAL 票池；前向收益 = 次开盘买→持有10日开盘卖（与
walk_forward_validator 回补同源）；三轨 A / ON / GATED；中位切分。
决策闸门（全过才建议 enabled=true，且须再跑全量 replay 确认）：
  improve_pp ≥ {min_pp} ∧ 前后半段稳定 ∧ 单侧 t ≥ {t_crit}（n_trials=1）
  ∧ ≥2 个 regime 改善为正（各 n≥3）∧ 门控翻转率 ≤ {max_flip}
════════════════════════════════════════════════════════════════
"""

MIN_REGIME_N = 3


def _kline_cache_patch():
    """进程内 LRU 缓存 read_kline_cache：A/B 扫描会对同一 code 反复读全史 K 线，
    parquet 无进程缓存，不缓存则 120 日 × ~85 训练日 × 50 票的读盘不可承受。"""
    import smcore.data.kline as kline_mod
    orig = kline_mod.read_kline_cache

    @functools.lru_cache(maxsize=2048)
    def cached(code, adjust=None, base_dir=None):
        return orig(code, adjust=adjust or kline_mod.DEFAULT_ADJUST, base_dir=base_dir)

    kline_mod.read_kline_cache = cached


def _load_dal(sd: str) -> pd.DataFrame | None:
    p = STOCK_DATA_DIR / f"Daily-Action-List-{sd}.csv"
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p, encoding="utf-8-sig")
    except Exception:
        return None
    if "股票代码" not in df.columns or "综合评分" not in df.columns or df.empty:
        return None
    df = df.copy()
    df["股票代码"] = df["股票代码"].astype(str).str.strip()
    df["综合评分"] = pd.to_numeric(df["综合评分"], errors="coerce")
    df = df.dropna(subset=["综合评分"]).reset_index(drop=True)
    return df if len(df) >= 10 else None


def _day_fwd(codes: list[str], sd: str) -> dict[str, float]:
    out = {}
    for c in codes:
        r = _forward_return_from_kdata(c, sd, WF_HOLD_DAYS)
        if r is not None:
            out[c] = float(r)
    return out


def _half_mean(fwd: dict[str, float], ranked_codes: list[str]) -> float | None:
    """中位切分前半的等权前向收益（缺失前向收益的票不参与均值）。"""
    half = ranked_codes[:max(1, len(ranked_codes) // 2)]
    vals = [fwd[c] for c in half if c in fwd]
    return float(np.mean(vals)) if len(vals) >= 3 else None


def _cum_pct(daily: list[float]) -> float:
    acc = 1.0
    for r in daily:
        acc *= (1.0 + r / 100.0)
    return (acc - 1.0) * 100.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(STOCK_DATA_DIR / "ml_factors_fuse_ab.md"))
    ap.add_argument("--limit", type=int, default=0, help="调试：只跑最近 N 个信号日")
    args = ap.parse_args()
    t0 = time.time()

    fuse_cfg = dict(CONFIG.get("ml_factors", {}).get("fuse_integration", {}))
    wf = ml._load_wf_report(fuse_cfg.get("report_path"))
    if not wf or not wf.get("fold_records"):
        print("ml_factors.json 缺失或无 fold_records —— 先跑 scripts/run_ml_factors.py --emit-json",
              flush=True)
        return 1

    print(PRE_REGISTRATION.format(min_pp=MIN_IMPROVE_PP, t_crit=T_CRIT,
                                  max_flip=MAX_FLIP), flush=True)
    print(f"walk-forward 报告：{wf.get('n_folds')} 折 IC={wf.get('mean_ic')} "
          f"ICIR={wf.get('icir')} gate.activate={wf.get('gate', {}).get('activate')}",
          flush=True)

    days = _all_signal_days()
    if args.limit:
        days = days[-args.limit:]
    print(f"信号日 {len(days)} 个（{days[0]}~{days[-1]}），kline LRU 缓存已开启", flush=True)
    _kline_cache_patch()

    rows = []
    for k, sd in enumerate(days):
        dal = _load_dal(sd)
        if dal is None:
            continue
        pool = dal["股票代码"].tolist()
        base_scores = dal.set_index("股票代码")["综合评分"].to_dict()
        fwd = _day_fwd(pool, sd)
        if len(fwd) < 6:
            continue

        gate = ml.evaluate_fuse_gate(wf, fuse_cfg, as_of=sd)
        z = ml.ml_scores_for_date(sd, pool, fuse_cfg, signal_days=days)

        ranked_a = sorted(pool, key=lambda c: base_scores.get(c, -1e9), reverse=True)
        ret_a = _half_mean(fwd, ranked_a)

        ranked_on = ranked_a
        ret_on = ret_a
        if z:
            cap = float(fuse_cfg.get("max_bonus", 10.0))
            w = float(fuse_cfg.get("weight", 3.0))
            resc = {c: base_scores.get(c, 0.0) + round(max(-cap, min(cap, w * zz)), 2)
                    for c, zz in z.items()}
            ranked_on = sorted(pool, key=lambda c: resc.get(c, -1e9), reverse=True)
            ret_on = _half_mean(fwd, ranked_on)

        gated_active = bool(gate["allow"] and z)
        ranked_g = ranked_on if gated_active else ranked_a
        ret_g = ret_on if gated_active else ret_a

        # 直接证据：ML z 与基准综合分对全池前向收益的 Rank-IC（当日）
        ic_ml = _spearman_on(pool, z, fwd) if z else None
        ic_base = _spearman_on(pool, {c: base_scores.get(c, 0.0) for c in pool}, fwd)

        rows.append({
            "sd": sd, "regime": _regime_as_of(sd), "n_pool": len(pool),
            "gate_allow": gate["allow"], "has_z": bool(z), "gated_active": gated_active,
            "ret_a": ret_a, "ret_on": ret_on, "ret_gated": ret_g,
            "ic_ml": ic_ml, "ic_base": ic_base,
        })
        if (k + 1) % 10 == 0:
            print(f"  [{k + 1}/{len(days)}] {sd} 完成（{time.time() - t0:.0f}s）", flush=True)

    if len(rows) < 20:
        print(f"有效信号日不足（{len(rows)} < 20），不出结论。", flush=True)
        return 1

    df = pd.DataFrame(rows)
    valid = df.dropna(subset=["ret_a", "ret_gated"])
    diff = (valid["ret_gated"] - valid["ret_a"]).tolist()
    improve_pp = round(_cum_pct(valid["ret_gated"].tolist())
                       - _cum_pct(valid["ret_a"].tolist()), 2)
    on_valid = df.dropna(subset=["ret_a", "ret_on"])
    on_improve = round(_cum_pct(on_valid["ret_on"].tolist())
                       - _cum_pct(on_valid["ret_a"].tolist()), 2)

    half = max(1, len(valid) // 2)
    first = _cum_pct(valid["ret_gated"].tolist()[:half]) - _cum_pct(valid["ret_a"].tolist()[:half])
    second = _cum_pct(valid["ret_gated"].tolist()[half:]) - _cum_pct(valid["ret_a"].tolist()[half:])
    stable = bool(first > 0 and second > 0)

    sig = significance_report(diff, n_trials=1, sr_benchmark=0.0,
                              significance=0.05, min_t_stat=T_CRIT)

    by_regime: dict[str, dict] = {}
    for _, r in valid.iterrows():
        g = by_regime.setdefault(r["regime"], {"g": [], "a": []})
        g["g"].append(r["ret_gated"])
        g["a"].append(r["ret_a"])
    regime_rows, regime_ok_n = [], 0
    for name, g in sorted(by_regime.items()):
        if len(g["g"]) < MIN_REGIME_N:
            continue
        d = _cum_pct(g["g"]) - _cum_pct(g["a"])
        regime_rows.append((name, len(g["g"]), d))
        if d > 0:
            regime_ok_n += 1
    regime_ok = regime_ok_n >= 2

    flip = round(1.0 - float(valid["gated_active"].mean()), 4)
    gate_pass = {
        "improve_ok": improve_pp >= MIN_IMPROVE_PP,
        "stable": stable,
        "significant": bool(sig.get("significant")) and (np.mean(diff) > 0 if diff else False),
        "regime_ok": regime_ok,
        "flip_ok": flip <= MAX_FLIP,
    }
    robust = all(gate_pass.values())

    ic_m = df["ic_ml"].dropna()
    ic_b = df["ic_base"].dropna()
    lines = [
        "# 预注册 OOS A/B：ML 叠加因子接入融合评分",
        "",
        f"- 生成：{time.strftime('%Y-%m-%d %H:%M:%S')}；信号日 {len(days)} 个，"
        f"有效对比日 **{len(valid)}**（三轨同票日 {len(df) - len(valid)} 个）",
        f"- walk-forward 研究闸门：{wf.get('n_folds')} 折 IC={wf.get('mean_ic')} "
        f"ICIR={wf.get('icir')} 激活={wf.get('gate', {}).get('activate')}",
        f"- GATED 生效日占比：{1 - flip:.1%}（门控翻转率 {flip:.1%}，上限 {MAX_FLIP:.0%}）",
        "",
        "## 一、三轨累计前向收益（中位切分前半，%）",
        "",
        "| 轨道 | 累计收益 | vs A |",
        "|---|---|---|",
        f"| A（综合评分排序） | {_cum_pct(valid['ret_a'].tolist()):+.2f} | — |",
        f"| ON（无条件叠加 ML 分） | {_cum_pct(on_valid['ret_on'].tolist()):+.2f} | {on_improve:+.2f}pp |",
        f"| GATED（接入门控放行才叠加） | {_cum_pct(valid['ret_gated'].tolist()):+.2f} | {improve_pp:+.2f}pp |",
        "",
        "## 二、直接证据：全池 Rank-IC（当日）",
        "",
        f"- ML z 平均 Rank-IC = **{ic_m.mean():+.4f}**（正占比 {float((ic_m > 0).mean()):.1%}，n={len(ic_m)}）",
        f"- 基准综合分平均 Rank-IC = {ic_b.mean():+.4f}（正占比 {float((ic_b > 0).mean()):.1%}，n={len(ic_b)}）",
        "",
        "## 三、决策闸门（预注册）",
        "",
        "| 守卫 | 值 | 通过? |",
        "|---|---|---|",
        f"| improve_pp（需≥{MIN_IMPROVE_PP}） | {improve_pp:+.2f} | {gate_pass['improve_ok']} |",
        f"| 前后半段稳定 | 前 {first:+.2f} / 后 {second:+.2f} | {stable} |",
        f"| 单侧 t（需≥{T_CRIT}） | t={sig.get('t_stat')} | {gate_pass['significant']} |",
        f"| regime 分层 | {regime_ok_n} 个改善为正 | {regime_ok} |",
        f"| 门控翻转率（≤{MAX_FLIP}） | {flip} | {gate_pass['flip_ok']} |",
        f"| **robust** | — | **{robust}** |",
        "",
        "## 四、regime 分层明细",
        "",
        "| regime | n | GATED−A (pp) |",
        "|---|---|---|",
        *[f"| {name} | {n} | {d:+.2f} |" for name, n, d in regime_rows],
        "",
        "## 五、判定与后续",
        "",
        (f"- **robust={robust}** → "
         + ("满足预注册闸门：可把 fuse_integration.enabled 置 true，"
            "并跑一次全量 replay_history 复放确认后接入生产。"
            if robust else
            "未满足预注册闸门：fuse_integration.enabled 保持 false；"
            "待样本扩容（月度重验）后重跑本脚本。")),
        "- 诚实约束：中位切分近似隔离「ML 对既有候选池排序的边际改善」，不重跑 fuse 全管线"
        "（caps/sizing 二阶效应不在对比内）；结论为方向性，开启前须全量 replay 确认。",
        "",
        "## 附：逐日明细（GATED 生效日）",
        "",
        "| 信号日 | regime | 池 | 门控 | A% | GATED% | ML IC | 基准 IC |",
        "|---|---|---|---|---|---|---|---|",
        *[
            f"| {r.sd} | {r.regime} | {r.n_pool} | {'✓' if r.gated_active else '—'} | "
            f"{r.ret_a if r.ret_a is not None else float('nan'):+.3f} | "
            f"{r.ret_gated if r.ret_gated is not None else float('nan'):+.3f} | "
            f"{r.ic_ml if r.ic_ml is not None else float('nan'):+.3f} | "
            f"{r.ic_base if r.ic_base is not None else float('nan'):+.3f} |"
            for r in valid.itertuples()
        ],
    ]
    out = Path(args.out)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"\nA={_cum_pct(valid['ret_a'].tolist()):+.2f}%  GATED={_cum_pct(valid['ret_gated'].tolist()):+.2f}%  "
          f"improve={improve_pp:+.2f}pp  t={sig.get('t_stat')}  flip={flip}  robust={robust}", flush=True)
    print(f"DONE in {time.time() - t0:.0f}s -> {out}", flush=True)
    return 0


def _spearman_on(codes: list[str], scores: dict, fwd: dict[str, float]) -> float | None:
    """scores 对当日全池前向收益的 Rank-IC（缺失收益的票剔除）。"""
    xs, ys = [], []
    for c in codes:
        if c in fwd and c in scores and scores[c] == scores[c]:
            xs.append(float(scores[c]))
            ys.append(fwd[c])
    if len(xs) < 10:
        return None
    xr = pd.Series(xs).rank().values
    yr = pd.Series(ys).rank().values
    if np.std(xr) == 0 or np.std(yr) == 0:
        return None
    return float(np.corrcoef(xr, yr)[0, 1])


if __name__ == "__main__":
    raise SystemExit(main())
