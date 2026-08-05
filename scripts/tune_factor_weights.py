#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""factor_scoring 权重的 walk-forward 稳健性调优（离线、因果、无未来函数）。

与 walk_forward_validator 同口径：用本地 k_data 缓存计算「信号日次开盘买入 → hold_days
后开盘卖出」的前向收益作为地面真值（不联网、不重跑引擎）。

调优对象：factor_scoring 的 (scale × 因子权重组合)。对每个候选配置：
- 在每个信号日 Ti，用该配置算候选票因子分，按因子分排序取 头 N / 尾 N；
- 计算 头N均值 − 尾N均值 的多空价差（因子预测力的 IC 代理）；
- 样本外聚合：平均价差、按因子分三档的单调性（高档>低档）、前后半段稳定性。
仅当「改进幅度 ≥ 阈值 且 单调 且 前后半稳定」才视为稳健，自动写回 risk_config.json。

基本面因子（quality/value/fundflow）由 use_fundamentals 控制：本沙箱无网→缓存为空→
这些因子贡献恒为 0，调优自动聚焦动量类权重；在联网主机填充缓存后，调优会自动纳入它们
（同一套框架，无需改脚本）。

用法：
  python scripts/tune_factor_weights.py                 # 调优并打印/写回稳健配置
  VE_TOPN=8 VE_HOLD_DAYS=10 python scripts/tune_factor_weights.py
  VE_DRYRUN=1 python scripts/tune_factor_weights.py     # 只评估不改配置
"""
from __future__ import annotations

import glob
import json
import os
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy.factor_scoring import compute_factor_scores  # noqa: E402
from smcore.strategy.risk_rules import (  # noqa: E402
    CONFIG as RISK_CONFIG,
    compute_factor_scoring_params,
    save_risk_config,
)
from smcore.utils.code import format_stock_code  # noqa: E402

try:
    from smcore.strategy.risk_rules import STOCK_DATA_DIR
except Exception:  # pragma: no cover
    from smcore.config.defaults import PROJECT_ROOT
    STOCK_DATA_DIR = PROJECT_ROOT / "stock_data"

TOPN = int(os.environ.get("VE_TOPN", "8"))
HOLD_DAYS = int(os.environ.get("VE_HOLD_DAYS", "10"))
DRY_RUN = os.environ.get("VE_DRYRUN", "0") == "1"
MIN_IMPROVE_PP = 0.05  # 相对当前配置，平均多空价差至少提升 0.05pp 才视为稳健改进

_SCALES = [3.0, 4.0, 5.0, 6.0, 8.0]

# 因子权重预设（仅动量类在本沙箱有效；基本面权重在联网填充缓存后生效）
_BASE = dict(w_momentum_20=1.0, w_momentum_60=0.7, w_rel_strength=0.6,
             w_volatility=-0.4, w_liquidity=0.3,
             w_quality=0.0, w_value=0.0, w_fund_flow=0.0)
_PRESETS = {
    "base": _BASE,
    "momentum_heavy": dict(_BASE, w_momentum_20=1.4, w_momentum_60=1.0, w_rel_strength=0.8),
    "rs_heavy": dict(_BASE, w_rel_strength=1.2, w_momentum_20=0.8),
    "lowvol_heavy": dict(_BASE, w_volatility=-0.9, w_liquidity=0.5),
    "quality_lean": dict(_BASE, w_quality=0.8, w_value=0.4),
    "value_lean": dict(_BASE, w_value=1.0, w_quality=0.3),
}


def _all_dal_days() -> list[str]:
    days = []
    for p in sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv")):
        m = re.search(r"(\d{8})", p.name)
        if m:
            days.append(m.group(1))
    return days


_KDATA_CACHE: dict[str, pd.DataFrame] = {}


def _load_cached_kdata(code: str) -> pd.DataFrame:
    if code in _KDATA_CACHE:
        return _KDATA_CACHE[code]
    cache = STOCK_DATA_DIR / "k_data" / f"{format_stock_code(code)}_qfq_full.csv"
    df = pd.DataFrame()
    if cache.exists():
        try:
            df = pd.read_csv(cache)
        except Exception:
            df = pd.DataFrame()
    if not df.empty:
        for col in ("open", "high", "low", "close"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date", "open"]).sort_values("date").reset_index(drop=True)
    _KDATA_CACHE[code] = df
    return df


def _forward_return(code: str, sd: str) -> float | None:
    df = _load_cached_kdata(code)
    if df.empty:
        return None
    future = df[df["date"] > pd.to_datetime(sd)]
    if future.empty:
        return None
    buy = float(future.iloc[0]["open"])
    if buy <= 0:
        return None
    idx = min(HOLD_DAYS, len(future) - 1)
    sell = float(future.iloc[idx]["open"])
    return (sell - buy) / buy * 100.0


def _day_picks(sd: str) -> tuple[list[tuple[str, float]], dict[str, float]]:
    """返回 (picks, base_scores)。picks=[(code, forward_return)]；base_scores 用 DAL 综合评分。

    综合评分作为「基础分」，因子分按 scale 加权后叠加（与生产 fusion.py 一致：
    综合评分 + scale·因子分），再按合成分取头 N，使 scale 在本口径下可真正调优。
    """
    dal = STOCK_DATA_DIR / f"Daily-Action-List-{sd}.csv"
    if not dal.exists():
        return [], {}
    try:
        d = pd.read_csv(dal, encoding="utf-8-sig")
    except Exception:
        return [], {}
    if "股票代码" not in d.columns:
        return [], {}
    picks = []
    base: dict[str, float] = {}
    for _, r in d.iterrows():
        c = str(r.get("股票代码")).strip()
        if not c:
            continue
        rp = _forward_return(c, sd)
        if rp is None:
            continue
        picks.append((c, rp))
        try:
            base[c] = float(r.get("综合评分")) if pd.notna(r.get("综合评分")) else 0.0
        except (TypeError, ValueError):
            base[c] = 0.0
    return picks, base


def _eval_config(params: dict, picks_by_day: dict, raw_by_day: dict, base_by_day: dict) -> dict:
    """融合集成口径评估某配置：综合评分 + scale·因子分 排序取头 N 的前向收益。

    返回该配置下「头 N 组合前向收益」的总/前半/后半均值，以及多空价差诊断。
    picks_by_day / raw_by_day / base_by_day 为预计算（与配置无关），避免重复读 CSV。
    """
    port: list[tuple[str, float]] = []      # (day, topN portfolio return)
    spreads: list[tuple[str, float]] = []
    for sd, picks in picks_by_day.items():
        if len(picks) < 2 * TOPN:
            continue
        codes = [c for c, _ in picks]
        scores = compute_factor_scores(codes, sd, params, raw_cache=raw_by_day.get(sd))
        base = base_by_day.get(sd, {})
        combined = {c: base.get(c, 0.0) + float(params.get("scale", 4.0)) * scores.get(c, 0.0)
                    for c in codes}
        scored = sorted(zip(codes, [p for _, p in picks]),
                        key=lambda x: combined[x[0]], reverse=True)
        rets = [r for _, r in scored]
        top = rets[:TOPN]
        bot = rets[-TOPN:]
        port.append((sd, sum(top) / len(top)))
        spreads.append((sd, sum(top) / len(top) - sum(bot) / len(bot)))

    if not port:
        return {"n_days": 0, "avg_port": None}
    avg = sum(p for _, p in port) / len(port)
    half = max(1, len(port) // 2)
    first = sum(p for _, p in port[:half]) / half
    second = sum(p for _, p in port[half:]) / (len(port) - half)
    avg_spread = sum(s for _, s in spreads) / len(spreads)
    return {
        "n_days": len(port),
        "avg_port": round(avg, 4),
        "first_half": round(first, 4),
        "second_half": round(second, 4),
        "avg_spread": round(avg_spread, 4),
    }


def main() -> int:
    cur = compute_factor_scoring_params()
    cur_scale = cur["scale"]
    cur_weights = {k: cur[k] for k in ("w_momentum_20", "w_momentum_60", "w_rel_strength",
                                       "w_volatility", "w_liquidity", "w_quality",
                                       "w_value", "w_fund_flow")}

    # 预计算（与配置无关）：每信号日的前向收益 + 价格原始因子 + 综合评分(base)，仅算一次
    from smcore.strategy.factor_scoring import raw_factors_batch
    days = _all_dal_days()
    picks_by_day: dict = {}
    raw_by_day: dict = {}
    base_by_day: dict = {}
    for sd in days:
        picks, base = _day_picks(sd)
        if picks:
            picks_by_day[sd] = picks
            raw_by_day[sd] = raw_factors_batch([c for c, _ in picks], sd)
            base_by_day[sd] = base

    cur_res = _eval_config({**cur_weights, "scale": cur_scale,
                            "use_fundamentals": cur["use_fundamentals"]},
                           picks_by_day, raw_by_day, base_by_day)
    cur_avg = cur_res.get("avg_port") or 0.0
    print(f"当前配置 scale={cur_scale} 头N组合平均前向收益={cur_avg:+.4f}pp "
          f"（多空价差诊断 {cur_res.get('avg_spread'):+.4f}pp，{cur_res.get('n_days')} 天）")

    grid = []
    for name, preset in _PRESETS.items():
        for sc in _SCALES:
            p = {**preset, "scale": sc, "use_fundamentals": cur["use_fundamentals"]}
            r = _eval_config(p, picks_by_day, raw_by_day, base_by_day)
            if r.get("avg_port") is None:
                continue
            grid.append({"preset": name, "scale": sc, "params": p, **r})

    grid.sort(key=lambda x: (x["avg_port"], x["avg_spread"]), reverse=True)
    best = grid[0] if grid else None
    print(f"\n候选配置数：{len(grid)}，最佳：{best['preset']} scale={best['scale']} "
          f"头N收益={best['avg_port']:+.4f}pp" if best else "无有效配置")

    # 稳健性判定：改进幅度 ≥ 阈值 且 前后两半都相对当前配置改善（防过拟合单段行情）
    robust = False
    improve = 0.0
    if best:
        improve = round(best["avg_port"] - cur_avg, 4)
        cur_first = cur_res.get("first_half") or 0.0
        cur_second = cur_res.get("second_half") or 0.0
        stable_both = (best["first_half"] - cur_first >= -0.01) and (best["second_half"] - cur_second >= -0.01)
        robust = (improve >= MIN_IMPROVE_PP) and stable_both

    print("\n=== Top5 配置（头N组合前向收益 / 多空价差诊断） ===")
    print(f"{'preset':>14}{'scale':>7}{'头N收益':>10}{'价差':>10}{'前半':>9}{'后半':>9}")
    for g in grid[:5]:
        print(f"{g['preset']:>14}{g['scale']:>7}{g['avg_port']:>+10.4f}"
              f"{g['avg_spread']:>+10.4f}{g['first_half']:>+9.4f}{g['second_half']:>+9.4f}")

    rec = {
        "current": {"preset": "base", "scale": cur_scale, "weights": cur_weights,
                    "avg_port": cur_avg},
        "recommended": None,
        "improvement_pp": improve,
        "robust": robust,
    }
    if best:
        rec["recommended"] = {"preset": best["preset"], "scale": best["scale"],
                              "weights": {k: best["params"][k] for k in cur_weights},
                              "avg_port": best["avg_port"]}

    # 落盘明细
    out = ROOT / "stock_data" / "factor_weight_tune.json"
    out.write_text(json.dumps(rec, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\n明细已写：{out}")

    if not robust:
        print(f"[tune] 未达稳健阈值（改进={improve:+.4f}pp ≥ {MIN_IMPROVE_PP} 且 前后两半都改善），"
              f"保持当前配置，不写回。")
        return 0

    if DRY_RUN:
        print(f"[tune] --dry-run：将把因子权重更新为 preset={best['preset']} scale={best['scale']}"
              f"（改进 {improve:+.4f}pp），但不写配置。")
        return 0

    # 写回 risk_config.json（更新 factor_scoring 子块，其余超参不变）
    import copy
    new_full = copy.deepcopy(RISK_CONFIG)
    fs_block = new_full.setdefault("factor_scoring", {})
    fs_block["enabled"] = cur["enabled"]
    fs_block["scale"] = best["scale"]
    fs_block["use_fundamentals"] = cur["use_fundamentals"]
    for k in cur_weights:
        fs_block[k] = best["params"][k]
    try:
        save_risk_config(new_full)
        print(f"[tune] 已写回 risk_config.json factor_scoring：scale={best['scale']} "
              f"preset={best['preset']}（改进 {improve:+.4f}pp）")
    except Exception as e:  # pragma: no cover
        print(f"[tune] 写回失败：{e}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
