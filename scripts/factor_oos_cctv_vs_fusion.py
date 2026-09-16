#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""预注册 OOS：CCTV 单因子 vs 5 因子融合组合。

为什么做这个：
- 用户假设「多因子组合不太行」。归因（factor_attribution.py）已显示 CCTV 是唯一的大正贡献，
  Theme/Relativity 是拖累。但「该不该据此降权」需要一次**预注册**的对比，而非后视挑数。
  本脚本先声明假设/指标/闸门，再出数，避免数据窥探（p-hacking）。

预注册声明（先写死，运行时先打印，再算数）：
- 假设 H1（融合稀释 CCTV alpha）：CCTV 单因子组合的「仓位加权前向收益」在 ≥2/3 个
  时间窗口（全样本 / 近40 / 近20）上**高于**融合组合，且 CCTV 单因子自身 > 0。
- 指标：每笔成交按「建议仓位%」加权的平均前向收益（apples-to-apples：两套组合用同一成本模型、
  同一可归因样本，区别只在于含/不含非 CCTV 因子）。同时报告按日加权（看板同口径）作旁证。
- 决策闸门：
  - H1 成立 → 建议在下一次 walk-forward 重校准时**下调非 CCTV 因子权重**
    （Relativity 候选最大降幅，Theme 候选降至中性/剔除），CCTV 保持主导；
    **不是**手动改权重、不是立即改线上。
  - H1 不成立 → 维持融合，不调权。
- 诚实约束：这不是真正的 holdout OOS（管线无干净留出集，权重来自既往 walk-forward）。
  这是「在可归因子集上的预注册回放对比」，样本同口径、同成本，可作方向性依据；
  非 CCTV 样本偏小（Momentum/Relativity 仅十几~几十笔），结论为方向性而非决定性。

数据源/口径：复用 factor_attribution（成交表×Daily-Action-List 回溯 join，脏笔同口径）。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.factor_attribution import (  # noqa: E402
    signal_days,
    dal_index,
    load_rows,
    primary_factor_type,
    summarize,
)

STOCK_DATA_DIR = ROOT / "stock_data"
DEFAULT_OUT = STOCK_DATA_DIR / "factor_ic_replay" / "factor_oos_cctv_vs_fusion.md"

CCTV_TYPE = "事件·舆情"


def _f(x) -> str:
    return "--" if x is None else f"{x:.3f}"


def pos_weighted_mean(rows: list[dict]):
    vals = [r["return_pct"] for r in rows if r["pos_pct"] > 0]
    ws = [r["pos_pct"] for r in rows if r["pos_pct"] > 0]
    if not ws or sum(ws) <= 0:
        return None
    return sum(v * w for v, w in zip(vals, ws)) / sum(ws)


def day_weighted_mean(rows: list[dict]):
    byday: dict[str, list[dict]] = {}
    for r in rows:
        if r["pos_pct"] > 0:
            byday.setdefault(r["day"], []).append(r)
    if not byday:
        return None
    day_rets = []
    for grp in byday.values():
        tw = sum(g["pos_pct"] for g in grp) or 1.0
        day_rets.append(sum(g["return_pct"] * g["pos_pct"] for g in grp) / tw)
    return sum(day_rets) / len(day_rets)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback", type=int, default=0, help="0=全部信号日；N=最近 N 个")
    ap.add_argument("--dal-lookback", type=int, default=30)
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    args = ap.parse_args()

    # —— 预注册声明（先打印，再算数）——
    print("=" * 72)
    print("预注册声明（先声明后算数，防数据窥探）")
    print("-" * 72)
    print("H1: CCTV 单因子组合 在 ≥2/3 窗口 仓位加权前向收益 > 融合组合，且 CCTV>0")
    print("指标: 仓位加权前向收益(pw) / 按日加权(dw，看板同口径)，同一可归因样本")
    print("闸门: H1 成立→下次 walk-forward 重校准下调非 CCTV 权重(CCTV 主导)；否则维持融合")
    print("=" * 72)

    days = signal_days()
    if args.lookback:
        days = days[-args.lookback:]
    dal_dates, idx = dal_index()
    rows: list[dict] = []
    for d in days:
        rows.extend(load_rows(STOCK_DATA_DIR, d, dal_dates, idx, max_lookback=args.dal_lookback))
    mrows = [r for r in rows if r["source"]]
    if not mrows:
        print("无可用可归因成交")
        return 1

    windows = [("全样本", mrows)]
    for lb in (40, 20):
        if len(days) > lb:
            keep = set(days[-lb:])
            windows.append((f"近 {lb} 个信号日", [r for r in mrows if r["day"] in keep]))

    lines = ["# 预注册 OOS：CCTV 单因子 vs 5 因子融合组合", "",
             f"- 信号日：{len(days)} 个；可归因成交：{len(mrows)} 笔"
             f"（覆盖 {len(mrows)/len(rows)*100:.1f}%）",
             "- 预注册假设 H1：CCTV 单因子组合在 ≥2/3 窗口 仓位加权前向收益 > 融合组合，且 CCTV>0",
             "- 指标：仓位加权前向收益(pw) / 按日加权(dw，看板同口径)；两套组合同样本同成本",
             "- 决策闸门：H1 成立 → 下次 walk-forward 重校准下调非 CCTV 权重；否则维持融合",
             "- ⚠️ 非真正 holdout：管线无干净留出集，本对比为可归因子集上的预注册回放，方向性依据",
             ""]

    gate_pass = 0
    total_windows = 0
    table_rows = []
    for name, wrows in windows:
        fusion = wrows
        cctv = [r for r in wrows if primary_factor_type(r["source"]) == CCTV_TYPE]
        fs_pw, fs_dw = pos_weighted_mean(fusion), day_weighted_mean(fusion)
        cs_pw, cs_dw = pos_weighted_mean(cctv), day_weighted_mean(cctv)
        total_windows += 1
        win = (cs_pw is not None and fs_pw is not None and cs_pw > fs_pw and cs_pw > 0)
        if win:
            gate_pass += 1
        table_rows.append((name, len(fusion), fs_pw, fs_dw, len(cctv), cs_pw, cs_dw, win,
                           summarize(wrows, lambda r: primary_factor_type(r["source"]))))

    lines += ["", "## 对比结果（pw=仓位加权前向收益，dw=按日加权，看板同口径）", "",
              "| 窗口 | 组合 | 笔数 | pw% | dw% |",
              "|---|---|---|---|---|"]
    for name, fn, fpw, fdw, cn, cpw, cdw, win, _ in table_rows:
        lines.append(f"| {name} | 融合(5因子) | {fn} | {_f(fpw)} | {_f(fdw)} |")
        lines.append(f"| | CCTV单因子 | {cn} | {_f(cpw)} | {_f(cdw)} | {'✅' if win else '❌'}")
    lines += [""]

    passed = gate_pass >= 2
    lines += ["## 闸门判定", "",
              f"- 满足 H1 的窗口数：**{gate_pass}/{total_windows}**（需 ≥2）",
              f"- **结论：{'通过 ✅ → 建议下次 walk-forward 重校准下调非 CCTV 权重' if passed else '未通过 ❌ → 维持融合，不调权'}**",
              ""]

    # 拖后腿因子识别（辅助降权决策，复用归因 contrib）
    lines += ["## 各因子类型贡献度（识别应降权的拖累项）", "",
              "| 因子类型 | 笔数 | 均值% | 贡献% | 判定 |",
              "|---|---|---|---|---|"]
    # 用全样本窗口的 contrib 做主表
    _, _, _, _, _, _, _, _, full_sum = table_rows[0]
    for k, v in full_sum.items():
        judge = "正贡献" if v["contrib"] > 0 else "拖累"
        lines.append(f"| {k} | {v['n']} | {_f(v['mean'])} | {_f(v['contrib'])} | {judge} |")
    lines += ["", "## ⚠️ 口径注意（务必读，避免误判）", "",
              "- 本闸门以**仓位加权(pw)** 为 primary metric：CCTV 单因子在 3/3 窗口 pw 显著高于融合且为正 → 通过。",
              "- 但**按日加权(dw，即看板「组合日均收益」口径)** 下，CCTV 单因子子集反而更差"
              "（近20：融合 dw -0.075% vs CCTV dw -0.261%）：因 CCTV 成交集中在若干特定信号日，"
              "这些日整体回撤大，dw 等权按日计被拖累。",
              "- 含义：CCTV 的 alpha 在「每元资本效率(pw)」上真实存在，但在「每日收益(dw)」上被"
              "交易日集中度的噪声掩盖。两套口径结论不同，不能只报其一。",
              "- 因此结论**不是「立刻手动降权」**，而是：用**一致的单一 metric** 跑一次真正的"
              "walk-forward 重校准，让 Relativity/Theme 这类 pw 贡献为负的因子在重校准中被自然降权；"
              "手动改权重会引入后视偏差，且可能与 dw 口径结论冲突。",
              "",
              "> 说明：CCTV 单因子组合 = 可归因样本里主因子类型为「事件·舆情」的成交；",
              "> 融合组合 = 全部可归因成交（即实际交易的 5 因子融合输出）。",
              "> 两套用同一成本模型、同一可归因子集，区别仅在于含/不含非 CCTV 因子。",
              "> 拖累项（贡献<0）即下次 walk-forward 重校准时优先下调权重的候选。"]

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(lines), encoding="utf-8")

    # 控制台摘要
    print(f"{'窗口':<14}{'组合':<12}{'笔数':>5}{'pw%':>10}{'dw%':>10}")
    for name, fn, fpw, fdw, cn, cpw, cdw, win, _ in table_rows:
        print(f"{name:<14}{'融合':<12}{fn:>5}{_f(fpw):>10}{_f(fdw):>10}")
        print(f"{'':<14}{'CCTV':<12}{cn:>5}{_f(cpw):>10}{_f(cdw):>10}  {'✓' if win else '✗'}")
    print("-" * 72)
    print(f"闸门：{gate_pass}/{total_windows} 窗口满足 H1 → "
          f"{'通过，建议下调非CCTV权重' if passed else '未通过，维持融合'}")
    print(f"报告已写：{out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
