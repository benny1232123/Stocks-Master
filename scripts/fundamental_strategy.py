#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""基本面·质量价值 正交策略生成器（2026-09-17）。

定位：在现有 5 策略（boll=反转·均值回归 / relativity=相对强度·资金流 /
theme=题材 / cctv=事件·舆情 / momentum=动量）之外，补一个**与价格/成交量完全正交**
的维度——基本面质量+估值。数据源是 `stock_data/fundamental_cache/<code>.json`
（字段：roe / gross_margin / pb / mkt_cap / turnover / amount_20），由
`smcore/strategy/fundamental.py` 在联网环境填充、生产优先读缓存（offline-safe）。

复合分 = 质量(z(roe), z(gross_margin)) 与 估值(z(-pb)) 的等权截面 z 分，
全部相对「当下 fundamental_cache 全集」标准化——只看相对高低、不依赖绝对水平，
因此无需对 roe 做年度化（年度化只影响绝对水平，不影响截面排序）。

设计纪律（与全系统一致）：
- 纯缓存读取、离线可跑；缓存为空或样本不足时输出空表（**绝不联网补取、绝不抛错**），
  对融合链路是 no-op（fuse_signals 读到空 picks 即跳过）。
- 输出严格沿用 `Stock-Selection-<Name>-YYYYMMDD.csv` 契约（股票代码/股票名称/综合分），
  由 `picks_loader._load_fundamental_picks` 消费。
- 不碰任何权重/融合逻辑——权重完全交给 `compute_adaptive_allocation` 按该策略
  **已实现 edge** 自适应决定（初期无历史→地板权重，跑出正 edge 才被加分）。
- 刻意不含 amount_20/turnover（那与 relativity 的资金流重叠，非正交）；如需加成长维度，
  待 fundamental_cache 补「营收/利润增速」字段后再扩。

用法::

    python scripts/fundamental_strategy.py                 # 用今天日期
    python scripts/fundamental_strategy.py --date 20260916 --top 40
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import os
from datetime import datetime, date as date_cls
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "stock_data" / "fundamental_cache"
OUT_DIR = ROOT / "stock_data"


def _zscore(values: list[float]) -> dict:
    """返回 {原值索引: z}，样本<2 时全部返回 0（无离散度→无证据）。"""
    n = len(values)
    if n < 2:
        return {i: 0.0 for i in range(n)}
    m = sum(values) / n
    var = sum((v - m) ** 2 for v in values) / n
    sd = math.sqrt(var)
    if sd <= 0:
        return {i: 0.0 for i in range(n)}
    return {i: (values[i] - m) / sd for i in range(n)}


def build_composite(top: int = 40) -> list[dict]:
    """扫描 fundamental_cache，返回按复合分降序的候选列表（含 code/name/score）。"""
    paths = sorted(glob.glob(str(CACHE_DIR / "*.json")))
    if len(paths) < 5:
        return []  # 样本不足以做截面标准化 → 空表（no-op）

    rows: list[dict] = []
    for p in paths:
        code = os.path.splitext(os.path.basename(p))[0]
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
        except Exception:
            continue
        roe = d.get("roe")
        gm = d.get("gross_margin")
        pb = d.get("pb")
        if roe is None or gm is None:
            continue  # 质量维度缺失则跳过该票（质量为本策略核心）
        rows.append({"code": code, "roe": float(roe), "gm": float(gm), "pb": (float(pb) if pb is not None else None)})

    if len(rows) < 5:
        return []

    roe_z = _zscore([r["roe"] for r in rows])
    gm_z = _zscore([r["gm"] for r in rows])
    pb_present = [r["pb"] is not None for r in rows]
    if all(pb_present):
        pb_z = _zscore([-r["pb"] for r in rows])  # 低 pb = 高估值分
        has_value = True
    else:
        has_value = False

    out = []
    for i, r in enumerate(rows):
        quality = (roe_z[i] + gm_z[i]) / 2.0
        comp = quality + (pb_z[i] if has_value else 0.0)
        denom = 2.0 if has_value else 1.0
        comp = comp / denom
        # 映射到 0-100 便于阅读（纯展示，排序由 comp 决定）
        score = max(0.0, min(100.0, 50.0 + comp * 15.0))
        out.append({"code": r["code"], "name": "", "score": round(score, 2)})

    out.sort(key=lambda x: x["score"], reverse=True)
    return out[:top]


def main() -> int:
    ap = argparse.ArgumentParser(description="基本面·质量价值 正交策略生成器")
    ap.add_argument("--date", default=datetime.now().strftime("%Y%m%d"), help="信号日 YYYYMMDD")
    ap.add_argument("--top", type=int, default=40, help="输出候选数上限")
    ap.add_argument("--out-dir", default=str(OUT_DIR), help="CSV 输出目录")
    args = ap.parse_args()

    # 日期校验
    try:
        datetime.strptime(args.date, "%Y%m%d")
    except ValueError:
        print(f"[fundamental] 非法日期 {args.date}")
        return 2

    picks = build_composite(top=args.top)
    out_path = Path(args.out_dir) / f"Stock-Selection-Fundamental-{args.date}.csv"
    if not picks:
        # 空表也要写（保持契约一致，loader 读到空表即跳过）
        import csv
        with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
            csv.DictWriter(f, fieldnames=["股票代码", "股票名称", "综合分"]).writeheader()
        print(f"[fundamental] 样本不足，输出空表 {out_path}")
        return 0

    import csv
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["股票代码", "股票名称", "综合分"])
        w.writeheader()
        for p in picks:
            w.writerow({"股票代码": p["code"], "股票名称": p["name"], "综合分": p["score"]})
    print(f"[fundamental] Stock-Selection-Fundamental-{args.date}.csv 已保存，{len(picks)} 只")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
