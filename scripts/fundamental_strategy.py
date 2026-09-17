#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""基本面族正交策略生成器（2026-09-17 由单一 fundamental 拆分而来）。

定位：在价格/成交量策略（boll=反转·均值回归 / relativity=相对强度·资金流）之外，
提供三个**彼此正交**的基本面维度，各自独立出选股 CSV、独立进分配器，便于横向对比
谁的真实 edge 更高（这就是「因子驱动主导」的菜单）：

- Quality（基本面·质量）：截面 z(ROE) + z(毛利率)      —— 高盈利质量
- Value  （基本面·估值）：截面 z(-PE) + z(-PB)          —— 低估值
- Size   （基本面·规模）：截面 z(-总市值)               —— 小盘

数据源：`stock_data/fundamental_cache/<code>.json`（flat 快照，字段
roe / gross_margin / pe / pb / mkt_cap，由 smcore/strategy/fundamental.py 联网填充）。
纯缓存读取、离线可跑；样本不足或字段缺失时该项跳过 / 输出空表（**绝不联网、绝不抛错**），
对融合链路是 no-op（fuse_signals 读到空 picks 即跳过）。

设计纪律（与全系统一致）：
- 输出严格沿用 `Stock-Selection-<Name>-YYYYMMDD.csv` 契约（股票代码/股票名称/综合分），
  由 `picks_loader._load_fund_factor_picks` 消费。
- 不碰任何权重/融合逻辑——权重完全交给 `compute_adaptive_allocation` 按各策略**已实现
  edge** 自适应决定；初期无历史 → 无证据门控只给 floor 探索权重，跑出正 edge 才加分。
- 三个因子**刻意不含** turnover/amount_20（与 relativity 的资金流维度重叠，非正交）；
  成长(revenue_growth) 待 v2 PIT 缓存刷新后再加。
- ⚠️ 缓存是「当前快照」而非时点精确(PIT)：历史信号日回填用的是当下截面（离线可达的最佳
  近似），因此各历史日的选股内容相同、仅文件名不同——用于让策略在回放/归因中有完整候选
  历史；不影响权重逻辑（权重只看真实前向 edge）。

用法::

    python scripts/fundamental_strategy.py                        # 用今天日期
    python scripts/fundamental_strategy.py --date 20260916 --top 40
    python scripts/fundamental_strategy.py --all-signal-days      # 回填全部历史信号日
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import os
import re
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "stock_data" / "fundamental_cache"
DATA_DIR = ROOT / "stock_data"

FACTORS = ("Quality", "Value", "Size")


def _zscore(values: list[float]) -> list[float]:
    """返回与输入等长的 z 分列表；样本<2 或无离散度时全 0（无证据）。"""
    n = len(values)
    if n < 2:
        return [0.0] * n
    m = sum(values) / n
    var = sum((v - m) ** 2 for v in values) / n
    sd = math.sqrt(var)
    if sd <= 0:
        return [0.0] * n
    return [(v - m) / sd for v in values]


def _load_cache() -> list[dict]:
    """扫描 flat 缓存 → [{code, roe, gm, pe, pb, mkt_cap}]（缺失字段为 None）。"""
    rows: list[dict] = []
    for p in sorted(glob.glob(str(CACHE_DIR / "*.json"))):
        code = os.path.splitext(os.path.basename(p))[0]
        try:
            d = json.loads(Path(p).read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(d, dict):
            continue

        def _f(k):
            v = d.get(k)
            try:
                return float(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        rows.append({
            "code": code,
            "roe": _f("roe"),
            "gm": _f("gross_margin"),
            "pe": _f("pe"),
            "pb": _f("pb"),
            "mkt_cap": _f("mkt_cap"),
        })
    return rows


def _score_quality(rows: list[dict]) -> dict:
    """质量 = (z(roe) + z(gross_margin)) / 2；需两者同时可得。"""
    idx = [i for i, r in enumerate(rows) if r["roe"] is not None and r["gm"] is not None]
    if len(idx) < 5:
        return {}
    roe_z = _zscore([rows[i]["roe"] for i in idx])
    gm_z = _zscore([rows[i]["gm"] for i in idx])
    return {rows[idx[k]]["code"]: (roe_z[k] + gm_z[k]) / 2.0 for k in range(len(idx))}


def _score_value(rows: list[dict]) -> dict:
    """估值 = z(-pe) 与 z(-pb) 按各自可得子集标准化后取均值（低估值→高分）。

    合理性过滤与数据层一致：pe∈(0,300)、pb∈(0,50)，剔除异常/负值。
    """
    pe_idx = [i for i, r in enumerate(rows) if r["pe"] is not None and 0 < r["pe"] < 300]
    pb_idx = [i for i, r in enumerate(rows) if r["pb"] is not None and 0 < r["pb"] < 50]
    if len(pe_idx) < 5 and len(pb_idx) < 5:
        return {}
    pe_z: dict = {}
    if len(pe_idx) >= 5:
        z = _zscore([-rows[i]["pe"] for i in pe_idx])
        pe_z = {rows[pe_idx[k]]["code"]: z[k] for k in range(len(pe_idx))}
    pb_z: dict = {}
    if len(pb_idx) >= 5:
        z = _zscore([-rows[i]["pb"] for i in pb_idx])
        pb_z = {rows[pb_idx[k]]["code"]: z[k] for k in range(len(pb_idx))}
    out: dict = {}
    for c in set(pe_z) | set(pb_z):
        vals = [x for x in (pe_z.get(c), pb_z.get(c)) if x is not None]
        out[c] = sum(vals) / len(vals)
    return out


def _score_size(rows: list[dict]) -> dict:
    """规模 = z(-mkt_cap)（小市值→高分）。"""
    idx = [i for i, r in enumerate(rows) if r["mkt_cap"] is not None and r["mkt_cap"] > 0]
    if len(idx) < 5:
        return {}
    z = _zscore([-rows[i]["mkt_cap"] for i in idx])
    return {rows[idx[k]]["code"]: z[k] for k in range(len(idx))}


_SCORERS = {"Quality": _score_quality, "Value": _score_value, "Size": _score_size}


def build_factor(factor: str, top: int = 40, rows: list[dict] | None = None) -> list[dict]:
    """返回某因子按复合分降序的候选 [{code, name, score}]（映射到 0-100 便于阅读）。"""
    if rows is None:
        rows = _load_cache()
    if len(rows) < 5:
        return []
    comp = _SCORERS[factor](rows)
    if not comp:
        return []
    items = sorted(comp.items(), key=lambda kv: kv[1], reverse=True)[:top]
    return [
        {"code": c, "name": "", "score": round(max(0.0, min(100.0, 50.0 + z * 15.0)), 2)}
        for c, z in items
    ]


def write_csv(factor: str, date_str: str, picks: list[dict], out_dir: str) -> Path:
    """按契约写 `Stock-Selection-<factor>-<date>.csv`（空表也写，保持一致契约）。"""
    out_path = Path(out_dir) / f"Stock-Selection-{factor}-{date_str}.csv"
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["股票代码", "股票名称", "综合分"])
        w.writeheader()
        for p in picks:
            w.writerow({"股票代码": p["code"], "股票名称": p["name"], "综合分": p["score"]})
    return out_path


def _signal_days() -> list[str]:
    """历史信号日 = 存在 Daily-Action-List 的日期（升序）。"""
    days = []
    for f in glob.glob(str(DATA_DIR / "Daily-Action-List-*.csv")):
        m = re.search(r"Daily-Action-List-(\d{8})\.csv", f)
        if m:
            days.append(m.group(1))
    return sorted(set(days))


def main() -> int:
    ap = argparse.ArgumentParser(description="基本面族（Quality/Value/Size）正交策略生成器")
    ap.add_argument("--date", default=datetime.now().strftime("%Y%m%d"), help="信号日 YYYYMMDD")
    ap.add_argument("--top", type=int, default=40, help="每个因子输出候选数上限")
    ap.add_argument("--out-dir", default=str(DATA_DIR), help="CSV 输出目录")
    ap.add_argument("--all-signal-days", action="store_true",
                    help="回填全部历史信号日（缓存快照相同 → 各日内容一致、仅文件名不同）")
    args = ap.parse_args()

    if args.all_signal_days:
        rows = _load_cache()  # 只读一次，供所有日期复用
        per_factor = {f: build_factor(f, top=args.top, rows=rows) for f in FACTORS}
        summary = "/".join(f"{f}={len(per_factor[f])}" for f in FACTORS)
        days = _signal_days()
        for d in days:
            for f in FACTORS:
                write_csv(f, d, per_factor[f], args.out_dir)
        print(f"[fundamental] 已回填 {len(days)} 个信号日 × {len(FACTORS)} 因子（{summary}）")
        return 0

    try:
        datetime.strptime(args.date, "%Y%m%d")
    except ValueError:
        print(f"[fundamental] 非法日期 {args.date}")
        return 2

    for f in FACTORS:
        picks = build_factor(f, top=args.top)
        p = write_csv(f, args.date, picks, args.out_dir)
        print(f"[fundamental] {p.name} 已保存，{len(picks)} 只")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
