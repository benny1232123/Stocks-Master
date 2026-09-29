#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""验证期复核成绩单（2026-10 复核检查点用）：四只票 vs 新菜单系统，同一时段对决。

背景：用户选择继续持有 4 只套牢票进入验证期（2026-09-29 起），同时新菜单
（maxret10 置换）上线。约定一周后用双方成绩单复核「抗 vs 系统」。

A 面（四只票）：现价 vs 09-26 收盘基线（验证期起点）的逐票盈亏、硬条件触发历史
  （holdings_watch floor/target 判定）、组合市值变动；
B 面（新菜单系统）：20260929 起 DAL 的逐日票池 mark-to-market（入场=次开盘，
  盯市=最新收盘），未实现收益按日等权；10 日持有的最终回补收益待到期后另算。

用法：python scripts/verification_checkpoint.py [--start 20260929]
     （--start = 验证期起点；默认 20260929）
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.config.defaults import STOCK_DATA_DIR  # noqa: E402
from smcore.strategy.boll_levels import _compute_boll_levels  # noqa: E402
from smcore.data.kline import read_kline_cache  # noqa: E402

CONFIG = STOCK_DATA_DIR / "holdings_watch.json"
OUT = STOCK_DATA_DIR / "verification_checkpoint.md"
BASELINE_DATE = "20260926"  # 验证期起点（置换验收日收盘）


def _kline(code: str) -> pd.DataFrame:
    d = read_kline_cache(code, base_dir=STOCK_DATA_DIR / "k_data")
    d["date"] = pd.to_datetime(d["date"])
    return d.sort_values("date").dropna(subset=["close"]).reset_index(drop=True)


def _close_at(code: str, date: str) -> float | None:
    d = _kline(code)
    s = d[d["date"] <= pd.Timestamp(date)]
    return float(s["close"].iloc[-1]) if len(s) else None


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="20260929")
    args = ap.parse_args()
    start = args.start

    pos = json.loads(CONFIG.read_text(encoding="utf-8"))
    L = [f"# 验证期复核成绩单（起点 {start}）",
         f"- 生成：{datetime.now().strftime('%Y-%m-%d %H:%M')}；数据截至本地 k_data 最新收盘",
         ""]

    # ── A 面：四只票 ──
    L += ["## A 面：四只套牢票（用户自持，非系统清单）", "",
          "| 代码 | 验证期基线 | 现价 | 验证期盈亏 | 总浮亏 | 硬底线 | 状态 |", "|---|---|---|---|---|---|---|"]
    a_base_total = a_now_total = 0.0
    for p in pos:
        base = _close_at(p["code"], BASELINE_DATE)
        lv = _compute_boll_levels(p["code"]) or {}
        px = lv.get("close")
        state = "持有"
        if p.get("floor") and px and px <= p["floor"]:
            state = "⚠️ALERT"
        elif p.get("target") and px and px >= p["target"]:
            state = "TAKE"
        a_base_total += (base or 0) * p["qty"]
        a_now_total += (px or 0) * p["qty"]
        L.append(f"| {p['code']} {p.get('name','')} | {base:.2f} | {px:.2f} | "
                 f"{(px / base - 1):+.1%} | {(px / p['cost'] - 1):+.1%} | "
                 f"{p.get('floor') or '—'} | {state} |")
    qty_total = sum(p["qty"] for p in pos)
    a_base_avg = a_base_total / qty_total if qty_total else 0
    a_now_avg = a_now_total / qty_total if qty_total else 0
    L += ["", f"- 四只票合计：验证期基线 {a_base_total:,.0f} → 现市值 {a_now_total:,.0f} "
              f"（验证期 **{(a_now_avg / a_base_avg - 1):+.1%}**，按等权股数）"]

    # ── B 面：新菜单系统 DAL ──
    L += ["", "## B 面：新菜单系统（20260929 起 DAL，票池盯市）", "",
          "| 信号日 | 票数 | 入场成本(等权) | 现值(等权) | 盯市盈亏 |", "|---|---|---|---|---|"]
    b_rows = []
    for dal in sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv")):
        sd = dal.stem.split("-")[-1]
        if sd < start:
            continue
        try:
            d = pd.read_csv(dal, encoding="utf-8-sig")
        except Exception:
            continue
        codes = [str(c).strip().zfill(6) for c in d["股票代码"].dropna().astype(str)]
        if len(codes) < 3:
            continue
        entrys, marks = [], []
        for c in codes:
            k = _kline(c)
            after = k[k["date"] > pd.Timestamp(sd)]
            if after.empty:
                continue
            entry = float(after.iloc[0]["open"])
            mark = float(k["close"].iloc[-1])
            entrys.append(entry)
            marks.append(mark / entry - 1.0)
        if len(marks) < 3:
            continue
        b_rows.append((sd, len(marks), float(np.mean(marks))))
    for sd, n, r in b_rows:
        L.append(f"| {sd} | {n} | 1.0 | {(1 + r):.4f} | {r:+.1%} |")
    if b_rows:
        avg_r = float(np.mean([r for _, _, r in b_rows]))
        L += ["", f"- B 面等权平均盯市收益：**{avg_r:+.2%}**/清单"
                  f"（{len(b_rows)} 个信号日；10 日持有到期收益待到期后用 "
                  f"walk_forward 口径回补）"]
    L += ["", "## 读法", "",
          "- A vs B 同期对比 = 「抗 vs 系统」的复核输入；A 面含硬条件执行状态，"
          "B 面为纸面盯市（未含 10 日到期出场）。",
          "- 复核决策框架不变：数据摆平后，方向由条件与纪律说话。",
          ""]
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"复核成绩单已写 {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
