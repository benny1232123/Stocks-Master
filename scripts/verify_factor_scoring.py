#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""验证第四档因子打分（factor_scoring）是否真能选出未来表现更好的票。

方法（严格的 walk-forward 因子有效性检验，因果正确）：
- 对每个历史信号日 sd，用「信号日及之前」的本地 k_data 算每只候选票的因子分
  （动量 ret20/ret60、相对强度、波动率、流动性，截面 z-score 加权）。
- 按因子分排序，取高分组(top-N)与低分组(bottom-N)，分别构造信号清单，
  用 run_forward_signal_backtest（enable_exits=True、自适应出场）回测各自组合收益。
- 若高分组平均收益 > 低分组，说明因子分对未来收益有单调预测力 → 因子有效。

全部离线（本地 k_data），因子分用信号日及之前数据，不含未来函数。
因子权重来自 risk_config.json 的 factor_scoring 块（enabled 开关仅控制 fusion 是否接入，
本脚本直接用 compute_factor_scores 纯函数，不受 enabled 影响）。

用法：
  python scripts/verify_factor_scoring.py                 # 全量
  VE_MAX_DAYS=6 python scripts/verify_factor_scoring.py   # 快验
  VE_TOPN=8 VE_HOLD_DAYS=10 python scripts/verify_factor_scoring.py
"""
from __future__ import annotations

import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.backtest.engine import run_forward_signal_backtest  # noqa: E402
from smcore.strategy import factor_scoring as fs  # noqa: E402
from smcore.strategy import risk_rules as rr  # noqa: E402
from smcore.strategy.adaptive_weights import STOCK_DATA_DIR  # noqa: E402
from smcore.utils.code import format_stock_code  # noqa: E402

HOLD_DAYS = int(os.environ.get("VE_HOLD_DAYS", "10"))
INIT_CAP = 1_000_000.0
MAX_DAYS = int(os.environ.get("VE_MAX_DAYS", "999"))
TOPN = int(os.environ.get("VE_TOPN", "10"))


def _cached_exists(code: str) -> bool:
    cache = STOCK_DATA_DIR / "k_data" / f"{format_stock_code(code)}_qfq_full.csv"
    if not cache.exists():
        return False
    try:
        df = pd.read_csv(cache)
    except Exception:
        return False
    return not df.empty


def _read_dal(dal_path: Path) -> list[str]:
    try:
        d = pd.read_csv(dal_path, encoding="utf-8-sig")
    except Exception:
        return []
    if "股票代码" not in d.columns:
        return []
    out = []
    for _, r in d.iterrows():
        c = format_stock_code(r.get("股票代码"))
        if c:
            out.append(c)
    return out


def _run(sig: pd.DataFrame) -> dict:
    res = run_forward_signal_backtest(
        sig.copy(), hold_days=HOLD_DAYS, initial_capital=INIT_CAP, enable_exits=True
    )
    return res.summary


def main() -> int:
    today = date.today()
    cutoff = today - timedelta(days=HOLD_DAYS + 20)
    dals = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))
    sds: list[tuple[date, Path]] = []
    for dal in dals:
        m = re.search(r"(\d{8})", dal.name)
        if not m:
            continue
        y, mo, da = m.group(1)[:4], m.group(1)[4:6], m.group(1)[6:8]
        sd = date(int(y), int(mo), int(da))
        if sd > cutoff:
            continue
        sds.append((sd, dal))
    sds.sort()
    if MAX_DAYS < len(sds):
        sds = sds[-MAX_DAYS:]

    params = rr.compute_factor_scoring_params()
    if os.environ.get("VE_INVERT") == "1":
        # 反转实验：把动量类权重取负（短期反转异象），测试因子方向是否更有效
        for _k in ("w_momentum_20", "w_momentum_60", "w_rel_strength"):
            params[_k] = -params[_k]
    high_rets: list[float] = []
    low_rets: list[float] = []
    rows: list[dict] = []
    for sd, dal in sds:
        codes = [c for c in _read_dal(dal) if _cached_exists(c)]
        if len(codes) < 6:
            continue
        fscore = fs.compute_factor_scores(codes, sd.strftime("%Y%m%d"), params)
        ranked = sorted(codes, key=lambda c: fscore.get(c, 0.0), reverse=True)
        n = min(TOPN, len(ranked) // 2)
        if n < 2:
            continue
        high = ranked[:n]
        low = ranked[-n:]
        sh = pd.DataFrame([{"日期": sd, "代码": c, "来源策略": "factor"} for c in high])
        sl = pd.DataFrame([{"日期": sd, "代码": c, "来源策略": "factor"} for c in low])
        rh = _run(sh)
        rl = _run(sl)
        if "error" in rh or "error" in rl:
            continue
        high_rets.append(rh["total_return"])
        low_rets.append(rl["total_return"])
        rows.append({
            "sd": sd.strftime("%Y%m%d"),
            "n": len(codes),
            "high": rh["total_return"],
            "low": rl["total_return"],
            "diff": round(rh["total_return"] - rl["total_return"], 3),
        })
        print(f"[vf] {sd} high={rh['total_return']} low={rl['total_return']}", file=sys.stderr)

    n_days = len(rows)
    high_avg = round(sum(high_rets) / n_days, 3) if n_days else None
    low_avg = round(sum(low_rets) / n_days, 3) if n_days else None
    spread_avg = round(high_avg - low_avg, 3) if (high_avg is not None and low_avg is not None) else None
    pos_days = sum(1 for r in rows if r["diff"] > 0)
    spread_win = round(pos_days / n_days * 100, 1) if n_days else None

    lines = []
    lines.append(f"# 第四档因子打分有效性验证（walk-forward 多空，hold_days={HOLD_DAYS}）\n")
    lines.append(f"- 信号日样本：{n_days} 个（离线安全，纯本地行情，因果正确）")
    lines.append(f"- 因子权重：mom20={params['w_momentum_20']} mom60={params['w_momentum_60']} "
                 f"rs={params['w_rel_strength']} vol={params['w_volatility']} liq={params['w_liquidity']}")
    lines.append(f"- 每组取 top/bottom {TOPN}（或半数）回测各自组合收益\n")
    lines.append("## 汇总\n")
    lines.append(f"- 高分组平均组合收益：**{high_avg}%**")
    lines.append(f"- 低分组平均组合收益：{low_avg}%")
    lines.append(f"- 多空价差（高−低）：**{spread_avg} pp**")
    lines.append(f"- 价差为正的天数占比：{spread_win}%（{pos_days}/{n_days}）\n")
    lines.append("## 逐信号日\n")
    lines.append("| 信号日 | 候选数 | 高分组收益% | 低分组收益% | 价差(pp) |")
    lines.append("|--------|--------|-----------|-----------|----------|")
    for r in rows:
        lines.append(f"| {r['sd']} | {r['n']} | {r['high']} | {r['low']} | {r['diff']} |")
    lines.append("\n## 结论\n")
    if spread_avg is not None and spread_avg > 0:
        lines.append(
            f"- 高因子分组平均收益 **高于** 低分组 {spread_avg} pp，价差为正占比 {spread_win}%"
            f"→ 因子分对未来收益有单调预测力，**因子有效**，建议 `factor_scoring.enabled=true`。"
        )
    else:
        lines.append(
            f"- 高因子分组平均收益未高于低分组（价差 {spread_avg} pp）→ 当前权重下因子未显现预测力，"
            f"保持 `factor_scoring.enabled=false`，需调权重或仅作辅助。"
        )
    md = "\n".join(lines) + "\n"
    out_path = STOCK_DATA_DIR / "verify_factor_scoring.md"
    out_path.write_text(md, encoding="utf-8")
    print(md)
    print(f"\n[vf] 报告已写入 {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
