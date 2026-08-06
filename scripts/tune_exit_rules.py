#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""出场阈值 walk-forward 扫描（离线，仅本地 k_data 缓存）。

目的：在「partial_take_profit=开 / market_friction(limit_down)=开 / vol_target=关」的固定前提下，
扫描 exit 块的四个阈值 base 值（stop_loss / take_profit / trailing_stop / hold_days），
寻找能稳健提升组合收益率的设定。

严谨性：逐组改写 rr.CONFIG["exit"] 的 base 值后让引擎走 compute_adaptive_exit_params 的
波动率缩放逻辑（与生产完全一致），跑完还原 —— 这样扫出的「最佳 base」可直接写回 risk_config.json。
trend_exit_ma 生产是 regime 自适应(40/60/90)，此处不扫，交给引擎默认。

稳健性守卫：信号日按时间排序前后两半，要求候选在「前半均值 > 基线前半」「后半均值 > 基线后半」
且「整体改进 ≥ 阈值」才判为稳健，避免过拟合单段行情。

用法：
  python scripts/tune_exit_rules.py                 # 全量离线安全信号日
  VE_MAX_DAYS=12 python scripts/tune_exit_rules.py  # 仅最近 12 日（快验）
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
from smcore.strategy import risk_rules as rr  # noqa: E402
from smcore.strategy.adaptive_weights import STOCK_DATA_DIR  # noqa: E402
from smcore.utils.code import format_stock_code  # noqa: E402

INIT_CAP = 1_000_000.0
MAX_SIGNAL_DAYS = int(os.environ.get("VE_MAX_DAYS", "999"))
# 扫描网格（base 值；生产会再经波动率缩放）——围绕基线 2 档，控制组合数
SL = [0.06, 0.10]
TP = [0.05, 0.08]
TR = [0.04, 0.06]
HD = [8, 14]
BASELINE = (0.08, 0.06, 0.05, 10)  # 当前 risk_config.json 真实值
IMPROVE_THRESH_PP = 0.10  # 整体改进至少 +0.10pp 才算有意义


def _cached_exists(code: str) -> bool:
    cache = STOCK_DATA_DIR / "k_data" / f"{format_stock_code(code)}_qfq_full.csv"
    if not cache.exists():
        return False
    try:
        return not pd.read_csv(cache).empty
    except Exception:
        return False


def _read_dal(dal_path: Path) -> list[tuple[str, str]]:
    try:
        d = pd.read_csv(dal_path, encoding="utf-8-sig")
    except Exception:
        return []
    if "股票代码" not in d.columns:
        return []
    out = []
    for _, r in d.iterrows():
        c = format_stock_code(r.get("股票代码"))
        if not c:
            continue
        s = r.get("来源策略")
        s = "" if (s is None or pd.isna(s)) else str(s).strip()
        out.append((c, s))
    return out


def _run(sig: pd.DataFrame) -> dict:
    res = run_forward_signal_backtest(
        sig.copy(),
        hold_days=rr.CONFIG["exit"]["hold_days"],  # 引擎不回退 CONFIG，须显式传改写后值
        initial_capital=INIT_CAP,
        enable_exits=True,
        vol_target=False,
        partial_take_profit=True,
        model_limit_down=True,
        stop_loss_pct=None,   # None → 引擎走 compute_adaptive_exit_params(波动率缩放，与生产一致)
        take_profit_pct=None,
        trailing_stop_pct=None,
        trend_exit_ma=None,   # None → 引擎走 regime 自适应(40/60/90)
    )
    return res.summary


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return round(sum(xs) / len(xs), 4) if xs else None


def main() -> int:
    today = date.today()
    cutoff = today - timedelta(days=10 + 20)

    dals = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))
    sds: list[tuple[date, Path]] = []
    for dal in dals:
        m = re.search(r"(\d{8})", dal.name)
        if not m:
            continue
        sd = date(int(m.group(1)[:4]), int(m.group(1)[4:6]), int(m.group(1)[6:8]))
        if sd > cutoff:
            continue
        sds.append((sd, dal))
    sds.sort()
    if MAX_SIGNAL_DAYS < len(sds):
        sds = sds[-MAX_SIGNAL_DAYS:]
    print(f"[tune] 离线安全信号日(<= {cutoff}): {len(sds)} 个", file=sys.stderr)

    # 预构建每个信号日的候选(仅本地有缓存的)
    days = []
    for sd, dal in sds:
        pairs = [(c, s) for c, s in _read_dal(dal) if _cached_exists(c)]
        if len(pairs) < 5:
            continue
        days.append((sd, pd.DataFrame(
            [{"日期": sd, "代码": c, "来源策略": s} for c, s in pairs])))
    print(f"[tune] 纳入回测信号日: {len(days)} 个", file=sys.stderr)

    # 收集每个组合在每个信号日的收益
    # combo -> list of (sd, total_return, win_rate, max_dd)
    records: dict[tuple, list] = {}

    def _eval(combo, sl, tp, tr, hd):
        # 改写 CONFIG base（生产一致缩放）
        saved = (
            rr.CONFIG["exit"]["stop_loss_pct"]["base"],
            rr.CONFIG["exit"]["take_profit_pct"]["base"],
            rr.CONFIG["exit"]["trailing_stop_pct"]["base"],
            rr.CONFIG["exit"]["hold_days"],
        )
        rr.CONFIG["exit"]["stop_loss_pct"]["base"] = sl
        rr.CONFIG["exit"]["take_profit_pct"]["base"] = tp
        rr.CONFIG["exit"]["trailing_stop_pct"]["base"] = tr
        rr.CONFIG["exit"]["hold_days"] = hd
        try:
            recs = []
            for sd, sig in days:
                s = _run(sig)
                if "error" in s:
                    recs.append((sd, None, None, None))
                else:
                    recs.append((sd, s["total_return"], s["win_rate"], s["max_drawdown"]))
            records[combo] = recs
        finally:
            rr.CONFIG["exit"]["stop_loss_pct"]["base"] = saved[0]
            rr.CONFIG["exit"]["take_profit_pct"]["base"] = saved[1]
            rr.CONFIG["exit"]["trailing_stop_pct"]["base"] = saved[2]
            rr.CONFIG["exit"]["hold_days"] = saved[3]

    # 基线
    _eval(BASELINE, *BASELINE)
    # 网格
    total = len(SL) * len(TP) * len(TR) * len(HD)
    done = 0
    for sl in SL:
        for tp in TP:
            for tr in TR:
                for hd in HD:
                    combo = (sl, tp, tr, hd)
                    if combo == BASELINE:
                        continue
                    _eval(combo, sl, tp, tr, hd)
                    done += 1
                    print(f"[tune] {done}/{total} sl={sl} tp={tp} tr={tr} hd={hd}", file=sys.stderr)

    # 汇总
    def _agg(combo):
        recs = records[combo]
        rets = [r[1] for r in recs]
        wins = [r[2] for r in recs]
        dds = [r[3] for r in recs]
        return _mean(rets), _mean(wins), _mean(dds), len([x for x in rets if x is not None])

    base_ret, base_win, base_dd, base_n = _agg(BASELINE)

    # 前后半稳健性
    base_recs = records[BASELINE]
    half = len(base_recs) // 2
    base_h1 = _mean([r[1] for r in base_recs[:half]])
    base_h2 = _mean([r[1] for r in base_recs[half:]])

    rows = []
    for combo in records:
        if combo == BASELINE:
            continue
        ret, win, dd, n = _agg(combo)
        if ret is None:
            continue
        recs = records[combo]
        h1 = _mean([r[1] for r in recs[:half]])
        h2 = _mean([r[1] for r in recs[half:]])
        robust = (
            h1 is not None and h2 is not None and base_h1 is not None and base_h2 is not None
            and h1 > base_h1 and h2 > base_h2
            and (ret - base_ret) >= IMPROVE_THRESH_PP
        )
        rows.append({
            "combo": combo, "ret": ret, "win": win, "dd": dd, "n": n,
            "d_ret": round(ret - base_ret, 4), "h1": h1, "h2": h2, "robust": robust,
        })
    rows.sort(key=lambda x: x["ret"], reverse=True)

    # 输出
    lines = []
    lines.append("# 出场阈值 walk-forward 扫描\n")
    lines.append(f"- 信号日样本：{base_n} 个（离线安全，纯本地行情）")
    lines.append(f"- 固定：partial_take_profit=开 / market_friction(limit_down)=开 / vol_target=关")
    lines.append(f"- 基线(exit base): SL={BASELINE[0]} TP={BASELINE[1]} TR={BASELINE[2]} HD={BASELINE[3]}"
                 f" → 平均组合收益 {base_ret}% / 胜率 {base_win}% / 回撤 {base_dd}%")
    lines.append(f"- 稳健性守卫：前后半均优于基线 且 整体改进 ≥ {IMPROVE_THRESH_PP}pp\n")

    lines.append("| 排名 | SL | TP | TR | HD | 样本 | 平均收益% | Δ收益(pp) | 胜率% | 回撤% | 前半 | 后半 | 稳健 |")
    lines.append("|------|----|----|----|----|------|----------|-----------|-------|-------|------|------|------|")
    for i, r in enumerate(rows[:15], 1):
        sl, tp, tr, hd = r["combo"]
        lines.append(
            f"| {i} | {sl} | {tp} | {tr} | {hd} | {r['n']} | {r['ret']} | "
            f"{r['d_ret']:+} | {r['win']} | {r['dd']} | {r['h1']} | {r['h2']} | "
            f"{'✓' if r['robust'] else '·'} |"
        )

    robust_rows = [r for r in rows if r["robust"]]
    lines.append("\n## 结论\n")
    if robust_rows:
        best = robust_rows[0]
        sl, tp, tr, hd = best["combo"]
        lines.append(
            f"- **稳健改进候选**：SL={sl} TP={tp} TR={tr} HD={hd}，"
            f"平均组合收益 {best['ret']}%（基线 {base_ret}%，Δ {best['d_ret']:+}pp），"
            f"前半 {best['h1']} / 后半 {best['h2']} 均优于基线，胜率 {best['win']}%、回撤 {best['dd']}%。"
        )
        lines.append(
            f"- 建议写回 risk_config.json 的 exit.base：stop_loss_pct={sl}、take_profit_pct={tp}、"
            f"trailing_stop_pct={tr}、hold_days={hd}（其余缩放逻辑不变）。"
        )
    else:
        lines.append(
            f"- 在 {len(rows)} 个候选中**未找到通过稳健性守卫的组合**：出场阈值微调在当前样本上"
            f"未产生稳健的收益率提升（置信不足）。建议保留当前基线配置（SL={BASELINE[0]} TP={BASELINE[1]}"
            f" TR={BASELINE[2]} HD={BASELINE[3]}），不做过拟合式调参。"
        )
        lines.append(
            "- 说明：此前 verify_new_exits 已确认 partial_take_profit=开 / limit_down=开 为净正改进并保留；"
            "本扫描聚焦其下的阈值微调，结论是微观调参空间有限。"
        )

    md = "\n".join(lines) + "\n"
    out_path = STOCK_DATA_DIR / "tune_exit_rules.md"
    out_path.write_text(md, encoding="utf-8")
    print(md)
    print(f"\n[tune] 报告已写入 {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
