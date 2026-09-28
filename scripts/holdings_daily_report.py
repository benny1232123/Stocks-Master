#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""持仓日报生成器（2026-09-28）：纪律核对 + 技术结构 + 市场状态 + 盈亏汇总。

双用途：
- 本地：python scripts/holdings_daily_report.py → stock_data/holdings_report.md
- 云端：build_sections() 被 notify_holdings_analysis.py 导入，
  作为「纪律与结构监控」section 嵌入每日持仓分析（holdings_analysis_{date}）。

数据源优先级：云端读 Supabase 活持仓（smcore.holdings，FIFO）；
无云端/本地回退 stock_data/holdings_watch.json（含人工设定的 floor/target 条件）。

输出刻意**不进 git 推送**（本地路径）——但云端 holdings_analysis 文件本身
经仓库回写由 Render 看板读取（admin 权限保护，与既有日报同权限）。
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
OUT = STOCK_DATA_DIR / "holdings_report.md"
DISASTER_LINE = 18400  # 账户灾难线（Grossman-Zhou 式，用户设定）


def _kline(code: str) -> pd.DataFrame:
    d = read_kline_cache(code, base_dir=STOCK_DATA_DIR / "k_data")
    d["date"] = pd.to_datetime(d["date"])
    d = d.sort_values("date").reset_index(drop=True)
    for c in ("close", "high", "low"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    return d.dropna(subset=["close"])


def _weekly(d: pd.DataFrame) -> pd.DataFrame:
    return d.set_index("date").resample("W").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last"}).dropna()


def _rsi14(wc: pd.Series) -> float:
    delta = wc.diff().dropna()
    up = delta.clip(lower=0).tail(14).mean()
    dn = (-delta.clip(upper=0)).tail(14).mean()
    return 100 - 100 / (1 + up / dn) if dn > 0 else 100.0


def _positions() -> tuple[dict, str]:
    """持仓来源：Supabase 活仓（FIFO）优先，回退本地 watch 配置。

    返回 ({code6: {cost, qty, floor, target, note}}, 来源说明)。
    """
    watch: dict[str, dict] = {}
    if CONFIG.exists():
        try:
            for p in json.loads(CONFIG.read_text(encoding="utf-8")):
                watch[str(p["code"]).zfill(6)] = p
        except Exception:
            watch = {}
    try:
        from smcore.holdings import load_trades, compute_fifo_positions
        pos_df, _ = compute_fifo_positions(load_trades())
        if pos_df.empty:
            raise ValueError("无活仓")
        out = {}
        for _, r in pos_df.iterrows():
            c6 = str(r["code"]).zfill(6)
            w = watch.get(c6, {})
            out[c6] = {"cost": float(r["avg_cost"]), "qty": float(r["qty"]),
                       "floor": w.get("floor"), "target": w.get("target"),
                       "note": w.get("note", ""), "name": w.get("name", "")}
        return out, "Supabase 活持仓（FIFO）"
    except Exception:
        out = {}
        for c6, w in watch.items():
            out[c6] = {"cost": float(w["cost"]), "qty": float(w.get("qty", 0)),
                       "floor": w.get("floor"), "target": w.get("target"),
                       "note": w.get("note", ""), "name": w.get("name", "")}
        return out, "本地 holdings_watch.json"


def _tech_block(code: str, d: pd.DataFrame | None = None) -> dict:
    d = d if d is not None else _kline(code)
    w = _weekly(d)
    wc = w["close"]
    px = float(d["close"].iloc[-1])
    ma60 = float(d["close"].tail(60).mean())
    w20hi, w20lo = float(w["high"].tail(20).max()), float(w["low"].tail(20).min())
    y = d.set_index("date")["close"].resample("YE").last().pct_change()
    return {
        "px": px, "date": str(d["date"].iloc[-1].date()),
        "ma60": ma60, "dist60": px / ma60 - 1,
        "w10": float(wc.tail(10).mean()), "w20": float(wc.tail(20).mean()),
        "w30": float(wc.tail(30).mean()),
        "w20hi": w20hi, "w20lo": w20lo,
        "w20pos": (px - w20lo) / (w20hi - w20lo) if w20hi > w20lo else 0.5,
        "rsi": _rsi14(wc),
        "y_pct": float((px - d["close"].min()) / (d["close"].max() - d["close"].min())),
        "ytd": float(y.iloc[-1]) if len(y) else float("nan"),
    }


def build_sections() -> tuple[str, str, dict]:
    """构建「纪律与结构监控」section → (section_md, section_html, meta)。

    meta: {alerts, n, total_mv, total_cost, data_date, source, disaster_line}
    供本地 main() 写独立日报，也供 notify_holdings_analysis.py 嵌入云端日报。
    """
    pos, source = _positions()
    rows, alerts = [], 0
    total_mv = total_cost = 0.0
    data_date = None
    for code, p in sorted(pos.items()):
        try:
            d = _kline(code)
            lv = _compute_boll_levels(code) or {}
        except Exception as exc:
            rows.append({"code": code, "name": p.get("name", ""), "state": "无数据",
                         "note": f"k_data 缺失（{exc}）", "px": None, "cost": p["cost"],
                         "pnl": None, "qty": p["qty"], "mv": 0.0, "floor": p.get("floor"),
                         "target": p.get("target"), "tech": None, "ytd": None})
            continue
        t = _tech_block(code, d)
        data_date = max(data_date or "", t["date"])
        px = t["px"]
        mv = px * p.get("qty", 0)
        cost = p["cost"] * p.get("qty", 0)
        total_mv += mv
        total_cost += cost
        floor, target = p.get("floor"), p.get("target")
        auto_floor = lv.get("lower")
        if floor is None and auto_floor:
            floor = round(auto_floor, 2)
        state, note = "持有", []
        if floor and px <= floor:
            state, alerts = "ALERT", alerts + 1
            note.append(f"⚠️ 跌破硬底线 {floor}")
        if target and px >= target:
            state = "TAKE"
            note.append(f"到达减仓位 {target}")
        if not note:
            note.append(f"观望（下轨参考 {auto_floor:.2f}）" if auto_floor else "观望")
        rows.append({"code": code, "name": p.get("name", ""), "state": state, "note": "；".join(note),
                     "px": px, "cost": p["cost"], "pnl": px / p["cost"] - 1, "qty": p.get("qty", 0),
                     "mv": mv, "floor": floor, "target": target, "tech": t,
                     "ytd": t["ytd"], "dist60": t["dist60"]})

    md = ["## 🛡️ 纪律与结构监控（Stocks-Master 硬条件核对）", ""]
    if alerts:
        md.insert(1, f"**⚠️ 今日 {alerts} 项 ALERT —— 触发即执行，不再讨论。**")
    md.append("")
    md.append("| 代码 | 状态 | 现价 | 硬底线 | 减仓位 | 盈亏 | 备注 |")
    md.append("|---|---|---|---|---|---|---|")
    for r in rows:
        px_s = f"{r['px']:.2f}" if r["px"] else "—"
        pnl_s = f"{r['pnl']:+.1%}" if r["pnl"] is not None else "—"
        md.append(f"| {r['code']} {r['name']} | **{r['state']}** | {px_s} | "
                  f"{r['floor'] or '—'} | {r['target'] or '—'} | {pnl_s} | {r['note']} |")
    md.append("")
    md.append("| 代码 | 现价vs MA60 | 10/20/30周线 | 20周区间位置 | 周RSI14 | 年内 | 全史分位 |")
    md.append("|---|---|---|---|---|---|---|")
    for r in rows:
        t = r.get("tech")
        if not t:
            continue
        p10 = "上" if t["px"] > t["w10"] else "下"
        p20 = "上" if t["px"] > t["w20"] else "下"
        p30 = "上" if t["px"] > t["w30"] else "下"
        md.append(f"| {r['code']} | {t['dist60']:+.1%} | {p10}/{p20}/{p30} "
                  f"({t['w10']:.2f}/{t['w20']:.2f}/{t['w30']:.2f}) | {t['w20pos']:.0%} | "
                  f"{t['rsi']:.0f} | {t['ytd']:+.0%} | {t['y_pct']:.0%} |")
    md += [
        "",
        f"> 决策框架（不变项）：触发即执行；卖出资金回系统（防御市遵守现金指引）；"
        f"账户灾难线 **{DISASTER_LINE:,}**（触发则股票仓位减半）；防御市禁止补仓。"
        f"持仓来源：{source}。",
        "",
    ]
    section_md = "\n".join(md)

    # 简洁 HTML（与云端的 sections_html 拼接模式兼容）
    th = "".join(f"<th>{h}</th>" for h in
                 ("代码", "状态", "现价", "硬底线", "减仓位", "盈亏", "备注"))
    trs = []
    for r in rows:
        px_s = f"{r['px']:.2f}" if r["px"] else "—"
        pnl_s = f"{r['pnl']:+.1%}" if r["pnl"] is not None else "—"
        color = "#c0392b" if r["state"] == "ALERT" else ("#27ae60" if r["state"] == "TAKE" else "#333")
        trs.append(
            f"<tr><td>{r['code']} {r['name']}</td><td style='color:{color};font-weight:bold'>"
            f"{r['state']}</td><td>{px_s}</td><td>{r['floor'] or '—'}</td>"
            f"<td>{r['target'] or '—'}</td><td>{pnl_s}</td><td>{r['note']}</td></tr>")
    section_html = (
        "<div class='section'><h2>🛡️ 纪律与结构监控（Stocks-Master 硬条件核对）</h2>"
        + (f"<p style='color:#c0392b;font-weight:bold'>⚠️ 今日 {alerts} 项 ALERT —— 触发即执行。</p>"
           if alerts else "")
        + f"<table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse'>"
          f"<tr>{th}</tr>{''.join(trs)}</table>"
        + f"<p style='color:#666'>框架：触发即执行；资金回系统；灾难线 {DISASTER_LINE:,}；"
          f"防御市禁补仓。来源：{source}。</p></div>")

    meta = {"alerts": alerts, "n": len(rows), "total_mv": total_mv, "total_cost": total_cost,
            "data_date": data_date, "source": source, "disaster_line": DISASTER_LINE}
    return section_md, section_html, meta


def main() -> int:
    section_md, _html, meta = build_sections()
    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    regime, reg_date = "未知", ""
    rf = STOCK_DATA_DIR / "regime-latest.json"
    if rf.exists():
        try:
            m = json.loads(rf.read_text(encoding="utf-8"))
            regime, reg_date = m.get("regime", regime), m.get("date", "")
        except Exception:
            pass
    L = [
        f"# 持仓日报 {today}",
        "",
        f"- 数据截至：{meta['data_date'] or 'N/A'} 收盘｜市场状态：**{regime}**（快照 {reg_date}，可能滞后）",
        f"- 组合市值 **{meta['total_mv']:,.0f}** / 成本 {meta['total_cost']:,.0f} / "
        f"浮亏 {meta['total_mv'] - meta['total_cost']:+,.0f}"
        f"（{meta['total_mv'] / max(1e-9, meta['total_cost']) - 1:+.1%}）｜"
        f"**ALERT {meta['alerts']} 项**",
        "",
        section_md,
    ]
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    dated = STOCK_DATA_DIR / f"holdings_report_{datetime.now().strftime('%Y%m%d')}.md"
    dated.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"持仓日报已写 {OUT}（ALERT {meta['alerts']}）")
    return 1 if meta["alerts"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
