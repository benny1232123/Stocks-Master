#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""每日 World Model 云端条件单操作清单生成与邮件推送脚本。"""
from __future__ import annotations

import json
import os
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from smcore.notify.email import send_email

RESCUE_STRATEGY_CFG = {
    "002284": {"name": "亚太股份", "lock_qty": 500, "unit": 200, "pulse_pct": 0.022, "stop_price": 8.08, "role": "主力置换（高抛 + 破位保护）"},
    "603390": {"name": "通达电气", "lock_qty": 0, "unit": 200, "pulse_pct": 0.020, "stop_price": None, "role": "微型弱势（脉冲全清腾挪）"},
    "600269": {"name": "赣粤高速", "lock_qty": 1000, "unit": 800, "pulse_pct": 0.022, "stop_price": None, "role": "防御减仓（留 1000 股吃分红）"},
    "603187": {"name": "海容冷链", "lock_qty": 500, "unit": 0, "pulse_pct": 0.025, "stop_price": 9.80, "role": "破净白马（站上 MA20 坚定持有）"},
}


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def fetch_net_holdings() -> dict[str, int]:
    defaults = {"002284": 900, "603187": 500, "600269": 1800, "603390": 200}
    sb_url = os.getenv("SUPABASE_URL", "").strip()
    sb_key = os.getenv("SUPABASE_KEY", "").strip()
    if not sb_url or not sb_key:
        return defaults
    try:
        req = urllib.request.Request(
            f"{sb_url}/rest/v1/trades?select=code,side,quantity",
            headers={"apikey": sb_key, "Authorization": f"Bearer {sb_key}"},
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(req, timeout=8) as resp:
            rows = json.loads(resp.read().decode("utf-8"))
        net: dict[str, int] = {}
        for r in rows:
            c = str(r.get("code", "")).zfill(6)
            q = int(r.get("quantity", 0))
            side = str(r.get("side", "BUY")).upper()
            net[c] = net.get(c, 0) + (q if side == "BUY" else -q)
        return {k: v for k, v in net.items() if v > 0} or defaults
    except Exception:
        return defaults


def to_tencent_sym(code: str) -> str:
    c = str(code).zfill(6)
    if c.startswith(("60", "68", "90", "51", "58")):
        return f"sh{c}"
    if c.startswith(("00", "30", "20", "15")):
        return f"sz{c}"
    if c.startswith(("8", "4", "92")):
        return f"bj{c}"
    return f"sh{c}"


def fetch_quotes(codes: list[str]) -> dict[str, dict]:
    syms = [to_tencent_sym(c) for c in codes]
    url = "http://qt.gtimg.cn/q=" + ",".join(syms)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    out: dict[str, dict] = {}
    with opener.open(req, timeout=8) as resp:
        text = resp.read().decode("gbk", errors="ignore")
    for line in text.split(";"):
        if "~" not in line:
            continue
        f = line.split("~")
        if len(f) < 33:
            continue
        out[f[2]] = {
            "name": f[1],
            "price": float(f[3] or 0),
            "pre_close": float(f[4] or 0),
            "change_pct": float(f[32] or 0),
        }
    return out


def main() -> int:
    load_dotenv(ROOT / ".env")
    today_str = datetime.now().strftime("%Y-%m-%d")
    holdings = fetch_net_holdings()
    codes = list(RESCUE_STRATEGY_CFG.keys()) + ["510300"]
    quotes = fetch_quotes(codes)

    lines_md = [
        f"# 📋 今日国信金太阳云端条件单操作清单 ({today_str})",
        "",
        "策略模型：World Model & JEV 自适应置换（委托价选「对手价/买一价」，有效期选「30天」）",
        "",
        "| 标的 | 现价 (涨跌幅) | 持仓/锁底仓 | 上涨高抛条件单 | 破位保护线 | 预计释放现金 |",
        "| :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    rows_html = []

    for code, cfg in RESCUE_STRATEGY_CFG.items():
        q = quotes.get(code, {})
        price = q.get("price", 0.0)
        pre_close = q.get("pre_close", price) or price
        chg = q.get("change_pct", 0.0)
        total_qty = holdings.get(code, 0)
        releasable = max(0, total_qty - cfg["lock_qty"])
        sell_qty = min(cfg["unit"], releasable) if cfg["unit"] > 0 else 0
        base_ref = max(pre_close, price * 0.99) if pre_close > 0 else price
        trigger = round(base_ref * (1.0 + cfg["pulse_pct"]), 2) if base_ref > 0 else 0.0
        est_cash = round(sell_qty * trigger) if sell_qty > 0 and trigger > 0 else 0

        sell_str = f">= {trigger:.2f} 元卖 {sell_qty} 股" if sell_qty > 0 else "暂不卖出（继续持有）"
        stop_str = f"<= {cfg['stop_price']:.2f} 元" if cfg["stop_price"] else "--"
        cash_str = f"约 {est_cash:,} 元" if est_cash > 0 else "--"
        chg_str = f"{chg:+.2f}%"
        lines_md.append(
            f"| {cfg['name']} ({code}) | {price:.2f} ({chg_str}) | {total_qty}/{cfg['lock_qty']} | {sell_str} | {stop_str} | {cash_str} |"
        )
        rows_html.append(
            f"<tr>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;'><b>{cfg['name']}</b> ({code})</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;'>{price:.2f} ({chg_str})</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;'>{total_qty} / 锁{cfg['lock_qty']}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;color:#2563eb;font-weight:bold;'>{sell_str}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;color:#d97706;'>{stop_str}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;'>{cash_str}</td>"
            f"</tr>"
        )

    etf_q = quotes.get("510300", {})
    etf_p = etf_q.get("price", 4.0)
    etf_buy = round((etf_q.get("pre_close", etf_p) or etf_p) * 0.992, 3)

    lines_md += [
        "",
        f"## 🎯 正期望置换池：510300 沪深300ETF（现价 {etf_p:.3f} 元）",
        f"- 卖出释放现金后，可在条件单设置 **<= {etf_buy:.3f} 元买入 500 股**（或直接手动转入）。",
    ]
    md_text = "\n".join(lines_md)

    html_body = (
        f"<div style='font-family:sans-serif;max-width:780px;margin:0 auto;'>"
        f"<h2>📋 今日国信金太阳云端条件单操作清单 ({today_str})</h2>"
        f"<p style='color:#666;font-size:13px;'>模型：World Model &amp; JEV 自适应置换（委托价选「对手价/买一价」，有效期选「30天」）</p>"
        f"<table style='width:100%;border-collapse:collapse;font-size:13px;text-align:left;'>"
        f"<thead><tr style='background:#f8fafc;border-bottom:2px solid #cbd5e1;'>"
        f"<th style='padding:8px;'>标的</th><th style='padding:8px;'>最新现价</th><th style='padding:8px;'>持仓/锁底仓</th>"
        f"<th style='padding:8px;'>上涨高抛条件单</th><th style='padding:8px;'>破位保护线</th><th style='padding:8px;'>预计释放现金</th>"
        f"</tr></thead><tbody>{''.join(rows_html)}</tbody></table>"
        f"<div style='margin-top:16px;padding:12px;background:#f5f3ff;border-left:4px solid #7c3aed;font-size:13px;'>"
        f"<b>🎯 正期望置换资金池（510300 沪深300ETF）</b>：现价 <b>{etf_p:.3f} 元</b>，"
        f"释放现金后可设回落条件单 <b>&le; {etf_buy:.3f} 元买入 500 股</b>。</div></div>"
    )

    out_dir = ROOT / "stock_data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_html = out_dir / f"rescue_action_plan_{today_str.replace('-', '')}.html"
    out_html.write_text(html_body, encoding="utf-8")

    logs: list[str] = []
    sent = send_email(
        subject=f"【Stocks-Master】今日云端条件单操作清单 ({today_str})",
        content=md_text,
        html_content=html_body,
        log_lines=logs,
    )
    if sent:
        print(f"[OK] 操作清单邮件已发送！报告已落盘: {out_html}")
    else:
        print(f"[INFO] 报告已落盘: {out_html}；本地未推送邮件原因: {'; '.join(logs)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
