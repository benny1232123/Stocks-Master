#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""每日收盘 Guéant (2023) 非对称网格检测 + 智能确认消音邮件推送脚本。"""
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

GUEANT_CFG = {
    "002284": {"name": "亚太股份", "lock_qty": 500, "unit": 200},
    "603187": {"name": "海容冷链", "lock_qty": 300, "unit": 100},
    "600269": {"name": "赣粤高速", "lock_qty": 1000, "unit": 400},
    "603390": {"name": "通达电气", "lock_qty": 100, "unit": 100},
}

ACK_TOKEN = "sm_rescue_2026"
ATR_DIFF_THRESHOLD = 0.008  # ATR 变化超过 0.8% 才提醒重新调整


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
    return f"sz{c}"


def fetch_quotes_and_atr(codes: list[str]) -> dict[str, dict]:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    syms = [to_tencent_sym(c) for c in codes]
    url = "http://qt.gtimg.cn/q=" + ",".join(syms)
    out: dict[str, dict] = {}
    with opener.open(urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}), timeout=8) as resp:
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
            "atr_pct": 0.025,
        }

    for c in codes:
        sym = to_tencent_sym(c)
        try:
            kurl = f"http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={sym},day,,,16,qfq"
            with opener.open(urllib.request.Request(kurl, headers={"User-Agent": "Mozilla/5.0"}), timeout=6) as resp:
                raw = json.loads(resp.read().decode("utf-8"))["data"][sym]["qfqday"]
            if len(raw) >= 15 and c in out:
                trs = []
                for i in range(1, len(raw)):
                    h = float(raw[i][3])
                    l = float(raw[i][4])
                    pc = float(raw[i - 1][2])
                    trs.append(max(h - l, abs(h - pc), abs(l - pc)))
                atr = sum(trs[-14:]) / 14.0
                last_c = float(raw[-1][2])
                if last_c > 0:
                    out[c]["atr_pct"] = max(0.015, min(0.050, atr / last_c))
        except Exception:
            pass
    return out


def main() -> int:
    load_dotenv(ROOT / ".env")
    force = "--force" in sys.argv
    today_str = datetime.now().strftime("%Y-%m-%d")
    holdings = fetch_net_holdings()
    quotes = fetch_quotes_and_atr(list(GUEANT_CFG.keys()))

    current_atrs: dict[str, float] = {}
    grid_info: dict[str, dict] = {}
    for code, cfg in GUEANT_CFG.items():
        atr = quotes.get(code, {}).get("atr_pct", 0.025)
        current_atrs[code] = round(atr, 4)
        grid_info[code] = {
            "sell_pct": round(0.8 * atr * 100, 1),
            "buy_pct": round(1.3 * atr * 100, 1),
        }

    pending_file = ROOT / "pending_plan.json"
    pending_file.write_text(
        json.dumps({"date": today_str, "atrs": current_atrs, "grid": grid_info}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    ack_file = ROOT / "ack_state.json"
    changed_codes: list[str] = []
    need_notify = force or (not ack_file.exists())
    if ack_file.exists() and not force:
        try:
            ack_data = json.loads(ack_file.read_text(encoding="utf-8"))
            ack_atrs = ack_data.get("atrs", {})
            for code in GUEANT_CFG:
                old_atr = float(ack_atrs.get(code, 0.0))
                if old_atr == 0.0 or abs(current_atrs[code] - old_atr) >= ATR_DIFF_THRESHOLD:
                    changed_codes.append(code)
            if changed_codes:
                need_notify = True
        except Exception:
            need_notify = True
    else:
        changed_codes = list(GUEANT_CFG.keys())

    if not need_notify:
        print(f"[SKIP] {today_str} 各股 ATR 相比已确认基线变化均 < 0.8%，无需调整手机条件单，静默跳过。")
        return 0

    tunnel_file = ROOT / "tunnel_url.txt"
    tunnel_url = tunnel_file.read_text(encoding="utf-8").strip() if tunnel_file.exists() else ""
    ack_link = f"{tunnel_url}/ack?token={ACK_TOKEN}" if tunnel_url else "https://stocks-master.pages.dev/"

    lines_md = [
        f"# 📋 今日收盘 Guéant 网格参数提醒 ({today_str})",
        "",
        "检测到你的持仓波动率（ATR）已达到调整阈值（或尚未确认最新参数），请核对手机国信金太阳 App【网格交易】参数：",
        "",
        "| 标的 | 最新收盘 | 14日ATR | 上涨卖出幅度 | 下跌买入幅度 | 单笔股数 | 持仓护栏 |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    rows_html = []

    for code, cfg in GUEANT_CFG.items():
        q = quotes.get(code, {})
        price = q.get("price", 0.0)
        chg = q.get("change_pct", 0.0)
        atr = current_atrs[code]
        sell_pct = grid_info[code]["sell_pct"]
        buy_pct = grid_info[code]["buy_pct"]
        total_qty = holdings.get(code, 0)
        unit = cfg["unit"]
        max_qty = total_qty + unit
        p_sell = round(price * (1.0 + 0.8 * atr), 2) if price > 0 else 0.0
        p_buy = round(price * (1.0 - 1.3 * atr), 2) if price > 0 else 0.0
        chg_str = f"{chg:+.2f}%"
        badge = " <span style='background:#fef3c7;color:#d97706;padding:1px 6px;border-radius:4px;font-size:11px;'>建议调整</span>" if code in changed_codes else ""

        lines_md.append(
            f"| {cfg['name']} ({code}) | {price:.2f} ({chg_str}) | {atr*100:.2f}% | +{sell_pct:.1f}% (>= {p_sell:.2f}) | -{buy_pct:.1f}% (<= {p_buy:.2f}) | {unit} 股 | 留{cfg['lock_qty']} / 上限{max_qty} |"
        )
        rows_html.append(
            f"<tr>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;'><b>{cfg['name']}</b> ({code}){badge}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;'>{price:.2f} ({chg_str})</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;'>{atr*100:.2f}%</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;color:#ef4444;font-weight:bold;'>+{sell_pct:.1f}% <span style='font-weight:normal;color:#64748b;font-size:11px;'>(&ge;{p_sell:.2f})</span></td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;color:#2563eb;font-weight:bold;'>-{buy_pct:.1f}% <span style='font-weight:normal;color:#64748b;font-size:11px;'>(&le;{p_buy:.2f})</span></td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;'>{unit} 股</td>"
            f"<td style='padding:8px;border-bottom:1px solid #eee;font-size:12px;color:#475569;'>留{cfg['lock_qty']} / 上限{max_qty}</td>"
            f"</tr>"
        )

    lines_md += ["", f"确认链接（点此确认后将不再重复提醒）：{ack_link}"]
    md_text = "\n".join(lines_md)

    html_body = (
        f"<div style='font-family:-apple-system,sans-serif;max-width:840px;margin:0 auto;color:#1e293b;'>"
        f"<h2>📋 收盘网格参数提醒 ({today_str})</h2>"
        f"<p style='color:#64748b;font-size:13px;'>系统每日 15:05 自动检测 14 日真实波幅。仅当波动率变化超过 0.8% 或尚未确认时发信提醒：</p>"
        f"<table style='width:100%;border-collapse:collapse;font-size:13px;text-align:left;'>"
        f"<thead><tr style='background:#f8fafc;border-bottom:2px solid #cbd5e1;'>"
        f"<th style='padding:8px;'>标的</th><th style='padding:8px;'>收盘价</th><th style='padding:8px;'>14日ATR</th>"
        f"<th style='padding:8px;'>上涨卖出幅度</th><th style='padding:8px;'>下跌买入幅度</th><th style='padding:8px;'>单笔股数</th><th style='padding:8px;'>持仓护栏</th>"
        f"</tr></thead><tbody>{''.join(rows_html)}</tbody></table>"
        f"<div style='margin:24px 0;text-align:center;'>"
        f"<a href='{ack_link}' style='display:inline-block;background:#2563eb;color:#ffffff;text-decoration:none;padding:12px 28px;border-radius:8px;font-weight:bold;font-size:15px;'>"
        f"✅ 我已在手机按此参数设置好（点击确认不再提醒）</a></div>"
        f"<p style='color:#94a3b8;font-size:12px;text-align:center;'>点击上方按钮后，在波动率发生显著变化前，系统将保持静默不再发送邮件。</p></div>"
    )

    out_dir = ROOT / "stock_data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_html = out_dir / f"rescue_action_plan_{today_str.replace('-', '')}.html"
    out_html.write_text(html_body, encoding="utf-8")

    logs: list[str] = []
    sent = send_email(
        subject=f"【Stocks-Master】收盘网格参数确认提醒 ({today_str})",
        content=md_text,
        html_content=html_body,
        log_lines=logs,
    )
    if sent:
        print(f"[OK] 收盘确认提醒邮件已发送！报告落盘: {out_html}")
    else:
        print(f"[INFO] 报告落盘: {out_html}；邮件未发: {'; '.join(logs)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
