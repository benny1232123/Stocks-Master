"""每日持仓个股分析推送。

读取当前 FIFO 持仓 → 逐只 build_stock_analysis → 生成报告
→ 落盘 stock_data/holdings_analysis_YYYYMMDD.md（兼容）+ .html（美观版）
→ 多通道推送：SMTP 邮件 / PushPlus 微信（任一配置即推送；都不配仅落盘）。

设计为「可本地手动跑，也可挂 CI 每日自动跑」（同一条命令）。

环境变量：
- TODAY / SIGNAL_DATE : 信号日 YYYYMMDD（默认今天）
- KLINE_BACKEND       : 建议 akshare（CI 海外 Runner 稳定）
- SUPABASE_URL/KEY    : 持仓数据源（auto 模式，与 daily-pick 共用 secrets）
- TRADES_BACKEND      : json | supabase | auto（默认 auto）
- SMTP_HOST/PORT/USER/PASS/TO : 邮件推送（缺任意 → 跳过邮件）

退出码：0 = 正常（含空持仓）；1 = 致命错误（持仓读取失败等）。
"""
from __future__ import annotations

import os
import sys
from datetime import date

# 允许从仓库根目录直接运行（python scripts/notify_holdings_analysis.py）
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from smcore.storage.trades_repo import get_trade_repository
from smcore.holdings import compute_fifo_positions, load_trades
from smcore.analysis import build_stock_analysis
from smcore.config.defaults import STOCK_DATA_DIR
from smcore.notify.email import send_email


def _today_str() -> str:
    for key in ("TODAY", "SIGNAL_DATE"):
        val = (os.getenv(key) or "").strip()
        if val:
            return val
    return date.today().strftime("%Y%m%d")


def fmt_num(v, digits: int = 2, default: str = "—") -> str:
    if v is None:
        return default
    try:
        return f"{float(v):.{digits}f}"
    except (TypeError, ValueError):
        return default


def render_stock(analysis: dict) -> str:
    """把单只票的分析字典渲染成 Markdown 段落。"""
    code = analysis.get("code", "")
    if analysis.get("error"):
        return f"### {code}\n\n⚠️ 分析失败：{analysis['error']}\n"

    latest = analysis.get("latest", {}) or {}
    metrics = analysis.get("metrics", {}) or {}
    close = latest.get("close")
    up, lo, mid = latest.get("upper"), latest.get("lower"), latest.get("middle")
    ma5, ma10, ma20, ma60 = (
        latest.get("ma5"),
        latest.get("ma10"),
        latest.get("ma20"),
        latest.get("ma60"),
    )
    rsi = latest.get("rsi")
    dif, dea, hist = latest.get("dif"), latest.get("dea"), latest.get("macd_hist")
    k, d, j = latest.get("k_val"), latest.get("d_val"), latest.get("j_val")
    sig = metrics.get("signal_text") or latest.get("signal_text") or "—"

    # 趋势
    trend = "—"
    if close is not None and ma20 is not None and ma60 is not None:
        if close > ma20 > ma60:
            trend = "多头（价>MA20>MA60）"
        elif close < ma20 < ma60:
            trend = "空头（价<MA20<MA60）"
        else:
            trend = "震荡"

    # 布林带位置
    band_pos = "—"
    if close is not None and up is not None and lo is not None and up > lo:
        pct = (close - lo) / (up - lo) * 100
        band_pos = f"{pct:.1f}%（上轨{fmt_num(up)} / 下轨{fmt_num(lo)} / 中轨{fmt_num(mid)}）"

    # MACD 状态
    macd_state = "—"
    if dif is not None and dea is not None and hist is not None:
        if dif > 0 and hist > 0:
            macd_state = "金叉区·多头动能"
        elif dif < 0 and hist < 0:
            macd_state = "死叉区·空头动能"
        elif hist > 0:
            macd_state = "红柱·转强"
        else:
            macd_state = "绿柱·偏弱"

    # KDJ 状态
    kdj_state = "—"
    if k is not None and d is not None and j is not None:
        if j > 100:
            kdj_state = f"超买(J>100) K={k:.0f} D={d:.0f} J={j:.0f}"
        elif j < 0:
            kdj_state = f"超卖(J<0) K={k:.0f} D={d:.0f} J={j:.0f}"
        else:
            kdj_state = f"K={k:.0f} D={d:.0f} J={j:.0f}"

    lines = [
        f"### {code}",
        "",
        f"- **Boll 信号**：{sig}",
        f"- **现价**：{fmt_num(close)} ｜ **趋势**：{trend}",
        f"- **均线**：MA5={fmt_num(ma5)} MA10={fmt_num(ma10)} MA20={fmt_num(ma20)} MA60={fmt_num(ma60)}",
        f"- **布林带位置**：{band_pos}",
        f"- **RSI(14)**：{fmt_num(rsi, 1)}",
        f"- **MACD**：DIF={fmt_num(dif)} DEA={fmt_num(dea)} 柱={fmt_num(hist)} → {macd_state}",
        f"- **KDJ**：{kdj_state}",
    ]

    # 基本面
    fund = analysis.get("fundamentals")
    if isinstance(fund, dict):
        if fund.get("error"):
            lines.append(f"- **基本面**：暂不可用（{fund['error']}）")
        else:
            fl = []
            if fund.get("pe") is not None:
                fl.append(f"PE={fmt_num(fund['pe'])}")
            if fund.get("pb") is not None:
                fl.append(f"PB={fmt_num(fund['pb'])}")
            if fund.get("mkt_cap") is not None:
                fl.append(f"总市值={fmt_num(fund['mkt_cap'])}亿")
            if fund.get("roe") is not None:
                fl.append(f"ROE={fmt_num(fund['roe'], 1)}%")
            if fund.get("gross_margin") is not None:
                fl.append(f"毛利率={fmt_num(fund['gross_margin'], 1)}%")
            if fund.get("revenue_growth") is not None:
                fl.append(f"营收增速={fmt_num(fund['revenue_growth'], 1)}%")
            if fund.get("turnover") is not None:
                fl.append(f"换手={fmt_num(fund['turnover'], 2)}%")
            lines.append(f"- **基本面**：{' / '.join(fl) if fl else '暂无数据'}")
    else:
        lines.append("- **基本面**：暂无数据")

    lines.append("")
    return "\n".join(lines)


# ───────────────────────── HTML 美观报告（PushPlus / 本地预览）─────────────────────────
# 内联 CSS，无外部依赖；涨=红、跌=绿（A股习惯）。PushPlus template=html 会在微信里渲染成文章。
_HTML_STYLE = """
*{box-sizing:border-box;margin:0;padding:0}
body{background:#eef0f3;font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei",sans-serif;color:#1f2329;padding:16px}
.wrap{max-width:680px;margin:0 auto}
.top{background:linear-gradient(135deg,#2b5876,#4e4376);color:#fff;border-radius:16px;padding:18px 20px;margin-bottom:14px;box-shadow:0 4px 14px rgba(43,88,118,.25)}
.top h1{font-size:20px;font-weight:700;letter-spacing:.5px}
.meta{font-size:13px;opacity:.9;margin-top:6px}
.card{background:#fff;border-radius:14px;padding:14px 16px;margin-bottom:12px;box-shadow:0 1px 4px rgba(0,0,0,.06)}
.card-head{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.code{font-size:19px;font-weight:700;letter-spacing:.5px}
.badge{font-size:12px;padding:3px 11px;border-radius:20px;font-weight:600}
.badge.bull{background:#fff1f0;color:#d4380d}
.badge.bear{background:#f6ffed;color:#389e0d}
.badge.neutral{background:#f0f1f3;color:#5a6068}
.price{font-size:19px;font-weight:700;margin-left:auto}
.chg{font-size:13px;font-weight:600;padding:3px 9px;border-radius:8px}
.chg.bull{background:#fff1f0;color:#d4380d}
.chg.bear{background:#f6ffed;color:#389e0d}
.chg.neutral{color:#5a6068}
.sig{font-size:13px;color:#5a6068;margin:8px 0 4px}
.boll{margin:8px 0 4px}
.boll-track{height:9px;background:linear-gradient(90deg,#389e0d,#fadb14,#d4380d);border-radius:6px;position:relative}
.boll-mark{position:absolute;top:50%;width:15px;height:15px;background:#fff;border:3px solid #2b5876;border-radius:50%;transform:translate(-50%,-50%)}
.boll-lab{font-size:11px;color:#8a9099;margin-top:5px}
.metrics{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0 4px}
.m{background:#f7f8fa;border-radius:9px;padding:6px 10px;min-width:62px}
.ml{display:block;font-size:11px;color:#8a9099}
.mv{display:block;font-size:15px;font-weight:600;margin-top:2px}
.mv.warn{color:#d4380d}
.mv.cool{color:#389e0d}
.state{font-size:12px;color:#5a6068;margin:4px 0 8px}
.chips{display:flex;flex-wrap:wrap;gap:6px}
.chip{font-size:11px;background:#eef2ff;color:#3b5bdb;padding:3px 9px;border-radius:20px}
.chip.dim{background:#f0f1f3;color:#a0a6ad}
.err{color:#d4380d;font-size:13px;margin-top:6px}
.foot{font-size:11px;color:#a0a6ad;text-align:center;margin-top:12px;line-height:1.7}
"""


def render_stock_html(analysis: dict) -> str:
    """把单只票的分析字典渲染成好看的 HTML 卡片。"""
    code = analysis.get("code", "")
    if analysis.get("error"):
        return (
            f'<div class="card"><div class="card-head"><span class="code">{code}</span></div>'
            f'<div class="err">⚠️ 分析失败：{analysis["error"]}</div></div>'
        )

    latest = analysis.get("latest", {}) or {}
    metrics = analysis.get("metrics", {}) or {}
    close = latest.get("close")
    up, lo = latest.get("upper"), latest.get("lower")
    ma5, ma10, ma20, ma60 = (
        latest.get("ma5"), latest.get("ma10"), latest.get("ma20"), latest.get("ma60")
    )
    rsi = latest.get("rsi")
    dif, dea, hist = latest.get("dif"), latest.get("dea"), latest.get("macd_hist")
    k, d, j = latest.get("k_val"), latest.get("d_val"), latest.get("j_val")
    sig = metrics.get("signal_text") or latest.get("signal_text") or "—"

    # 趋势 + 颜色
    trend, trend_cls = "震荡", "neutral"
    if close is not None and ma20 is not None and ma60 is not None:
        if close > ma20 > ma60:
            trend, trend_cls = "多头", "bull"
        elif close < ma20 < ma60:
            trend, trend_cls = "空头", "bear"

    # 当日涨跌幅（取序列末两根收盘价）
    chg_pct, chg_cls = None, "neutral"
    rows = (analysis.get("series", {}) or {}).get("rows", []) or []
    if len(rows) >= 2 and rows[-1].get("close") is not None and rows[-2].get("close"):
        try:
            cur = float(rows[-1]["close"])
            prev = float(rows[-2]["close"])
            if prev:
                chg_pct = (cur - prev) / prev * 100
                chg_cls = "bull" if chg_pct >= 0 else "bear"
        except (TypeError, ValueError):
            pass

    # 布林带位置
    band_pos = None
    if close is not None and up is not None and lo is not None and up > lo:
        band_pos = max(0.0, min(100.0, (close - lo) / (up - lo) * 100))

    macd_state = "—"
    if dif is not None and dea is not None and hist is not None:
        if dif > 0 and hist > 0:
            macd_state = "金叉·多头动能"
        elif dif < 0 and hist < 0:
            macd_state = "死叉·空头动能"
        elif hist > 0:
            macd_state = "红柱转强"
        else:
            macd_state = "绿柱偏弱"
    kdj_state = "—"
    if k is not None and d is not None and j is not None:
        if j > 100:
            kdj_state = f"超买 J={j:.0f}"
        elif j < 0:
            kdj_state = f"超卖 J={j:.0f}"
        else:
            kdj_state = f"K={k:.0f} D={d:.0f} J={j:.0f}"

    def _m(label: str, val, cls: str = "") -> str:
        return (
            f'<div class="m"><span class="ml">{label}</span>'
            f'<span class="mv {cls}">{fmt_num(val)}</span></div>'
        )

    rsi_cls = ""
    if rsi is not None:
        rsi_cls = "warn" if rsi > 70 else ("cool" if rsi < 30 else "")

    # 基本面标签
    fund = analysis.get("fundamentals")
    chips: list[str] = []
    if isinstance(fund, dict) and not fund.get("error"):
        if fund.get("pe") is not None:
            chips.append(("PE", fmt_num(fund["pe"])))
        if fund.get("pb") is not None:
            chips.append(("PB", fmt_num(fund["pb"])))
        if fund.get("mkt_cap") is not None:
            chips.append(("市值", f'{fmt_num(fund["mkt_cap"])}亿'))
        if fund.get("roe") is not None:
            chips.append(("ROE", f'{fmt_num(fund["roe"], 1)}%'))
        if fund.get("gross_margin") is not None:
            chips.append(("毛利", f'{fmt_num(fund["gross_margin"], 1)}%'))
        if fund.get("revenue_growth") is not None:
            chips.append(("营收增速", f'{fmt_num(fund["revenue_growth"], 1)}%'))
        if fund.get("turnover") is not None:
            chips.append(("换手", f'{fmt_num(fund["turnover"], 2)}%'))
    chip_html = (
        "".join(f'<span class="chip">{l} {v}</span>' for l, v in chips)
        if chips
        else '<span class="chip dim">暂无基本面数据</span>'
    )

    bar_html = ""
    if band_pos is not None:
        bar_html = (
            f'<div class="boll"><div class="boll-track">'
            f'<div class="boll-mark" style="left:{band_pos:.1f}%"></div></div>'
            f'<div class="boll-lab">布林带位置 {band_pos:.0f}%（下轨 {fmt_num(lo)} → 上轨 {fmt_num(up)}）</div></div>'
        )

    chg_html = (
        f'<span class="chg {chg_cls}">{"+" if chg_pct >= 0 else ""}{chg_pct:.2f}%</span>'
        if chg_pct is not None
        else ""
    )

    return f"""
    <div class="card">
      <div class="card-head">
        <span class="code">{code}</span>
        <span class="badge {trend_cls}">{trend}</span>
        <span class="price">{fmt_num(close)}</span>
        {chg_html}
      </div>
      <div class="sig">Boll 信号：{sig}</div>
      {bar_html}
      <div class="metrics">
        {_m("MA5", ma5)} {_m("MA10", ma10)} {_m("MA20", ma20)} {_m("MA60", ma60)}
        {_m("RSI", rsi, rsi_cls)} {_m("DIF", dif)} {_m("DEA", dea)} {_m("MACD柱", hist)}
        {_m("K", k)} {_m("D", d)} {_m("J", j)}
      </div>
      <div class="state">MACD：{macd_state} ｜ KDJ：{kdj_state}</div>
      <div class="chips">{chip_html}</div>
    </div>"""


def build_html_report(today: str, backend: str, summary_line: str, sections_html: str) -> str:
    return f"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>持仓个股分析 {today}</title><style>{_HTML_STYLE}</style></head>
<body><div class="wrap">
  <div class="top"><h1>📊 持仓个股分析日报</h1>
    <div class="meta">📅 {today} ｜ 数据源 {backend} ｜ {summary_line}</div></div>
  {sections_html}
  <div class="foot">Stocks-Master 自动生成 · 技术面 Boll/MACD/RSI/KDJ/MA + 基本面/资金面<br>
  仅供研究参考，不构成投资建议</div>
</div></body></html>"""


def main() -> int:
    today = _today_str()
    log_lines: list[str] = []

    # 1) 读取持仓
    try:
        repo = get_trade_repository()
        trades = load_trades()
        pos_df, _closed = compute_fifo_positions(trades)
    except Exception as exc:  # 持仓读取失败属致命
        print(f"[FATAL] 读取持仓失败: {exc}")
        return 1

    backend = repo.backend_name
    log_lines.append(f"持仓数据源: {backend} | 交易记录: {len(trades)} 条")

    if pos_df.empty:
        log_lines.append("当前无持仓，跳过个股分析")
        md = (
            f"# 持仓个股分析日报占位\n\n> 数据源：{backend}\n\n"
            f"**当前无持仓**（FIFO 计算结果为空），无需推送个股分析。\n"
        )
        html = build_html_report(
            today, backend, "无持仓",
            '<div class="card"><div class="sig">当前无持仓，无需推送个股分析。</div></div>',
        )
    else:
        codes = [str(c) for c in pos_df["代码"].tolist()]
        log_lines.append(f"当前持仓 {len(codes)} 只: {', '.join(codes)}")

        sections: list[str] = []
        sections_html: list[str] = []
        ok, failed = 0, 0
        for code in codes:
            try:
                analysis = build_stock_analysis(code)
            except Exception as exc:
                failed += 1
                log_lines.append(f"分析 {code} 异常: {exc}")
                sections.append(f"### {code}\n\n⚠️ 分析异常：{exc}\n")
                sections_html.append(
                    f'<div class="card"><div class="card-head"><span class="code">{code}</span>'
                    f'</div><div class="err">⚠️ 分析异常：{exc}</div></div>'
                )
                continue
            if analysis.get("error"):
                failed += 1
            else:
                ok += 1
            sections.append(render_stock(analysis))
            sections_html.append(render_stock_html(analysis))

        summary_line = f"成功 {ok} 只 / 失败 {failed} 只 / 共 {len(codes)} 只"
        md = (
            f"# 持仓个股分析日报 · {today}\n\n"
            f"> 数据源：{backend} ｜ {summary_line}\n\n"
            + "\n".join(sections)
            + f"\n---\n\n_本报告由 Stocks-Master 自动生成（技术面 Boll/MACD/RSI/KDJ/MA + 基本面/资金面）。\n"
            f"仅供研究参考，不构成投资建议。生成时间 {today}。_\n"
        )
        html = build_html_report(today, backend, summary_line, "\n".join(sections_html))

    # 2) 落盘（Markdown 兼容 + HTML 美观版）
    md_path = STOCK_DATA_DIR / f"holdings_analysis_{today}.md"
    html_path = STOCK_DATA_DIR / f"holdings_analysis_{today}.html"
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(md, encoding="utf-8")
    html_path.write_text(html, encoding="utf-8")

    print("\n".join(log_lines))
    print(f"[OK] 报告已落盘:\n  - {md_path}\n  - {html_path}")

    # 3) 邮件推送（配置 SMTP 即推送，否则仅落盘）
    subject = f"【持仓日报】{today} · 持仓个股分析"
    if send_email(subject, md, log_lines=log_lines):
        print("[OK] 已通过邮件推送")
    else:
        print("[INFO] 未配置 SMTP，仅落盘报告（不推送）")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
