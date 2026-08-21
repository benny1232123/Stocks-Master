"""每日持仓个股分析推送。

读取当前 FIFO 持仓 → 逐只 build_stock_analysis → 生成 Markdown 报告
→ 落盘 stock_data/holdings_analysis_YYYYMMDD.md + 可选 SMTP 邮件推送。

设计为「可本地手动跑，也可挂 CI 每日自动跑」（同一条命令）。

环境变量：
- TODAY / SIGNAL_DATE : 信号日 YYYYMMDD（默认今天）
- KLINE_BACKEND       : 建议 akshare（CI 海外 Runner 稳定）
- SUPABASE_URL/KEY    : 持仓数据源（auto 模式，与 daily-pick 共用 secrets）
- TRADES_BACKEND      : json | supabase | auto（默认 auto）
- SMTP_HOST/PORT/USER/PASS/TO : 邮件推送（缺任意 → 仅落盘，不报错）

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
        report = (
            f"# 持仓个股分析日报 · {today}\n\n"
            f"> 数据源：{backend}\n\n"
            f"**当前无持仓**（FIFO 计算结果为空），无需推送个股分析。\n"
        )
        out_path = STOCK_DATA_DIR / f"holdings_analysis_{today}.md"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report, encoding="utf-8")
        print("\n".join(log_lines))
        print(f"[OK] 报告已落盘: {out_path}")
        return 0

    codes = [str(c) for c in pos_df["代码"].tolist()]
    log_lines.append(f"当前持仓 {len(codes)} 只: {', '.join(codes)}")

    # 2) 逐只分析
    sections: list[str] = []
    ok, failed = 0, 0
    for code in codes:
        try:
            analysis = build_stock_analysis(code)
        except Exception as exc:
            failed += 1
            log_lines.append(f"分析 {code} 异常: {exc}")
            sections.append(f"### {code}\n\n⚠️ 分析异常：{exc}\n")
            continue
        if analysis.get("error"):
            failed += 1
        else:
            ok += 1
        sections.append(render_stock(analysis))

    # 3) 组装报告
    summary_line = f"成功 {ok} 只 / 失败 {failed} 只 / 共 {len(codes)} 只"
    report = (
        f"# 持仓个股分析日报 · {today}\n\n"
        f"> 数据源：{backend} ｜ {summary_line}\n\n"
        + "\n".join(sections)
        + f"\n---\n\n_本报告由 Stocks-Master 自动生成（技术面 Boll/MACD/RSI/KDJ/MA + 基本面/资金面）。\n"
        f"仅供研究参考，不构成投资建议。生成时间 {today}。_\n"
    )

    out_path = STOCK_DATA_DIR / f"holdings_analysis_{today}.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")

    print("\n".join(log_lines))
    print(f"[OK] 报告已落盘: {out_path}  ({summary_line})")

    # 4) 邮件推送（未配 SMTP 则仅落盘，不报错）
    subject = f"【持仓日报】{today} · {len(codes)}只持仓个股分析"
    if send_email(subject, report, log_lines=log_lines):
        print("[OK] 邮件已推送")
    else:
        print("[INFO] 未配置 SMTP 或缺失参数，仅落盘报告（不推送邮件）")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
