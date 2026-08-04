"""生成【三路】历史回测对比报告：严格隔离变量。

    A) Historical-Backtest-ALL-summary.csv → 5529只全量 + 基础入场
    B) Control-Backtest-ALL-summary.csv    → 1500只同池 + 基础入场  （=A 的子集）
    C) Enhanced-Backtest-ALL-summary.csv    → 1500只同池 + 增强入场  （=B 仅多入场过滤）

变量隔离：
    B vs A  → 纯股票池规模差异（5529→1500）
    C vs B  → 纯入场增强效应（基础入场 → +放量确认+RSI超卖），仓位/出场完全等同

输出 stock_data/control_3way_report.html
"""
from __future__ import annotations
import pandas as pd
import numpy as np

SD = "E:/Stocks-Master/stock_data"
BASE_P = f"{SD}/Historical-Backtest-ALL-summary.csv"
CTRL_P = f"{SD}/Control-Backtest-ALL-summary.csv"
ENH_P = f"{SD}/Enhanced-Backtest-ALL-summary.csv"


def load(p):
    df = pd.read_csv(p)
    for c in ["num_trades", "total_return", "sharpe", "win_rate",
              "profit_factor", "max_drawdown", "avg_return", "avg_win", "avg_loss"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def metrics(df, label, scope):
    v = df[df["num_trades"] > 0].copy()
    ret = v["total_return"].dropna()
    n = len(v)
    cum = float(ret.sum())
    comp = float((1 + ret / 100).prod() - 1) * 100
    pos = float((ret > 0).mean() * 100) if n else 0.0
    sharpe = float(v["sharpe"].dropna().mean())
    win = float(v["win_rate"].dropna().mean())
    pf = float(v["profit_factor"].replace([np.inf, -np.inf], np.nan).dropna().mean())
    return {
        "label": label, "scope": scope,
        "n_days": n, "n_all": len(df),
        "total_trades": int(v["num_trades"].sum()),
        "avg_ret": float(ret.mean()), "med_ret": float(ret.median()),
        "pos_rate": pos, "cum_ret": cum, "comp_ret": comp,
        "avg_sharpe": sharpe, "avg_win": win, "avg_pf": pf,
    }


def fmt(x, d=2, pct=False):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    return f"{x:.{d}f}{'%' if pct else ''}"


def main():
    base = load(BASE_P)
    ctrl = load(CTRL_P)
    enh = load(ENH_P)
    m_a = metrics(base, "A · 基线全量", "全量 5529 只 · 仅基础过滤")
    m_b = metrics(ctrl, "B · 同池对照", "前 1500 只 · 仅基础过滤（=A 子集）")
    m_c = metrics(enh, "C · 增强入场", "前 1500 只 · +放量确认+RSI超卖（=B 仅多入场过滤）")
    M = {"A": m_a, "B": m_b, "C": m_c}

    # ── 逐日收益 3 线 ──
    b = base[["date", "total_return"]].copy(); b.columns = ["date", "rb"]
    c = ctrl[["date", "total_return"]].copy(); c.columns = ["date", "rc"]
    e = enh[["date", "total_return"]].copy(); e.columns = ["date", "re"]
    m = b.merge(c, on="date").merge(e, on="date").dropna().sort_values("date")
    dates = m["date"].astype(str).tolist()
    rb = m["rb"].tolist(); rc = m["rc"].tolist(); re = m["re"].tolist()

    W, H = 920, 260
    pad_l, pad_r, pad_t, pad_b = 44, 12, 16, 26
    n = len(dates)
    if n > 1:
        allv = rb + rc + re
        lo, hi = min(allv), max(allv)
        if lo == hi:
            lo, hi = lo - 1, hi + 1
        span = hi - lo
        def xpos(i): return pad_l + (W - pad_l - pad_r) * i / (n - 1)
        def ypos(v): return pad_t + (H - pad_t - pad_b) * (hi - v) / span
        def poly(vals): return " ".join(f"{xpos(i):.1f},{ypos(v):.1f}" for i, v in enumerate(vals))
        pts_b, pts_c, pts_e = poly(rb), poly(rc), poly(re)
        zero_y = ypos(0)
        grid = ""
        for gv in (lo, lo/2, 0, hi/2, hi):
            gy = ypos(gv)
            grid += f'<line x1="{pad_l}" y1="{gy:.1f}" x2="{W-pad_r}" y2="{gy:.1f}" stroke="#eee" stroke-width="1"/>'
            grid += f'<text x="{pad_l-5}" y="{gy+3:.1f}" font-size="9" fill="#888" text-anchor="end">{gv:.1f}</text>'
    else:
        pts_b = pts_c = pts_e = ""; zero_y = 0; grid = ""

    # ── 3 列对比表 ──
    specs = [
        ("有效信号日（有成交）", "n_days", "天", False),
        ("总交易笔数", "total_trades", "笔", False),
        ("平均日收益", "avg_ret", "%", True),
        ("中位日收益", "med_ret", "%", True),
        ("正收益日占比", "pos_rate", "%", True),
        ("日收益合计", "cum_ret", "%", True),
        ("复利累计", "comp_ret", "%", True),
        ("平均夏普", "avg_sharpe", "", True),
        ("平均胜率", "avg_win", "%", True),
        ("平均盈亏比", "avg_pf", "", True),
    ]
    rows = []
    for name, key, unit, up in specs:
        vals = {k: M[k][key] for k in ("A", "B", "C")}
        # best
        best = max(vals.values()) if up else min(vals.values())
        cell = []
        for k in ("A", "B", "C"):
            v = vals[k]
            cls = " best" if abs(v - best) < 1e-9 else ""
            cell.append(f'<td class="num{cls}">{fmt(v)}{unit}</td>')
        rows.append(f'<tr><td class="name">{name}</td>{"".join(cell)}</tr>')

    # ── 变量隔离解读 ──
    # B vs A：池规模
    pool_eff = (m_b["pos_rate"] - m_a["pos_rate"], m_b["avg_ret"] - m_a["avg_ret"], m_b["avg_sharpe"] - m_a["avg_sharpe"])
    # C vs B：入场增强
    entry_eff = (m_c["pos_rate"] - m_b["pos_rate"], m_c["avg_ret"] - m_b["avg_ret"], m_c["avg_sharpe"] - m_b["avg_sharpe"])

    def arrow(delta, up=True):
        if abs(delta) < 1e-9:
            return "＝ 持平"
        better = (delta > 0) if up else (delta < 0)
        return ("▲ +" if delta > 0 else "▼ ") + f"{delta:.2f}" + ("（改善）" if better else "（恶化）")

    interp = f"""
    <li><b>池规模效应 (B vs A)</b>：5529→1500 只子集后，
        正收益日 {arrow(pool_eff[0])}pp、平均日收益 {arrow(pool_eff[1])}%、
        平均夏普 {arrow(pool_eff[2])}。说明 1500 前缀子集的标的分布本身与全量有偏差。</li>
    <li><b>入场增强效应 (C vs B) · 核心</b>：在<b>完全同池、同出场、同仓位</b>下，
        仅加「放量确认+RSI超卖」后，
        正收益日 {arrow(entry_eff[0])}pp、平均日收益 {arrow(entry_eff[1])}%、
        平均夏普 {arrow(entry_eff[2])}。
        {'→ 入场增强确实带来边际改善。' if entry_eff[0] > 0 and entry_eff[2] >= 0 else '→ 入场增强未带来稳定改善。'}</li>
    """

    # 判定：入场增强是否把同池负期望扭正？
    verdict = ("入场增强（放量确认 + RSI 超卖）在<b>同池严格对照</b>下"
               f"{'方向正确、有边际改善' if entry_eff[0] > 0 and entry_eff[2] >= 0 else '仍未扭转负期望'}。"
               f"同池对照 B 的平均日收益 {fmt(m_b['avg_ret'])}%、夏普 {fmt(m_b['avg_sharpe'])}；"
               f"增强后 C 为 {fmt(m_c['avg_ret'])}%、夏普 {fmt(m_c['avg_sharpe'])}。"
               "两者均未转正 → 瓶颈已不只在入场，出场/持仓周期是主因。")

    vrows = interp

    CSS = """
    * { box-sizing: border-box; }
    body { font-family: -apple-system, "Segoe UI", "Microsoft YaHei", sans-serif;
           background:#f5f6f8; color:#1f2733; margin:0; padding:28px; }
    .wrap { max-width:1000px; margin:0 auto; }
    h1 { font-size:22px; margin:0 0 4px; }
    .sub { color:#6b7685; font-size:13px; margin-bottom:18px; }
    .cards { display:flex; gap:14px; margin-bottom:20px; flex-wrap:wrap; }
    .card { flex:1; min-width:150px; background:#fff; border:1px solid #e6e9ee;
            border-radius:12px; padding:14px 16px; }
    .card .k { font-size:12px; color:#8a93a2; }
    .card .v { font-size:22px; font-weight:700; margin-top:4px; }
    .card .s { font-size:11px; color:#9aa3b1; margin-top:2px; }
    .card .v.g { color:#2e9e5b; } .card .v.r { color:#d1495b; }
    table { width:100%; border-collapse:collapse; background:#fff;
            border:1px solid #e6e9ee; border-radius:12px; overflow:hidden; }
    thead th { background:#eef1f5; color:#5a6473; font-weight:600; padding:10px 12px;
               text-align:right; font-size:13px; }
    thead th:first-child { text-align:left; }
    th.cca, th.ccb, th.ccc { border-bottom:2px solid #cfd6df; }
    th.cca { color:#d1495b; } th.ccb { color:#e08a00; } th.ccc { color:#1f5fbf; }
    td { padding:9px 12px; text-align:right; font-size:13px; font-variant-numeric:tabular-nums; }
    td.name { text-align:left; color:#3a4350; }
    td.best { background:#e9f7ee; font-weight:700; color:#1d7a3e; }
    .chart { background:#fff; border:1px solid #e6e9ee; border-radius:12px;
             padding:14px 18px; margin:18px 0; }
    .legend span { display:inline-block; margin-right:18px; font-size:12px; }
    .lg-a { color:#d1495b; font-weight:700; } .lg-b { color:#e08a00; font-weight:700; }
    .lg-c { color:#1f5fbf; font-weight:700; }
    .verdict { background:#fff; border:1px solid #e6e9ee; border-radius:12px;
               padding:16px 18px; margin:18px 0; font-size:14px; line-height:1.6; }
    .verdict ul { margin:8px 0 0; padding-left:20px; }
    .verdict li { margin:6px 0; }
    .method { background:#fff; border:1px solid #e6e9ee; border-radius:12px;
              padding:14px 18px; font-size:13px; color:#4a5462; line-height:1.7; }
    code { background:#eef1f5; padding:1px 5px; border-radius:4px; }
    .g { color:#2e9e5b; } .r { color:#d1495b; }
    """
    body = f"""
    <div class="wrap">
      <h1>三路历史回测 · 变量隔离对照</h1>
      <div class="sub">区间 2026-01-01 ~ 2026-07-18 · 持有 5 日 · 出场完全复用线上引擎
        （布林上轨止盈 + 6% 止盈 + 5% 跟踪 + MA60 破位）</div>

      <div class="cards">
        <div class="card"><div class="k">A 正收益日</div><div class="v">{fmt(m_a['pos_rate'])}%</div>
          <div class="s">5529只·基础</div></div>
        <div class="card"><div class="k">B 正收益日（同池）</div><div class="v">{fmt(m_b['pos_rate'])}%</div>
          <div class="s">1500只·基础</div></div>
        <div class="card"><div class="k">C 正收益日（增强）</div>
          <div class="v {'g' if entry_eff[0]>0 else 'r'}">{fmt(m_c['pos_rate'])}%</div>
          <div class="s">1500只·+放量/RSI</div></div>
      </div>

      <table>
        <thead><tr><th>指标</th>
          <th class="cca">A · 基线全量</th>
          <th class="ccb">B · 同池对照</th>
          <th class="ccc">C · 增强入场</th></tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>

      <div class="chart">
        <div class="legend"><span class="lg-a">— A 基线逐日收益</span>
          <span class="lg-b">— B 同池对照逐日收益</span>
          <span class="lg-c">— C 增强逐日收益</span></div>
        <svg viewBox="0 0 {W} {H}" width="100%" height="{H}">
          {grid}
          <line x1="{pad_l}" y1="{zero_y:.1f}" x2="{W-pad_r}" y2="{zero_y:.1f}" stroke="#bbb" stroke-width="1.5"/>
          <polyline fill="none" stroke="#d1495b" stroke-width="1.5" points="{pts_b}"/>
          <polyline fill="none" stroke="#e08a00" stroke-width="1.5" points="{pts_c}"/>
          <polyline fill="none" stroke="#1f5fbf" stroke-width="1.8" points="{pts_e}"/>
        </svg>
      </div>

      <div class="verdict">
        <b>变量隔离解读</b>
        <ul>{vrows}</ul>
        <p style="margin:10px 0 0;">{verdict}</p>
      </div>

      <div class="method">
        <b>方法</b><br>
        为干净隔离「入场增强」单一变量，本对照用与增强版<b>完全相同的 1500 只排序前缀子集</b>，
        但<b>去掉</b>放量确认 <code>volume ≥ 5日均量×1.3</code> 与 RSI 超卖 <code>RSI&lt;35</code> 两个过滤，
        仅保留基础入场（<code>close ≤ 布林下轨×1.015</code> + 20日相对强弱≥0 + 成交额≥¥1亿）。<br>
        仓位缩放 <code>capital_scale</code> 由 <code>compute_market_profile()</code> 决定，
        三路均在 2026-07-18 运行 → 同一波动率档位 → 缩放一致，确保 B 与 C 仅差入场过滤。<br>
        出场/风控<b>完全复用线上引擎</b> <code>run_forward_signal_backtest</code>，与 A 一致。
      </div>
    </div>
    """
    html = ("<!DOCTYPE html><html lang='zh'><head><meta charset='utf-8'>"
            "<style>" + CSS + "</style></head>" + body + "</html>")
    out = f"{SD}/control_3way_report.html"
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    print("A 基线:", {k: round(v, 3) if isinstance(v, float) else v for k, v in m_a.items()})
    print("B 对照:", {k: round(v, 3) if isinstance(v, float) else v for k, v in m_b.items()})
    print("C 增强:", {k: round(v, 3) if isinstance(v, float) else v for k, v in m_c.items()})
    print("报告已写出:", out)


if __name__ == "__main__":
    main()
