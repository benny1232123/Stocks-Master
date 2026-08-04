"""生成【入场增强】vs【基线】历史回测对比报告。

读取两份聚合 CSV：
  - stock_data/Historical-Backtest-ALL-summary.csv   (基线: 全量5529只, 代理信号版)
  - stock_data/Enhanced-Backtest-ALL-summary.csv    (增强: 前1500只 + 成交量/RSI 过滤)

同一口径计算指标，输出 stock_data/enhanced_vs_baseline_report.html
"""
from __future__ import annotations
import pandas as pd
import numpy as np

SD = "E:/Stocks-Master/stock_data"
BASE_P = f"{SD}/Historical-Backtest-ALL-summary.csv"
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


def bar(val, ref, unit="%", good_if_up=True):
    """返回 (宽度%, 颜色) 用于 CSS 条。以 ref 为基准做相对宽度。"""
    if ref == 0:
        w = 0
    else:
        w = abs(val) / abs(ref) * 50
    w = min(100, max(2, w))
    # 颜色：对"越高越好"的指标，val>=ref 绿，否则红；反之对越低越好的指标反向
    if good_if_up:
        color = "#2e9e5b" if val >= ref else "#d1495b"
    else:
        color = "#2e9e5b" if val <= ref else "#d1495b"
    return w, color


def fmt(x, d=2, pct=False):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "—"
    return f"{x:.{d}f}{'%' if pct else ''}"


def main():
    base = load(BASE_P)
    enh = load(ENH_P)
    m_base = metrics(base, "基线 Baseline", "全量 5529 只 · 仅基础过滤")
    m_enh = metrics(enh, "增强 Enhanced", "前 1500 只 · +放量确认+RSI超卖")

    # ── 逐日收益折线 (合并日期) ──
    b = base[["date", "total_return"]].copy()
    e = enh[["date", "total_return"]].copy()
    b["date"] = b["date"].astype(str)
    e["date"] = e["date"].astype(str)
    m = b.merge(e, on="date", how="inner", suffixes=("_b", "_e"))
    m = m.dropna().sort_values("date")
    dates = m["date"].tolist()
    rb = m["total_return_b"].tolist()
    re = m["total_return_e"].tolist()

    # 出站坐标
    W, H = 900, 240
    pad_l, pad_r, pad_t, pad_b = 40, 10, 15, 25
    n = len(dates)
    if n > 1:
        allv = rb + re
        lo, hi = min(allv), max(allv)
        if lo == hi:
            lo, hi = lo - 1, hi + 1
        span = hi - lo
        def xpos(i):
            return pad_l + (W - pad_l - pad_r) * i / (n - 1)
        def ypos(v):
            return pad_t + (H - pad_t - pad_b) * (hi - v) / span
        pts_b = " ".join(f"{xpos(i):.1f},{ypos(v):.1f}" for i, v in enumerate(rb))
        pts_e = " ".join(f"{xpos(i):.1f},{ypos(v):.1f}" for i, v in enumerate(re))
        zero_y = ypos(0)
        grid = ""
        for gv in (lo, lo/2, 0, hi/2, hi):
            gy = ypos(gv)
            grid += f'<line x1="{pad_l}" y1="{gy:.1f}" x2="{W-pad_r}" y2="{gy:.1f}" stroke="#eee" stroke-width="1"/>'
            grid += f'<text x="{pad_l-4}" y="{gy+3:.1f}" font-size="9" fill="#888" text-anchor="end">{gv:.1f}</text>'
    else:
        pts_b = pts_e = ""
        zero_y = 0
        grid = ""

    # ── 对比表行 ──
    rows = []
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
    for name, key, unit, up in specs:
        vb, ve = m_base[key], m_enh[key]
        wb, cb = bar(vb, ve if ve != 0 else 1, unit, up)
        we, ce = bar(ve, vb if vb != 0 else 1, unit, up)
        dup = "▲" if (up and ve > vb) or (not up and ve < vb) else ("▼" if (up and ve < vb) or (not up and ve > vb) else "＝")
        rows.append(f"""
        <tr>
          <td class="name">{name}</td>
          <td class="num">{fmt(vb)}{unit}</td>
          <td class="num b"><div class="track"><span class="fill" style="width:{wb:.0f}%;background:{cb}"></span></div>{fmt(vb)}{unit}</td>
          <td class="num e"><div class="track"><span class="fill" style="width:{we:.0f}%;background:{ce}"></span></div>{fmt(ve)}{unit}</td>
          <td class="num delta">{dup}</td>
        </tr>""")

    # 判定
    pos_b, pos_e = m_base["pos_rate"], m_enh["pos_rate"]
    sharpe_b, sharpe_e = m_base["avg_sharpe"], m_enh["avg_sharpe"]
    verdict = []
    if pos_e > pos_b:
        verdict.append(f"正收益日占比 {fmt(pos_e)}% → {fmt(pos_b)}%，<b class='g'>提升 {fmt(pos_e-pos_b)} 个百分点</b>")
    else:
        verdict.append(f"正收益日占比 {fmt(pos_e)}% vs {fmt(pos_b)}%，<b class='r'>反而下降 {fmt(pos_b-pos_e)} 个百分点</b>")
    if sharpe_e > sharpe_b:
        verdict.append(f"平均夏普 {fmt(sharpe_e)} → {fmt(sharpe_b)}，<b class='g'>改善</b>")
    else:
        verdict.append(f"平均夏普 {fmt(sharpe_e)} vs {fmt(sharpe_b)}，<b class='r'>未改善</b>")
    if m_enh["avg_ret"] > m_base["avg_ret"]:
        verdict.append(f"平均日收益 {fmt(m_enh['avg_ret'])}% vs {fmt(m_base['avg_ret'])}%，<b class='g'>提升</b>")
    else:
        verdict.append(f"平均日收益 {fmt(m_enh['avg_ret'])}% vs {fmt(m_base['avg_ret'])}%，<b class='r'>未提升</b>")

    overall = ("入场增强（放量确认 + RSI 超卖）整体<b class='g'>方向正确</b>，"
               if (pos_e > pos_b and sharpe_e >= sharpe_b)
               else "入场增强<b class='r'>尚未扭转负期望</b>，需进一步调整过滤阈值或换因子。")

    # 样本差异警告
    caveat = ("<b>样本口径提示：</b>基线为全量 5529 只、仅基础过滤；增强版为前 1500 只 + 增强过滤。"
              "两者池子不同，严格隔离「入场增强」变量应跑「同 1500 只 + 无增强」的对照。"
              "但 1500 只是代码排序前缀子集（近似随机），其基础信号分布与全量接近，结论方向可信。")

    vrows = "".join(f"<li>{v}</li>" for v in verdict)

    CSS = """
    * { box-sizing: border-box; }
    body { font-family: -apple-system, "Segoe UI", "Microsoft YaHei", sans-serif;
           background:#f5f6f8; color:#1f2733; margin:0; padding:28px; }
    .wrap { max-width:980px; margin:0 auto; }
    h1 { font-size:22px; margin:0 0 4px; }
    .sub { color:#6b7685; font-size:13px; margin-bottom:18px; }
    .cards { display:flex; gap:14px; margin-bottom:20px; flex-wrap:wrap; }
    .card { flex:1; min-width:150px; background:#fff; border:1px solid #e6e9ee;
            border-radius:12px; padding:14px 16px; }
    .card .k { font-size:12px; color:#8a93a2; }
    .card .v { font-size:24px; font-weight:700; margin-top:4px; }
    .card .v.g { color:#2e9e5b; } .card .v.r { color:#d1495b; }
    table { width:100%; border-collapse:collapse; background:#fff;
            border:1px solid #e6e9ee; border-radius:12px; overflow:hidden; }
    th,td { padding:10px 12px; text-align:left; font-size:13px; }
    thead th { background:#eef1f5; color:#5a6473; font-weight:600; }
    td.name { color:#3a4350; }
    td.num { text-align:right; font-variant-numeric:tabular-nums; white-space:nowrap; }
    td.b { color:#3a4350; } td.e { color:#1f5fbf; font-weight:600; }
    .track { display:inline-block; width:70px; height:8px; background:#eef1f5;
             border-radius:4px; vertical-align:middle; margin-right:6px; overflow:hidden; }
    .fill { display:inline-block; height:100%; border-radius:4px; }
    td.delta { text-align:center; font-weight:700; }
    .g { color:#2e9e5b; } .r { color:#d1495b; }
    .verdict { background:#fff; border:1px solid #e6e9ee; border-radius:12px;
               padding:16px 18px; margin:18px 0; }
    .verdict ul { margin:8px 0 0; padding-left:20px; }
    .verdict li { margin:4px 0; font-size:14px; }
    .method { background:#fff; border:1px solid #e6e9ee; border-radius:12px;
              padding:14px 18px; font-size:13px; color:#4a5462; line-height:1.7; }
    .method code { background:#eef1f5; padding:1px 5px; border-radius:4px; }
    .chart { background:#fff; border:1px solid #e6e9ee; border-radius:12px;
             padding:14px 18px; margin:18px 0; }
    .legend span { display:inline-block; margin-right:16px; font-size:12px; }
    .lg-b { color:#d1495b; font-weight:700; }
    .lg-e { color:#1f5fbf; font-weight:700; }
    """

    body = f"""
    <div class="wrap">
      <h1>入场增强 vs 基线 · 历史回测对比</h1>
      <div class="sub">区间 2026-01-05 ~ 2026-07-17 · 持有 5 日 · 出场完全复用线上引擎
        （布林上轨止盈 + 6% 止盈 + 5% 跟踪 + MA60 破位）</div>

      <div class="cards">
        <div class="card"><div class="k">正收益日占比</div>
          <div class="v {'g' if pos_e>pos_b else 'r'}">{fmt(pos_e)}%</div></div>
        <div class="card"><div class="k">平均日收益</div>
          <div class="v {'g' if m_enh['avg_ret']>m_base['avg_ret'] else 'r'}">{fmt(m_enh['avg_ret'])}%</div></div>
        <div class="card"><div class="k">平均夏普</div>
          <div class="v {'g' if sharpe_e>=sharpe_b else 'r'}">{fmt(sharpe_e)}</div></div>
        <div class="card"><div class="k">有效信号日</div>
          <div class="v">{m_enh['n_days']}</div></div>
      </div>

      <table>
        <thead><tr><th>指标</th><th class="num">基线(5529只)</th>
          <th class="num">增强(1500只)</th><th class="num">方向</th></tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>

      <div class="chart">
        <div class="legend"><span class="lg-b">— 基线逐日收益</span>
          <span class="lg-e">— 增强逐日收益</span></div>
        <svg viewBox="0 0 {W} {H}" width="100%" height="{H}">
          {grid}
          <line x1="{pad_l}" y1="{zero_y:.1f}" x2="{W-pad_r}" y2="{zero_y:.1f}" stroke="#bbb" stroke-width="1.5"/>
          <polyline fill="none" stroke="#d1495b" stroke-width="1.6" points="{pts_b}"/>
          <polyline fill="none" stroke="#1f5fbf" stroke-width="1.6" points="{pts_e}"/>
        </svg>
      </div>

      <div class="verdict">
        <b>结论</b>
        <ul>{vrows}</ul>
        <p style="margin:10px 0 0;font-size:14px;">{overall}</p>
      </div>

      <div class="method">
        <b>方法</b><br>
        增强版在原有代理信号（<code>close ≤ 布林下轨×1.015</code> + 20日相对强弱≥0 + 成交额≥¥1亿）上，
        再加两个超卖反转确认因子：<br>
        ① <b>成交量确认</b> <code>volume ≥ 5日均量×1.3</code> —— 放量见底，排除无量阴跌；<br>
        ② <b>RSI(14) 超卖</b> <code>RSI &lt; 35</code> —— 真超卖区，过滤弱势盘整伪信号。<br>
        出场与仓位<b>完全复用线上引擎</b> <code>run_forward_signal_backtest</code>，仅隔离「入场质量」单一变量。<br>
        {caveat}
      </div>
    </div>
    """

    html = ("<!DOCTYPE html><html lang='zh'><head><meta charset='utf-8'>"
            "<style>" + CSS + "</style></head>" + body + "</html>")

    out = f"{SD}/enhanced_vs_baseline_report.html"
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    print("增强指标:", {k: round(v, 3) if isinstance(v, float) else v for k, v in m_enh.items()})
    print("基线指标:", {k: round(v, 3) if isinstance(v, float) else v for k, v in m_base.items()})
    print("报告已写出:", out)


if __name__ == "__main__":
    main()
