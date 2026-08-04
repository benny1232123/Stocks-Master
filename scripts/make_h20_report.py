"""生成【五路】历史回测对照报告：验证"出场/持仓周期是不是真瓶颈"。

    A) Historical-Backtest-ALL-summary.csv  → 5529只全量 · 基础入场 · 持有5日
    B) Control-Backtest-ALL-summary.csv      → 1500只同池 · 基础入场 · 持有5日   (=A子集, =③ baseline)
    C) Enhanced-Backtest-ALL-summary.csv     → 1500只同池 · 增强入场 · 持有5日   (放量+RSI)
    C20) EnhancedH20-Backtest-ALL-summary.csv→ 1500只同池 · 增强入场 · 持有20日  ← 本次(uNoes0)
    D) Param-Scan-results.csv (hold20 行)    → 1500只同池 · 基础入场 · 持有20日  (③, 不同格式聚合行)

核心隔离：
    C20 vs C  → 同入场(增强)、同池，唯一变量 持有20日 vs 5日  → 出场/持仓周期效应
    C20 vs D  → 同持有20日、同池，唯一变量 增强 vs 基础入场     → 入场增强在长持有下边际
    C20 vs B  → 跨越(入场+持有) 两变量，看组合是否救活

输出 stock_data/h20_5way_report.html
"""
from __future__ import annotations
import pandas as pd
import numpy as np

SD = "E:/Stocks-Master/stock_data"
A_P = f"{SD}/Historical-Backtest-ALL-summary.csv"
B_P = f"{SD}/Control-Backtest-ALL-summary.csv"
C_P = f"{SD}/Enhanced-Backtest-ALL-summary.csv"
C20_P = f"{SD}/EnhancedH20-Backtest-ALL-summary.csv"
PS_P = f"{SD}/Param-Scan-results.csv"


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
    if x is None or (isinstance(x, float) and (np.isnan(x) or x == float("inf"))):
        return "—"
    return f"{x:.{d}f}{'%' if pct else ''}"


def main():
    a = metrics(load(A_P), "A · 基线全量", "5529只 · 基础入场 · 持有5日")
    b = metrics(load(B_P), "B · 同池对照", "1500只 · 基础入场 · 持有5日 (＝③baseline)")
    c = metrics(load(C_P), "C · 增强·持5", "1500只 · 增强入场 · 持有5日 (放量+RSI)")
    c20 = metrics(load(C20_P), "C20 · 增强·持20", "1500只 · 增强入场 · 持有20日 ←本次")

    # ── D(③ hold20) 来自 Param-Scan 不同格式聚合行 ──
    ps = pd.read_csv(PS_P)
    row = ps[ps["param_set"] == "hold20"].iloc[0]
    d = {
        "label": "D · 基线·持20", "scope": "1500只 · 基础入场 · 持有20日 (③)",
        "n_days": int(row["days"]), "n_all": int(row["days"]),
        "total_trades": int(row["total_trades"]),
        "avg_ret": float(row["avg_return"]), "med_ret": float(row["median_return"]),
        "pos_rate": float(row["win_days_pct"]),
        "cum_ret": float(row["avg_return"]) * int(row["days"]),  # 估算(合计=均值×天数)
        "comp_ret": None,            # Param-Scan 无逐日 → 复利累计缺失
        "avg_sharpe": float(row["sharpe"]),
        "avg_win": float(row["win_rate"]),
        "avg_pf": None,              # Param-Scan 无盈亏比列
    }

    M = {"A": a, "B": b, "C": c, "C20": c20, "D": d}
    ORDER = ["A", "B", "C", "C20", "D"]
    COLOR = {"A": "#d1495b", "B": "#e08a00", "C": "#1f5fbf",
             "C20": "#2e9e5b", "D": "#7b4ea8"}
    NAME = {"A": "A · 基线全量", "B": "B · 同池对照", "C": "C · 增强·持5",
            "C20": "C20 · 增强·持20", "D": "D · 基线·持20"}

    # ── 逐日收益 4 线 (A/B/C/C20；D 无逐日) ──
    da = load(A_P)[["date", "total_return"]].copy(); da.columns = ["date", "ra"]
    db = load(B_P)[["date", "total_return"]].copy(); db.columns = ["date", "rb"]
    dc = load(C_P)[["date", "total_return"]].copy(); dc.columns = ["date", "rc"]
    d20 = load(C20_P)[["date", "total_return"]].copy(); d20.columns = ["date", "r20"]
    m = (da.merge(db, on="date").merge(dc, on="date").merge(d20, on="date")
          .dropna().sort_values("date"))
    dates = m["date"].astype(str).tolist()
    ra, rb, rc, r20 = m["ra"].tolist(), m["rb"].tolist(), m["rc"].tolist(), m["r20"].tolist()

    W, H = 960, 300
    pad_l, pad_r, pad_t, pad_b = 46, 12, 16, 30
    n = len(dates)
    if n > 1:
        allv = ra + rb + rc + r20
        lo, hi = min(allv), max(allv)
        if lo == hi:
            lo, hi = lo - 1, hi + 1
        span = hi - lo
        def xpos(i): return pad_l + (W - pad_l - pad_r) * i / (n - 1)
        def ypos(v): return pad_t + (H - pad_t - pad_b) * (hi - v) / span
        def poly(vals): return " ".join(f"{xpos(i):.1f},{ypos(v):.1f}" for i, v in enumerate(vals))
        p_a, p_b, p_c, p_20 = poly(ra), poly(rb), poly(rc), poly(r20)
        zero_y = ypos(0)
        grid = ""
        for gv in (lo, lo/2, 0, hi/2, hi):
            gy = ypos(gv)
            grid += f'<line x1="{pad_l}" y1="{gy:.1f}" x2="{W-pad_r}" y2="{gy:.1f}" stroke="#eee" stroke-width="1"/>'
            grid += f'<text x="{pad_l-5}" y="{gy+3:.1f}" font-size="9" fill="#888" text-anchor="end">{gv:.1f}</text>'
    else:
        p_a = p_b = p_c = p_20 = ""; zero_y = 0; grid = ""

    # ── 对比表 ──
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
        vals = {k: M[k][key] for k in ORDER}
        present = [v for v in vals.values() if v is not None and not (isinstance(v, float) and np.isnan(v))]
        best = max(present) if (up and present) else (min(present) if present else None)
        cell = []
        for k in ORDER:
            v = vals[k]
            cls = ""
            if best is not None and v is not None and not (isinstance(v, float) and np.isnan(v)):
                if abs(v - best) < 1e-9:
                    cls = " best"
            cell.append(f'<td class="num{cls}" style="color:{COLOR[k]}">{fmt(v)}{unit}</td>')
        rows.append(f'<tr><td class="name">{name}</td>{"".join(cell)}</tr>')

    # ── 效应解读 ──
    def arrow(delta, up=True):
        if delta is None or abs(delta) < 1e-9:
            return "＝ 持平"
        return ("▲ +" if delta > 0 else "▼ ") + f"{delta:.2f}" + ("（改善）" if (delta > 0) == up else "（恶化）")

    # C20 vs C：持有周期效应（同增强入场）
    hold_eff = (c20["pos_rate"] - c["pos_rate"], c20["avg_ret"] - c["avg_ret"], c20["avg_sharpe"] - c["avg_sharpe"])
    # C20 vs D：入场增强效应（同持有20日）
    entry_eff = (c20["pos_rate"] - d["pos_rate"], c20["avg_ret"] - d["avg_ret"], c20["avg_sharpe"] - d["avg_sharpe"])
    # C20 vs B：组合效应（两变量都变）
    combo_eff = (c20["pos_rate"] - b["pos_rate"], c20["avg_ret"] - b["avg_ret"], c20["avg_sharpe"] - b["avg_sharpe"])

    # 判定
    hold_helps = hold_eff[0] > 0 and hold_eff[2] > 0
    entry_helps = entry_eff[0] > 0 and entry_eff[2] > 0
    combo_pos = c20["avg_sharpe"] > 0 or c20["avg_ret"] > 0
    if combo_pos:
        verdict = ("<b>组合救活成功。</b>增强入场 × 持有20日 让平均日收益转正 / 夏普转正，"
                   "证明<b>出场/持仓周期确为原瓶颈</b>，且可通过「更好入场 + 更长持有」破解。")
    elif hold_helps and entry_helps:
        verdict = ("<b>方向双正确但未救活。</b>延长持有(C20 vs C)与入场增强(C20 vs D)各自都带来改善，"
                   "组合后仍为负期望——说明这套「布林下轨抄底 + 均值回归持有」框架在 2026H1 下行/震荡市里"
                   "结构性偏弱，单靠入场+持有调参已接近天花板。")
    elif hold_helps and not entry_helps:
        verdict = ("<b>延长持有有帮助、入场增强在长持有下失效。</b>出场/持仓周期确为瓶颈，"
                   "但增强入场的长持边际被稀释。")
    elif not hold_helps and entry_helps:
        verdict = ("<b>入场增强有效、延长持有反而更差。</b>说明原 hold5 止盈节奏更适配该信号，"
                   "瓶颈不在单纯持有长度，而在止盈/止损结构。")
    else:
        verdict = ("<b>两者都未能改善。</b>入场与出场调参均失效，策略框架需换信号源或加牛熊择时。")

    interp = f"""
    <li><b>持有周期效应 (C20 vs C) · 同增强入场</b>：持有 5日→20日，
        正收益日 {arrow(hold_eff[0])}pp、平均日收益 {arrow(hold_eff[1])}%、平均夏普 {arrow(hold_eff[2])}。
        {'→ 延长持有确实改善，出场/持仓是瓶颈。' if hold_helps else '→ 延长持有未改善。'}</li>
    <li><b>入场增强效应 (C20 vs D) · 同持有20日</b>：基础→增强入场，
        正收益日 {arrow(entry_eff[0])}pp、平均日收益 {arrow(entry_eff[1])}%、平均夏普 {arrow(entry_eff[2])}。
        {'→ 入场增强在长持有下仍有正边际。' if entry_helps else '→ 入场增强在长持有下边际消失/转负。'}</li>
    <li><b>组合效应 (C20 vs B) · 跨两变量</b>：基础持5→增强持20，
        正收益日 {arrow(combo_eff[0])}pp、平均日收益 {arrow(combo_eff[1])}%、平均夏普 {arrow(combo_eff[2])}。
        这是「增强入场＋更长持有」相对线上现状(B)的总提升。</li>
    """

    CSS = """
    * { box-sizing: border-box; }
    body { font-family: -apple-system, "Segoe UI", "Microsoft YaHei", sans-serif;
           background:#f5f6f8; color:#1f2733; margin:0; padding:28px; }
    .wrap { max-width:1040px; margin:0 auto; }
    h1 { font-size:22px; margin:0 0 4px; }
    .sub { color:#6b7685; font-size:13px; margin-bottom:18px; }
    .cards { display:flex; gap:12px; margin-bottom:20px; flex-wrap:wrap; }
    .card { flex:1; min-width:160px; background:#fff; border:1px solid #e6e9ee;
            border-radius:12px; padding:13px 15px; border-top:3px solid #ccc; }
    .card .k { font-size:11px; color:#8a93a2; }
    .card .v { font-size:21px; font-weight:700; margin-top:3px; }
    .card .s { font-size:11px; color:#9aa3b1; margin-top:2px; }
    table { width:100%; border-collapse:collapse; background:#fff;
            border:1px solid #e6e9ee; border-radius:12px; overflow:hidden; }
    thead th { background:#eef1f5; color:#5a6473; font-weight:600; padding:10px 10px;
               text-align:right; font-size:12px; }
    thead th:first-child { text-align:left; }
    td { padding:8px 10px; text-align:right; font-size:12.5px; font-variant-numeric:tabular-nums; }
    td.name { text-align:left; color:#3a4350; font-weight:500; }
    td.best { background:#e9f7ee; font-weight:700; }
    .chart { background:#fff; border:1px solid #e6e9ee; border-radius:12px;
             padding:14px 18px; margin:18px 0; }
    .legend span { display:inline-block; margin-right:16px; font-size:12px; }
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
      <h1>五路历史回测 · 出场是不是真瓶颈？</h1>
      <div class="sub">区间 2026-01-01 ~ 2026-07-18 · 出场复用线上引擎
        （布林上轨止盈 + 6% 止盈 + 5% 跟踪 + MA60 破位）· 仓位缩放同档
        · D 行来自 ③ Param-Scan 聚合（无逐日，合计为估算）</div>

      <div class="cards">
        <div class="card" style="border-top-color:{COLOR['C20']}">
          <div class="k">C20 增强·持20（本次）</div>
          <div class="v" style="color:{COLOR['C20']}">{fmt(c20['avg_ret'])}%</div>
          <div class="s">正收益日 {fmt(c20['pos_rate'])}% · 夏普 {fmt(c20['avg_sharpe'])}</div></div>
        <div class="card" style="border-top-color:{COLOR['C']}">
          <div class="k">C 增强·持5</div>
          <div class="v" style="color:{COLOR['C']}">{fmt(c['avg_ret'])}%</div>
          <div class="s">正收益日 {fmt(c['pos_rate'])}% · 夏普 {fmt(c['avg_sharpe'])}</div></div>
        <div class="card" style="border-top-color:{COLOR['D']}">
          <div class="k">D 基线·持20（③）</div>
          <div class="v" style="color:{COLOR['D']}">{fmt(d['avg_ret'])}%</div>
          <div class="s">正收益日 {fmt(d['pos_rate'])}% · 夏普 {fmt(d['avg_sharpe'])}</div></div>
        <div class="card" style="border-top-color:{COLOR['B']}">
          <div class="k">B 同池·持5（线上现状）</div>
          <div class="v" style="color:{COLOR['B']}">{fmt(b['avg_ret'])}%</div>
          <div class="s">正收益日 {fmt(b['pos_rate'])}% · 夏普 {fmt(b['avg_sharpe'])}</div></div>
      </div>

      <table>
        <thead><tr><th>指标</th>
          <th style="color:{COLOR['A']}">A · 基线全量</th>
          <th style="color:{COLOR['B']}">B · 同池对照</th>
          <th style="color:{COLOR['C']}">C · 增强·持5</th>
          <th style="color:{COLOR['C20']}">C20 · 增强·持20</th>
          <th style="color:{COLOR['D']}">D · 基线·持20</th></tr></thead>
        <tbody>{''.join(rows)}</tbody>
      </table>

      <div class="chart">
        <div class="legend">
          <span style="color:{COLOR['A']}">— A 基线逐日收益</span>
          <span style="color:{COLOR['B']}">— B 同池对照</span>
          <span style="color:{COLOR['C']}">— C 增强·持5</span>
          <span style="color:{COLOR['C20']}">— C20 增强·持20</span></div>
        <svg viewBox="0 0 {W} {H}" width="100%" height="{H}">
          {grid}
          <line x1="{pad_l}" y1="{zero_y:.1f}" x2="{W-pad_r}" y2="{zero_y:.1f}" stroke="#bbb" stroke-width="1.5"/>
          <polyline fill="none" stroke="{COLOR['A']}" stroke-width="1.2" points="{p_a}"/>
          <polyline fill="none" stroke="{COLOR['B']}" stroke-width="1.2" points="{p_b}"/>
          <polyline fill="none" stroke="{COLOR['C']}" stroke-width="1.4" points="{p_c}"/>
          <polyline fill="none" stroke="{COLOR['C20']}" stroke-width="2.2" points="{p_20}"/>
        </svg>
      </div>

      <div class="verdict">
        <b>变量隔离解读</b>
        <ul>{interp}</ul>
        <p style="margin:10px 0 0;"><b>判定：</b>{verdict}</p>
      </div>

      <div class="method">
        <b>方法</b><br>
        五路均用 <b>2026-07-18</b> 同档波动率下定价的 <code>capital_scale</code>，保证仓位缩放一致。
        A/B/C/C20 的逐日收益、夏普、胜率、盈亏比均按同一口径从各自
        <code>*-ALL-summary.csv</code> 的 <code>total_return</code> 列计算。
        D(③ hold20) 直接取自早前 <code>Param-Scan</code> 实验的聚合行
        （与 B 同池同基础入场、仅持有 20 日），其「复利累计 / 平均盈亏比」两项 Param-Scan 未留存，表中以「—」标示，
        日收益合计按 均值×天数 估算。
      </div>
    </div>
    """
    html = ("<!DOCTYPE html><html lang='zh'><head><meta charset='utf-8'>"
            "<style>" + CSS + "</style></head>" + body + "</html>")
    out = f"{SD}/h20_5way_report.html"
    with open(out, "w", encoding="utf-8") as f:
        f.write(html)
    for k in ORDER:
        print(k, {kk: (round(vv, 3) if isinstance(vv, float) else vv)
                  for kk, vv in M[k].items() if kk in ("avg_ret", "pos_rate", "avg_sharpe", "comp_ret", "avg_pf")})
    print("报告已写出:", out)


if __name__ == "__main__":
    main()
