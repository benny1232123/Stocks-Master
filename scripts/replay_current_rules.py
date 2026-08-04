"""按「当前规则」重筛历史已发布清单，并对比纸盘表现。

背景
----
历史 Daily-Action-List 是按不同版本代码逐步生成的：早期（0610~0714）用的是
旧版融合逻辑，且很多日期的原始候选文件已丢失；0720 之后才相对完整。本脚本
不重跑整套策略引擎（那会引入实时指数/regime 的未来函数），而是对「已经真实
发生过的入选清单」再施加**当前版本新增的风险中性化规则**：

  1. 行业权重上限  apply_sector_weight_cap  (任一行业入选总仓位 ≤ 全组合×20%)
  2. 组合 β 软约束 _apply_beta_cap          (组合 β>1.4 逐步剔除最高 β 个股)

两套约束都严格因果：
  - 行业映射来自本地 sector_map.json（无日期）。
  - β 估计用 _estimate_betas(as_of=信号日)，内部已把个股与沪深300 序列截断到
    信号日（loc[:target]），不窥探未来。

随后用与 paper_tracker 一致的「持有至下一信号日」逻辑，分别跑：
  (A) 原始已发布清单   (B) 当前风险规则重筛后清单
对比累计收益 / 最大回撤 / 与沪深300 的超额，回答：
  “若当时就启用了现在的风险中性化，历史纸盘会更好还是更差？”

纯本地、不重跑回测引擎、fail-soft、有界。产物（JSON/MD）默认写入 stock_data/，
并在 .gitignore 忽略，不入库。
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from smcore.config.defaults import (  # noqa: E402
    MAX_SECTOR_WEIGHT_PCT,
    PORTFOLIO_BETA_CEILING,
    STOCK_DATA_DIR,
)
from smcore.strategy import sectors as sec  # noqa: E402
from smcore.strategy.fusion import (  # noqa: E402
    _apply_beta_cap,
    _estimate_betas,
    _portfolio_beta,
)
from smcore.utils.code import format_stock_code  # noqa: E402
from paper_tracker import _max_drawdown, _stock_return_between  # noqa: E402
from walk_forward_validator import _all_signal_days, _load_cached_kdata  # noqa: E402


def _load_dal(date: str) -> pd.DataFrame:
    p = STOCK_DATA_DIR / f"Daily-Action-List-{date}.csv"
    if not p.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(p, encoding="utf-8-sig", dtype=str)
    except Exception:
        return pd.DataFrame()


def replay_one(date: str):
    """对单日已发布清单施加当前风险中性化规则。返回 (orig_df, risk_df, stats)。"""
    orig = _load_dal(date)
    if orig.empty:
        return orig, orig, {"date": date, "skipped": "no DAL"}
    if "建议仓位%" not in orig.columns:
        return orig, orig, {"date": date, "skipped": "no 建议仓位% col"}

    df = orig.copy()
    df["建议仓位%"] = pd.to_numeric(df["建议仓位%"], errors="coerce").fillna(0.0)
    orig_n = len(df)
    orig_deploy = float(df["建议仓位%"].sum())

    # 1) 行业权重上限（因果：本地 sector_map）
    df2, sector_hit = sec.apply_sector_weight_cap(df, top_n=len(df))
    sector_n = len(df2)

    # 2) 组合 β 软约束（因果：as_of=信号日）
    codes = [format_stock_code(c) for c in df2["股票代码"]]
    betas = _estimate_betas(codes, as_of_yyyymmdd=date)
    df3, beta_trim = _apply_beta_cap(df2, betas, max_beta=PORTFOLIO_BETA_CEILING)
    risk_n = len(df3)

    stats = {
        "date": date,
        "orig_n": orig_n,
        "after_sector_n": sector_n,
        "after_beta_n": risk_n,
        "dropped_by_sector": orig_n - sector_n,
        "dropped_by_beta": sector_n - risk_n,
        "orig_deploy_pct": round(orig_deploy, 2),
        "risk_deploy_pct": round(float(df3["建议仓位%"].sum()), 2),
        "sector_cap_hit": bool(sector_hit),
        "beta_trimmed": int(beta_trim),
        "portfolio_beta": round(_portfolio_beta(df3, betas), 3),
    }
    return orig, df3, stats


def _weights_from_df(df: pd.DataFrame) -> dict[str, float]:
    if df is None or df.empty:
        return {}
    out = {}
    for _, r in df.iterrows():
        c = format_stock_code(r.get("股票代码"))
        w = pd.to_numeric(r.get("建议仓位%"), errors="coerce")
        if c and pd.notna(w) and w > 0:
            out[c] = float(w)
    return out


def _paper_curve(weights_by_day: dict[str, dict[str, float]], days: list[str]) -> dict:
    """与 paper_tracker 一致的持有至下一信号日模拟，但权重来自传入 map。"""
    value = 1.0
    curve = []
    realized = 0
    for i in range(len(days) - 1):
        sd, nxt = days[i], days[i + 1]
        wmap = weights_by_day.get(sd, {})
        if not wmap:
            continue
        alloc = {}
        for c, w in wmap.items():
            r = _stock_return_between(c, sd, nxt)
            if r is None:
                continue
            alloc[c] = (w, r)
        if not alloc:
            continue
        tot = sum(w for w, _ in alloc.values())
        period_ret = sum((w / tot) * r for w, r in alloc.values())
        value *= (1 + period_ret / 100.0)
        realized += 1
        curve.append({"from": sd, "to": nxt, "ret": round(period_ret, 3),
                      "value": round(value, 6)})
    total = (value - 1) * 100.0
    mdd = _max_drawdown([1.0] + [c["value"] for c in curve])
    return {
        "realized_periods": realized,
        "final_value": round(value, 6),
        "total_return_pct": round(total, 2),
        "max_drawdown_pct": round(mdd, 2),
    }


def run() -> dict:
    days = _all_signal_days()
    days = [d for d in days if _load_dal(d) is not None and not _load_dal(d).empty]
    if len(days) < 2:
        return {"error": "可用 DAL 不足 2 个", "n_days": len(days)}

    stats = []
    orig_weights: dict[str, dict[str, float]] = {}
    risk_weights: dict[str, dict[str, float]] = {}
    for d in days:
        orig, risk, st = replay_one(d)
        stats.append(st)
        orig_weights[d] = _weights_from_df(orig)
        risk_weights[d] = _weights_from_df(risk)

    orig_perf = _paper_curve(orig_weights, days)
    risk_perf = _paper_curve(risk_weights, days)

    n_dropped_any = sum(1 for s in stats if s.get("dropped_by_sector", 0) + s.get("dropped_by_beta", 0) > 0)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n_days": len(days),
        "first_day": days[0],
        "last_day": days[-1],
        "days_with_any_drop": n_dropped_any,
        "orig_perf": orig_perf,
        "risk_perf": risk_perf,
        "delta": {
            "total_return_pct": round(risk_perf["total_return_pct"] - orig_perf["total_return_pct"], 2),
            "max_drawdown_pct": round(risk_perf["max_drawdown_pct"] - orig_perf["max_drawdown_pct"], 2),
        },
        "per_day": stats,
        "caveats": [
            "仅重放「风险中性化」这一层新规则；自适应权重/融合阈值等未重算（需原始候选文件，早期日期已丢失）。",
            "β 估计依赖本地 k_data 与沪深300 序列，已按信号日截断（loc[:target]），严格因果；若沪深300 序列缺失则 β 回退 1.0（中性）。",
            "纸盘为「持有至下一信号日再平衡」的毛收益，不含交易成本与滑点；与 paper_tracker 口径一致。",
        ],
    }


def _format_md(res: dict) -> str:
    if "error" in res:
        return f"# 当前规则重筛历史\n\n⚠️ {res['error']}\n"
    lines = [
        "# 当前规则重筛历史清单（风险中性化）",
        "",
        f"- 信号日区间：**{res['first_day']} ~ {res['last_day']}**（共 {res['n_days']} 日，"
        f"其中 {res['days_with_any_drop']} 日因当前风险规则被剔除至少 1 只）",
        "",
        "## 原始 vs 当前风险规则 · 纸盘对比",
        "",
        "| 口径 | 累计收益 | 最大回撤 |",
        "| --- | --- | --- |",
        f"| 原始已发布清单 | {res['orig_perf']['total_return_pct']:+.2f}% | {res['orig_perf']['max_drawdown_pct']:+.2f}% |",
        f"| 当前风险规则重筛 | {res['risk_perf']['total_return_pct']:+.2f}% | {res['risk_perf']['max_drawdown_pct']:+.2f}% |",
        f"| **Δ（风险规则 - 原始）** | **{res['delta']['total_return_pct']:+.2f}pp** | **{res['delta']['max_drawdown_pct']:+.2f}pp** |",
        "",
        "## 逐日剔除情况（前 12 日）",
        "",
        "| 日期 | 原始数 | 行业上限剔除 | β上限剔除 | 风险后数 | 风险后部署% | 组合β |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for s in res["per_day"][:12]:
        if s.get("skipped"):
            continue
        lines.append(
            f"| {s['date']} | {s['orig_n']} | {s['dropped_by_sector']} | "
            f"{s['dropped_by_beta']} | {s['after_beta_n']} | {s['risk_deploy_pct']:.1f} | "
            f"{s['portfolio_beta']} |"
        )
    lines += ["", "## 说明 / 局限", ""]
    lines += [f"- {c}" for c in res["caveats"]]
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--emit-json", default=str(STOCK_DATA_DIR / "replay_current_rules.json"))
    ap.add_argument("--emit-md", default=str(STOCK_DATA_DIR / "replay_current_rules.md"))
    args = ap.parse_args()

    res = run()
    if args.emit_json:
        Path(args.emit_json).write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[replay] JSON -> {args.emit_json}")
    if args.emit_md:
        Path(args.emit_md).write_text(_format_md(res), encoding="utf-8")
        print(f"[replay] MD   -> {args.emit_md}")

    if "error" in res:
        print(res["error"])
        return
    print(f"[replay] 区间 {res['first_day']}~{res['last_day']} 共 {res['n_days']} 日，"
          f"{res['days_with_any_drop']} 日被剔除")
    print(f"[replay] 原始收益 {res['orig_perf']['total_return_pct']:+.2f}% / "
          f"回撤 {res['orig_perf']['max_drawdown_pct']:+.2f}%")
    print(f"[replay] 风险后收益 {res['risk_perf']['total_return_pct']:+.2f}% / "
          f"回撤 {res['risk_perf']['max_drawdown_pct']:+.2f}%")
    print(f"[replay] Δ收益 {res['delta']['total_return_pct']:+.2f}pp / "
          f"Δ回撤 {res['delta']['max_drawdown_pct']:+.2f}pp")


if __name__ == "__main__":
    main()
