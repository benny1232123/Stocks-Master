#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""relativity 参数敏感度扫描（离线）：RS_UP_TOL × RS_MAX_STALE_DAYS。

审计 Round 15/19 明确 defer 的末项——relativity 的「相对强弱 up_tol」与「停牌/流动性时效
stale_days」两参数此前是手工拍的（-0.005 / 7），未做 walk-forward 稳健性检验。本脚本在
**离线、零网络**前提下隔离这两个参数的敏感度：

- 候选宇宙：本地 k_data 缓存（全 A 股 OHLC），不重跑 shareholder/资金流等需联网的候选构建。
- 基准：用已缓存的上证综指 sh.000001 作代理（生产用 sh.000300；仅影响绝对校准，不影响
  参数组合的**相对排序**）。
- 对每个信号日，逐票调用与生产一致的 relative_strength_pass 逻辑（down_outperf/min_up_ratio
  等其余 RS 参数保持默认值不动），仅扫 up_tol 与 stale_days 两维。
- edge 指标：通过集在「信号日后首交易日买 → 持有 FORWARD_DAYS 交易日卖」的**等权前向收益**
  （同时给「超额收益 = 个股 - 同期指数」剔除 Beta 漂移），以及通过率（衡量筛选严格度）。

输出按 edge（超额收益）降序的参数组合表，并做前后半段 walk-forward 稳定性检查，
给出稳健推荐值。纯研究脚本，零实盘、零写入。
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.config.defaults import STOCK_DATA_DIR  # noqa: E402
from smcore.utils.code import format_stock_code  # noqa: E402

# ── 与生产一致的 RS 默认（仅 up_tol / stale_days 被扫描，其余不动）──
LOOKBACK_DAYS = 100
MIN_OVERLAP_DAYS = 30
DOWN_OUTPERF = 0.0
MIN_UP_RATIO = 0.6
MIN_DOWN_RATIO = 0.7
MIN_UP_DAYS = 5
MIN_DOWN_DAYS = 5
PRICE_LOWER_LIMIT = 5.0
PRICE_UPPER_LIMIT = 30.0
INDEX_PROXY = "000001"          # 缓存的上证综指；生产用 sh.000300（仅校准差异）
FORWARD_DAYS = 10

# 扫描网格
UP_TOL_GRID = [-0.010, -0.007, -0.005, -0.003, -0.001, 0.0, 0.005]
STALE_GRID = [3, 5, 7, 10, 14]


def _load_close_map() -> dict[str, "object"]:
    """预载 k_data 缓存：code6 -> 以 date 为索引的 close Series（含 open 用于前向买入价）。"""
    import pandas as pd

    out: dict[str, pd.DataFrame] = {}
    for p in sorted(STOCK_DATA_DIR.glob("k_data/*_qfq_full.csv")):
        code = format_stock_code(p.name.split("_")[0])
        if not code or not code.isdigit():
            continue
        try:
            df = pd.read_csv(p, usecols=["date", "open", "close"])
        except Exception:
            continue
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for c in ("open", "close"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["date", "close"]).sort_values("date").set_index("date")
        if len(df) >= MIN_OVERLAP_DAYS:
            out[code] = df
    return out


def _forward_return(df: "object", sd: date, fwd: int) -> tuple[float | None, float | None]:
    """信号日 sd 后首交易日买入，持有 fwd 个交易日卖出。返回 (个股收益, 指数同期收益)。"""
    import numpy as np

    future = df.index[df.index.date > sd]
    if len(future) <= fwd:
        return None, None
    buy_i = future[0]
    sell_i = future[fwd]
    buy_px = float(df["close"].loc[buy_i])
    sell_px = float(df["close"].loc[sell_i])
    if buy_px <= 0:
        return None, None
    stock_ret = sell_px / buy_px - 1.0
    return stock_ret, None  # 指数收益在调用处用 index df 单独算


def run_sweep(limit_days: int | None = None, emit_json: str | None = None) -> dict:
    import numpy as np
    import pandas as pd

    close_map = _load_close_map()
    idx_df = close_map.get(INDEX_PROXY)
    if idx_df is None:
        return {"error": f"指数代理 {INDEX_PROXY} 的 k_data 缓存缺失（需 stock_data/k_data/000001_qfq_full.csv）"}

    # 信号日
    days = []
    for f in sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv")):
        suf = f.name.replace("Daily-Action-List-", "").replace(".csv", "")
        if len(suf) == 8 and suf.isdigit():
            try:
                days.append(date(int(suf[:4]), int(suf[4:6]), int(suf[6:8])))
            except ValueError:
                continue
    days.sort()
    if limit_days:
        days = days[:limit_days]
    if len(days) < 2:
        return {"error": "信号日不足 2 个，无法扫描"}

    # 累加器：combo -> {pass, denom, edge_sum, edge_n}
    acc: dict[tuple[float, int], dict] = {
        (ut, st): {"pass": 0, "denom": 0, "edge_sum": 0.0, "edge_n": 0,
                   "excess_sum": 0.0}
        for ut in UP_TOL_GRID for st in STALE_GRID
    }
    # walk-forward 稳定性：前半 / 后半 段分别累计 edge
    mid = len(days) // 2
    acc_early: dict = {k: 0.0 for k in acc}
    acc_late: dict = {k: 0.0 for k in acc}
    acc_early_n: dict = {k: 0 for k in acc}
    acc_late_n: dict = {k: 0 for k in acc}

    total_eval = 0
    for di, sd in enumerate(days):
        start = sd - timedelta(days=LOOKBACK_DAYS)
        end = sd
        # 指数窗口
        i_win = idx_df.loc[start:end]
        if len(i_win) < MIN_OVERLAP_DAYS:
            continue
        i_ret = i_win["close"].pct_change().dropna()
        # 指数同期前向收益（超额基准）
        i_fwd = _index_fwd(i_win, sd, FORWARD_DAYS)

        for code, s_df in close_map.items():
            s_win = s_df.loc[start:end]
            if len(s_win) < MIN_OVERLAP_DAYS:
                continue
            latest_trade = s_win.index[-1].date()
            latest_close = float(s_win["close"].iloc[-1])
            if latest_close < PRICE_LOWER_LIMIT or latest_close > PRICE_UPPER_LIMIT:
                continue
            s_ret = s_win["close"].pct_change().dropna()
            # 合并个股/指数日收益（按日期）
            m = pd.concat([s_ret.rename("sret"), i_ret.rename("iret")], axis=1, join="inner").dropna()
            if len(m) < MIN_OVERLAP_DAYS:
                continue
            up_mask = (m["iret"] > 0).values
            down_mask = (m["iret"] < 0).values
            up_days = int(up_mask.sum())
            down_days = int(down_mask.sum())
            if up_days < MIN_UP_DAYS or down_days < MIN_DOWN_DAYS:
                continue
            diff_up = (m["sret"] - m["iret"]).values[up_mask]
            diff_down = (m["sret"] - m["iret"]).values[down_mask]
            down_ok = int((diff_down >= DOWN_OUTPERF).sum())
            down_ratio = down_ok / down_days if down_days else 0.0
            # 前向收益
            stock_fwd, _ = _forward_return(s_df, sd, FORWARD_DAYS)
            if stock_fwd is None:
                continue
            excess = (stock_fwd - i_fwd) if i_fwd is not None else stock_fwd

            total_eval += 1
            for st in STALE_GRID:
                stale_fail = (end - latest_trade).days > st
                if stale_fail:
                    continue
                acc[(UP_TOL_GRID[0], st)]["denom"] += 1  # denom 与 up_tol 无关，记一次即可
                for ut in UP_TOL_GRID:
                    up_ok = int((diff_up >= ut).sum())
                    up_ratio = up_ok / up_days if up_days else 0.0
                    passed = (up_ratio >= MIN_UP_RATIO) and (down_ratio >= MIN_DOWN_RATIO)
                    key = (ut, st)
                    if passed:
                        acc[key]["pass"] += 1
                        acc[key]["edge_sum"] += stock_fwd
                        acc[key]["edge_n"] += 1
                        acc[key]["excess_sum"] += excess
                        if di < mid:
                            acc_early[key] += excess
                            acc_early_n[key] += 1
                        else:
                            acc_late[key] += excess
                            acc_late_n[key] += 1

    rows = []
    for (ut, st), a in acc.items():
        n = a["edge_n"]
        rows.append({
            "up_tol": ut, "stale_days": st,
            "pass_rate": round(a["pass"] / a["denom"], 4) if a["denom"] else 0.0,
            "n_passed": a["pass"],
            "mean_fwd_ret": round(a["edge_sum"] / n, 4) if n else None,
            "mean_excess": round(a["excess_sum"] / n, 4) if n else None,
            "wf_early_excess": round(acc_early[(ut, st)] / acc_early_n[(ut, st)], 4) if acc_early_n[(ut, st)] else None,
            "wf_late_excess": round(acc_late[(ut, st)] / acc_late_n[(ut, st)], 4) if acc_late_n[(ut, st)] else None,
        })
    rows.sort(key=lambda r: (r["mean_excess"] if r["mean_excess"] is not None else -9e9), reverse=True)

    res = {
        "n_signal_days": len(days),
        "n_universe": len(close_map),
        "total_eval": total_eval,
        "index_proxy": INDEX_PROXY,
        "forward_days": FORWARD_DAYS,
        "grid_up_tol": UP_TOL_GRID,
        "grid_stale": STALE_GRID,
        "rows": rows,
    }
    if emit_json:
        try:
            with open(emit_json, "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False, indent=2, default=str)
        except Exception as e:  # pragma: no cover
            print(f"写 JSON 失败：{e}")
    return res


def _index_fwd(idx_df: "object", sd: date, fwd: int):
    """指数同期前向收益（与个股窗口对齐）。"""
    future = idx_df.index[idx_df.index.date > sd]
    if len(future) <= fwd:
        return None
    buy_i = future[0]
    sell_i = future[fwd]
    buy_px = float(idx_df["close"].loc[buy_i])
    sell_px = float(idx_df["close"].loc[sell_i])
    if buy_px <= 0:
        return None
    return sell_px / buy_px - 1.0


def _fmt(res: dict) -> str:
    if res.get("error"):
        return f"# relativity 参数扫描\n\n错误：{res['error']}\n"
    lines = [
        "# relativity 参数敏感度扫描（RS_UP_TOL × RS_MAX_STALE_DAYS，离线）",
        "",
        f"- 信号日：**{res['n_signal_days']}**　候选宇宙：**{res['n_universe']}** 只（本地 k_data）",
        f"- 累计逐票评估：**{res['total_eval']}**　指数代理：**{res['index_proxy']}**（生产 sh.000300）",
        f"- 前向持有：**{res['forward_days']}** 交易日；edge 按「超额收益(个股-指数)」降序",
        "",
        "| up_tol | stale | 通过率 | 通过数 | 均前向收益 | 均超额 | 前半超额 | 后半超额 |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for r in res["rows"]:
        lines.append(
            f"| {r['up_tol']:+} | {r['stale_days']} | {r['pass_rate']:.2%} | {r['n_passed']} | "
            f"{r['mean_fwd_ret'] if r['mean_fwd_ret'] is not None else '—'} | "
            f"{r['mean_excess'] if r['mean_excess'] is not None else '—'} | "
            f"{r['wf_early_excess'] if r['wf_early_excess'] is not None else '—'} | "
            f"{r['wf_late_excess'] if r['wf_late_excess'] is not None else '—'} |"
        )
    lines.append("")
    lines.append("> 说明：down_outperf/min_up_ratio/min_down_ratio/价格上下限等均保持生产默认值不动，")
    lines.append("> 仅扫 up_tol 与 stale_days 两维。候选宇宙为本地 k_data 全量（未含 shareholder/资金流联网筛选），")
    lines.append("> 故通过率绝对值偏高、前向收益含噪声；但**参数组合的相对排序**对生产选择有参考意义。")
    lines.append("> walk-forward 稳定性看「前半超额 / 后半超额」是否同号且接近——同号越稳，推荐越可信。")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit-days", type=int, default=None, help="仅扫描前 N 个信号日（调试用）")
    ap.add_argument("--emit-json", default=None)
    args = ap.parse_args()
    res = run_sweep(limit_days=args.limit_days, emit_json=args.emit_json)
    print(_fmt(res))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
