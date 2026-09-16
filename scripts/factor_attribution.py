#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""按「因子类型」归因前向回测收益：到底是哪类因子在赚钱 / 拖后腿。

为什么需要它：
- 看板的「总体总结」只给**整体组合**口径（含仓位/现金/相关性），看不出构成。
- 而「融合是不是在创造价值」这个问题，必须拆解到因子层面才能回答：
  如果某类因子单干就能跑赢融合组合，说明**融合在稀释它**。

数据源与口径（严格与看板一致，避免两套数）：
- 成交：stock_data/Multi-Backtest-<date>-trades.csv（前向回测真实成交，含 return_pct）。
  ⚠️ 交易表里没有策略字段 → 必须与当日 Daily-Action-List-<date>.csv 按股票代码 join 取「来源策略」。
- 脏笔剔除：复用 backend.trade_sanitize._is_corrupt_trade（同日买卖 / 止盈却亏损），
  与 /api/backtests/daily-summary 完全一致。
- 因子类型：复用 smcore.strategy.factor_types（唯一真源，与报告/监控/前端同源）。

归因口径说明（重要）：
- 一个标的可能命中多策略（来源策略="Boll/Momentum"）→ 本脚本按**主类型**
  （来源策略首个）归因，保证笔数不重复、可加总；同时输出**策略级**明细供交叉核对。
- 贡献 = 按「建议仓位%」加权的收益占比，用于解释「等权笔均为正、组合日为负」的差额。

用法：
    python scripts/factor_attribution.py [--lookback N] [--out 路径]
    --lookback 0（默认）= 全部信号日；20/40 = 最近 N 个信号日（与看板窗口一致）
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.trade_sanitize import _is_corrupt_trade  # noqa: E402
from smcore.strategy.factor_types import (  # noqa: E402
    FACTOR_TYPE_ORDER,
    factor_type_of,
)

STOCK_DATA_DIR = ROOT / "stock_data"
DEFAULT_OUT = STOCK_DATA_DIR / "factor_ic_replay" / "factor_attribution.md"


def _norm_code(x) -> str:
    """股票代码归一：接受 600426 / '600426' / '600426.SH' / '1'(丢前导零) 等写法 → 6 位字符串。

    ⚠️ 关键坑：Daily-Action-List 的「股票代码」列在部分历史批次里被写成整数（前导零丢失，
    如 000001 → 1），而成交表里是标准 6 位。不补零会导致 join 大面积失败
    （首次运行 1009 笔里 590 笔落到「其他」，归因直接失真）。故统一 zfill(6)。
    """
    s = str(x).strip()
    if s.endswith(".0"):  # pandas 把纯数字列读成 float
        s = s[:-2]
    m = re.search(r"(\d+)", s)
    return m.group(1).zfill(6) if m else s


def signal_days(data_dir: Path = STOCK_DATA_DIR) -> list[str]:
    """有「成交表 + 当日清单」两者齐全的信号日（升序）。缺清单则无法取来源策略，直接跳过。"""
    out = []
    for f in sorted(data_dir.glob("Multi-Backtest-*-trades.csv")):
        tag = f.name[len("Multi-Backtest-"):-len("-trades.csv")]
        if (data_dir / f"Daily-Action-List-{tag}.csv").exists():
            out.append(tag)
    return out


def dal_index(data_dir: Path = STOCK_DATA_DIR) -> tuple[list[str], dict[str, dict]]:
    """预加载全部 Daily-Action-List → (升序日期列表, {date: {code: (来源策略, 建议仓位%)}})。"""
    dates: list[str] = []
    idx: dict[str, dict] = {}
    for f in sorted(data_dir.glob("Daily-Action-List-*.csv")):
        tag = f.name[len("Daily-Action-List-"):-len(".csv")]
        try:
            ddf = pd.read_csv(f, dtype=str)
        except Exception:
            continue
        if ddf.empty or "来源策略" not in ddf.columns:
            continue
        m: dict[str, tuple[str, float]] = {}
        for _, r in ddf.iterrows():
            try:
                pos = float(r.get("建议仓位%") or 0)
            except (TypeError, ValueError):
                pos = 0.0
            m[_norm_code(r.get("股票代码"))] = (str(r.get("来源策略") or ""), pos)
        if m:
            dates.append(tag)
            idx[tag] = m
    return dates, idx


def load_rows(data_dir: Path, batch_tag: str, dates: list[str], idx: dict,
              max_lookback: int = 30) -> list[dict]:
    """读单批成交，按「买入日之前最近的、含该代码的清单」取来源策略。

    ⚠️ 为什么不能只用同日清单：
      ① 一个 Multi-Backtest 批次可能跨多个信号日（summary 有 signal_start~signal_end）；
      ② 批次买入日常晚于批次日期（实测 20260618 批次买入日 = 2026-06-22，而当日清单仅 5 行却成交 10 笔）。
      同日 join 实测只匹配 41.5%，归因严重失真 → 改为按买入日回溯最近的含该码清单。
    """
    try:
        tdf = pd.read_csv(data_dir / f"Multi-Backtest-{batch_tag}-trades.csv", dtype=str)
    except Exception:
        return []
    if tdf.empty or "return_pct" not in tdf.columns:
        return []
    tdf = tdf[~tdf.apply(_is_corrupt_trade, axis=1)]
    if tdf.empty:
        return []
    rows = []
    for _, r in tdf.iterrows():
        try:
            rp = float(r.get("return_pct"))
        except (TypeError, ValueError):
            continue
        code = _norm_code(r.get("code"))
        # 买入日归一为 YYYYMMDD，与清单日期同一量纲后再比较
        buy = str(r.get("buy_date") or "").strip().replace("-", "")
        source, pos = "", 0.0
        cands = [d for d in dates if (not buy or d <= buy)]
        for d in reversed(cands[-max_lookback:]):
            hit = idx[d].get(code)
            if hit:
                source, pos = hit
                break
        rows.append({"day": batch_tag, "code": code, "source": source,
                     "return_pct": rp, "pos_pct": pos})
    return rows


def load_batch_returns(data_dir: Path, days: list[str]) -> dict[str, float]:
    """读各批次 summary 的 total_return —— 这就是看板「组合日均收益」的原始数据，
    与前端 /api/backtests/daily-summary 完全同口径（含现金/真实组合构建）。"""
    out: dict[str, float] = {}
    for tag in days:
        try:
            sdf = pd.read_csv(data_dir / f"Multi-Backtest-{tag}-summary.csv")
        except Exception:
            continue
        if sdf.empty or "total_return" not in sdf.columns:
            continue
        try:
            out[tag] = float(sdf.iloc[0]["total_return"])
        except (TypeError, ValueError):
            continue
    return out


def primary_factor_type(source: str) -> str:
    """来源策略（可多策略）→ 主因子类型：取首个，保证归因不重复。"""
    if not source:
        return "其他"
    return factor_type_of(str(source).split("/")[0].strip())


def _stats(vals: list[float], weights: list[float] | None = None) -> dict:
    """一组收益的基础统计：等权均值/中位数/胜率/盈亏比 + 加权均值。"""
    if not vals:
        return {"n": 0, "mean": None, "median": None, "win_rate": None,
                "avg_win": None, "avg_loss": None, "payoff": None, "wmean": None}
    n = len(vals)
    wins = [v for v in vals if v > 0]
    losses = [v for v in vals if v <= 0]
    aw = sum(wins) / len(wins) if wins else None
    al = sum(losses) / len(losses) if losses else None
    wmean = None
    if weights and sum(weights) > 0:
        wmean = sum(v * w for v, w in zip(vals, weights)) / sum(weights)
    return {
        "n": n,
        "mean": round(sum(vals) / n, 3),
        "median": round(float(pd.Series(vals).median()), 3),
        "win_rate": round(len(wins) / n * 100, 1),
        "avg_win": round(aw, 3) if aw is not None else None,
        "avg_loss": round(al, 3) if al is not None else None,
        "payoff": round(abs(aw / al), 2) if (aw is not None and al) else None,
        "wmean": round(wmean, 3) if wmean is not None else None,
    }


def summarize(rows: list[dict], keyfn) -> dict[str, dict]:
    """按 keyfn 分组统计，并补「贡献占比」（按建议仓位%加权对组合的贡献）。"""
    groups: dict[str, list[dict]] = {}
    for r in rows:
        groups.setdefault(keyfn(r), []).append(r)
    total_w = sum(r["pos_pct"] for r in rows) or 1.0
    out = {}
    for k, grp in groups.items():
        vals = [r["return_pct"] for r in grp]
        ws = [r["pos_pct"] for r in grp]
        st = _stats(vals, ws)
        # 贡献 = Σ(收益×仓位) / Σ(全部仓位) —— 各项相加 = 组合加权收益
        st["contrib"] = round(sum(r["return_pct"] * r["pos_pct"] for r in grp) / total_w, 3)
        out[k] = st
    return out


def portfolio(rows: list[dict]) -> dict:
    """组合整体：加权（按建议仓位%）vs 等权——解释看板上两者的差额。"""
    vals = [r["return_pct"] for r in rows if r["pos_pct"] > 0]
    ws = [r["pos_pct"] for r in rows if r["pos_pct"] > 0]
    byday: dict[str, list[dict]] = {}
    for r in rows:
        if r["pos_pct"] > 0:
            byday.setdefault(r["day"], []).append(r)
    day_rets = []
    for day, grp in byday.items():
        tw = sum(g["pos_pct"] for g in grp) or 1.0
        day_rets.append(sum(g["return_pct"] * g["pos_pct"] for g in grp) / tw)
    return {
        "n_trades": len(rows),
        "n_trades_pos": len(vals),
        "equal_mean": round(sum(r["return_pct"] for r in rows) / len(rows), 3) if rows else None,
        "weighted_mean": round(sum(v * w for v, w in zip(vals, ws)) / sum(ws), 3) if ws and sum(ws) else None,
        "n_days": len(day_rets),
        "day_mean": round(sum(day_rets) / len(day_rets), 3) if day_rets else None,
        "day_positive": sum(1 for d in day_rets if d > 0),
    }


def _ordered(d: dict) -> list[tuple[str, dict]]:
    keys = [k for k in FACTOR_TYPE_ORDER if k in d]
    keys += [k for k in d if k not in FACTOR_TYPE_ORDER]
    return [(k, d[k]) for k in keys]


def _table(d: dict, title: str) -> list[str]:
    lines = ["", f"### {title}", "",
             "| 分组 | 笔数 | 均值% | 中位% | 胜率% | 均赢% | 均亏% | 盈亏比 | 仓位加权% | 贡献% |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    for k, v in _ordered(d):
        f = lambda x: "--" if x is None else f"{x}"  # noqa: E731
        lines.append(
            f"| {k} | {v['n']} | {f(v['mean'])} | {f(v['median'])} | {f(v['win_rate'])} | "
            f"{f(v['avg_win'])} | {f(v['avg_loss'])} | {f(v['payoff'])} | {f(v['wmean'])} | {f(v['contrib'])} |"
        )
    return lines


def build_report(rows_all: list[dict], days: list[str], lookback: int,
                 brets: dict[str, float] | None = None) -> str:
    windows = [("全样本", rows_all)]
    for lb in (40, 20):
        if len(days) > lb:
            keep = set(days[-lb:])
            windows.append((f"近 {lb} 个信号日", [r for r in rows_all if r["day"] in keep]))

    matched = sum(1 for r in rows_all if r["source"])
    cov = matched / len(rows_all) * 100 if rows_all else 0.0
    lines = ["# 按因子类型的前向回测归因", "",
             f"- 信号日：{len(days)} 个（{days[0]} ~ {days[-1]}）" if days else "- 无数据",
             f"- 成交笔数：{len(rows_all)}（已剔除同日买卖 / 止盈却亏损的脏笔）",
             f"- **可归因覆盖：{matched}/{len(rows_all)}（{cov:.1f}%）** —— 未匹配=买入日回溯清单中查无此码",
             f"- 归因口径：主因子类型（来源策略首个，保证不重复计数）；因子类型映射与报告/监控/前端同源",
             f"- 归因表只统计**可归因**笔数；组合整体指标用**全部**成交（与看板一致）", ""
             ]
    for name, rows in windows:
        if not rows:
            continue
        p = portfolio(rows)
        mrows = [r for r in rows if r["source"]]
        lines += ["", f"## {name}", ""]
        lines += [f"- 组合整体（全部成交）：**按日加权均值 {p['day_mean']}%**"
                  f"（{p['day_positive']}/{p['n_days']} 个信号日为正）；等权笔均 **{p['equal_mean']}%**；"
                  f"仓位加权 **{p['weighted_mean']}%**"]
        if brets:
            wdays = sorted({r["day"] for r in rows})
            vals = [brets[d] for d in wdays if d in brets]
            if vals:
                pos = sum(1 for v in vals if v > 0)
                lines += [f"- **看板同口径**（批次 total_return 均值）：**{round(sum(vals)/len(vals), 3)}%**"
                          f"（{pos}/{len(vals)} 个信号日为正）← 与前端「组合日均收益」一致"]
        lines += _table(summarize(mrows, lambda r: primary_factor_type(r["source"])), "按因子类型")
        lines += _table(summarize(mrows, lambda r: str(r["source"]).split("/")[0].strip() or "未知"), "按策略（交叉核对）")
    lines += ["", "> 「贡献%」按建议仓位%加权，各项相加 = 组合加权收益，用于解释等权笔均与组合日的差额。",
              "> 「仓位加权%」= 该组内部的仓位加权收益，可与该组等权均值对比看仓位分配是否帮倒忙。"]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback", type=int, default=0, help="0=全部信号日；N=最近 N 个")
    ap.add_argument("--dal-lookback", type=int, default=30,
                    help="归因时向买入日之前回溯多少个清单日（越大覆盖率越高，但可归因越旧）")
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    args = ap.parse_args()

    days = signal_days()
    if args.lookback:
        days = days[-args.lookback:]
    dal_dates, idx = dal_index()
    rows: list[dict] = []
    for d in days:
        rows.extend(load_rows(STOCK_DATA_DIR, d, dal_dates, idx, max_lookback=args.dal_lookback))
    if not rows:
        print("无可用成交数据（需 Multi-Backtest-*-trades.csv + 当日 Daily-Action-List-*.csv 齐全）")
        return 1

    brets = load_batch_returns(STOCK_DATA_DIR, days)
    report = build_report(rows, days, args.lookback, brets)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report, encoding="utf-8")

    p = portfolio(rows)
    mrows = [r for r in rows if r["source"]]
    print("=" * 64)
    print(f"信号日 {len(days)} 个 · 成交 {len(rows)} 笔 · 可归因 {len(mrows)} 笔"
          f"（{len(mrows)/len(rows)*100:.1f}%）")
    print(f"组合：按日加权 {p['day_mean']}% · 正收益日 {p['day_positive']}/{p['n_days']} · 等权笔均 {p['equal_mean']}%")
    print("-" * 64)
    for k, v in _ordered(summarize(mrows, lambda r: primary_factor_type(r["source"]))):
        print(f"  {k:<12} n={v['n']:>4}  均值={v['mean']:>7}%  胜率={v['win_rate']:>5}%  "
              f"仓位加权={v['wmean']}%  贡献={v['contrib']}%")
    print("-" * 64)
    print(f"报告已写：{out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
