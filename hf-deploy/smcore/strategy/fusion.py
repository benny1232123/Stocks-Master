"""信号融合 —— 把四策略结果合并为"今日操作清单"。

此前四策略各自出 CSV、各自推送，用户收到四份独立报告后还要人工合并判断"今天到底买什么"。
本模块读取当日四策略结果，合并去重、打分、算止损止盈、分配仓位，输出一份操作清单。

输入（stock_data/ 下当日文件，缺失则回退最近）：
- Stock-Selection-Boll-YYYYMMDD.csv          (股票代码, 股票名称, 建议买入价)
- Stock-Selection-Relativity-YYYYMMDD.csv    (+ 上涨满足率, 抗跌满足率)
- Stock-Selection-Ashare-Theme-Turnover-*.csv (+ 综合分, 题材标签)
- CCTV-Sector-Stock-Pool-YYYYMMDD.csv        (股票代码, 股票名称, 板块, 热度分)

输出：
- stock_data/Daily-Action-List-YYYYMMDD.csv
- 日报段落文本（可追加到现有推送）
"""
from __future__ import annotations

import csv
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from smcore.config.defaults import DEFAULT_K, DEFAULT_WINDOW, STOCK_DATA_DIR
from smcore.data import ensure_logout, fetch_daily_k, session
from smcore.indicators import calc_bollinger
from smcore.strategy import build_strategy_allocation
from smcore.utils.code import format_stock_code
from smcore.utils.format import fmt_num, to_float

# 各策略在综合评分中的权重（命中该策略即得基础分，多策略叠加加分）
STRATEGY_BASE_SCORE = {
    "boll": 40,       # Boll 是主策略，命中即得 40 分
    "relativity": 25,  # 相对强弱命中 +25
    "theme": 20,       # 题材命中 +20
    "cctv": 15,        # CCTV 命中 +15
}
MULTI_HIT_BONUS = 5  # 每多命中一个策略额外 +5


def _find_strategy_csv(pattern: str, date_yyyymmdd: str) -> Optional[Path]:
    """按日期找策略结果 CSV，找不到则回退最近一期。"""
    # 优先当天根目录
    preferred = STOCK_DATA_DIR / f"{pattern}-{date_yyyymmdd}.csv"
    if preferred.exists():
        return preferred
    # 根目录最近
    candidates = sorted(STOCK_DATA_DIR.glob(f"{pattern}-*.csv"), key=lambda p: p.name, reverse=True)
    if candidates:
        return candidates[0]
    # archive 最近
    archive = STOCK_DATA_DIR / "archive"
    if archive.exists():
        candidates = sorted(archive.rglob(f"{pattern}-*.csv"), key=lambda p: p.name, reverse=True)
        if candidates:
            return candidates[0]
    return None


def _load_boll_picks(date_yyyymmdd: str) -> dict:
    """读取 Boll 选股结果，返回 {code: {name, buy_price}}。"""
    path = _find_strategy_csv("Stock-Selection-Boll", date_yyyymmdd)
    if not path:
        return {}
    picks = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            code = format_stock_code(row.get("股票代码", ""))
            if not code:
                continue
            picks[code] = {
                "name": (row.get("股票名称") or "").strip(),
                "buy_price": to_float(row.get("建议买入价")),
            }
    return picks


def _load_relativity_picks(date_yyyymmdd: str) -> dict:
    """读取相对强弱结果，返回 {code: {name, up_ratio, down_ratio}}。"""
    path = _find_strategy_csv("Stock-Selection-Relativity", date_yyyymmdd)
    if not path:
        return {}
    picks = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            code = format_stock_code(row.get("股票代码", ""))
            if not code:
                continue
            picks[code] = {
                "name": (row.get("股票名称") or "").strip(),
                "up_ratio": to_float(row.get("上涨满足率")),
                "down_ratio": to_float(row.get("抗跌满足率")),
            }
    return picks


def _load_theme_picks(date_yyyymmdd: str) -> dict:
    """读取题材策略结果，返回 {code: {name, score, theme}}。"""
    path = _find_strategy_csv("Stock-Selection-Ashare-Theme-Turnover", date_yyyymmdd)
    if not path:
        return {}
    picks = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            code = format_stock_code(row.get("股票代码", ""))
            if not code:
                continue
            picks[code] = {
                "name": (row.get("股票名称") or "").strip(),
                "score": to_float(row.get("综合分")),
                "theme": (row.get("题材标签") or "").strip(),
            }
    return picks


def _load_cctv_picks(date_yyyymmdd: str) -> dict:
    """读取 CCTV 股票池，返回 {code: {name, sector, heat}}。"""
    path = _find_strategy_csv("CCTV-Sector-Stock-Pool", date_yyyymmdd)
    if not path:
        return {}
    picks = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            code = format_stock_code(row.get("股票代码", ""))
            if not code:
                continue
            picks[code] = {
                "name": (row.get("股票名称") or "").strip(),
                "sector": (row.get("板块") or "").strip(),
                "heat": to_float(row.get("热度分")),
            }
    return picks


def _compute_boll_levels(code: str) -> dict:
    """拉前复权 K 线算 Boll 水位（止损=下轨，止盈=上轨）。"""
    end = date.today()
    start = end.fromordinal(end.toordinal() - 120)  # 120 天前
    df = fetch_daily_k(code, start, end, adjust="qfq")
    if len(df) < DEFAULT_WINDOW:
        return {}
    boll = calc_bollinger(df, window=DEFAULT_WINDOW, k=DEFAULT_K)
    last = boll.iloc[-1]
    return {
        "close": float(last["close"]),
        "lower": float(last["Lower"]) if pd.notna(last.get("Lower")) else None,
        "upper": float(last["Upper"]) if pd.notna(last.get("Upper")) else None,
        "ma20": float(last["MA"]) if pd.notna(last.get("MA")) else None,
    }


def fuse_signals(
    date_yyyymmdd: str,
    *,
    total_capital: float = 100000.0,
    max_picks: int = 15,
    fetch_levels: bool = True,
) -> tuple[pd.DataFrame, str]:
    """融合四策略信号，输出今日操作清单。

    Args:
        date_yyyymmdd: 日期字符串 YYYYMMDD
        total_capital: 总资金（元），用于算每只票建议仓位金额
        max_picks: 最多输出几只票
        fetch_levels: 是否拉 K 线算止损止盈（批量调用网络较慢，可关闭只出清单）

    Returns:
        (result_df, report_text)
    """
    boll = _load_boll_picks(date_yyyymmdd)
    relativity = _load_relativity_picks(date_yyyymmdd)
    theme = _load_theme_picks(date_yyyymmdd)
    cctv = _load_cctv_picks(date_yyyymmdd)

    # 合并所有代码
    all_codes = set(boll) | set(relativity) | set(theme) | set(cctv)
    if not all_codes:
        return pd.DataFrame(), "今日无任何策略命中，无可操作清单。"

    # 策略权重分配（基于信号可用性）
    alloc = build_strategy_allocation(
        "震荡轮动",  # 默认震荡，实际应由市场状态模块给
        boll_rows_count=len(boll),
        theme_rows_count=len(theme),
        has_cctv_hot=bool(cctv),
        macro_level="low",
    )
    weights = alloc["final_weights"]

    rows = []
    with session() as ok:
        for code in all_codes:
            hit_strategies = []
            score = 0
            name = ""
            buy_price = None

            if code in boll:
                hit_strategies.append("Boll")
                score += STRATEGY_BASE_SCORE["boll"]
                name = boll[code]["name"] or name
                buy_price = boll[code].get("buy_price")
            if code in relativity:
                hit_strategies.append("Relativity")
                score += STRATEGY_BASE_SCORE["relativity"]
                name = relativity[code]["name"] or name
            if code in theme:
                hit_strategies.append("Theme")
                score += STRATEGY_BASE_SCORE["theme"]
                name = theme[code]["name"] or name
                # Theme 综合分作为额外加权（综合分 0-100，按 10% 加）
                theme_score = theme[code].get("score") or 0
                score += min(theme_score * 0.1, 10)
            if code in cctv:
                hit_strategies.append("CCTV")
                score += STRATEGY_BASE_SCORE["cctv"]
                name = cctv[code]["name"] or name

            # 多策略命中加分
            if len(hit_strategies) > 1:
                score += (len(hit_strategies) - 1) * MULTI_HIT_BONUS

            # 仓位分配：按命中策略中权重最大的那个分配，单票取该策略权重的 1/N（N=该策略候选数）
            # 避免大池子策略（如 CCTV 673只）把仓位稀释到 0
            strategy_pick_counts = {
                "boll": len(boll),
                "relativity": len(relativity),
                "theme": len(theme),
                "cctv": len(cctv),
            }
            # 取命中策略中权重最高者
            best_weight = 0
            for s in hit_strategies:
                skey = s.lower()
                w = weights.get(skey, 0)
                cnt = max(strategy_pick_counts.get(skey, 1), 1)
                share = w / cnt  # 该策略仓位均分到每只票
                if share > best_weight:
                    best_weight = share
            position_pct = min(best_weight / 100.0, 0.3)  # 单票上限 30%
            position_amount = total_capital * position_pct

            row = {
                "股票代码": code,
                "股票名称": name,
                "命中策略数": len(hit_strategies),
                "来源策略": "/".join(hit_strategies),
                "综合评分": round(score, 1),
                "建议买入价": buy_price,
                "建议仓位%": round(position_pct * 100, 1),
                "建议金额": round(position_amount, 0),
            }

            if fetch_levels and ok:
                levels = _compute_boll_levels(code)
                if levels:
                    row["最新价"] = levels.get("close")
                    row["止损价(下轨)"] = levels.get("lower")
                    row["止盈价(上轨)"] = levels.get("upper")
                    row["MA20"] = levels.get("ma20")

            rows.append(row)

    ensure_logout()

    df = pd.DataFrame(rows).sort_values("综合评分", ascending=False).reset_index(drop=True)
    df = df.head(max_picks)

    # 生成日报段落
    report = _build_report_text(df, date_yyyymmdd, len(boll), len(relativity), len(theme), len(cctv))
    return df, report


def _build_report_text(
    df: pd.DataFrame,
    date_yyyymmdd: str,
    n_boll: int,
    n_relativity: int,
    n_theme: int,
    n_cctv: int,
) -> str:
    """生成日报段落。"""
    if df.empty:
        return "\n## 今日操作清单\n- 无候选"

    lines = [
        f"\n## 今日操作清单（{date_yyyymmdd}）",
        f"- 信号来源: Boll {n_boll} | Relativity {n_relativity} | Theme {n_theme} | CCTV {n_cctv}",
        f"- 融合后候选: {len(df)} 只（按综合评分排序）",
        "",
        "| 代码 | 名称 | 命中 | 评分 | 仓位% | 止损 | 止盈 |",
        "|------|------|------|------|-------|------|------|",
    ]
    for _, r in df.iterrows():
        stop = fmt_num(r.get("止损价(下轨)"), digits=2, na="-")
        take = fmt_num(r.get("止盈价(上轨)"), digits=2, na="-")
        lines.append(
            f"| {r['股票代码']} | {r['股票名称']} | {r['命中策略数']} | {r['综合评分']} | {r['建议仓位%']} | {stop} | {take} |"
        )
    lines.append("")
    lines.append("- 止损=Boll下轨，止盈=Boll上轨（前复权）；仓位为建议上限，单票不超过 30%。")
    return "\n".join(lines)


def save_action_list(df: pd.DataFrame, date_yyyymmdd: str) -> Optional[Path]:
    """保存操作清单 CSV，返回路径。"""
    if df.empty:
        return None
    path = STOCK_DATA_DIR / f"Daily-Action-List-{date_yyyymmdd}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path
