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
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from smcore.config.defaults import DEFAULT_K, DEFAULT_WINDOW, STOCK_DATA_DIR
from smcore.data import fetch_daily_k
from smcore.indicators import calc_bollinger
from smcore.strategy import build_strategy_allocation
from smcore.utils.code import format_stock_code
from smcore.utils.format import fmt_num, to_float

# 各策略在综合评分中的权重（命中该策略即得基础分，多策略叠加加分）
# 占比依据 2026-07-11 实测前向10日 edge（窗口 0606-0626，硬止损+真实成本）：
#   Boll      收益 -5.48% / 回撤 -8.64%   → 最抗跌，提权为锚
#   Relativity 收益 -9.57% / 回撤 -13.86% → 最差且集中度高，砍权
#   Momentum  无历史样本；Relativity 弱势警示「纯强度」风险，适度而非激进
#   Theme/CCTV 窗口内无样本（0628 后才出现信号），暂保持谨慎
STRATEGY_BASE_SCORE = {
    "boll": 45,       # 主策略（超卖均值回归），实测最抗跌，提权
    "relativity": 15,  # 相对强弱实测最差，砍权
    "momentum": 20,    # 动量/相对强度，适度（待实测验证）
    "theme": 15,       # 题材，谨慎
    "cctv": 10,        # CCTV 舆情噪声大，最低
}
MULTI_HIT_BONUS = 5  # 每多命中一个策略额外 +5

# 趋势守卫：价格低于 MA20 超过该比例，视作破位/下降通道自由落体股，剔除。
# 阈值设为 12%——保留 Boll 轻度超卖票（近下轨通常仅低于 MA20 几个百分点），
# 但剔除明显破位（如单日 -20%+ 的崩盘股），从信号层防尾部巨亏。
TREND_GUARD_BELOW_MA20 = 0.12


def _passes_trend_guard(price, ma20) -> bool:
    """趋势守卫：价格远低于 MA20 则剔除（破位/下降通道）；数据缺失则保守保留。"""
    if price is None or ma20 is None:
        return True
    try:
        price = float(price)
        ma20 = float(ma20)
    except (TypeError, ValueError):
        return True
    if ma20 <= 0 or price <= 0:
        return True
    return price >= ma20 * (1 - TREND_GUARD_BELOW_MA20)


def _extract_date_from_filename(path: Path) -> Optional[str]:
    """从文件名末尾提取 YYYYMMDD，例如 Stock-Selection-Boll-20260704.csv。"""
    suffix = path.stem.rsplit("-", 1)[-1]
    if len(suffix) == 8 and suffix.isdigit():
        return suffix
    return None


def _find_strategy_csv(
    pattern: str,
    date_yyyymmdd: str,
    *,
    max_stale_days: int = 3,
) -> Optional[tuple[Path, str]]:
    """按日期找策略结果 CSV，仅在 max_stale_days 内回退到最近一期。

    此前各策略独立回退到“各自最新文件”，可能把不同日期的信号混在一起，
    导致操作清单基于过期/错日数据。现在统一限制回退窗口并返回实际日期。
    """
    preferred = STOCK_DATA_DIR / f"{pattern}-{date_yyyymmdd}.csv"
    if preferred.exists():
        return preferred, date_yyyymmdd

    requested = datetime.strptime(date_yyyymmdd, "%Y%m%d").date()
    best: Optional[tuple[Path, str]] = None

    def _consider(paths: list[Path]) -> None:
        nonlocal best
        for path in paths:
            actual_date = _extract_date_from_filename(path)
            if not actual_date:
                continue
            actual = datetime.strptime(actual_date, "%Y%m%d").date()
            stale_days = (requested - actual).days
            if stale_days < 0 or stale_days > max_stale_days:
                continue
            if best is None or actual_date > best[1]:
                best = (path, actual_date)

    _consider(sorted(STOCK_DATA_DIR.glob(f"{pattern}-*.csv"), key=lambda p: p.name, reverse=True))

    archive = STOCK_DATA_DIR / "archive"
    if archive.exists():
        _consider(sorted(archive.rglob(f"{pattern}-*.csv"), key=lambda p: p.name, reverse=True))

    return best


def _load_boll_picks(date_yyyymmdd: str, *, max_stale_days: int = 3) -> tuple[dict, Optional[str]]:
    """读取 Boll 选股结果，返回 ({code: {...}}, 实际数据日期)。"""
    found = _find_strategy_csv("Stock-Selection-Boll", date_yyyymmdd, max_stale_days=max_stale_days)
    if not found:
        return {}, None
    path, actual_date = found
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
    return picks, actual_date


def _load_relativity_picks(date_yyyymmdd: str, *, max_stale_days: int = 3) -> tuple[dict, Optional[str]]:
    """读取相对强弱结果，返回 ({code: {...}}, 实际数据日期)。"""
    found = _find_strategy_csv("Stock-Selection-Relativity", date_yyyymmdd, max_stale_days=max_stale_days)
    if not found:
        return {}, None
    path, actual_date = found
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
    return picks, actual_date


def _load_theme_picks(date_yyyymmdd: str, *, max_stale_days: int = 3) -> tuple[dict, Optional[str]]:
    """读取题材策略结果，返回 ({code: {...}}, 实际数据日期)。"""
    found = _find_strategy_csv("Stock-Selection-Ashare-Theme-Turnover", date_yyyymmdd, max_stale_days=max_stale_days)
    if not found:
        return {}, None
    path, actual_date = found
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
    return picks, actual_date


def _load_cctv_picks(date_yyyymmdd: str, *, max_stale_days: int = 3) -> tuple[dict, Optional[str]]:
    """读取 CCTV 股票池，返回 ({code: {...}}, 实际数据日期)。"""
    found = _find_strategy_csv("CCTV-Sector-Stock-Pool", date_yyyymmdd, max_stale_days=max_stale_days)
    if not found:
        return {}, None
    path, actual_date = found
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
    return picks, actual_date


def _load_momentum_picks(date_yyyymmdd: str, *, max_stale_days: int = 3) -> tuple[dict, Optional[str]]:
    """读取动量策略结果，返回 ({code: {...}}, 实际数据日期)。缺失则空（fail-soft）。"""
    found = _find_strategy_csv("Stock-Selection-Momentum", date_yyyymmdd, max_stale_days=max_stale_days)
    if not found:
        return {}, None
    path, actual_date = found
    picks = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            code = format_stock_code(row.get("股票代码", ""))
            if not code:
                continue
            picks[code] = {
                "name": (row.get("股票名称") or "").strip(),
                "momentum": to_float(row.get("动量分")),
            }
    return picks, actual_date


def _compute_boll_levels(code: str) -> dict:
    """拉前复权 K 线算 Boll 水位（止损=下轨，止盈=上轨）。"""
    end = date.today()
    start = end - timedelta(days=120)  # 120 天前
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
    trend_guard: bool = True,
    max_stale_days: int = 3,
) -> tuple[pd.DataFrame, str]:
    """融合四策略信号，输出今日操作清单。

    Args:
        date_yyyymmdd: 日期字符串 YYYYMMDD
        total_capital: 总资金（元），用于算每只票建议仓位金额
        max_picks: 最多输出几只票
        fetch_levels: 是否拉 K 线算止损止盈（批量调用网络较慢，可关闭只出清单）
        max_stale_days: 允许回退的历史策略文件最大天数（默认 3 天）

    Returns:
        (result_df, report_text)
    """
    boll, boll_date = _load_boll_picks(date_yyyymmdd, max_stale_days=max_stale_days)
    relativity, rel_date = _load_relativity_picks(date_yyyymmdd, max_stale_days=max_stale_days)
    theme, theme_date = _load_theme_picks(date_yyyymmdd, max_stale_days=max_stale_days)
    cctv, cctv_date = _load_cctv_picks(date_yyyymmdd, max_stale_days=max_stale_days)
    momentum, mom_date = _load_momentum_picks(date_yyyymmdd, max_stale_days=max_stale_days)

    source_dates = {
        "Boll": boll_date,
        "Relativity": rel_date,
        "Theme": theme_date,
        "CCTV": cctv_date,
        "Momentum": mom_date,
    }

    # 合并所有代码
    all_codes = set(boll) | set(relativity) | set(theme) | set(cctv) | set(momentum)
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
        if code in momentum:
            hit_strategies.append("Momentum")
            score += STRATEGY_BASE_SCORE["momentum"]
            name = momentum[code]["name"] or name

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
            "momentum": len(momentum),
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

        if fetch_levels:
            levels = _compute_boll_levels(code)
            if levels:
                row["最新价"] = levels.get("close")
                row["止损价(下轨)"] = levels.get("lower")
                row["止盈价(上轨)"] = levels.get("upper")
                row["MA20"] = levels.get("ma20")

        rows.append(row)

    df = pd.DataFrame(rows)
    # 趋势守卫：剔除价格远低于 MA20 的破位/下降通道股（自由落体风险）
    filtered_out = 0
    if trend_guard:
        before = len(df)
        mask = df.apply(
            lambda r: _passes_trend_guard(r.get("最新价"), r.get("MA20")),
            axis=1,
        )
        df = df[mask].reset_index(drop=True)
        filtered_out = before - len(df)
    df = df.sort_values("综合评分", ascending=False).reset_index(drop=True)
    df = df.head(max_picks)

    # 生成日报段落
    report = _build_report_text(
        df,
        date_yyyymmdd,
        len(boll),
        len(relativity),
        len(theme),
        len(cctv),
        len(momentum),
        source_dates=source_dates,
        max_stale_days=max_stale_days,
    )
    if filtered_out:
        report += f"\n- 🛡️ 趋势守卫剔除 {filtered_out} 只破位/下降通道股（价格低于 MA20 超 12%）"
    return df, report


def _build_report_text(
    df: pd.DataFrame,
    date_yyyymmdd: str,
    n_boll: int,
    n_relativity: int,
    n_theme: int,
    n_cctv: int,
    n_momentum: int = 0,
    *,
    source_dates: dict[str, Optional[str]] | None = None,
    max_stale_days: int = 3,
) -> str:
    """生成日报段落。"""
    if df.empty:
        stale_notes = _format_source_date_notes(date_yyyymmdd, source_dates or {}, max_stale_days=max_stale_days)
        if stale_notes:
            return "\n## 今日操作清单\n- 无候选\n" + stale_notes
        return "\n## 今日操作清单\n- 无候选"

    lines = [
        f"\n## 今日操作清单（{date_yyyymmdd}）",
        f"- 信号来源: Boll {n_boll} | Relativity {n_relativity} | Momentum {n_momentum} | Theme {n_theme} | CCTV {n_cctv}",
        f"- 融合后候选: {len(df)} 只（按综合评分排序）",
    ]
    stale_notes = _format_source_date_notes(date_yyyymmdd, source_dates or {}, max_stale_days=max_stale_days)
    if stale_notes:
        lines.append(stale_notes)
    lines.extend([
        "",
        "| 代码 | 名称 | 命中 | 评分 | 仓位% | 止损 | 止盈 |",
        "|------|------|------|------|-------|------|------|",
    ])
    for _, r in df.iterrows():
        stop = fmt_num(r.get("止损价(下轨)"), digits=2, na="-")
        take = fmt_num(r.get("止盈价(上轨)"), digits=2, na="-")
        lines.append(
            f"| {r['股票代码']} | {r['股票名称']} | {r['命中策略数']} | {r['综合评分']} | {r['建议仓位%']} | {stop} | {take} |"
        )
    lines.append("")
    lines.append("- 止损=Boll下轨，止盈=Boll上轨（前复权）；仓位为建议上限，单票不超过 30%。")
    return "\n".join(lines)


def _format_source_date_notes(
    date_yyyymmdd: str,
    source_dates: dict[str, Optional[str]],
    *,
    max_stale_days: int,
) -> str:
    """标注各策略实际使用的数据日期，过期数据显式警告。"""
    notes: list[str] = []
    for name, actual in source_dates.items():
        if not actual:
            notes.append(f"- ⚠️ {name}: 无可用数据（{max_stale_days} 天内未找到）")
            continue
        if actual != date_yyyymmdd:
            notes.append(f"- ⚠️ {name}: 使用 {actual} 的数据（非当日 {date_yyyymmdd}）")
    if not notes:
        return ""
    return "\n".join(["", "**数据日期说明**", *notes])


def save_action_list(df: pd.DataFrame, date_yyyymmdd: str) -> Optional[Path]:
    """保存操作清单 CSV，返回路径。"""
    if df.empty:
        return None
    path = STOCK_DATA_DIR / f"Daily-Action-List-{date_yyyymmdd}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path
