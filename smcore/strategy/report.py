"""操作清单的落盘与日报文本生成。

从 fusion.py 抽出的「报告段落 + CSV 落盘」职责，便于单测文本组装逻辑。
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from smcore.config.defaults import STOCK_DATA_DIR
from smcore.utils.format import fmt_num

# 操作清单 CSV 的标准列（与正常 Daily-Action-List 完全一致；空清单占位时也用此表头，
# 保证下游统一 read_csv 读回仍是空 DataFrame，接口零兼容风险）。
ACTION_LIST_COLUMNS = [
    "股票代码", "股票名称", "命中策略数", "来源策略", "综合评分", "权重",
    "建议买入价", "建议仓位%", "建议金额", "最新价", "止损价(下轨)",
    "止盈价(上轨)", "MA20", "stop_pct",
]


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
    max_single_weight_pct: float = 10.0,
) -> str:
    """生成日报段落。"""
    # ── 策略贡献度汇总（显眼置顶，一眼看出哪几个策略在出力）──
    strat_raw = {
        "Boll": n_boll,
        "Relativity": n_relativity,
        "Theme": n_theme,
        "CCTV": n_cctv,
        "Momentum": n_momentum,
    }
    sd = source_dates or {}
    contrib_lines = []
    for name, cnt in strat_raw.items():
        actual = sd.get(name)
        if actual is None:
            status = "❌ 缺失（未找到文件）"
        elif cnt == 0:
            status = f"⚪ 产出=0（{actual} 数据为空）"
        elif actual != date_yyyymmdd:
            status = f"✅ {cnt} 只（{actual}，回退{max_stale_days}天内）"
        else:
            status = f"✅ {cnt} 只"
        contrib_lines.append(f"- {name}: {status}")
    active_count = sum(1 for c in strat_raw.values() if c > 0)

    if df.empty:
        stale_notes = _format_source_date_notes(date_yyyymmdd, sd, max_stale_days=max_stale_days)
        header = "\n## 今日操作清单\n- 无候选"
        summary = "\n### 策略贡献度\n" + "\n".join(contrib_lines) + (
            f"\n> 📊 仅 {active_count}/5 个策略有输出，清单可能不完整。" if active_count < 3 else ""
        )
        return header + ("\n" + stale_notes if stale_notes else "") + summary

    lines = [
        f"\n## 今日操作清单（{date_yyyymmdd}）",
        f"- 融合后候选: {len(df)} 只（按综合评分排序）",
        "",
        "### 策略贡献度",
        *contrib_lines,
        "" if active_count >= 3 else f"> ⚠️ 仅 {active_count}/5 个策略有输出，回测/决策参考价值有限。",
        "",
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
    lines.append(f"- 止损=Boll下轨，止盈=Boll上轨（前复权）；仓位为建议上限，单票不超过 {max_single_weight_pct:.0f}%。")
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


def save_action_list(
    df: pd.DataFrame,
    date_yyyymmdd: str,
    *,
    placeholder_when_empty: bool = False,
) -> Optional[Path]:
    """保存操作清单 CSV，返回路径。

    placeholder_when_empty=True 时，即使候选为空也写一份「仅表头」占位 CSV：
    下游 read_csv 读回仍是 empty DataFrame（接口零兼容风险），避免无候选日看起来像
    「漏跑」，提升可追溯性。占位行为由配置 action_list.placeholder_when_empty 控制。
    """
    if df.empty:
        if not placeholder_when_empty:
            return None
        cols = list(df.columns) if len(df.columns) else ACTION_LIST_COLUMNS
        path = STOCK_DATA_DIR / f"Daily-Action-List-{date_yyyymmdd}.csv"
        pd.DataFrame(columns=cols).to_csv(path, index=False, encoding="utf-8-sig")
        return path
    path = STOCK_DATA_DIR / f"Daily-Action-List-{date_yyyymmdd}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def save_action_report(date_yyyymmdd: str, text: str) -> Optional[Path]:
    """把日报叙述文本（含「无候选」原因 / 策略贡献度）落盘为同目录 .md。

    即便候选为空也落盘，便于事后追溯「为什么今天没有操作清单」。文本为空则不写。
    """
    if not text:
        return None
    path = STOCK_DATA_DIR / f"Daily-Action-List-{date_yyyymmdd}.md"
    try:
        path.write_text(text, encoding="utf-8")
    except Exception:
        return None
    return path
