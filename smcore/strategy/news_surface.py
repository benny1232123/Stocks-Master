"""个股 / 日报「消息面」数据构建（smcore 多策略体系）。

数据来源（均为 CCTV 舆情策略产物，部署于 Render 的仓库内已跟踪）：
- CCTV-Hot-Sectors-{date}.csv      板块热度 + 舆论分（已跟踪）
- CCTV-Sector-News-Matched-{date}.csv  新闻条目（标题/片段/板块/舆论分/情感词命中）（需跟踪）
- CCTV-Sector-Stock-Pool-{date}.csv  板块 -> 个股池（已跟踪，用于 个股 -> 板块 反查）

输出供前端两处消费：
- 日报：build_news_surface() 返回全市场舆情（热门板块 + 新闻流）
- 个股：build_news_surface(code) 额外按该股票所属热门板块过滤相关新闻
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from smcore.config.defaults import STOCK_DATA_DIR

# 舆论分阈值（同前端 A股 涨红跌绿约定：偏多=红 / 偏空=绿 / 中性=蓝）。
# 打分端已去掉中性词/宏观词的负权重（纯 正向-负向 加权净分），单条新闻的典型得分
# 是 0/±1/±2，故阈值从 ±3.0 收紧到 ±2.0，避免真实偏多新闻被标成中性。
BULL_THRESHOLD = 2.0
BEAR_THRESHOLD = -2.0


def _num(v):
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        f = float(v)
        return None if pd.isna(f) else f
    except (TypeError, ValueError):
        return None


def _safe(v):
    if v is None:
        return None
    t = str(v).strip()
    return t if t and t.lower() != "nan" else None


def _latest_file(prefix: str) -> Path | None:
    files = sorted(STOCK_DATA_DIR.glob(f"{prefix}-*.csv"), reverse=True)
    return files[0] if files else None


def _resolve_file(prefix: str, as_of: str | None) -> Path | None:
    """返回指定日期的 CCTV 文件（若存在），否则回退到「不晚于 as_of 的最新文件」。

    护栏核心：新闻日期永远 ≤ 选股日（as_of），绝不展示领先选股日的新闻；
    同时若 as_of 当天文件缺失，回退到最近一个 ≤ as_of 的文件，避免面板空白。
    """
    if as_of:
        cand = STOCK_DATA_DIR / f"{prefix}-{as_of}.csv"
        if cand.exists():
            return cand
        # 回退到不晚于 as_of 的最新文件（按日期降序扫描，命中首个 ≤ as_of 的）
        for p in sorted(STOCK_DATA_DIR.glob(f"{prefix}-*.csv"), reverse=True):
            m = re.search(r"(\d{8})", p.name)
            if m and m.group(1) <= as_of:
                return p
        return None
    return _latest_file(prefix)


def _read_csv(path: Path | None):
    if path is None:
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
        return df if not df.empty else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _sentiment_label(score: float | None) -> str:
    if score is None:
        return "neutral"
    if score >= BULL_THRESHOLD:
        return "bull"
    if score <= BEAR_THRESHOLD:
        return "bear"
    return "neutral"


def _read_hot_sectors(max_sectors: int = 12, as_of: str | None = None) -> tuple[list[dict], str]:
    """返回 (热门板块列表, 数据日期)。as_of 给定时优先读该日 CCTV 文件。"""
    path = _resolve_file("CCTV-Hot-Sectors", as_of)
    if path is None:
        return [], ""
    df = _read_csv(path)
    out: list[dict] = []
    if not df.empty and "板块" in df.columns:
        for _, r in df.head(max_sectors).iterrows():
            out.append({
                "sector": _safe(r.get("板块")) or "",
                "heat": _num(r.get("热度分")),
                "sentiment": _num(r.get("舆论分")),
                "mentions": _num(r.get("提及次数")),
                "change": _safe(r.get("较上一期热度变化")),
            })
    m = re.search(r"(\d{8})", path.name)
    return out, (m.group(1) if m else "")


def _stock_sectors(code: str, as_of: str | None = None) -> list[str]:
    """由最新个股池反查该股票所属热门板块。as_of 给定时优先读该日个股池。"""
    if not code:
        return []
    path = _resolve_file("CCTV-Sector-Stock-Pool", as_of)
    df = _read_csv(path)
    if df.empty or "股票代码" not in df.columns or "板块" not in df.columns:
        return []
    cd = str(code).zfill(6)
    sub = df[df["股票代码"].astype(str).str.zfill(6) == cd]
    return sorted({str(s).strip() for s in sub["板块"].tolist() if str(s).strip()})


def _read_news_items(sectors: list[str] | None, max_news: int = 12, as_of: str | None = None) -> tuple[list[dict], str]:
    """返回 (新闻条目列表, 数据日期)。

    sectors 不为空时只取命中这些板块的新闻（个股视角）；否则返回全市场最新新闻流。
    as_of 给定时优先读该日新闻文件。
    """
    path = _resolve_file("CCTV-Sector-News-Matched", as_of)
    if path is None:
        return [], ""
    df = _read_csv(path)
    m = re.search(r"(\d{8})", path.name)
    date_tag = m.group(1) if m else ""
    if df.empty or "标题" not in df.columns or "板块" not in df.columns:
        return [], date_tag

    # 按标题去重：同一新闻命中多个板块时只保留一条，避免 UI 重复刷屏。
    # 新闻片段由生成端保证为「整段新闻的总结陈述」（完整句，不含标题前缀），原样透传；
    # 仅对旧格式数据（无句号边界的硬切片段）兜底截到 120 字。
    seen: dict[str, dict] = {}
    for _, r in df.iterrows():
        sector = _safe(r.get("板块")) or ""
        if sectors and sector not in sectors:
            continue
        s = _num(r.get("舆论分"))
        title = _safe(r.get("标题")) or "(无标题)"
        preview = _safe(r.get("新闻片段")) or ""
        if preview and not preview.endswith(("。", "！", "？", "”", "…")):
            preview = preview[:300]
        rec = {
            "title": title,
            "preview": preview,
            "sector": sector,
            "sentiment": s,
            "sentiment_label": _sentiment_label(s),
            "pos": _num(r.get("正向词命中")) or 0,
            "neg": _num(r.get("负向词命中")) or 0,
            "keywords": [k for k in str(r.get("命中关键词", "") or "").split("|") if k],
        }
        existing = seen.get(title)
        if existing is None or abs(s or 0) > abs(existing.get("sentiment") or 0):
            seen[title] = rec
    recs = list(seen.values())
    recs.sort(key=lambda x: abs(x["sentiment"] or 0), reverse=True)
    return recs[:max_news], date_tag


def _latest_selection_date() -> str | None:
    """最新选股日（Daily-Action-List 最新文件日期），作为新闻面板的对齐基准。"""
    try:
        from smcore.artifacts import find_latest_file
    except Exception:
        return None
    latest = find_latest_file("Daily-Action-List-*.csv")
    if latest is None:
        return None
    m = re.search(r"(\d{8})", latest.name)
    return m.group(1) if m else None


def build_news_surface(code: str | None = None, max_news: int = 12, max_sectors: int = 12, as_of_date: str | None = None) -> dict:
    """构建消息面载荷。

    - code=None: 全市场舆情（热门板块 + 全量新闻流）
    - code 给定: 额外反查该股票所属热门板块，并据板块过滤相关新闻
    - as_of_date: 强制对齐到指定选股日；缺省时自动取最新选股日
      （护栏：新闻日期始终与选股面板一致，避免周末/补跑导致两面板日期漂移）。
    """
    if as_of_date is None:
        as_of_date = _latest_selection_date()
    hot, hot_date = _read_hot_sectors(max_sectors, as_of_date)
    stock_sectors = _stock_sectors(code, as_of_date) if code else []
    # 个股视角：仅当其确实出现在热门板块池时才过滤相关新闻；
    # 否则不展示无关的全市场新闻（前端改用市场热点板块兜底）。
    if code and not stock_sectors:
        items, item_date = [], ""
    else:
        items, item_date = _read_news_items(stock_sectors or None, max_news, as_of_date)

    # 个股关联板块的热度分（从热门板块表中取，带情绪/变化）
    rel_sectors = []
    if stock_sectors:
        hot_map = {h["sector"]: h for h in hot}
        for sec in stock_sectors:
            if sec in hot_map:
                rel_sectors.append(hot_map[sec])
            else:
                rel_sectors.append({"sector": sec, "heat": None, "sentiment": None, "mentions": None, "change": None})

    date_tag = item_date or hot_date
    return {
        "date": date_tag,
        "hot_sectors": hot,
        "stock_sectors": rel_sectors,
        "news_items": items,
        "has_data": bool(hot or items),
        "in_pool": bool(stock_sectors),
    }
