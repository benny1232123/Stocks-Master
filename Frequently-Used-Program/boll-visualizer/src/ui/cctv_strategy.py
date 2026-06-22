"""央视新闻板块监测 —— Streamlit UI 模块

抓取新闻联播文本 → 舆情情绪分析 → 热门板块识别 → 关联股票池
通过 importlib 延迟加载 Stock-Selection-CCTV-Sectors.py（文件名含连字符）。
"""

from __future__ import annotations

import datetime
import importlib.util
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# 路径与模块加载
# ---------------------------------------------------------------------------

# 本文件位于 boll-visualizer/src/ui/，往上两级即 Frequently-Used-Program/
_PARENT_DIR = Path(__file__).resolve().parents[3]  # ui/ → src/ → boll-visualizer/ → Frequently-Used-Program/


def _get_cctv_module():
    """延迟加载 CCTV 脚本模块（文件名含连字符，不能直接 import）。"""
    if not hasattr(_get_cctv_module, "_mod"):
        _spec = importlib.util.spec_from_file_location(
            "cctv_sectors",
            str(_PARENT_DIR / "Stock-Selection-CCTV-Sectors.py"),
        )
        if _spec is None or _spec.loader is None:
            raise ImportError("无法定位 Stock-Selection-CCTV-Sectors.py")
        _mod = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_mod)
        _get_cctv_module._mod = _mod
    return _get_cctv_module._mod


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------

def _safe_text(value: Any) -> str:
    """将任意值转为清洗后的字符串。"""
    return "" if value is None else str(value).strip()


def _sentiment_label(score: float) -> str:
    """根据舆论分返回直观标签。"""
    if score >= 3:
        return "🟢 正面"
    if score <= -3:
        return "🔴 负面"
    return "🟡 中性"


def _build_sentiment_highlight(text: str, pos_words: list[str], neg_words: list[str]) -> str:
    """为新闻文本生成带情绪标记的 HTML 片段（用于 expander 展示）。"""
    import html as _html
    safe = _html.escape(text)
    # 高亮正面词
    for w in pos_words:
        safe = safe.replace(_html.escape(w), f'<span style="color:#2ca02c;font-weight:600">{_html.escape(w)}</span>')
    # 高亮负面词
    for w in neg_words:
        safe = safe.replace(_html.escape(w), f'<span style="color:#d62728;font-weight:600">{_html.escape(w)}</span>')
    return safe


# ---------------------------------------------------------------------------
# 主渲染入口
# ---------------------------------------------------------------------------

def render_cctv_tab() -> None:
    """渲染央视新闻板块监测的完整 UI（内联布局，不使用 sidebar）。"""

    st.title("📺 央视新闻板块监测")
    st.caption("抓取新闻联播文本 → 舆情情绪分析 → 热门板块识别 → 关联股票池")

    # ── 检查 akshare 依赖 ──
    try:
        import akshare  # noqa: F401
    except ImportError:
        st.error(
            "缺少 akshare 依赖，请先安装：`pip install akshare`。\n\n"
            "akshare 用于获取央视新闻联播文本和 A 股行情数据。"
        )
        return

    # ── 参数区（内联） ──
    with st.container():
        col1, col2 = st.columns([1, 1])
        with col1:
            target_date = st.date_input(
                "日期选择",
                value=datetime.date.today(),
                key="cctv_target_date",
                help="选择要分析的新闻日期，默认今天。若当日无新闻会自动回退到前一天。",
            )
        with col2:
            top_n = st.slider(
                "Top N（展示板块数）",
                min_value=5,
                max_value=50,
                value=15,
                step=1,
                key="cctv_top_n",
            )

        col3, col4 = st.columns([1, 1])
        with col3:
            include_extra = st.checkbox(
                "包含补充资讯源",
                value=True,
                key="cctv_include_extra",
                help="同时抓取财联社、新浪等补充新闻源，扩展关键词覆盖面。",
            )
        with col4:
            show_sw_stocks = st.checkbox(
                "显示申万行业成分股",
                value=True,
                key="cctv_show_sw_stocks",
                help="将申万行业指数映射到对应板块，展示成分股股票池。",
            )

    # ── 执行按钮 ──
    st.markdown("---")
    run_clicked = st.button("开始分析", type="primary", use_container_width=True, key="cctv_run_btn")

    # ── 执行分析 ──
    if run_clicked:
        _run_analysis(
            target_date=target_date,
            top_n=top_n,
            include_extra=include_extra,
            show_sw_stocks=show_sw_stocks,
        )

    # ── 展示已缓存的结果 ──
    _render_results()


# ---------------------------------------------------------------------------
# 分析流程
# ---------------------------------------------------------------------------

def _run_analysis(
    *,
    target_date: datetime.date,
    top_n: int,
    include_extra: bool,
    show_sw_stocks: bool,
) -> None:
    """执行完整的央视新闻板块分析流程，结果写入 session_state。"""

    try:
        cctv = _get_cctv_module()
    except Exception as exc:
        st.error(f"加载 CCTV 脚本模块失败：{exc}")
        return

    date_str = target_date.strftime("%Y%m%d")
    progress = st.progress(0, text="正在抓取央视新闻...")

    # ── 步骤 1：抓取新闻 ──
    try:
        fetched_date, news_df, raw_count = cctv.fetch_cctv_news(date_str, fallback=True)
    except Exception as exc:
        st.error(f"抓取新闻失败：{exc}")
        return

    if news_df.empty:
        st.warning("未获取到可用新闻数据，请检查日期或网络连接。")
        return

    progress.progress(15, text=f"获取到 {len(news_df)} 条新闻，正在抓取补充资讯...")

    # ── 步骤 2：补充资讯源（可选） ──
    keyword_news_df = news_df
    extra_news_df = pd.DataFrame()
    if include_extra:
        try:
            extra_news_df, _extra_logs = cctv.fetch_extra_news_bundle(
                "cls,sina",
                per_source_limit=120,
                timeout_seconds=8,
            )
            if not extra_news_df.empty:
                keyword_news_df = pd.concat(
                    [news_df, extra_news_df], ignore_index=True, sort=False
                )
        except Exception:
            # 补充源失败不影响主流程
            pass

    progress.progress(30, text="正在构建板块关键词...")

    # ── 步骤 3：构建关键词 & 板块热度 ──
    try:
        sector_keywords, emerging_df = cctv._build_auto_sector_keywords(
            keyword_news_df, top_n=40
        )
    except Exception as exc:
        st.error(f"构建关键词失败：{exc}")
        return

    progress.progress(50, text="正在分析新闻情绪与板块匹配...")

    # ── 步骤 4：逐条情绪分析 + 板块匹配 ──
    try:
        sector_df, matched_df, match_stats = cctv.build_sector_heat(
            keyword_news_df, sector_keywords
        )
    except Exception as exc:
        st.error(f"板块热度分析失败：{exc}")
        return

    if sector_df.empty:
        st.warning("未匹配到任何板块关键词，可尝试扩展词库或更换日期。")
        return

    progress.progress(70, text="正在计算板块排名与股票池...")

    # ── 步骤 5：对每条新闻单独做情绪分析（用于详情展示） ──
    news_details = []
    for _, row in news_df.iterrows():
        text = cctv._get_news_text(row) if hasattr(cctv, "_get_news_text") else ""
        if not text:
            # 回退：拼接所有非空字段
            text = " ".join(_safe_text(v) for v in row.values if _safe_text(v))
        if not text:
            continue
        title = cctv._extract_title(row) if hasattr(cctv, "_extract_title") else text[:60]
        score, pos, neg, neutral, macro = cctv._sentiment_score(text)
        matches = cctv._match_sectors(text, sector_keywords)
        news_details.append({
            "title": title,
            "text": text,
            "score": score,
            "pos": pos,
            "neg": neg,
            "neutral": neutral,
            "macro": macro,
            "sectors": [s for s, _ in matches],
            "label": _sentiment_label(score),
        })

    progress.progress(85, text="正在构建申万行业股票池...")

    # ── 步骤 6：申万行业成分股映射（可选） ──
    stock_pool_df = pd.DataFrame()
    if show_sw_stocks:
        try:
            sector_df_enriched = cctv.enrich_with_prev_change(fetched_date or date_str, sector_df.copy())
            stock_pool_df = cctv.build_sector_stock_pool(
                fetched_date or date_str,
                sector_df_enriched,
                {},
                sector_keywords,
                use_sw_industry=True,
            )
        except Exception:
            # 股票池构建失败不阻塞主流程
            pass

    progress.progress(100, text="分析完成！")

    # ── 将结果写入 session_state ──
    st.session_state["cctv_result_date"] = fetched_date or date_str
    st.session_state["cctv_news_count_raw"] = raw_count
    st.session_state["cctv_news_count"] = len(news_df)
    st.session_state["cctv_extra_count"] = len(extra_news_df)
    st.session_state["cctv_sector_df"] = sector_df
    st.session_state["cctv_matched_df"] = matched_df
    st.session_state["cctv_news_details"] = news_details
    st.session_state["cctv_stock_pool_df"] = stock_pool_df
    st.session_state["cctv_emerging_df"] = emerging_df if emerging_df is not None else pd.DataFrame()
    st.session_state["cctv_match_stats"] = match_stats
    st.session_state["cctv_top_n"] = top_n
    st.session_state["cctv_ran"] = True


# ---------------------------------------------------------------------------
# 结果展示
# ---------------------------------------------------------------------------

def _render_results() -> None:
    """渲染已缓存在 session_state 中的分析结果。"""

    if not st.session_state.get("cctv_ran"):
        st.info("设置参数后点击「开始分析」以查看结果。")
        return

    date_str = st.session_state.get("cctv_result_date", "")
    sector_df: pd.DataFrame = st.session_state.get("cctv_sector_df", pd.DataFrame())
    news_details: list[dict] = st.session_state.get("cctv_news_details", [])
    stock_pool_df: pd.DataFrame = st.session_state.get("cctv_stock_pool_df", pd.DataFrame())
    emerging_df: pd.DataFrame = st.session_state.get("cctv_emerging_df", pd.DataFrame())
    raw_count = st.session_state.get("cctv_news_count_raw", 0)
    news_count = st.session_state.get("cctv_news_count", 0)
    extra_count = st.session_state.get("cctv_extra_count", 0)
    top_n = st.session_state.get("cctv_top_n", 15)

    if sector_df.empty:
        st.warning("无板块结果可展示。")
        return

    # ── 摘要指标 ──
    st.markdown("---")
    positive_count = sum(1 for d in news_details if d["score"] >= 3)
    negative_count = sum(1 for d in news_details if d["score"] <= -3)

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("日期", date_str)
    m2.metric("新闻总数", f"{raw_count}（去重后 {news_count}）")
    if extra_count > 0:
        m3.metric("补充资讯", f"{extra_count} 条")
    else:
        m3.metric("补充资讯", "未启用")
    m4.metric("正面新闻", positive_count)
    m5.metric("负面新闻", negative_count)

    # ── 板块排名表 ──
    st.subheader("🔥 热门板块排名")

    # 准备展示用的精简表
    display_cols_map = {
        "板块": "板块",
        "热度分": "综合得分",
        "正向词命中": "正面词频",
        "负向词命中": "负面词频",
        "提及次数": "相关新闻",
    }

    display_df = sector_df.head(top_n).copy()
    # 只保留存在的列
    available_cols = [c for c in display_cols_map if c in display_df.columns]
    display_df = display_df[available_cols].rename(columns=display_cols_map)

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
    )

    # ── CSV 下载 ──
    csv_data = sector_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "下载板块排名 CSV",
        data=csv_data,
        file_name=f"CCTV-Hot-Sectors-{date_str}.csv",
        mime="text/csv",
        use_container_width=True,
        key="cctv_download_sector_csv",
    )

    # ── 申万行业股票池 ──
    if not stock_pool_df.empty:
        st.subheader("📊 关联股票池（申万行业映射）")

        # 按板块分组展示
        pool_sectors = stock_pool_df["板块"].unique().tolist() if "板块" in stock_pool_df.columns else []
        if pool_sectors:
            selected_sector = st.selectbox(
                "选择板块查看成分股",
                options=pool_sectors,
                key="cctv_pool_sector_select",
            )
            sector_stocks = stock_pool_df[stock_pool_df["板块"] == selected_sector]
            pool_display_cols = ["股票代码", "股票名称", "热度分", "置信度", "匹配线索"]
            pool_display_cols = [c for c in pool_display_cols if c in sector_stocks.columns]
            st.dataframe(
                sector_stocks[pool_display_cols],
                use_container_width=True,
                hide_index=True,
            )
            st.caption(f"共 {len(sector_stocks)} 只股票")

            # 股票池 CSV 下载
            pool_csv = stock_pool_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "下载完整股票池 CSV",
                data=pool_csv,
                file_name=f"CCTV-Sector-Stock-Pool-{date_str}.csv",
                mime="text/csv",
                key="cctv_download_pool_csv",
            )

    # ── 新热点候选词 ──
    if not emerging_df.empty:
        with st.expander("🔍 新热点候选词", expanded=False):
            st.dataframe(
                emerging_df.head(20),
                use_container_width=True,
                hide_index=True,
            )

    # ── 新闻详情（情绪高亮） ──
    st.subheader("📰 新闻情绪详情")

    # 按情绪排序展示
    sorted_details = sorted(news_details, key=lambda d: d["score"], reverse=True)

    # 筛选器
    filter_option = st.radio(
        "情绪筛选",
        options=["全部", "仅正面", "仅负面", "仅命中板块"],
        horizontal=True,
        key="cctv_news_filter",
    )

    if filter_option == "仅正面":
        sorted_details = [d for d in sorted_details if d["score"] >= 3]
    elif filter_option == "仅负面":
        sorted_details = [d for d in sorted_details if d["score"] <= -3]
    elif filter_option == "仅命中板块":
        sorted_details = [d for d in sorted_details if d["sectors"]]

    # 限制展示数量，避免页面过长
    max_show = min(len(sorted_details), 50)
    if len(sorted_details) > max_show:
        st.caption(f"共 {len(sorted_details)} 条，仅展示前 {max_show} 条")

    for idx, detail in enumerate(sorted_details[:max_show]):
        # 构造标题行
        sector_tags = "、".join(detail["sectors"][:3]) if detail["sectors"] else "无板块命中"
        header = (
            f"**{detail['label']}** | "
            f"舆论分: {detail['score']:+.1f} | "
            f"正面: {detail['pos']} 负面: {detail['neg']} | "
            f"板块: {sector_tags}"
        )
        with st.expander(f"[{idx + 1}] {detail['title'][:80]}", expanded=False):
            st.markdown(header)
            # 情绪高亮文本
            try:
                cctv = _get_cctv_module()
                highlighted = _build_sentiment_highlight(
                    detail["text"][:500],
                    list(cctv.POSITIVE_WORDS),
                    list(cctv.NEGATIVE_WORDS),
                )
                st.markdown(highlighted, unsafe_allow_html=True)
            except Exception:
                st.text(detail["text"][:500])
