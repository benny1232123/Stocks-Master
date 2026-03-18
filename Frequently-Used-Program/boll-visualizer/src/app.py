from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import streamlit as st

from core.boll_strategy import analyze_stocks
from core.data_fetcher import fetch_all_a_share_codes, fetch_code_name_map
from core.full_flow_strategy import analyze_stocks_full_flow
from ui.charts import build_bollinger_figure
from ui.dashboard import parse_codes_input, render_overview_metrics, to_export_csv_bytes
from utils.config import (
    DEFAULT_ADJUST,
    DEFAULT_DAYS_BACK,
    DEFAULT_DEBT_ASSET_RATIO_LIMIT,
    DEFAULT_EXCLUDE_GEM_SCI,
    DEFAULT_K,
    DEFAULT_NEAR_RATIO,
    DEFAULT_PRICE_UPPER_LIMIT,
    DEFAULT_WINDOW,
)
from utils.logger import get_logger

logger = get_logger()


def main() -> None:
    st.set_page_config(page_title="Boll 可视化选股", layout="wide")
    st.title("📈 Boll 全流程选股（无东方财富接口）")
    st.caption("流程：同花顺资金流 → baostock 基本面 → 新浪流通股东 → Boll 信号。")

    default_end = date.today()
    default_start = default_end - timedelta(days=DEFAULT_DAYS_BACK)

    with st.sidebar:
        st.subheader("参数设置")
        analysis_mode = st.radio(
            "运行模式",
            options=["全流程（Selection Boll）", "仅Boll（跳过资金流/基本面/股东）"],
            index=0,
        )
        full_flow_mode = analysis_mode.startswith("全流程")

        codes_text = st.text_area(
            "股票代码（支持逗号、空格、换行）",
            value="600519, 000001, 601318",
            height=120,
        )
        start_date = st.date_input("开始日期", value=default_start)
        end_date = st.date_input("结束日期", value=default_end)
        window = st.slider("均线窗口", min_value=10, max_value=60, value=DEFAULT_WINDOW, step=1)
        k = st.number_input("标准差倍数 k", min_value=0.8, max_value=3.0, value=float(DEFAULT_K), step=0.01)
        near_ratio = st.number_input(
            "接近下轨阈值（倍数）",
            min_value=1.0,
            max_value=1.05,
            value=float(DEFAULT_NEAR_RATIO),
            step=0.001,
            format="%.3f",
        )
        adjust = st.selectbox(
            "复权方式",
            options=["qfq", "hfq"],
            index=0 if DEFAULT_ADJUST == "qfq" else 1,
            help="qfq=前复权，hfq=后复权",
        )

        price_upper_limit = float(DEFAULT_PRICE_UPPER_LIMIT)
        debt_asset_ratio_limit = float(DEFAULT_DEBT_ASSET_RATIO_LIMIT)
        exclude_gem_sci = bool(DEFAULT_EXCLUDE_GEM_SCI)
        if full_flow_mode:
            price_upper_limit = st.number_input(
                "股价上限",
                min_value=1.0,
                max_value=300.0,
                value=float(DEFAULT_PRICE_UPPER_LIMIT),
                step=1.0,
            )
            debt_asset_ratio_limit = st.number_input(
                "资产负债率上限(%)",
                min_value=10.0,
                max_value=100.0,
                value=float(DEFAULT_DEBT_ASSET_RATIO_LIMIT),
                step=1.0,
            )
            exclude_gem_sci = st.checkbox("排除创业板(30*)和科创板(688*)", value=DEFAULT_EXCLUDE_GEM_SCI)
        else:
            st.caption("仅Boll模式：直接对输入股票计算布林信号，跳过资金流/基本面/股东环节。")
            st.caption("可直接点击“全市场仅Boll分析”一键跑全A股（耗时较长）。")

        run_btn = st.button("开始分析", type="primary", use_container_width=True)
        run_market_boll_btn = False
        if not full_flow_mode:
            run_market_boll_btn = st.button("全市场仅Boll分析", use_container_width=True)

    if run_btn or run_market_boll_btn:
        if start_date > end_date:
            st.warning("开始日期不能晚于结束日期。")
        else:
            run_scope = "custom"
            if run_market_boll_btn:
                if full_flow_mode:
                    st.warning("全市场按钮仅在“仅Boll”模式下可用。")
                    return
                with st.spinner("正在获取全市场A股代码..."):
                    codes = fetch_all_a_share_codes()
                if not codes:
                    st.error("未获取到全市场代码，请检查网络后重试。")
                    return
                run_scope = "market"
            else:
                codes = parse_codes_input(codes_text)
                if not codes:
                    st.warning("请输入至少一个有效的6位股票代码。")
                    return

            spinner_text = (
                "正在执行全流程筛选（同花顺/新浪/baostock）..."
                if full_flow_mode
                else (
                    "正在执行全市场仅Boll分析（耗时较长）..."
                    if run_scope == "market"
                    else "正在执行仅Boll分析（跳过前置筛选）..."
                )
            )
            with st.spinner(spinner_text):
                try:
                    if full_flow_mode:
                        result_df, data_map, flow_stats = analyze_stocks_full_flow(
                            codes=codes,
                            start_date=start_date,
                            end_date=end_date,
                            window=window,
                            k=float(k),
                            near_ratio=float(near_ratio),
                            adjust=adjust,
                            price_upper_limit=float(price_upper_limit),
                            debt_asset_ratio_limit=float(debt_asset_ratio_limit),
                            exclude_gem_sci=bool(exclude_gem_sci),
                        )
                    else:
                        code_name_map = {} if run_scope == "market" else fetch_code_name_map(codes)
                        result_df, data_map = analyze_stocks(
                            codes=codes,
                            start_date=start_date,
                            end_date=end_date,
                            window=window,
                            k=float(k),
                            near_ratio=float(near_ratio),
                            adjust=adjust,
                            code_name_map=code_name_map,
                        )
                        flow_stats = {
                            "输入代码数": len(codes),
                            "Boll命中": int(result_df["命中策略"].sum())
                            if (not result_df.empty and "命中策略" in result_df.columns)
                            else 0,
                        }
                except Exception as error:
                    logger.exception("全流程分析失败: %s", error)
                    st.error(f"全流程分析失败：{error}")
                    return

            st.session_state["boll_result_df"] = result_df
            st.session_state["boll_data_map"] = data_map
            st.session_state["boll_window"] = window
            st.session_state["boll_k"] = float(k)
            st.session_state["boll_flow_stats"] = flow_stats
            st.session_state["boll_mode"] = "full" if full_flow_mode else "boll_only"
            st.session_state["boll_scope"] = run_scope

            logger.info("全流程分析完成: count=%s", len(result_df))

    result_df: pd.DataFrame = st.session_state.get("boll_result_df", pd.DataFrame())
    data_map: dict[str, pd.DataFrame] = st.session_state.get("boll_data_map", {})
    flow_stats: dict[str, int] = st.session_state.get("boll_flow_stats", {})
    mode: str = st.session_state.get("boll_mode", "full")
    scope: str = st.session_state.get("boll_scope", "custom")
    selected_window = st.session_state.get("boll_window", DEFAULT_WINDOW)
    selected_k = st.session_state.get("boll_k", DEFAULT_K)

    if result_df.empty:
        st.info("在左侧设置参数后点击“开始分析”。")
        return

    render_overview_metrics(result_df)
    if flow_stats:
        if mode == "full":
            st.caption(
                " → ".join(
                    [
                        f"输入{flow_stats.get('输入代码数', 0)}",
                        f"板块后{flow_stats.get('板块过滤后', 0)}",
                        f"资金流{flow_stats.get('资金流通过', 0)}",
                        f"基本面{flow_stats.get('基本面通过', 0)}",
                        f"前置汇合{flow_stats.get('前置汇合通过', 0)}",
                        f"股东{flow_stats.get('股东通过', 0)}",
                        f"Boll命中{flow_stats.get('Boll命中', 0)}",
                    ]
                )
            )
            with st.expander("Selection Boll 全流程分步统计", expanded=False):
                st.caption(
                    " | ".join(
                        [
                            f"3日资金命中: {flow_stats.get('3日资金命中', 0)}",
                            f"5日资金命中: {flow_stats.get('5日资金命中', 0)}",
                            f"10日资金命中: {flow_stats.get('10日资金命中', 0)}",
                        ]
                    )
                )
            if flow_stats.get("资金流通过", 0) == 0:
                st.warning(
                    "当前卡在资金流过滤。可尝试：提高‘股价上限’（如 300）、扩大股票池，或换一个交易日重试。"
                )
        else:
            st.caption(
                (
                    f"{'全市场' if scope == 'market' else '仅Boll'}模式：输入{flow_stats.get('输入代码数', 0)}，"
                    f"跳过资金流/基本面/股东，Boll命中{flow_stats.get('Boll命中', 0)}"
                )
            )
    st.dataframe(result_df, use_container_width=True, hide_index=True)

    file_name = f"Stock-Selection-Boll-{date.today().strftime('%Y%m%d')}.csv"
    st.download_button(
        "下载筛选结果 CSV",
        data=to_export_csv_bytes(result_df),
        file_name=file_name,
        mime="text/csv",
        use_container_width=True,
    )

    available_codes = [
        str(code)
        for code in result_df["股票代码"].tolist()
        if str(code) in data_map and not data_map[str(code)].empty
    ]
    if not available_codes:
        if mode == "full":
            st.warning("当前没有可绘制 Boll 图表的股票（可能尚未通过前置筛选）。")
        else:
            st.warning("当前没有可绘制 Boll 图表的股票（可能日期区间数据不足）。")
        return

    selected_code = st.selectbox("选择要查看图表的股票", options=available_codes)
    selected_row = result_df[result_df["股票代码"].astype(str) == selected_code].iloc[0]
    chart_df = data_map.get(selected_code, pd.DataFrame())

    if chart_df.empty:
        st.warning("该股票暂无可绘图数据。")
        return

    fig = build_bollinger_figure(
        chart_df,
        stock_code=selected_code,
        stock_name=str(selected_row.get("股票名称", "")),
        window=int(selected_window),
        k=float(selected_k),
    )
    st.plotly_chart(fig, use_container_width=True)
    if mode == "full":
        st.caption(
            (
                f"最新信号：{selected_row.get('信号', '')} | "
                f"资金流通过：{'是' if bool(selected_row.get('资金流通过', False)) else '否'} | "
                f"重要股东通过：{'是' if bool(selected_row.get('重要股东通过', False)) else '否'}"
            )
        )
    else:
        st.caption(f"最新信号：{selected_row.get('信号', '')} | 命中策略：{'是' if bool(selected_row.get('命中策略', False)) else '否'}")


if __name__ == "__main__":
    main()
