from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import streamlit as st

from core.backtester import backtest_boll_signals
from core.boll_strategy import analyze_stocks
from core.data_fetcher import clear_cache, fetch_all_a_share_codes, fetch_code_name_map, fetch_daily_k_data, get_cache_overview
from core.full_flow_strategy import analyze_stocks_full_flow
from core.indicators import calc_bollinger
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
    st.title("📈 Boll 全流程选股")
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
        force_refresh = st.checkbox(
            "强制刷新缓存（重新抓取）",
            value=False,
            help="勾选后忽略本地缓存，重新请求行情与筛选数据。",
        )

        with st.expander("缓存管理", expanded=False):
            overview = get_cache_overview()
            total_info = overview.get("all", {})
            st.caption(
                f"总缓存：{total_info.get('files', 0)} 个文件，{total_info.get('size_mb', 0)} MB"
            )

            k_info = overview.get("k_data", {})
            flow_info = overview.get("fund_flow", {})
            universe_info = overview.get("universe", {})
            st.caption(
                " | ".join(
                    [
                        f"K线 {k_info.get('files', 0)} 个/{k_info.get('size_mb', 0)} MB",
                        f"资金流 {flow_info.get('files', 0)} 个/{flow_info.get('size_mb', 0)} MB",
                        f"股票池 {universe_info.get('files', 0)} 个/{universe_info.get('size_mb', 0)} MB",
                    ]
                )
            )

            clear_scope_label = st.selectbox(
                "清理范围",
                options=["全部", "K线", "资金流", "股票池"],
                index=0,
            )
            older_days = int(
                st.number_input(
                    "仅清理早于天数（0 表示全部）",
                    min_value=0,
                    max_value=3650,
                    value=0,
                    step=1,
                )
            )
            if st.button("清理缓存", use_container_width=True):
                scope_map = {"全部": "all", "K线": "k_data", "资金流": "fund_flow", "股票池": "universe"}
                clear_result = clear_cache(
                    scope=scope_map[clear_scope_label],
                    older_than_days=None if older_days == 0 else older_days,
                )
                st.success(
                    f"已清理 {clear_result.get('deleted_files', 0)} 个文件，释放 {clear_result.get('deleted_mb', 0)} MB"
                )
                if int(clear_result.get("failed_files", 0)) > 0:
                    st.warning(f"有 {clear_result.get('failed_files', 0)} 个文件清理失败")
                st.rerun()

        price_upper_limit = float(DEFAULT_PRICE_UPPER_LIMIT)
        debt_asset_ratio_limit = float(DEFAULT_DEBT_ASSET_RATIO_LIMIT)
        exclude_gem_sci = bool(DEFAULT_EXCLUDE_GEM_SCI)
        max_workers = 4
        max_retries = 2
        retry_backoff_seconds = 0.5
        request_interval_seconds = 0.0
        boll_max_workers = 8
        default_fast_days = min(180, max(60, int(window) * 3))
        market_fast_mode = True
        market_fast_days = default_fast_days
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

            with st.expander("并发与重试（高级）", expanded=False):
                max_workers = int(
                    st.slider("并发评估线程数", min_value=1, max_value=12, value=4, step=1)
                )
                max_retries = int(
                    st.slider("网络重试次数", min_value=0, max_value=5, value=2, step=1)
                )
                retry_backoff_seconds = float(
                    st.number_input(
                        "重试基准退避(秒)",
                        min_value=0.0,
                        max_value=5.0,
                        value=0.5,
                        step=0.1,
                        format="%.1f",
                    )
                )
                request_interval_seconds = float(
                    st.number_input(
                        "请求限流间隔(秒)",
                        min_value=0.0,
                        max_value=1.0,
                        value=0.0,
                        step=0.01,
                        format="%.2f",
                    )
                )
        else:
            st.caption("仅Boll模式：直接对输入股票计算布林信号，跳过资金流/基本面/股东环节。")
            st.caption("可直接点击“全市场仅Boll分析”一键跑全A股（耗时较长）。")
            with st.expander("仅Boll性能优化", expanded=False):
                boll_max_workers = int(
                    st.slider(
                        "全市场并发线程数",
                        min_value=1,
                        max_value=16,
                        value=8,
                        step=1,
                    )
                )
                market_fast_mode = st.checkbox(
                    "全市场极速模式（仅近N日筛选）",
                    value=True,
                    help="只在全市场仅Boll分析生效。通常使用近60-180天即可计算最新布林信号。",
                )
                market_fast_days = int(
                    st.slider(
                        "极速模式回看天数",
                        min_value=max(30, int(window) + 5),
                        max_value=365,
                        value=default_fast_days,
                        step=5,
                    )
                )

        run_btn = st.button("开始分析", type="primary", use_container_width=True)
        run_market_boll_btn = False
        run_market_full_btn = False
        if full_flow_mode:
            run_market_full_btn = st.button("全市场全流程分析", use_container_width=True)
        else:
            run_market_boll_btn = st.button("全市场仅Boll分析", use_container_width=True)

    if run_btn or run_market_boll_btn or run_market_full_btn:
        progress_bar = st.progress(0)
        progress_text = st.empty()

        def on_progress(stage: str, done: int, total: int, message: str) -> None:
            safe_total = total if total > 0 else 1
            ratio = max(0.0, min(1.0, done / safe_total))

            if full_flow_mode:
                if stage == "init":
                    percent = 3
                    label = "初始化全流程..."
                elif stage == "fund_flow":
                    percent = 5 + int(ratio * 20)
                    label = f"资金流阶段 {done}/{total}"
                elif stage == "evaluate":
                    percent = 25 + int(ratio * 70)
                    label = f"逐股评估阶段 {done}/{total}"
                elif stage == "done":
                    percent = 100
                    label = "全流程完成"
                else:
                    percent = int(ratio * 100)
                    label = stage
            else:
                if stage == "init":
                    percent = 3
                    label = "初始化仅Boll..."
                elif stage == "evaluate":
                    percent = 5 + int(ratio * 90)
                    label = f"仅Boll阶段 {done}/{total}"
                elif stage == "done":
                    percent = 100
                    label = "仅Boll完成"
                else:
                    percent = int(ratio * 100)
                    label = stage

            progress_bar.progress(max(0, min(100, percent)))
            progress_text.caption(f"{label} | {message}")

        if start_date > end_date:
            st.warning("开始日期不能晚于结束日期。")
        else:
            run_scope = "custom"
            if run_market_boll_btn or run_market_full_btn:
                if run_market_boll_btn and full_flow_mode:
                    st.warning("全市场仅Boll按钮仅在“仅Boll”模式下可用。")
                    return
                if run_market_full_btn and not full_flow_mode:
                    st.warning("全市场全流程按钮仅在“全流程”模式下可用。")
                    return
                with st.spinner("正在获取全市场A股代码..."):
                    codes = fetch_all_a_share_codes(force_refresh=bool(force_refresh))
                if not codes:
                    st.error("未获取到全市场代码，请检查网络后重试。")
                    return
                progress_bar.progress(2)
                progress_text.caption(f"已获取全市场代码：{len(codes)}")
                run_scope = "market"
            else:
                codes = parse_codes_input(codes_text)
                if not codes:
                    st.warning("请输入至少一个有效的6位股票代码。")
                    return

            spinner_text = (
                "正在执行全市场全流程分析（耗时较长，建议保持网络稳定）..."
                if (full_flow_mode and run_scope == "market")
                else (
                "正在执行全流程筛选（同花顺/新浪/baostock）..."
                if full_flow_mode
                else (
                    "正在执行全市场仅Boll分析（耗时较长）..."
                    if run_scope == "market"
                    else "正在执行仅Boll分析（跳过前置筛选）..."
                )
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
                            force_refresh=bool(force_refresh),
                            max_workers=int(max_workers),
                            max_retries=int(max_retries),
                            retry_backoff_seconds=float(retry_backoff_seconds),
                            request_interval_seconds=float(request_interval_seconds),
                            progress_callback=on_progress,
                        )
                        if run_scope == "market" and not result_df.empty:
                            # 全市场全流程时只保留命中标的图表，降低会话内存占用。
                            hit_codes = set(
                                result_df[result_df["命中策略"].astype(bool)]["股票代码"].astype(str).tolist()
                            )
                            if hit_codes:
                                data_map = {
                                    str(code): chart_df
                                    for code, chart_df in data_map.items()
                                    if str(code) in hit_codes
                                }
                            else:
                                data_map = {}
                    else:
                        code_name_map = (
                            {}
                            if run_scope == "market"
                            else fetch_code_name_map(codes, force_refresh=bool(force_refresh))
                        )

                        analysis_start_date = start_date
                        effective_workers = 1
                        retain_all_charts = True
                        if run_scope == "market":
                            effective_workers = int(max(1, boll_max_workers))
                            retain_all_charts = False
                            if market_fast_mode:
                                fast_start_date = end_date - timedelta(days=max(1, int(market_fast_days) - 1))
                                analysis_start_date = max(start_date, fast_start_date)

                        result_df, data_map = analyze_stocks(
                            codes=codes,
                            start_date=analysis_start_date,
                            end_date=end_date,
                            window=window,
                            k=float(k),
                            near_ratio=float(near_ratio),
                            adjust=adjust,
                            code_name_map=code_name_map,
                            force_refresh=bool(force_refresh),
                            max_workers=effective_workers,
                            retain_all_charts=retain_all_charts,
                            progress_callback=on_progress,
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

            progress_bar.progress(100)
            progress_text.caption("分析已完成，可查看结果表和图表。")

            st.session_state["boll_result_df"] = result_df
            st.session_state["boll_data_map"] = data_map
            st.session_state["boll_window"] = window
            st.session_state["boll_k"] = float(k)
            st.session_state["boll_near_ratio"] = float(near_ratio)
            st.session_state["boll_start_date"] = start_date
            st.session_state["boll_end_date"] = end_date
            st.session_state["boll_adjust"] = adjust
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
    selected_near_ratio = st.session_state.get("boll_near_ratio", DEFAULT_NEAR_RATIO)

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
                                f"A档数量: {flow_stats.get('A档数量', 0)}",
                                f"平均评分: {flow_stats.get('平均评分', 0)}",
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

    if mode == "full":
        available_codes = [
            str(code)
            for code in result_df["股票代码"].tolist()
            if str(code) in data_map and not data_map[str(code)].empty
        ]
    else:
        available_codes = list(dict.fromkeys(result_df["股票代码"].astype(str).tolist()))

    if not available_codes:
        if mode == "full":
            st.warning("当前没有可绘制 Boll 图表的股票（可能尚未通过前置筛选）。")
        else:
            st.warning("当前没有可绘制 Boll 图表的股票（可能日期区间数据不足）。")
        return

    selected_code = st.selectbox("选择要查看图表的股票", options=available_codes)
    selected_row = result_df[result_df["股票代码"].astype(str) == selected_code].iloc[0]
    chart_df = data_map.get(selected_code, pd.DataFrame())

    if chart_df.empty and mode == "boll_only":
        with st.spinner("正在按需加载该股票图表数据..."):
            lazy_start_date = st.session_state.get("boll_start_date", default_start)
            lazy_end_date = st.session_state.get("boll_end_date", default_end)
            lazy_adjust = st.session_state.get("boll_adjust", DEFAULT_ADJUST)
            raw_df = fetch_daily_k_data(
                code=selected_code,
                start_date=lazy_start_date,
                end_date=lazy_end_date,
                adjust=str(lazy_adjust),
                use_cache=True,
                force_refresh=False,
            )
            if not raw_df.empty:
                chart_df = calc_bollinger(raw_df, window=int(selected_window), k=float(selected_k))
                data_map[selected_code] = chart_df
                st.session_state["boll_data_map"] = data_map

    if chart_df.empty:
        st.warning("该股票暂无可绘图数据。")
        return

    fig = build_bollinger_figure(
        chart_df,
        stock_code=selected_code,
        stock_name=str(selected_row.get("股票名称", "")),
        window=int(selected_window),
        k=float(selected_k),
        near_ratio=float(selected_near_ratio),
    )
    st.plotly_chart(fig, use_container_width=True)
    if mode == "full":
        st.caption(
            (
                f"最新信号：{selected_row.get('信号', '')} | "
                f"资金流通过：{'是' if bool(selected_row.get('资金流通过', False)) else '否'} | "
                f"重要股东通过：{'是' if bool(selected_row.get('重要股东通过', False)) else '否'} | "
                f"综合评分：{selected_row.get('综合评分', '-')}（{selected_row.get('评分等级', '-')}）"
            )
        )
    else:
        st.caption(f"最新信号：{selected_row.get('信号', '')} | 命中策略：{'是' if bool(selected_row.get('命中策略', False)) else '否'}")

    with st.expander("历史信号回测（样本内）", expanded=False):
        summary_df, details_df = backtest_boll_signals(
            chart_df,
            horizons=(5, 10, 20),
            near_ratio=float(selected_near_ratio),
        )

        if summary_df.empty:
            st.caption("当前样本区间内没有可回测的有效信号。")
        else:
            st.dataframe(summary_df, use_container_width=True, hide_index=True)
            st.caption("说明：该回测为样本内历史统计，不代表未来收益。")
            if not details_df.empty:
                st.dataframe(details_df.tail(30), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
