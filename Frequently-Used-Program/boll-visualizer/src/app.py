from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import pandas as pd
import streamlit as st

from core.backtester import backtest_boll_signals
from core.boll_strategy import analyze_stocks
from core.data_fetcher import clear_cache, fetch_code_name_map, fetch_daily_k_data, get_cache_overview
from core.full_flow_strategy import analyze_stocks_full_flow
from core.indicators import calc_bollinger
from core.task_manager import (
    get_task,
    list_tasks,
    load_task_result_dataframe,
    submit_market_analysis_task,
)
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
from utils.presets import (
    ALLOWED_KEYS,
    delete_parameter_preset,
    load_parameter_presets,
    parse_date_value,
    upsert_parameter_preset,
)

logger = get_logger()

ANALYSIS_MODES = ["全流程（Selection Boll）", "仅Boll（跳过资金流/基本面/股东）"]
INT_KEYS = {"window", "max_workers", "max_retries", "boll_max_workers", "market_fast_days"}
FLOAT_KEYS = {
    "k",
    "near_ratio",
    "price_upper_limit",
    "debt_asset_ratio_limit",
    "retry_backoff_seconds",
    "request_interval_seconds",
}
BOOL_KEYS = {"force_refresh", "exclude_gem_sci", "market_fast_mode"}


def _build_default_ui_values(default_start: date, default_end: date) -> dict[str, Any]:
    default_fast_days = min(180, max(60, int(DEFAULT_WINDOW) * 3))
    return {
        "analysis_mode": ANALYSIS_MODES[0],
        "codes_text": "600519, 000001, 601318",
        "start_date": default_start,
        "end_date": default_end,
        "window": int(DEFAULT_WINDOW),
        "k": float(DEFAULT_K),
        "near_ratio": float(DEFAULT_NEAR_RATIO),
        "adjust": DEFAULT_ADJUST,
        "force_refresh": False,
        "price_upper_limit": float(DEFAULT_PRICE_UPPER_LIMIT),
        "debt_asset_ratio_limit": float(DEFAULT_DEBT_ASSET_RATIO_LIMIT),
        "exclude_gem_sci": bool(DEFAULT_EXCLUDE_GEM_SCI),
        "max_workers": 8,
        "max_retries": 2,
        "retry_backoff_seconds": 0.5,
        "request_interval_seconds": 0.0,
        "boll_max_workers": 8,
        "market_fast_mode": True,
        "market_fast_days": default_fast_days,
    }


def _ensure_ui_state(defaults: dict[str, Any]) -> None:
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def _collect_current_ui_values(defaults: dict[str, Any]) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for key in ALLOWED_KEYS:
        values[key] = st.session_state.get(key, defaults.get(key))
    return values


def _apply_preset_to_session(values: dict[str, Any], defaults: dict[str, Any]) -> None:
    for key in ALLOWED_KEYS:
        if key not in values:
            continue

        raw_value = values.get(key)
        fallback = defaults.get(key)

        if key in {"start_date", "end_date"}:
            st.session_state[key] = parse_date_value(raw_value, fallback=fallback)
            continue

        if key in INT_KEYS:
            try:
                st.session_state[key] = int(raw_value)
            except Exception:
                st.session_state[key] = int(fallback)
            continue

        if key in FLOAT_KEYS:
            try:
                st.session_state[key] = float(raw_value)
            except Exception:
                st.session_state[key] = float(fallback)
            continue

        if key in BOOL_KEYS:
            st.session_state[key] = bool(raw_value)
            continue

        if key == "analysis_mode":
            mode_value = str(raw_value)
            st.session_state[key] = mode_value if mode_value in ANALYSIS_MODES else defaults["analysis_mode"]
            continue

        st.session_state[key] = raw_value if raw_value is not None else fallback


def _render_preset_panel(defaults: dict[str, Any]) -> None:
    with st.expander("参数预设", expanded=False):
        presets = load_parameter_presets()
        preset_names = sorted(presets.keys())

        selected_name = st.selectbox(
            "已保存预设",
            options=["", *preset_names],
            format_func=lambda x: "请选择预设" if x == "" else x,
            key="preset_selected_name",
        )

        save_name = st.text_input(
            "保存为预设名",
            key="preset_save_name",
            placeholder="例如：全市场极速",
        )

        col1, col2, col3 = st.columns(3)
        if col1.button("加载", use_container_width=True, disabled=(selected_name == "")):
            _apply_preset_to_session(presets.get(selected_name, {}), defaults)
            st.success(f"已加载预设：{selected_name}")
            st.rerun()

        if col2.button("保存当前", use_container_width=True):
            target_name = save_name.strip() or selected_name.strip()
            if not target_name:
                st.warning("请先输入预设名")
            else:
                upsert_parameter_preset(target_name, _collect_current_ui_values(defaults))
                st.success(f"已保存预设：{target_name}")
                st.rerun()

        if col3.button("删除", use_container_width=True, disabled=(selected_name == "")):
            deleted = delete_parameter_preset(selected_name)
            if deleted:
                st.success(f"已删除预设：{selected_name}")
            else:
                st.warning("预设不存在或删除失败")
            st.rerun()


def _load_task_result_into_session(task: dict[str, Any], default_start: date, default_end: date) -> tuple[bool, str]:
    task_id = str(task.get("task_id", "")).strip()
    if not task_id:
        return False, "任务ID无效"

    result_df = load_task_result_dataframe(task_id)
    if result_df.empty:
        return False, "任务结果为空或结果文件不可读取"

    params = task.get("params", {}) if isinstance(task.get("params"), dict) else {}
    mode = "full" if str(task.get("mode", "full")) == "full" else "boll_only"

    st.session_state["boll_result_df"] = result_df
    st.session_state["boll_data_map"] = {}
    st.session_state["boll_window"] = int(float(params.get("window", DEFAULT_WINDOW)))
    st.session_state["boll_k"] = float(params.get("k", DEFAULT_K))
    st.session_state["boll_near_ratio"] = float(params.get("near_ratio", DEFAULT_NEAR_RATIO))
    st.session_state["boll_start_date"] = parse_date_value(params.get("start_date"), default_start)
    st.session_state["boll_end_date"] = parse_date_value(params.get("end_date"), default_end)
    st.session_state["boll_adjust"] = str(params.get("adjust", DEFAULT_ADJUST))
    st.session_state["boll_flow_stats"] = task.get("flow_stats", {}) if isinstance(task.get("flow_stats"), dict) else {}
    st.session_state["boll_mode"] = mode
    st.session_state["boll_scope"] = str(task.get("scope", "market")) or "market"
    return True, f"已加载任务 {task_id} 结果"


def _render_task_center(default_start: date, default_end: date) -> None:
    with st.expander("后台任务中心（全市场异步任务）", expanded=False):
        col_left, col_right = st.columns([3, 1])
        col_left.caption("这里会显示全市场任务进度与历史结果。")
        if col_right.button("刷新任务", use_container_width=True):
            st.rerun()

        tasks = list_tasks(limit=30)
        if not tasks:
            st.caption("暂无任务，点击左侧“全市场全流程分析”或“全市场仅Boll分析”后会出现在这里。")
            return

        if "task_selected_id" not in st.session_state:
            st.session_state["task_selected_id"] = str(tasks[0].get("task_id", ""))

        options = [str(task.get("task_id", "")) for task in tasks if str(task.get("task_id", ""))]
        if not options:
            st.caption("暂无可展示任务。")
            return

        if st.session_state.get("task_selected_id") not in options:
            st.session_state["task_selected_id"] = options[0]

        selected_task_id = st.selectbox(
            "任务列表",
            options=options,
            key="task_selected_id",
            format_func=lambda task_id: next(
                (
                    f"{task_id} | {item.get('title', '')} | {item.get('status', '')} | {item.get('created_at', '')}"
                    for item in tasks
                    if str(item.get("task_id", "")) == task_id
                ),
                task_id,
            ),
        )

        task = get_task(selected_task_id)
        if not task:
            st.warning("未找到任务详情，请刷新后重试。")
            return

        status = str(task.get("status", ""))
        percent = float(task.get("progress_percent", 0.0) or 0.0)
        percent = max(0.0, min(1.0, percent))

        st.progress(int(percent * 100))
        st.caption(
            " | ".join(
                [
                    f"状态: {status}",
                    f"阶段: {task.get('progress_stage', '')}",
                    f"进度: {task.get('progress_done', 0)}/{task.get('progress_total', 0)}",
                    f"信息: {task.get('progress_message', '')}",
                ]
            )
        )

        progress_events = task.get("progress_events", [])
        if isinstance(progress_events, list) and progress_events:
            with st.expander("步骤日志（调试）", expanded=True):
                event_count = len(progress_events)
                if event_count > 30:
                    show_limit = int(
                        st.slider(
                            "显示最近步骤数",
                            min_value=30,
                            max_value=min(2000, event_count),
                            value=min(300, event_count),
                            step=10,
                            key=f"task_event_limit_{selected_task_id}",
                        )
                    )
                else:
                    show_limit = event_count

                visible_events = progress_events[-show_limit:]
                event_rows: list[dict[str, object]] = []
                start_index = max(1, event_count - len(visible_events) + 1)
                for offset, item in enumerate(visible_events, start=0):
                    done = item.get("done", 0)
                    total = item.get("total", 0)
                    event_rows.append(
                        {
                            "序号": start_index + offset,
                            "时间": str(item.get("time", "")),
                            "阶段": str(item.get("stage", "")),
                            "进度": f"{done}/{total}",
                            "信息": str(item.get("message", "")),
                        }
                    )

                st.dataframe(pd.DataFrame(event_rows), use_container_width=True, hide_index=True)
                st.caption(f"已记录步骤: {event_count}")

        if status == "failed":
            st.error(str(task.get("error", "任务失败")))
        elif status in {"pending", "running"}:
            st.info("任务运行中，可点击“刷新任务”查看最新进度。")
        elif status == "success":
            st.success("任务已完成，可加载到当前页面。")
            if st.button("加载该任务结果", use_container_width=True, key=f"load_task_{selected_task_id}"):
                ok, message = _load_task_result_into_session(task, default_start=default_start, default_end=default_end)
                if ok:
                    st.success(message)
                    st.rerun()
                else:
                    st.error(message)


def _run_sync_analysis(
    *,
    full_flow_mode: bool,
    full_flow_fast_mode: bool,
    codes: list[str],
    start_date: date,
    end_date: date,
    window: int,
    k: float,
    near_ratio: float,
    adjust: str,
    force_refresh: bool,
    price_upper_limit: float,
    debt_asset_ratio_limit: float,
    exclude_gem_sci: bool,
    max_workers: int,
    max_retries: int,
    retry_backoff_seconds: float,
    request_interval_seconds: float,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame], dict[str, int | float]]:
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
                percent = 5 + int(ratio * 15)
                label = f"资金流阶段 {done}/{total}"
            elif stage == "evaluate":
                percent = 20 + int(ratio * 5)
                label = f"基础信息阶段 {done}/{total}"
            elif stage == "fundamental":
                percent = 25 + int(ratio * 45)
                label = f"基本面阶段 {done}/{total}"
            elif stage == "shareholder":
                percent = 70 + int(ratio * 15)
                label = f"股东阶段 {done}/{total}"
            elif stage == "boll":
                percent = 85 + int(ratio * 13)
                label = f"Boll阶段 {done}/{total}"
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

    if full_flow_mode:
        with st.spinner("正在执行全流程筛选（同花顺/新浪/baostock）..."):
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
                fast_mode=bool(full_flow_fast_mode),
                progress_callback=on_progress,
            )
    else:
        with st.spinner("正在执行仅Boll分析（跳过前置筛选）..."):
            code_name_map = fetch_code_name_map(codes, force_refresh=bool(force_refresh))
            result_df, data_map = analyze_stocks(
                codes=codes,
                start_date=start_date,
                end_date=end_date,
                window=window,
                k=float(k),
                near_ratio=float(near_ratio),
                adjust=adjust,
                code_name_map=code_name_map,
                force_refresh=bool(force_refresh),
                max_workers=1,
                retain_all_charts=True,
                progress_callback=on_progress,
            )
            flow_stats = {
                "输入代码数": len(codes),
                "Boll命中": int(result_df["命中策略"].sum())
                if (not result_df.empty and "命中策略" in result_df.columns)
                else 0,
            }

    progress_bar.progress(100)
    progress_text.caption("分析已完成，可查看结果表和图表。")
    return result_df, data_map, flow_stats


def main() -> None:
    st.set_page_config(page_title="Boll 可视化选股", layout="wide")
    st.title("📈 Boll 全流程选股")
    st.caption("流程：同花顺资金流 → baostock 基本面 → 新浪流通股东 → Boll 信号。")

    default_end = date.today()
    default_start = default_end - timedelta(days=DEFAULT_DAYS_BACK)
    ui_defaults = _build_default_ui_values(default_start=default_start, default_end=default_end)
    _ensure_ui_state(ui_defaults)

    with st.sidebar:
        _render_preset_panel(ui_defaults)

        st.subheader("参数设置")
        analysis_mode = st.radio(
            "运行模式",
            options=ANALYSIS_MODES,
            index=ANALYSIS_MODES.index(st.session_state["analysis_mode"])
            if st.session_state["analysis_mode"] in ANALYSIS_MODES
            else 0,
            key="analysis_mode",
        )
        full_flow_mode = analysis_mode.startswith("全流程")

        codes_text = st.text_area(
            "股票代码（支持逗号、空格、换行）",
            height=120,
            key="codes_text",
        )
        start_date = st.date_input("开始日期", key="start_date")
        end_date = st.date_input("结束日期", key="end_date")
        window = int(st.slider("均线窗口", min_value=10, max_value=60, step=1, key="window"))
        k = float(st.number_input("标准差倍数 k", min_value=0.8, max_value=3.0, step=0.01, key="k"))
        near_ratio = float(
            st.number_input(
                "接近下轨阈值（倍数）",
                min_value=1.0,
                max_value=1.05,
                step=0.001,
                format="%.3f",
                key="near_ratio",
            )
        )
        adjust = st.selectbox(
            "复权方式",
            options=["qfq", "hfq"],
            index=0 if st.session_state.get("adjust", DEFAULT_ADJUST) == "qfq" else 1,
            help="qfq=前复权，hfq=后复权",
            key="adjust",
        )
        force_refresh = bool(
            st.checkbox(
                "强制刷新缓存（重新抓取）",
                help="勾选后忽略本地缓存，重新请求行情与筛选数据。",
                key="force_refresh",
            )
        )

        with st.expander("缓存管理", expanded=False):
            overview = get_cache_overview()
            total_info = overview.get("all", {})
            st.caption(f"总缓存：{total_info.get('files', 0)} 个文件，{total_info.get('size_mb', 0)} MB")

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
                key="cache_clear_scope",
            )
            older_days = int(
                st.number_input(
                    "仅清理早于天数（0 表示全部）",
                    min_value=0,
                    max_value=3650,
                    step=1,
                    value=0,
                    key="cache_clear_older_days",
                )
            )
            if st.button("清理缓存", use_container_width=True, key="cache_clear_btn"):
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

        price_upper_limit = float(st.session_state.get("price_upper_limit", DEFAULT_PRICE_UPPER_LIMIT))
        debt_asset_ratio_limit = float(st.session_state.get("debt_asset_ratio_limit", DEFAULT_DEBT_ASSET_RATIO_LIMIT))
        exclude_gem_sci = bool(st.session_state.get("exclude_gem_sci", DEFAULT_EXCLUDE_GEM_SCI))
        max_workers = int(st.session_state.get("max_workers", 4))
        max_retries = int(st.session_state.get("max_retries", 2))
        retry_backoff_seconds = float(st.session_state.get("retry_backoff_seconds", 0.5))
        request_interval_seconds = float(st.session_state.get("request_interval_seconds", 0.0))
        boll_max_workers = int(st.session_state.get("boll_max_workers", 8))
        market_fast_mode = bool(st.session_state.get("market_fast_mode", True))
        market_fast_days = int(st.session_state.get("market_fast_days", min(180, max(60, window * 3))))

        if full_flow_mode:
            price_upper_limit = float(
                st.number_input(
                    "股价上限",
                    min_value=1.0,
                    max_value=300.0,
                    step=1.0,
                    key="price_upper_limit",
                )
            )
            debt_asset_ratio_limit = float(
                st.number_input(
                    "资产负债率上限(%)",
                    min_value=10.0,
                    max_value=100.0,
                    step=1.0,
                    key="debt_asset_ratio_limit",
                )
            )
            exclude_gem_sci = bool(st.checkbox("排除创业板(30*)和科创板(688*)", key="exclude_gem_sci"))

            with st.expander("并发与重试（高级）", expanded=False):
                market_fast_mode = bool(
                    st.checkbox(
                        "全流程极速模式（5分钟目标）",
                        help=(
                            "启用后会优先走本地缓存，并且只对资金流通过的股票执行基本面；"
                            "同时跳过最慢的盈利预测接口，显著缩短全市场耗时。"
                        ),
                        key="market_fast_mode",
                    )
                )
                max_workers = int(st.slider("并发评估线程数", min_value=1, max_value=16, step=1, key="max_workers"))
                max_retries = int(st.slider("网络重试次数", min_value=0, max_value=5, step=1, key="max_retries"))
                retry_backoff_seconds = float(
                    st.number_input(
                        "重试基准退避(秒)",
                        min_value=0.0,
                        max_value=5.0,
                        step=0.1,
                        format="%.1f",
                        key="retry_backoff_seconds",
                    )
                )
                request_interval_seconds = float(
                    st.number_input(
                        "请求限流间隔(秒)",
                        min_value=0.0,
                        max_value=1.0,
                        step=0.01,
                        format="%.2f",
                        key="request_interval_seconds",
                    )
                )

                if market_fast_mode:
                    st.caption("极速模式建议：关闭强制刷新并保持请求限流为 0，可在多次运行后稳定进入分钟级。")
        else:
            st.caption("仅Boll模式：直接对输入股票计算布林信号，跳过资金流/基本面/股东环节。")
            st.caption("可直接点击“全市场仅Boll分析”一键跑全A股（耗时较长）。")
            with st.expander("仅Boll性能优化", expanded=False):
                boll_max_workers = int(
                    st.slider(
                        "全市场并发线程数",
                        min_value=1,
                        max_value=16,
                        step=1,
                        key="boll_max_workers",
                    )
                )
                market_fast_mode = bool(
                    st.checkbox(
                        "全市场极速模式（仅近N日筛选）",
                        help="只在全市场仅Boll分析生效。通常使用近60-180天即可计算最新布林信号。",
                        key="market_fast_mode",
                    )
                )
                market_fast_days = int(
                    st.slider(
                        "极速模式回看天数",
                        min_value=max(30, int(window) + 5),
                        max_value=365,
                        step=5,
                        key="market_fast_days",
                    )
                )

        run_btn = st.button("开始分析", type="primary", use_container_width=True, key="run_custom_btn")
        run_market_boll_btn = False
        run_market_full_btn = False
        if full_flow_mode:
            run_market_full_btn = st.button("全市场全流程分析（后台）", use_container_width=True, key="run_market_full_btn")
        else:
            run_market_boll_btn = st.button("全市场仅Boll分析（后台）", use_container_width=True, key="run_market_boll_btn")

    if run_market_boll_btn or run_market_full_btn:
        if start_date > end_date:
            st.warning("开始日期不能晚于结束日期。")
        else:
            try:
                task_id = submit_market_analysis_task(
                    mode="full" if run_market_full_btn else "boll_only",
                    start_date=start_date,
                    end_date=end_date,
                    window=int(window),
                    k=float(k),
                    near_ratio=float(near_ratio),
                    adjust=str(adjust),
                    force_refresh=bool(force_refresh),
                    price_upper_limit=float(price_upper_limit),
                    debt_asset_ratio_limit=float(debt_asset_ratio_limit),
                    exclude_gem_sci=bool(exclude_gem_sci),
                    max_workers=int(max_workers),
                    max_retries=int(max_retries),
                    retry_backoff_seconds=float(retry_backoff_seconds),
                    request_interval_seconds=float(request_interval_seconds),
                    boll_max_workers=int(boll_max_workers),
                    market_fast_mode=bool(market_fast_mode),
                    market_fast_days=int(market_fast_days),
                )
                st.session_state["task_selected_id"] = task_id
                st.success(f"任务已提交：{task_id}，可在“后台任务中心”查看进度。")
                st.rerun()
            except Exception as error:
                logger.exception("后台任务提交失败: %s", error)
                st.error(f"后台任务提交失败：{error}")

    if run_btn:
        if start_date > end_date:
            st.warning("开始日期不能晚于结束日期。")
        else:
            codes = parse_codes_input(codes_text)
            if not codes:
                st.warning("请输入至少一个有效的6位股票代码。")
            else:
                try:
                    result_df, data_map, flow_stats = _run_sync_analysis(
                        full_flow_mode=full_flow_mode,
                        full_flow_fast_mode=bool(market_fast_mode),
                        codes=codes,
                        start_date=start_date,
                        end_date=end_date,
                        window=int(window),
                        k=float(k),
                        near_ratio=float(near_ratio),
                        adjust=str(adjust),
                        force_refresh=bool(force_refresh),
                        price_upper_limit=float(price_upper_limit),
                        debt_asset_ratio_limit=float(debt_asset_ratio_limit),
                        exclude_gem_sci=bool(exclude_gem_sci),
                        max_workers=int(max_workers),
                        max_retries=int(max_retries),
                        retry_backoff_seconds=float(retry_backoff_seconds),
                        request_interval_seconds=float(request_interval_seconds),
                    )
                except Exception as error:
                    logger.exception("分析失败: %s", error)
                    st.error(f"分析失败：{error}")
                else:
                    st.session_state["boll_result_df"] = result_df
                    st.session_state["boll_data_map"] = data_map
                    st.session_state["boll_window"] = int(window)
                    st.session_state["boll_k"] = float(k)
                    st.session_state["boll_near_ratio"] = float(near_ratio)
                    st.session_state["boll_start_date"] = start_date
                    st.session_state["boll_end_date"] = end_date
                    st.session_state["boll_adjust"] = str(adjust)
                    st.session_state["boll_flow_stats"] = flow_stats
                    st.session_state["boll_mode"] = "full" if full_flow_mode else "boll_only"
                    st.session_state["boll_scope"] = "custom"
                    logger.info("分析完成: count=%s", len(result_df))

    _render_task_center(default_start=default_start, default_end=default_end)

    result_df: pd.DataFrame = st.session_state.get("boll_result_df", pd.DataFrame())
    data_map: dict[str, pd.DataFrame] = st.session_state.get("boll_data_map", {})
    flow_stats: dict[str, int | float] = st.session_state.get("boll_flow_stats", {})
    mode: str = st.session_state.get("boll_mode", "full")
    scope: str = st.session_state.get("boll_scope", "custom")
    selected_window = int(st.session_state.get("boll_window", DEFAULT_WINDOW))
    selected_k = float(st.session_state.get("boll_k", DEFAULT_K))
    selected_near_ratio = float(st.session_state.get("boll_near_ratio", DEFAULT_NEAR_RATIO))

    if result_df.empty:
        st.info("在左侧设置参数后点击“开始分析”或“全市场...（后台）”。")
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
                st.warning("当前卡在资金流过滤。可尝试：提高‘股价上限’、扩大股票池，或换一个交易日重试。")
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

    available_codes = list(dict.fromkeys(result_df["股票代码"].astype(str).tolist()))
    if not available_codes:
        st.warning("当前没有可绘制 Boll 图表的股票。")
        return

    selected_code = st.selectbox("选择要查看图表的股票", options=available_codes)
    selected_row = result_df[result_df["股票代码"].astype(str) == selected_code].iloc[0]
    chart_df = data_map.get(selected_code, pd.DataFrame())

    if chart_df.empty:
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
