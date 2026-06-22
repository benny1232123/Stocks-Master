"""相对强弱策略 UI — 资金流筛选 → 基本面过滤 → 股东筛选 → 指数相对强弱验证。"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import sys
import threading
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# 定位父目录 Frequently-Used-Program/ 以便导入 Stock-Selection-Relativity.py
# ---------------------------------------------------------------------------
_PARENT_DIR = Path(__file__).resolve().parents[3]  # ui/ → src/ → boll-visualizer/ → Frequently-Used-Program/
if str(_PARENT_DIR) not in sys.path:
    sys.path.insert(0, str(_PARENT_DIR))

# ---------------------------------------------------------------------------
# 延迟加载 Relativity 模块（含 baostock/akshare 等重依赖）
# ---------------------------------------------------------------------------
_RELATIVITY_MODULE = None
_LOAD_ERROR: str | None = None
_LOAD_LOCK = threading.Lock()


def _get_relativity_module():
    """首次调用时加载 Stock-Selection-Relativity 模块，后续返回缓存。

    由于脚本文件名含连字符（Stock-Selection-Relativity.py），无法直接 import，
    因此使用 importlib 动态加载。
    """
    global _RELATIVITY_MODULE, _LOAD_ERROR

    if _RELATIVITY_MODULE is not None:
        return _RELATIVITY_MODULE

    with _LOAD_LOCK:
        if _RELATIVITY_MODULE is not None:
            return _RELATIVITY_MODULE

        script_path = _PARENT_DIR / "Stock-Selection-Relativity.py"
        if not script_path.exists():
            _LOAD_ERROR = f"找不到脚本文件：{script_path}"
            raise ImportError(_LOAD_ERROR)

        try:
            spec = importlib.util.spec_from_file_location("relativity_selection", str(script_path))
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            _RELATIVITY_MODULE = module
            return module
        except ImportError as exc:
            _LOAD_ERROR = str(exc)
            raise
        except Exception as exc:
            _LOAD_ERROR = f"加载模块失败：{exc}"
            raise ImportError(_LOAD_ERROR) from exc


# ---------------------------------------------------------------------------
# UI 渲染入口
# ---------------------------------------------------------------------------
def render_relativity_tab() -> None:
    """渲染相对强弱策略的完整 UI（内联，无侧边栏）。"""
    st.title("📊 相对强弱策略")
    st.caption("资金流筛选 → 基本面过滤 → 股东筛选 → 指数相对强弱验证")

    # ── 参数面板 ──────────────────────────────────────────────
    _render_params()

    # ── 开始分析按钮 ──────────────────────────────────────────
    if st.button("开始分析", type="primary", use_container_width=True, key="relativity_run_btn"):
        _run_pipeline()

    # ── 结果展示 ──────────────────────────────────────────────
    _render_results()


# ---------------------------------------------------------------------------
# 参数面板
# ---------------------------------------------------------------------------
def _render_params() -> None:
    """在页面主体区域渲染参数输入控件（两列布局）。"""
    col1, col2 = st.columns(2)

    with col1:
        st.number_input(
            "股价下限",
            min_value=0.0,
            max_value=1000.0,
            step=1.0,
            key="relativity_price_lower_limit",
            value=st.session_state.get("relativity_price_lower_limit", 5.0),
        )
        st.number_input(
            "股价上限",
            min_value=1.0,
            max_value=1000.0,
            step=1.0,
            key="relativity_price_upper_limit",
            value=st.session_state.get("relativity_price_upper_limit", 30.0),
        )
        st.number_input(
            "资产负债率上限 (%)",
            min_value=10.0,
            max_value=100.0,
            step=1.0,
            key="relativity_debt_asset_ratio_limit",
            value=st.session_state.get("relativity_debt_asset_ratio_limit", 70.0),
        )

    with col2:
        st.number_input(
            "相对强弱回看天数",
            min_value=20,
            max_value=500,
            step=5,
            key="relativity_rs_lookback_days",
            value=st.session_state.get("relativity_rs_lookback_days", 100),
        )
        st.number_input(
            "抗跌满足率下限",
            min_value=0.0,
            max_value=1.0,
            step=0.05,
            format="%.2f",
            key="relativity_min_down_ratio",
            value=st.session_state.get("relativity_min_down_ratio", 0.70),
        )
        st.text_input(
            "对比指数代码",
            key="relativity_index_code",
            value=st.session_state.get("relativity_index_code", "sh.000001"),
            help="baostock 格式，如 sh.000001（上证指数）、sh.000300（沪深300）",
        )


# ---------------------------------------------------------------------------
# 分析管线
# ---------------------------------------------------------------------------
def _run_pipeline() -> None:
    """依次执行资金流 → 基本面 → 候选合并 → 股东 → 相对强弱。"""

    # 读取参数
    price_lower = float(st.session_state.get("relativity_price_lower_limit", 5.0))
    price_upper = float(st.session_state.get("relativity_price_upper_limit", 30.0))
    debt_limit = float(st.session_state.get("relativity_debt_asset_ratio_limit", 70.0))
    lookback_days = int(st.session_state.get("relativity_rs_lookback_days", 100))
    min_down_ratio = float(st.session_state.get("relativity_min_down_ratio", 0.70))
    index_code = str(st.session_state.get("relativity_index_code", "sh.000001"))

    # 归一化抗跌满足率（兼容 0~100 写法）
    if min_down_ratio > 1.5:
        min_down_ratio = min_down_ratio / 100.0
    min_down_ratio = max(0.0, min(min_down_ratio, 1.0))

    # 加载模块
    try:
        mod = _get_relativity_module()
    except ImportError as exc:
        st.error(
            f"无法加载相对强弱模块：{exc}\n\n"
            "请确认已安装以下依赖：\n"
            "- akshare (`pip install akshare`)\n"
            "- baostock (`pip install baostock`)\n"
            "- pandas (`pip install pandas`)"
        )
        return
    except Exception as exc:
        st.error(f"加载相对强弱模块时发生未知错误：{exc}")
        return

    # 清空上一轮结果
    st.session_state.pop("relativity_result_df", None)
    st.session_state.pop("relativity_log_lines", None)

    progress_bar = st.progress(0)
    log_container = st.empty()
    log_lines: list[str] = []

    def _append_log(text: str) -> None:
        """将日志行追加到列表并刷新显示。"""
        log_lines.append(text)
        # 仅显示最近 50 行，避免页面过长
        display_lines = log_lines[-50:]
        log_container.text("\n".join(display_lines))

    class _StreamlitWriter(io.StringIO):
        """重定向 print() 输出到 Streamlit 文本容器。"""

        def write(self, text: str) -> int:
            stripped = text.rstrip("\n")
            if stripped:
                _append_log(stripped)
            return len(text)

    writer = _StreamlitWriter()

    with st.spinner("正在执行相对强弱分析管线..."):
        try:
            # ── 步骤 1：资金流筛选 ──────────────────────────────
            _append_log("▶ 步骤 1/5：获取资金流数据...")
            progress_bar.progress(5)
            with contextlib.redirect_stdout(writer):
                fund_flow_codes = mod.get_fund_flow_codes(price_upper, price_lower, sleep_seconds=0.0)
            total_fund = sum(len(v) for v in fund_flow_codes.values())
            _append_log(f"  资金流候选总数: {total_fund}")
            progress_bar.progress(20)

            # ── 步骤 2：基本面过滤 ──────────────────────────────
            _append_log("▶ 步骤 2/5：获取基本面数据...")
            now = datetime.now()
            report_date_profit, report_date_holder, zcfz_dates, current_year = mod.resolve_report_dates(now)
            with contextlib.redirect_stdout(writer):
                cashflow_codes, profit_codes, zcfz_codes, profit_forecast_codes = mod.get_fundamental_codes(
                    debt_limit, report_date_profit, zcfz_dates, current_year, sleep_seconds=0.0
                )
            _append_log(
                f"  现金流: {len(cashflow_codes)} | 利润: {len(profit_codes)} | "
                f"资产负债率: {len(zcfz_codes)} | 盈利预测: {len(profit_forecast_codes)}"
            )
            progress_bar.progress(40)

            # ── 步骤 3：合并候选 ────────────────────────────────
            _append_log("▶ 步骤 3/5：合并候选股票...")
            with contextlib.redirect_stdout(writer):
                candidate_codes = mod.build_candidate_codes(
                    cashflow_codes, profit_codes, zcfz_codes, profit_forecast_codes, fund_flow_codes
                )
            _append_log(f"  合并后候选: {len(candidate_codes)}")
            progress_bar.progress(50)

            if not candidate_codes:
                _append_log("⚠ 候选股票为空，无法继续。")
                progress_bar.progress(100)
                st.warning("经过资金流 + 基本面筛选后没有候选股票，请调整参数后重试。")
                st.session_state["relativity_log_lines"] = log_lines
                return

            # ── 步骤 4：股东筛选 ────────────────────────────────
            _append_log("▶ 步骤 4/5：流通股东筛选...")
            with contextlib.redirect_stdout(writer):
                final_candidates = mod.filter_by_shareholders(
                    candidate_codes, report_date_holder, sleep_seconds=0.0, max_workers=4
                )
            _append_log(f"  股东筛选后: {len(final_candidates)}")
            progress_bar.progress(65)

            if not final_candidates:
                _append_log("⚠ 股东筛选后无候选股票。")
                progress_bar.progress(100)
                st.warning("股东筛选后没有剩余候选股票，请调整参数后重试。")
                st.session_state["relativity_log_lines"] = log_lines
                return

            # 获取股票名称映射
            with contextlib.redirect_stdout(writer):
                code_name_map = mod.get_code_name_map()

            # ── 步骤 5：相对强弱评估 ────────────────────────────
            _append_log("▶ 步骤 5/5：指数相对强弱评估...")
            progress_bar.progress(70)

            today_text = now.strftime("%Y%m%d")

            # baostock 登录
            try:
                import baostock as bs
                bs.login()
            except Exception as exc:
                _append_log(f"baostock 登录失败: {exc}")
                st.error(f"baostock 登录失败：{exc}")
                st.session_state["relativity_log_lines"] = log_lines
                return

            try:
                with contextlib.redirect_stdout(writer):
                    selected_rows = mod.run_relative_strength(
                        final_candidates,
                        code_name_map,
                        index_code,
                        lookback_days,
                        today_text,
                        max_workers=4,
                        resume=False,
                        price_lower_limit=price_lower,
                        price_upper_limit=price_upper,
                        min_down_ratio=min_down_ratio,
                        bs_request_interval_seconds=0.05,
                        bs_max_retries=2,
                    )
            finally:
                try:
                    bs.logout()
                except Exception:
                    pass

            progress_bar.progress(100)

            # 构造结果 DataFrame
            if selected_rows:
                result_df = pd.DataFrame(selected_rows)
                # 清理检查点列
                pass_col = "是否通过"
                if pass_col in result_df.columns:
                    result_df = result_df.drop(columns=[pass_col], errors="ignore")
                # 按抗跌满足率 + 上涨满足率降序
                sort_cols = [c for c in ["抗跌满足率", "上涨满足率"] if c in result_df.columns]
                if sort_cols:
                    result_df = result_df.sort_values(sort_cols, ascending=False, na_position="last")
                st.session_state["relativity_result_df"] = result_df
                _append_log(f"✅ 分析完成，共选出 {len(result_df)} 只股票。")
            else:
                st.session_state["relativity_result_df"] = pd.DataFrame()
                _append_log("⚠ 没有选出符合相对强弱策略的股票。")

            st.session_state["relativity_log_lines"] = log_lines
            st.success(f"分析完成！共选出 {len(selected_rows)} 只股票。")

        except Exception as exc:
            _append_log(f"❌ 分析过程中发生错误：{exc}")
            st.error(f"分析失败：{exc}")
            st.session_state["relativity_log_lines"] = log_lines


# ---------------------------------------------------------------------------
# 结果展示
# ---------------------------------------------------------------------------
def _render_results() -> None:
    """展示分析结果表格和下载按钮。"""
    result_df: pd.DataFrame = st.session_state.get("relativity_result_df", pd.DataFrame())
    log_lines: list[str] = st.session_state.get("relativity_log_lines", [])

    # 日志展开器
    if log_lines:
        with st.expander("分析日志", expanded=False):
            st.text("\n".join(log_lines[-200:]))

    if result_df.empty:
        if not log_lines:
            st.info("设置参数后点击「开始分析」运行相对强弱策略。")
        return

    # 指标摘要
    col1, col2, col3 = st.columns(3)
    col1.metric("选出股票数", len(result_df))
    if "抗跌满足率" in result_df.columns:
        avg_down = result_df["抗跌满足率"].dropna()
        col2.metric("平均抗跌满足率", f"{avg_down.mean():.1%}" if not avg_down.empty else "N/A")
    else:
        col2.metric("平均抗跌满足率", "N/A")
    if "上涨满足率" in result_df.columns:
        avg_up = result_df["上涨满足率"].dropna()
        col3.metric("平均上涨满足率", f"{avg_up.mean():.1%}" if not avg_up.empty else "N/A")
    else:
        col3.metric("平均上涨满足率", "N/A")

    # 结果表格
    st.dataframe(result_df, use_container_width=True, hide_index=True)

    # CSV 下载
    csv_bytes = result_df.to_csv(index=False).encode("utf-8-sig")
    file_name = f"Stock-Selection-Relativity-{datetime.now().strftime('%Y%m%d')}.csv"
    st.download_button(
        "下载筛选结果 CSV",
        data=csv_bytes,
        file_name=file_name,
        mime="text/csv",
        use_container_width=True,
        key="relativity_download_btn",
    )
