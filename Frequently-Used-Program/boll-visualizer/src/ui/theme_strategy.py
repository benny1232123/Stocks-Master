"""短线题材换手策略 UI 模块

包装 Stock-Selection-Ashare-Theme-Turnover.py 的核心逻辑，
在 Streamlit 页面中提供参数配置、扫描执行与结果展示功能。

数据流：
  央视新闻热门板块 CSV → 板块成分股池 → A股扫描（换手率 + 动量） → 综合评分排序输出
"""

from __future__ import annotations

import importlib.util
import types
from pathlib import Path

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# 路径常量
# ---------------------------------------------------------------------------

# 当前文件: boll-visualizer/src/ui/theme_strategy.py
# parents[0] = ui/, parents[1] = src/, parents[2] = boll-visualizer/
# parents[3] = Frequently-Used-Program/
_PARENT_DIR = Path(__file__).resolve().parents[3]  # Frequently-Used-Program/
_THEME_SCRIPT = _PARENT_DIR / "Stock-Selection-Ashare-Theme-Turnover.py"

# ---------------------------------------------------------------------------
# 延迟加载主题策略脚本（文件名含连字符，无法直接 import）
# ---------------------------------------------------------------------------


def _get_theme_module():
    """延迟加载并缓存 Stock-Selection-Ashare-Theme-Turnover 模块。"""
    if not hasattr(_get_theme_module, "_mod"):
        if not _THEME_SCRIPT.exists():
            raise FileNotFoundError(
                f"未找到主题策略脚本:\n{_THEME_SCRIPT}\n"
                "请确认 Stock-Selection-Ashare-Theme-Turnover.py 存在于上级目录中。"
            )
        _spec = importlib.util.spec_from_file_location(
            "theme_turnover",
            str(_THEME_SCRIPT),
        )
        _mod = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_mod)
        _get_theme_module._mod = _mod
    return _get_theme_module._mod


# ---------------------------------------------------------------------------
# 辅助：构造参数命名空间（模拟 argparse.Namespace）
# ---------------------------------------------------------------------------


def _build_args(**kwargs) -> types.SimpleNamespace:
    """将关键字参数打包为 SimpleNamespace，供 build_strategy_candidates 使用。"""
    defaults = dict(
        top_n=30,
        max_stocks=1200,
        max_workers=8,
        hot_sector_top_n=5,
        min_latest_turn=0.8,
        min_avg_turn5=0.6,
        min_latest_amount=2.0e8,
        min_latest_price=5.0,
        max_latest_price=30.0,
        include_gem=False,
        bs_timeout_seconds=15.0,
        bs_request_interval_seconds=0.05,
        bs_max_retries=2,
    )
    defaults.update(kwargs)
    return types.SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# 辅助：检查 CCTV 板块数据文件是否存在
# ---------------------------------------------------------------------------


def _check_cctv_data_files(mod) -> dict:
    """检查主题策略所需的 CCTV 数据文件情况，返回诊断信息。"""
    info: dict = {
        "hot_sector_file": None,
        "sector_pool_files": [],
        "shared_seed_files": [],
    }

    # 热门板块文件
    try:
        hot_file = mod._latest_hot_sector_file()
        info["hot_sector_file"] = hot_file
    except Exception:
        pass

    # 板块成分股池文件
    try:
        pool_files = list(mod._iter_data_files(mod.SECTOR_STOCK_POOL_PATTERN))
        info["sector_pool_files"] = pool_files
    except Exception:
        pass

    # 共享种子池文件
    try:
        seed_files = list(mod.DATA_DIR.glob(mod.SHARED_SEED_PATTERN))
        info["shared_seed_files"] = seed_files
    except Exception:
        pass

    return info


# ---------------------------------------------------------------------------
# 辅助：结果 DataFrame 导出
# ---------------------------------------------------------------------------


def _export_csv_bytes(df: pd.DataFrame) -> bytes:
    """将 DataFrame 导出为 UTF-8 BOM 编码的 CSV bytes。"""
    return df.to_csv(index=False).encode("utf-8-sig")


# ---------------------------------------------------------------------------
# 主渲染函数
# ---------------------------------------------------------------------------


def render_theme_tab() -> None:
    """渲染「短线题材换手策略」完整 UI（内联布局，不使用侧边栏）。"""

    st.title("🔥 短线题材换手策略")
    st.caption("央视新闻热门板块 → 政策题材动量筛选 → 换手率验证 → 短线候选")

    # ---- 第一步：尝试加载依赖库 ----
    missing_deps: list[str] = []
    try:
        import akshare  # noqa: F401
    except ImportError:
        missing_deps.append("akshare")
    try:
        import baostock  # noqa: F401
    except ImportError:
        missing_deps.append("baostock")

    if missing_deps:
        st.error(
            f"缺少依赖库：**{', '.join(missing_deps)}**\n\n"
            f"请运行 `pip install {' '.join(missing_deps)}` 后刷新页面。"
        )
        return

    # ---- 第二步：加载主题策略模块 ----
    try:
        mod = _get_theme_module()
    except FileNotFoundError as exc:
        st.error(str(exc))
        return
    except Exception as exc:
        st.error(f"加载主题策略脚本失败: {exc}")
        return

    # ---- 第三步：检查 CCTV 数据文件 ----
    diag = _check_cctv_data_files(mod)
    if diag["hot_sector_file"] is None:
        st.warning(
            "⚠️ 未找到央视新闻热门板块数据文件（`CCTV-Hot-Sectors-*.csv`）。\n\n"
            "本策略依赖「央视新闻联播策略」的输出数据。请先运行该策略生成板块文件，"
            "否则将回退使用申万行业分类作为替代。"
        )
    if not diag["sector_pool_files"]:
        st.info(
            "💡 未找到板块成分股池文件（`CCTV-Sector-Stock-Pool-*.csv`）。"
            "如有该文件可提升题材匹配精度，无文件时策略仍可运行。"
        )

    # ---- 参数配置区 ----
    st.divider()
    st.subheader("📋 策略参数")

    # 第一行：核心筛选参数
    col1, col2, col3 = st.columns(3)
    with col1:
        top_n = st.slider(
            "输出数量 Top N",
            min_value=10,
            max_value=100,
            value=st.session_state.get("theme_top_n", 30),
            key="theme_top_n",
        )
    with col2:
        min_latest_turn = st.number_input(
            "最新换手率下限 (%)",
            min_value=0.0,
            max_value=20.0,
            value=st.session_state.get("theme_min_latest_turn", 0.8),
            step=0.1,
            format="%.1f",
            key="theme_min_latest_turn",
        )
    with col3:
        min_avg_turn5 = st.number_input(
            "近5日平均换手率下限 (%)",
            min_value=0.0,
            max_value=20.0,
            value=st.session_state.get("theme_min_avg_turn5", 0.6),
            step=0.1,
            format="%.1f",
            key="theme_min_avg_turn5",
        )

    # 第二行：成交额与股价范围
    col4, col5, col6 = st.columns(3)
    with col4:
        # 用户输入以"亿"为单位，内部转换为元
        min_amount_yi = st.number_input(
            "最新成交额下限 (亿元)",
            min_value=0.0,
            max_value=100.0,
            value=st.session_state.get("theme_min_amount_yi", 2.0),
            step=0.5,
            format="%.1f",
            key="theme_min_amount_yi",
        )
    with col5:
        min_price = st.number_input(
            "股价下限 (元)",
            min_value=0.0,
            max_value=100.0,
            value=st.session_state.get("theme_min_price", 5.0),
            step=1.0,
            format="%.1f",
            key="theme_min_price",
        )
    with col6:
        max_price = st.number_input(
            "股价上限 (元)",
            min_value=1.0,
            max_value=500.0,
            value=st.session_state.get("theme_max_price", 30.0),
            step=1.0,
            format="%.1f",
            key="theme_max_price",
        )

    # 第三行：性能与市场参数
    col7, col8, col9 = st.columns(3)
    with col7:
        max_stocks = st.number_input(
            "最大扫描股票数",
            min_value=100,
            max_value=6000,
            value=st.session_state.get("theme_max_stocks", 1200),
            step=100,
            key="theme_max_stocks",
        )
    with col8:
        max_workers = st.number_input(
            "并发数",
            min_value=1,
            max_value=32,
            value=st.session_state.get("theme_max_workers", 8),
            step=1,
            key="theme_max_workers",
        )
    with col9:
        include_gem = st.checkbox(
            "包含创业板 (300开头)",
            value=st.session_state.get("theme_include_gem", False),
            key="theme_include_gem",
        )

    # ---- 执行按钮 ----
    st.divider()
    run_clicked = st.button(
        "🚀 开始扫描",
        type="primary",
        use_container_width=True,
        key="theme_run_btn",
    )

    if not run_clicked and "theme_result_df" not in st.session_state:
        # 首次打开且未运行过，不显示结果区
        return

    # ---- 执行扫描 ----
    if run_clicked:
        # 构造参数（成交额从亿元转换为元）
        args = _build_args(
            top_n=top_n,
            max_stocks=max_stocks,
            max_workers=max_workers,
            min_latest_turn=min_latest_turn,
            min_avg_turn5=min_avg_turn5,
            min_latest_amount=min_amount_yi * 1e8,
            min_latest_price=min_price,
            max_latest_price=max_price,
            include_gem=include_gem,
        )

        import socket
        socket.setdefaulttimeout(max(3.0, float(args.bs_timeout_seconds)))

        with st.spinner("正在扫描 A 股，请稍候（首次运行可能需要数分钟）..."):
            try:
                import baostock as bs

                login_res = bs.login()
                if login_res.error_code != "0":
                    st.error(f"baostock 登录失败: {login_res.error_msg}")
                    return

                try:
                    result_df, hot_sectors = mod.build_strategy_candidates(args)
                finally:
                    bs.logout()

            except Exception as exc:
                st.error(f"扫描过程中出错: {exc}")
                return

        # 存入 session_state
        st.session_state["theme_result_df"] = result_df
        st.session_state["theme_hot_sectors"] = hot_sectors

    # ---- 展示结果 ----
    result_df: pd.DataFrame = st.session_state.get("theme_result_df", pd.DataFrame())
    hot_sectors: list = st.session_state.get("theme_hot_sectors", [])

    # 热门板块信息
    if hot_sectors:
        st.info(f"**热门板块**: {', '.join(hot_sectors)}")

    if result_df.empty:
        st.warning(
            "未筛选出符合条件的股票。可尝试放宽参数条件（降低换手率/成交额下限，扩大股价范围等）。"
        )
        return

    # 汇总指标
    total_candidates = len(result_df)
    display_df = result_df.head(top_n)

    st.divider()
    st.subheader(f"📊 扫描结果（共 {total_candidates} 只候选，展示前 {len(display_df)} 只）")

    # 汇总指标卡片
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("候选股票数", total_candidates)
    m2.metric("展示数量", len(display_df))
    if "综合分" in result_df.columns:
        m3.metric("最高综合分", f"{result_df['综合分'].iloc[0]:.2f}")
        m4.metric("平均综合分", f"{result_df['综合分'].mean():.2f}")

    # 选择展示列（如果列存在）
    display_cols = [
        "股票代码",
        "股票名称",
        "最新价",
        "建议买入价",
        "最新换手率%",
        "近5日换手均值%",
        "成交额放大倍数",
        "5日涨跌幅%",
        "20日涨跌幅%",
        "距20日高点比",
        "题材命中数",
        "题材标签",
        "综合分",
    ]
    display_cols = [c for c in display_cols if c in display_df.columns]

    st.dataframe(
        display_df[display_cols],
        use_container_width=True,
        hide_index=True,
        height=min(400 + len(display_df) * 35, 800),
    )

    # CSV 下载
    csv_bytes = _export_csv_bytes(display_df)
    st.download_button(
        label="📥 下载结果 CSV",
        data=csv_bytes,
        file_name="theme_strategy_result.csv",
        mime="text/csv",
        key="theme_download_btn",
    )
