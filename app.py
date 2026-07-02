"""Stocks-Master — A股智能选股系统

Hugging Face Spaces 入口。无需本地部署，浏览器即用。
数据源：akshare（东财），全云端 HTTP 接口。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# ── 路径设置：让 pages/ 的子模块能找到 smcore 和 visualizer 代码 ──
ROOT = Path(__file__).resolve().parent
VIZ_SRC = ROOT / "Frequently-Used-Program" / "boll-visualizer" / "src"
for p in [str(ROOT), str(VIZ_SRC)]:
    if p not in sys.path:
        sys.path.insert(0, p)

# 云端强制使用 akshare（无需 baostock C 扩展）
os.environ.setdefault("KLINE_BACKEND", "akshare")

# ── 首页 ──
import streamlit as st

st.set_page_config(
    page_title="Stocks-Master",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("📊 Stocks-Master")
st.caption("A股智能选股与持仓管理系统 · 全云端运行 · 零费用")

st.markdown("---")

# 功能卡片
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("### 📊 首页看板")
    st.markdown("大盘指数、市场热度、宏观指标一目了然")
with col2:
    st.markdown("### 🔍 选股中心")
    st.markdown("布林带扫描 + 多策略融合，智能选股")
with col3:
    st.markdown("### 📈 个股分析")
    st.markdown("K线 + 布林带 + MACD/RSI/KDJ，深度分析")

col4, col5 = st.columns(2)
with col4:
    st.markdown("### 💼 持仓管理")
    st.markdown("交易录入、FIFO盈亏追踪、持仓全貌")
with col5:
    st.markdown("### ⏪ 策略回测")
    st.markdown("历史信号回测，评估策略表现")

st.markdown("---")
st.markdown("👈 **从左侧边栏选择功能页面**")

# 底部信息
st.markdown("---")
st.caption(
    "数据来源：akshare（东方财富） | "
    "运行平台：Hugging Face Spaces（免费） | "
    "代码仓库：GitHub"
)
