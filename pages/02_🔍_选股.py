"""选股中心：布林带扫描 + 策略融合 + 操作清单

候选股票列表一天只拉一次，结果持久化。
扫描结果也可以导出存档。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VIZ_SRC = ROOT / "Frequently-Used-Program" / "boll-visualizer" / "src"
for p in [str(ROOT), str(VIZ_SRC)]:
    if p not in sys.path:
        sys.path.insert(0, p)
os.environ.setdefault("KLINE_BACKEND", "akshare")

import streamlit as st
import pandas as pd
from datetime import date

st.set_page_config(page_title="选股中心", page_icon="🔍", layout="wide")

from smcore.cache_daily import get_daily, force_refresh


# ═══════════════════════════════════════════════
# 布林带扫描
# ═══════════════════════════════════════════════


def scan_boll_batch(
    codes: list[str],
    window: int = 20,
    k: float = 1.645,
    near_ratio: float = 1.015,
    days_back: int = 180,
) -> pd.DataFrame:
    """扫描一批股票的布林带信号。"""
    from smcore.data.kline import fetch_daily_k
    from smcore.indicators.boll import calc_bollinger, evaluate_boll_signal

    results = []
    total = len(codes)
    progress = st.progress(0, text=f"扫描中 0/{total}")
    status = st.empty()

    for i, code in enumerate(codes):
        try:
            kdf = fetch_daily_k(code, days_back=days_back)
            if kdf.empty or len(kdf) < window:
                continue
            kdf = calc_bollinger(kdf, window=window, k=k)
            sig = evaluate_boll_signal(kdf, window=window, k=k, near_ratio=near_ratio)

            results.append({
                "代码": code,
                "最新价": sig.get("price"),
                "中轨": sig.get("middle"),
                "下轨": sig.get("lower"),
                "上轨": sig.get("upper"),
                "信号": sig.get("signal", "无"),
                "距下轨%": sig.get("dist_to_lower_pct"),
                "距上轨%": sig.get("dist_to_upper_pct"),
            })
        except Exception:
            pass
        progress.progress((i + 1) / total, text=f"扫描中 {i+1}/{total}")

    progress.empty()
    status.empty()

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df = df.sort_values("距下轨%", ascending=True)
    return df


def _fetch_candidate_codes(price_min: float, price_max: float) -> list[str]:
    """从新浪源获取 A 股列表并按价格筛选（每日缓存，一天只拉一次）。"""
    import akshare as ak
    from smcore.utils.code import format_stock_code
    try:
        spot = ak.stock_zh_a_spot()
    except Exception:
        return []
    if spot is None or spot.empty:
        return []
    spot = spot[(spot["最新价"] >= price_min) & (spot["最新价"] <= price_max)]
    return [format_stock_code(c) for c in spot["代码"].tolist() if format_stock_code(c)]


def get_candidate_codes(price_min: float, price_max: float) -> tuple[list[str], str | None]:
    """获取候选股票列表（每日缓存）。"""
    # 缓存 key 带价格范围，不同范围独立缓存
    cache_key = f"candidate_codes_{int(price_min)}_{int(price_max)}"
    codes, cache_date = get_daily(cache_key, _fetch_candidate_codes, price_min, price_max)
    return codes or [], cache_date


# ═══════════════════════════════════════════════
# 页面渲染
# ═══════════════════════════════════════════════

st.title("🔍 选股中心")

tab1, tab2 = st.tabs(["布林带选股", "策略融合（全流程）"])

# ── Tab 1：布林带选股 ──
with tab1:
    st.subheader("布林带信号扫描")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        window = st.slider("布林窗口", 10, 60, 20, help="计算中轨的移动平均周期")
    with col2:
        k = st.slider("K 值（标准差倍数）", 1.0, 3.0, 1.645, 0.005,
                      help="1.645=90%置信  |  2.0=95%  |  2.576=99%")
    with col3:
        price_min = st.number_input("最低股价", 1.0, 500.0, 5.0, step=1.0)
    with col4:
        price_max = st.number_input("最高股价", 1.0, 500.0, 30.0, step=1.0)

    near_ratio = st.slider(
        "接近下轨阈值", 1.0, 1.1, 1.015, 0.001,
        help="现价 / 下轨 < 此值视为「接近下轨」信号"
    )

    scan_col1, scan_col2 = st.columns([1, 3])
    with scan_col1:
        do_scan = st.button("🚀 开始扫描", type="primary", use_container_width=True)

    manual_codes = st.text_input(
        "或手动输入代码（用逗号/空格/换行分隔，留空则扫描全市场）",
        placeholder="例如：000001, 600519, 002415",
    )

    if do_scan:
        if manual_codes.strip():
            codes = [c.strip() for c in manual_codes.replace("\n", ",").replace(" ", ",").split(",") if c.strip()]
            cache_date = None
        else:
            with st.spinner("正在获取 A 股列表（每日缓存，一天只拉一次）..."):
                codes, cache_date = get_candidate_codes(price_min, price_max)
            if cache_date:
                if cache_date != date.today().strftime("%Y-%m-%d"):
                    st.info(f"📋 候选列表来自 {cache_date} 的缓存（今日未刷新）")

        if not codes:
            st.warning("没有符合条件的股票")
        else:
            st.info(f"共 {len(codes)} 只股票在扫描范围，预计需要数分钟...")
            df_result = scan_boll_batch(codes, window=window, k=k, near_ratio=near_ratio)

            if df_result.empty:
                st.warning("未发现布林带信号")
            else:
                st.success(f"扫描完成，发现 {len(df_result)} 条信号")

                near_count = (df_result["信号"] == "靠近下轨").sum()
                touch_count = (df_result["信号"] == "触及下轨").sum()
                upper_count = (df_result["信号"].str.contains("上轨", na=False)).sum()
                st.write(
                    f"靠近下轨 {near_count} | 触及下轨 {touch_count} | 上轨相关 {upper_count}"
                )

                disp_cols = ["代码", "最新价", "中轨", "下轨", "上轨", "信号", "距下轨%"]
                st.dataframe(
                    df_result[disp_cols].style.format({
                        "最新价": "{:.2f}", "中轨": "{:.2f}", "下轨": "{:.2f}",
                        "上轨": "{:.2f}", "距下轨%": "{:.1f}%",
                    }),
                    use_container_width=True,
                    height=500,
                )

                csv = df_result.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                st.download_button(
                    "📥 导出 CSV",
                    csv,
                    f"boll_scan_{date.today().strftime('%Y%m%d')}.csv",
                    "text/csv",
                )

# ── Tab 2：策略融合 ──
with tab2:
    st.subheader("策略融合选股（全流程）")
    st.caption("融合布林带 + 题材 + 相对性 + 央视 + 资金流信号，生成综合操作清单")

    col_cap, col_pick = st.columns(2)
    with col_cap:
        total_capital = st.number_input("总资金（元）", 10000, 1000000, 100000, step=10000)
    with col_pick:
        max_picks = st.slider("最大选股数", 5, 30, 15)

    if st.button("🔬 运行策略融合", type="primary"):
        with st.spinner("正在运行策略融合... 这可能需要 5-15 分钟"):
            try:
                from smcore.strategy import fuse_signals, save_action_list

                today = date.today().strftime("%Y%m%d")
                df, meta = fuse_signals(today, total_capital=total_capital, max_picks=max_picks)

                if df.empty:
                    st.warning("策略融合未产出结果（可能无可选标的或网络问题）")
                else:
                    path = save_action_list(df, today)

                    st.success(f"策略融合完成！选出 {len(df)} 只股票")

                    if meta:
                        with st.expander("📊 选股统计"):
                            st.markdown(meta)

                    st.dataframe(df, use_container_width=True, height=600)

                    if path:
                        csv = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                        st.download_button(
                            "📥 下载操作清单",
                            csv,
                            path.name,
                            "text/csv",
                        )

            except Exception as e:
                st.error(f"策略融合失败：{e}")
                st.info("提示：检查网络连接，或尝试布林带选股标签页的简单扫描。")
