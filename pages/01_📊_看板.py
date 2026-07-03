"""首页看板：大盘指数 + 市场概况 + 宏观指标"""
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
from datetime import date, timedelta

st.set_page_config(page_title="首页看板", page_icon="📊", layout="wide")

# ═══════════════════════════════════════════════
# 数据获取（带缓存）
# ═══════════════════════════════════════════════

INDEX_MAP = {
    "上证指数": "sh000001",
    "深证成指": "sz399001",
    "创业板指": "sz399006",
    "科创50": "sh000688",
    "沪深300": "sh000300",
}


@st.cache_data(ttl=300, show_spinner="正在获取大盘数据...")
def fetch_index_snapshot() -> pd.DataFrame:
    """获取主要指数最新行情（新浪HTTP源）。"""
    try:
        from smcore.data.quote_sina import fetch_sina_index_quotes
        quotes = fetch_sina_index_quotes(INDEX_MAP.values())
        if not quotes:
            return pd.DataFrame()
        rows = []
        for name, code in INDEX_MAP.items():
            code6 = code[2:]  # 去掉 sh/sz 前缀
            info = quotes.get(code6)
            if info and info.get("price"):
                price = info["price"]
                pre_close = info.get("pre_close")
                change_pct = ((price - pre_close) / pre_close * 100) if pre_close else 0.0
                change_amt = (price - pre_close) if pre_close else 0.0
                rows.append({
                    "指数": name,
                    "最新价": price,
                    "涨跌幅": change_pct,
                    "涨跌额": change_amt,
                })
        return pd.DataFrame(rows)
    except Exception as e:
        st.warning(f"获取指数数据失败：{e}")
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner="正在获取市场热度...")
def fetch_market_breadth() -> dict:
    """获取全市场涨跌家数（新浪源）。"""
    try:
        import akshare as ak
        df = ak.stock_zh_a_spot()
        if df is None or df.empty:
            return {}
        up = (df["涨跌幅"] > 0).sum()
        down = (df["涨跌幅"] < 0).sum()
        flat = (df["涨跌幅"] == 0).sum()
        total = len(df)
        return {
            "上涨": int(up),
            "下跌": int(down),
            "平盘": int(flat),
            "总数": total,
            "上涨比例": round(up / total * 100, 1) if total else 0,
        }
    except Exception:
        return {}


@st.cache_data(ttl=3600, show_spinner="正在获取宏观数据...")
def fetch_macro_snapshot() -> dict:
    """获取关键宏观指标。"""
    result = {}
    today = date.today()
    start = (today - timedelta(days=90)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    try:
        import akshare as ak
        # 离岸人民币
        usdcny = ak.currency_boc_sina(symbol="美元")
        if usdcny is not None and not usdcny.empty:
            last = usdcny.iloc[-1]
            result["美元/人民币"] = float(last.get("中行折算价", 0)) / 100 if "中行折算价" in last else None

        # Shibor 隔夜
        shibor = ak.rate_interbank(market="上海银行间同业拆放利率", symbol="Shibor", indicator="隔夜")
        if shibor is not None and not shibor.empty:
            result["Shibor隔夜"] = float(shibor.iloc[-1].get("利率", 0)) if "利率" in shibor else None
    except Exception:
        pass

    return result


# ═══════════════════════════════════════════════
# 页面渲染
# ═══════════════════════════════════════════════

st.title("📊 市场看板")

# --- 指数快照 ---
st.subheader("主要指数")
index_df = fetch_index_snapshot()

if not index_df.empty:
    cols = st.columns(len(index_df))
    for i, (_, row) in enumerate(index_df.iterrows()):
        change = row["涨跌幅"]
        color = "#E33E3E" if change > 0 else ("#009966" if change < 0 else "#666")
        arrow = "↑" if change > 0 else ("↓" if change < 0 else "→")
        with cols[i]:
            st.metric(
                label=row["指数"],
                value=f"{row['最新价']:.2f}",
                delta=f"{arrow} {change:+.2f}% | {row['涨跌额']:+.2f}",
            )
else:
    st.info("指数数据加载中，请稍候...")

st.markdown("---")

# --- 市场热度 ---
col1, col2 = st.columns([1, 1])
with col1:
    st.subheader("🔥 市场热度")
    breadth = fetch_market_breadth()
    if breadth:
        up_pct = breadth["上涨比例"]
        st.write(f"**上涨 {breadth['上涨']}** | **下跌 {breadth['下跌']}** | 平盘 {breadth['平盘']}")

        # 热度条
        st.progress(up_pct / 100, text=f"上涨占比 {up_pct}%")

        # 热度评价
        if up_pct >= 70:
            st.success("🟢 市场亢奋 — 注意追高风险")
        elif up_pct >= 45:
            st.info("🟡 温和偏暖 — 正常市场")
        elif up_pct >= 25:
            st.warning("🟠 偏冷 — 观望为主")
        else:
            st.error("🔴 极度冷淡 — 恐慌中孕育机会")

# --- 宏观指标 ---
with col2:
    st.subheader("🌍 宏观速览")
    macro = fetch_macro_snapshot()
    if macro:
        for key, val in macro.items():
            st.metric(label=key, value=f"{val:.4f}" if val else "N/A")
    else:
        st.caption("暂无宏观数据")

st.markdown("---")

# --- 最新选股结果 ---
st.subheader("📋 最新操作清单")

action_lists = sorted(ROOT.glob("stock_data/Daily-Action-List-*.csv"), reverse=True)
if action_lists:
    latest = action_lists[0]
    try:
        df_al = pd.read_csv(latest, encoding="utf-8-sig")
        st.caption(f"来源：{latest.name} | {len(df_al)} 只候选股")

        # 只显示关键列
        cols_show = [c for c in ["股票代码", "股票名称", "建议买入价", "综合评分", "建议仓位"]
                     if c in df_al.columns]
        if cols_show:
            st.dataframe(
                df_al[cols_show].head(10),
                use_container_width=True,
                hide_index=True,
            )
    except Exception:
        st.caption("暂无操作清单数据")
else:
    st.info("还没有操作清单。去「选股中心」跑一次选股吧 👉")

st.markdown("---")
st.caption(f"数据更新时间：{date.today()} | 数据来源：新浪财经")
