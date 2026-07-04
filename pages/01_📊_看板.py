"""首页看板：大盘指数 + 市场概况 + 宏观指标

数据一天只跑一次，结果持久化到 stock_data/daily_cache/。
当天没跑出来用前一天的，页面标注实际数据日期。
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

st.set_page_config(page_title="首页看板", page_icon="📊", layout="wide")

# ═══════════════════════════════════════════════
# 数据获取函数（新浪 API 优先 + akshare 兜底 + st.cache_data 会话缓存）
# ═══════════════════════════════════════════════

INDEX_MAP = {
    "上证指数": "sh000001",
    "深证成指": "sz399001",
    "创业板指": "sz399006",
    "科创50": "sh000688",
    "沪深300": "sh000300",
}


@st.cache_data(ttl=300)  # 5 分钟会话缓存，避免每次交互重新获取
def _get_index_snapshot():
    """获取主要指数最新行情（新浪HTTP源）。"""
    from smcore.data.quote_sina import fetch_sina_index_quotes
    quotes = fetch_sina_index_quotes(INDEX_MAP.values())
    if not quotes:
        return pd.DataFrame()
    rows = []
    for name, code in INDEX_MAP.items():
        code6 = code[2:]
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


@st.cache_data(ttl=300)
def _get_market_breadth():
    """获取全市场涨跌家数（akshare 新浪源，~25s 首次，后续走缓存）。"""
    try:
        import akshare as ak
        df = ak.stock_zh_a_spot()
        if df is not None and not df.empty:
            up = (df["涨跌幅"] > 0).sum()
            down = (df["涨跌幅"] < 0).sum()
            flat = (df["涨跌幅"] == 0).sum()
            total = len(df)
            return {
                "上涨": int(up), "下跌": int(down), "平盘": int(flat),
                "总数": total,
                "上涨比例": round(up / total * 100, 1) if total else 0,
            }
    except Exception:
        pass
    return None


@st.cache_data(ttl=300)
def _get_macro_snapshot():
    """获取关键宏观指标。"""
    try:
        import akshare as ak
        from datetime import timedelta
        result = {}
        today = date.today()
        start = (today - timedelta(days=90)).strftime("%Y%m%d")
        end = today.strftime("%Y%m%d")

        try:
            usdcny = ak.currency_boc_sina(symbol="美元")
            if usdcny is not None and not usdcny.empty:
                last = usdcny.iloc[-1]
                result["美元/人民币"] = float(last.get("中行折算价", 0)) / 100 if "中行折算价" in last else None
        except Exception:
            pass

        try:
            shibor = ak.rate_interbank(market="上海银行间同业拆放利率", symbol="Shibor", indicator="隔夜")
            if shibor is not None and not shibor.empty:
                result["Shibor隔夜"] = float(shibor.iloc[-1].get("利率", 0)) if "利率" in shibor else None
        except Exception:
            pass

        return result if result else None
    except Exception:
        return None


from smcore.cache_daily import get_daily, force_refresh
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout


def _clear_st_cache():
    """清除 st.cache_data 会话缓存。"""
    _get_index_snapshot.clear()
    _get_market_breadth.clear()
    _get_macro_snapshot.clear()


def _fetch_all_parallel():
    """并行获取三组数据，返回 (index_df, breadth, macro, index_date, breadth_date, macro_date)。"""
    def _idx():
        return get_daily("index_snapshot", _get_index_snapshot)
    def _brd():
        return get_daily("market_breadth", _get_market_breadth)
    def _mac():
        return get_daily("macro_snapshot", _get_macro_snapshot)

    with ThreadPoolExecutor(max_workers=3) as pool:
        f_idx, f_brd, f_mac = pool.submit(_idx), pool.submit(_brd), pool.submit(_mac)
        try:
            index_df, index_date = f_idx.result(timeout=30)
        except Exception:
            index_df, index_date = pd.DataFrame(), None
        try:
            breadth, breadth_date = f_brd.result(timeout=30)
        except Exception:
            breadth, breadth_date = None, None
        try:
            macro, macro_date = f_mac.result(timeout=30)
        except Exception:
            macro, macro_date = None, None

    return index_df, breadth, macro, index_date, breadth_date, macro_date


# ═══════════════════════════════════════════════
# 页面渲染
# ═══════════════════════════════════════════════

st.title("📊 市场看板")

# 强制刷新按钮
col_refresh, col_date = st.columns([1, 4])
with col_refresh:
    if st.button("🔄 刷新数据", help="丢弃今天缓存，重新获取"):
        force_refresh("index_snapshot")
        force_refresh("market_breadth")
        force_refresh("macro_snapshot")
        _clear_st_cache()
        st.rerun()

# 并行获取所有数据（三个请求同时发出，总耗时 ≈ 最慢的一个，而非三者之和）
index_df, breadth, macro, index_date, breadth_date, macro_date = _fetch_all_parallel()

# --- 指数快照 ---
st.subheader("主要指数")

if index_df is not None and not index_df.empty:
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

# --- 市场热度 & 宏观指标 ---
col1, col2 = st.columns([1, 1])
with col1:
    st.subheader("🔥 市场热度")
    if breadth:
        up_pct = breadth["上涨比例"]
        st.write(f"**上涨 {breadth['上涨']}** | **下跌 {breadth['下跌']}** | 平盘 {breadth['平盘']}")

        st.progress(up_pct / 100, text=f"上涨占比 {up_pct}%")

        if up_pct >= 70:
            st.success("🟢 市场亢奋 — 注意追高风险")
        elif up_pct >= 45:
            st.info("🟡 温和偏暖 — 正常市场")
        elif up_pct >= 25:
            st.warning("🟠 偏冷 — 观望为主")
        else:
            st.error("🔴 极度冷淡 — 恐慌中孕育机会")
    elif breadth_date:
        st.caption(f"暂无今日数据（上次有效：{breadth_date}），请刷新重试")
    else:
        st.caption("市场热度数据获取失败，请刷新重试")

# --- 宏观指标 ---
with col2:
    st.subheader("🌍 宏观速览")
    if macro:
        for key, val in macro.items():
            st.metric(label=key, value=f"{val:.4f}" if val else "N/A")
    elif macro_date:
        st.caption(f"暂无今日数据（上次有效：{macro_date}），请刷新重试")
    else:
        st.caption("宏观数据获取失败，请刷新重试")

st.markdown("---")

# --- 最新选股结果 ---
st.subheader("📋 最新操作清单")

action_lists = sorted(ROOT.glob("stock_data/Daily-Action-List-*.csv"), reverse=True)
if action_lists:
    latest = action_lists[0]
    try:
        df_al = pd.read_csv(latest, encoding="utf-8-sig")
        st.caption(f"来源：{latest.name} | {len(df_al)} 只候选股")

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

# 数据日期标注
dates_used = [d for d in [index_date, breadth_date, macro_date] if d]
if dates_used:
    unique_dates = sorted(set(dates_used))
    if len(unique_dates) == 1:
        st.caption(f"📊 数据日期：{unique_dates[0]} | 数据来源：新浪财经 | 点「刷新数据」可重新获取")
    else:
        parts = []
        if index_date:
            parts.append(f"指数 {index_date}")
        if breadth_date:
            parts.append(f"热度 {breadth_date}")
        if macro_date:
            parts.append(f"宏观 {macro_date}")
        st.caption(f"📊 数据日期：{' / '.join(parts)} | 数据来源：新浪财经")
else:
    st.caption("数据来源：新浪财经")
