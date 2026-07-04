"""预热看板数据：预计算 index/market_breadth/macro 缓存，commit 到仓库。

由 GitHub Actions 每日 cron 调用，结果 commit 到 git。
Render 部署时仓库已包含最新缓存，页面秒开。

用法：
    python scripts/prewarm_dashboard.py
"""
from __future__ import annotations

import os
import pickle
import sys
from datetime import date
from pathlib import Path

# 路径设置
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.setdefault("KLINE_BACKEND", "akshare")

import pandas as pd

# ── 复用看板的三个数据获取函数 ──
# 直接 import smcore 而非看板页面（避免 streamlit 依赖）

INDEX_MAP = {
    "上证指数": "sh000001",
    "深证成指": "sz399001",
    "创业板指": "sz399006",
    "科创50": "sh000688",
    "沪深300": "sh000300",
}

CACHE_DIR = ROOT / "stock_data" / "daily_cache"


def fetch_index_snapshot() -> pd.DataFrame:
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


def fetch_market_breadth() -> dict:
    """获取全市场涨跌家数（akshare 新浪源）。"""
    import akshare as ak
    df = ak.stock_zh_a_spot()
    if df is None or df.empty:
        return None
    up = (df["涨跌幅"] > 0).sum()
    down = (df["涨跌幅"] < 0).sum()
    flat = (df["涨跌幅"] == 0).sum()
    total = len(df)
    return {
        "上涨": int(up), "下跌": int(down), "平盘": int(flat),
        "总数": total,
        "上涨比例": round(up / total * 100, 1) if total else 0,
    }


def fetch_macro_snapshot() -> dict:
    """获取关键宏观指标。"""
    import akshare as ak
    from datetime import timedelta
    result = {}
    today = date.today()

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


def save_cache(key: str, data) -> None:
    """保存单个缓存到 stock_data/daily_cache/。"""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    today = date.today().strftime("%Y-%m-%d")
    path = CACHE_DIR / f"{key}_{today}.pkl"
    with open(path, "wb") as f:
        pickle.dump(data, f)
    print(f"  ✓ {path.name}")


def clean_old_cache(keep_days: int = 7) -> int:
    """清理超过 keep_days 天的旧缓存文件。"""
    if not CACHE_DIR.exists():
        return 0
    from datetime import datetime
    cutoff = datetime.now().timestamp() - keep_days * 86400
    count = 0
    for f in CACHE_DIR.glob("*.pkl"):
        if f.stat().st_mtime < cutoff:
            f.unlink(missing_ok=True)
            count += 1
    return count


def main():
    print("🔥 预热看板数据...")

    # 1. 指数快照（新浪 HTTP，秒级）
    print("  [1/3] 指数快照...")
    try:
        idx = fetch_index_snapshot()
        if idx is not None and not idx.empty:
            save_cache("index_snapshot", idx)
        else:
            print("  ⚠ 指数快照为空，跳过")
    except Exception as e:
        print(f"  ✗ 指数快照失败: {e}")

    # 2. 市场涨跌家数（akshare，~25s）
    print("  [2/3] 市场涨跌家数（~25s）...")
    try:
        breadth = fetch_market_breadth()
        if breadth:
            save_cache("market_breadth", breadth)
        else:
            print("  ⚠ 涨跌家数为空，跳过")
    except Exception as e:
        print(f"  ✗ 涨跌家数失败: {e}")

    # 3. 宏观指标（akshare，~10s）
    print("  [3/3] 宏观指标...")
    try:
        macro = fetch_macro_snapshot()
        if macro:
            save_cache("macro_snapshot", macro)
        else:
            print("  ⚠ 宏观指标为空，跳过")
    except Exception as e:
        print(f"  ✗ 宏观指标失败: {e}")

    # 4. 清理旧缓存
    removed = clean_old_cache(keep_days=7)
    if removed:
        print(f"  🗑 清理了 {removed} 个旧缓存文件")

    print("✅ 预热完成")


if __name__ == "__main__":
    main()
