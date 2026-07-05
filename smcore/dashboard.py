"""Dashboard data helpers shared by the API and cache prewarm script."""
from __future__ import annotations

import os
import pickle
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJECT_ROOT / "stock_data" / "daily_cache"

INDEX_MAP = {
    "上证指数": "sh000001",
    "深证成指": "sz399001",
    "创业板指": "sz399006",
    "科创50": "sh000688",
    "沪深300": "sh000300",
}


def configure_runtime() -> None:
    """Apply the runtime defaults needed by the data layer."""
    os.environ.setdefault("KLINE_BACKEND", "akshare")


def fetch_index_snapshot() -> pd.DataFrame:
    """Fetch the latest index snapshot from the Sina HTTP source."""
    from smcore.data.quote_sina import fetch_sina_index_quotes

    quotes = fetch_sina_index_quotes(INDEX_MAP.values())
    if not quotes:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for name, code in INDEX_MAP.items():
        code6 = code[2:]
        info = quotes.get(code6)
        if info and info.get("price") is not None:
            price = float(info["price"])
            pre_close = info.get("pre_close")
            change_pct = ((price - pre_close) / pre_close * 100) if pre_close else 0.0
            change_amt = (price - pre_close) if pre_close else 0.0
            rows.append(
                {
                    "指数": name,
                    "最新价": price,
                    "涨跌幅": change_pct,
                    "涨跌额": change_amt,
                }
            )
    return pd.DataFrame(rows)


def fetch_market_breadth() -> dict[str, Any] | None:
    """Fetch the market breadth snapshot."""
    import akshare as ak

    df = ak.stock_zh_a_spot()
    if df is None or df.empty:
        return None

    up = (df["涨跌幅"] > 0).sum()
    down = (df["涨跌幅"] < 0).sum()
    flat = (df["涨跌幅"] == 0).sum()
    total = len(df)
    return {
        "上涨": int(up),
        "下跌": int(down),
        "平盘": int(flat),
        "总数": int(total),
        "上涨比例": round(up / total * 100, 1) if total else 0,
    }


def fetch_macro_snapshot() -> dict[str, Any] | None:
    """Fetch a small macro snapshot for the dashboard."""
    import akshare as ak

    result: dict[str, Any] = {}

    try:
        usdcny = ak.currency_boc_sina(symbol="美元")
        if usdcny is not None and not usdcny.empty:
            last = usdcny.iloc[-1]
            if "中行折算价" in last:
                result["美元/人民币"] = float(last.get("中行折算价", 0)) / 100
    except Exception:
        pass

    try:
        shibor = ak.rate_interbank(market="上海银行间同业拆放利率", symbol="Shibor", indicator="隔夜")
        if shibor is not None and not shibor.empty:
            if "利率" in shibor.columns:
                result["Shibor隔夜"] = float(shibor.iloc[-1].get("利率", 0))
    except Exception:
        pass

    return result or None


def build_dashboard_payload() -> dict[str, Any]:
    """Build a JSON-friendly dashboard payload for the frontend."""
    payload: dict[str, Any] = {"generated_at": datetime.now().isoformat(timespec="seconds")}

    index_snapshot = fetch_index_snapshot()
    if not index_snapshot.empty:
        payload["index_snapshot"] = index_snapshot.to_dict(orient="records")
    else:
        payload["index_snapshot"] = []

    payload["market_breadth"] = fetch_market_breadth() or {}
    payload["macro_snapshot"] = fetch_macro_snapshot() or {}
    return payload


def save_cache(key: str, data: Any) -> Path:
    """Save a dashboard cache file under stock_data/daily_cache."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    today = date.today().strftime("%Y-%m-%d")
    path = CACHE_DIR / f"{key}_{today}.pkl"
    with open(path, "wb") as file_handle:
        pickle.dump(data, file_handle)
    return path


def clean_old_cache(keep_days: int = 7) -> int:
    """Delete cache files older than keep_days."""
    if not CACHE_DIR.exists():
        return 0

    cutoff = datetime.now().timestamp() - keep_days * 86400
    removed = 0
    for file_path in CACHE_DIR.glob("*.pkl"):
        if file_path.stat().st_mtime < cutoff:
            file_path.unlink(missing_ok=True)
            removed += 1
    return removed


def prewarm_dashboard_cache(keep_days: int = 7) -> dict[str, Any]:
    """Refresh the dashboard cache files used by the UI."""
    configure_runtime()

    result: dict[str, Any] = {}

    index_snapshot = fetch_index_snapshot()
    if not index_snapshot.empty:
        result["index_snapshot"] = save_cache("index_snapshot", index_snapshot).name

    market_breadth = fetch_market_breadth()
    if market_breadth:
        result["market_breadth"] = save_cache("market_breadth", market_breadth).name

    macro_snapshot = fetch_macro_snapshot()
    if macro_snapshot:
        result["macro_snapshot"] = save_cache("macro_snapshot", macro_snapshot).name

    result["removed_cache_files"] = clean_old_cache(keep_days=keep_days)
    return result