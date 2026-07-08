"""Dashboard data helpers shared by the API and cache prewarm script."""
from __future__ import annotations

import concurrent.futures
import os
import pickle
import threading
import time
from datetime import date, datetime, timedelta
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


# 看板数据拉取超时（秒）。超时即视为失败并跳过该数据源，避免单接口卡死拖垮预热。
# 默认 60s：stock_zh_a_spot 拉全市场实时快照较大，CI 网络慢时需更长时间。
DASHBOARD_API_TIMEOUT = float(os.getenv("DASHBOARD_API_TIMEOUT", "60"))


def _call_with_timeout(func, timeout_seconds):
    """单任务超时包装：daemon 线程执行，超时抛 TimeoutError。非并发，不增加接口压力；daemon 线程超时后不阻塞进程退出。"""
    box: dict[str, Any] = {}

    def _run() -> None:
        try:
            box["result"] = func()
        except BaseException as err:  # noqa: BLE001 - 透传异常到主线程
            box["error"] = err

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    worker.join(timeout=timeout_seconds)
    if worker.is_alive():
        raise concurrent.futures.TimeoutError(f"调用超时（>{timeout_seconds}s）")
    if "error" in box:
        raise box["error"]
    return box.get("result")


def _call_with_retry(func, timeout_seconds, retries=2, backoff=3.0):
    """超时调用 + 重试：瞬断网络/超时错误自动重试，避免一次失败就放弃数据源。"""
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return _call_with_timeout(func, timeout_seconds)
        except Exception as exc:  # noqa: BLE001 - 透传异常，重试后由调用方决定
            last_exc = exc
            if attempt < retries:
                print(f"[dashboard] 调用失败，{backoff}s 后重试 ({attempt + 1}/{retries}): {exc}")
                time.sleep(backoff)
                continue
    raise last_exc


def _safe_fetch(func, timeout_seconds, label, default, retries=2):
    """超时 + 重试 + 容错：重试耗尽仍失败才返回 default。"""
    try:
        return _call_with_retry(func, timeout_seconds, retries=retries)
    except Exception as exc:
        print(f"[dashboard] {label} 获取失败（已跳过）: {exc}")
        return default


def _load_cache(key: str) -> Any:
    """Load a dated cache file if it exists."""
    today = date.today().strftime("%Y-%m-%d")
    path = CACHE_DIR / f"{key}_{today}.pkl"
    if not path.exists():
        return None
    try:
        with open(path, "rb") as file_handle:
            return pickle.load(file_handle)
    except Exception:
        return None


def fetch_index_snapshot() -> pd.DataFrame:
    """Fetch the latest index snapshot from the Sina HTTP source."""
    from smcore.data.quote_sina import fetch_sina_index_quotes

    try:
        quotes = fetch_sina_index_quotes(INDEX_MAP.values())
    except Exception as exc:
        print(f"[dashboard] 指数快照获取失败（已跳过）: {exc}")
        return pd.DataFrame()
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

    df = _safe_fetch(lambda: ak.stock_zh_a_spot(), DASHBOARD_API_TIMEOUT, "市场宽度", None)
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
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
        usdcny = _call_with_retry(lambda: ak.currency_boc_sina(symbol="美元"), DASHBOARD_API_TIMEOUT)
        if usdcny is not None and not usdcny.empty:
            last = usdcny.iloc[-1]
            if "中行折算价" in last:
                result["美元/人民币"] = float(last.get("中行折算价", 0)) / 100
    except Exception:
        pass

    # ── SHIBOR 隔夜（多源回退 + 昨日缓存兜底）─────────────────────────
    # ak.rate_interbank 经常返回空/超时，这里依次尝试多个数据源，
    # 全部失败时读取昨日缓存，避免前端显示 "--"。
    result["Shibor隔夜"] = _fetch_shibor_overnight()

    return result or None


def _fetch_shibor_overnight() -> float | None:
    """获取 SHIBOR 隔夜利率，多源回退 + 昨日缓存兜底。

    数据源优先级：
      1) ak.rate_interbank（主源，实时）
      2) ak.shibor_data（备源，历史数据取最新）
      3) 昨日 macro_snapshot 缓存文件（兜底，保证不返回 None）

    Returns:
        float | None: 隔夜利率（%），None 仅在完全无法获取时返回。
    """
    import akshare as ak

    # --- 源 1: rate_interbank（当前主源）---
    try:
        shibor = _call_with_retry(
            lambda: ak.rate_interbank(market="上海银行间同业拆放利率", symbol="Shibor", indicator="隔夜"),
            DASHBOARD_API_TIMEOUT,
        )
        if shibor is not None and not shibor.empty and "利率" in shibor.columns:
            val = float(shibor.iloc[-1].get("利率", 0))
            if val > 0:
                print(f"[dashboard] SHIBOR 隔夜 (rate_interbank): {val}%")
                return val
    except Exception as exc:
        print(f"[dashboard] SHIBOR 源1 rate_interbank 失败: {exc}")

    # --- 源 2: shibor_data（备选历史接口）---
    try:
        df = _call_with_retry(
            lambda: ak.shibor_data(),
            DASHBOARD_API_TIMEOUT,
        )
        if df is not None and not df.empty:
            # 取最近一行隔夜利率
            last = df.iloc[-1]
            for col in ("隔夜", "O/N", "ON"):
                if col in last and pd.notna(last[col]):
                    val = float(last[col])
                    if val > 0:
                        print(f"[dashboard] SHIBOR 隔夜 (shibor_data, col={col}): {val}%")
                        return val
    except Exception as exc:
        print(f"[dashboard] SHIBOR 源2 shibor_data 失败: {exc}")

    # --- 源 3: shibor_quote_history（备选报价接口）---
    try:
        df = _call_with_retry(
            lambda: ak.shibor_quote_history(symbol="隔夜"),
            DASHBOARD_API_TIMEOUT,
        )
        if df is not None and not df.empty:
            last = df.iloc[-1]
            for col in ("最新价", "利率", "price"):
                if col in last and pd.notna(last[col]):
                    val = float(last[col])
                    if val > 0:
                        print(f"[dashboard] SHIBOR 隔夜 (shibor_quote_history): {val}%")
                        return val
    except Exception as exc:
        print(f"[dashboard] SHIBOR 源3 shibor_quote_history 失败: {exc}")

    # --- 兜底: 读昨日缓存 ---
    try:
        yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        cache_path = CACHE_DIR / f"macro_snapshot_{yesterday}.pkl"
        if cache_path.exists():
            with open(cache_path, "rb") as f:
                cached = pickle.load(f)
            cached_val = cached.get("Shibor隔夜")
            if cached_val is not None and float(cached_val) > 0:
                print(f"[dashboard] SHIBOR 隔夜 (昨日缓存兜底): {cached_val}%")
                return float(cached_val)
    except Exception as exc:
        print(f"[dashboard] SHIBOR 缓存兜底读取失败: {exc}")

    print("[dashboard] ⚠️ SHIBOR 隔夜所有数据源均失败，返回 None")
    return None


def build_dashboard_payload() -> dict[str, Any]:
    """Build a JSON-friendly dashboard payload for the frontend."""
    payload: dict[str, Any] = {"generated_at": datetime.now().isoformat(timespec="seconds")}

    cached_index = _load_cache("index_snapshot")
    if isinstance(cached_index, pd.DataFrame) and not cached_index.empty:
        payload["index_snapshot"] = cached_index.to_dict(orient="records")
    else:
        payload["index_snapshot"] = []

    cached_breadth = _load_cache("market_breadth")
    payload["market_breadth"] = cached_breadth if isinstance(cached_breadth, dict) else {}

    cached_macro = _load_cache("macro_snapshot")
    payload["macro_snapshot"] = cached_macro if isinstance(cached_macro, dict) else {}
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