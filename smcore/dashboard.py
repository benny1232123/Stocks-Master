"""Dashboard data helpers shared by the API and cache prewarm script."""
from __future__ import annotations

import concurrent.futures
import os
import pickle
import threading
import time

import requests
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
    """Apply the runtime defaults needed by the data layer.

    K 线默认优先通达信（最快最稳）；CI/云端若无 pytdx 或不可达，kline 的
    回退链会自动切到 akshare，无需手动配置。
    """
    os.environ.setdefault("KLINE_BACKEND", "tdx")


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
    """Fetch the latest index snapshot.

    优先通达信直连（毫秒级、稳），失败回退新浪 HTTP 源。
    """
    # 通达信优先
    try:
        from smcore.data.tdx_client import available as tdx_available, get_client
        if tdx_available():
            cli = get_client()
            snap = _call_with_timeout(lambda: cli.get_index_snapshot(INDEX_MAP), 20)
            if snap:
                return pd.DataFrame(snap)
    except Exception as exc:
        print(f"[dashboard] 指数快照 Tdx 失败，回退新浪: {exc}")

    # 回退：新浪 HTTP
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


# ── 东方财富轻量计数接口（海外友好主源）─────────────────────────────
# 不拉全量快照，而是按涨跌幅过滤后读取 data.total（单次仅返回计数，
# payload 极小）。东财 push2 CDN 与日 K 线同源，海外可达（GitHub/Render）。
_EM_FS_ALL = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
_EM_HOSTS = [
    "https://82.push2.eastmoney.com/api/qt/clist/get",
    "https://push2.eastmoney.com/api/qt/clist/get",
]
_EM_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://quote.eastmoney.com/",
}


def _em_breadth_count(fs: str) -> int | None:
    """单次请求东财 clist，按过滤条件读取 data.total（仅计数）。"""
    params = {
        "pn": 1,
        "pz": 1,
        "po": 1,
        "np": 1,
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": fs,
        "fields": "f12,f14",
        "_": int(time.time() * 1000),
    }
    last_err: Exception | None = None
    for host in _EM_HOSTS:
        try:
            r = requests.get(host, params=params, timeout=10, headers=_EM_HEADERS)
            j = r.json()
            data = j.get("data")
            if isinstance(data, dict) and data.get("total") is not None:
                return int(data["total"])
        except Exception as exc:  # noqa: BLE001 - 换 host 重试
            last_err = exc
            continue
    if last_err:
        raise last_err
    return None


def _fetch_breadth_eastmoney_count() -> dict[str, Any] | None:
    """海外友好的市场宽度：东财计数接口，4 次 tiny 请求，不拉全量快照。"""
    up = _em_breadth_count(_EM_FS_ALL + "+f3>0")
    dn = _em_breadth_count(_EM_FS_ALL + "+f3<0")
    fl = _em_breadth_count(_EM_FS_ALL + "+f3=0")
    tot = _em_breadth_count(_EM_FS_ALL)
    if not tot:
        return None
    return {
        "上涨": int(up or 0),
        "下跌": int(dn or 0),
        "平盘": int(fl or 0),
        "总数": int(tot),
        "上涨比例": round((up or 0) / tot * 100, 1),
    }


def fetch_market_breadth() -> dict[str, Any] | None:
    """Fetch the market breadth snapshot (up/down counts across A-shares).

    数据链路口径（云端=海外，本地=国内）：
      1) 通达信直连（仅本机/国内可达，毫秒级全市场，最准）
      2) 东财计数接口（轻量 4 次 tiny 请求，海外友好 —— 云端主源）
      3) 全量快照兜底（东方财富/新浪，重，海外可能超时，仅最后手段）
    任意一层拿到数据即返回，全失败返回 None（前端显示「暂无」而非崩溃）。
    """
    # 1) 通达信优先（直连券商行情服务器，全市场快照毫秒到秒级；云端无 pytdx 自动跳过）
    try:
        from smcore.data.tdx_client import available as tdx_available, get_client
        if tdx_available():
            cli = get_client()
            b = _call_with_timeout(cli.get_market_breadth, 30)
            if b and b.get("总数"):
                return b
    except Exception as exc:
        print(f"[dashboard] 市场宽度 Tdx 失败，回退: {exc}")

    # 2) 东财计数接口（轻量，海外友好 —— 云端主源）
    b = _safe_fetch(_fetch_breadth_eastmoney_count, 30, "市场宽度(东财计数)", None, retries=2)
    if b:
        return b

    # 3) 全量快照兜底（重，海外可能超时）
    import akshare as ak

    for name, fn in (
        ("东方财富", lambda: ak.stock_zh_a_spot_em()),
        ("新浪", lambda: ak.stock_zh_a_spot()),
    ):
        df = _safe_fetch(fn, 45, f"市场宽度({name})", None, retries=1)
        if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
            chg_col = "涨跌幅" if "涨跌幅" in df.columns else None
            if chg_col is None:
                continue
            up = (df[chg_col] > 0).sum()
            dn = (df[chg_col] < 0).sum()
            fl = (df[chg_col] == 0).sum()
            tot = len(df)
            return {
                "上涨": int(up),
                "下跌": int(dn),
                "平盘": int(fl),
                "总数": int(tot),
                "上涨比例": round(up / tot * 100, 1) if tot else 0,
            }
    return None


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

    # --- 源 1: macro_china_shibor_all（主源，最快最稳）---
    try:
        df = _call_with_retry(
            lambda: ak.macro_china_shibor_all(),
            DASHBOARD_API_TIMEOUT,
        )
        if df is not None and not df.empty and "O/N-定价" in df.columns:
            val = float(df.iloc[-1]["O/N-定价"])
            if val > 0:
                print(f"[dashboard] SHIBOR 隔夜 (macro_china_shibor_all): {val}%")
                return val
    except Exception as exc:
        print(f"[dashboard] SHIBOR 源1 macro_china_shibor_all 失败: {exc}")

    # --- 源 2: rate_interbank（备源）---
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
        print(f"[dashboard] SHIBOR 源2 rate_interbank 失败: {exc}")

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