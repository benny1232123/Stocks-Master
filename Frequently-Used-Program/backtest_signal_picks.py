from __future__ import annotations

import argparse
import re
from datetime import datetime, timedelta
from pathlib import Path

import akshare as ak
import pandas as pd
from pandas.errors import EmptyDataError
from functools import lru_cache


ROOT_DIR = Path(__file__).resolve().parents[1]
STOCK_DATA_DIR = ROOT_DIR / "stock_data"
LOCAL_K_CACHE_DIR = ROOT_DIR / "Frequently-Used-Program" / "boll-visualizer" / "stock_data" / "cache" / "k_data"
TRADES_COLUMNS = [
    "策略",
    "信号日期",
    "股票代码",
    "股票名称",
    "买入日期",
    "卖出日期",
    "买入价(次日开盘)",
    "卖出价(持有N日收盘)",
    "成交买入价(含滑点)",
    "成交卖出价(含滑点)",
    "持有交易日",
    "毛收益率(%)",
    "净收益率(%)",
    "交易成本冲击(%)",
    "区间最大回撤(%)",
]
DEFAULT_STRATEGY_WEIGHTS = {
    "boll": 40.0,
    "theme": 25.0,
    "relativity": 20.0,
    "cctv": 10.0,
    "cash": 5.0,
}
STRATEGY_KEYS = ["boll", "theme", "relativity", "cctv", "cash"]


def _to_ak_a_symbol(symbol: str) -> str:
    return str(symbol).strip()


def _to_ak_daily_symbol(symbol: str) -> str:
    code = str(symbol).strip()
    digits = "".join(ch for ch in code if ch.isdigit())
    if len(digits) != 6:
        return code
    return ("sh" + digits) if digits.startswith("6") else ("sz" + digits)


def _to_ak_index_symbol(index_code: str) -> str:
    text = str(index_code).strip().lower().replace(".", "")
    if text.startswith("sh") or text.startswith("sz"):
        return text
    if text.isdigit() and len(text) == 6:
        return ("sh" + text) if text.startswith("0") else ("sz" + text)
    return text


def _fetch_hist_via_akshare(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    def _normalize_from_cn(df: pd.DataFrame) -> pd.DataFrame:
        col_map = {str(c).strip(): str(c) for c in df.columns}
        date_col = col_map.get("日期", "")
        open_col = col_map.get("开盘", "")
        close_col = col_map.get("收盘", "")
        if not date_col or not open_col or not close_col:
            return pd.DataFrame()

        out_df = pd.DataFrame()
        out_df["date"] = pd.to_datetime(df[date_col], errors="coerce")
        out_df["open"] = pd.to_numeric(df[open_col], errors="coerce")
        out_df["close"] = pd.to_numeric(df[close_col], errors="coerce")
        out_df = out_df.dropna(subset=["date", "open", "close"]).sort_values("date").reset_index(drop=True)
        return out_df

    def _normalize_from_en(df: pd.DataFrame) -> pd.DataFrame:
        col_map = {str(c).strip().lower(): str(c) for c in df.columns}
        date_col = col_map.get("date", "")
        open_col = col_map.get("open", "")
        close_col = col_map.get("close", "")
        if not date_col or not open_col or not close_col:
            return pd.DataFrame()

        out_df = pd.DataFrame()
        out_df["date"] = pd.to_datetime(df[date_col], errors="coerce")
        out_df["open"] = pd.to_numeric(df[open_col], errors="coerce")
        out_df["close"] = pd.to_numeric(df[close_col], errors="coerce")
        out_df = out_df.dropna(subset=["date", "open", "close"]).sort_values("date").reset_index(drop=True)
        return out_df

    # Primary path: Eastmoney history endpoint.
    try:
        raw = ak.stock_zh_a_hist(
            symbol=_to_ak_a_symbol(symbol),
            period="daily",
            start_date=str(start_date),
            end_date=str(end_date),
            adjust="qfq",
        )
        if raw is not None and not raw.empty:
            out = _normalize_from_cn(raw)
            if not out.empty:
                return out
    except Exception:
        pass

    # Fallback path: Sina daily endpoint is more stable in some network environments.
    try:
        raw_daily = ak.stock_zh_a_daily(
            symbol=_to_ak_daily_symbol(symbol),
            start_date=str(start_date),
            end_date=str(end_date),
            adjust="qfq",
        )
        if raw_daily is not None and not raw_daily.empty:
            out = _normalize_from_en(raw_daily)
            if not out.empty:
                return out
    except Exception:
        pass

    return pd.DataFrame()


@lru_cache(maxsize=2048)
def _load_symbol_history(symbol: str) -> pd.DataFrame:
    symbol = str(symbol).strip()
    local_cache_path = LOCAL_K_CACHE_DIR / f"{symbol}_qfq_full.csv"
    if local_cache_path.exists():
        cached_df = pd.read_csv(local_cache_path, encoding="utf-8-sig")
        if not cached_df.empty and all(col in cached_df.columns for col in ["date", "open", "close"]):
            out = pd.DataFrame()
            out["date"] = pd.to_datetime(cached_df["date"], errors="coerce")
            out["open"] = pd.to_numeric(cached_df["open"], errors="coerce")
            out["close"] = pd.to_numeric(cached_df["close"], errors="coerce")
            out = out.dropna(subset=["date", "open", "close"]).sort_values("date").reset_index(drop=True)
            if not out.empty:
                return out

    return pd.DataFrame()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按每日选股信号CSV回测策略表现")
    parser.add_argument(
        "--signals-glob",
        default="stock_data/Stock-Selection-Boll-*.csv",
        help="信号文件通配符（相对项目根目录）",
    )
    parser.add_argument("--top-n", type=int, default=10, help="每个信号日仅回测前N只")
    parser.add_argument("--hold-days", type=int, default=5, help="持有交易日天数")
    parser.add_argument("--start-date", default="", help="起始信号日 YYYYMMDD")
    parser.add_argument("--end-date", default="", help="结束信号日 YYYYMMDD")
    parser.add_argument("--buy-slip-bps", type=float, default=5.0, help="买入滑点（基点），默认 5 bps")
    parser.add_argument("--sell-slip-bps", type=float, default=5.0, help="卖出滑点（基点），默认 5 bps")
    parser.add_argument("--buy-fee-rate", type=float, default=0.0003, help="买入佣金费率，默认 0.0003")
    parser.add_argument("--sell-fee-rate", type=float, default=0.0003, help="卖出佣金费率，默认 0.0003")
    parser.add_argument("--sell-stamp-tax-rate", type=float, default=0.001, help="卖出印花税费率，默认 0.001")
    parser.add_argument(
        "--daily-strategy-ratios",
        default="",
        help="每日策略配比，格式: boll=40,theme=25,relativity=20,cctv=10,cash=5（总和自动归一）",
    )
    parser.add_argument(
        "--daily-ratios-csv",
        default="",
        help="按日期策略配比CSV（列示例: 信号日期,boll,theme,relativity,cctv,cash）",
    )
    parser.add_argument(
        "--auto-market-ratios",
        action="store_true",
        help="按每日市场状态自动决定配比（配比CSV优先级更高）",
    )
    parser.add_argument("--initial-capital", type=float, default=100000.0, help="组合回测初始资金，默认 100000")
    parser.add_argument(
        "--output-prefix",
        default="",
        help="输出文件前缀，默认 stock_data/Signal-Backtest-YYYYMMDD",
    )
    parser.add_argument(
        "--relativity-min-down-ratio-pct",
        type=float,
        default=70.0,
        help="Relativity 最低抗跌满足率(%%)，默认 70；同时兼容 0~1 或 0~100 数据口径",
    )
    parser.add_argument(
        "--theme-cctv-only",
        dest="theme_cctv_only",
        action="store_true",
        default=True,
        help="题材策略仅回测命中 CCTV 股票池的标的（默认开启）",
    )
    parser.add_argument(
        "--disable-theme-cctv-only",
        dest="theme_cctv_only",
        action="store_false",
        help="关闭题材策略 CCTV 过滤",
    )
    parser.add_argument(
        "--allow-cache-fallback",
        dest="allow_cache_fallback",
        action="store_true",
        default=True,
        help="当 akshare 拉取失败时允许回退本地缓存（默认开启）",
    )
    parser.add_argument(
        "--no-cache-fallback",
        dest="allow_cache_fallback",
        action="store_false",
        help="禁用本地缓存兜底，强制仅使用在线数据",
    )
    return parser.parse_args()


def _resolve_path(path_text: str) -> Path:
    p = Path(path_text)
    if not p.is_absolute():
        p = (ROOT_DIR / p).resolve()
    return p


def _extract_signal_date_from_file(path: Path) -> str:
    # 兼容 UI 复制文件时添加前缀（如 0001_...），优先提取形如 20xxxxxx 的日期片段。
    candidates = re.findall(r"(\d{8})", path.stem)
    if not candidates:
        return ""

    for token in reversed(candidates):
        if token.startswith("20"):
            return token

    return candidates[-1]


def _normalize_code(value: object) -> str:
    text = str(value or "")
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _infer_strategy_from_file(path: Path) -> str:
    stem = path.stem.lower()
    if "stock-selection-boll" in stem:
        return "boll"
    if "stock-selection-relativity" in stem:
        return "relativity"
    if "stock-selection-ashare-theme-turnover" in stem:
        return "theme"
    if "stock-selection-cctv-sectors" in stem:
        return "cctv"
    return "other"


def _parse_daily_strategy_weights(raw_text: str) -> dict[str, float]:
    weights = {k: float(v) for k, v in DEFAULT_STRATEGY_WEIGHTS.items()}
    text = str(raw_text or "").strip()
    if not text:
        return weights

    tokens = [t.strip() for t in text.split(",") if t.strip()]
    for token in tokens:
        if "=" not in token:
            continue
        key, value_text = token.split("=", 1)
        key = key.strip().lower()
        if key not in weights:
            continue
        try:
            value = float(value_text.strip())
        except Exception:
            continue
        weights[key] = max(value, 0.0)

    total = sum(weights.values())
    if total <= 1e-12:
        return {k: float(v) for k, v in DEFAULT_STRATEGY_WEIGHTS.items()}

    return {k: (v * 100.0 / total) for k, v in weights.items()}


def _normalize_weight_dict(weights: dict[str, float], fallback: dict[str, float]) -> dict[str, float]:
    out = {k: max(float(weights.get(k, 0.0)), 0.0) for k in STRATEGY_KEYS}
    total = sum(out.values())
    if total <= 1e-12:
        return {k: float(fallback.get(k, 0.0)) for k in STRATEGY_KEYS}
    return {k: out[k] * 100.0 / total for k in STRATEGY_KEYS}


def _load_daily_ratio_table(path_text: str, fallback_weights: dict[str, float]) -> dict[str, dict[str, float]]:
    table_path_text = str(path_text or "").strip()
    if not table_path_text:
        return {}

    table_path = _resolve_path(table_path_text)
    if not table_path.exists():
        raise SystemExit(f"未找到每日配比表: {table_path}")

    ratio_df = pd.read_csv(table_path, encoding="utf-8-sig")
    if ratio_df.empty:
        return {}

    lower_map = {str(c).strip().lower(): str(c) for c in ratio_df.columns}
    date_col = ""
    for c in ["信号日期", "date", "trade_date", "日期"]:
        key = c.strip().lower()
        if key in lower_map:
            date_col = lower_map[key]
            break
    if not date_col:
        raise SystemExit("每日配比表缺少日期列（信号日期/date/日期）")

    col_map: dict[str, str] = {}
    for key in STRATEGY_KEYS:
        if key in lower_map:
            col_map[key] = lower_map[key]

    date_weights: dict[str, dict[str, float]] = {}
    for row in ratio_df.itertuples(index=False):
        row_dict = row._asdict()
        date_raw = row_dict.get(date_col, "")
        date_dt = pd.to_datetime(date_raw, errors="coerce")
        if pd.isna(date_dt):
            continue
        date_text = date_dt.strftime("%Y%m%d")

        raw_weights: dict[str, float] = {}
        for key in STRATEGY_KEYS:
            col = col_map.get(key, "")
            value = row_dict.get(col, fallback_weights.get(key, 0.0)) if col else fallback_weights.get(key, 0.0)
            raw_weights[key] = pd.to_numeric(pd.Series([value]), errors="coerce").fillna(0.0).iloc[0]

        date_weights[date_text] = _normalize_weight_dict(raw_weights, fallback=fallback_weights)

    return date_weights


def _fetch_index_close_series(index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    def _normalize_index_df(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        col_map_raw = {str(c).strip(): str(c) for c in df.columns}
        date_col = col_map_raw.get("date", "")
        close_col = col_map_raw.get("close", "")

        if not date_col or not close_col:
            col_map_lc = {str(c).strip().lower(): str(c) for c in df.columns}
            date_col = col_map_lc.get("date", "")
            close_col = col_map_lc.get("close", "")

        if not date_col or not close_col:
            return pd.DataFrame()

        out_df = pd.DataFrame()
        out_df["date"] = pd.to_datetime(df[date_col], errors="coerce")
        out_df["close"] = pd.to_numeric(df[close_col], errors="coerce")
        out_df = out_df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
        return out_df

    symbol = _to_ak_index_symbol(index_code)
    out = pd.DataFrame()

    # Primary path: Eastmoney index endpoint.
    try:
        raw = ak.stock_zh_index_daily_em(symbol=symbol)
        out = _normalize_index_df(raw)
    except Exception:
        out = pd.DataFrame()

    # Fallback path: Sina index endpoint.
    if out.empty:
        try:
            raw_fallback = ak.stock_zh_index_daily(symbol=symbol)
            out = _normalize_index_df(raw_fallback)
        except Exception:
            out = pd.DataFrame()

    if out.empty:
        return out

    start_dt = pd.to_datetime(str(start_date), format="%Y%m%d", errors="coerce")
    end_dt = pd.to_datetime(str(end_date), format="%Y%m%d", errors="coerce")
    if pd.notna(start_dt):
        out = out[out["date"] >= start_dt]
    if pd.notna(end_dt):
        out = out[out["date"] <= end_dt]
    out = out.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    return out


def _calc_index_metrics(index_df: pd.DataFrame) -> pd.DataFrame:
    if index_df.empty:
        return pd.DataFrame(columns=["date", "ret_5", "ret_20", "vol_20"])

    m = index_df.copy().sort_values("date").reset_index(drop=True)
    m["ret_5"] = (m["close"] / m["close"].shift(5) - 1.0) * 100.0
    m["ret_20"] = (m["close"] / m["close"].shift(20) - 1.0) * 100.0
    m["vol_20"] = m["close"].pct_change().rolling(20).std() * 100.0
    return m[["date", "ret_5", "ret_20", "vol_20"]]


def _classify_market_regime(ret_5: float | None, ret_20: float | None, vol_20: float | None, hs300_ret_20: float | None) -> str:
    if ret_20 is None:
        return "side"

    up_cond = ret_20 >= 4.0 and (ret_5 is not None and ret_5 >= 0.0) and (vol_20 is None or vol_20 <= 1.8)
    down_cond = ret_20 <= -4.0 or ((ret_5 is not None and ret_5 <= -3.0) and (vol_20 is not None and vol_20 >= 1.8))

    if hs300_ret_20 is not None and hs300_ret_20 <= -5.0:
        down_cond = True
    if hs300_ret_20 is not None and hs300_ret_20 >= 5.0 and ret_20 >= 3.0 and (ret_5 is not None and ret_5 >= 0.0):
        up_cond = True

    if down_cond:
        return "down"
    if up_cond:
        return "up"
    return "side"


def _weights_from_regime(regime: str, side_fallback: dict[str, float]) -> dict[str, float]:
    if regime == "up":
        return _normalize_weight_dict(
            {"theme": 35.0, "cctv": 15.0, "boll": 25.0, "relativity": 20.0, "cash": 5.0},
            fallback=DEFAULT_STRATEGY_WEIGHTS,
        )
    if regime == "down":
        return _normalize_weight_dict(
            {"cash": 60.0, "boll": 25.0, "relativity": 10.0, "theme": 5.0, "cctv": 0.0},
            fallback=DEFAULT_STRATEGY_WEIGHTS,
        )
    return _normalize_weight_dict(side_fallback, fallback=DEFAULT_STRATEGY_WEIGHTS)


def _regime_cn_name(regime: str) -> str:
    if regime == "up":
        return "趋势上行"
    if regime == "down":
        return "下行防御"
    return "震荡轮动"


def _build_market_based_daily_weights(
    signal_dates: list[str],
    fallback_weights: dict[str, float],
) -> tuple[dict[str, dict[str, float]], dict[str, dict[str, str]]]:
    valid_dates = sorted({d for d in signal_dates if isinstance(d, str) and len(d) == 8 and d.isdigit()})
    if not valid_dates:
        return {}, {}

    start_dt = pd.to_datetime(valid_dates[0], format="%Y%m%d", errors="coerce")
    end_dt = pd.to_datetime(valid_dates[-1], format="%Y%m%d", errors="coerce")
    if pd.isna(start_dt) or pd.isna(end_dt):
        return {}, {}

    fetch_start = (start_dt - timedelta(days=80)).strftime("%Y%m%d")
    fetch_end = (end_dt + timedelta(days=5)).strftime("%Y%m%d")

    sh_df = _fetch_index_close_series("sh.000001", fetch_start, fetch_end)
    hs300_df = _fetch_index_close_series("sh.000300", fetch_start, fetch_end)
    if sh_df.empty:
        return {}, {}

    sh_m = _calc_index_metrics(sh_df)
    hs_m = _calc_index_metrics(hs300_df)

    output: dict[str, dict[str, float]] = {}
    meta: dict[str, dict[str, str]] = {}
    for d in valid_dates:
        d_dt = pd.to_datetime(d, format="%Y%m%d", errors="coerce")
        if pd.isna(d_dt):
            continue

        sh_row = sh_m[sh_m["date"] <= d_dt]
        hs_row = hs_m[hs_m["date"] <= d_dt] if not hs_m.empty else pd.DataFrame()
        if sh_row.empty:
            continue

        sh_last = sh_row.iloc[-1]
        hs_last = hs_row.iloc[-1] if not hs_row.empty else None

        ret_5 = float(sh_last["ret_5"]) if pd.notna(sh_last["ret_5"]) else None
        ret_20 = float(sh_last["ret_20"]) if pd.notna(sh_last["ret_20"]) else None
        vol_20 = float(sh_last["vol_20"]) if pd.notna(sh_last["vol_20"]) else None
        hs_ret_20 = None
        if hs_last is not None and pd.notna(hs_last["ret_20"]):
            hs_ret_20 = float(hs_last["ret_20"])

        regime = _classify_market_regime(ret_5=ret_5, ret_20=ret_20, vol_20=vol_20, hs300_ret_20=hs_ret_20)
        output[d] = _weights_from_regime(regime, side_fallback=fallback_weights)

        r5_txt = "NA" if ret_5 is None else f"{ret_5:.2f}%"
        r20_txt = "NA" if ret_20 is None else f"{ret_20:.2f}%"
        vol_txt = "NA" if vol_20 is None else f"{vol_20:.2f}%"
        hs20_txt = "NA" if hs_ret_20 is None else f"{hs_ret_20:.2f}%"
        meta[d] = {
            "市场状态": _regime_cn_name(regime),
            "配比原因": f"上证5日{r5_txt}, 上证20日{r20_txt}, 波动20日{vol_txt}, 沪深300 20日{hs20_txt}",
        }

    return output, meta


def _fallback_next_business_day(signal_date_text: str) -> str:
    dt = pd.to_datetime(signal_date_text, format="%Y%m%d", errors="coerce")
    if pd.isna(dt):
        return ""
    next_dt = dt + timedelta(days=1)
    while next_dt.weekday() >= 5:
        next_dt += timedelta(days=1)
    return next_dt.strftime("%Y%m%d")


def _build_next_trade_day_map(signal_dates: list[str]) -> dict[str, str]:
    valid_dates = sorted({d for d in signal_dates if isinstance(d, str) and len(d) == 8 and d.isdigit()})
    if not valid_dates:
        return {}

    start_dt = pd.to_datetime(valid_dates[0], format="%Y%m%d", errors="coerce")
    end_dt = pd.to_datetime(valid_dates[-1], format="%Y%m%d", errors="coerce")
    if pd.isna(start_dt) or pd.isna(end_dt):
        return {d: _fallback_next_business_day(d) for d in valid_dates}

    fetch_start = (start_dt - timedelta(days=30)).strftime("%Y%m%d")
    fetch_end = (end_dt + timedelta(days=30)).strftime("%Y%m%d")
    sh_df = _fetch_index_close_series("sh.000001", fetch_start, fetch_end)
    if sh_df.empty:
        return {d: _fallback_next_business_day(d) for d in valid_dates}

    trade_dates = pd.to_datetime(sh_df["date"], errors="coerce").dropna().sort_values().reset_index(drop=True)
    if trade_dates.empty:
        return {d: _fallback_next_business_day(d) for d in valid_dates}

    trade_index = pd.Index(trade_dates)
    output: dict[str, str] = {}
    for d in valid_dates:
        dt = pd.to_datetime(d, format="%Y%m%d", errors="coerce")
        if pd.isna(dt):
            output[d] = _fallback_next_business_day(d)
            continue
        pos = trade_index.searchsorted(dt, side="right")
        if pos < len(trade_index):
            output[d] = pd.Timestamp(trade_index[pos]).strftime("%Y%m%d")
        else:
            output[d] = _fallback_next_business_day(d)
    return output


def _select_files_by_next_trade_day(selected_files: list[tuple[Path, str, str]]) -> list[tuple[Path, str, str]]:
    if not selected_files:
        return []

    date_map = _build_next_trade_day_map([d for _, d, _ in selected_files])
    buckets: dict[tuple[str, str], list[tuple[Path, str, str]]] = {}
    for file_path, signal_date, strategy_key in selected_files:
        entry_date = date_map.get(signal_date, _fallback_next_business_day(signal_date))
        if not entry_date:
            continue
        key = (strategy_key, entry_date)
        buckets.setdefault(key, []).append((file_path, signal_date, strategy_key))

    def _pick_one(cands: list[tuple[Path, str, str]]) -> tuple[Path, str, str]:
        # 同一买入日优先使用周五信号；若周五缺失则使用较新的周末信号作为替代。
        scored: list[tuple[int, str, str, tuple[Path, str, str]]] = []
        for item in cands:
            fp, ds, _ = item
            dt = pd.to_datetime(ds, format="%Y%m%d", errors="coerce")
            weekday = int(dt.weekday()) if pd.notna(dt) else -1
            friday_priority = 1 if weekday == 4 else 0
            scored.append((friday_priority, ds, fp.name, item))
        scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
        return scored[0][3]

    picked = [_pick_one(v) for _, v in sorted(buckets.items(), key=lambda x: (x[0][1], x[0][0]))]
    return sorted(picked, key=lambda x: (x[1], x[2], x[0].name))


def _pick_code_name_cols(df: pd.DataFrame) -> tuple[str, str]:
    cols = {str(c).strip().lower(): str(c) for c in df.columns}

    code_col = ""
    for c in ["股票代码", "code", "symbol", "证券代码"]:
        k = c.lower()
        if k in cols:
            code_col = cols[k]
            break

    name_col = ""
    for c in ["股票名称", "name", "证券名称"]:
        k = c.lower()
        if k in cols:
            name_col = cols[k]
            break

    if not code_col:
        raise ValueError("信号文件缺少代码列（股票代码/code/symbol）")
    return code_col, name_col


def _load_signal_file(path: Path, top_n: int) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except EmptyDataError:
        return pd.DataFrame(columns=["code", "name"])
    if df.empty:
        return pd.DataFrame(columns=["code", "name"])

    code_col, name_col = _pick_code_name_cols(df)
    out = pd.DataFrame()
    out["code"] = df[code_col].apply(_normalize_code)
    out["name"] = df[name_col].astype(str).fillna("") if name_col else ""
    out = out[out["code"].str.len() == 6].drop_duplicates(subset=["code"], keep="first")
    if top_n > 0:
        out = out.head(top_n)
    return out.reset_index(drop=True)


@lru_cache(maxsize=512)
def _load_cctv_codes_by_signal_date(signal_date_text: str) -> frozenset[str]:
    date_text = str(signal_date_text or "").strip()
    if len(date_text) != 8 or (not date_text.isdigit()):
        return frozenset()

    patterns = [
        f"stock_data/CCTV-Sector-Stock-Pool-{date_text}*.csv",
        f"stock_data/archive/*/cctv/CCTV-Sector-Stock-Pool-{date_text}*.csv",
    ]
    candidates: list[Path] = []
    for pattern in patterns:
        candidates.extend(ROOT_DIR.glob(pattern))

    if not candidates:
        return frozenset()

    code_set: set[str] = set()
    for path in sorted(set(candidates), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            df = pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            continue
        if df.empty:
            continue

        code_col = ""
        lower_map = {str(c).strip().lower(): str(c) for c in df.columns}
        for key in ["股票代码", "code", "symbol", "证券代码"]:
            col = lower_map.get(key.lower(), "")
            if col:
                code_col = col
                break

        if not code_col:
            continue

        codes = df[code_col].astype(str).apply(_normalize_code)
        code_set.update(c for c in codes.tolist() if len(c) == 6)

    return frozenset(code_set)


def _filter_theme_signal_with_cctv(signal_df: pd.DataFrame, signal_date_text: str) -> pd.DataFrame:
    if signal_df.empty:
        return signal_df

    cctv_codes = _load_cctv_codes_by_signal_date(signal_date_text)
    if not cctv_codes:
        return pd.DataFrame(columns=signal_df.columns)

    out = signal_df[signal_df["code"].isin(cctv_codes)].copy()
    return out.reset_index(drop=True)


def _load_theme_signal_file_with_cctv(path: Path, top_n: int, signal_date_text: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except EmptyDataError:
        return pd.DataFrame(columns=["code", "name"])
    if df.empty:
        return pd.DataFrame(columns=["code", "name"])

    code_col, name_col = _pick_code_name_cols(df)
    work = pd.DataFrame()
    work["code"] = df[code_col].apply(_normalize_code)
    work["name"] = df[name_col].astype(str).fillna("") if name_col else ""

    if "题材命中数" in df.columns:
        work["_theme_hit"] = pd.to_numeric(df["题材命中数"], errors="coerce").fillna(0.0) > 0
    elif "题材标签" in df.columns:
        work["_theme_hit"] = df["题材标签"].astype(str).str.strip() != ""
    else:
        work["_theme_hit"] = False

    work = work[work["code"].str.len() == 6].drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    if work.empty:
        return pd.DataFrame(columns=["code", "name"])

    cctv_codes = _load_cctv_codes_by_signal_date(signal_date_text)
    filtered = pd.DataFrame(columns=work.columns)
    if cctv_codes:
        filtered = work[work["code"].isin(cctv_codes)].copy()

    # If CCTV pool intersection is empty, fallback to theme-hit marks from source file.
    if filtered.empty:
        filtered = work[work["_theme_hit"] == True].copy()

    filtered = filtered[["code", "name"]]
    if top_n > 0:
        filtered = filtered.head(top_n)
    return filtered.reset_index(drop=True)


def _to_percent_like(value: object) -> float | None:
    num = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(num):
        return None
    val = float(num)
    if val <= 1.5:
        return val * 100.0
    return val


def _load_relativity_signal_file(path: Path, top_n: int, min_down_ratio_pct: float) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except EmptyDataError:
        return pd.DataFrame(columns=["code", "name"])
    if df.empty:
        return pd.DataFrame(columns=["code", "name"])

    code_col, name_col = _pick_code_name_cols(df)
    out = pd.DataFrame()
    out["code"] = df[code_col].apply(_normalize_code)
    out["name"] = df[name_col].astype(str).fillna("") if name_col else ""

    down_col = ""
    lower_map = {str(c).strip().lower(): str(c) for c in df.columns}
    for key in ["抗跌满足率", "down_ratio", "抗跌率"]:
        col = lower_map.get(key.lower(), "")
        if col:
            down_col = col
            break

    if down_col:
        threshold = max(float(min_down_ratio_pct), 0.0)
        down_pct = df[down_col].apply(_to_percent_like)
        out = out[down_pct >= threshold]

    out = out[out["code"].str.len() == 6].drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    if top_n > 0:
        out = out.head(top_n)
    return out


def _fetch_hist(symbol: str, start_date: str, end_date: str, *, allow_cache_fallback: bool = True) -> pd.DataFrame:
    hist = _load_symbol_history(symbol)
    start_dt = pd.to_datetime(start_date, format="%Y%m%d", errors="coerce")
    end_dt = pd.to_datetime(end_date, format="%Y%m%d", errors="coerce")
    if not hist.empty:
        cached_start = pd.to_datetime(hist["date"].min(), errors="coerce")
        cached_end = pd.to_datetime(hist["date"].max(), errors="coerce")
        if pd.notna(start_dt) and pd.notna(end_dt) and pd.notna(cached_start) and pd.notna(cached_end):
            if cached_start <= start_dt and cached_end >= end_dt:
                out = hist.copy()
                out = out[(out["date"] >= start_dt) & (out["date"] <= end_dt)].reset_index(drop=True)
                if not out.empty:
                    return out

    live_hist = _fetch_hist_via_akshare(symbol=symbol, start_date=start_date, end_date=end_date)
    if not live_hist.empty:
        return live_hist

    if hist.empty or (not allow_cache_fallback):
        return pd.DataFrame()

    out = hist.copy()
    if pd.notna(start_dt):
        out = out[out["date"] >= start_dt]
    if pd.notna(end_dt):
        out = out[out["date"] <= end_dt]
    return out.reset_index(drop=True)


def _calc_returns_with_costs(
    entry_price: float,
    exit_price: float,
    buy_slip_bps: float,
    sell_slip_bps: float,
    buy_fee_rate: float,
    sell_fee_rate: float,
    sell_stamp_tax_rate: float,
) -> tuple[float, float, float, float, float]:
    buy_slip = max(float(buy_slip_bps), 0.0) / 10000.0
    sell_slip = max(float(sell_slip_bps), 0.0) / 10000.0
    buy_fee = max(float(buy_fee_rate), 0.0)
    sell_fee = max(float(sell_fee_rate), 0.0)
    sell_tax = max(float(sell_stamp_tax_rate), 0.0)

    buy_exec_price = entry_price * (1.0 + buy_slip)
    sell_exec_price = exit_price * (1.0 - sell_slip)

    gross_ret = sell_exec_price / buy_exec_price - 1.0
    net_buy_cash = buy_exec_price * (1.0 + buy_fee)
    net_sell_cash = sell_exec_price * (1.0 - sell_fee - sell_tax)
    net_ret = net_sell_cash / net_buy_cash - 1.0
    cost_ret = gross_ret - net_ret
    return buy_exec_price, sell_exec_price, gross_ret, net_ret, cost_ret


def _backtest_single_pick(
    signal_date_text: str,
    code: str,
    name: str,
    hold_days: int,
    buy_slip_bps: float,
    sell_slip_bps: float,
    buy_fee_rate: float,
    sell_fee_rate: float,
    sell_stamp_tax_rate: float,
    allow_cache_fallback: bool,
) -> dict[str, object] | None:
    signal_dt = pd.to_datetime(signal_date_text, format="%Y%m%d", errors="coerce")
    if pd.isna(signal_dt):
        return None

    holding_days = max(int(hold_days), 1)

    # 预留更多自然日，避免节假日导致交易日不足。
    end_dt = signal_dt + timedelta(days=holding_days * 3 + 15)
    hist = _fetch_hist(
        code,
        signal_dt.strftime("%Y%m%d"),
        end_dt.strftime("%Y%m%d"),
        allow_cache_fallback=allow_cache_fallback,
    )
    if hist.empty:
        return None

    after_signal = hist[hist["date"] > signal_dt].reset_index(drop=True)
    if len(after_signal) < holding_days:
        return None

    entry_row = after_signal.iloc[0]
    exit_row = after_signal.iloc[holding_days - 1]

    entry_price = float(entry_row["open"])
    exit_price = float(exit_row["close"])
    if entry_price <= 0:
        return None

    window = after_signal.iloc[:holding_days]
    min_close = float(window["close"].min()) if not window.empty else entry_price
    max_drawdown = min(min_close / entry_price - 1.0, 0.0)

    buy_exec_price, sell_exec_price, gross_ret, net_ret, cost_ret = _calc_returns_with_costs(
        entry_price=entry_price,
        exit_price=exit_price,
        buy_slip_bps=buy_slip_bps,
        sell_slip_bps=sell_slip_bps,
        buy_fee_rate=buy_fee_rate,
        sell_fee_rate=sell_fee_rate,
        sell_stamp_tax_rate=sell_stamp_tax_rate,
    )

    return {
        "信号日期": signal_date_text,
        "股票代码": code,
        "股票名称": name,
        "买入日期": entry_row["date"].strftime("%Y-%m-%d"),
        "卖出日期": exit_row["date"].strftime("%Y-%m-%d"),
        "买入价(次日开盘)": round(entry_price, 4),
        "卖出价(持有N日收盘)": round(exit_price, 4),
        "成交买入价(含滑点)": round(buy_exec_price, 4),
        "成交卖出价(含滑点)": round(sell_exec_price, 4),
        "持有交易日": holding_days,
        "毛收益率(%)": round(gross_ret * 100.0, 3),
        "净收益率(%)": round(net_ret * 100.0, 3),
        "交易成本冲击(%)": round(cost_ret * 100.0, 3),
        "区间最大回撤(%)": round(max_drawdown * 100.0, 3),
    }


def summarize(trades_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if trades_df.empty:
        summary_df = pd.DataFrame(
            [
                {
                    "样本数": 0,
                    "净胜率(%)": 0.0,
                    "平均毛收益率(%)": 0.0,
                    "平均净收益率(%)": 0.0,
                    "中位净收益率(%)": 0.0,
                    "平均成本冲击(%)": 0.0,
                    "平均最大回撤(%)": 0.0,
                    "净收益回撤比": 0.0,
                }
            ]
        )
        daily_df = pd.DataFrame(columns=["信号日期", "样本数", "日均毛收益率(%)", "日均净收益率(%)", "净胜率(%)"])
        return summary_df, daily_df

    gross_rets = pd.to_numeric(trades_df["毛收益率(%)"], errors="coerce").fillna(0.0)
    net_rets = pd.to_numeric(trades_df["净收益率(%)"], errors="coerce").fillna(0.0)
    cost_impacts = pd.to_numeric(trades_df["交易成本冲击(%)"], errors="coerce").fillna(0.0)
    dds = pd.to_numeric(trades_df["区间最大回撤(%)"], errors="coerce").fillna(0.0)

    net_win_rate = float((net_rets > 0).mean() * 100.0)
    avg_gross_ret = float(gross_rets.mean())
    avg_net_ret = float(net_rets.mean())
    med_net_ret = float(net_rets.median())
    avg_cost_impact = float(cost_impacts.mean())
    avg_dd = float(dds.mean())
    rr = (avg_net_ret / abs(avg_dd)) if abs(avg_dd) > 1e-9 else float("inf")

    summary_df = pd.DataFrame(
        [
            {
                "样本数": int(len(trades_df)),
                "净胜率(%)": round(net_win_rate, 2),
                "平均毛收益率(%)": round(avg_gross_ret, 3),
                "平均净收益率(%)": round(avg_net_ret, 3),
                "中位净收益率(%)": round(med_net_ret, 3),
                "平均成本冲击(%)": round(avg_cost_impact, 3),
                "平均最大回撤(%)": round(avg_dd, 3),
                "净收益回撤比": "INF" if rr == float("inf") else round(rr, 3),
            }
        ]
    )

    daily = trades_df.copy()
    daily["毛收益率(%)"] = pd.to_numeric(daily["毛收益率(%)"], errors="coerce").fillna(0.0)
    daily["净收益率(%)"] = pd.to_numeric(daily["净收益率(%)"], errors="coerce").fillna(0.0)
    daily_group = (
        daily.groupby("信号日期", as_index=False)
        .agg(
            样本数=("净收益率(%)", "count"),
            **{"日均毛收益率(%)": ("毛收益率(%)", "mean")},
            **{"日均净收益率(%)": ("净收益率(%)", "mean")},
            **{"净胜率(%)": ("净收益率(%)", lambda x: (x > 0).mean() * 100.0)},
        )
        .sort_values("信号日期")
        .reset_index(drop=True)
    )
    daily_group["日均毛收益率(%)"] = daily_group["日均毛收益率(%)"].round(3)
    daily_group["日均净收益率(%)"] = daily_group["日均净收益率(%)"].round(3)
    daily_group["净胜率(%)"] = daily_group["净胜率(%)"].round(2)

    return summary_df, daily_group


def summarize_portfolio_daily(
    trades_df: pd.DataFrame,
    strategy_weights_pct: dict[str, float],
    initial_capital: float,
    daily_weight_map: dict[str, dict[str, float]] | None = None,
    daily_meta_map: dict[str, dict[str, str]] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    initial_capital = max(float(initial_capital), 1.0)
    if trades_df.empty:
        summary_df = pd.DataFrame(
            [
                {
                    "初始资金(元)": round(initial_capital, 2),
                    "期末资金(元)": round(initial_capital, 2),
                    "组合累计收益率(%)": 0.0,
                    "组合年化收益率(%)": 0.0,
                    "组合最大回撤(%)": 0.0,
                    "回撤恢复比": 0.0,
                }
            ]
        )
        daily_cols = [
            "信号日期",
            "配比来源",
            "市场状态",
            "配比原因",
            "boll权重(%)",
            "theme权重(%)",
            "relativity权重(%)",
            "cctv权重(%)",
            "cash权重(%)",
            "boll净收益率(%)",
            "theme净收益率(%)",
            "relativity净收益率(%)",
            "cctv净收益率(%)",
            "组合净收益率(%)",
            "组合资金(元)",
            "组合回撤(%)",
        ]
        return summary_df, pd.DataFrame(columns=daily_cols)

    daily_strategy = (
        trades_df.groupby(["信号日期", "策略"], as_index=False)["净收益率(%)"]
        .mean()
        .pivot(index="信号日期", columns="策略", values="净收益率(%)")
        .fillna(0.0)
    )
    for key in ["boll", "theme", "relativity", "cctv"]:
        if key not in daily_strategy.columns:
            daily_strategy[key] = 0.0

    daily_strategy = daily_strategy.sort_index()
    base_weights = _normalize_weight_dict(strategy_weights_pct, fallback=DEFAULT_STRATEGY_WEIGHTS)
    effective_daily_weights = daily_weight_map or {}
    effective_daily_meta = daily_meta_map or {}

    portfolio_ret_vals: list[float] = []
    weight_rows: list[dict[str, float]] = []
    source_rows: list[str] = []
    regime_rows: list[str] = []
    reason_rows: list[str] = []
    for signal_date in daily_strategy.index:
        date_weights = effective_daily_weights.get(str(signal_date), base_weights)
        normalized = _normalize_weight_dict(date_weights, fallback=base_weights)
        weight_rows.append(normalized)

        day_meta = effective_daily_meta.get(str(signal_date), {})
        source_rows.append(str(day_meta.get("配比来源", "手工默认配比")))
        regime_rows.append(str(day_meta.get("市场状态", "未指定")))
        reason_rows.append(str(day_meta.get("配比原因", "未提供")))

        day_ret = (
            float(daily_strategy.at[signal_date, "boll"]) * normalized.get("boll", 0.0) / 100.0
            + float(daily_strategy.at[signal_date, "theme"]) * normalized.get("theme", 0.0) / 100.0
            + float(daily_strategy.at[signal_date, "relativity"]) * normalized.get("relativity", 0.0) / 100.0
            + float(daily_strategy.at[signal_date, "cctv"]) * normalized.get("cctv", 0.0) / 100.0
        )
        portfolio_ret_vals.append(day_ret)

    portfolio_ret = pd.Series(portfolio_ret_vals, index=daily_strategy.index, dtype=float)
    weight_df = pd.DataFrame(weight_rows, index=daily_strategy.index)

    daily_df = pd.DataFrame(index=daily_strategy.index)
    daily_df["配比来源"] = source_rows
    daily_df["市场状态"] = regime_rows
    daily_df["配比原因"] = reason_rows
    daily_df["boll权重(%)"] = weight_df["boll"].round(2)
    daily_df["theme权重(%)"] = weight_df["theme"].round(2)
    daily_df["relativity权重(%)"] = weight_df["relativity"].round(2)
    daily_df["cctv权重(%)"] = weight_df["cctv"].round(2)
    daily_df["cash权重(%)"] = weight_df["cash"].round(2)
    daily_df["boll净收益率(%)"] = daily_strategy["boll"].round(3)
    daily_df["theme净收益率(%)"] = daily_strategy["theme"].round(3)
    daily_df["relativity净收益率(%)"] = daily_strategy["relativity"].round(3)
    daily_df["cctv净收益率(%)"] = daily_strategy["cctv"].round(3)
    daily_df["组合净收益率(%)"] = portfolio_ret.round(3)

    capital_curve = (1.0 + portfolio_ret / 100.0).cumprod() * initial_capital
    peak = capital_curve.cummax()
    drawdown_pct = (capital_curve / peak - 1.0) * 100.0

    daily_df["组合资金(元)"] = capital_curve.round(2)
    daily_df["组合回撤(%)"] = drawdown_pct.round(3)
    daily_df = daily_df.reset_index().rename(columns={"index": "信号日期"})

    final_capital = float(capital_curve.iloc[-1]) if len(capital_curve) else initial_capital
    total_ret = (final_capital / initial_capital - 1.0) * 100.0 if initial_capital > 0 else 0.0
    days = max(len(daily_df), 1)
    annual_ret = ((final_capital / initial_capital) ** (252.0 / days) - 1.0) * 100.0 if initial_capital > 0 else 0.0
    max_dd = float(drawdown_pct.min()) if len(drawdown_pct) else 0.0
    recovery = (total_ret / abs(max_dd)) if abs(max_dd) > 1e-9 else float("inf")

    summary_df = pd.DataFrame(
        [
            {
                "初始资金(元)": round(initial_capital, 2),
                "期末资金(元)": round(final_capital, 2),
                "组合累计收益率(%)": round(total_ret, 3),
                "组合年化收益率(%)": round(annual_ret, 3),
                "组合最大回撤(%)": round(max_dd, 3),
                "回撤恢复比": "INF" if recovery == float("inf") else round(recovery, 3),
            }
        ]
    )
    return summary_df, daily_df


def _in_range(date_text: str, start_text: str, end_text: str) -> bool:
    if not date_text:
        return False
    if start_text and date_text < start_text:
        return False
    if end_text and date_text > end_text:
        return False
    return True


def default_output_prefix() -> Path:
    today_text = datetime.now().strftime("%Y%m%d")
    return STOCK_DATA_DIR / f"Signal-Backtest-{today_text}"


def main() -> int:
    args = parse_args()

    try:
        pattern = str(args.signals_glob).strip()
        if not pattern:
            raise SystemExit("signals-glob 不能为空")

        signal_files = sorted([p for p in ROOT_DIR.glob(pattern)], key=lambda p: p.name)
        if not signal_files:
            raise SystemExit(f"未找到信号文件: {pattern}")

        selected_files = []
        for f in signal_files:
            ds = _extract_signal_date_from_file(f)
            if _in_range(ds, args.start_date.strip(), args.end_date.strip()):
                selected_files.append((f, ds, _infer_strategy_from_file(f)))

        selected_files = _select_files_by_next_trade_day(selected_files)

        if not selected_files:
            raise SystemExit("在给定日期范围内未找到信号文件")

        strategy_weights = _parse_daily_strategy_weights(args.daily_strategy_ratios)
        csv_weight_map = _load_daily_ratio_table(args.daily_ratios_csv, fallback_weights=strategy_weights)
        signal_dates = sorted({d for _, d, _ in selected_files if d})

        market_weight_map: dict[str, dict[str, float]] = {}
        market_meta_map: dict[str, dict[str, str]] = {}
        if bool(args.auto_market_ratios):
            market_weight_map, market_meta_map = _build_market_based_daily_weights(
                signal_dates=signal_dates,
                fallback_weights=strategy_weights,
            )

        daily_weight_map: dict[str, dict[str, float]] = {}
        daily_meta_map: dict[str, dict[str, str]] = {}
        for signal_date in signal_dates:
            if signal_date in csv_weight_map:
                daily_weight_map[signal_date] = csv_weight_map[signal_date]
                daily_meta_map[signal_date] = {
                    "配比来源": "配比CSV",
                    "市场状态": "CSV指定",
                    "配比原因": "使用上传CSV中的当日权重",
                }
            elif signal_date in market_weight_map:
                daily_weight_map[signal_date] = market_weight_map[signal_date]
                daily_meta_map[signal_date] = {
                    "配比来源": "市场自动配比",
                    "市场状态": market_meta_map.get(signal_date, {}).get("市场状态", "市场自动"),
                    "配比原因": market_meta_map.get(signal_date, {}).get("配比原因", "按指数状态自动配比"),
                }
            else:
                daily_weight_map[signal_date] = _normalize_weight_dict(strategy_weights, fallback=DEFAULT_STRATEGY_WEIGHTS)
                if bool(args.auto_market_ratios):
                    daily_meta_map[signal_date] = {
                        "配比来源": "自动配比回退手工",
                        "市场状态": "自动数据缺失",
                        "配比原因": "当日指数数据不可用，回退到页面/参数默认权重",
                    }
                else:
                    daily_meta_map[signal_date] = {
                        "配比来源": "手工默认配比",
                        "市场状态": "未启用自动",
                        "配比原因": "使用页面/参数输入的默认权重",
                    }

        trades_rows: list[dict[str, object]] = []
        for file_path, signal_date, strategy_key in selected_files:
            top_n = max(int(args.top_n), 1)
            if strategy_key == "theme" and bool(args.theme_cctv_only):
                signal_df = _load_theme_signal_file_with_cctv(file_path, top_n=top_n, signal_date_text=signal_date)
            elif strategy_key == "relativity":
                signal_df = _load_relativity_signal_file(
                    file_path,
                    top_n=top_n,
                    min_down_ratio_pct=float(args.relativity_min_down_ratio_pct),
                )
            else:
                signal_df = _load_signal_file(file_path, top_n=top_n)
            if signal_df.empty:
                continue

            for row in signal_df.itertuples(index=False):
                rec = _backtest_single_pick(
                    signal_date_text=signal_date,
                    code=str(row.code),
                    name=str(row.name),
                    hold_days=max(int(args.hold_days), 1),
                    buy_slip_bps=float(args.buy_slip_bps),
                    sell_slip_bps=float(args.sell_slip_bps),
                    buy_fee_rate=float(args.buy_fee_rate),
                    sell_fee_rate=float(args.sell_fee_rate),
                    sell_stamp_tax_rate=float(args.sell_stamp_tax_rate),
                    allow_cache_fallback=bool(args.allow_cache_fallback),
                )
                if rec is not None:
                    rec["策略"] = strategy_key
                    trades_rows.append(rec)

        trades_df = pd.DataFrame(trades_rows, columns=TRADES_COLUMNS)
        summary_df, daily_df = summarize(trades_df)
        portfolio_summary_df, portfolio_daily_df = summarize_portfolio_daily(
            trades_df=trades_df,
            strategy_weights_pct=strategy_weights,
            initial_capital=float(args.initial_capital),
            daily_weight_map=daily_weight_map,
            daily_meta_map=daily_meta_map,
        )

        out_prefix = _resolve_path(args.output_prefix) if args.output_prefix.strip() else default_output_prefix()
        out_prefix.parent.mkdir(parents=True, exist_ok=True)

        trades_path = Path(str(out_prefix) + "-trades.csv")
        daily_path = Path(str(out_prefix) + "-daily.csv")
        summary_path = Path(str(out_prefix) + "-summary.csv")
        portfolio_daily_path = Path(str(out_prefix) + "-portfolio-daily.csv")
        portfolio_summary_path = Path(str(out_prefix) + "-portfolio-summary.csv")

        trades_df.to_csv(trades_path, index=False, encoding="utf-8-sig")
        daily_df.to_csv(daily_path, index=False, encoding="utf-8-sig")
        summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
        portfolio_daily_df.to_csv(portfolio_daily_path, index=False, encoding="utf-8-sig")
        portfolio_summary_df.to_csv(portfolio_summary_path, index=False, encoding="utf-8-sig")

        print("信号回测完成。")
        print(summary_df.to_string(index=False))
        print(portfolio_summary_df.to_string(index=False))
        print(f"已保存: {trades_path}")
        print(f"已保存: {daily_path}")
        print(f"已保存: {summary_path}")
        print(f"已保存: {portfolio_daily_path}")
        print(f"已保存: {portfolio_summary_path}")
        return 0
    finally:
        pass


if __name__ == "__main__":
    raise SystemExit(main())
