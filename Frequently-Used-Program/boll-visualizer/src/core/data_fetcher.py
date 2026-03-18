from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable
import hashlib
import re

import akshare as ak
import baostock as bs
import pandas as pd

from utils.config import STOCK_DATA_DIR


DAILY_K_COLUMNS = ["date", "open", "high", "low", "close", "volume", "amount"]
FUND_FLOW_COLUMNS = ["code", "latest_price", "net_inflow"]
UNIVERSE_COLUMNS = ["code", "code_name"]

CACHE_DIR = STOCK_DATA_DIR / "cache"
K_DATA_CACHE_DIR = CACHE_DIR / "k_data"
FUND_FLOW_CACHE_DIR = CACHE_DIR / "fund_flow"
UNIVERSE_CACHE_DIR = CACHE_DIR / "universe"

CACHE_SCOPE_DIRS: dict[str, Path] = {
    "k_data": K_DATA_CACHE_DIR,
    "fund_flow": FUND_FLOW_CACHE_DIR,
    "universe": UNIVERSE_CACHE_DIR,
}


def format_stock_code(code: str | int) -> str:
    digits = "".join(ch for ch in str(code) if ch.isdigit())
    return digits.zfill(6)


def to_baostock_code(code: str | int) -> str:
    code_6 = format_stock_code(code)
    return f"sh.{code_6}" if code_6.startswith("6") else f"sz.{code_6}"


def parse_amount_text(raw_value: object) -> float:
    if raw_value is None:
        return float("nan")

    text = str(raw_value).strip().replace(",", "")
    if not text or text in {"-", "--", "nan", "None"}:
        return float("nan")

    multiplier = 1.0
    if text.endswith("亿"):
        multiplier = 1e8
        text = text[:-1]
    elif text.endswith("万"):
        multiplier = 1e4
        text = text[:-1]

    matched = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not matched:
        return float("nan")

    return float(matched.group(0)) * multiplier


def _to_date_string(value: str | date | datetime) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    text = str(value)
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def _to_date_only(value: str | date | datetime) -> date:
    return pd.to_datetime(value).date()


def _result_set_to_df(result_set) -> pd.DataFrame:
    rows: list[list[str]] = []
    while result_set.next():
        rows.append(result_set.get_row_data())
    return pd.DataFrame(rows, columns=result_set.fields)


def _ensure_cache_dir(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)


def _is_cache_fresh(cache_path: Path, max_cache_age_hours: float) -> bool:
    if not cache_path.exists():
        return False
    if max_cache_age_hours <= 0:
        return True
    age_seconds = datetime.now().timestamp() - cache_path.stat().st_mtime
    return age_seconds <= max_cache_age_hours * 3600


def _read_csv_safe(file_path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(file_path)
    except Exception:
        return pd.DataFrame()


def _find_latest_cache_file(directory: Path, pattern: str) -> Path | None:
    if not directory.exists():
        return None
    files = [item for item in directory.glob(pattern) if item.is_file()]
    if not files:
        return None
    files.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return files[0]


def _safe_cache_key(text: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", str(text)).strip("_")
    digest = hashlib.md5(str(text).encode("utf-8")).hexdigest()[:8]
    return f"{cleaned or 'key'}_{digest}"


def _empty_daily_k_df() -> pd.DataFrame:
    return pd.DataFrame(columns=DAILY_K_COLUMNS)


def _normalize_daily_k_data(data_frame: pd.DataFrame) -> pd.DataFrame:
    if data_frame is None or data_frame.empty:
        return _empty_daily_k_df()
    if "date" not in data_frame.columns:
        return _empty_daily_k_df()

    normalized = data_frame.copy()
    for column_name in ["open", "high", "low", "close", "volume", "amount"]:
        if column_name not in normalized.columns:
            normalized[column_name] = pd.NA
        normalized[column_name] = pd.to_numeric(normalized[column_name], errors="coerce")

    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized = normalized.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    if normalized.empty:
        return _empty_daily_k_df()

    normalized["date"] = normalized["date"].dt.strftime("%Y-%m-%d")
    return normalized[DAILY_K_COLUMNS]


def _slice_daily_k_range(
    data_frame: pd.DataFrame,
    start_day: date,
    end_day: date,
) -> pd.DataFrame:
    if data_frame.empty:
        return _empty_daily_k_df()

    temp = data_frame.copy()
    temp["_dt"] = pd.to_datetime(temp["date"], errors="coerce")
    temp = temp.dropna(subset=["_dt"])
    if temp.empty:
        return _empty_daily_k_df()

    mask = (temp["_dt"].dt.date >= start_day) & (temp["_dt"].dt.date <= end_day)
    temp = temp[mask].copy()
    if temp.empty:
        return _empty_daily_k_df()

    temp["date"] = temp["_dt"].dt.strftime("%Y-%m-%d")
    temp = temp.drop(columns=["_dt"])
    return _normalize_daily_k_data(temp)


def _merge_daily_k_dataframes(*frames: pd.DataFrame) -> pd.DataFrame:
    valid = [frame for frame in frames if frame is not None and not frame.empty]
    if not valid:
        return _empty_daily_k_df()

    merged = pd.concat(valid, ignore_index=True)
    merged = _normalize_daily_k_data(merged)
    if merged.empty:
        return _empty_daily_k_df()

    merged = merged.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
    return _normalize_daily_k_data(merged)


def _resolve_k_cache_coverage(data_frame: pd.DataFrame) -> tuple[date | None, date | None]:
    if data_frame.empty:
        return None, None
    dt_series = pd.to_datetime(data_frame["date"], errors="coerce").dropna()
    if dt_series.empty:
        return None, None
    return dt_series.min().date(), dt_series.max().date()


def _build_daily_k_full_cache_path(code: str | int, adjust: str) -> Path:
    code_6 = format_stock_code(code)
    adjust_text = str(adjust).lower()
    return K_DATA_CACHE_DIR / f"{code_6}_{adjust_text}_full.csv"


def _load_cached_daily_k_full_data(
    code: str | int,
    adjust: str,
    max_cache_age_hours: float,
    allow_stale: bool = False,
) -> pd.DataFrame | None:
    code_6 = format_stock_code(code)
    adjust_text = str(adjust).lower()
    full_path = _build_daily_k_full_cache_path(code_6, adjust_text)

    candidate_paths: list[Path] = []
    if full_path.exists():
        candidate_paths.append(full_path)
    else:
        legacy_pattern = f"{code_6}_{adjust_text}_*.csv"
        legacy_path = _find_latest_cache_file(K_DATA_CACHE_DIR, legacy_pattern)
        if legacy_path is not None:
            candidate_paths.append(legacy_path)

    for candidate in candidate_paths:
        if not candidate.exists():
            continue
        if not allow_stale and not _is_cache_fresh(candidate, max_cache_age_hours=max_cache_age_hours):
            continue
        cached_df = _read_csv_safe(candidate)
        normalized = _normalize_daily_k_data(cached_df)
        if normalized.empty:
            continue
        if candidate != full_path:
            _save_cached_daily_k_full_data(code_6, adjust_text, normalized)
        return normalized

    return None


def _save_cached_daily_k_full_data(code: str | int, adjust: str, data_frame: pd.DataFrame) -> None:
    normalized = _normalize_daily_k_data(data_frame)
    if normalized.empty:
        return
    _ensure_cache_dir(K_DATA_CACHE_DIR)
    cache_path = _build_daily_k_full_cache_path(code, adjust)
    normalized.to_csv(cache_path, index=False, encoding="utf-8-sig")


def _query_daily_k_data_in_current_session(
    code: str | int,
    start_date: str | date,
    end_date: str | date,
    adjust: str = "qfq",
) -> pd.DataFrame:
    adjust_map = {"hfq": "1", "qfq": "2", "bfq": "3"}
    adjust_flag = adjust_map.get(str(adjust).lower(), "2")

    result_set = bs.query_history_k_data_plus(
        to_baostock_code(code),
        "date,code,open,high,low,close,volume,amount",
        start_date=_to_date_string(start_date),
        end_date=_to_date_string(end_date),
        frequency="d",
        adjustflag=adjust_flag,
    )
    if result_set.error_code != "0":
        return _empty_daily_k_df()

    return _normalize_daily_k_data(_result_set_to_df(result_set))


def fetch_daily_k_data(
    code: str | int,
    start_date: str | date,
    end_date: str | date,
    adjust: str = "qfq",
    use_cache: bool = True,
    force_refresh: bool = False,
    max_cache_age_hours: float = 24.0,
) -> pd.DataFrame:
    if use_cache and not force_refresh:
        cached_full = _load_cached_daily_k_full_data(
            code=code,
            adjust=adjust,
            max_cache_age_hours=max_cache_age_hours,
            allow_stale=True,
        )
        if cached_full is not None:
            request_start = _to_date_only(start_date)
            request_end = _to_date_only(end_date)
            cache_min, cache_max = _resolve_k_cache_coverage(cached_full)
            if cache_min is not None and cache_max is not None and cache_min <= request_start and cache_max >= request_end:
                return _slice_daily_k_range(cached_full, request_start, request_end)

    login_result = bs.login()
    if login_result.error_code != "0":
        if use_cache:
            stale_full = _load_cached_daily_k_full_data(
                code=code,
                adjust=adjust,
                max_cache_age_hours=max_cache_age_hours,
                allow_stale=True,
            )
            if stale_full is not None:
                request_start = _to_date_only(start_date)
                request_end = _to_date_only(end_date)
                return _slice_daily_k_range(stale_full, request_start, request_end)
        return _empty_daily_k_df()

    try:
        return fetch_daily_k_data_in_session(
            code=code,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
            use_cache=use_cache,
            force_refresh=force_refresh,
            max_cache_age_hours=max_cache_age_hours,
        )
    finally:
        bs.logout()


def fetch_daily_k_data_in_session(
    code: str | int,
    start_date: str | date,
    end_date: str | date,
    adjust: str = "qfq",
    use_cache: bool = True,
    force_refresh: bool = False,
    max_cache_age_hours: float = 24.0,
) -> pd.DataFrame:
    request_start = _to_date_only(start_date)
    request_end = _to_date_only(end_date)
    if request_start > request_end:
        return _empty_daily_k_df()

    code_6 = format_stock_code(code)
    adjust_text = str(adjust).lower()
    full_cache_path = _build_daily_k_full_cache_path(code_6, adjust_text)

    cached_full = pd.DataFrame()
    cache_fresh = False
    if use_cache and not force_refresh:
        cached_data = _load_cached_daily_k_full_data(
            code=code_6,
            adjust=adjust_text,
            max_cache_age_hours=max_cache_age_hours,
            allow_stale=True,
        )
        if cached_data is not None:
            cached_full = cached_data
            cache_fresh = _is_cache_fresh(full_cache_path, max_cache_age_hours=max_cache_age_hours)

    cache_min, cache_max = _resolve_k_cache_coverage(cached_full)
    cache_covers_request = bool(
        cache_min is not None
        and cache_max is not None
        and cache_min <= request_start
        and cache_max >= request_end
    )

    if cache_covers_request and (cache_fresh or request_end < date.today() - timedelta(days=1)):
        return _slice_daily_k_range(cached_full, request_start, request_end)

    fetch_segments: list[tuple[date, date]] = []
    if force_refresh or cached_full.empty:
        fetch_segments.append((request_start, request_end))
    else:
        if cache_min is None or cache_max is None:
            fetch_segments.append((request_start, request_end))
        else:
            if request_start < cache_min:
                left_end = min(request_end, cache_min - timedelta(days=1))
                if request_start <= left_end:
                    fetch_segments.append((request_start, left_end))
            if request_end > cache_max:
                right_start = max(request_start, cache_max + timedelta(days=1))
                if right_start <= request_end:
                    fetch_segments.append((right_start, request_end))
            if cache_covers_request and not cache_fresh and request_end >= date.today() - timedelta(days=1):
                tail_start = max(request_start, request_end - timedelta(days=10))
                fetch_segments.append((tail_start, request_end))

    fetched_parts: list[pd.DataFrame] = []
    for segment_start, segment_end in fetch_segments:
        part_df = _query_daily_k_data_in_current_session(
            code=code_6,
            start_date=segment_start,
            end_date=segment_end,
            adjust=adjust_text,
        )
        if not part_df.empty:
            fetched_parts.append(part_df)

    merged_full = _merge_daily_k_dataframes(cached_full, *fetched_parts)
    if merged_full.empty and not cached_full.empty:
        merged_full = _normalize_daily_k_data(cached_full)

    if use_cache and not merged_full.empty:
        _save_cached_daily_k_full_data(code_6, adjust_text, merged_full)

    if merged_full.empty:
        return _empty_daily_k_df()

    return _slice_daily_k_range(merged_full, request_start, request_end)


def _empty_universe_df() -> pd.DataFrame:
    return pd.DataFrame(columns=UNIVERSE_COLUMNS)


def _normalize_all_stock_basic(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or raw_df.empty or "code" not in raw_df.columns:
        return _empty_universe_df()

    normalized = raw_df.copy()
    code_series = normalized["code"].astype(str)
    code_series = code_series[code_series.str.match(r"^(sh|sz)\.\d{6}$", na=False)]
    code_series = code_series.str[-6:]

    sh_mask = code_series.str.match(r"^(600|601|603|605|688)\d{3}$", na=False)
    sz_mask = code_series.str.match(r"^(000|001|002|003|300|301)\d{3}$", na=False)
    code_series = code_series[sh_mask | sz_mask]

    normalized = normalized.loc[code_series.index].copy()
    normalized["code"] = code_series.values

    if "code_name" in normalized.columns:
        normalized["code_name"] = normalized["code_name"].astype(str)
    elif normalized.shape[1] >= 2:
        normalized["code_name"] = normalized.iloc[:, 1].astype(str)
    else:
        normalized["code_name"] = ""

    normalized = normalized[UNIVERSE_COLUMNS].drop_duplicates(subset=["code"], keep="first")
    normalized = normalized.sort_values("code").reset_index(drop=True)
    return normalized


def _build_universe_cache_path(anchor_day: date | None = None) -> Path:
    day_text = (anchor_day or date.today()).strftime("%Y%m%d")
    return UNIVERSE_CACHE_DIR / f"a_share_basic_{day_text}.csv"


def _load_cached_universe_data(
    cache_path: Path,
    max_cache_age_hours: float,
    allow_stale: bool = False,
) -> pd.DataFrame | None:
    if not cache_path.exists():
        return None
    if not allow_stale and not _is_cache_fresh(cache_path, max_cache_age_hours=max_cache_age_hours):
        return None

    cached_df = _read_csv_safe(cache_path)
    normalized = _normalize_all_stock_basic(cached_df)
    return normalized if not normalized.empty else None


def _save_cached_universe_data(cache_path: Path, data_frame: pd.DataFrame) -> None:
    if data_frame.empty:
        return
    _ensure_cache_dir(UNIVERSE_CACHE_DIR)
    data_frame.to_csv(cache_path, index=False, encoding="utf-8-sig")


def fetch_all_a_share_basic(
    max_lookback_days: int = 10,
    use_cache: bool = True,
    force_refresh: bool = False,
    max_cache_age_hours: float = 24.0,
) -> pd.DataFrame:
    today_cache_path = _build_universe_cache_path()
    if use_cache and not force_refresh:
        cached_df = _load_cached_universe_data(
            cache_path=today_cache_path,
            max_cache_age_hours=max_cache_age_hours,
            allow_stale=False,
        )
        if cached_df is not None:
            return cached_df

    login_result = bs.login()
    raw_df = pd.DataFrame()
    if login_result.error_code == "0":
        try:
            for offset in range(max_lookback_days + 1):
                day = (date.today() - timedelta(days=offset)).strftime("%Y-%m-%d")
                result_set = bs.query_all_stock(day=day)
                if result_set.error_code != "0":
                    continue
                temp_df = _result_set_to_df(result_set)
                if not temp_df.empty:
                    raw_df = temp_df
                    break
        finally:
            bs.logout()

    normalized = _normalize_all_stock_basic(raw_df)
    if not normalized.empty:
        if use_cache:
            _save_cached_universe_data(today_cache_path, normalized)
        return normalized

    if use_cache:
        latest_cache = _find_latest_cache_file(UNIVERSE_CACHE_DIR, "a_share_basic_*.csv")
        if latest_cache is not None:
            stale_df = _load_cached_universe_data(
                cache_path=latest_cache,
                max_cache_age_hours=max_cache_age_hours,
                allow_stale=True,
            )
            if stale_df is not None:
                return stale_df

    return _empty_universe_df()


def fetch_all_a_share_codes(
    max_lookback_days: int = 10,
    use_cache: bool = True,
    force_refresh: bool = False,
    max_cache_age_hours: float = 24.0,
) -> list[str]:
    basic_df = fetch_all_a_share_basic(
        max_lookback_days=max_lookback_days,
        use_cache=use_cache,
        force_refresh=force_refresh,
        max_cache_age_hours=max_cache_age_hours,
    )
    if basic_df.empty:
        return []
    return sorted(basic_df["code"].dropna().astype(str).unique().tolist())


def fetch_code_name_map(
    codes: Iterable[str] | None = None,
    use_cache: bool = True,
    force_refresh: bool = False,
    max_cache_age_hours: float = 24.0,
) -> dict[str, str]:
    if not codes:
        return {}

    normalized_codes = [format_stock_code(code) for code in codes]
    normalized_codes = list(dict.fromkeys(normalized_codes))
    code_name_map: dict[str, str] = {}

    basic_df = fetch_all_a_share_basic(
        max_lookback_days=10,
        use_cache=use_cache,
        force_refresh=force_refresh,
        max_cache_age_hours=max_cache_age_hours,
    )
    if not basic_df.empty:
        basic_map = dict(zip(basic_df["code"].astype(str), basic_df["code_name"].astype(str)))
        for code in normalized_codes:
            name = basic_map.get(code, "").strip()
            if name and name.lower() not in {"nan", "none"}:
                code_name_map[code] = name

    missing_codes = [code for code in normalized_codes if code not in code_name_map]
    if not missing_codes:
        return code_name_map

    login_result = bs.login()
    if login_result.error_code != "0":
        return code_name_map

    try:
        for code in missing_codes:
            result_set = bs.query_stock_basic(code=to_baostock_code(code))
            if result_set.error_code != "0":
                continue
            data_frame = _result_set_to_df(result_set)
            if data_frame.empty:
                continue
            name_column = "code_name" if "code_name" in data_frame.columns else None
            if not name_column:
                continue
            code_name_map[code] = str(data_frame.iloc[0][name_column])
    finally:
        bs.logout()

    return code_name_map


def _empty_fund_flow_df() -> pd.DataFrame:
    return pd.DataFrame(columns=FUND_FLOW_COLUMNS)


def _normalize_fund_flow_snapshot(data_frame: pd.DataFrame) -> pd.DataFrame:
    if data_frame is None or data_frame.empty:
        return _empty_fund_flow_df()

    normalized = data_frame.copy()
    if set(FUND_FLOW_COLUMNS).issubset(normalized.columns):
        normalized["code"] = normalized["code"].map(format_stock_code)
        normalized["latest_price"] = pd.to_numeric(normalized["latest_price"], errors="coerce")
        normalized["net_inflow"] = pd.to_numeric(normalized["net_inflow"], errors="coerce")
        normalized = normalized.dropna(subset=["code"]).drop_duplicates(subset=["code"], keep="last")
        return normalized[FUND_FLOW_COLUMNS].reset_index(drop=True)

    if normalized.shape[1] < 7:
        return _empty_fund_flow_df()

    normalized["code"] = normalized.iloc[:, 1].map(format_stock_code)
    normalized["latest_price"] = pd.to_numeric(normalized.iloc[:, 3], errors="coerce")
    normalized["net_inflow"] = normalized.iloc[:, 6].map(parse_amount_text)
    normalized = normalized.dropna(subset=["code"]).drop_duplicates(subset=["code"], keep="last")
    return normalized[FUND_FLOW_COLUMNS].reset_index(drop=True)


def _build_fund_flow_cache_path(period_symbol: str, anchor_day: date | None = None) -> Path:
    day_text = (anchor_day or date.today()).strftime("%Y%m%d")
    period_key = _safe_cache_key(period_symbol)
    return FUND_FLOW_CACHE_DIR / f"fund_flow_{period_key}_{day_text}.csv"


def _load_cached_fund_flow_snapshot(
    cache_path: Path,
    max_cache_age_hours: float,
    allow_stale: bool = False,
) -> pd.DataFrame | None:
    if not cache_path.exists():
        return None
    if not allow_stale and not _is_cache_fresh(cache_path, max_cache_age_hours=max_cache_age_hours):
        return None

    cached_df = _read_csv_safe(cache_path)
    normalized = _normalize_fund_flow_snapshot(cached_df)
    return normalized if not normalized.empty else None


def _save_cached_fund_flow_snapshot(cache_path: Path, data_frame: pd.DataFrame) -> None:
    if data_frame.empty:
        return
    _ensure_cache_dir(FUND_FLOW_CACHE_DIR)
    data_frame.to_csv(cache_path, index=False, encoding="utf-8-sig")


def fetch_fund_flow_snapshot(
    period_symbol: str,
    use_cache: bool = True,
    force_refresh: bool = False,
    max_cache_age_hours: float = 6.0,
) -> pd.DataFrame:
    today_cache_path = _build_fund_flow_cache_path(period_symbol)
    if use_cache and not force_refresh:
        cached_df = _load_cached_fund_flow_snapshot(
            cache_path=today_cache_path,
            max_cache_age_hours=max_cache_age_hours,
            allow_stale=False,
        )
        if cached_df is not None:
            return cached_df

    fetched_df = pd.DataFrame()
    try:
        raw_df = ak.stock_fund_flow_individual(symbol=period_symbol)
        if raw_df is not None:
            fetched_df = _normalize_fund_flow_snapshot(raw_df)
    except Exception:
        fetched_df = pd.DataFrame()

    if not fetched_df.empty:
        if use_cache:
            _save_cached_fund_flow_snapshot(today_cache_path, fetched_df)
        return fetched_df

    if use_cache:
        period_key = _safe_cache_key(period_symbol)
        latest_cache = _find_latest_cache_file(FUND_FLOW_CACHE_DIR, f"fund_flow_{period_key}_*.csv")
        if latest_cache is not None:
            stale_df = _load_cached_fund_flow_snapshot(
                cache_path=latest_cache,
                max_cache_age_hours=max_cache_age_hours,
                allow_stale=True,
            )
            if stale_df is not None:
                return stale_df

    return _empty_fund_flow_df()


def get_cache_overview() -> dict[str, dict[str, object]]:
    overview: dict[str, dict[str, object]] = {}
    total_files = 0
    total_bytes = 0

    for scope, directory in CACHE_SCOPE_DIRS.items():
        files = [item for item in directory.glob("**/*") if item.is_file()] if directory.exists() else []
        file_count = len(files)
        size_bytes = sum(item.stat().st_size for item in files)
        latest_update = "-"
        if files:
            latest_ts = max(item.stat().st_mtime for item in files)
            latest_update = datetime.fromtimestamp(latest_ts).strftime("%Y-%m-%d %H:%M:%S")

        overview[scope] = {
            "files": file_count,
            "size_bytes": size_bytes,
            "size_mb": round(size_bytes / (1024 * 1024), 3),
            "latest_update": latest_update,
        }
        total_files += file_count
        total_bytes += size_bytes

    overview["all"] = {
        "files": total_files,
        "size_bytes": total_bytes,
        "size_mb": round(total_bytes / (1024 * 1024), 3),
        "latest_update": "-",
    }
    return overview


def clear_cache(scope: str = "all", older_than_days: int | None = None) -> dict[str, object]:
    scope_key = str(scope).lower().strip()
    if scope_key not in {"all", *CACHE_SCOPE_DIRS.keys()}:
        raise ValueError(f"不支持的缓存范围: {scope}")

    target_dirs = list(CACHE_SCOPE_DIRS.values()) if scope_key == "all" else [CACHE_SCOPE_DIRS[scope_key]]

    cutoff_time: datetime | None = None
    if older_than_days is not None and int(older_than_days) > 0:
        cutoff_time = datetime.now() - timedelta(days=int(older_than_days))

    deleted_files = 0
    deleted_bytes = 0
    failed_files = 0

    for directory in target_dirs:
        if not directory.exists():
            continue
        for item in directory.glob("**/*"):
            if not item.is_file():
                continue
            if cutoff_time is not None:
                item_time = datetime.fromtimestamp(item.stat().st_mtime)
                if item_time >= cutoff_time:
                    continue
            try:
                file_size = item.stat().st_size
                item.unlink()
                deleted_files += 1
                deleted_bytes += file_size
            except Exception:
                failed_files += 1

    return {
        "scope": scope_key,
        "deleted_files": deleted_files,
        "deleted_bytes": deleted_bytes,
        "deleted_mb": round(deleted_bytes / (1024 * 1024), 3),
        "failed_files": failed_files,
    }


def infer_report_period(anchor_date: date | datetime | str | None = None) -> tuple[int, int]:
    if anchor_date is None:
        anchor = date.today()
    else:
        anchor = pd.to_datetime(anchor_date).date()

    current_year = anchor.year
    current_month = anchor.month
    if current_month < 5:
        return current_year - 1, 3
    if current_month < 9:
        return current_year, 1
    if current_month < 11:
        return current_year, 2
    return current_year, 3


def previous_report_period(year: int, quarter: int) -> tuple[int, int]:
    if quarter <= 1:
        return year - 1, 4
    return year, quarter - 1
