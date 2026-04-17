import argparse
import datetime
import json
import os
import re
import socket
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import baostock as bs
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "stock_data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR = DATA_DIR / "auto_logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "stocks_data.db"

SECTOR_HINTS_PATH = DATA_DIR / "cctv_sector_stock_map.json"
HOT_SECTOR_PATTERN = "CCTV-Hot-Sectors-*.csv"
SECTOR_STOCK_POOL_PATTERN = "CCTV-Sector-Stock-Pool-*.csv"
SHARED_SEED_PATTERN = "Stock-Selection-Shared-Seed-*.csv"
DEFAULT_BS_TIMEOUT_SECONDS = float(os.getenv("BS_REQUEST_TIMEOUT_SECONDS", "15"))
DEFAULT_BS_REQUEST_INTERVAL_SECONDS = float(os.getenv("BS_REQUEST_INTERVAL_SECONDS", "0.05"))
DEFAULT_BS_MAX_RETRIES = int(os.getenv("BS_MAX_RETRIES", "2"))


_BS_RATE_LIMIT_LOCK = threading.Lock()
_BS_NEXT_ALLOWED_AT = 0.0


def parse_args():
    parser = argparse.ArgumentParser(description="A股短线题材策略（政策题材+动量，换手率可放宽）")
    parser.add_argument("--top-n", type=int, default=30, help="输出前N只股票")
    parser.add_argument("--max-stocks", type=int, default=1200, help="最多扫描多少只A股")
    parser.add_argument("--max-workers", type=int, default=8, help="扫描并发数，默认8")
    parser.add_argument("--hot-sector-top-n", type=int, default=5, help="使用CCTV热点板块前N")
    parser.add_argument("--min-latest-turn", type=float, default=0.8, help="最新换手率下限(%)")
    parser.add_argument("--min-avg-turn5", type=float, default=0.6, help="近5日平均换手率下限(%)")
    parser.add_argument("--min-latest-amount", type=float, default=2.0e8, help="最新成交额下限(元)")
    parser.add_argument("--min-latest-price", type=float, default=5.0, help="最新价格下限(元)")
    parser.add_argument("--max-latest-price", type=float, default=30.0, help="最新价格上限(元)")
    parser.add_argument("--output", default="", help="自定义输出CSV路径")
    parser.add_argument("--bs-timeout-seconds", type=float, default=DEFAULT_BS_TIMEOUT_SECONDS, help="baostock请求超时秒数")
    parser.add_argument("--bs-request-interval-seconds", type=float, default=DEFAULT_BS_REQUEST_INTERVAL_SECONDS, help="baostock请求最小间隔秒数")
    parser.add_argument("--bs-max-retries", type=int, default=DEFAULT_BS_MAX_RETRIES, help="baostock请求失败重试次数")
    return parser.parse_args()


def _to_float(value):
    try:
        return float(value)
    except Exception:
        return None


def _throttle_bs_request(interval_seconds):
    interval = max(0.0, float(interval_seconds))
    if interval <= 0:
        return

    global _BS_NEXT_ALLOWED_AT
    with _BS_RATE_LIMIT_LOCK:
        now = time.time()
        if now < _BS_NEXT_ALLOWED_AT:
            time.sleep(_BS_NEXT_ALLOWED_AT - now)
            now = time.time()
        _BS_NEXT_ALLOWED_AT = now + interval


def _latest_hot_sector_file():
    files = sorted(_iter_data_files(HOT_SECTOR_PATTERN), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _iter_data_files(pattern):
    files = list(DATA_DIR.glob(pattern))
    archive_dir = DATA_DIR / "archive"
    if archive_dir.exists():
        files.extend(list(archive_dir.rglob(pattern)))
    return files


def _normalize_code(code_text):
    code = str(code_text or "").strip()
    if not code:
        return ""
    code = code.lower().replace(".", "")
    if code.startswith("sh"):
        digits = code[2:]
        return f"sh.{digits}" if digits else ""
    if code.startswith("sz"):
        digits = code[2:]
        return f"sz.{digits}" if digits else ""
    if code.startswith("6"):
        return f"sh.{code}"
    if code.startswith(("0", "3")):
        return f"sz.{code}"
    return ""


def _load_shared_seed_universe(max_stocks, trade_day_text):
    today_tag = datetime.datetime.now().strftime("%Y%m%d")
    trade_tag = trade_day_text.replace("-", "")
    preferred = [
        DATA_DIR / f"Stock-Selection-Shared-Seed-{today_tag}.csv",
        DATA_DIR / f"Stock-Selection-Shared-Seed-{trade_tag}.csv",
    ]
    discovered = sorted(DATA_DIR.glob(SHARED_SEED_PATTERN), key=lambda p: p.stat().st_mtime, reverse=True)

    ordered_files = []
    seen = set()
    for f in preferred + discovered:
        if f.exists() and f not in seen:
            ordered_files.append(f)
            seen.add(f)

    for f in ordered_files:
        try:
            df = pd.read_csv(f, encoding="utf-8-sig")
        except Exception:
            continue
        if df.empty:
            continue

        if "股票代码" in df.columns:
            code_col = "股票代码"
        elif "code" in df.columns:
            code_col = "code"
        else:
            continue

        name_col = "股票名称" if "股票名称" in df.columns else ("name" if "name" in df.columns else None)
        tmp = df.copy()
        tmp["code"] = tmp[code_col].map(_normalize_code)
        tmp = tmp[tmp["code"].str.startswith(("sh.60", "sz.00", "sz.30"), na=False)]
        if tmp.empty:
            continue

        if name_col:
            tmp["name"] = tmp[name_col].astype(str)
        else:
            tmp["name"] = ""

        tmp = tmp.drop_duplicates(subset=["code"], keep="first")
        if max_stocks > 0:
            tmp = tmp.head(max_stocks)
        return tmp[["code", "name"]].to_dict("records"), str(f)

    return [], ""


def _load_hot_sectors(top_n):
    f = _latest_hot_sector_file()
    if f is None:
        return []
    try:
        df = pd.read_csv(f, encoding="utf-8-sig")
    except Exception:
        return []

    if df.empty or "板块" not in df.columns:
        return []

    sort_col = "热度分" if "热度分" in df.columns else "提及次数"
    tmp = df.copy()
    tmp[sort_col] = pd.to_numeric(tmp[sort_col], errors="coerce")
    tmp = tmp.sort_values(sort_col, ascending=False)
    return [str(x).strip() for x in tmp["板块"].head(top_n).tolist() if str(x).strip()]


def _load_sector_stock_pool_map(hot_sectors):
    if not hot_sectors:
        return {}

    files = sorted(_iter_data_files(SECTOR_STOCK_POOL_PATTERN), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return {}

    path = files[0]
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return {}

    if df.empty:
        return {}

    lower_map = {str(c).strip().lower(): str(c) for c in df.columns}
    code_col = ""
    for k in ["股票代码", "code", "symbol", "证券代码"]:
        col = lower_map.get(k.lower(), "")
        if col:
            code_col = col
            break

    sector_col = ""
    for k in ["板块", "sector", "主题", "题材"]:
        col = lower_map.get(k.lower(), "")
        if col:
            sector_col = col
            break

    if not code_col or not sector_col:
        return {}

    hot_set = {str(x).strip() for x in hot_sectors if str(x).strip()}
    out = {}
    for _, row in df.iterrows():
        sec = str(row.get(sector_col, "") or "").strip()
        if not sec or sec not in hot_set:
            continue
        norm_code = _normalize_code(row.get(code_col, ""))
        if not norm_code:
            continue
        out.setdefault(norm_code, set()).add(sec)
    return out


def _load_hot_sector_pool_universe(hot_sectors):
    if not hot_sectors:
        return []

    files = sorted(_iter_data_files(SECTOR_STOCK_POOL_PATTERN), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return []

    path = files[0]
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return []
    if df.empty:
        return []

    lower_map = {str(c).strip().lower(): str(c) for c in df.columns}
    code_col = ""
    for k in ["股票代码", "code", "symbol", "证券代码"]:
        col = lower_map.get(k.lower(), "")
        if col:
            code_col = col
            break

    name_col = ""
    for k in ["股票名称", "name", "证券名称"]:
        col = lower_map.get(k.lower(), "")
        if col:
            name_col = col
            break

    sector_col = ""
    for k in ["板块", "sector", "主题", "题材"]:
        col = lower_map.get(k.lower(), "")
        if col:
            sector_col = col
            break

    if not code_col or not sector_col:
        return []

    hot_set = {str(x).strip() for x in hot_sectors if str(x).strip()}
    rows = []
    seen = set()
    for _, row in df.iterrows():
        sec = str(row.get(sector_col, "") or "").strip()
        if not sec or sec not in hot_set:
            continue
        norm_code = _normalize_code(row.get(code_col, ""))
        if not norm_code:
            continue
        # 题材策略只处理主板/中小创，过滤北交等其它市场编码。
        if not norm_code.startswith(("sh.60", "sz.00", "sz.30")):
            continue
        if norm_code in seen:
            continue
        seen.add(norm_code)
        rows.append({
            "code": norm_code,
            "name": str(row.get(name_col, "") or "").strip() if name_col else "",
        })
    return rows


def _merge_universe_with_hot_pool(base_universe, hot_pool_universe):
    merged = []
    seen = set()

    for item in base_universe or []:
        code = str(item.get("code", "") or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        merged.append({"code": code, "name": str(item.get("name", "") or "")})

    for item in hot_pool_universe or []:
        code = str(item.get("code", "") or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        merged.append({"code": code, "name": str(item.get("name", "") or "")})

    return merged


def _load_sector_hints():
    if not SECTOR_HINTS_PATH.exists():
        return {}
    try:
        raw = json.loads(SECTOR_HINTS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

    result = {}
    if isinstance(raw, dict):
        for sec, hints in raw.items():
            if not isinstance(sec, str) or not isinstance(hints, list):
                continue
            cleaned = [str(x).strip() for x in hints if str(x).strip()]
            if cleaned:
                result[sec.strip()] = cleaned
    return result


def _match_theme(stock_code, stock_name, hot_sectors, sector_hints, sector_code_map):
    by_code = sorted(list((sector_code_map or {}).get(stock_code, set())))
    name = (stock_name or "").strip()
    matched = list(by_code)
    matched_set = set(matched)

    for sec in hot_sectors:
        sec_hits = 0
        for kw in sector_hints.get(sec, []):
            if kw and kw in name:
                sec_hits += 1
        if sec in name:
            sec_hits += 1
        if sec_hits > 0 and sec not in matched_set:
            matched.append(sec)
            matched_set.add(sec)

    return matched


def _latest_trading_day(today_text, lookback_days=45, request_interval_seconds=0.0):
    start_text = (datetime.datetime.strptime(today_text, "%Y-%m-%d") - datetime.timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    _throttle_bs_request(request_interval_seconds)
    rs = bs.query_trade_dates(start_date=start_text, end_date=today_text)
    if rs.error_code != "0":
        return today_text

    latest_trade_day = None
    while rs.next():
        row = rs.get_row_data()
        if not row or len(row) < 2:
            continue
        trade_date, is_trading = row[0], row[1]
        if is_trading == "1":
            latest_trade_day = trade_date

    return latest_trade_day or today_text


def _cache_table_name(cache_key):
    key = cache_key.replace("stock_data/", "").replace(".csv", "")
    key = re.sub(r"[^0-9a-zA-Z_]+", "_", key)
    key = re.sub(r"_+", "_", key).strip("_")
    if not key:
        key = "table"
    if key[0].isdigit():
        key = f"t_{key}"
    return key


def _read_cache_df(table_name):
    if not DB_PATH.exists():
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def _write_cache_df(table_name, df):
    if df is None or df.empty:
        return
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    except Exception:
        pass
    finally:
        conn.close()


def _query_all_a_stocks(max_stocks, day_text, request_interval_seconds=0.0, max_retries=2):
    cache_key = f"stock_data/baostock_all_stock_{day_text}.csv"
    table_name = _cache_table_name(cache_key)
    cached_df = _read_cache_df(table_name)
    if not cached_df.empty and {"code", "name"}.issubset(cached_df.columns):
        cached_df = cached_df.copy()
        cached_df["code"] = cached_df["code"].astype(str)
        cached_df["name"] = cached_df["name"].astype(str)
        cached_df = cached_df[cached_df["code"].str.startswith(("sh.60", "sz.00", "sz.30"))]
        if max_stocks > 0:
            cached_df = cached_df.head(max_stocks)
        return cached_df[["code", "name"]].to_dict("records")

    def _query_once(day_arg):
        retries = max(0, int(max_retries))
        rs = None
        for attempt in range(retries + 1):
            _throttle_bs_request(request_interval_seconds)
            rs = bs.query_all_stock(day=day_arg) if day_arg else bs.query_all_stock()
            if rs is not None and rs.error_code == "0":
                break
            if attempt < retries:
                time.sleep(min(0.2 * (attempt + 1), 1.0))

        if rs is None or rs.error_code != "0":
            return []

        out_rows = []
        while rs.next():
            row = rs.get_row_data()
            row_map = dict(zip(rs.fields, row))
            code = row_map.get("code", "")
            name = row_map.get("code_name", "")
            trade_status = row_map.get("tradeStatus", "")

            if trade_status not in ("1", ""):
                continue
            if not code.startswith(("sh.60", "sz.00", "sz.30")):
                continue
            out_rows.append({"code": code, "name": name})
        return out_rows

    rows = _query_once(day_text)
    if not rows:
        rows = _query_once("")
    if not rows:
        try:
            prev_day = (datetime.datetime.strptime(day_text, "%Y-%m-%d") - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        except Exception:
            prev_day = ""
        if prev_day:
            rows = _query_once(prev_day)
    if not rows:
        return []

    _write_cache_df(table_name, pd.DataFrame(rows))

    if max_stocks > 0:
        return rows[:max_stocks]
    return rows


def _fetch_recent_k(code, end_date_text, lookback_days=45, request_interval_seconds=0.0, max_retries=2):
    cache_key = f"stock_data/baostock_k_{code}_{end_date_text}_{lookback_days}.csv"
    table_name = _cache_table_name(cache_key)
    cached_df = _read_cache_df(table_name)
    if not cached_df.empty:
        cached_df = cached_df.copy()
        for col in ["close", "amount", "turn", "pctChg", "isST"]:
            if col in cached_df.columns:
                cached_df[col] = pd.to_numeric(cached_df[col], errors="coerce")
        keep_cols = [c for c in ["close", "amount", "turn"] if c in cached_df.columns]
        if keep_cols:
            cached_df = cached_df.dropna(subset=keep_cols)
        if not cached_df.empty:
            return cached_df.reset_index(drop=True)

    start_date_text = (datetime.datetime.strptime(end_date_text, "%Y-%m-%d") - datetime.timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    retries = max(0, int(max_retries))
    data_list = []
    rs_fields = []
    for attempt in range(retries + 1):
        _throttle_bs_request(request_interval_seconds)
        rs = bs.query_history_k_data_plus(
            code,
            "date,code,close,amount,turn,pctChg,isST",
            start_date=start_date_text,
            end_date=end_date_text,
            frequency="d",
            adjustflag="2",
        )
        if rs is None or rs.error_code != "0":
            if attempt < retries:
                time.sleep(min(0.2 * (attempt + 1), 1.0))
            continue

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        rs_fields = list(getattr(rs, "fields", []) or [])
        if data_list and rs_fields:
            break

        if attempt < retries:
            time.sleep(min(0.2 * (attempt + 1), 1.0))

    if not data_list or not rs_fields:
        return pd.DataFrame()

    df = pd.DataFrame(data_list, columns=rs_fields)
    for col in ["close", "amount", "turn", "pctChg", "isST"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["close", "amount", "turn"])
    _write_cache_df(table_name, df)
    return df.reset_index(drop=True)


def _calc_score(row):
    latest_turn = row["最新换手率%"]
    avg_turn5 = row["近5日换手均值%"]
    vol_ratio = row["成交额放大倍数"]
    ret5 = row["5日涨跌幅%"]
    ret20 = row["20日涨跌幅%"]
    near_high = row["距20日高点比"]
    theme_hits = row["题材命中数"]

    turn_score = min(avg_turn5 / 10.0, 1.0) * 10 + min(latest_turn / 15.0, 1.0) * 5
    flow_score = min(vol_ratio / 2.5, 1.0) * 25
    mom_score = min(max(ret5, 0.0) / 8.0, 1.0) * 10 + min(max(ret20, 0.0) / 25.0, 1.0) * 10
    high_score = min(max(near_high - 0.9, 0.0) / 0.1, 1.0) * 10
    theme_score = min(theme_hits, 3) / 3.0 * 30

    penalty = 0.0
    if ret5 > 12:
        penalty += 5.0
    if ret20 > 45:
        penalty += 5.0

    return round(turn_score + flow_score + mom_score + high_score + theme_score - penalty, 2)


def _append_log(log_path, message):
    if not log_path:
        return
    try:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with log_path.open("a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {message}\n")
    except Exception:
        pass


def _evaluate_theme_candidate(item, hot_sectors, sector_hints, sector_code_map, end_date_text, args):
    code = item["code"]
    name = item["name"]

    start_ts = time.time()
    kdf = _fetch_recent_k(
        code,
        end_date_text,
        request_interval_seconds=args.bs_request_interval_seconds,
        max_retries=args.bs_max_retries,
    )
    cost = time.time() - start_ts
    slow_msg = f"slow k data: {code} {name} cost={cost:.2f}s" if cost >= 5.0 else None
    if len(kdf) < 25:
        return None, slow_msg

    latest = kdf.iloc[-1]
    if _to_float(latest.get("isST")) == 1:
        return None, slow_msg

    close_series = kdf["close"]
    amount_series = kdf["amount"]
    turn_series = kdf["turn"]

    latest_close = float(close_series.iloc[-1])
    latest_amount = float(amount_series.iloc[-1])
    latest_turn = float(turn_series.iloc[-1])
    avg_turn5 = float(turn_series.tail(5).mean())

    prev_amount_window = amount_series.iloc[-10:-1]
    avg_amount_prev = float(prev_amount_window.mean()) if len(prev_amount_window) > 0 else 0.0
    if avg_amount_prev <= 0:
        return None, slow_msg
    vol_ratio = latest_amount / avg_amount_prev

    close_6 = float(close_series.iloc[-6])
    close_21 = float(close_series.iloc[-21])
    ret5 = (latest_close / close_6 - 1.0) * 100.0 if close_6 > 0 else 0.0
    ret20 = (latest_close / close_21 - 1.0) * 100.0 if close_21 > 0 else 0.0

    max20 = float(close_series.tail(20).max())
    near_high = latest_close / max20 if max20 > 0 else 0.0

    if latest_turn < args.min_latest_turn:
        return None, slow_msg
    if latest_close < args.min_latest_price:
        return None, slow_msg
    if latest_close > args.max_latest_price:
        return None, slow_msg
    if avg_turn5 < args.min_avg_turn5:
        return None, slow_msg
    if latest_amount < args.min_latest_amount:
        return None, slow_msg
    if near_high < 0.9:
        return None, slow_msg
    if ret20 < 0 or ret20 > 60:
        return None, slow_msg

    themes = _match_theme(code, name, hot_sectors, sector_hints, sector_code_map)

    row = {
        "股票代码": code,
        "股票名称": name,
        "最新价": round(latest_close, 2),
        "最新换手率%": round(latest_turn, 2),
        "近5日换手均值%": round(avg_turn5, 2),
        "最新成交额": round(latest_amount, 0),
        "成交额放大倍数": round(vol_ratio, 2),
        "5日涨跌幅%": round(ret5, 2),
        "20日涨跌幅%": round(ret20, 2),
        "距20日高点比": round(near_high, 3),
        "题材命中数": len(themes),
        "题材标签": ",".join(themes),
    }
    row["综合分"] = _calc_score(row)
    return row, slow_msg


def build_strategy_candidates(args, log_path=None):
    hot_sectors = _load_hot_sectors(args.hot_sector_top_n)
    sector_hints = _load_sector_hints()
    sector_code_map = _load_sector_stock_pool_map(hot_sectors)

    today_text = datetime.datetime.now().strftime("%Y-%m-%d")
    trade_day_text = _latest_trading_day(
        today_text,
        request_interval_seconds=args.bs_request_interval_seconds,
    )
    universe, seed_path = _load_shared_seed_universe(args.max_stocks, trade_day_text)
    if universe:
        _append_log(log_path, f"共享候选池命中: {seed_path} | 样本数: {len(universe)}")
    else:
        universe = _query_all_a_stocks(
            args.max_stocks,
            trade_day_text,
            request_interval_seconds=args.bs_request_interval_seconds,
            max_retries=args.bs_max_retries,
        )

    hot_pool_universe = _load_hot_sector_pool_universe(hot_sectors)
    base_count = len(universe)
    universe = _merge_universe_with_hot_pool(universe, hot_pool_universe)
    added_hot_pool = max(len(universe) - base_count, 0)
    print(f"扫描股票数量: {len(universe)}")
    print(f"热点板块: {', '.join(hot_sectors) if hot_sectors else '无'}")
    print(f"热点成分股映射: {len(sector_code_map)}")
    print(f"热点成分股并入: +{added_hot_pool}")
    _append_log(log_path, f"扫描股票数量: {len(universe)}")
    _append_log(log_path, f"热点板块: {', '.join(hot_sectors) if hot_sectors else '无'}")
    _append_log(log_path, f"热点成分股映射: {len(sector_code_map)}")
    _append_log(log_path, f"热点成分股并入: +{added_hot_pool}")

    # Align K data window with the latest trading day to avoid empty results on non-trading days.
    end_date_text = trade_day_text
    rows = []
    total_count = len(universe)
    started_at = time.time()
    last_report_at = started_at
    progress_every = 30
    report_interval_sec = 12

    worker_count = max(1, int(args.max_workers))
    if worker_count > 1 and total_count > 1:
        print(f"启用题材并发扫描: workers={worker_count}")
        _append_log(log_path, f"启用题材并发扫描: workers={worker_count}")
        _append_log(
            log_path,
            f"baostock参数: timeout={args.bs_timeout_seconds}s, interval={args.bs_request_interval_seconds}s, retries={args.bs_max_retries}",
        )
        with ThreadPoolExecutor(max_workers=worker_count) as ex:
            futures = {
                ex.submit(_evaluate_theme_candidate, item, hot_sectors, sector_hints, sector_code_map, end_date_text, args): item
                for item in universe
            }
            completed = 0
            for fut in as_completed(futures):
                completed += 1
                try:
                    row, slow_msg = fut.result()
                except Exception as exc:
                    row, slow_msg = None, None
                    _append_log(log_path, f"worker error: {type(exc).__name__}: {exc}")
                if slow_msg:
                    _append_log(log_path, slow_msg)
                if row is not None:
                    rows.append(row)

                now_ts = time.time()
                should_report = (completed % progress_every == 0) or ((now_ts - last_report_at) >= report_interval_sec)
                if should_report:
                    elapsed = max(now_ts - started_at, 1e-6)
                    speed = completed / elapsed
                    remain = max(total_count - completed, 0)
                    eta_sec = int(remain / speed) if speed > 1e-9 else -1
                    eta_text = f"{eta_sec}s" if eta_sec >= 0 else "N/A"
                    msg = f"进度: {completed}/{total_count} ({completed/total_count:.1%}) | 速率: {speed:.2f}只/s | 预计剩余: {eta_text}"
                    print(msg)
                    _append_log(log_path, msg)
                    last_report_at = now_ts
    else:
        _append_log(
            log_path,
            f"baostock参数: timeout={args.bs_timeout_seconds}s, interval={args.bs_request_interval_seconds}s, retries={args.bs_max_retries}",
        )
        for idx, item in enumerate(universe, start=1):
            row, slow_msg = _evaluate_theme_candidate(item, hot_sectors, sector_hints, sector_code_map, end_date_text, args)
            if slow_msg:
                _append_log(log_path, slow_msg)
            if row is not None:
                rows.append(row)

            now_ts = time.time()
            should_report = (idx % progress_every == 0) or ((now_ts - last_report_at) >= report_interval_sec)
            if should_report:
                elapsed = max(now_ts - started_at, 1e-6)
                speed = idx / elapsed
                remain = max(total_count - idx, 0)
                eta_sec = int(remain / speed) if speed > 1e-9 else -1
                eta_text = f"{eta_sec}s" if eta_sec >= 0 else "N/A"
                msg = f"进度: {idx}/{total_count} ({idx/total_count:.1%}) | 速率: {speed:.2f}只/s | 预计剩余: {eta_text}"
                print(msg)
                _append_log(log_path, msg)
                last_report_at = now_ts

    if not rows:
        return pd.DataFrame(), hot_sectors

    out_df = pd.DataFrame(rows)
    out_df = out_df.sort_values(by=["综合分", "题材命中数", "成交额放大倍数"], ascending=[False, False, False])
    return out_df.reset_index(drop=True), hot_sectors


def main():
    args = parse_args()

    # 防止单个baostock请求长时间无响应导致线程池卡死。
    socket.setdefaulttimeout(max(3.0, float(args.bs_timeout_seconds)))

    log_file = LOG_DIR / f"theme_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    _append_log(log_file, f"start: {__file__}")
    _append_log(log_file, f"args: {args}")

    login_res = bs.login()
    if login_res.error_code != "0":
        print(f"baostock 登录失败: {login_res.error_msg}")
        return 1

    try:
        result_df, hot_sectors = build_strategy_candidates(args, log_path=log_file)
    finally:
        bs.logout()

    today_text = datetime.datetime.now().strftime("%Y%m%d")
    if args.output.strip():
        out_path = Path(args.output.strip())
        if not out_path.is_absolute():
            out_path = (ROOT_DIR / out_path).resolve()
    else:
        out_path = DATA_DIR / f"Stock-Selection-Ashare-Theme-Turnover-{today_text}.csv"

    if result_df.empty:
        print("未筛选出符合条件的股票。")
        print(f"热点板块参考: {', '.join(hot_sectors) if hot_sectors else '无'}")
        return 0

    top_df = result_df.head(args.top_n)
    top_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"策略输出已保存: {out_path}")
    print(f"总候选数: {len(result_df)}，展示前{len(top_df)}只")
    print(top_df[["股票代码", "股票名称", "综合分", "最新换手率%", "成交额放大倍数", "题材标签"]].to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
