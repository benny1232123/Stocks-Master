import argparse
import datetime
import json
import time
from pathlib import Path

import baostock as bs
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "stock_data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR = DATA_DIR / "auto_logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

SECTOR_HINTS_PATH = DATA_DIR / "cctv_sector_stock_map.json"
HOT_SECTOR_PATTERN = "CCTV-Hot-Sectors-*.csv"


def parse_args():
    parser = argparse.ArgumentParser(description="A股短线题材策略（政策题材+动量，换手率可放宽）")
    parser.add_argument("--top-n", type=int, default=30, help="输出前N只股票")
    parser.add_argument("--max-stocks", type=int, default=1200, help="最多扫描多少只A股")
    parser.add_argument("--hot-sector-top-n", type=int, default=5, help="使用CCTV热点板块前N")
    parser.add_argument("--min-latest-turn", type=float, default=0.8, help="最新换手率下限(%)")
    parser.add_argument("--min-avg-turn5", type=float, default=0.6, help="近5日平均换手率下限(%)")
    parser.add_argument("--min-latest-amount", type=float, default=2.0e8, help="最新成交额下限(元)")
    parser.add_argument("--min-latest-price", type=float, default=5.0, help="最新价格下限(元)")
    parser.add_argument("--max-latest-price", type=float, default=30.0, help="最新价格上限(元)")
    parser.add_argument("--output", default="", help="自定义输出CSV路径")
    return parser.parse_args()


def _to_float(value):
    try:
        return float(value)
    except Exception:
        return None


def _latest_hot_sector_file():
    files = sorted(DATA_DIR.glob(HOT_SECTOR_PATTERN), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


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


def _match_theme(stock_name, hot_sectors, sector_hints):
    name = (stock_name or "").strip()
    matched = []

    for sec in hot_sectors:
        sec_hits = 0
        for kw in sector_hints.get(sec, []):
            if kw and kw in name:
                sec_hits += 1
        if sec in name:
            sec_hits += 1
        if sec_hits > 0:
            matched.append(sec)

    return matched


def _latest_trading_day(today_text, lookback_days=45):
    start_text = (datetime.datetime.strptime(today_text, "%Y-%m-%d") - datetime.timedelta(days=lookback_days)).strftime("%Y-%m-%d")
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


def _query_all_a_stocks(max_stocks, day_text):
    rs = bs.query_all_stock(day=day_text)
    if rs.error_code != "0":
        rs = bs.query_all_stock()
    if rs.error_code != "0":
        return []

    rows = []
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
        rows.append({"code": code, "name": name})

    if max_stocks > 0:
        return rows[:max_stocks]
    return rows


def _fetch_recent_k(code, end_date_text, lookback_days=45):
    start_date_text = (datetime.datetime.strptime(end_date_text, "%Y-%m-%d") - datetime.timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rs = bs.query_history_k_data_plus(
        code,
        "date,code,close,amount,turn,pctChg,isST",
        start_date=start_date_text,
        end_date=end_date_text,
        frequency="d",
        adjustflag="2",
    )
    if rs.error_code != "0":
        return pd.DataFrame()

    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    if not data_list:
        return pd.DataFrame()

    df = pd.DataFrame(data_list, columns=rs.fields)
    for col in ["close", "amount", "turn", "pctChg", "isST"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["close", "amount", "turn"])
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


def build_strategy_candidates(args, log_path=None):
    hot_sectors = _load_hot_sectors(args.hot_sector_top_n)
    sector_hints = _load_sector_hints()

    today_text = datetime.datetime.now().strftime("%Y-%m-%d")
    trade_day_text = _latest_trading_day(today_text)
    universe = _query_all_a_stocks(args.max_stocks, trade_day_text)
    print(f"扫描股票数量: {len(universe)}")
    print(f"热点板块: {', '.join(hot_sectors) if hot_sectors else '无'}")
    _append_log(log_path, f"扫描股票数量: {len(universe)}")
    _append_log(log_path, f"热点板块: {', '.join(hot_sectors) if hot_sectors else '无'}")

    # Align K data window with the latest trading day to avoid empty results on non-trading days.
    end_date_text = trade_day_text
    rows = []
    total_count = len(universe)
    started_at = time.time()
    last_report_at = started_at
    progress_every = 30
    report_interval_sec = 12

    for idx, item in enumerate(universe, start=1):
        code = item["code"]
        name = item["name"]

        start_ts = time.time()
        kdf = _fetch_recent_k(code, end_date_text)
        cost = time.time() - start_ts
        if cost >= 5.0:
            _append_log(log_path, f"slow k data: {code} {name} cost={cost:.2f}s")
        if len(kdf) < 25:
            continue

        latest = kdf.iloc[-1]
        if _to_float(latest.get("isST")) == 1:
            continue

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
            continue
        vol_ratio = latest_amount / avg_amount_prev

        close_6 = float(close_series.iloc[-6])
        close_21 = float(close_series.iloc[-21])
        ret5 = (latest_close / close_6 - 1.0) * 100.0 if close_6 > 0 else 0.0
        ret20 = (latest_close / close_21 - 1.0) * 100.0 if close_21 > 0 else 0.0

        max20 = float(close_series.tail(20).max())
        near_high = latest_close / max20 if max20 > 0 else 0.0

        if latest_turn < args.min_latest_turn:
            continue
        if latest_close < args.min_latest_price:
            continue
        if latest_close > args.max_latest_price:
            continue
        if avg_turn5 < args.min_avg_turn5:
            continue
        if latest_amount < args.min_latest_amount:
            continue
        if near_high < 0.9:
            continue
        if ret20 < 0 or ret20 > 60:
            continue

        themes = _match_theme(name, hot_sectors, sector_hints)

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
