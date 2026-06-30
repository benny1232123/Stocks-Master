# --- 新增：宏观新闻多日趋势 ---
def _build_macro_news_trend_summary(window_days=3, top_n=0, *, auto_fetch=True):
    """
    汇总最近 window_days 天的宏观新闻风险事件趋势。
    """
    today = datetime.now()
    news_files = []
    for i in range(window_days):
        d = today - timedelta(days=i)
        date_str = d.strftime("%Y%m%d")
        f = _ensure_news_file(date_str, auto_fetch=auto_fetch)
        if f and f.exists():
            news_files.append((f, date_str))
    if not news_files:
        return "\n- 宏观新闻趋势: 近几天无新闻文件"

    try:
        burst_min = max(int(os.getenv("MACRO_RISK_BURST_MIN_COUNT", "3").strip() or "3"), 1)
    except Exception:
        burst_min = 3
    try:
        burst_top_n = max(int(os.getenv("MACRO_RISK_BURST_TOP_N", "10").strip() or "10"), 1)
    except Exception:
        burst_top_n = 10
    burst_tokens = _extract_burst_tokens(news_files, min_count=burst_min, top_n=burst_top_n)
    if not burst_tokens:
        burst_tokens = set(MACRO_RISK_SIGNAL_FRAGMENTS)
    all_events = _collect_macro_risk_events(
        news_files,
        burst_tokens,
        auto_mode=True,
    )
    if not all_events:
        return "\n- 宏观新闻趋势: 近几天无高/中风险新闻"
    # 按日期降序、风险分排序
    all_events.sort(key=lambda x: (x["date"], -x["risk_score"]))
    lines = ["\n## 宏观新闻风险趋势（近%d天）:" % window_days]
    cur_date = None
    count = 0
    for item in all_events:
        if cur_date != item["date"]:
            lines.append(f"- {item['date']}:")
            cur_date = item["date"]
            count = 0
        if top_n > 0 and count >= top_n:
            continue
        score = item["risk_score"]
        level = "高" if score >= 4 else "中" if score >= 2 else "低"
        tags = "/".join(item["tags"]) if item["tags"] else "综合"
        lines.append(f"    [{level}] {item['title']} | 影响链条: {tags}")
        count += 1
    return "\n".join(lines)

import csv
import argparse
import json
import os
import queue
import re
import sqlite3
import smtplib
import subprocess
import sys
import threading
import time
import traceback
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta
from email.message import EmailMessage
from functools import lru_cache
from pathlib import Path
from urllib import error, request

import akshare as ak
import baostock as bs
import pandas as pd


# --- 宏观外部数据：美股 / 汇率 / 期货 ---

import math as _math

# --- 共享内核（smcore）接入 ---
# 此前本文件自带第三套 Boll/代码标准化/K线获取实现，且用 adjustflag="3"(不复权)，
# 导致除权除息日布林带断裂、信号失真。现统一委托 smcore（前复权 + 单例会话 + 唯一 Boll 实现）。
import sys as _sys
from pathlib import Path as _Path

_REPO_ROOT = _Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_REPO_ROOT))

from smcore.data import fetch_daily_k as _smcore_fetch_daily_k, session as _smcore_session
from smcore.indicators import calc_bollinger as _smcore_calc_bollinger
from smcore.utils.code import (
    format_stock_code as _smcore_format_code,
    to_baostock_code as _smcore_to_bs_code,
    to_ak_index_symbol as _smcore_to_ak_index,
)
from smcore.cache import (
    cache_table_name as _smcore_cache_table_name,
    read_cache_df as _smcore_read_cache_df,
    write_cache_df as _smcore_write_cache_df,
)
from smcore.utils.format import (
    to_float as _smcore_to_float,
    normalize_confidence_label as _smcore_normalize_confidence_label,
    format_yi as _smcore_format_yi,
    safe_pct as _smcore_safe_pct,
    to_percent_like as _smcore_to_percent_like,
    fmt_pct as _smcore_fmt_pct,
    fmt_num as _smcore_fmt_num,
)
from smcore.strategy import (
    env_int_percent as _smcore_env_int_percent,
    normalize_weight_map as _smcore_normalize_weight_map,
    rebalance_for_signal_availability as _smcore_rebalance_for_signal_availability,
    format_position_units as _smcore_format_position_units,
    build_strategy_allocation as _smcore_build_strategy_allocation,
)
from smcore.data.index import (
    fetch_index_close_series as _smcore_fetch_index_close_series,
    calc_index_metrics as _smcore_calc_index_metrics,
)
from smcore.risk import (
    safe_float as _smcore_safe_float,
    fetch_us_market_data as _smcore_fetch_us_market_data,
    fetch_fx_data as _smcore_fetch_fx_data,
    fetch_futures_data as _smcore_fetch_futures_data,
    assess_us_market_risk as _smcore_assess_us_market_risk,
    assess_fx_risk as _smcore_assess_fx_risk,
    assess_futures_risk as _smcore_assess_futures_risk,
    # 宏观词库与文本函数（3b-2 抽出）
    MACRO_STOPWORDS as _SM_MACRO_STOPWORDS,
    MACRO_NOISE_TOKENS as _SM_MACRO_NOISE_TOKENS,
    MACRO_RISK_SIGNAL_FRAGMENTS as _SM_MACRO_RISK_SIGNAL_FRAGMENTS,
    MACRO_RISK_STRONG_FRAGMENTS as _SM_MACRO_RISK_STRONG_FRAGMENTS,
    MACRO_RISK_SOFT_FRAGMENTS as _SM_MACRO_RISK_SOFT_FRAGMENTS,
    MACRO_RISK_POSITIVE_HINTS as _SM_MACRO_RISK_POSITIVE_HINTS,
    CCTV_NOISE_SECTORS as _SM_CCTV_NOISE_SECTORS,
    MACRO_RISK_EXCLUDED_NEWS_TITLES as _SM_MACRO_RISK_EXCLUDED_NEWS_TITLES,
    MACRO_PROMO_TITLE_KEYWORDS as _SM_MACRO_PROMO_TITLE_KEYWORDS,
    is_macro_noise_token as _smcore_is_macro_noise_token,
    is_cctv_noise_sector as _smcore_is_cctv_noise_sector,
    is_macro_risk_term_allowed as _smcore_is_macro_risk_term_allowed,
    has_positive_macro_context as _smcore_has_positive_macro_context,
    is_macro_risk_excluded_news_title as _smcore_is_macro_risk_excluded_news_title,
    is_promo_or_historical_title as _smcore_is_promo_or_historical_title,
    clean_macro_terms as _smcore_clean_macro_terms,
    extract_macro_tokens as _smcore_extract_macro_tokens,
    nlp_level_to_score as _smcore_nlp_level_to_score,
    get_nlp_classifier as _smcore_get_nlp_classifier,
    nlp_risk_classify as _smcore_nlp_risk_classify,
    extract_burst_tokens as _smcore_extract_burst_tokens,
    collect_macro_risk_events as _smcore_collect_macro_risk_events,
    macro_risk_level as _smcore_macro_risk_level,
)


def _safe_float(val, default=None):
    # 委托 smcore.risk（全项目唯一实现）。
    return _smcore_safe_float(val, default)


def _fetch_us_market_data():
    # 委托 smcore.risk。
    return _smcore_fetch_us_market_data()


def _fetch_fx_data():
    # 委托 smcore.risk。
    return _smcore_fetch_fx_data()


def _fetch_futures_data():
    # 委托 smcore.risk。
    return _smcore_fetch_futures_data()


def _assess_us_market_risk(us_data):
    # 委托 smcore.risk。
    return _smcore_assess_us_market_risk(us_data)


def _assess_fx_risk(fx_data):
    # 委托 smcore.risk。
    return _smcore_assess_fx_risk(fx_data)


def _assess_futures_risk(futures_data):
    # 委托 smcore.risk。
    return _smcore_assess_futures_risk(futures_data)


def _build_macro_external_summary():
    """汇总美股/汇率/期货的外部风险信号，返回文本和综合风险等级。"""
    us_data = _fetch_us_market_data()
    fx_data = _fetch_fx_data()
    futures_data = _fetch_futures_data()

    us_level, us_reason = _assess_us_market_risk(us_data)
    fx_level, fx_reason = _assess_fx_risk(fx_data)
    fut_level, fut_reason = _assess_futures_risk(futures_data)

    level_map = {"high": 3, "medium": 2, "low": 1}
    max_level = max(level_map.get(us_level, 1), level_map.get(fx_level, 1), level_map.get(fut_level, 1))
    overall = {v: k for k, v in level_map.items()}[max_level]

    lines = []
    lines.append("\n## 7b) 宏观外部市场信号")
    lines.append("- 数据来源: akshare (Sina/FX/期货历史)")

    # 美股
    if us_data:
        parts = []
        for name in ["sp500", "nasdaq", "dow"]:
            info = us_data.get(name)
            if not info:
                continue
            label = {"sp500": "标普500", "nasdaq": "纳指", "dow": "道指"}[name]
            c = info.get("close")
            r1 = info.get("ret_1d")
            r5 = info.get("ret_5d")
            r20 = info.get("ret_20d")
            seg = f"{label}"
            if c:
                seg += f" {c:.0f}"
            if r1 is not None:
                seg += f" 1日{'+' if r1 >= 0 else ''}{r1:.1f}%"
            if r5 is not None:
                seg += f" 5日{'+' if r5 >= 0 else ''}{r5:.1f}%"
            if r20 is not None:
                seg += f" 20日{'+' if r20 >= 0 else ''}{r20:.1f}%"
            parts.append(seg)
        if parts:
            lines.append(f"- 美股: {' | '.join(parts)} [{us_level}]")
    else:
        lines.append("- 美股: 数据获取失败")

    # 汇率
    if fx_data:
        fx_parts = []
        for key, label in [("usdcny", "USD/CNY"), ("eurcny", "EUR/CNY"), ("gbpcny", "GBP/CNY")]:
            val = fx_data.get(key)
            if val is not None and not (isinstance(val, float) and _math.isnan(val)):
                fx_parts.append(f"{label}={val:.4f}")
        if fx_parts:
            lines.append(f"- 汇率: {' | '.join(fx_parts)} [{fx_level}]")
    else:
        lines.append("- 汇率: 数据获取失败")

    # 期货
    if futures_data:
        fut_parts = []
        for name in ["crude_oil", "gold", "copper"]:
            info = futures_data.get(name)
            if not info:
                continue
            label = {"crude_oil": "原油", "gold": "黄金", "copper": "铜"}[name]
            c = info.get("close")
            r1 = info.get("ret_1d")
            r5 = info.get("ret_5d")
            seg = f"{label}"
            if c:
                seg += f" {c:.1f}"
            if r1 is not None:
                seg += f" 1日{'+' if r1 >= 0 else ''}{r1:.1f}%"
            if r5 is not None:
                seg += f" 5日{'+' if r5 >= 0 else ''}{r5:.1f}%"
            fut_parts.append(seg)
        if fut_parts:
            lines.append(f"- 期货: {' | '.join(fut_parts)} [{fut_level}]")
    else:
        lines.append("- 期货: 数据获取失败")

    # 综合
    reasons = [r for r in [us_reason, fx_reason, fut_reason] if r]
    if overall == "high":
        lines.append(f"- 综合风险: **高** - {'; '.join(reasons)}")
    elif overall == "medium":
        lines.append(f"- 综合风险: 中 - {'; '.join(reasons)}")
    else:
        lines.append("- 综合风险: 低 - 外部市场整体平稳")

    return "\n".join(lines), overall


# --- 经济日历：news_economic_baidu 结构化数据 ---
def _fetch_economic_calendar_risk(window_days=7):
    """从百度经济日历获取近期重要事件，返回 (summary_text, risk_level)。"""
    try:
        df = ak.news_economic_baidu()
        if df is None or df.empty:
            return "", "low"
    except Exception:
        return "", "low"

    try:
        cols = df.columns.tolist()
        date_col, country_col, event_col = cols[0], cols[2], cols[3]
        actual_col, forecast_col, prev_col, imp_col = cols[4], cols[5], cols[6], cols[7]
    except Exception:
        return "", "low"

    now = datetime.now()
    cutoff = now - timedelta(days=window_days)
    cutoff_str = cutoff.strftime("%Y-%m-%d")

    risks = []
    for _, row in df.iterrows():
        try:
            event_date = str(row.iloc[0]).strip()
            country = str(row.iloc[2]).strip()
            event = str(row.iloc[3]).strip()
            importance = int(row.iloc[7]) if pd.notna(row.iloc[7]) else 0
            actual = row.iloc[4]
            forecast = row.iloc[5]
        except Exception:
            continue
        if event_date < cutoff_str:
            continue
        if importance < 2:
            continue
        miss_detected = False
        if pd.notna(actual) and pd.notna(forecast):
            try:
                av = float(actual)
                fv = float(forecast)
                if fv != 0:
                    miss_pct = abs(av - fv) / abs(fv) * 100
                    if miss_pct > 20:
                        miss_detected = True
            except (ValueError, TypeError):
                pass
        if miss_detected:
            direction = "好于" if av > fv else "差于"
            risks.append(f"{country} {event}: 实际{actual} vs 预期{forecast} ({direction}预期)")

    if not risks:
        return "", "low"

    level = "high" if len(risks) >= 2 else "medium"
    lines = [f"\n#### 经济日历风险（近{window_days}天）"]
    for r in risks[:5]:
        lines.append(f"- {r}")
    if len(risks) > 5:
        lines.append(f"- ... 共{len(risks)}条")
    return "\n".join(lines), level


ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "Stock-Selection-Boll.py"
THEME_SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "Stock-Selection-Ashare-Theme-Turnover.py"
CCTV_SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "Stock-Selection-CCTV-Sectors.py"
RELATIVITY_SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "Stock-Selection-Relativity.py"
CLEANUP_SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "cleanup_stock_data.py"
ARCHIVE_SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "archive_stock_data.py"
COMPRESS_SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "compress_stock_data.py"
STOCK_DATA_DIR = ROOT_DIR / "stock_data"
LOG_DIR = STOCK_DATA_DIR / "auto_logs"
PIPELINE_TOTAL_STEPS = 8
DB_PATH = STOCK_DATA_DIR / "stocks_data.db"
RUN_LOG_FILE = None
RUN_LOG_LOCK = threading.Lock()
MACRO_STOPWORDS = _SM_MACRO_STOPWORDS
MACRO_NOISE_TOKENS = _SM_MACRO_NOISE_TOKENS
MACRO_RISK_SIGNAL_FRAGMENTS = _SM_MACRO_RISK_SIGNAL_FRAGMENTS
MACRO_RISK_STRONG_FRAGMENTS = _SM_MACRO_RISK_STRONG_FRAGMENTS
MACRO_RISK_SOFT_FRAGMENTS = _SM_MACRO_RISK_SOFT_FRAGMENTS
MACRO_RISK_POSITIVE_HINTS = _SM_MACRO_RISK_POSITIVE_HINTS



def _is_macro_noise_token(token):
    return str(token).strip() in MACRO_NOISE_TOKENS


# ── 以下宏观词库/文本/NLP/事件函数已委托 smcore.risk.macro（3b-2 抽出） ──
CCTV_NOISE_SECTORS = _SM_CCTV_NOISE_SECTORS
MACRO_RISK_EXCLUDED_NEWS_TITLES = _SM_MACRO_RISK_EXCLUDED_NEWS_TITLES
MACRO_PROMO_TITLE_KEYWORDS = _SM_MACRO_PROMO_TITLE_KEYWORDS


def _is_macro_noise_token(token):
    return _smcore_is_macro_noise_token(token)


def _is_cctv_noise_sector(name):
    return _smcore_is_cctv_noise_sector(name)


def _is_macro_risk_term_allowed(term):
    return _smcore_is_macro_risk_term_allowed(term)


def _has_positive_macro_context(text):
    return _smcore_has_positive_macro_context(text)


def _is_macro_risk_excluded_news_title(title):
    return _smcore_is_macro_risk_excluded_news_title(title)


def _is_promo_or_historical_title(title):
    return _smcore_is_promo_or_historical_title(title)


def _clean_macro_terms(values):
    return _smcore_clean_macro_terms(values)


def _nlp_level_to_score(level):
    return _smcore_nlp_level_to_score(level)


def _get_nlp_classifier():
    return _smcore_get_nlp_classifier()


def _nlp_risk_classify(text):
    return _smcore_nlp_risk_classify(text)


def _extract_macro_tokens(text):
    return _smcore_extract_macro_tokens(text)


def _extract_burst_tokens(news_files, *, min_count=3, top_n=10):
    return _smcore_extract_burst_tokens(news_files, min_count=min_count, top_n=top_n)


def _collect_macro_risk_events(news_files, burst_tokens, *, auto_mode=True):
    return _smcore_collect_macro_risk_events(news_files, burst_tokens, auto_mode=auto_mode)


def _macro_risk_level(macro_risk_summary):
    return _smcore_macro_risk_level(macro_risk_summary)

def _append_log(log_lines, message):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {message}"
    print(line)
    log_lines.append(line)
    if RUN_LOG_FILE is not None:
        try:
            with RUN_LOG_LOCK:
                RUN_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
                with RUN_LOG_FILE.open("a", encoding="utf-8") as f:
                    f.write(line + "\n")
        except Exception:
            pass


def _stage_tag(step_index, stage_name, *, percent=None, total_steps=PIPELINE_TOTAL_STEPS):
    safe_step = max(1, min(int(step_index), int(total_steps)))
    pct = int(percent) if percent is not None else int(round(safe_step * 100 / total_steps))
    pct = max(0, min(100, pct))
    return f"[{pct:>3d}%][{safe_step}/{total_steps} {stage_name}]"




def _run_command_with_live_output(
    log_lines,
    *,
    cmd,
    cwd,
    step_index,
    stage_name,
    idle_timeout_seconds=0,
    kill_grace_seconds=8,
):
    start_percent = int((step_index - 1) * 100 / PIPELINE_TOTAL_STEPS)
    running_percent = max(start_percent, int(step_index * 100 / PIPELINE_TOTAL_STEPS) - 1)
    done_percent = int(step_index * 100 / PIPELINE_TOTAL_STEPS)
    cmd = list(cmd)
    if cmd and "python" in str(cmd[0]).lower() and "-u" not in cmd[1:3]:
        # Force unbuffered Python stdout/stderr so long-running steps show live progress.
        cmd.insert(1, "-u")

    display_cmd = " ".join(str(part) for part in cmd)
    _append_log(log_lines, f"{_stage_tag(step_index, stage_name, percent=start_percent)} start: {display_cmd}")

    started = time.monotonic()
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")

    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    lines = []
    out_queue = queue.Queue()

    def _reader() -> None:
        try:
            if proc.stdout is None:
                return
            for raw in proc.stdout:
                out_queue.put(raw)
        finally:
            out_queue.put(None)

    reader_thread = threading.Thread(target=_reader, name=f"reader-{stage_name}", daemon=True)
    reader_thread.start()

    idle_timeout = max(0, int(float(idle_timeout_seconds or 0)))
    timed_out = False
    deadline = time.monotonic() + idle_timeout if idle_timeout > 0 else None

    while True:
        try:
            raw = out_queue.get(timeout=1.0)
        except queue.Empty:
            raw = ""

        if raw is None:
            if proc.poll() is not None:
                break
            continue

        if raw:
            line = str(raw).rstrip("\r\n")
            if line:
                lines.append(line)
                _append_log(log_lines, f"{_stage_tag(step_index, stage_name, percent=running_percent)} {line}")
                if deadline is not None:
                    deadline = time.monotonic() + idle_timeout

        if deadline is not None and time.monotonic() > deadline and proc.poll() is None:
            timed_out = True
            _append_log(
                log_lines,
                f"{_stage_tag(step_index, stage_name, percent=running_percent)} no output for {idle_timeout}s, terminating...",
            )
            try:
                proc.terminate()
                proc.wait(timeout=max(1, int(kill_grace_seconds)))
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
            break

        if proc.poll() is not None and out_queue.empty():
            break

    returncode = proc.wait()
    if timed_out and returncode == 0:
        returncode = 124
    elapsed = time.monotonic() - started
    status = "OK" if returncode == 0 else f"FAILED({returncode})"
    _append_log(log_lines, f"{_stage_tag(step_index, stage_name, percent=done_percent)} done: {status}, elapsed={elapsed:.1f}s")

    tail = "\n".join(lines[-40:]) if lines else ""
    return returncode, tail


def _run_command_capture(*, cmd, cwd):
    started = time.monotonic()
    cmd = list(cmd)
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")

    completed = subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    elapsed = time.monotonic() - started
    output = completed.stdout or ""
    if completed.stderr:
        output = output + ("\n" if output else "") + completed.stderr
    tail = "\n".join([line for line in output.splitlines() if line][-80:])
    return int(completed.returncode), tail, elapsed


def _find_result_csv(today_yyyymmdd):
    preferred = STOCK_DATA_DIR / f"Stock-Selection-Boll-{today_yyyymmdd}.csv"
    if preferred.exists():
        return preferred

    candidates = sorted(
        STOCK_DATA_DIR.glob("Stock-Selection-Boll-*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _find_theme_result_csv(today_yyyymmdd):
    preferred = STOCK_DATA_DIR / f"Stock-Selection-Ashare-Theme-Turnover-{today_yyyymmdd}.csv"
    if preferred.exists():
        return preferred

    candidates = sorted(
        STOCK_DATA_DIR.glob("Stock-Selection-Ashare-Theme-Turnover-*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _find_relativity_result_csv(today_yyyymmdd):
    preferred = STOCK_DATA_DIR / f"Stock-Selection-Relativity-{today_yyyymmdd}.csv"
    if preferred.exists():
        return preferred

    candidates = sorted(
        STOCK_DATA_DIR.glob("Stock-Selection-Relativity-*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _find_shared_seed_csv(today_yyyymmdd):
    preferred = STOCK_DATA_DIR / f"Stock-Selection-Shared-Seed-{today_yyyymmdd}.csv"
    if preferred.exists():
        return preferred

    candidates = sorted(
        STOCK_DATA_DIR.glob("Stock-Selection-Shared-Seed-*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _find_archived_file_by_name(file_name):
    archive_root = STOCK_DATA_DIR / "archive"
    if not archive_root.exists():
        return None
    candidates = sorted(
        archive_root.rglob(file_name),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _read_rows(csv_path, limit=20):
    rows = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                {
                    "code": (row.get("股票代码") or "").strip(),
                    "name": (row.get("股票名称") or "").strip(),
                }
            )
    preview = rows[:limit]
    return rows, preview


def _cache_table_name(cache_key):
    # 委托 smcore.cache（全项目统一缓存层）。
    return _smcore_cache_table_name(cache_key)


def _read_cache_df(table_name):
    # 委托 smcore.cache。
    return _smcore_read_cache_df(table_name)


def _write_cache_df(table_name, df):
    # 委托 smcore.cache。
    _smcore_write_cache_df(table_name, df)


def _read_theme_rows(csv_path, limit=None):
    rows = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                {
                    "code": (row.get("股票代码") or "").strip(),
                    "name": (row.get("股票名称") or "").strip(),
                    "score": (row.get("综合分") or "").strip(),
                    "theme": (row.get("题材标签") or "").strip(),
                    "turn": (row.get("最新换手率%") or "").strip(),
                }
            )
    if isinstance(limit, int) and limit > 0:
        return rows[:limit]
    return rows


def _find_cctv_stock_pool_csv(today_yyyymmdd):
    preferred = STOCK_DATA_DIR / f"CCTV-Sector-Stock-Pool-{today_yyyymmdd}.csv"
    if preferred.exists():
        return preferred

    # 若当天文件不存在，回退最近一期，避免跨日运行时题材被清空。
    candidates = sorted(
        STOCK_DATA_DIR.glob("CCTV-Sector-Stock-Pool-*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


@lru_cache(maxsize=32)
def _load_cctv_codes_by_date(date_yyyymmdd):
    path = _find_cctv_stock_pool_csv(str(date_yyyymmdd or "").strip())
    if not path or (not path.exists()):
        return frozenset()

    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return frozenset()
    if df.empty:
        return frozenset()

    code_col = ""
    lower_map = {str(c).strip().lower(): str(c) for c in df.columns}
    for key in ["股票代码", "code", "symbol", "证券代码"]:
        col = lower_map.get(key.lower(), "")
        if col:
            code_col = col
            break
    if not code_col:
        return frozenset()

    codes = df[code_col].astype(str).apply(_normalize_code)
    return frozenset(c for c in codes.tolist() if len(c) == 6)


def _filter_theme_rows_with_cctv(theme_rows, *, date_yyyymmdd):
    if not theme_rows:
        return []
    cctv_codes = _load_cctv_codes_by_date(date_yyyymmdd)
    if not cctv_codes:
        return []
    return [item for item in theme_rows if _normalize_code(item.get("code")) in cctv_codes]


def _normalize_code(code):
    # 委托 smcore（全项目唯一实现），保持本文件内调用签名不变。
    return _smcore_format_code(code)


def _to_float(value):
    # 委托 smcore.utils.format。
    return _smcore_to_float(value)


def _normalize_confidence_label(raw_value):
    # 委托 smcore.utils.format。
    return _smcore_normalize_confidence_label(raw_value)


def _format_yi(value):
    # 委托 smcore.utils.format。
    return _smcore_format_yi(value)


def _to_bs_code(code):
    # 委托 smcore（全项目唯一实现）。
    return _smcore_to_bs_code(code)


def _safe_pct(numerator, denominator):
    # 委托 smcore.utils.format。
    return _smcore_safe_pct(numerator, denominator)


def _to_percent_like(value):
    # 委托 smcore.utils.format。
    return _smcore_to_percent_like(value)


def _fmt_pct(value, digits=2, signed=False, na="N/A"):
    # 委托 smcore.utils.format。
    return _smcore_fmt_pct(value, digits=digits, signed=signed, na=na)


def _fmt_num(value, digits=2, na="N/A"):
    # 委托 smcore.utils.format。
    return _smcore_fmt_num(value, digits=digits, na=na)


def _fetch_dividend_yield_ttm(code, end_date_text):
    """返回股息率(TTM, %)，失败则返回 None。"""
    norm = _normalize_code(code)
    if not norm:
        return None

    cache_key = f"stock_data/dividend_yield_ttm_{norm}_{end_date_text}.csv"
    table_name = _cache_table_name(cache_key)
    cached_df = _read_cache_df(table_name)
    if not cached_df.empty:
        cached_value = _to_float(cached_df.iloc[-1].get("dv_ttm"))
        if cached_value is not None:
            return cached_value

    try:
        df = ak.stock_a_lg_indicator(symbol=norm)
    except Exception:
        return None
    if df is None or df.empty:
        return None

    row = df.iloc[-1].to_dict()
    raw_value = None
    for key in ("dv_ttm", "dv_ratio", "dividend_yield", "股息率", "股息率(%)"):
        if key in row:
            raw_value = row.get(key)
            break
    if raw_value is None:
        return None

    pct_value = _to_percent_like(raw_value)
    if pct_value is None:
        return None
    _write_cache_df(table_name, pd.DataFrame([{"dv_ttm": pct_value}]))
    return pct_value


def _filter_rows_by_dividend_yield(rows, *, min_yield_pct, log_lines=None, label=""):
    if not rows:
        return []
    if min_yield_pct is None or min_yield_pct <= 0:
        return rows

    end_date_text = datetime.now().strftime("%Y-%m-%d")
    kept = []
    low_count = 0
    missing_count = 0
    for item in rows:
        code = _normalize_code(item.get("code"))
        if not code:
            continue
        dv_pct = _fetch_dividend_yield_ttm(code, end_date_text)
        if dv_pct is None:
            missing_count += 1
            kept.append(item)
            continue
        if dv_pct < float(min_yield_pct):
            low_count += 1
            continue
        kept.append(item)

    if log_lines is not None:
        label_text = f"{label} " if label else ""
        _append_log(
            log_lines,
            (
                f"股息率过滤: {label_text}min={min_yield_pct:.2f}% "
                f"kept={len(kept)} filtered={low_count} missing={missing_count}"
            ),
        )
    return kept


def _fetch_bs_latest_row(bs_code, end_date_text, lookback_days=40):
    cache_key = f"stock_data/bs_latest_row_{bs_code}_{end_date_text}_adj3.csv"
    table_name = _cache_table_name(cache_key)
    cached_df = _read_cache_df(table_name)
    if not cached_df.empty:
        row = cached_df.iloc[-1].to_dict()
        if row:
            return row

    start_date_text = (datetime.strptime(end_date_text, "%Y-%m-%d") - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rs = bs.query_history_k_data_plus(
        bs_code,
        "date,code,close,turn,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
        start_date=start_date_text,
        end_date=end_date_text,
        frequency="d",
        adjustflag="2",  # 前复权（统一口径）。此前用 "3" 不复权，close 与 Boll 口径不一致。
    )
    if rs.error_code != "0":
        return None

    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    if not data_list or not rs.fields:
        return None
    row = dict(zip(rs.fields, data_list[-1]))
    _write_cache_df(table_name, pd.DataFrame([row]))
    return row


def _fetch_bs_close_series(bs_code, end_date_text, lookback_days=60):
    cache_key = f"stock_data/bs_close_{bs_code}_{end_date_text}_{lookback_days}_adj3.csv"
    table_name = _cache_table_name(cache_key)
    cached_df = _read_cache_df(table_name)
    if not cached_df.empty and "close" in cached_df.columns:
        cached_df = cached_df.copy()
        cached_df["close"] = pd.to_numeric(cached_df["close"], errors="coerce")
        cached_df = cached_df.dropna(subset=["close"])
        if not cached_df.empty:
            return cached_df.reset_index(drop=True)

    start_date_text = (datetime.strptime(end_date_text, "%Y-%m-%d") - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rs = bs.query_history_k_data_plus(
        bs_code,
        "date,close",
        start_date=start_date_text,
        end_date=end_date_text,
        frequency="d",
        adjustflag="2",  # 前复权（统一口径）。此前用 "3" 不复权，close 与 Boll 口径不一致。
    )
    if rs.error_code != "0":
        return pd.DataFrame()

    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    if not data_list or not rs.fields:
        return pd.DataFrame()
    df = pd.DataFrame(data_list, columns=rs.fields)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"]).reset_index(drop=True)
    if not df.empty:
        _write_cache_df(table_name, df)
    return df


def _calc_boll_levels(close_series, *, k=1.645):
    if close_series is None or len(close_series) < 20:
        return {}
    tail = close_series.tail(20)
    ma20 = float(tail.mean())
    std20 = float(tail.std())
    upper = ma20 + k * std20
    lower = ma20 - k * std20
    return {"ma20": ma20, "upper": upper, "lower": lower}


def _build_indicator_levels(rows, *, k=1.645, lookback_days=60):
    # 委托 smcore：前复权 K 线 + 唯一 Boll 实现 + 进程级单例会话。
    # 此前自带 bs.login()/adjustflag="3"/手算 Boll，是命令行侧"不可信"的根因。
    if not rows:
        return {}

    end_date_text = datetime.now().strftime("%Y-%m-%d")
    start_date_text = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    levels = {}
    with _smcore_session() as ok:
        if not ok:
            return {}
        for item in rows:
            code = (item.get("code") or "").strip()
            key = _normalize_code(code)
            if not key:
                continue
            df = _smcore_fetch_daily_k(key, start_date_text, end_date_text, adjust="qfq")
            if df.empty:
                continue
            close_series = pd.to_numeric(df["close"], errors="coerce").dropna()
            if len(close_series) < 20:
                continue
            latest_close = float(close_series.iloc[-1])
            ma10 = float(close_series.tail(10).mean()) if len(close_series) >= 10 else None
            boll_df = _smcore_calc_bollinger(df, window=20, k=k)
            last = boll_df.iloc[-1]
            levels[key] = {
                "close": latest_close,
                "ma10": ma10,
                "ma20": float(last["MA"]) if pd.notna(last.get("MA")) else None,
                "upper": float(last["Upper"]) if pd.notna(last.get("Upper")) else None,
                "lower": float(last["Lower"]) if pd.notna(last.get("Lower")) else None,
            }
    return levels


def _to_ak_index_symbol(index_code):
    # 委托 smcore（全项目唯一实现）。
    return _smcore_to_ak_index(index_code)


def _fetch_index_close_series(index_code, start_date_text, end_date_text):
    return _smcore_fetch_index_close_series(index_code, start_date_text, end_date_text)


def _calc_index_metrics(index_df):
    return _smcore_calc_index_metrics(index_df)


def _env_int_percent(name, default):
    return _smcore_env_int_percent(name, default)


def _normalize_weight_map(weights):
    return _smcore_normalize_weight_map(weights)


def _rebalance_for_signal_availability(weights, *, boll_rows_count, theme_rows_count, has_cctv_hot):
    return _smcore_rebalance_for_signal_availability(weights, boll_rows_count=boll_rows_count, theme_rows_count=theme_rows_count, has_cctv_hot=has_cctv_hot)


def _format_position_units(weight, units=10):
    return _smcore_format_position_units(weight, units=units)


def _build_strategy_allocation(regime, *, boll_rows_count, theme_rows_count, has_cctv_hot, macro_level):
    return _smcore_build_strategy_allocation(regime, boll_rows_count=boll_rows_count, theme_rows_count=theme_rows_count, has_cctv_hot=has_cctv_hot, macro_level=macro_level)


def _build_market_and_strategy_summary(*, boll_rows_count, theme_rows_count, macro_risk_summary, cctv_summary, has_cctv_hot, macro_external_level="low"):
    simple_report = os.getenv("REPORT_SIMPLE", "1").strip() != "0"
    end_date_text = datetime.now().strftime("%Y-%m-%d")
    start_date_text = (datetime.strptime(end_date_text, "%Y-%m-%d") - timedelta(days=100)).strftime("%Y-%m-%d")

    with ThreadPoolExecutor(max_workers=2) as ex:
        fut_sh = ex.submit(_fetch_index_close_series, "sh.000001", start_date_text, end_date_text)
        fut_hs = ex.submit(_fetch_index_close_series, "sh.000300", start_date_text, end_date_text)
        sh_df = fut_sh.result()
        hs300_df = fut_hs.result()

    sh_metrics_df = _calc_index_metrics(sh_df)
    hs300_metrics_df = _calc_index_metrics(hs300_df)

    sh_metrics = sh_metrics_df.iloc[-1].to_dict() if not sh_metrics_df.empty else {}
    hs300_metrics = hs300_metrics_df.iloc[-1].to_dict() if not hs300_metrics_df.empty else {}

    sh_ret_20 = _to_float(sh_metrics.get("ret_20d"))
    sh_ret_5 = _to_float(sh_metrics.get("ret_5d"))
    sh_vol_20 = _to_float(sh_metrics.get("vol_20d"))

    macro_level = _macro_risk_level(macro_risk_summary)
    # 外部市场数据为主信号，CCTV新闻最多提升一级
    level_rank = {"high": 3, "medium": 2, "low": 1}
    ext_rank = level_rank.get(macro_external_level, 1)
    news_rank = level_rank.get(macro_level, 1)
    final_rank = ext_rank
    if news_rank > ext_rank:
        final_rank = min(ext_rank + 1, 3)  # 新闻最多将风险提升1级
    macro_level = {v: k for k, v in level_rank.items()}[final_rank]

    regime = "震荡轮动"
    if sh_ret_20 is not None and sh_ret_5 is not None:
        if sh_ret_20 >= 4.0 and sh_ret_5 >= 0 and (sh_vol_20 is None or sh_vol_20 <= 1.8):
            regime = "趋势上行"
        elif sh_ret_20 <= -4.0 or (sh_ret_5 <= -3.0 and (sh_vol_20 is not None and sh_vol_20 >= 1.8)):
            regime = "下行防御"

    if macro_level == "high":
        regime = "下行防御"

    alloc = _build_strategy_allocation(
        regime,
        boll_rows_count=boll_rows_count,
        theme_rows_count=theme_rows_count,
        has_cctv_hot=has_cctv_hot,
        macro_level=macro_level,
    )

    lines = [
        "\n## 3) 市场状态与策略总览",
        "- 数据源: 回测同口径指数日线（akshare-Eastmoney主源 + Sina回退；上证 sh000001 + 沪深300 sh000300）",
        f"- 宏观风险: {macro_level}",
        f"- 市场判定: {regime}",
        f"- {_suggest_holding_days(regime, macro_level)}",
        f"- 信号补充: Boll命中数={boll_rows_count} 题材候选数={theme_rows_count} CCTV热点={'有' if has_cctv_hot else '无'}",
    ]

    if sh_metrics:
        lines.append(
            "- 上证指标: "
            f"5日{_fmt_pct(sh_metrics.get('ret_5d'), signed=True)} "
            f"20日{_fmt_pct(sh_metrics.get('ret_20d'), signed=True)} "
            f"20日波动{_fmt_pct(sh_metrics.get('vol_20d'))}"
        )
    if hs300_metrics:
        lines.append(
            "- 沪深300指标: "
            f"5日{_fmt_pct(hs300_metrics.get('ret_5d'), signed=True)} "
            f"20日{_fmt_pct(hs300_metrics.get('ret_20d'), signed=True)}"
        )

    explain = []
    if not simple_report:
        explain = [
            "\n## 4) 每日策略如何得到（可复盘）",
            "1. 输入数据:",
            f"- 指数与波动: 上证5日={_fmt_pct(sh_ret_5, signed=True)} 上证20日={_fmt_pct(sh_ret_20, signed=True)} 波动20日={_fmt_pct(sh_vol_20)}",
            f"- 风险与热度: 宏观风险={macro_level} CCTV热点={'有' if has_cctv_hot else '无'}",
            f"- 候选可用性: Boll={boll_rows_count} Theme={theme_rows_count}",
            "2. 市场判定规则:",
            "- 规则A: 若宏观风险=high，则直接切到下行防御。",
            "- 规则B: 否则若上证20日>=4% 且 上证5日>=0 且 波动<=1.8%，判定趋势上行。",
            "- 规则C: 否则若上证20日<=-4% 或（上证5日<=-3% 且 波动>=1.8%），判定下行防御。",
            "- 规则D: 其余情形判定为震荡轮动。",
            f"- 今日命中结果: {regime}",
            "3. 配比生成:",
            f"- 先按市场模板生成基础权重: boll={alloc['base_weights'].get('boll', 0)} theme={alloc['base_weights'].get('theme', 0)} cctv={alloc['base_weights'].get('cctv', 0)} relativity={alloc['base_weights'].get('relativity', 0)} cash={alloc['base_weights'].get('cash', 0)}",
            "- 再按信号可用性回流: Boll=0回流现金；Theme=0回流现金；无CCTV时CCTV权重优先回流Theme，否则回流现金。",
            f"- 最终执行权重: boll={alloc['final_weights'].get('boll', 0)} theme={alloc['final_weights'].get('theme', 0)} cctv={alloc['final_weights'].get('cctv', 0)} relativity={alloc['final_weights'].get('relativity', 0)} cash={alloc['final_weights'].get('cash', 0)}",
        ]

    reco = ["\n## 策略建议"]
    if regime == "趋势上行":
        reco.extend(
            [
                "1. 主策略: 题材轮动 + CCTV热点跟随（提高进攻仓位，快进快出）。",
                "2. 辅策略: Boll信号用于低吸/回踩确认，避免追高单日大阳。",
                "3. 参数建议: THEME_MAX_STOCKS=1200, THEME_TOP_N=30, ENABLE_THEME_STRATEGY=1。",
                "4. 策略原理: 上行期板块扩散更快，强势题材具备更高的资金承接与延续性。",
                "5. 失效信号: 指数放量长上影或热点日内大面积炸板时，降低进攻仓位。",
            ]
        )
    elif regime == "下行防御":
        reco.extend(
            [
                "1. 主策略: 防守优先（降低总仓位，缩短持有周期，控制回撤）。",
                "2. 辅策略: 仅跟踪 Boll超跌反弹 + 高确定性龙头，题材策略降权。",
                "3. 参数建议: ENABLE_THEME_STRATEGY=0 或 THEME_TOP_N=10，严格执行止损。",
                "4. 策略原理: 下行阶段贝塔拖累明显，先控制回撤再等待趋势重新确立。",
                "5. 失效信号: 出现连续放量阳线并突破关键均线，可逐步恢复进攻参数。",
            ]
        )
    else:
        reco.extend(
            [
                "1. 主策略: 震荡轮动（Boll低吸高抛 + 题材择强切换）。",
                "2. 辅策略: 关注相对强弱脚本(Stock-Selection-Relativity.py)做强者恒强过滤。",
                "3. 参数建议: THEME_MAX_STOCKS=600, THEME_TOP_N=20，保持分散持仓。",
                "4. 策略原理: 震荡市中单一主线持续性弱，分批低吸高抛更容易提高胜率。",
                "5. 失效信号: 指数单边突破并伴随成交放大，应切换到趋势模式参数。",
            ]
        )

    reco.extend(
        [
            _suggest_holding_days(regime, macro_level),
            alloc["ratio_line"],
            alloc["unit_line"],
            alloc["priority_line"],
            alloc["adaption_line"],
        ]
    )

    risk_ctrl = []
    if not simple_report:
        risk_ctrl = [
            "\n## 执行与风控清单",
            "1. 单票仓位上限: 建议不超过总资金的10%-15%。",
            "2. 止损纪律: 破位或回撤超过预设阈值时机械止损。",
            "3. 止盈纪律: 分批止盈，避免盈利回吐。",
            "4. 复盘重点: 记录命中来源（Boll/题材/CCTV）与次日延续性。",
        ]

    return "\n".join(lines + explain + reco + risk_ctrl), regime


def _build_fundamental_summary(rows, top_n=20):
    """对命中股票做简单基本面速览，使用 baostock，返回可拼接到消息的文本。"""
    if not rows:
        return ""

    login_res = bs.login()
    if login_res.error_code != "0":
        return "- baostock 登录失败（已跳过）。"

    try:
        lines = []
        end_date_text = datetime.now().strftime("%Y-%m-%d")
        for item in rows[:top_n]:
            code = _normalize_code(item.get("code", ""))
            if not code:
                continue
            display_name = (item.get("name") or "").strip()
            bs_code = _to_bs_code(code)
            if not bs_code:
                lines.append(f"- {code} {display_name} | 指标缺失")
                continue

            row = _fetch_bs_latest_row(bs_code, end_date_text)
            if row is None:
                lines.append(f"- {code} {display_name} | 指标缺失")
                continue

            pe = _to_float(row.get("peTTM"))
            pb = _to_float(row.get("pbMRQ"))
            turnover = _to_float(row.get("turn"))
            pct_chg = _to_float(row.get("pctChg"))

            if pe is None or pe <= 0:
                view = "盈利波动/亏损，谨慎"
            elif pe <= 25 and (pb is not None and pb <= 3):
                view = "估值相对合理"
            elif pe <= 40 and (pb is None or pb <= 5):
                view = "估值中性"
            else:
                view = "估值偏高，注意回撤"

            if turnover is not None and turnover >= 8:
                view = f"{view}；换手较高"

            pe_text = f"{pe:.2f}" if pe is not None else "N/A"
            pb_text = f"{pb:.2f}" if pb is not None else "N/A"
            tr_text = f"{turnover:.2f}%" if turnover is not None else "N/A"
            pct_text = f"{pct_chg:.2f}%" if pct_chg is not None else "N/A"
            lines.append(
                f"- {code} {display_name} | PE:{pe_text} PB:{pb_text} 换手:{tr_text} 涨跌幅:{pct_text} | {view}"
            )
    finally:
        bs.logout()

    if not lines:
        return ""
    return "\n".join(lines)


def _build_message(success, csv_path=None, rows=None, run_output_tail=""):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not success:
        return (
            "# Stocks-Master Daily Run Failed\n"
            f"> Time: {now}\n\n"
            "Stock-Selection-Boll.py failed.\n\n"
            "Recent output:\n"
            f"{run_output_tail or '(no output)'}"
        )

    if csv_path is None:
        return (
            "# Stocks-Master 日报\n"
            f"> 时间: {now}\n\n"
            "## 1) 执行总览\n"
            "- 主流程执行完成，但未找到 Boll 结果 CSV。"
        )

    total = len(rows or [])
    preview_items = (rows or [])[:20]
    preview_levels = _build_indicator_levels(preview_items)
    preview_lines = []
    for item in preview_items:
        level = preview_levels.get(_normalize_code(item.get("code", "")), {})
        stop = level.get("lower") or level.get("ma20")
        take = level.get("upper")
        stop_text = _fmt_num(stop) if stop is not None else "N/A"
        take_text = _fmt_num(take) if take is not None else "N/A"
        risk_text = f" 止损:{stop_text} 止盈:{take_text}" if stop is not None or take is not None else ""
        if item["name"]:
            preview_lines.append(f"- {item['code']} {item['name']}{risk_text}")
        else:
            preview_lines.append(f"- {item['code']}{risk_text}")

    preview_block = "\n".join(preview_lines) if preview_lines else "- (empty)"
    return (
        "# Stocks-Master 日报\n"
        f"> 时间: {now}\n\n"
        "## 1) 执行总览\n"
        f"- Boll候选总数: {total}\n"
        f"- 结果文件: {csv_path}\n"
        "- 口径: 技术面(Boll) + 题材热度 + 相对强弱 + 宏观与CCTV热点。\n\n"
        "## 2) Boll候选明细(前20)\n"
        f"{preview_block}"
    )


def _build_theme_message(
    theme_csv_path=None,
    theme_rows=None,
    *,
    raw_count=0,
    cctv_only=False,
    cctv_count=0,
):
    if theme_csv_path is None:
        return "\n## 5) 题材策略\n- 本次未找到结果文件。"
    picks = len(theme_rows or [])
    if picks == 0:
        return (
            "\n## 5) 题材策略（仅显示命中当天 CCTV 热点）\n"
            f"- 结果文件: {theme_csv_path}\n"
            f"- 原始候选数: {int(raw_count)}\n"
            f"- CCTV匹配数: {int(cctv_count)}\n"
            "- 候选数: 0\n"
            "- 原理: 题材策略通过政策/舆情关键词 + 换手活跃度 + 动量筛选弹性方向。"
        )

    levels = _build_indicator_levels(theme_rows or [])
    lines = []
    for item in (theme_rows or []):
        score = item.get("score") or "N/A"
        turn = item.get("turn") or "N/A"
        theme = (item.get("theme") or "").strip()
        theme_text = theme if theme else "无"
        level = levels.get(_normalize_code(item.get("code", "")), {})
        stop = level.get("ma10") or level.get("ma20")
        take = level.get("upper")
        stop_text = _fmt_num(stop) if stop is not None else "N/A"
        take_text = _fmt_num(take) if take is not None else "N/A"
        risk_text = f" 止损:{stop_text} 止盈:{take_text}" if stop is not None or take is not None else ""
        lines.append(
            f"- {item['code']} {item['name']} | 分数:{score} 换手:{turn}%{risk_text} | 匹配题材:{theme_text}"
        )

    title = "\n## 5) 题材策略（仅显示命中当天 CCTV 热点，全量）\n" if cctv_only else "\n## 5) 题材策略(全量)\n"
    count_lines = [f"- 原始候选数: {int(raw_count)}"]
    if cctv_only:
        count_lines.append(f"- CCTV匹配数: {int(cctv_count)}")
    count_lines.append(f"- 展示数量: {picks}")

    return (
        title
        +
        f"- 结果文件: {theme_csv_path}\n"
        + "\n".join(count_lines)
        + "\n"
        +
        "- 原理: 综合分越高，通常代表题材匹配度更高、资金活跃度更强、短期动量更好。\n"
        "- 风险: 题材轮动切换快，需结合止盈止损，不可单凭分数重仓。\n"
        + "\n".join(lines)
    )


def _read_relativity_rows(csv_path, limit=20, min_down_ratio_pct=None):
    rows = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            entry = {
                "code": (row.get("股票代码") or "").strip(),
                "name": (row.get("股票名称") or "").strip(),
                "up_ratio": (row.get("上涨满足率") or "").strip(),
                "down_ratio": (row.get("抗跌满足率") or "").strip(),
                "overlap_days": (row.get("对齐交易日") or "").strip(),
            }
            if min_down_ratio_pct is not None:
                down_pct = _to_percent_like(entry.get("down_ratio"))
                if down_pct is None or down_pct < float(min_down_ratio_pct):
                    continue
            rows.append(entry)
    return rows[:limit]


def _build_relativity_message(relativity_csv_path=None, relativity_rows=None):
    if relativity_csv_path is None:
        return "\n## 6) 相对强弱策略\n- 本次未找到结果文件。"
    picks = len(relativity_rows or [])
    if picks == 0:
        return f"\n## 6) 相对强弱策略\n- 结果文件: {relativity_csv_path}\n- 候选数: 0"

    levels = _build_indicator_levels((relativity_rows or [])[:20])
    lines = []
    for item in (relativity_rows or [])[:20]:
        up_pct = _to_percent_like(item.get("up_ratio"))
        down_pct = _to_percent_like(item.get("down_ratio"))
        up_text = f"{up_pct:.1f}%" if up_pct is not None else "N/A"
        down_text = f"{down_pct:.1f}%" if down_pct is not None else "N/A"
        overlap = item.get("overlap_days") or "N/A"
        level = levels.get(_normalize_code(item.get("code", "")), {})
        stop = level.get("ma20") or level.get("ma10")
        take = level.get("upper")
        stop_text = _fmt_num(stop) if stop is not None else "N/A"
        take_text = _fmt_num(take) if take is not None else "N/A"
        risk_text = f" 止损:{stop_text} 止盈:{take_text}" if stop is not None or take is not None else ""
        lines.append(
            f"- {item.get('code', '')} {item.get('name', '')} | 上涨满足率:{up_text} 抗跌满足率:{down_text} 对齐交易日:{overlap}{risk_text}"
        )

    return (
        "\n## 6) 相对强弱策略(前20)\n"
        f"- 结果文件: {relativity_csv_path}\n"
        f"- 展示数量: {picks}\n"
        "- 原理: 对比指数涨跌日中的相对表现，优先筛选顺风不弱、逆风抗跌的个股。\n"
        + "\n".join(lines)
    )


def _find_latest_cctv_hot_file(today_yyyymmdd):
    preferred = STOCK_DATA_DIR / f"CCTV-Hot-Sectors-{today_yyyymmdd}.csv"
    if preferred.exists():
        return preferred
    files = sorted(
        STOCK_DATA_DIR.glob("CCTV-Hot-Sectors-*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return files[0] if files else None


def _extract_date_from_filename(path_obj):
    m = re.search(r"(\d{8})", path_obj.stem)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d").date()
    except Exception:
        return None


def _find_latest_cctv_hot_file_with_age():
    files = sorted(
        STOCK_DATA_DIR.glob("CCTV-Hot-Sectors-*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        archive_root = STOCK_DATA_DIR / "archive"
        if archive_root.exists():
            files = sorted(
                archive_root.rglob("CCTV-Hot-Sectors-*.csv"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
    if not files:
        return None, None

    latest = files[0]
    file_date = _extract_date_from_filename(latest)
    if file_date is None:
        age_days = max((datetime.now().date() - datetime.fromtimestamp(latest.stat().st_mtime).date()).days, 0)
        return latest, age_days

    age_days = max((datetime.now().date() - file_date).days, 0)
    return latest, age_days


def _iter_all_cctv_hot_files():
    files = list(STOCK_DATA_DIR.glob("CCTV-Hot-Sectors-*.csv"))
    archive_root = STOCK_DATA_DIR / "archive"
    if archive_root.exists():
        files.extend(list(archive_root.rglob("CCTV-Hot-Sectors-*.csv")))
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)


def _collect_cctv_files_in_window(window_days=3):
    today = datetime.now().date()
    candidates = []
    recent_candidates = []
    max_lookback_days = max(int(window_days), 1) + 7
    min_samples = max(3, min(int(window_days), 5))
    for p in _iter_all_cctv_hot_files():
        d = _extract_date_from_filename(p)
        if d is None:
            continue
        age = (today - d).days
        if 0 <= age < max(int(window_days), 1):
            recent_candidates.append((p, d))
        elif 0 <= age < max_lookback_days:
            candidates.append((p, d))

    if len(recent_candidates) >= min_samples:
        candidates = recent_candidates
    else:
        candidates = recent_candidates + candidates

    # 若窗口内无数据，回退到最近一期，避免日报缺失该模块。
    if not candidates:
        latest = _find_latest_cctv_hot_file(today.strftime("%Y%m%d"))
        if latest is not None:
            d = _extract_date_from_filename(latest) or today
            candidates.append((latest, d))

    # 统计时按日期升序，方便计算区间变化。
    return sorted(candidates, key=lambda x: x[1])


def _build_cctv_period_summary(window_days=3, top_n=0):
    period_files = _collect_cctv_files_in_window(window_days=window_days)
    if not period_files:
        return ""

    agg = {}
    for p, d in period_files:
        try:
            with p.open("r", encoding="utf-8-sig", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    sec = (row.get("板块") or "").strip()
                    if _is_cctv_noise_sector(sec):
                        continue
                    heat = _to_float(row.get("热度分"))
                    if not sec or heat is None:
                        continue

                    item = agg.setdefault(
                        sec,
                        {
                            "sum_heat": 0.0,
                            "count": 0,
                            "first_date": d,
                            "first_heat": heat,
                            "last_date": d,
                            "last_heat": heat,
                        },
                    )
                    item["sum_heat"] += heat
                    item["count"] += 1
                    if d < item["first_date"]:
                        item["first_date"] = d
                        item["first_heat"] = heat
                    if d > item["last_date"]:
                        item["last_date"] = d
                        item["last_heat"] = heat
        except Exception:
            continue

    if not agg:
        return ""

    rows = []
    for sec, item in agg.items():
        avg_heat = item["sum_heat"] / max(item["count"], 1)
        delta = item["last_heat"] - item["first_heat"] if item["count"] >= 2 else None
        rows.append(
            {
                "sec": sec,
                "avg_heat": avg_heat,
                "delta": delta,
                "count": item["count"],
            }
        )

    rows.sort(key=lambda x: x["avg_heat"], reverse=True)
    show_rows = rows[:top_n] if isinstance(top_n, int) and top_n > 0 else rows
    sample_days = len(period_files)
    title_suffix = f"Top{top_n}" if isinstance(top_n, int) and top_n > 0 else "全覆盖"

    lines = [
        f"\nCCTV 热门板块 {title_suffix}（近{max(int(window_days), 1)}日优先，样本不足自动回补前几日）:",
        f"- 样本天数: {sample_days}",
        "- 口径: 按板块热度分做区间均值排序，并给出区间净变化",
    ]
    for idx, row in enumerate(show_rows, start=1):
        delta = row["delta"]
        if delta is None:
            delta_text = "--"
            trend = "样本不足"
        else:
            delta_text = f"{delta:+.2f}"
            if delta > 0:
                trend = "升温"
            elif delta < 0:
                trend = "降温"
            else:
                trend = "持平"
        lines.append(
            f"{idx}. {row['sec']} | 区间均值:{row['avg_heat']:.2f} | 区间变化:{delta_text} ({trend}) | 上榜次数:{row['count']}"
        )
    return "\n".join(lines)


def _find_latest_news_file(today_yyyymmdd):
    preferred = STOCK_DATA_DIR / f"{today_yyyymmdd}_news.csv"
    if preferred.exists():
        return preferred
    files = list(STOCK_DATA_DIR.glob("*_news.csv"))
    archive_root = STOCK_DATA_DIR / "archive"
    if archive_root.exists():
        files.extend(list(archive_root.rglob("*_news.csv")))
    files = sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _find_news_file_by_date(date_str):
    preferred = STOCK_DATA_DIR / f"{date_str}_news.csv"
    if preferred.exists():
        return preferred
    archive_root = STOCK_DATA_DIR / "archive"
    if archive_root.exists():
        matches = list(archive_root.rglob(f"{date_str}_news.csv"))
        if matches:
            matches.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            return matches[0]
    return None


def _ensure_news_file(date_str, *, auto_fetch=True):
    preferred = STOCK_DATA_DIR / f"{date_str}_news.csv"
    existing = _find_news_file_by_date(date_str)
    if existing is not None:
        return existing
    if not auto_fetch:
        return None
    try:
        df = ak.news_cctv(date=date_str)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    try:
        df.to_csv(preferred, index=False, encoding="utf-8-sig")
    except Exception:
        return None
    return preferred


def _backfill_news_files(today_yyyymmdd, window_days, *, auto_fetch=True, log_lines=None):
    if not auto_fetch:
        return
    window_days = max(int(window_days or 0), 1)
    for i in range(window_days):
        date_str = (datetime.strptime(today_yyyymmdd, "%Y%m%d") - timedelta(days=i)).strftime("%Y%m%d")
        if _find_news_file_by_date(date_str) is not None:
            continue
        try:
            df = ak.news_cctv(date=date_str)
        except Exception:
            if log_lines is not None:
                _append_log(log_lines, f"[macro-news] backfill failed: {date_str}")
            continue
        if df is None or df.empty:
            if log_lines is not None:
                _append_log(log_lines, f"[macro-news] backfill empty: {date_str}")
            continue
        preferred = STOCK_DATA_DIR / f"{date_str}_news.csv"
        try:
            df.to_csv(preferred, index=False, encoding="utf-8-sig")
        except Exception:
            if log_lines is not None:
                _append_log(log_lines, f"[macro-news] backfill save failed: {date_str}")
            continue
        if log_lines is not None:
            _append_log(log_lines, f"[macro-news] backfill saved: {date_str}")


def _build_macro_risk_summary(today_yyyymmdd, window_days=3, top_n=0, *, auto_fetch=True):
    window_days = max(int(window_days or 0), 1)
    news_files = []
    for i in range(window_days):
        date_str = (datetime.strptime(today_yyyymmdd, "%Y%m%d") - timedelta(days=i)).strftime("%Y%m%d")
        f = _ensure_news_file(date_str, auto_fetch=auto_fetch)
        if f and f.exists():
            news_files.append((f, date_str))

    if not news_files:
        return "- 新闻源: 未找到可用新闻文件\n- 解读: 本次跳过宏观风险打分"

    try:
        burst_min = max(int(os.getenv("MACRO_RISK_BURST_MIN_COUNT", "3").strip() or "3"), 1)
    except Exception:
        burst_min = 3
    try:
        burst_top_n = max(int(os.getenv("MACRO_RISK_BURST_TOP_N", "10").strip() or "10"), 1)
    except Exception:
        burst_top_n = 10
    burst_tokens = _extract_burst_tokens(news_files, min_count=burst_min, top_n=burst_top_n)
    if not burst_tokens:
        burst_tokens = set(MACRO_RISK_SIGNAL_FRAGMENTS)

    events = _collect_macro_risk_events(
        news_files,
        burst_tokens,
        auto_mode=True,
    )

    if not events:
        return (
            f"- 新闻源: 近{window_days}天({len(news_files)}个文件)\n"
            "- 风险事件: 未命中高/中风险关键词\n"
            "- 解读: 当前宏观风险信号偏平稳"
        )

    dedup_events = []
    seen_keys = set()
    for item in events:
        key = (item.get("date", ""), item.get("title", ""))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        dedup_events.append(item)

    count_mode = os.getenv("MACRO_RISK_COUNT_MODE", "entry").strip().lower() or "entry"
    if count_mode == "day":
        count_mode = "entry"
    if count_mode not in {"avg", "entry"}:
        count_mode = "entry"

    if count_mode == "avg":
        day_scores = {}
        for item in dedup_events:
            date_key = item.get("date", "")
            score = item.get("risk_score", 0)
            if not date_key:
                continue
            acc = day_scores.get(date_key)
            if acc is None:
                day_scores[date_key] = [score, 1]
            else:
                acc[0] += score
                acc[1] += 1
        day_avg = {k: (v[0] / max(v[1], 1)) for k, v in day_scores.items()}
        high_hits = sum(1 for score in day_avg.values() if score >= 4)
        medium_hits = sum(1 for score in day_avg.values() if 2 <= score < 4)
    else:
        high_hits = sum(1 for item in dedup_events if item.get("risk_score", 0) >= 4)
        medium_hits = sum(1 for item in dedup_events if 2 <= item.get("risk_score", 0) < 4)
    try:
        high_threshold = max(int(os.getenv("MACRO_RISK_HIGH_HITS", "2").strip() or "2"), 1)
    except Exception:
        high_threshold = 2
    try:
        medium_threshold = max(int(os.getenv("MACRO_RISK_MEDIUM_HITS", "2").strip() or "2"), 1)
    except Exception:
        medium_threshold = 2

    events = sorted(dedup_events, key=lambda x: (x.get("date", ""), x["risk_score"]), reverse=True)
    if isinstance(top_n, int) and top_n > 0:
        events = events[:top_n]

    lines = [
        f"- 新闻源: 近{window_days}天({len(news_files)}个文件)",
        f"- 命中统计({count_mode}): 高={high_hits} 中={medium_hits} | 判定阈值: 高>={high_threshold}，中>={medium_threshold}",
        "- 解读: 仅用于交易关注方向，不构成投资建议",
    ]
    if count_mode == "avg" and day_avg:
        day_items = sorted(day_avg.items())
        day_line = ", ".join(f"{d}:{score:.1f}" for d, score in day_items)
        lines.append(f"- 日均风险分: {day_line}")
    for idx, item in enumerate(events, start=1):
        score = item["risk_score"]
        level = "高" if score >= 4 else "中" if score >= 2 else "低"
        tags = "/".join(item["tags"]) if item["tags"] else "综合"
        date_tag = item.get("date", "")
        prefix = f"{date_tag} " if date_tag else ""
        lines.append(f"{idx}. [{level}] {prefix}{item['title']} | 影响链条: {tags}")

    return "\n".join(lines)


def _suggest_holding_days(regime, macro_level):
    if regime == "下行防御" or macro_level == "high":
        return "持有周期建议: 1-2个交易日，T+1快进快出，触发止损立即减仓。"
    if regime == "趋势上行":
        return "持有周期建议: 3-7个交易日，强势题材2-4日滚动，提高资金周转。"
    return "持有周期建议: 2-4个交易日，冲高分批止盈，回撤及时落袋。"


def _read_cctv_top_summary(csv_path, top_n=5):
    if csv_path is None or not csv_path.exists():
        return ""
    try:
        rows = []
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if isinstance(top_n, int) and top_n > 0 and i >= top_n:
                    break
                sec = (row.get("板块") or "").strip()
                heat = (row.get("热度分") or "").strip()
                change = (row.get("较上一期热度变化") or "N/A").strip()
                if sec and not _is_cctv_noise_sector(sec):
                    rows.append((sec, heat or "N/A", change or "N/A"))
        if not rows:
            return ""

        title = "CCTV 热门板块 Top5:" if isinstance(top_n, int) and top_n > 0 else "CCTV 热门板块 全覆盖:"
        lines = [f"\n{title}"]
        for idx, (sec, heat, change) in enumerate(rows, start=1):
            heat_text = _fmt_num(heat, digits=2, na="N/A")
            trend = "升温"
            change_text = str(change)
            try:
                c = float(change)
                change_text = f"{c:+.2f}"
                if c < 0:
                    trend = "降温"
                elif c == 0:
                    trend = "持平"
            except Exception:
                change_up = str(change).upper()
                if change_up in {"NEW", "N/A", "NA", ""}:
                    trend = "首期样本"
                    change_text = "--"
                else:
                    trend = "变化待定"
            lines.append(f"{idx}. {sec} | 热度:{heat_text} | 变化:{change_text} ({trend})")
        return "\n".join(lines)
    except Exception:
        return ""


def send_email(subject, content, csv_path, log_lines, extra_attachment_paths=None):
    # 委托 smcore.notify（全项目唯一推送实现），保持本文件内签名不变。
    from smcore.notify import send_email as _smcore_email
    return _smcore_email(subject, content, csv_path=csv_path, log_lines=log_lines, extra_attachment_paths=extra_attachment_paths)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run daily Boll selection and send notifications.",
    )
    parser.add_argument(
        "--test-notify",
        action="store_true",
        help="Only send a test notification without running stock selection.",
    )
    parser.add_argument(
        "--test-email-only",
        action="store_true",
        help="Only test email channel without running stock selection.",
    )
    parser.add_argument(
        "--subject",
        default="",
        help="Custom subject for test email mode.",
    )
    parser.add_argument(
        "--fast-mode",
        action="store_true",
        help="Use faster defaults for daily automation (mainly theme scan size).",
    )
    return parser.parse_args()


def _build_test_message():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return (
        "# Stocks-Master Notify Test\n"
        f"> Time: {now}\n\n"
        "This is a test message from auto_notify_boll.py.\n"
        "If you receive this, SMTP/Webhook config is working."
    )


def _run_data_cleanup(log_lines):
    if not CLEANUP_SCRIPT_PATH.exists():
        _append_log(log_lines, f"Cleanup script not found: {CLEANUP_SCRIPT_PATH}")
        return

    keep_days = os.getenv("CLEANUP_KEEP_DAYS", "30").strip() or "30"
    log_keep_days = os.getenv("CLEANUP_LOG_KEEP_DAYS", keep_days).strip() or keep_days
    plots_keep_days = os.getenv("CLEANUP_PLOTS_KEEP_DAYS", keep_days).strip() or keep_days
    dry_run = os.getenv("CLEANUP_DRY_RUN", "0").strip() == "1"

    cmd = [
        sys.executable,
        str(CLEANUP_SCRIPT_PATH),
        "--keep-days",
        keep_days,
        "--log-keep-days",
        log_keep_days,
        "--plots-keep-days",
        plots_keep_days,
    ]
    if dry_run:
        cmd.append("--dry-run")

    returncode, tail = _run_command_with_live_output(
        log_lines,
        cmd=cmd,
        cwd=ROOT_DIR,
        step_index=6,
        stage_name="cleanup",
    )
    if returncode != 0 and tail:
        _append_log(log_lines, "--- Cleanup output tail ---")
        for line in tail.splitlines():
            log_lines.append(line)


def _run_data_archive(log_lines):
    if not ARCHIVE_SCRIPT_PATH.exists():
        _append_log(log_lines, f"Archive script not found: {ARCHIVE_SCRIPT_PATH}")
        return

    keep_root_days = os.getenv("ARCHIVE_KEEP_ROOT_DAYS", "7").strip() or "7"
    archive_keep_days = os.getenv("ARCHIVE_KEEP_DAYS", "365").strip() or "365"
    dry_run = os.getenv("ARCHIVE_DRY_RUN", "0").strip() == "1"
    archive_all_root_dated = os.getenv("ARCHIVE_ALL_ROOT_DATED", "1").strip() != "0"

    cmd = [
        sys.executable,
        str(ARCHIVE_SCRIPT_PATH),
        "--keep-root-days",
        keep_root_days,
        "--archive-keep-days",
        archive_keep_days,
    ]
    if archive_all_root_dated:
        cmd.append("--archive-all-root-dated")
    if dry_run:
        cmd.append("--dry-run")

    mode_text = "all-root-dated" if archive_all_root_dated else "recent-only"
    _append_log(log_lines, f"{_stage_tag(5, 'archive', percent=58)} mode={mode_text}")

    returncode, tail = _run_command_with_live_output(
        log_lines,
        cmd=cmd,
        cwd=ROOT_DIR,
        step_index=5,
        stage_name="archive",
    )
    if returncode != 0 and tail:
        _append_log(log_lines, "--- Archive output tail ---")
        for line in tail.splitlines():
            log_lines.append(line)


def _run_data_compress(log_lines):
    if not COMPRESS_SCRIPT_PATH.exists():
        _append_log(log_lines, f"Compress script not found: {COMPRESS_SCRIPT_PATH}")
        return

    auto_logs_keep_days = os.getenv("COMPRESS_AUTO_LOGS_KEEP_DAYS", "30").strip() or "30"
    plots_keep_days = os.getenv("COMPRESS_PLOTS_KEEP_DAYS", "30").strip() or "30"
    ui_uploads_keep_days = os.getenv("COMPRESS_UI_UPLOADS_KEEP_DAYS", "30").strip() or "30"
    checkpoints_keep_days = os.getenv("COMPRESS_CHECKPOINTS_KEEP_DAYS", "180").strip() or "180"
    dry_run = os.getenv("COMPRESS_DRY_RUN", "0").strip() == "1"

    cmd = [
        sys.executable,
        str(COMPRESS_SCRIPT_PATH),
        "--auto-logs-keep-days",
        auto_logs_keep_days,
        "--plots-keep-days",
        plots_keep_days,
        "--ui-uploads-keep-days",
        ui_uploads_keep_days,
        "--checkpoints-keep-days",
        checkpoints_keep_days,
    ]
    if dry_run:
        cmd.append("--dry-run")

    _append_log(log_lines, f"{_stage_tag(8, 'compress', percent=74)} start")

    returncode, tail = _run_command_with_live_output(
        log_lines,
        cmd=cmd,
        cwd=ROOT_DIR,
        step_index=8,
        stage_name="compress",
    )
    if returncode != 0 and tail:
        _append_log(log_lines, "--- Compress output tail ---")
        for line in tail.splitlines():
            log_lines.append(line)


def main():
    global RUN_LOG_FILE
    args = parse_args()
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    RUN_LOG_FILE = LOG_DIR / f"boll_auto_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    try:
        RUN_LOG_FILE.write_text("", encoding="utf-8")
    except Exception:
        RUN_LOG_FILE = None
    log_lines = []
    fast_mode = args.fast_mode or os.getenv("FAST_MODE", "0").strip() == "1"

    _append_log(log_lines, f"Python: {sys.executable}")
    if fast_mode:
        _append_log(log_lines, "Fast mode enabled.")

    if args.test_notify or args.test_email_only:
        _append_log(log_lines, "Test mode enabled. Stock selection run is skipped.")
        msg = _build_test_message()

        pushed = False

        subject = args.subject.strip() or "Stocks-Master Notify Test"
        pushed = send_email(subject, msg, None, log_lines) or pushed

        if not pushed:
            _append_log(log_lines, "No push channel configured/succeeded in test mode.")

        if RUN_LOG_FILE is not None:
            RUN_LOG_FILE.write_text("\n".join(log_lines), encoding="utf-8")
            _append_log(log_lines, f"Log saved: {RUN_LOG_FILE}")
        return 0 if pushed else 1

    _append_log(log_lines, "[  0%] Pipeline started (8 steps): 1=boll, 2=cctv, 3=macro-news, 4=theme+relativity, 5=archive, 6=cleanup, 7=notify, 8=compress")
    main_returncode, output_tail = _run_command_with_live_output(
        log_lines,
        cmd=[sys.executable, str(SCRIPT_PATH)],
        cwd=ROOT_DIR,
        step_index=1,
        stage_name="boll",
    )

    success = main_returncode == 0

    enable_cctv = os.getenv("ENABLE_CCTV_STRATEGY", "1").strip() != "0"
    cctv_summary = ""
    if enable_cctv:
        cctv_cmd = [sys.executable, str(CCTV_SCRIPT_PATH), "--top-n", "5", "--emerging-top-n", "20"]
        cctv_disable_extra_news = os.getenv("CCTV_DISABLE_EXTRA_NEWS", "0").strip() == "1"
        cctv_extra_sources = os.getenv("CCTV_EXTRA_NEWS_SOURCES", "cls,sina").strip() or "cls,sina"
        cctv_extra_limit = os.getenv("CCTV_EXTRA_NEWS_LIMIT", "120").strip() or "120"
        cctv_extra_timeout = os.getenv("CCTV_EXTRA_NEWS_TIMEOUT", "8").strip() or "8"

        if cctv_disable_extra_news:
            cctv_cmd.append("--disable-extra-news")
        else:
            cctv_cmd.extend([
                "--extra-news-sources",
                cctv_extra_sources,
                "--extra-news-limit",
                cctv_extra_limit,
                "--extra-news-timeout",
                cctv_extra_timeout,
            ])
            _append_log(
                log_lines,
                (
                    f"{_stage_tag(2, 'cctv', percent=15)} extra-news enabled "
                    f"(sources={cctv_extra_sources}, limit={cctv_extra_limit}, timeout={cctv_extra_timeout}s)"
                ),
            )

        cctv_returncode, cctv_tail = _run_command_with_live_output(
            log_lines,
            cmd=cctv_cmd,
            cwd=ROOT_DIR,
            step_index=2,
            stage_name="cctv",
        )
        if cctv_returncode != 0 and cctv_tail:
            _append_log(log_lines, "--- CCTV output tail ---")
            for line in cctv_tail.splitlines():
                log_lines.append(line)

        cctv_stats_days_raw = os.getenv("CCTV_STATS_DAYS", "3").strip() or "3"
        try:
            cctv_stats_days = max(int(cctv_stats_days_raw), 1)
        except Exception:
            cctv_stats_days = 3
        _append_log(log_lines, f"{_stage_tag(2, 'cctv')} stats window={cctv_stats_days}d")
        cctv_summary = _build_cctv_period_summary(window_days=cctv_stats_days, top_n=0)

        if not cctv_summary:
            cctv_file = _find_latest_cctv_hot_file(datetime.now().strftime("%Y%m%d"))
            cctv_summary = _read_cctv_top_summary(cctv_file, top_n=0)
    else:
        _append_log(log_lines, f"{_stage_tag(2, 'cctv')} skipped by ENABLE_CCTV_STRATEGY=0")

    _append_log(log_lines, f"{_stage_tag(3, 'macro-news', percent=29)} collecting risk summary")
    macro_window_days_raw = os.getenv("MACRO_RISK_WINDOW_DAYS", "3").strip() or "3"
    try:
        macro_window_days = max(int(macro_window_days_raw), 1)
    except Exception:
        macro_window_days = 3
    macro_auto_fetch = os.getenv("MACRO_RISK_AUTO_FETCH_NEWS", "1").strip() != "0"
    today_yyyymmdd = datetime.now().strftime("%Y%m%d")
    _backfill_news_files(today_yyyymmdd, macro_window_days, auto_fetch=macro_auto_fetch, log_lines=log_lines)
    macro_risk_summary = _build_macro_risk_summary(
        today_yyyymmdd,
        window_days=macro_window_days,
        top_n=0,
        auto_fetch=macro_auto_fetch,
    )
    _append_log(log_lines, f"{_stage_tag(3, 'macro-news')} done")

    # --- 新增：插入宏观新闻多日趋势 ---
    macro_news_trend = _build_macro_news_trend_summary(window_days=macro_window_days, top_n=0, auto_fetch=macro_auto_fetch)
    _append_log(log_lines, macro_news_trend)

    # --- 宏观外部数据：美股/汇率/期货 ---
    macro_external_summary = ""
    macro_external_level = "low"
    try:
        macro_external_summary, macro_external_level = _build_macro_external_summary()
        _append_log(log_lines, f"{_stage_tag(3, 'macro-external')} done, level={macro_external_level}")
    except Exception as exc:
        _append_log(log_lines, f"{_stage_tag(3, 'macro-external')} failed: {exc}")

    # --- 经济日历风险 ---
    macro_economic_summary = ""
    macro_economic_level = "low"
    try:
        macro_economic_summary, macro_economic_level = _fetch_economic_calendar_risk(window_days=macro_window_days)
        if macro_economic_summary:
            _append_log(log_lines, f"{_stage_tag(3, 'macro-economic')} done, level={macro_economic_level}")
    except Exception as exc:
        _append_log(log_lines, f"{_stage_tag(3, 'macro-economic')} failed: {exc}")

    # 外部+经济风险取最高
    level_rank = {"high": 3, "medium": 2, "low": 1}
    combined_ext_rank = max(level_rank.get(macro_external_level, 1), level_rank.get(macro_economic_level, 1))
    macro_external_level = {v: k for k, v in level_rank.items()}[combined_ext_rank]

    min_price_text = os.getenv("MIN_STOCK_PRICE", "5").strip() or "5"
    max_price_text = os.getenv("MAX_STOCK_PRICE", "30").strip() or "30"
    min_dividend_yield_pct = _to_float(os.getenv("MIN_DIVIDEND_YIELD_PCT", "2").strip() or "2")
    if min_dividend_yield_pct is None:
        min_dividend_yield_pct = 0.0
    _append_log(log_lines, f"股息率下限: {min_dividend_yield_pct:.2f}% (MIN_DIVIDEND_YIELD_PCT)")

    theme_csv_path = None
    theme_rows = []
    theme_success = False
    theme_raw_count = 0
    theme_cctv_count = 0
    theme_cctv_only = os.getenv("THEME_CCTV_ONLY", "1").strip() != "0"
    enable_theme = os.getenv("ENABLE_THEME_STRATEGY", "1").strip() != "0"
    theme_cmd = []
    if enable_theme:
        default_theme_min_latest_turn = "0.8"
        default_theme_min_avg_turn5 = "0.6"
        default_theme_min_latest_amount = "120000000"
        default_theme_min_latest_price = min_price_text
        default_theme_max_latest_price = max_price_text
        default_theme_max_stocks = "600" if fast_mode else "1200"
        default_theme_top_n = "20" if fast_mode else "30"
        default_theme_workers = "4"

        theme_min_latest_turn = os.getenv("THEME_MIN_LATEST_TURN", default_theme_min_latest_turn).strip() or default_theme_min_latest_turn
        theme_min_avg_turn5 = os.getenv("THEME_MIN_AVG_TURN5", default_theme_min_avg_turn5).strip() or default_theme_min_avg_turn5
        theme_min_latest_amount = os.getenv("THEME_MIN_LATEST_AMOUNT", default_theme_min_latest_amount).strip() or default_theme_min_latest_amount
        theme_min_latest_price = os.getenv("THEME_MIN_LATEST_PRICE", default_theme_min_latest_price).strip() or default_theme_min_latest_price
        theme_max_latest_price = os.getenv("THEME_MAX_LATEST_PRICE", default_theme_max_latest_price).strip() or default_theme_max_latest_price
        theme_max_stocks = os.getenv("THEME_MAX_STOCKS", default_theme_max_stocks).strip() or default_theme_max_stocks
        theme_top_n = os.getenv("THEME_TOP_N", default_theme_top_n).strip() or default_theme_top_n
        theme_max_workers = os.getenv("THEME_MAX_WORKERS", default_theme_workers).strip() or default_theme_workers
        bs_timeout_seconds = os.getenv("BS_REQUEST_TIMEOUT_SECONDS", "15").strip() or "15"
        bs_request_interval_seconds = os.getenv("BS_REQUEST_INTERVAL_SECONDS", "0.05").strip() or "0.05"
        bs_max_retries = os.getenv("BS_MAX_RETRIES", "2").strip() or "2"

        _append_log(
            log_lines,
            f"{_stage_tag(4, 'theme', percent=43)} params: max_stocks={theme_max_stocks}, top_n={theme_top_n}, workers={theme_max_workers}, min_latest_turn={theme_min_latest_turn}, min_avg_turn5={theme_min_avg_turn5}, min_latest_amount={theme_min_latest_amount}, min_latest_price={theme_min_latest_price}, max_latest_price={theme_max_latest_price}, bs_timeout={bs_timeout_seconds}s, bs_interval={bs_request_interval_seconds}s, bs_retries={bs_max_retries}",
        )
        theme_cmd = [
            sys.executable,
            str(THEME_SCRIPT_PATH),
            "--top-n",
            str(theme_top_n),
            "--max-stocks",
            str(theme_max_stocks),
            "--max-workers",
            str(theme_max_workers),
            "--min-latest-turn",
            str(theme_min_latest_turn),
            "--min-avg-turn5",
            str(theme_min_avg_turn5),
            "--min-latest-amount",
            str(theme_min_latest_amount),
            "--min-latest-price",
            str(theme_min_latest_price),
            "--max-latest-price",
            str(theme_max_latest_price),
            "--bs-timeout-seconds",
            str(bs_timeout_seconds),
            "--bs-request-interval-seconds",
            str(bs_request_interval_seconds),
            "--bs-max-retries",
            str(bs_max_retries),
        ]
    else:
        _append_log(log_lines, f"{_stage_tag(4, 'theme')} skipped by ENABLE_THEME_STRATEGY=0")

    relativity_csv_path = None
    relativity_rows = []
    relativity_success = False
    relativity_min_down_ratio_pct = os.getenv("RELATIVITY_MIN_DOWN_RATIO_PCT", "70").strip() or "70"
    enable_relativity = os.getenv("ENABLE_RELATIVITY_STRATEGY", "1").strip() != "0"
    relativity_cmd = []
    if enable_relativity:
        relativity_cmd = [sys.executable, str(RELATIVITY_SCRIPT_PATH)]
        default_relativity_workers = "4"
        relativity_max_workers = os.getenv("RELATIVITY_MAX_WORKERS", default_relativity_workers).strip() or default_relativity_workers
        relativity_holder_max_workers = os.getenv("RELATIVITY_HOLDER_MAX_WORKERS", relativity_max_workers).strip() or relativity_max_workers
        relativity_resume = os.getenv("RELATIVITY_RESUME", "0").strip() == "1"
        relativity_sleep_seconds = os.getenv("RELATIVITY_SLEEP_SECONDS", "0").strip() or "0"
        relativity_disable_rs = os.getenv("RELATIVITY_DISABLE_RS", "0").strip() == "1"
        relativity_use_seed = os.getenv("RELATIVITY_USE_SEED", "1").strip() != "0"
        relativity_min_price = os.getenv("RELATIVITY_MIN_PRICE", min_price_text).strip() or min_price_text
        relativity_max_price = os.getenv("RELATIVITY_MAX_PRICE", max_price_text).strip() or max_price_text
        bs_timeout_seconds = os.getenv("BS_REQUEST_TIMEOUT_SECONDS", "15").strip() or "15"
        bs_request_interval_seconds = os.getenv("BS_REQUEST_INTERVAL_SECONDS", "0.05").strip() or "0.05"
        bs_max_retries = os.getenv("BS_MAX_RETRIES", "2").strip() or "2"

        relativity_cmd.extend([
            "--max-workers",
            str(relativity_max_workers),
            "--holder-max-workers",
            str(relativity_holder_max_workers),
            "--sleep-seconds",
            str(relativity_sleep_seconds),
            "--price-lower-limit",
            str(relativity_min_price),
            "--price-upper-limit",
            str(relativity_max_price),
            "--min-down-ratio",
            str(relativity_min_down_ratio_pct),
            "--bs-timeout-seconds",
            str(bs_timeout_seconds),
            "--bs-request-interval-seconds",
            str(bs_request_interval_seconds),
            "--bs-max-retries",
            str(bs_max_retries),
        ])
        if relativity_resume:
            relativity_cmd.append("--resume")
        if relativity_disable_rs:
            relativity_cmd.append("--disable-rs")

        shared_seed_csv = _find_shared_seed_csv(today_yyyymmdd)
        boll_seed_csv = STOCK_DATA_DIR / f"Stock-Selection-Boll-{today_yyyymmdd}.csv"
        if relativity_use_seed and shared_seed_csv and shared_seed_csv.exists():
            relativity_cmd.extend(["--seed-csv", str(shared_seed_csv)])
            _append_log(log_lines, f"{_stage_tag(4, 'relativity', percent=46)} seed from shared preselection csv: {shared_seed_csv}")
        elif relativity_use_seed and boll_seed_csv.exists():
            relativity_cmd.extend(["--seed-csv", str(boll_seed_csv)])
            _append_log(log_lines, f"{_stage_tag(4, 'relativity', percent=46)} seed from boll csv: {boll_seed_csv}")
        elif shared_seed_csv and shared_seed_csv.exists():
            _append_log(log_lines, f"{_stage_tag(4, 'relativity', percent=46)} seed disabled; using full relativity output")

        _append_log(
            log_lines,
            f"{_stage_tag(4, 'relativity', percent=47)} params: workers={relativity_max_workers}, holder_workers={relativity_holder_max_workers}, resume={int(relativity_resume)}, sleep={relativity_sleep_seconds}, disable_rs={int(relativity_disable_rs)}, use_seed={int(relativity_use_seed)}, min_price={relativity_min_price}, max_price={relativity_max_price}, min_down_ratio_pct={relativity_min_down_ratio_pct}, bs_timeout={bs_timeout_seconds}s, bs_interval={bs_request_interval_seconds}s, bs_retries={bs_max_retries}",
        )
    else:
        _append_log(log_lines, f"{_stage_tag(4, 'relativity')} skipped by ENABLE_RELATIVITY_STRATEGY=0")

    stage4_jobs = {}
    if enable_theme and theme_cmd:
        stage4_jobs["theme"] = theme_cmd
    if enable_relativity and relativity_cmd:
        stage4_jobs["relativity"] = relativity_cmd

    if stage4_jobs:
        stage4_heartbeat_seconds = max(3, int(float(os.getenv("STAGE4_HEARTBEAT_SECONDS", "5").strip() or "5")))
        theme_idle_timeout_seconds = max(0, int(float(os.getenv("THEME_IDLE_TIMEOUT_SECONDS", "120").strip() or "120")))
        relativity_idle_timeout_seconds = max(0, int(float(os.getenv("RELATIVITY_IDLE_TIMEOUT_SECONDS", "120").strip() or "120")))

        _append_log(
            log_lines,
            f"{_stage_tag(4, 'theme+relativity', percent=42)} watchdog: heartbeat={stage4_heartbeat_seconds}s, theme_idle_timeout={theme_idle_timeout_seconds}s, relativity_idle_timeout={relativity_idle_timeout_seconds}s",
        )
        _append_log(log_lines, f"{_stage_tag(4, 'theme+relativity', percent=42)} running concurrently ({','.join(stage4_jobs.keys())})")
        with ThreadPoolExecutor(max_workers=max(len(stage4_jobs), 1)) as ex:
            futures = {
                ex.submit(
                    _run_command_with_live_output,
                    log_lines,
                    cmd=cmd,
                    cwd=ROOT_DIR,
                    step_index=4,
                    stage_name=name,
                    idle_timeout_seconds=(theme_idle_timeout_seconds if name == "theme" else relativity_idle_timeout_seconds),
                ): name
                for name, cmd in stage4_jobs.items()
            }
            pending = set(futures.keys())
            while pending:
                done, pending = wait(pending, timeout=stage4_heartbeat_seconds, return_when=FIRST_COMPLETED)
                if not done:
                    running_names = [futures[f] for f in pending]
                    _append_log(
                        log_lines,
                        f"{_stage_tag(4, 'theme+relativity', percent=50)} still running ({','.join(sorted(running_names))})",
                    )
                    continue

                for fut in done:
                    name = futures[fut]
                    returncode, _tail = fut.result()
                    if name == "theme":
                        theme_success = returncode == 0
                    elif name == "relativity":
                        relativity_success = returncode == 0

    theme_csv_path = _find_theme_result_csv(today_yyyymmdd) if enable_theme else None
    if theme_csv_path and theme_csv_path.exists():
        theme_rows = _read_theme_rows(theme_csv_path)
        theme_raw_count = len(theme_rows)
        if theme_cctv_only:
            filtered_theme_rows = _filter_theme_rows_with_cctv(theme_rows, date_yyyymmdd=today_yyyymmdd)
            theme_cctv_count = len(filtered_theme_rows)
            if theme_raw_count > 0 and theme_cctv_count == 0:
                _append_log(log_lines, "Theme cctv-only 过滤后为空，回退到题材原始候选。")
            else:
                theme_rows = filtered_theme_rows
        else:
            theme_cctv_count = theme_raw_count
        theme_rows = _filter_rows_by_dividend_yield(
            theme_rows,
            min_yield_pct=min_dividend_yield_pct,
            log_lines=log_lines,
            label="theme",
        )
        _append_log(
            log_lines,
            f"Theme csv: {theme_csv_path} (raw={theme_raw_count}, cctv_matched={theme_cctv_count}, shown={len(theme_rows)}, cctv_only={int(theme_cctv_only)})",
        )
    elif enable_theme:
        _append_log(log_lines, "Theme strategy result csv not found.")

    relativity_csv_path = _find_relativity_result_csv(today_yyyymmdd) if enable_relativity else None
    if relativity_csv_path and relativity_csv_path.exists():
        relativity_rows = _read_relativity_rows(
            relativity_csv_path,
            limit=20,
            min_down_ratio_pct=float(relativity_min_down_ratio_pct),
        )
        relativity_rows = _filter_rows_by_dividend_yield(
            relativity_rows,
            min_yield_pct=min_dividend_yield_pct,
            log_lines=log_lines,
            label="relativity",
        )
        _append_log(log_lines, f"Relativity csv: {relativity_csv_path} (rows={len(relativity_rows)}, min_down_ratio_pct={relativity_min_down_ratio_pct})")
    elif enable_relativity:
        _append_log(log_lines, "Relativity strategy result csv not found.")

    csv_path = None
    rows = []
    today = today_yyyymmdd
    if success:
        csv_path = _find_result_csv(today)
        if csv_path and csv_path.exists():
            rows, _ = _read_rows(csv_path)
            rows = _filter_rows_by_dividend_yield(
                rows,
                min_yield_pct=min_dividend_yield_pct,
                log_lines=log_lines,
                label="boll",
            )
            _append_log(log_lines, f"Result csv: {csv_path} (rows={len(rows)})")
        else:
            _append_log(log_lines, "No result csv found after run.")

    msg = _build_message(
        success=success,
        csv_path=csv_path,
        rows=rows,
        run_output_tail=output_tail,
    )
    if success and rows:
        fundamental_text = _build_fundamental_summary(rows, top_n=20)
        if fundamental_text:
            msg = msg + "\n\n## 2.1) 基本面速览(前20)\n" + fundamental_text
    market_summary, regime = _build_market_and_strategy_summary(
        boll_rows_count=len(rows),
        theme_rows_count=len(theme_rows),
        macro_risk_summary=macro_risk_summary,
        cctv_summary=cctv_summary,
        has_cctv_hot=bool(_load_cctv_codes_by_date(today_yyyymmdd)),
        macro_external_level=macro_external_level,
    )
    _append_log(
        log_lines,
        (
            "MARKET_REGIME"
            f" | regime={regime}"
            f" | boll_rows={len(rows)}"
            f" | theme_rows={len(theme_rows)}"
            f" | macro_risk={_macro_risk_level(macro_risk_summary)}"
            f" | cctv_hot={'1' if bool(_load_cctv_codes_by_date(today_yyyymmdd)) else '0'}"
        ),
    )
    if market_summary:
        msg = msg + "\n" + market_summary
    msg = msg + "\n" + _build_theme_message(
        theme_csv_path=theme_csv_path,
        theme_rows=theme_rows,
        raw_count=theme_raw_count,
        cctv_only=theme_cctv_only,
        cctv_count=theme_cctv_count,
    )
    msg = msg + "\n" + _build_relativity_message(relativity_csv_path=relativity_csv_path, relativity_rows=relativity_rows)
    if macro_risk_summary:
        msg = msg + "\n\n## 7) 宏观与国际风险提示\n" + macro_risk_summary.lstrip()
    if macro_news_trend:
        msg = msg + "\n" + macro_news_trend.lstrip()
    if macro_external_summary:
        msg = msg + "\n" + macro_external_summary
    if macro_economic_summary:
        msg = msg + "\n" + macro_economic_summary
    if cctv_summary:
        msg = msg + "\n\n## 8) CCTV 热点概览\n" + cctv_summary.lstrip()

    if output_tail:
        _append_log(log_lines, "--- Last run output (tail) ---")
        for line in output_tail.splitlines():
            log_lines.append(line)

    enable_archive = os.getenv("ENABLE_AUTO_ARCHIVE", "1").strip() != "0"
    if enable_archive:
        _run_data_archive(log_lines)
    else:
        _append_log(log_lines, f"{_stage_tag(5, 'archive')} skipped by ENABLE_AUTO_ARCHIVE=0")

    enable_cleanup = os.getenv("ENABLE_AUTO_CLEANUP", "1").strip() != "0"
    if enable_cleanup:
        _run_data_cleanup(log_lines)
    else:
        _append_log(log_lines, f"{_stage_tag(6, 'cleanup')} skipped by ENABLE_AUTO_CLEANUP=0")

    # Daily archive may move files before notify; resolve attachment paths from archive.
    if csv_path and not csv_path.exists():
        archived_csv = _find_archived_file_by_name(csv_path.name)
        if archived_csv and archived_csv.exists():
            csv_path = archived_csv
            _append_log(log_lines, f"{_stage_tag(7, 'notify', percent=85)} resolved archived csv: {csv_path}")

    if theme_csv_path and not theme_csv_path.exists():
        archived_theme_csv = _find_archived_file_by_name(theme_csv_path.name)
        if archived_theme_csv and archived_theme_csv.exists():
            theme_csv_path = archived_theme_csv
            _append_log(log_lines, f"{_stage_tag(7, 'notify', percent=85)} resolved archived theme csv: {theme_csv_path}")

    if relativity_csv_path and not relativity_csv_path.exists():
        archived_relativity_csv = _find_archived_file_by_name(relativity_csv_path.name)
        if archived_relativity_csv and archived_relativity_csv.exists():
            relativity_csv_path = archived_relativity_csv
            _append_log(log_lines, f"{_stage_tag(7, 'notify', percent=85)} resolved archived relativity csv: {relativity_csv_path}")

    pushed = False

    subject = f"Stocks-Master Daily {'OK' if success else 'FAILED'} | {regime}"
    extra_csv_paths = []
    if theme_csv_path and theme_success:
        extra_csv_paths.append(theme_csv_path)
    if relativity_csv_path and relativity_success:
        extra_csv_paths.append(relativity_csv_path)
    _append_log(log_lines, f"{_stage_tag(7, 'notify', percent=93)} sending email")
    pushed = send_email(subject, msg, csv_path, log_lines, extra_attachment_paths=extra_csv_paths) or pushed

    if not pushed:
        _append_log(log_lines, f"{_stage_tag(7, 'notify')} no push channel configured/succeeded. Finished local run only")
    else:
        _append_log(log_lines, f"{_stage_tag(7, 'notify')} notification finished")

    enable_compress = os.getenv("ENABLE_AUTO_COMPRESS", "1").strip() != "0"
    if enable_compress:
        _run_data_compress(log_lines)
    else:
        _append_log(log_lines, f"{_stage_tag(8, 'compress')} skipped by ENABLE_AUTO_COMPRESS=0")

    _append_log(log_lines, "[100%] Pipeline finished")

    if RUN_LOG_FILE is not None:
        RUN_LOG_FILE.write_text("\n".join(log_lines), encoding="utf-8")
        _append_log(log_lines, f"Log saved: {RUN_LOG_FILE}")

    return 0 if success else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        try:
            LOG_DIR.mkdir(parents=True, exist_ok=True)
            fatal_log = LOG_DIR / f"boll_auto_fatal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            fatal_log.write_text(traceback.format_exc(), encoding="utf-8")
            print(f"[FATAL] unhandled exception captured: {fatal_log}")
        except Exception:
            pass
        raise
