import csv
import argparse
import json
import os
import re
import smtplib
import subprocess
import sys
import time
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from urllib import error, request

import akshare as ak
import baostock as bs
import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "Stock-Selection-Boll.py"
THEME_SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "Stock-Selection-Ashare-Theme-Turnover.py"
CCTV_SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "Stock-Selection-CCTV-Sectors.py"
RELATIVITY_SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "Stock-Selection-Relativity.py"
CLEANUP_SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "cleanup_stock_data.py"
ARCHIVE_SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "archive_stock_data.py"
STOCK_DATA_DIR = ROOT_DIR / "stock_data"
LOG_DIR = STOCK_DATA_DIR / "auto_logs"
PIPELINE_TOTAL_STEPS = 7


def _append_log(log_lines, message):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {message}"
    print(line)
    log_lines.append(line)


def _stage_tag(step_index, stage_name, *, percent=None, total_steps=PIPELINE_TOTAL_STEPS):
    safe_step = max(1, min(int(step_index), int(total_steps)))
    pct = int(percent) if percent is not None else int(round(safe_step * 100 / total_steps))
    pct = max(0, min(100, pct))
    return f"[{pct:>3d}%][{safe_step}/{total_steps} {stage_name}]"


def _run_command_with_live_output(log_lines, *, cmd, cwd, step_index, stage_name):
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
    if proc.stdout is not None:
        for raw in proc.stdout:
            line = raw.rstrip("\r\n")
            if not line:
                continue
            lines.append(line)
            _append_log(log_lines, f"{_stage_tag(step_index, stage_name, percent=running_percent)} {line}")

    returncode = proc.wait()
    elapsed = time.monotonic() - started
    status = "OK" if returncode == 0 else f"FAILED({returncode})"
    _append_log(log_lines, f"{_stage_tag(step_index, stage_name, percent=done_percent)} done: {status}, elapsed={elapsed:.1f}s")

    tail = "\n".join(lines[-40:]) if lines else ""
    return returncode, tail


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


def _read_theme_rows(csv_path, limit=20):
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
    return rows[:limit]


def _normalize_code(code):
    digits = "".join(ch for ch in str(code or "") if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _to_float(value):
    try:
        return float(value)
    except Exception:
        return None


def _normalize_confidence_label(raw_value):
    text = str(raw_value or "").strip().lower()
    if text in {"高", "high", "h"}:
        return "高"
    if text in {"中", "medium", "mid", "m"}:
        return "中"
    if text in {"低", "low", "l"}:
        return "低"
    return "中"


def _format_yi(value):
    num = _to_float(value)
    if num is None:
        return "N/A"
    return f"{num / 1e8:.1f}亿"


def _to_bs_code(code):
    norm = _normalize_code(code)
    if not norm:
        return ""
    return f"sh.{norm}" if norm.startswith("6") else f"sz.{norm}"


def _safe_pct(numerator, denominator):
    if denominator in (None, 0):
        return None
    return (numerator / denominator - 1.0) * 100.0


def _fmt_pct(value, digits=2, signed=False, na="N/A"):
    num = _to_float(value)
    if num is None:
        return na
    sign = "+" if signed else ""
    return f"{num:{sign}.{digits}f}%"


def _fmt_num(value, digits=2, na="N/A"):
    num = _to_float(value)
    if num is None:
        return na
    return f"{num:.{digits}f}"


def _fetch_bs_latest_row(bs_code, end_date_text, lookback_days=40):
    start_date_text = (datetime.strptime(end_date_text, "%Y-%m-%d") - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    rs = bs.query_history_k_data_plus(
        bs_code,
        "date,code,close,turn,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
        start_date=start_date_text,
        end_date=end_date_text,
        frequency="d",
        adjustflag="2",
    )
    if rs.error_code != "0":
        return None

    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    if not data_list or not rs.fields:
        return None
    row = dict(zip(rs.fields, data_list[-1]))
    return row


def _fetch_market_index_series(bs_code, end_date_text, lookback_days=90):
    market_code = str(bs_code or "").strip().lower().replace(".", "")
    if not market_code:
        return []

    start_date_text = (datetime.strptime(end_date_text, "%Y-%m-%d") - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    try:
        df = ak.stock_zh_index_daily_em(symbol=market_code)
    except Exception:
        return []
    if df is None or df.empty:
        return []

    col_map = {str(c).strip(): str(c) for c in df.columns}
    date_col = col_map.get("date", "")
    close_col = col_map.get("close", "")
    if not date_col or not close_col:
        return []

    work = df[[date_col, close_col]].copy()
    work["date"] = pd.to_datetime(work[date_col], errors="coerce")
    work["close"] = pd.to_numeric(work[close_col], errors="coerce")
    work = work.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)

    start_dt = pd.to_datetime(start_date_text, errors="coerce")
    end_dt = pd.to_datetime(end_date_text, errors="coerce")
    if pd.notna(start_dt):
        work = work[work["date"] >= start_dt]
    if pd.notna(end_dt):
        work = work[work["date"] <= end_dt]

    if work.empty:
        return []

    work["pct_chg"] = work["close"].pct_change() * 100.0
    out = [
        {
            "date": row.date.strftime("%Y-%m-%d"),
            "close": float(row.close),
            "pct_chg": (None if pd.isna(row.pct_chg) else float(row.pct_chg)),
        }
        for row in work.itertuples(index=False)
    ]
    return out


def _compute_index_metrics(series):
    if not series:
        return {}
    closes = [r["close"] for r in series if r.get("close") is not None]
    if len(closes) < 6:
        return {}

    latest = closes[-1]
    ret_5d = _safe_pct(latest, closes[-6])
    ret_20d = _safe_pct(latest, closes[-21]) if len(closes) >= 21 else None

    daily_rets = []
    for i in range(1, len(closes)):
        prev = closes[i - 1]
        cur = closes[i]
        if prev:
            daily_rets.append((cur / prev - 1.0) * 100.0)
    vol_20d = None
    if len(daily_rets) >= 20:
        sample = daily_rets[-20:]
        mean_v = sum(sample) / len(sample)
        var_v = sum((x - mean_v) ** 2 for x in sample) / len(sample)
        vol_20d = var_v ** 0.5

    return {
        "latest": latest,
        "ret_5d": ret_5d,
        "ret_20d": ret_20d,
        "vol_20d": vol_20d,
        "last_date": series[-1].get("date") or "",
    }


def _macro_risk_level(macro_risk_summary):
    if not macro_risk_summary:
        return "low"
    high_hits = macro_risk_summary.count("[高]")
    medium_hits = macro_risk_summary.count("[中]")
    if high_hits >= 2:
        return "high"
    if high_hits >= 1 or medium_hits >= 2:
        return "medium"
    return "low"


def _env_int_percent(name, default):
    text = os.getenv(name, "").strip()
    if not text:
        return int(default)
    try:
        value = int(float(text))
    except Exception:
        return int(default)
    return max(0, min(100, value))


def _normalize_weight_map(weights):
    normalized = {}
    for key, value in weights.items():
        try:
            normalized[key] = max(0, int(value))
        except Exception:
            normalized[key] = 0

    total = sum(normalized.values())
    if total <= 0:
        return {"boll": 40, "theme": 25, "cctv": 10, "relativity": 15, "cash": 10}

    scaled = {key: int(round(val * 100.0 / total)) for key, val in normalized.items()}
    delta = 100 - sum(scaled.values())
    if delta != 0:
        anchor = "cash" if "cash" in scaled else max(scaled, key=scaled.get)
        scaled[anchor] = max(0, scaled.get(anchor, 0) + delta)
    return scaled


def _rebalance_for_signal_availability(weights, *, boll_rows_count, theme_rows_count, has_cctv_hot):
    adjusted = dict(weights)

    if boll_rows_count <= 0 and adjusted.get("boll", 0) > 0:
        adjusted["cash"] = adjusted.get("cash", 0) + adjusted.get("boll", 0)
        adjusted["boll"] = 0

    if theme_rows_count <= 0 and adjusted.get("theme", 0) > 0:
        adjusted["cash"] = adjusted.get("cash", 0) + adjusted.get("theme", 0)
        adjusted["theme"] = 0

    if (not has_cctv_hot) and adjusted.get("cctv", 0) > 0:
        if theme_rows_count > 0:
            adjusted["theme"] = adjusted.get("theme", 0) + adjusted.get("cctv", 0)
        else:
            adjusted["cash"] = adjusted.get("cash", 0) + adjusted.get("cctv", 0)
        adjusted["cctv"] = 0

    return _normalize_weight_map(adjusted)


def _format_position_units(weight, units=10):
    return f"{weight * units / 100.0:.1f}成"


def _build_strategy_allocation(regime, *, boll_rows_count, theme_rows_count, has_cctv_hot, macro_level):
    if regime == "趋势上行":
        base_weights = {
            "theme": _env_int_percent("ALLOC_UP_THEME", 35),
            "cctv": _env_int_percent("ALLOC_UP_CCTV", 15),
            "boll": _env_int_percent("ALLOC_UP_BOLL", 25),
            "relativity": _env_int_percent("ALLOC_UP_RELATIVITY", 20),
            "cash": _env_int_percent("ALLOC_UP_CASH", 5),
        }
        priority_line = "- 执行优先级: 题材热度确认 > Boll回踩确认 > Relativity 强势过滤"
    elif regime == "下行防御":
        base_weights = {
            "cash": _env_int_percent("ALLOC_DOWN_CASH", 60),
            "boll": _env_int_percent("ALLOC_DOWN_BOLL", 25),
            "relativity": _env_int_percent("ALLOC_DOWN_RELATIVITY", 10),
            "theme": _env_int_percent("ALLOC_DOWN_THEME", 5),
            "cctv": _env_int_percent("ALLOC_DOWN_CCTV", 0),
        }
        priority_line = "- 执行优先级: 先控回撤，再做小仓位试错；题材策略明显降权。"
    else:
        theme_weight = 30 if theme_rows_count >= 20 else 25
        cctv_weight = 15 if has_cctv_hot else 10
        boll_weight = 35 if boll_rows_count >= 10 else 40
        relativity_weight = 20 if macro_level != "high" else 15
        cash_weight = 100 - theme_weight - cctv_weight - boll_weight - relativity_weight

        base_weights = {
            "boll": _env_int_percent("ALLOC_SIDE_BOLL", boll_weight),
            "theme": _env_int_percent("ALLOC_SIDE_THEME", theme_weight),
            "cctv": _env_int_percent("ALLOC_SIDE_CCTV", cctv_weight),
            "relativity": _env_int_percent("ALLOC_SIDE_RELATIVITY", relativity_weight),
            "cash": _env_int_percent("ALLOC_SIDE_CASH", cash_weight),
        }
        priority_line = "- 执行优先级: Boll定节奏，题材/CCTV找方向，Relativity做强弱确认。"

    normalized = _normalize_weight_map(base_weights)
    final_weights = _rebalance_for_signal_availability(
        normalized,
        boll_rows_count=boll_rows_count,
        theme_rows_count=theme_rows_count,
        has_cctv_hot=has_cctv_hot,
    )

    ratio_line = (
        "- 策略配比: "
        f"Boll低吸 {final_weights.get('boll', 0)}% | "
        f"题材轮动 {final_weights.get('theme', 0)}% | "
        f"CCTV跟随 {final_weights.get('cctv', 0)}% | "
        f"Relativity过滤 {final_weights.get('relativity', 0)}% | "
        f"现金观察 {final_weights.get('cash', 0)}%"
    )

    unit_line = (
        "- 仓位折算(10成): "
        f"Boll {_format_position_units(final_weights.get('boll', 0))} | "
        f"题材 {_format_position_units(final_weights.get('theme', 0))} | "
        f"CCTV {_format_position_units(final_weights.get('cctv', 0))} | "
        f"Relativity {_format_position_units(final_weights.get('relativity', 0))} | "
        f"现金 {_format_position_units(final_weights.get('cash', 0))}"
    )

    adaption_notes = []
    if boll_rows_count <= 0:
        adaption_notes.append("Boll候选不足")
    if theme_rows_count <= 0:
        adaption_notes.append("题材候选不足")
    if not has_cctv_hot:
        adaption_notes.append("CCTV热点缺失")
    if adaption_notes:
        adaption_line = "- 动态调整: " + "，".join(adaption_notes) + "，对应仓位已自动回流至其他策略或现金。"
    else:
        adaption_line = "- 动态调整: 当前信号完整，按默认推荐比例执行。"

    return [ratio_line, unit_line, priority_line, adaption_line]


def _build_market_and_strategy_summary(*, boll_rows_count, theme_rows_count, macro_risk_summary, cctv_summary):
    end_date_text = datetime.now().strftime("%Y-%m-%d")
    sh_series = _fetch_market_index_series("sh.000001", end_date_text, lookback_days=100)
    hs300_series = _fetch_market_index_series("sh.000300", end_date_text, lookback_days=100)

    sh_metrics = _compute_index_metrics(sh_series)
    hs300_metrics = _compute_index_metrics(hs300_series)

    sh_ret_20 = sh_metrics.get("ret_20d")
    sh_ret_5 = sh_metrics.get("ret_5d")
    sh_vol_20 = sh_metrics.get("vol_20d")

    macro_level = _macro_risk_level(macro_risk_summary)
    has_cctv_hot = bool(cctv_summary and "Top5" in cctv_summary)

    regime = "震荡轮动"
    if sh_ret_20 is not None and sh_ret_5 is not None:
        if sh_ret_20 >= 4.0 and sh_ret_5 >= 0 and (sh_vol_20 is None or sh_vol_20 <= 1.8):
            regime = "趋势上行"
        elif sh_ret_20 <= -4.0 or (sh_ret_5 <= -3.0 and (sh_vol_20 is not None and sh_vol_20 >= 1.8)):
            regime = "下行防御"

    if macro_level == "high":
        regime = "下行防御"

    lines = [
        "\n市场状态体检:",
        "- 数据源: akshare 指数日线（上证 sh000001 + 沪深300 sh000300）",
    ]

    if sh_metrics:
        lines.append(
            "- 上证: "
            f"5日{_fmt_pct(sh_metrics.get('ret_5d'), signed=True)} "
            f"20日{_fmt_pct(sh_metrics.get('ret_20d'), signed=True)} "
            f"20日波动{_fmt_pct(sh_metrics.get('vol_20d'))}"
        )
    if hs300_metrics:
        lines.append(
            "- 沪深300: "
            f"5日{_fmt_pct(hs300_metrics.get('ret_5d'), signed=True)} "
            f"20日{_fmt_pct(hs300_metrics.get('ret_20d'), signed=True)}"
        )

    lines.append(f"- 信号补充: Boll命中数={boll_rows_count} 题材候选数={theme_rows_count} CCTV热点={'有' if has_cctv_hot else '无'}")
    lines.append(f"- 宏观风险: {macro_level}")
    lines.append(f"- 市场判定: {regime}")
    lines.append("- 今日策略配比: 已按市场状态分配仓位比例，避免所有策略等权执行。")

    diag = [
        "\n判定依据说明:",
        "1. 趋势维度: 20日收益反映中短期方向，5日收益反映近端加速度。",
        "2. 波动维度: 20日波动率衡量资金分歧，波动过高时提高防守权重。",
        "3. 外生风险: 宏观新闻若出现高风险事件，优先下调风险敞口。",
        "4. 交易拥挤度: CCTV热点和题材候选数量用于确认市场活跃度。",
    ]

    reco = ["\n策略建议:"]
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
        _build_strategy_allocation(
            regime,
            boll_rows_count=boll_rows_count,
            theme_rows_count=theme_rows_count,
            has_cctv_hot=has_cctv_hot,
            macro_level=macro_level,
        )
    )

    risk_ctrl = [
        "\n执行与风控清单:",
        "1. 单票仓位上限: 建议不超过总资金的10%-15%。",
        "2. 止损纪律: 破位或回撤超过预设阈值时机械止损。",
        "3. 止盈纪律: 分批止盈，避免盈利回吐。",
        "4. 复盘重点: 记录命中来源（Boll/题材/CCTV）与次日延续性。",
    ]

    return "\n".join(lines + diag + reco + risk_ctrl), regime


def _build_fundamental_summary(rows, top_n=20):
    """对命中股票做简单基本面速览，使用 baostock，返回可拼接到消息的文本。"""
    if not rows:
        return ""

    login_res = bs.login()
    if login_res.error_code != "0":
        return "\n基本面速览: baostock 登录失败（已跳过）。"

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
    return "\n基本面速览(前20):\n" + "\n".join(lines)


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
            "## 执行总览\n"
            "- 主流程执行完成，但未找到 Boll 结果 CSV。"
        )

    total = len(rows or [])
    preview_lines = []
    for item in (rows or [])[:20]:
        if item["name"]:
            preview_lines.append(f"- {item['code']} {item['name']}")
        else:
            preview_lines.append(f"- {item['code']}")

    preview_block = "\n".join(preview_lines) if preview_lines else "- (empty)"
    return (
        "# Stocks-Master 日报\n"
        f"> 时间: {now}\n\n"
        "## 执行总览\n"
        f"- Boll候选总数: {total}\n"
        f"- 结果文件: {csv_path}\n"
        "- 说明: 本日报基于技术面(Boll)、题材热度、宏观新闻与CCTV热点综合生成。\n\n"
        "## Boll候选明细(前20)\n"
        f"{preview_block}"
    )


def _build_theme_message(theme_csv_path=None, theme_rows=None):
    if theme_csv_path is None:
        return "\n题材策略: 本次未找到结果文件。"
    picks = len(theme_rows or [])
    if picks == 0:
        return (
            "\n题材策略:\n"
            f"- 结果文件: {theme_csv_path}\n"
            "- 候选数: 0\n"
            "- 原理: 题材策略通过政策/舆情关键词 + 换手活跃度 + 动量筛选弹性方向。"
        )

    lines = []
    for item in (theme_rows or [])[:20]:
        score = item.get("score") or "N/A"
        turn = item.get("turn") or "N/A"
        theme = item.get("theme") or ""
        if theme:
            lines.append(f"- {item['code']} {item['name']} | 分数:{score} 换手:{turn}% | {theme}")
        else:
            lines.append(f"- {item['code']} {item['name']} | 分数:{score} 换手:{turn}%")

    return (
        "\n题材策略(前20):\n"
        f"- 结果文件: {theme_csv_path}\n"
        f"- 展示数量: {picks}\n"
        "- 原理: 综合分越高，通常代表题材匹配度更高、资金活跃度更强、短期动量更好。\n"
        "- 风险: 题材轮动切换快，需结合止盈止损，不可单凭分数重仓。\n"
        + "\n".join(lines)
    )


def _read_relativity_rows(csv_path, limit=20):
    rows = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                {
                    "code": (row.get("股票代码") or "").strip(),
                    "name": (row.get("股票名称") or "").strip(),
                    "up_ratio": (row.get("上涨满足率") or "").strip(),
                    "down_ratio": (row.get("抗跌满足率") or "").strip(),
                    "overlap_days": (row.get("对齐交易日") or "").strip(),
                }
            )
    return rows[:limit]


def _build_relativity_message(relativity_csv_path=None, relativity_rows=None):
    if relativity_csv_path is None:
        return "\n相对强弱策略: 本次未找到结果文件。"
    picks = len(relativity_rows or [])
    if picks == 0:
        return f"\n相对强弱策略:\n- 结果文件: {relativity_csv_path}\n- 候选数: 0"

    lines = []
    for item in (relativity_rows or [])[:20]:
        up_ratio = _to_float(item.get("up_ratio"))
        down_ratio = _to_float(item.get("down_ratio"))
        up_text = f"{up_ratio * 100:.1f}%" if up_ratio is not None else "N/A"
        down_text = f"{down_ratio * 100:.1f}%" if down_ratio is not None else "N/A"
        overlap = item.get("overlap_days") or "N/A"
        lines.append(
            f"- {item.get('code', '')} {item.get('name', '')} | 上涨满足率:{up_text} 抗跌满足率:{down_text} 对齐交易日:{overlap}"
        )

    return (
        "\n相对强弱策略(前20):\n"
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
    for p in _iter_all_cctv_hot_files():
        d = _extract_date_from_filename(p)
        if d is None:
            continue
        age = (today - d).days
        if 0 <= age < max(int(window_days), 1):
            candidates.append((p, d))

    # 若窗口内无数据，回退到最近一期，避免日报缺失该模块。
    if not candidates:
        latest = _find_latest_cctv_hot_file(today.strftime("%Y%m%d"))
        if latest is not None:
            d = _extract_date_from_filename(latest) or today
            candidates.append((latest, d))

    # 统计时按日期升序，方便计算区间变化。
    return sorted(candidates, key=lambda x: x[1])


def _build_cctv_period_summary(window_days=3, top_n=5):
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
    show_rows = rows[:top_n]
    sample_days = len(period_files)

    lines = [
        f"\nCCTV 热门板块 Top{top_n}（近{max(int(window_days), 1)}日统计）:",
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


def _build_macro_risk_summary(news_csv_path, top_n=3):
    if news_csv_path is None or not news_csv_path.exists():
        return "\n宏观与国际风险提示:\n- 新闻源: 未找到可用新闻文件\n- 解读: 本次跳过宏观风险打分"

    # 关键词尽量贴近A股交易语境：地缘冲突、能源价格、航运与外需链条
    risk_rules = {
        "地缘冲突": ["空袭", "冲突", "导弹", "军事", "战机", "袭击", "战争", "中东", "霍尔木兹"],
        "能源扰动": ["原油", "油价", "天然气", "重水", "核设施", "能源", "海峡"],
        "航运外需": ["航运", "港口", "外贸", "出口", "制裁", "关税", "供应链", "跨境"],
        "风险偏好": ["停摆", "大选", "不确定", "风险", "波动", "反击", "升级"],
    }
    easing_words = ["谈判", "缓和", "停火", "协议", "会谈"]

    events = []
    try:
        with news_csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                title = (row.get("title") or row.get("标题") or "").strip()
                content = (row.get("content") or row.get("内容") or "").strip()
                text = f"{title} {content}".lower()
                if not text.strip():
                    continue

                matched_tags = []
                risk_score = 0
                for tag, words in risk_rules.items():
                    hit = sum(1 for w in words if w.lower() in text)
                    if hit > 0:
                        matched_tags.append(tag)
                        risk_score += hit

                if risk_score == 0:
                    continue

                easing_hit = sum(1 for w in easing_words if w.lower() in text)
                risk_score = max(risk_score - easing_hit, 1)
                events.append(
                    {
                        "title": title or "(无标题)",
                        "tags": matched_tags,
                        "risk_score": risk_score,
                    }
                )
    except Exception:
        return "\n宏观与国际风险提示:\n- 新闻源: 读取失败\n- 解读: 本次跳过宏观风险打分"

    if not events:
        return (
            "\n宏观与国际风险提示:\n"
            f"- 新闻源: {news_csv_path.name}\n"
            "- 风险事件: 未命中高/中风险关键词\n"
            "- 解读: 当前宏观风险信号偏平稳"
        )

    events = sorted(events, key=lambda x: x["risk_score"], reverse=True)[:top_n]

    lines = [
        "\n宏观与国际风险提示:",
        f"- 新闻源: {news_csv_path.name}",
        "- 解读: 仅用于交易关注方向，不构成投资建议",
    ]
    for idx, item in enumerate(events, start=1):
        level = "高" if item["risk_score"] >= 4 else "中"
        tags = "/".join(item["tags"]) if item["tags"] else "综合"
        lines.append(f"{idx}. [{level}] {item['title']} | 影响链条: {tags}")

    return "\n".join(lines)


def _read_cctv_top_summary(csv_path, top_n=5):
    if csv_path is None or not csv_path.exists():
        return ""
    try:
        rows = []
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i >= top_n:
                    break
                sec = (row.get("板块") or "").strip()
                heat = (row.get("热度分") or "").strip()
                change = (row.get("较上一期热度变化") or "N/A").strip()
                if sec:
                    rows.append((sec, heat or "N/A", change or "N/A"))
        if not rows:
            return ""

        lines = ["\nCCTV 热门板块 Top5:"]
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


def send_wecom_markdown(webhook_url, content, log_lines):
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "content": content,
        },
    }
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=12) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            _append_log(log_lines, f"WeCom webhook sent. Response: {body}")
            return True
    except error.URLError as exc:
        _append_log(log_lines, f"WeCom webhook failed: {exc}")
        return False


def send_email(subject, content, csv_path, log_lines, extra_attachment_paths=None):
    host = os.getenv("SMTP_HOST", "").strip()
    port = int(os.getenv("SMTP_PORT", "465").strip())
    user = os.getenv("SMTP_USER", "").strip()
    password = os.getenv("SMTP_PASS", "").strip()
    to_addr = os.getenv("SMTP_TO", "").strip()

    missing = []
    if not host:
        missing.append("SMTP_HOST")
    if not user:
        missing.append("SMTP_USER")
    if not password:
        missing.append("SMTP_PASS")
    if not to_addr:
        missing.append("SMTP_TO")

    if missing:
        _append_log(log_lines, f"SMTP config incomplete; missing: {', '.join(missing)}")
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to_addr
    msg.set_content(content)

    attachment_paths = []
    if csv_path:
        attachment_paths.append(csv_path)
    if extra_attachment_paths:
        attachment_paths.extend(extra_attachment_paths)

    attached_names = set()
    for p in attachment_paths:
        if not p:
            continue
        p = Path(p)
        if not p.exists():
            continue
        if p.name in attached_names:
            continue
        with p.open("rb") as f:
            msg.add_attachment(
                f.read(),
                maintype="text",
                subtype="csv",
                filename=p.name,
            )
        attached_names.add(p.name)
    if attached_names:
        _append_log(log_lines, f"Email attachments: {', '.join(sorted(attached_names))}")

    try:
        with smtplib.SMTP_SSL(host, port, timeout=15) as server:
            server.login(user, password)
            server.send_message(msg)
        _append_log(log_lines, "SMTP email sent.")
        return True
    except Exception as exc:
        _append_log(log_lines, f"SMTP email failed: {exc}")
        return False


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


def main():
    args = parse_args()
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_lines = []
    fast_mode = args.fast_mode or os.getenv("FAST_MODE", "0").strip() == "1"

    _append_log(log_lines, f"Python: {sys.executable}")
    if fast_mode:
        _append_log(log_lines, "Fast mode enabled.")

    if args.test_notify or args.test_email_only:
        _append_log(log_lines, "Test mode enabled. Stock selection run is skipped.")
        msg = _build_test_message()

        pushed = False
        if not args.test_email_only:
            webhook_url = os.getenv("WECOM_WEBHOOK_URL", "").strip()
            if webhook_url:
                pushed = send_wecom_markdown(webhook_url, msg, log_lines) or pushed
            else:
                _append_log(log_lines, "WECOM_WEBHOOK_URL is empty; skip wecom push.")

        subject = args.subject.strip() or "Stocks-Master Notify Test"
        pushed = send_email(subject, msg, None, log_lines) or pushed

        if not pushed:
            _append_log(log_lines, "No push channel configured/succeeded in test mode.")

        log_file = LOG_DIR / f"boll_auto_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_file.write_text("\n".join(log_lines), encoding="utf-8")
        _append_log(log_lines, f"Log saved: {log_file}")
        return 0 if pushed else 1

    _append_log(log_lines, "[  0%] Pipeline started (7 steps): 1=boll, 2=cctv, 3=macro-news, 4=theme+relativity, 5=archive, 6=cleanup, 7=notify")
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
        cctv_auto_accept = os.getenv("CCTV_AUTO_ACCEPT_KEYWORDS", "0").strip() == "1"
        cctv_disable_extra_news = os.getenv("CCTV_DISABLE_EXTRA_NEWS", "0").strip() == "1"
        cctv_extra_sources = os.getenv("CCTV_EXTRA_NEWS_SOURCES", "cls,sina").strip() or "cls,sina"
        cctv_extra_limit = os.getenv("CCTV_EXTRA_NEWS_LIMIT", "120").strip() or "120"

        if cctv_disable_extra_news:
            cctv_cmd.append("--disable-extra-news")
        else:
            cctv_cmd.extend(["--extra-news-sources", cctv_extra_sources, "--extra-news-limit", cctv_extra_limit])
            _append_log(
                log_lines,
                f"{_stage_tag(2, 'cctv', percent=15)} extra-news enabled (sources={cctv_extra_sources}, limit={cctv_extra_limit})",
            )

        if cctv_auto_accept:
            cctv_min_count = os.getenv("CCTV_AUTO_ACCEPT_MIN_COUNT", "4").strip() or "4"
            cctv_min_conf = _normalize_confidence_label(os.getenv("CCTV_AUTO_ACCEPT_MIN_CONF", "medium"))
            cctv_cmd.extend(
                [
                    "--auto-accept-keywords",
                    "--auto-accept-min-count",
                    str(cctv_min_count),
                    "--auto-accept-min-confidence",
                    str(cctv_min_conf),
                ]
            )
            _append_log(
                log_lines,
                f"{_stage_tag(2, 'cctv', percent=16)} auto-accept enabled (min_count={cctv_min_count}, min_conf={cctv_min_conf})",
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
        cctv_summary = _build_cctv_period_summary(window_days=cctv_stats_days, top_n=5)

        if not cctv_summary:
            cctv_file = _find_latest_cctv_hot_file(datetime.now().strftime("%Y%m%d"))
            cctv_summary = _read_cctv_top_summary(cctv_file, top_n=5)
    else:
        _append_log(log_lines, f"{_stage_tag(2, 'cctv')} skipped by ENABLE_CCTV_STRATEGY=0")

    _append_log(log_lines, f"{_stage_tag(3, 'macro-news', percent=29)} collecting risk summary")
    news_file = _find_latest_news_file(datetime.now().strftime("%Y%m%d"))
    macro_risk_summary = _build_macro_risk_summary(news_file, top_n=3)
    _append_log(log_lines, f"{_stage_tag(3, 'macro-news')} done")

    today_yyyymmdd = datetime.now().strftime("%Y%m%d")
    min_price_text = os.getenv("MIN_STOCK_PRICE", "5").strip() or "5"
    max_price_text = os.getenv("MAX_STOCK_PRICE", "30").strip() or "30"

    theme_csv_path = None
    theme_rows = []
    theme_success = False
    enable_theme = os.getenv("ENABLE_THEME_STRATEGY", "1").strip() != "0"
    if enable_theme:
        default_theme_min_latest_turn = "0.8"
        default_theme_min_avg_turn5 = "0.6"
        default_theme_min_latest_amount = "120000000"
        default_theme_min_latest_price = min_price_text
        default_theme_max_latest_price = max_price_text
        default_theme_max_stocks = "600" if fast_mode else "1200"
        default_theme_top_n = "20" if fast_mode else "30"

        theme_min_latest_turn = os.getenv("THEME_MIN_LATEST_TURN", default_theme_min_latest_turn).strip() or default_theme_min_latest_turn
        theme_min_avg_turn5 = os.getenv("THEME_MIN_AVG_TURN5", default_theme_min_avg_turn5).strip() or default_theme_min_avg_turn5
        theme_min_latest_amount = os.getenv("THEME_MIN_LATEST_AMOUNT", default_theme_min_latest_amount).strip() or default_theme_min_latest_amount
        theme_min_latest_price = os.getenv("THEME_MIN_LATEST_PRICE", default_theme_min_latest_price).strip() or default_theme_min_latest_price
        theme_max_latest_price = os.getenv("THEME_MAX_LATEST_PRICE", default_theme_max_latest_price).strip() or default_theme_max_latest_price
        theme_max_stocks = os.getenv("THEME_MAX_STOCKS", default_theme_max_stocks).strip() or default_theme_max_stocks
        theme_top_n = os.getenv("THEME_TOP_N", default_theme_top_n).strip() or default_theme_top_n

        _append_log(
            log_lines,
            f"{_stage_tag(4, 'theme', percent=43)} params: max_stocks={theme_max_stocks}, top_n={theme_top_n}, min_latest_turn={theme_min_latest_turn}, min_avg_turn5={theme_min_avg_turn5}, min_latest_amount={theme_min_latest_amount}, min_latest_price={theme_min_latest_price}, max_latest_price={theme_max_latest_price}",
        )

        theme_returncode, theme_tail = _run_command_with_live_output(
            log_lines,
            cmd=[
                sys.executable,
                str(THEME_SCRIPT_PATH),
                "--top-n",
                str(theme_top_n),
                "--max-stocks",
                str(theme_max_stocks),
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
            ],
            cwd=ROOT_DIR,
            step_index=4,
            stage_name="theme",
        )
        if theme_returncode != 0 and theme_tail:
            _append_log(log_lines, "--- Theme output tail ---")
            for line in theme_tail.splitlines():
                log_lines.append(line)
        theme_success = theme_returncode == 0
        theme_csv_path = _find_theme_result_csv(today_yyyymmdd)
        if theme_csv_path and theme_csv_path.exists():
            theme_rows = _read_theme_rows(theme_csv_path, limit=20)
            _append_log(log_lines, f"Theme csv: {theme_csv_path} (rows={len(theme_rows)})")
        else:
            _append_log(log_lines, "Theme strategy result csv not found.")
    else:
        _append_log(log_lines, f"{_stage_tag(4, 'theme')} skipped by ENABLE_THEME_STRATEGY=0")

    relativity_csv_path = None
    relativity_rows = []
    relativity_success = False
    enable_relativity = os.getenv("ENABLE_RELATIVITY_STRATEGY", "1").strip() != "0"
    if enable_relativity:
        relativity_cmd = [sys.executable, str(RELATIVITY_SCRIPT_PATH)]
        relativity_max_workers = os.getenv("RELATIVITY_MAX_WORKERS", "1").strip() or "1"
        relativity_resume = os.getenv("RELATIVITY_RESUME", "1").strip() == "1"
        relativity_sleep_seconds = os.getenv("RELATIVITY_SLEEP_SECONDS", "2").strip() or "2"
        relativity_disable_rs = os.getenv("RELATIVITY_DISABLE_RS", "0").strip() == "1"
        relativity_use_seed = os.getenv("RELATIVITY_USE_SEED", "1").strip() != "0"
        relativity_min_price = os.getenv("RELATIVITY_MIN_PRICE", min_price_text).strip() or min_price_text
        relativity_max_price = os.getenv("RELATIVITY_MAX_PRICE", max_price_text).strip() or max_price_text

        relativity_cmd.extend([
            "--max-workers",
            str(relativity_max_workers),
            "--sleep-seconds",
            str(relativity_sleep_seconds),
            "--price-lower-limit",
            str(relativity_min_price),
            "--price-upper-limit",
            str(relativity_max_price),
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
            f"{_stage_tag(4, 'relativity', percent=47)} params: workers={relativity_max_workers}, resume={int(relativity_resume)}, sleep={relativity_sleep_seconds}, disable_rs={int(relativity_disable_rs)}, use_seed={int(relativity_use_seed)}, min_price={relativity_min_price}, max_price={relativity_max_price}",
        )

        relativity_returncode, relativity_tail = _run_command_with_live_output(
            log_lines,
            cmd=relativity_cmd,
            cwd=ROOT_DIR,
            step_index=4,
            stage_name="relativity",
        )
        if relativity_returncode != 0 and relativity_tail:
            _append_log(log_lines, "--- Relativity output tail ---")
            for line in relativity_tail.splitlines():
                log_lines.append(line)
        relativity_success = relativity_returncode == 0
        relativity_csv_path = _find_relativity_result_csv(today_yyyymmdd)
        if relativity_csv_path and relativity_csv_path.exists():
            relativity_rows = _read_relativity_rows(relativity_csv_path, limit=20)
            _append_log(log_lines, f"Relativity csv: {relativity_csv_path} (rows={len(relativity_rows)})")
        else:
            _append_log(log_lines, "Relativity strategy result csv not found.")
    else:
        _append_log(log_lines, f"{_stage_tag(4, 'relativity')} skipped by ENABLE_RELATIVITY_STRATEGY=0")

    csv_path = None
    rows = []
    today = today_yyyymmdd
    if success:
        csv_path = _find_result_csv(today)
        if csv_path and csv_path.exists():
            rows, _ = _read_rows(csv_path)
            _append_log(log_lines, f"Result csv: {csv_path} (rows={len(rows)})")
        else:
            _append_log(log_lines, "No result csv found after run.")

    msg = _build_message(
        success=success,
        csv_path=csv_path,
        rows=rows,
        run_output_tail=output_tail,
    )
    if macro_risk_summary:
        msg = msg + "\n" + macro_risk_summary
    if success and rows:
        msg = msg + _build_fundamental_summary(rows, top_n=20)
    market_summary, regime = _build_market_and_strategy_summary(
        boll_rows_count=len(rows),
        theme_rows_count=len(theme_rows),
        macro_risk_summary=macro_risk_summary,
        cctv_summary=cctv_summary,
    )
    _append_log(
        log_lines,
        (
            "MARKET_REGIME"
            f" | regime={regime}"
            f" | boll_rows={len(rows)}"
            f" | theme_rows={len(theme_rows)}"
            f" | macro_risk={_macro_risk_level(macro_risk_summary)}"
            f" | cctv_hot={'1' if (cctv_summary and 'Top5' in cctv_summary) else '0'}"
        ),
    )
    if market_summary:
        msg = msg + "\n" + market_summary
    msg = msg + "\n" + _build_theme_message(theme_csv_path=theme_csv_path, theme_rows=theme_rows)
    msg = msg + "\n" + _build_relativity_message(relativity_csv_path=relativity_csv_path, relativity_rows=relativity_rows)
    if cctv_summary:
        msg = msg + "\n" + cctv_summary

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
    webhook_url = os.getenv("WECOM_WEBHOOK_URL", "").strip()
    if webhook_url:
        _append_log(log_lines, f"{_stage_tag(7, 'notify', percent=86)} sending WeCom message")
        pushed = send_wecom_markdown(webhook_url, msg, log_lines) or pushed
    else:
        _append_log(log_lines, f"{_stage_tag(7, 'notify', percent=86)} WECOM_WEBHOOK_URL is empty; skip wecom push")

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

    _append_log(log_lines, "[100%] Pipeline finished")

    log_file = LOG_DIR / f"boll_auto_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file.write_text("\n".join(log_lines), encoding="utf-8")
    _append_log(log_lines, f"Log saved: {log_file}")

    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
