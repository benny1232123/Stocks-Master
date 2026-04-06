import csv
import argparse
import json
import os
import smtplib
import subprocess
import sys
import time
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from urllib import error, request

import baostock as bs


ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "Stock-Selection-Boll.py"
THEME_SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "Stock-Selection-Ashare-Theme-Turnover.py"
CCTV_SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "Stock-Selection-CCTV-Sectors.py"
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
    display_cmd = " ".join(str(part) for part in cmd)
    _append_log(log_lines, f"{_stage_tag(step_index, stage_name, percent=start_percent)} start: {display_cmd}")

    started = time.monotonic()
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
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
            "# Stocks-Master Daily Run Completed\n"
            f"> Time: {now}\n\n"
            "Run completed, but no result csv was found."
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
        "# Stocks-Master Daily Run Completed\n"
        f"> Time: {now}\n"
        f"> Picks: {total}\n"
        f"> CSV: {csv_path}\n\n"
        "Boll策略候选(前20):\n"
        f"{preview_block}"
    )


def _build_theme_message(theme_csv_path=None, theme_rows=None):
    if theme_csv_path is None:
        return "\n题材策略: 本次未找到结果文件。"
    picks = len(theme_rows or [])
    if picks == 0:
        return f"\n题材策略:\n- 结果文件: {theme_csv_path}\n- 候选数: 0"

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
        "- 说明: 分数越高，代表题材匹配+资金活跃+动量越强\n"
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


def _find_latest_news_file(today_yyyymmdd):
    preferred = STOCK_DATA_DIR / f"{today_yyyymmdd}_news.csv"
    if preferred.exists():
        return preferred
    files = sorted(
        STOCK_DATA_DIR.glob("*_news.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return files[0] if files else None


def _build_macro_risk_summary(news_csv_path, top_n=3):
    if news_csv_path is None or not news_csv_path.exists():
        return ""

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
        return ""

    if not events:
        return ""

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
            trend = "升温"
            try:
                c = float(change)
                if c < 0:
                    trend = "降温"
                elif c == 0:
                    trend = "持平"
            except Exception:
                trend = "新上榜" if str(change).upper() == "NEW" else "变化待定"
            lines.append(f"{idx}. {sec} | 热度:{heat} | 变化:{change} ({trend})")
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

    _append_log(log_lines, f"Python: {sys.executable}")

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

    _append_log(log_lines, "[  0%] Pipeline started (7 steps): 1=boll, 2=cctv, 3=macro-news, 4=theme, 5=archive, 6=cleanup, 7=notify")
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
        cctv_returncode, cctv_tail = _run_command_with_live_output(
            log_lines,
            cmd=[sys.executable, str(CCTV_SCRIPT_PATH), "--top-n", "5", "--emerging-top-n", "20"],
            cwd=ROOT_DIR,
            step_index=2,
            stage_name="cctv",
        )
        if cctv_returncode != 0 and cctv_tail:
            _append_log(log_lines, "--- CCTV output tail ---")
            for line in cctv_tail.splitlines():
                log_lines.append(line)
        cctv_file = _find_latest_cctv_hot_file(datetime.now().strftime("%Y%m%d"))
        cctv_summary = _read_cctv_top_summary(cctv_file, top_n=5)
    else:
        _append_log(log_lines, f"{_stage_tag(2, 'cctv')} skipped by ENABLE_CCTV_STRATEGY=0")

    _append_log(log_lines, f"{_stage_tag(3, 'macro-news', percent=29)} collecting risk summary")
    news_file = _find_latest_news_file(datetime.now().strftime("%Y%m%d"))
    macro_risk_summary = _build_macro_risk_summary(news_file, top_n=3)
    _append_log(log_lines, f"{_stage_tag(3, 'macro-news')} done")

    theme_csv_path = None
    theme_rows = []
    theme_success = False
    enable_theme = os.getenv("ENABLE_THEME_STRATEGY", "1").strip() != "0"
    if enable_theme:
        theme_min_latest_turn = os.getenv("THEME_MIN_LATEST_TURN", "0.8").strip() or "0.8"
        theme_min_avg_turn5 = os.getenv("THEME_MIN_AVG_TURN5", "0.6").strip() or "0.6"
        theme_min_latest_amount = os.getenv("THEME_MIN_LATEST_AMOUNT", "120000000").strip() or "120000000"
        theme_max_stocks = os.getenv("THEME_MAX_STOCKS", "1200").strip() or "1200"
        theme_top_n = os.getenv("THEME_TOP_N", "30").strip() or "30"

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
        theme_csv_path = _find_theme_result_csv(datetime.now().strftime("%Y%m%d"))
        if theme_csv_path and theme_csv_path.exists():
            theme_rows = _read_theme_rows(theme_csv_path, limit=20)
            _append_log(log_lines, f"Theme csv: {theme_csv_path} (rows={len(theme_rows)})")
        else:
            _append_log(log_lines, "Theme strategy result csv not found.")
    else:
        _append_log(log_lines, f"{_stage_tag(4, 'theme')} skipped by ENABLE_THEME_STRATEGY=0")

    csv_path = None
    rows = []
    today = datetime.now().strftime("%Y%m%d")
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
    msg = msg + "\n" + _build_theme_message(theme_csv_path=theme_csv_path, theme_rows=theme_rows)
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

    pushed = False
    webhook_url = os.getenv("WECOM_WEBHOOK_URL", "").strip()
    if webhook_url:
        _append_log(log_lines, f"{_stage_tag(7, 'notify', percent=86)} sending WeCom message")
        pushed = send_wecom_markdown(webhook_url, msg, log_lines) or pushed
    else:
        _append_log(log_lines, f"{_stage_tag(7, 'notify', percent=86)} WECOM_WEBHOOK_URL is empty; skip wecom push")

    subject = f"Stocks-Master Daily {'OK' if success else 'FAILED'}"
    extra_csv_paths = [theme_csv_path] if theme_csv_path and theme_success else []
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
