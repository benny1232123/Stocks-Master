import csv
import argparse
import json
import os
import smtplib
import subprocess
import sys
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from urllib import error, request


ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT_DIR / "Frequently-Used-Program" / "Stock-Selection-Boll.py"
STOCK_DATA_DIR = ROOT_DIR / "stock_data"
LOG_DIR = STOCK_DATA_DIR / "auto_logs"


def _append_log(log_lines, message):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {message}"
    print(line)
    log_lines.append(line)


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
        "Top picks (up to 20):\n"
        f"{preview_block}"
    )


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


def send_email(subject, content, csv_path, log_lines):
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

    if csv_path and csv_path.exists():
        with csv_path.open("rb") as f:
            msg.add_attachment(
                f.read(),
                maintype="text",
                subtype="csv",
                filename=csv_path.name,
            )

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

    _append_log(log_lines, f"Start run script: {SCRIPT_PATH}")

    proc = subprocess.run(
        [sys.executable, str(SCRIPT_PATH)],
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    combined_output = "\n".join(
        x for x in [proc.stdout.strip(), proc.stderr.strip()] if x
    )
    output_tail = "\n".join(combined_output.splitlines()[-25:]) if combined_output else ""

    success = proc.returncode == 0
    _append_log(log_lines, f"Run finished with code {proc.returncode}")

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

    pushed = False
    webhook_url = os.getenv("WECOM_WEBHOOK_URL", "").strip()
    if webhook_url:
        pushed = send_wecom_markdown(webhook_url, msg, log_lines) or pushed
    else:
        _append_log(log_lines, "WECOM_WEBHOOK_URL is empty; skip wecom push.")

    subject = f"Stocks-Master Daily {'OK' if success else 'FAILED'}"
    pushed = send_email(subject, msg, csv_path, log_lines) or pushed

    if not pushed:
        _append_log(log_lines, "No push channel configured/succeeded. Finished local run only.")

    if output_tail:
        _append_log(log_lines, "--- Last run output (tail) ---")
        for line in output_tail.splitlines():
            log_lines.append(line)

    log_file = LOG_DIR / f"boll_auto_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file.write_text("\n".join(log_lines), encoding="utf-8")
    _append_log(log_lines, f"Log saved: {log_file}")

    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
