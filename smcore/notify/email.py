"""SMTP 邮件推送（带 CSV 附件）。"""
from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import Optional


def send_email(
    subject: str,
    content: str,
    csv_path: Optional[str] = None,
    log_lines: Optional[list] = None,
    extra_attachment_paths: Optional[list] = None,
    html_content: Optional[str] = None,
) -> bool:
    """通过 SMTP_SSL 发送邮件，可附带 CSV/HTML 附件。

    环境变量：SMTP_HOST / SMTP_PORT(默认465) / SMTP_USER / SMTP_PASS / SMTP_TO。
    html_content 不为空时，邮件以 multipart/alternative 发送（HTML 正文 + 纯文本兜底），
    大多数邮件客户端会渲染 HTML，同时保留 content 作为纯文本回退。
    """
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
        if log_lines is not None:
            log_lines.append(f"SMTP config incomplete; missing: {', '.join(missing)}")
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to_addr
    msg.set_content(content)

    # HTML 正文（可选）
    if html_content:
        try:
            msg.add_alternative(html_content, subtype="html")
        except Exception as exc:
            if log_lines is not None:
                log_lines.append(f"[WARN] HTML 正文添加失败，仅发纯文本: {exc}")

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
        subtype = "html" if p.suffix.lower() == ".html" else "csv"
        with p.open("rb") as f:
            msg.add_attachment(f.read(), maintype="text", subtype=subtype, filename=p.name)
        attached_names.add(p.name)
    if attached_names and log_lines is not None:
        log_lines.append(f"Email attachments: {', '.join(sorted(attached_names))}")

    try:
        with smtplib.SMTP_SSL(host, port, timeout=15) as server:
            server.login(user, password)
            server.send_message(msg)
        if log_lines is not None:
            log_lines.append("SMTP email sent.")
        return True
    except Exception as exc:
        if log_lines is not None:
            log_lines.append(f"SMTP email failed: {exc}")
        return False
