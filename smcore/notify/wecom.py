"""企业微信群机器人 webhook 推送。"""
from __future__ import annotations

import json
from typing import Optional
from urllib import error, request


def send_wecom_markdown(webhook_url: str, content: str, log_lines: Optional[list] = None) -> bool:
    """向企业微信机器人发送 markdown 消息。

    Args:
        webhook_url: 机器人 webhook 地址。
        content: markdown 文本。
        log_lines: 可选日志收集列表，追加推送结果。
    """
    payload = {
        "msgtype": "markdown",
        "markdown": {"content": content},
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
            if log_lines is not None:
                log_lines.append(f"WeCom webhook sent. Response: {body}")
            return True
    except error.URLError as exc:
        if log_lines is not None:
            log_lines.append(f"WeCom webhook failed: {exc}")
        return False
