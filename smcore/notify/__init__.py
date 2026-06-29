"""消息推送 —— 企业微信 webhook + SMTP 邮件。

此前推送逻辑内嵌在 auto_notify_boll.py(3306行巨石)，无法被可视化主线复用。
本模块提供单一推送入口，两条主线共用。
"""
from __future__ import annotations

from .email import send_email
from .wecom import send_wecom_markdown

__all__ = ["send_wecom_markdown", "send_email"]
