"""消息推送 —— SMTP 邮件。

此前推送逻辑内嵌在 auto_notify_boll.py(3306行巨石)，无法被可视化主线复用。
本模块提供单一推送入口，两条主线共用。
"""
from __future__ import annotations

from .email import send_email

__all__ = ["send_email"]
