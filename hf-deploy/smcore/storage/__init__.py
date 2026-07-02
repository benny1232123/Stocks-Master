"""云端存储 —— COS 对象存储（SCF 场景）。"""
from __future__ import annotations

from .cos import download_latest, download_file, get_latest_key, list_objects, upload_file

__all__ = ["upload_file", "download_file", "list_objects", "get_latest_key", "download_latest"]
