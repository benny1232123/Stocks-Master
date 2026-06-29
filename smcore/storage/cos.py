"""腾讯云 COS 对象存储辅助 —— 状态在云端。

SCF 无持久存储（/tmp 是临时的），操作清单等状态需存 COS。
本地 daemon 跑完选股后上传 COS，SCF 从 COS 读。

环境变量：
- COS_SECRET_ID / COS_SECRET_KEY  API 密钥
- COS_BUCKET                       存储桶名（如 stocks-master-1250000000）
- COS_REGION                       地域（如 ap-guangzhou）

未配置时所有函数降级返回 False/None，不影响本地流程。
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

logger = logging.getLogger("smcore.storage.cos")


def _cos_configured() -> bool:
    return all(
        os.getenv(k) for k in ("COS_SECRET_ID", "COS_SECRET_KEY", "COS_BUCKET", "COS_REGION")
    )


def _get_client():
    """获取 COS 客户端，未配置返回 None。"""
    if not _cos_configured():
        return None
    try:
        from qcloud_cos import CosConfig, CosS3Client
    except ImportError:
        try:
            from cossdk import CosConfig, CosS3Client
        except ImportError:
            logger.warning("COS SDK 未安装，跳过云端存储")
            return None
    config = CosConfig(
        Region=os.getenv("COS_REGION"),
        SecretId=os.getenv("COS_SECRET_ID"),
        SecretKey=os.getenv("COS_SECRET_KEY"),
        Scheme="https",
    )
    return CosS3Client(config)


def upload_file(local_path: str | Path, remote_key: str) -> bool:
    """上传文件到 COS。未配置或失败返回 False。"""
    client = _get_client()
    if client is None:
        return False
    local_path = Path(local_path)
    if not local_path.exists():
        return False
    try:
        client.upload_file(
            Bucket=os.getenv("COS_BUCKET"),
            Key=remote_key,
            LocalFilePath=str(local_path),
        )
        logger.info("已上传 COS: %s -> %s", local_path.name, remote_key)
        return True
    except Exception as e:
        logger.warning("COS 上传失败: %s", e)
        return False


def download_file(remote_key: str, local_path: str | Path) -> bool:
    """从 COS 下载文件。未配置或失败返回 False。"""
    client = _get_client()
    if client is None:
        return False
    local_path = Path(local_path)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        client.download_file(
            Bucket=os.getenv("COS_BUCKET"),
            Key=remote_key,
            DestFilePath=str(local_path),
        )
        return True
    except Exception as e:
        logger.warning("COS 下载失败: %s", e)
        return False


def list_objects(prefix: str) -> list[str]:
    """列出指定前缀下的对象 key。未配置返回空列表。"""
    client = _get_client()
    if client is None:
        return []
    try:
        resp = client.list_objects(
            Bucket=os.getenv("COS_BUCKET"),
            Prefix=prefix,
        )
        contents = resp.get("Contents") or []
        return [item["Key"] for item in contents if "Key" in item]
    except Exception as e:
        logger.warning("COS 列举失败: %s", e)
        return []


def get_latest_key(prefix: str) -> Optional[str]:
    """获取指定前缀下最新的对象 key（按名字排序，适合日期命名文件）。"""
    keys = list_objects(prefix)
    if not keys:
        return None
    return sorted(keys)[-1]


def download_latest(prefix: str, local_dir: str | Path) -> Optional[Path]:
    """下载指定前缀下最新文件到本地目录，返回本地路径。无则 None。"""
    key = get_latest_key(prefix)
    if not key:
        return None
    local_path = Path(local_dir) / Path(key).name
    if download_file(key, local_path):
        return local_path
    return None
