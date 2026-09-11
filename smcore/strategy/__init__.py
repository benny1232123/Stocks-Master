"""策略层 —— 信号融合等。

从 auto_notify_boll.py 巨石抽出，供两条主线复用。
原 allocation.py（硬编码 regime 权重表）已于 2026-09-12 删除：
零调用方，职责由 adaptive_weights（softmax+贝叶斯收缩）与
portfolio/risk_rules（风险中性化）接管。
"""
from __future__ import annotations

from .fusion import fuse_signals, save_action_list

__all__ = [
    "fuse_signals",
    "save_action_list",
]
