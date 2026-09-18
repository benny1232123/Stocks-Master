"""回测策略归因标签校验：A 批 DAL 不再被误标成退役策略。

背景（2026-09-18）
------------------
`scripts/daily_backtest.py::derive_strategies` 原先只认 RETIRED 名
（boll/relativity/theme/cctv/momentum），A 批 DAL（来源策略="PVCorr20/CVAmt20"）
经它时全部 token 落空 → 退化成 "boll,relativity,theme,cctv,momentum"，
回测面板把 A 批票错归到已退役的死策略。

修复后（同文件）：
- A 批因子 id / 展示标签 → 正确 A 批标签（见 factor_types.STRATEGY_LABEL）；
- 遗留名 → 原展示标签（兼容 2026-09-17 重构前生成的历史 DAL）；
- 空来源策略 → 空串，绝不退化成「全部退役策略」。

本测试锁住上述三种口径，防止回归。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.daily_backtest import derive_strategies  # noqa: E402


def test_abatch_dal_labeled_as_abatch():
    s = pd.Series(["PVCorr20/CVAmt20", "pvcorr60/Skew20", "VRatio20_120"])
    assert derive_strategies(s) == "CVAmt20,PVCorr20,PVCorr60,Skew20,VRatio20_120"


def test_legacy_dal_labeled_as_legacy():
    s = pd.Series(["Boll/Theme", "CCTV", "Momentum"])
    assert derive_strategies(s) == "Boll,CCTV,Momentum,Theme"


def test_mixed_token_each_resolved():
    s = pd.Series(["PVCorr20/Theme"])
    assert derive_strategies(s) == "PVCorr20,Theme"


def test_empty_no_retired_fallback():
    # 关键不变量：空来源策略必须返回空串，绝不能退化成「全部退役策略」
    assert derive_strategies(pd.Series([None, ""])) == ""
