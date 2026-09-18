"""回归测试：zoo_factor_strategy 的「信号日覆盖度门控」。

2026-09-17 A 批空产出事故的真正成因是信号日有效截面从 4363 塌到 1，旧告警只数
「文件是否存在」把空表当正常，CI 仍报 success。上游 scripts/prepull_klines.py 已做
权威门控（截面<1000 直接失败）；本测试锁定 zoo_factor_strategy.py 自身的防御性双保险
——即便 prepull 被绕过，截面塌缩/信号日缺失时也必须 fail-loud（return 1）而非静默产空表。

全程用 mock 的本地矩阵，无网络、无真实 K 线缓存依赖。
"""
from __future__ import annotations

import importlib.util
import sys
import unittest.mock as mock
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "zoo_factor_strategy_test", str(ROOT / "scripts" / "zoo_factor_strategy.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _make_matrices(n_codes: int = 400):
    codes = [f"{i:06d}" for i in range(n_codes)]
    dates = pd.date_range("2026-09-15", periods=2)
    close = pd.DataFrame(10.0, index=dates, columns=codes)
    mats = {c: close for c in ("close", "high", "low", "open", "volume", "amount")}
    mats["volume"] = close * 1e6
    mats["amount"] = close * 1e5
    return mats


def test_coverage_gate_collapsed_fails():
    zf = _load_module()
    mats = _make_matrices()
    with mock.patch.object(zf.fe, "load_matrices", lambda **k: mats), \
         mock.patch.object(zf.fz, "compute_factor",
                           lambda ctx, cand: pd.DataFrame(1.0, index=mats["close"].index,
                                                          columns=mats["close"].columns)), \
         mock.patch.object(zf.fe, "signal_day_coverage", lambda c, d: (1, 400)):
        sys.argv = ["zf", "--date", "20260916", "--top", "5"]
        assert zf.main() == 1


def test_coverage_gate_healthy_passes():
    zf = _load_module()
    mats = _make_matrices()
    with mock.patch.object(zf.fe, "load_matrices", lambda **k: mats), \
         mock.patch.object(zf.fz, "compute_factor",
                           lambda ctx, cand: pd.DataFrame(1.0, index=mats["close"].index,
                                                          columns=mats["close"].columns)), \
         mock.patch.object(zf.fe, "signal_day_coverage", lambda c, d: (400, 400)):
        sys.argv = ["zf", "--date", "20260916", "--top", "5"]
        assert zf.main() == 0


def test_coverage_gate_missing_day_fails():
    zf = _load_module()
    mats = _make_matrices()
    with mock.patch.object(zf.fe, "load_matrices", lambda **k: mats), \
         mock.patch.object(zf.fz, "compute_factor",
                           lambda ctx, cand: pd.DataFrame(1.0, index=mats["close"].index,
                                                          columns=mats["close"].columns)), \
         mock.patch.object(zf.fe, "signal_day_coverage", lambda c, d: (0, 400)):
        sys.argv = ["zf", "--date", "20990101", "--top", "5"]
        assert zf.main() == 1
