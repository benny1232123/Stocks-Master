"""基本面因子激活回归测试。

验证 factor_scoring 在 use_fundamentals=true 时确实把 quality/value/fundflow
三子因子并入「综合评分」增量，且缓存缺失/数据源不可用时降级为 0（中性、不崩溃）。

用本地伪 fundamental_cache 驱动，不依赖联网；pytest 用 Anaconda 跑。
"""
import sys
import json
from pathlib import Path

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.config.defaults import PROJECT_ROOT  # noqa: E402
from smcore.strategy import factor_scoring  # noqa: E402
from smcore.strategy import fundamental as fund_mod  # noqa: E402

CODES = ["000001", "600000", "000002"]
# 腾讯行情估值快照字段（新数据源：pe/pb/mkt_cap，单位亿元）
SPOT_COLS = ["代码", "pe", "pb", "mkt_cap"]
SPOT_ROWS = [
    ["000001", 6.0, 0.8, 3500.0],
    ["600000", 4.5, 0.6, 2800.0],
    ["000002", 12.0, 1.5, 2000.0],
]
# baostock 个股基本面：质量(roe/gross_margin) + 成长(revenue_growth) + 换手率 + 资金流量价(amount_20)
FUND = {
    "000001": {"roe": 15.0, "gross_margin": 40.0, "revenue_growth": 10.0, "turnover": 1.0, "amount_20": 1e9},
    "600000": {"roe": 11.0, "gross_margin": 35.0, "revenue_growth": 5.0, "turnover": 0.8, "amount_20": 8e8},
    "000002": {"roe": 8.0, "gross_margin": 25.0, "revenue_growth": -2.0, "turnover": 1.5, "amount_20": 2e8},
}
CACHE_DIR = fund_mod.CACHE_DIR
SPOT_FILE = fund_mod.SPOT_FILE

FAKE_RAW = lambda code, as_of, window=20: {"mom20": 0.05, "mom60": 0.1, "vol": 0.2, "liq": 1e9}


def _seed_cache():
    import pandas as pd
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(SPOT_ROWS, columns=SPOT_COLS).to_csv(SPOT_FILE, index=False, encoding="utf-8-sig")
    for c, d in FUND.items():
        (CACHE_DIR / f"{c}.json").write_text(json.dumps(d), encoding="utf-8")


def _clear_cache():
    import ctypes
    for f in [SPOT_FILE] + [CACHE_DIR / f"{c}.json" for c in CODES]:
        try:
            if f.exists():
                ctypes.windll.kernel32.DeleteFileW(str(f).replace("/", "\\"))
        except Exception:
            pass


BASE = dict(
    enabled=True, w_momentum_20=0.0, w_momentum_60=0.0, w_rel_strength=0.0,
    w_volatility=0.0, w_liquidity=0.0, w_quality=0.5, w_value=0.5,
    w_fund_flow=0.4, scale=1.0, max_bonus=100.0,
)


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setattr(factor_scoring, "_raw_factors", FAKE_RAW)
    monkeypatch.setattr(factor_scoring, "_index_ret20", lambda *a, **k: 0.0)
    _seed_cache()
    yield
    _clear_cache()


def test_fundamentals_activate(env):
    as_of = "20260805"
    off = factor_scoring.compute_factor_scores(CODES, as_of, {**BASE, "use_fundamentals": False})
    on = factor_scoring.compute_factor_scores(CODES, as_of, {**BASE, "use_fundamentals": True})
    assert all(v == 0.0 for v in off.values())            # 价格权重全0 → 关闭时全0
    assert any(v != 0.0 for v in on.values())              # 开启后基本面因子确有贡献
    assert on["600000"] > on["000002"]                     # 低估值(600000)应优于高估值(000002)
    assert all(-100.0 <= v <= 100.0 for v in on.values())  # 仍在 clamp 区间内


def test_fundamentals_missing_cache_neutral(monkeypatch):
    # 缓存缺失且数据源不可用 → 因子降级为0，绝不抛异常
    monkeypatch.setattr(factor_scoring, "_raw_factors", FAKE_RAW)
    monkeypatch.setattr(factor_scoring, "_index_ret20", lambda *a, **k: 0.0)
    monkeypatch.setattr(fund_mod, "fetch_fundamentals_batch", lambda *a, **k: {c: None for c in CODES})
    _clear_cache()
    scores = factor_scoring.compute_factor_scores(CODES, "20260805", {**BASE, "use_fundamentals": True})
    assert all(v == 0.0 for v in scores.values())
