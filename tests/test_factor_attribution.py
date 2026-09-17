"""离线单测：scripts/factor_attribution.py 的归因核心逻辑。

只测纯函数（代码归一 / 主因子类型 / 分组统计），不读真实数据目录：
真实数据依赖 CI 产物，沙箱里不可得；而这几个函数正是「归因对不对」的关键。
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.factor_attribution import (  # noqa: E402
    _norm_code,
    primary_factor_type,
    summarize,
)


def test_norm_code_pads_leading_zeros():
    """DAL 里被写成整数的代码（000001→1）必须补零，否则 join 大面积失败。"""
    assert _norm_code("1") == "000001"
    assert _norm_code(1) == "000001"
    assert _norm_code("600426") == "600426"
    assert _norm_code("600426.SH") == "600426"
    assert _norm_code("600426.0") == "600426"


def test_primary_factor_type_uses_first_strategy():
    """多策略取首个作为主类型，保证不重复计数（A 批因子标签/类型）。"""
    assert primary_factor_type("PVCorr20/Vol20") == "量价相关"
    assert primary_factor_type("Illiq20") == "非流动性"
    assert primary_factor_type("Skew20") == "收益偏度"
    assert primary_factor_type("CVAmt20") == "成交稳定"
    assert primary_factor_type("") == "其他"


def _row(src, rp, pos):
    return {"day": "20260901", "code": "000001", "source": src,
            "return_pct": rp, "pos_pct": pos}


def test_summarize_groups_and_contribution():
    rows = [_row("Illiq20", 10.0, 10.0), _row("PVCorr20", -10.0, 10.0)]
    out = summarize(rows, lambda r: primary_factor_type(r["source"]))
    assert out["非流动性"]["n"] == 1
    assert out["非流动性"]["mean"] == 10.0
    assert out["非流动性"]["win_rate"] == 100.0
    # 贡献 = 收益×仓位 / 总仓位 = 10*10/20 = 5.0
    assert out["非流动性"]["contrib"] == 5.0
    assert out["量价相关"]["contrib"] == -5.0


def test_summarize_win_rate_and_payoff():
    rows = [_row("Illiq20", 5.0, 1.0), _row("Illiq20", -2.0, 1.0), _row("Illiq20", 1.0, 1.0)]
    out = summarize(rows, lambda r: primary_factor_type(r["source"]))
    st = out["非流动性"]
    assert st["n"] == 3
    assert st["win_rate"] == pytest.approx(66.7, abs=0.1)
    assert st["avg_win"] == pytest.approx(3.0)
    assert st["avg_loss"] == -2.0
    assert st["payoff"] == pytest.approx(1.5)
