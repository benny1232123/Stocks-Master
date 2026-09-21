"""止损口径**单一真源**守卫（2026-09-21）。

背景（事故记录）：
  清单/网站/`PositionMonitor` 的逐只止损来自 fusion 写入 DAL 的 `stop_pct` 列，
  其唯一计算处是 `smcore/strategy/boll_levels.py::_compute_boll_levels`
  = `clamp(stop_pct_vol_mult(2.5) × 日σ, stop_pct_min(4%), stop_pct_max(12%))`。

  但 `scripts/daily_backtest.py` 曾就地**重算**成 `clamp(8 × 日σ, 6%, 15%)` 并覆盖该列 →
  实测 120/120 笔两套止损都不相等（中位 5.45% vs 15.00%，平均差 +4.74pp），
  即「你在清单上看到的止损」不是「回测/监控真正执行的止损」。

  本测试钉住修复后的不变式：
    ① 回测默认（source="dal"）**原样透传** DAL 值，缺失才按同一 boll_levels 口径补算；
    ② 旧口径只存在于显式 `source="legacy"` / `BACKTEST_STOP_SOURCE=legacy` 下；
    ③ boll 段的三个参数仍是唯一配置入口，且 fusion / boll_levels 的接线未断。
  再次分叉时这里应立刻变红。
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

from scripts.daily_backtest import (  # noqa: E402
    VOL_STOP_LEGACY_BOUNDS,
    VOL_STOP_MULT,
    _resolve_stops,
)


def _lv(mapping):
    return lambda code: mapping.get(code, {})


# ── ① 默认源 = dal：DAL 值原样透传 ──────────────────────────────────────────
def test_dal_passthrough_verbatim():
    dal = [0.04, 0.1019, None, float("nan"), "0.0978"]
    codes = ["000001", "600000", "300750", "002284", "603187"]
    fallback = {"300750": {"stop_pct": 0.077}, "002284": {"stop_pct": 0.09}}
    out = _resolve_stops(dal, codes, _lv(fallback), source="dal")
    assert out == [0.04, 0.1019, 0.077, 0.09, 0.0978]


def test_dal_missing_everywhere_returns_none():
    out = _resolve_stops([None, None], ["A", "B"], _lv({}), source="dal")
    assert out == [None, None]


def test_dal_source_never_applies_vol_multiplier():
    """dal 源下即使 vol20 存在，也不得用 VOL_STOP_MULT 重算（这是本次修复的核心）。"""
    lv = {"A": {"vol20": 0.05, "stop_pct": 0.11}}
    assert _resolve_stops([0.05], ["A"], _lv(lv), source="dal") == [0.05]
    assert _resolve_stops([None], ["A"], _lv(lv), source="dal") == [0.11]
    assert _resolve_stops(None, ["A"], _lv(lv), source="dal") == [0.11]


# ── ② 旧口径只在显式 legacy 下生效，且上下限不变 ────────────────────────────
def test_legacy_clamps_to_old_bounds():
    lo, hi = VOL_STOP_LEGACY_BOUNDS
    assert (lo, hi) == (0.06, 0.15)
    lv = {"LO": {"vol20": 0.005}, "MID": {"vol20": 0.01}, "HI": {"vol20": 0.05}}
    out = _resolve_stops(None, ["LO", "MID", "HI"], _lv(lv), source="legacy")
    assert out[0] == lo                       # 8×0.005=0.04 → 抬到下限
    assert abs(out[1] - 0.08) < 1e-12         # 8×0.01 = 0.08 中间值
    assert out[2] == hi                       # 8×0.05=0.40 → 压到上限
    assert VOL_STOP_MULT == 8.0


def test_legacy_without_vol_returns_none():
    assert _resolve_stops(None, ["A"], _lv({}), source="legacy") == [None]


def test_legacy_is_not_the_default_env_reading():
    """默认来源必须是 dal；只有显式设置才走 legacy。"""
    src = (ROOT / "scripts" / "daily_backtest.py").read_text(encoding="utf-8")
    assert 'os.environ.get("BACKTEST_STOP_SOURCE", "dal")' in src
    assert "VOL_STOP_LEGACY_BOUNDS" in src


# ── ③ 单一真源的接线：boll_levels 计算 → fusion 写 DAL → 监控/回测读 ─────────
def test_boll_stop_config_is_single_source():
    cfg = json.loads(
        (ROOT / "smcore" / "strategy" / "risk_config.json").read_text(encoding="utf-8")
    )
    b = cfg["boll"]
    assert b["stop_pct_vol_mult"] == 2.5
    assert b["stop_pct_min"] == 0.04
    assert b["stop_pct_max"] == 0.12


def test_boll_levels_reads_that_config():
    src = (ROOT / "smcore" / "strategy" / "boll_levels.py").read_text(encoding="utf-8")
    for key in ("stop_pct_vol_mult", "stop_pct_min", "stop_pct_max"):
        assert key in src, f"boll_levels 未使用配置项 {key}"


def test_fusion_writes_stop_pct_into_dal_row():
    src = (ROOT / "smcore" / "strategy" / "fusion.py").read_text(encoding="utf-8")
    assert 'row["stop_pct"] = levels.get("stop_pct")' in src


def test_daily_backtest_carries_dal_stop_into_sub():
    src = (ROOT / "scripts" / "daily_backtest.py").read_text(encoding="utf-8")
    assert 'sub["stop_pct"] = pd.to_numeric(df["stop_pct"]' in src
    assert "_resolve_stops(" in src


def test_summary_records_which_stop_source_was_used():
    src = (ROOT / "scripts" / "daily_backtest.py").read_text(encoding="utf-8")
    assert 'summary["stop_mode"]' in src
