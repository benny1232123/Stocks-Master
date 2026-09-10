"""反馈回路修复回归测试（2026-09-09）。

锁定的三件事，都是「系统看起来在跑、实际没生效」的隐性故障：

1. **regime 快照不被回放污染**：历史回放按日期升序跑完后，最后一个历史信号日
   不得覆盖 ``regime-latest.json``（实测曾把快照顶到 20260729，实际已 20260909）。
2. **归因口径可切换**：``CONFIG["edge"]["source"]`` 能切 universe / backtest / blend，
   且 universe 口径在「候选全集」上统计（样本量显著大于被截断的成交子集）。
3. **样本置信度折扣**：小样本高胜率不得把权重顶到 50%+（实测 boll n=6/胜率100%
   曾拿到 58%）。

全部不依赖网络：K 线与基准均用 monkeypatch 注入。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import smcore.strategy.adaptive_weights as aw  # noqa: E402
from smcore.strategy.adaptive_weights import (  # noqa: E402
    compute_edge,
    compute_universe_edge,
    save_regime_snapshot,
)


# ── 1. regime 快照污染防治 ──────────────────────────────────────────────


def test_replay_snapshot_does_not_overwrite_latest(tmp_path, monkeypatch):
    """回放写 regime_history/<date>.json，绝不覆盖 regime-latest.json。"""
    monkeypatch.setattr(aw, "STOCK_DATA_DIR", tmp_path)

    save_regime_snapshot({"date": "20260909", "regime": "下行防御"}, source="live")
    save_regime_snapshot({"date": "20260729", "regime": "震荡轮动"}, source="replay")

    latest = json.loads((tmp_path / "regime-latest.json").read_text(encoding="utf-8"))
    assert latest["date"] == "20260909", "回放覆盖了最新快照"
    assert latest["source"] == "live"
    assert "generated_at" in latest

    hist = tmp_path / "regime_history" / "20260729.json"
    assert hist.exists(), "回放快照应落到 regime_history/"


def test_snapshot_write_failure_is_visible(tmp_path, monkeypatch, capsys):
    """写入失败必须打告警，不能静默 pass（此前失败完全不可见）。"""
    monkeypatch.setattr(aw, "STOCK_DATA_DIR", tmp_path / "missing_dir")
    monkeypatch.setattr(aw, "datetime", _ExplodingDatetime())

    assert save_regime_snapshot({"date": "20260909"}, source="live") is None
    assert "[adaptive_weights] WARN" in capsys.readouterr().err


class _ExplodingDatetime:
    """让 datetime.now() 抛错，模拟快照构造阶段失败。"""

    @staticmethod
    def now():
        raise RuntimeError("boom")


# ── 2. 归因口径可切换 ───────────────────────────────────────────────────


def _fake_dal(tmp_path, n_days=40, per_day=8):
    """造 n_days 个信号日，每天 per_day 条候选，策略轮换。"""
    strats = ["Boll", "Theme", "CCTV", "Momentum", "Relativity"]
    dates = pd.bdate_range("2026-06-01", periods=n_days + 15).strftime("%Y%m%d")
    for d in dates[:n_days]:
        rows = [
            {"股票代码": f"{600000 + i:06d}", "股票名称": "X", "来源策略": strats[i % len(strats)]}
            for i in range(per_day)
        ]
        pd.DataFrame(rows).to_csv(tmp_path / f"Daily-Action-List-{d}.csv", index=False, encoding="utf-8-sig")
    return list(dates[:n_days])


def _fake_kline(code):
    """每只票给一根足够长的平价序列，前向收益恒为 0（便于断言样本量）。"""
    dates = pd.bdate_range("2026-05-01", periods=120).strftime("%Y-%m-%d")
    return pd.DataFrame(
        {"date": dates, "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "volume": 1, "amount": 1e8}
    )


def test_universe_edge_covers_full_candidate_set(tmp_path, monkeypatch):
    """universe 口径统计的是候选全集，不是被资金截断的成交子集。"""
    monkeypatch.setattr(aw, "STOCK_DATA_DIR", tmp_path)
    _fake_dal(tmp_path, n_days=40, per_day=8)
    monkeypatch.setattr(aw, "_benchmark_forward_ret", lambda *a, **k: 0.0)

    import smcore.data.kline as kl

    monkeypatch.setattr(kl, "read_kline_cache", _fake_kline, raising=False)

    edge = compute_universe_edge(window=20, hold_days=10)
    meta = edge.pop("__meta__")

    # 每天 8 条候选，策略轮换 → 每策略约 1-2 条/天；窗口 20 天 +10 个前瞻日
    assert meta["source"] == "universe"
    assert meta["signal_days"] == 30, "应多取 hold_days 个信号日，避免有效样本被未来数据不足吃掉"
    total_n = sum(v["n"] for v in edge.values())
    assert total_n >= 20 * 8 * 0.8, f"候选全集样本量过小: {total_n}"


def test_edge_source_switch(monkeypatch):
    """CONFIG['edge']['source'] 能切换归因口径。"""
    calls = []

    def _fake_universe(**kw):
        calls.append("universe")
        return {s: {"n": 5, "edge": 1.0, "avg_return": 1.0, "win_rate": 60.0} for s in aw.ALL_STRATEGIES}

    def _fake_backtest(w):
        calls.append("backtest")
        return {s: {"n": 3, "edge": 0.5, "avg_return": 0.5, "win_rate": 50.0} for s in aw.ALL_STRATEGIES}

    monkeypatch.setattr(aw, "compute_universe_edge", _fake_universe)
    monkeypatch.setattr(aw, "compute_strategy_edge", _fake_backtest)

    base = json.loads(json.dumps(aw.CONFIG))
    try:
        aw.CONFIG["edge"]["source"] = "backtest"
        compute_edge()
        assert calls == ["backtest"], "source=backtest 未走回测口径"

        calls.clear()
        aw.CONFIG["edge"]["source"] = "universe"
        compute_edge()
        assert calls == ["universe"], "source=universe 未走全集口径"

        calls.clear()
        aw.CONFIG["edge"]["source"] = "blend"
        out = compute_edge()
        assert set(calls) == {"backtest", "universe"}, "blend 应同时取两种口径"
        # blend 后 edge 应为加权值 (0.5*0.5 + 0.5*1.0) = 0.75
        assert abs(out["boll"]["edge"] - 0.75) < 1e-9
    finally:
        aw.CONFIG.clear()
        aw.CONFIG.update(base)


# ── 3. 样本置信度折扣 ───────────────────────────────────────────────────


def test_thin_sample_cannot_dominate_weight(monkeypatch):
    """小样本高胜率不得把权重顶到 50%+。"""
    edge = {
        "boll": {"n": 6, "edge": 5.58, "avg_return": 5.58, "win_rate": 100.0},
        "theme": {"n": 53, "edge": -0.31, "avg_return": -0.31, "win_rate": 47.2},
        "relativity": {"n": 17, "edge": -0.38, "avg_return": -0.38, "win_rate": 29.4},
        "momentum": {"n": 58, "edge": 0.84, "avg_return": 0.84, "win_rate": 32.8},
        "cctv": {"n": 82, "edge": -1.08, "avg_return": -1.08, "win_rate": 48.8},
    }
    w = aw.adaptive_weights(edge)
    top = max(w.values())
    assert top < 50, f"小样本策略权重过高: {w}"
    assert max(w.values()) == w["boll"], "boll edge 最高，应仍是第一但不可独大"


def test_confidence_discount_monotonic_in_n():
    """样本越多，置信度折扣越接近 1；n >= min_n_confident 后不再折扣。"""
    min_n = 30
    edge_tpl = {s: {"n": 0, "edge": 0.0, "avg_return": 0.0, "win_rate": 50.0} for s in aw.ALL_STRATEGIES}

    def _w_for(n):
        e = {s: dict(v) for s, v in edge_tpl.items()}
        e["boll"] = {"n": n, "edge": 5.0, "avg_return": 5.0, "win_rate": 100.0}
        return aw.adaptive_weights(e)["boll"]

    w_small = _w_for(5)
    w_mid = _w_for(15)
    w_big = _w_for(60)
    w_huge = _w_for(200)
    assert w_small < w_mid < w_big, "样本增加时权重应单调上升"
    assert w_big == w_huge, "n 超过 min_n_confident 后折扣应封顶为 1"
