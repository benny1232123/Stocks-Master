#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""P2-4 实验台账测试（全合成，不依赖联网 / 真实文件）。"""
from __future__ import annotations

import json

import pytest

from smcore.strategy import experiment_ledger as el


@pytest.fixture
def tmp_ledger(tmp_path, monkeypatch):
    p = tmp_path / "experiment_ledger.jsonl"
    monkeypatch.setattr(el, "LEDGER_PATH", p)
    return p


def test_append_and_load_roundtrip(tmp_ledger):
    eid = el.append_entry({"title": "t1", "author": "me", "signal_date": "20260807"})
    assert eid is not None
    recs = el.load_ledger()
    assert len(recs) == 1
    assert recs[0]["title"] == "t1"
    assert recs[0]["id"] == eid
    assert recs[0]["outcome"] == "pending"  # 默认


def test_id_increments_same_day(tmp_ledger):
    a = el.append_entry({"title": "a"})
    b = el.append_entry({"title": "b"})
    assert a != b
    # ID 形如 EXP-YYYYMMDD-NNN
    assert a.startswith("EXP-")
    assert int(a.split("-")[-1]) + 1 == int(b.split("-")[-1])


def test_load_skips_corrupt_lines(tmp_ledger):
    tmp_ledger.write_text('{"id":"EXP-1","title":"ok"}\nNOT JSON\n', encoding="utf-8")
    recs = el.load_ledger()
    assert len(recs) == 1
    assert recs[0]["title"] == "ok"


def test_load_missing_returns_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(el, "LEDGER_PATH", tmp_path / "nope.jsonl")
    assert el.load_ledger() == []


def test_enabled_env_off_returns_none(tmp_path, monkeypatch):
    monkeypatch.setattr(el, "LEDGER_PATH", tmp_path / "x.jsonl")
    monkeypatch.setenv("STOCKS_LEDGER", "0")
    assert el.append_entry({"title": "x"}) is None


def test_record_experiment_convenience(tmp_ledger):
    eid = el.record_experiment(
        title="调高主题权重",
        hypothesis="主题近期 alpha 更强",
        signal_date="20260807",
        config_before={"w_theme": 0.3},
        config_after={"w_theme": 0.4},
        metrics_before={"sharpe": 1.1},
        metrics_after={"sharpe": 1.3},
        outcome="pending",
        author="manual",
        notes="月度重验前试探",
    )
    recs = el.load_ledger()
    assert len(recs) == 1
    r = recs[0]
    assert r["config_before"] == {"w_theme": 0.3}
    assert r["config_after"] == {"w_theme": 0.4}
    assert r["metrics_after"]["sharpe"] == 1.3
    assert r["outcome"] == "pending"


def test_record_calibration_maps_robust_to_outcome(tmp_ledger):
    rec = {
        "current": {"shrinkage": 0.4, "FLOOR": 2.0},
        "recommended": {"shrinkage": 0.2, "FLOOR": 1.0},
        "improvement_pp": 3.5,
        "robust": True,
        "checks": {"improve_ok": True, "monotonic": True, "stable_ok": True},
        "current_report": {"adaptive_total_pct": 12.3, "equal_total_pct": 9.1},
    }
    eid = el.record_calibration(rec, signal_date="20260807",
                                author="walk_forward_validator")
    assert eid is not None
    r = el.load_ledger()[0]
    assert r["outcome"] == "pending"  # robust=True → 待采纳
    assert r["config_before"]["shrinkage"] == 0.4
    assert r["config_after"]["shrinkage"] == 0.2
    assert r["metrics_before"]["adaptive_total_pct"] == 12.3
    assert r["metrics_after"]["improvement_pp"] == 3.5

    # robust=False → rejected
    rec2 = dict(rec, robust=False, improvement_pp=-1.0)
    el.record_calibration(rec2, signal_date="20260807")
    recs = el.load_ledger()
    assert recs[1]["outcome"] == "rejected"


def test_summarize_counts_and_recent(tmp_ledger):
    el.record_experiment("a", outcome="adopted", signal_date="20260801")
    el.record_experiment("b", outcome="rejected", signal_date="20260802")
    el.record_experiment("c", outcome="pending", signal_date="20260803")
    s = el.summarize_ledger(recent=2)
    assert s["total"] == 3
    assert s["counts"]["adopted"] == 1
    assert s["counts"]["rejected"] == 1
    assert s["counts"]["pending"] == 1
    assert len(s["recent"]) == 2  # 仅最近 2 条
    assert s["recent"][-1]["title"] == "c"


def test_format_ledger_report_markdown(tmp_ledger):
    el.record_experiment("a", outcome="adopted", signal_date="20260801",
                         config_after={"x": 1})
    md = el.format_ledger_report()
    assert "实验台账汇总" in md
    assert "EXP-" in md
    assert "采纳=1" in md


def test_sanitize_invalid_outcome_defaults(tmp_ledger):
    eid = el.append_entry({"title": "x", "outcome": "bogus"})
    r = el.load_ledger()[0]
    assert r["outcome"] == "pending"
