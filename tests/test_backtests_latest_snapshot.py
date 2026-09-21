"""`/api/backtests/latest` 空快照守卫 + export 空载荷守卫（2026-09-21）。

事故记录：
  `export_web_data.py` 会把端点返回的「查不到产物」空结果
  （`{"latest": null, "preview": {"rows": [], "columns": []}}`）也写成快照，而端点原先
  「快照存在即返回」→ **快照自我毒化**：之后每次都命中空快照，永远返回 null、不可能自愈。
  另外该端点的候选文件家族只有退役的 `Signal-Backtest-*` / `Trade-Backtest-*` /
  `*-portfolio-summary`（远端 0 个文件），漏了现行产物 `Multi-Backtest-*-summary.csv`。

本测试钉住修复后的三条不变式：
  ① `latest` 为空的快照不算命中（继续走实时路径）；
  ② 非空快照仍然短路（保持快照优先的性能收益）；
  ③ 候选家族必须包含现行产物；export 侧不得写空载荷。
"""
import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

main_mod = importlib.import_module("backend.main")
exp = importlib.import_module("scripts.export_web_data")

EMPTY = {"latest": None, "preview": {"rows": [], "columns": []}}


_FAKE_NAME = "Multi-Backtest-20260918-summary.csv"


class _FakeArtifact:
    """端点返回 latest.__dict__，故必须是**实例属性**（类属性不会进 __dict__）。"""

    def __init__(self):
        self.name = _FAKE_NAME
        self.path = "stock_data/" + _FAKE_NAME
        self.modified_at = 1
        self.size = 123


def test_null_snapshot_is_treated_as_miss(monkeypatch):
    """毒化的空快照不能赢——必须继续走实时路径拿到真实产物。"""
    monkeypatch.setattr(main_mod, "_web_snapshot", lambda name: dict(EMPTY))
    monkeypatch.setattr(main_mod, "find_latest_file_any", lambda pats: _FakeArtifact())
    monkeypatch.setattr(main_mod, "preview_csv",
                        lambda p, *a, **k: {"rows": [["1"]], "columns": ["c"]})
    out = main_mod.latest_backtest()
    assert out["latest"] is not None, "空的 latest 快照把实时路径短路了（自我毒化回归）"
    assert out["latest"]["name"] == _FAKE_NAME


def test_empty_snapshot_and_no_artifact_returns_contract(monkeypatch):
    monkeypatch.setattr(main_mod, "_web_snapshot", lambda name: {"latest": None})
    monkeypatch.setattr(main_mod, "find_latest_file_any", lambda pats: None)
    assert main_mod.latest_backtest() == EMPTY


def test_nonempty_snapshot_still_short_circuits(monkeypatch):
    """非空快照仍须短路（快照优先的性能设计不能被破坏）。"""
    good = {"latest": {"name": "x.csv"}, "preview": {"rows": [], "columns": []}}
    monkeypatch.setattr(main_mod, "_web_snapshot", lambda name: dict(good))

    def _boom(pats):
        pytest.fail("非空快照下不应走到实时路径")

    monkeypatch.setattr(main_mod, "find_latest_file_any", _boom)
    assert main_mod.latest_backtest()["latest"]["name"] == "x.csv"


def test_current_product_family_is_searched(monkeypatch):
    """候选家族必须包含现行管线产物（Multi-Backtest-*-summary.csv）。"""
    seen = {}

    def _f(pats):
        seen["pats"] = list(pats)
        return None

    monkeypatch.setattr(main_mod, "_web_snapshot", lambda name: None)
    monkeypatch.setattr(main_mod, "find_latest_file_any", _f)
    main_mod.latest_backtest()
    assert "Multi-Backtest-*-summary.csv" in seen["pats"]
    # 退役家族保留（兼容遗留产物目录），但不能是唯一候选
    assert len(seen["pats"]) >= 2


def test_export_is_empty_payload_rules():
    assert exp._is_empty_payload({"latest": None, "preview": {}}) is True
    assert exp._is_empty_payload({"latest": {}, "preview": {}}) is True
    assert exp._is_empty_payload({"latest": {"name": "a.csv"}}) is False
    assert exp._is_empty_payload({"macro": 1}) is False      # 无 latest 键 → 不受影响
    assert exp._is_empty_payload({"latest": None, "macro": 1}) is True
    assert exp._is_empty_payload([1, 2]) is False
    assert exp._is_empty_payload(None) is False


def test_export_loop_skips_empty_payload():
    """export 主循环必须真的调用该守卫，否则毒丸仍会被写盘。"""
    src = (ROOT / "scripts" / "export_web_data.py").read_text(encoding="utf-8")
    assert "_is_empty_payload(data)" in src
    assert "continue" in src.split("_is_empty_payload(data)")[1][:200]
