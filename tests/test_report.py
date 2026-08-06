"""测试空清单占位落盘与日报文本落盘。"""
from pathlib import Path

import pandas as pd
import pytest

from smcore.strategy import report


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(report, "STOCK_DATA_DIR", tmp_path)
    return tmp_path


def test_empty_no_placeholder_returns_none(data_dir):
    df = pd.DataFrame()
    assert report.save_action_list(df, "20260805", placeholder_when_empty=False) is None
    # 不应落盘任何文件
    assert not (data_dir / "Daily-Action-List-20260805.csv").exists()


def test_empty_placeholder_writes_header_only(data_dir):
    df = pd.DataFrame()
    path = report.save_action_list(df, "20260805", placeholder_when_empty=True)
    assert path is not None
    f = data_dir / "Daily-Action-List-20260805.csv"
    assert f.exists()
    back = pd.read_csv(f)
    assert back.empty
    assert list(back.columns) == report.ACTION_LIST_COLUMNS


def test_empty_placeholder_keeps_df_columns(data_dir):
    df = pd.DataFrame(columns=["a", "b"])
    path = report.save_action_list(df, "20260805", placeholder_when_empty=True)
    back = pd.read_csv(path)
    assert list(back.columns) == ["a", "b"]


def test_empty_report_written(data_dir):
    p = report.save_action_report("20260805", "## 今日操作清单\n- 无候选")
    assert p is not None
    txt = (data_dir / "Daily-Action-List-20260805.md").read_text(encoding="utf-8")
    assert "无候选" in txt


def test_report_empty_text_no_write(data_dir):
    assert report.save_action_report("20260805", "") is None
    assert not (data_dir / "Daily-Action-List-20260805.md").exists()


def test_nonempty_unaffected(data_dir):
    df = pd.DataFrame([{"股票代码": "000001", "股票名称": "平安银行"}])
    path = report.save_action_list(df, "20260805")
    back = pd.read_csv(path)
    assert len(back) == 1


def test_run_strategy_fusion_placeholder(data_dir, monkeypatch):
    from smcore import selection as selection_mod

    empty = pd.DataFrame()
    rep = "## 今日操作清单\n- 无候选"
    monkeypatch.setattr(selection_mod, "fuse_signals", lambda *a, **k: (empty, rep))
    out = selection_mod.run_strategy_fusion("20260805")
    assert out["count"] == 0
    assert out["placeholder"] is True
    assert out["saved_path"] is not None
    assert out["report_path"] is not None
    assert (data_dir / "Daily-Action-List-20260805.csv").exists()
    assert (data_dir / "Daily-Action-List-20260805.md").exists()
