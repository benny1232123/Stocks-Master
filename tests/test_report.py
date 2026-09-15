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


def test_build_report_text_factor_type_rollup_and_column():
    """2026-09-15 因子类型分类：非空清单须同时含「因子类型贡献度」归并小节
    与明细表「因子类型」列，且值由来源策略正确推导。"""
    df = pd.DataFrame([
        {"股票代码": "000001", "股票名称": "平安银行", "命中策略数": 2,
         "综合评分": 70, "建议仓位%": 5, "止损价(下轨)": 10.0, "止盈价(上轨)": 12.0,
         "来源策略": "Boll/Momentum"},
        {"股票代码": "600000", "股票名称": "浦发银行", "命中策略数": 1,
         "综合评分": 60, "建议仓位%": 4, "止损价(下轨)": 9.0, "止盈价(上轨)": 11.0,
         "来源策略": "CCTV"},
    ])
    # n_boll=2, n_momentum=3, n_theme=1, n_cctv=1 → 归并：反转·均值回归2 / 动量3 / 题材·事件2
    text = report._build_report_text(
        df, "20260911",
        n_boll=2, n_relativity=0, n_theme=1, n_cctv=1, n_momentum=3,
    )
    assert "### 因子类型贡献度" in text
    assert "反转·均值回归: 2 只" in text
    assert "动量: 3 只" in text
    assert "题材·事件: 2 只" in text
    # 明细表含因子类型列，且按来源策略推导
    assert "| 代码 | 名称 | 命中 | 因子类型 |" in text
    assert "| 000001 | 平安银行 | 2 | 反转·均值回归/动量 |" in text
    assert "| 600000 | 浦发银行 | 1 | 题材·事件 |" in text


def test_build_report_empty_still_has_factor_type_section():
    """空清单路径（df.empty）也必须带「因子类型贡献度」小节，保持两段报告结构一致。"""
    text = report._build_report_text(
        pd.DataFrame(), "20260911",
        n_boll=0, n_relativity=0, n_theme=0, n_cctv=0, n_momentum=0,
    )
    assert "### 因子类型贡献度" in text
