import importlib.util
from pathlib import Path

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parent / "Stock-Selection-CCTV-Sectors.py"
spec = importlib.util.spec_from_file_location("cctv_strategy", MODULE_PATH)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def test_merge_keywords_adds_without_duplicates():
    base = {"A": ["x", "y"]}
    overlay = {"A": ["y", "z"], "B": ["k"]}
    merged = mod._merge_keywords(base, overlay)
    assert merged["A"] == ["x", "y", "z"]
    assert merged["B"] == ["k"]


def test_confidence_tier_has_three_levels():
    assert mod._confidence_tier(25, 5, 3) == "高"
    assert mod._confidence_tier(11, 2, 1) == "中"
    assert mod._confidence_tier(3, 1, -1) == "观察"


def test_suggest_keyword_sector_returns_dataframe():
    emerging = pd.DataFrame([
        {"候选关键词": "人形机器人", "出现次数": 5},
        {"候选关键词": "低空旅游", "出现次数": 4},
    ])
    sector_keywords = {
        "机器人": ["机器人", "人形机器人"],
        "低空经济": ["低空经济", "低空旅游"],
    }
    out = mod.suggest_keyword_sector(emerging, sector_keywords)
    assert not out.empty
    assert "建议板块" in out.columns


def test_build_n_day_sector_board_aggregates_latest_days(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "DATA_DIR", tmp_path)

    pd.DataFrame([
        {"板块": "人工智能", "热度分": 10, "提及次数": 3, "舆论分": 4},
        {"板块": "机器人", "热度分": 6, "提及次数": 2, "舆论分": 2},
    ]).to_csv(tmp_path / "CCTV-Hot-Sectors-20260408.csv", index=False, encoding="utf-8-sig")

    pd.DataFrame([
        {"板块": "人工智能", "热度分": 8, "提及次数": 2, "舆论分": 3},
        {"板块": "低空经济", "热度分": 7, "提及次数": 2, "舆论分": 3},
    ]).to_csv(tmp_path / "CCTV-Hot-Sectors-20260409.csv", index=False, encoding="utf-8-sig")

    pd.DataFrame([
        {"板块": "低空经济", "热度分": 5, "提及次数": 1, "舆论分": 2},
    ]).to_csv(tmp_path / "CCTV-Hot-Sectors-20260410.csv", index=False, encoding="utf-8-sig")

    out, used_days = mod.build_n_day_sector_board("20260410", 2)

    assert used_days == ["20260409", "20260410"]
    assert not out.empty
    assert out.iloc[0]["板块"] == "低空经济"
    assert float(out.iloc[0]["累计热度分"]) == 12.0
