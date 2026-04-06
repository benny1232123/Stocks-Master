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
