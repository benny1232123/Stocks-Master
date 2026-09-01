"""CCTV Tier-1 NLP 验收回归测试（jieba 分词 + 加权情感词典 + 同义词扩展）。

对应提交 5b122e5（feat: Tier-1 NLP 升级）+ 8fe1f5d（内置板块词库兜底）。
覆盖四类关键行为：
1. 程度副词放大（大幅增长 → +1.8）
2. 否定词翻转（未增长 → -1.0）
3. 板块匹配（直接命中 + 同义词扩展 + 子串误命中防护）
4. 内置词库兜底（不依赖网络，板块发现可用）
"""
import importlib.util
from pathlib import Path

import pandas as pd

_MODULE_PATH = Path(__file__).resolve().parent.parent / "smcore" / "strategies" / "cctv.py"
_spec = importlib.util.spec_from_file_location("cctv_tier1", _MODULE_PATH)
assert _spec is not None and _spec.loader is not None
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


def _sent(text):
    return mod._sentiment_score(text)


# —— 1. 程度副词放大 ——
def test_intensifier_amplifies_positive():
    score, pos, neg, neu, macro = _sent("公司业绩大幅增长")
    assert pos == 1 and neg == 0
    assert abs(score - 1.8) < 0.01  # 大幅 ×1.8


def test_no_intensifier_keeps_baseline():
    score, pos, neg, _, _ = _sent("公司业绩增长")
    assert abs(score - 1.0) < 0.01


# —— 2. 否定词翻转 ——
def test_negator_flips_positive():
    score, pos, neg, _, _ = _sent("公司业绩未增长")
    assert abs(score - (-1.0)) < 0.01


def test_negator_flips_negative():
    score, pos, neg, _, _ = _sent("业绩未下滑")
    assert abs(score - 1.0) < 0.01


# —— 3. 板块匹配 ——
def test_direct_keyword_hit():
    kw = mod._BUILTIN_SECTOR_KEYWORDS
    matched = mod._match_sectors("今天锂电池板块大涨", kw)
    sectors = [s for s, _ in matched]
    assert "锂电" in sectors


def test_synonym_expansion_hits_sector():
    # 动力电池 不在内置词库关键词内，但经 sector_synonyms 的 锂电池->动力电池 映射命中锂电
    kw = mod._BUILTIN_SECTOR_KEYWORDS
    matched = mod._match_sectors("动力电池出货量创新高", kw)
    sectors = [s for s, _ in matched]
    assert "锂电" in sectors


def test_substring_false_positive_guard():
    # 停车费 不得误命中 汽车（token 精确匹配而非子串包含）
    kw = {"汽车": ["汽车"], "新能源车": ["新能源车"]}
    matched = mod._match_sectors("小区停车费涨价引发业主讨论", kw)
    assert len(matched) == 0


# —— 4. 情感方向粗判 ——
def test_negative_text_net_score_below_zero():
    score, pos, neg, _, _ = _sent("经济下滑风险加大，企业承压收缩")
    assert pos == 0 and neg >= 2
    assert score < 0


def test_neutral_macro_text_no_polarity_boom():
    score, pos, neg, neu, macro = _sent("全国经济工作会议召开")
    assert pos == 0 and neg == 0
    assert neu >= 1 and macro >= 1


# —— 5. 内置词库兜底（板块发现不依赖网络）——
def test_builtin_sector_keywords_available():
    assert len(mod._BUILTIN_SECTOR_KEYWORDS) >= 40
    # 关键热点板块必须在词库中
    for sec in ["电力", "半导体", "人工智能", "机器人", "新能源车", "军工", "医药", "消费"]:
        assert sec in mod._BUILTIN_SECTOR_KEYWORDS, f"缺板块: {sec}"


def test_auto_sector_keywords_builds_from_builtin():
    # 即使申万接口不可用，内置词库也能从新闻中产出板块关键词表
    news_df = pd.DataFrame([
        {"title": "人工智能大模型加速落地，算力需求持续提升", "content": ""},
        {"title": "光伏组件出口大幅增长，硅料价格回暖", "content": ""},
        {"title": "机器人产业景气度超预期，减速器扩产", "content": ""},
    ])
    sector_keywords, emerging = mod._build_auto_sector_keywords(news_df, top_n=20)
    assert "人工智能" in sector_keywords
    assert "光伏" in sector_keywords
    assert "机器人" in sector_keywords
