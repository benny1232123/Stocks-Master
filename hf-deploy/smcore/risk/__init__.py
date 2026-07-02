"""宏观风险模块。

外部市场数据获取（美股/汇率/期货）+ 风险评估 + NLP 分类 + 事件收集。
从 auto_notify_boll.py(3306行巨石) 逐步抽出。
"""
from __future__ import annotations

from .external import (
    safe_float,
    fetch_us_market_data,
    fetch_fx_data,
    fetch_futures_data,
    assess_us_market_risk,
    assess_fx_risk,
    assess_futures_risk,
)
from .macro import (
    MACRO_STOPWORDS,
    MACRO_NOISE_TOKENS,
    MACRO_RISK_SIGNAL_FRAGMENTS,
    MACRO_RISK_STRONG_FRAGMENTS,
    MACRO_RISK_SOFT_FRAGMENTS,
    MACRO_RISK_POSITIVE_HINTS,
    CCTV_NOISE_SECTORS,
    MACRO_RISK_EXCLUDED_NEWS_TITLES,
    MACRO_PROMO_TITLE_KEYWORDS,
    is_macro_noise_token,
    is_cctv_noise_sector,
    is_macro_risk_term_allowed,
    has_positive_macro_context,
    is_macro_risk_excluded_news_title,
    is_promo_or_historical_title,
    clean_macro_terms,
    extract_macro_tokens,
    nlp_level_to_score,
    get_nlp_classifier,
    nlp_risk_classify,
    extract_burst_tokens,
    collect_macro_risk_events,
    macro_risk_level,
)

__all__ = [
    # external
    "safe_float",
    "fetch_us_market_data",
    "fetch_fx_data",
    "fetch_futures_data",
    "assess_us_market_risk",
    "assess_fx_risk",
    "assess_futures_risk",
    # macro 词库
    "MACRO_STOPWORDS",
    "MACRO_NOISE_TOKENS",
    "MACRO_RISK_SIGNAL_FRAGMENTS",
    "MACRO_RISK_STRONG_FRAGMENTS",
    "MACRO_RISK_SOFT_FRAGMENTS",
    "MACRO_RISK_POSITIVE_HINTS",
    "CCTV_NOISE_SECTORS",
    "MACRO_RISK_EXCLUDED_NEWS_TITLES",
    "MACRO_PROMO_TITLE_KEYWORDS",
    # macro 函数
    "is_macro_noise_token",
    "is_cctv_noise_sector",
    "is_macro_risk_term_allowed",
    "has_positive_macro_context",
    "is_macro_risk_excluded_news_title",
    "is_promo_or_historical_title",
    "clean_macro_terms",
    "extract_macro_tokens",
    "nlp_level_to_score",
    "get_nlp_classifier",
    "nlp_risk_classify",
    "extract_burst_tokens",
    "collect_macro_risk_events",
    "macro_risk_level",
]
