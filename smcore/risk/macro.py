"""宏观新闻风险判定 —— 词库 + 文本匹配 + NLP 分类。

从 auto_notify_boll.py(3306行巨石) 抽出。这部分是纯文本/NLP 逻辑，
仅依赖 re/csv/os/lru_cache，不涉及 baostock 或 pipeline 执行框架，
因此可被两条主线复用（例如可视化界面想独立评估某条新闻风险）。

包含：
- 词库常量（停用词/噪声词/强风险词元/软风险词元/正向语境提示/排除标题/宣传关键词）
- 文本匹配函数（is_xxx / clean / has_positive_context / extract_tokens）
- NLP 分类器（可选，MACRO_RISK_NLP_ENABLE=1 开启）
- burst token 提取 + 宏观风险事件收集（读 news CSV，纯文件 IO）
"""
from __future__ import annotations

import csv
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Iterable


# ── 词库常量（从巨石原样搬移，保持行为一致） ──

MACRO_STOPWORDS = {
    "中国", "经济", "市场", "企业", "行业", "部门", "地方",
    "今年", "今日", "昨天", "消息", "报道", "记者", "相关", "持续", "推进", "表示",
}

MACRO_NOISE_TOKENS = {
    "报道", "日报道", "日称", "日表示", "今日", "今天", "昨日", "昨天",
    "其中", "此外", "与此同时", "截至", "目前", "近日", "今年以来", "今年前",
    "今年一季度", "一季度", "月份", "个月", "亿元", "万户", "公里", "百分点",
    "同比增长", "增长", "发展", "合作", "能力", "基础设施", "农业", "教育",
    "集群", "战略", "推进", "表示", "以上", "时期", "会议指出", "人工智能",
    "十五五", "粤港澳大湾区", "个万亿级产业", "开局之年", "规划纲要提出",
    "产业赋能和场", "习近平指出", "习近平在人民", "习近平强调",
    "第十届中俄博", "义新欧",
}

MACRO_RISK_SIGNAL_FRAGMENTS = (
    "爆发", "袭击", "空袭", "制裁", "升级", "冲突", "断供", "中断", "停摆",
    "危机", "紧张", "动荡", "禁运", "关闭", "撤离", "波动", "飙升", "暴跌",
    "谈判", "缓和", "停火", "协议", "会谈", "战争", "军事", "战机", "导弹",
    "中东", "霍尔木兹", "原油", "油价", "天然气", "能源", "海峡", "核设施",
    "航运", "港口", "外贸", "出口", "供应链", "跨境", "关税", "不确定", "风险",
    "大选", "反击",
)

MACRO_RISK_STRONG_FRAGMENTS = frozenset({
    "爆发", "袭击", "空袭", "制裁", "冲突", "断供", "中断", "停摆", "危机",
    "紧张", "动荡", "禁运", "关闭", "撤离", "飙升", "暴跌", "战争", "军事",
    "战机", "导弹", "反击", "核设施",
})

MACRO_RISK_SOFT_FRAGMENTS = frozenset({
    "升级", "谈判", "会谈", "协议", "能源", "原油", "油价", "天然气",
    "供应链", "跨境", "外贸", "出口", "关税", "波动",
})

MACRO_RISK_POSITIVE_HINTS = (
    "高质量发展", "赋能", "提质", "提效", "推进", "促进", "优化", "改善",
    "增长", "回升", "回暖", "稳住", "扩大", "加强", "提升", "升级改造",
    "建设", "投产", "开工", "竣工", "发布", "出台", "支持", "发展", "创新",
    "合作", "达成", "签约", "获批", "实现", "完成", "落地", "开幕", "启动", "深化",
)

CCTV_NOISE_SECTORS = frozenset({
    "月份", "其中", "今年以来", "集团", "十五五", "今年", "今日", "昨日",
})

# 联播快讯为多条简讯拼盘，标题固定，不参与宏观风险命中与 burst 统计
MACRO_RISK_EXCLUDED_NEWS_TITLES = frozenset({
    "国际联播快讯", "国内联播快讯", "联播快讯",
    "新闻联播", "朝闻天下", "晚间新闻",
})

# 宣传/历史纪录片类关键词，匹配到时降低风险等级
MACRO_PROMO_TITLE_KEYWORDS = (
    "伟大征程", "复兴之路", "辉煌中国", "厉害了", "奋斗",
    "初心使命", "红色沃土", "时代华章", "新征程", "百年风华",
    "星星之火", "燎原", "长征", "赶考", "答卷",
)


# ── 文本匹配纯函数 ──

def is_macro_noise_token(token) -> bool:
    return str(token).strip() in MACRO_NOISE_TOKENS


def is_cctv_noise_sector(name) -> bool:
    text = str(name or "").strip()
    if not text:
        return True
    if text.startswith("热词:") or "热词" in text:
        return True
    return text in CCTV_NOISE_SECTORS


def is_macro_risk_term_allowed(term) -> bool:
    text = str(term or "").strip()
    if not text or len(text) > 12:
        return False
    if text in MACRO_STOPWORDS or is_macro_noise_token(text):
        return False
    return any(fragment in text for fragment in MACRO_RISK_SIGNAL_FRAGMENTS)


def has_positive_macro_context(text) -> bool:
    return any(hint in text for hint in MACRO_RISK_POSITIVE_HINTS)


def is_macro_risk_excluded_news_title(title) -> bool:
    t = (title or "").strip()
    if not t:
        return True
    if t in MACRO_RISK_EXCLUDED_NEWS_TITLES:
        return True
    # 联播快讯变体：只要包含"联播快讯"四个字就排除
    if "联播快讯" in t:
        return True
    return False


def is_promo_or_historical_title(title) -> bool:
    """判断是否为宣传/历史纪录片类标题，这类标题的风险关键词不计入宏观风险。"""
    t = (title or "").strip()
    if not t:
        return False
    # 带【】的节目标题通常为宣传/专题类
    if re.search(r"【[^】]{2,20}】", t):
        return any(kw in t for kw in MACRO_PROMO_TITLE_KEYWORDS)
    return any(kw in t for kw in MACRO_PROMO_TITLE_KEYWORDS)


def clean_macro_terms(values: Iterable) -> list:
    cleaned = []
    seen = set()
    for value in values or []:
        term = str(value).strip()
        if not term or term in seen or not is_macro_risk_term_allowed(term):
            continue
        cleaned.append(term)
        seen.add(term)
    return cleaned


def extract_macro_tokens(text: str) -> list:
    return re.findall(r"[\u4e00-\u9fff]{2,6}", text)


# ── NLP 分类（可选） ──

def nlp_level_to_score(level: str) -> int:
    if level == "high":
        return 4
    if level == "medium":
        return 2
    return 0


@lru_cache(maxsize=1)
def get_nlp_classifier():
    try:
        enable = os.getenv("MACRO_RISK_NLP_ENABLE", "").strip()
    except Exception:
        enable = ""
    if enable != "1":
        return None
    try:
        from transformers import pipeline
    except Exception:
        return None
    model_name = os.getenv("MACRO_RISK_NLP_MODEL", "MoritzLaurer/mDeBERTa-v3-base-mnli-xnli").strip()
    if not model_name:
        model_name = "MoritzLaurer/mDeBERTa-v3-base-mnli-xnli"
    try:
        device = int(os.getenv("MACRO_RISK_NLP_DEVICE", "-1").strip() or "-1")
    except Exception:
        device = -1
    return pipeline("zero-shot-classification", model=model_name, device=device)


def nlp_risk_classify(text: str):
    classifier = get_nlp_classifier()
    if classifier is None:
        return None
    labels = ["高风险", "中风险", "低风险"]
    template = os.getenv("MACRO_RISK_NLP_TEMPLATE", "这条新闻的宏观风险属于{}。 ").strip()
    if not template:
        template = "这条新闻的宏观风险属于{}。"
    try:
        result = classifier(text, labels, hypothesis_template=template)
    except Exception:
        return None

    ranked_labels = result.get("labels") or []
    ranked_scores = result.get("scores") or []
    if not ranked_labels or not ranked_scores:
        return None

    top_label = ranked_labels[0]
    try:
        top_score = float(ranked_scores[0])
    except Exception:
        top_score = 0.0
    try:
        high_threshold = float(os.getenv("MACRO_RISK_NLP_HIGH_THRESHOLD", "0.55").strip() or "0.55")
    except Exception:
        high_threshold = 0.55
    try:
        medium_threshold = float(os.getenv("MACRO_RISK_NLP_MEDIUM_THRESHOLD", "0.45").strip() or "0.45")
    except Exception:
        medium_threshold = 0.45

    if os.getenv("MACRO_RISK_NLP_DEBUG", "0").strip() == "1":
        try:
            dbg_scores = ", ".join(f"{lab}:{score:.3f}" for lab, score in zip(ranked_labels, ranked_scores))
        except Exception:
            dbg_scores = ""
        print(f"[NLP] {top_label}:{top_score:.3f} | {dbg_scores}")

    if top_label == "高风险" and top_score >= high_threshold:
        return "high", top_score
    if top_label == "中风险" and top_score >= medium_threshold:
        return "medium", top_score
    if top_label == "低风险" and top_score >= medium_threshold:
        return "low", top_score
    return None


# ── burst token 提取 + 宏观风险事件收集（纯文件 IO，无 baostock） ──

def extract_burst_tokens(news_files, *, min_count=3, top_n=10) -> set:
    counts = {}
    for f, _date_str in news_files:
        try:
            with f.open("r", encoding="utf-8-sig", newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    title = (row.get("title") or row.get("标题") or "").strip()
                    content = (row.get("content") or row.get("内容") or "").strip()
                    if is_macro_risk_excluded_news_title(title):
                        continue
                    text = f"{title} {content}"
                    for token in extract_macro_tokens(text):
                        if not is_macro_risk_term_allowed(token):
                            continue
                        counts[token] = counts.get(token, 0) + 1
        except Exception:
            continue

    items = [(k, v) for k, v in counts.items() if v >= min_count]
    items.sort(key=lambda x: (-x[1], x[0]))
    if top_n > 0:
        items = items[:top_n]
    return {k for k, _v in items}


def collect_macro_risk_events(news_files, burst_tokens, *, auto_mode=True) -> list:
    events = []
    nlp_only = os.getenv("MACRO_RISK_NLP_ONLY", "0").strip() == "1"
    nlp_enabled = os.getenv("MACRO_RISK_NLP_ENABLE", "0").strip() == "1" or nlp_only
    nlp_mode = os.getenv("MACRO_RISK_NLP_MODE", "hit-only").strip().lower() or "hit-only"

    for f, date_str in news_files:
        try:
            with f.open("r", encoding="utf-8-sig", newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    title = (row.get("title") or row.get("标题") or "").strip()
                    content = (row.get("content") or row.get("内容") or "").strip()
                    if is_macro_risk_excluded_news_title(title):
                        continue
                    text = f"{title} {content}".lower()
                    if not text.strip():
                        continue

                    matched_tags = []
                    risk_score = 0
                    nlp_result = None
                    if nlp_enabled and (nlp_only or nlp_mode == "all"):
                        nlp_result = nlp_risk_classify(text)

                    burst_hit_tokens = []
                    burst_hit_tokens = [tok for tok in burst_tokens if tok.lower() in text]
                    if burst_hit_tokens:
                        uniq_hits = list(dict.fromkeys(burst_hit_tokens))
                        strong_hit = any(tok in MACRO_RISK_STRONG_FRAGMENTS for tok in uniq_hits)
                        soft_hits = [tok for tok in uniq_hits if tok in MACRO_RISK_SOFT_FRAGMENTS]
                        if (not strong_hit) and soft_hits:
                            if has_positive_macro_context(text):
                                continue
                            if len(soft_hits) < 2:
                                continue
                        risk_score = len(uniq_hits)
                        matched_tags = uniq_hits[:3]

                    # 宣传/历史纪录片类标题：风险关键词降级处理
                    if is_promo_or_historical_title(title):
                        if risk_score > 0:
                            risk_score = max(1, risk_score // 2)
                        if matched_tags:
                            matched_tags.append("宣传/历史")
                        # 宣传类且无NLP结果时直接跳过
                        if risk_score <= 1 and not nlp_result:
                            continue

                    if risk_score == 0 and not nlp_result:
                        continue

                    if nlp_result:
                        nlp_level, _nlp_score = nlp_result
                        nlp_score = nlp_level_to_score(nlp_level)
                        if nlp_score > 0:
                            risk_score = max(risk_score, nlp_score)
                            if "NLP" not in matched_tags:
                                matched_tags.append("NLP")

                    pre_easing_score = risk_score
                    risk_score = max(risk_score, 1)

                    if os.getenv("MACRO_RISK_DEBUG", "0").strip() == "1":
                        try:
                            print(
                                f"[MACRO-DEBUG] date={date_str} title={title!r} pre_score={pre_easing_score} final_score={risk_score} tags={matched_tags} nlp={nlp_result}"
                            )
                        except Exception:
                            pass

                    events.append(
                        {
                            "date": date_str,
                            "title": title or "(无标题)",
                            "tags": matched_tags,
                            "risk_score": risk_score,
                        }
                    )

        except Exception:
            continue

    return events


# ── 宏观风险等级判定（从巨石 _macro_risk_level 搬移） ──

def macro_risk_level(macro_risk_summary: str) -> str:
    """从宏观风险摘要文本反推风险等级 high/medium/low。"""
    if not macro_risk_summary:
        return "low"
    import re
    high_hits = 0
    medium_hits = 0
    headline_match = re.search(r"- 命中统计\((?:entry|avg)\): 高=(\d+) 中=(\d+)", macro_risk_summary)
    if headline_match:
        try:
            high_hits = int(headline_match.group(1))
        except Exception:
            high_hits = 0
        try:
            medium_hits = int(headline_match.group(2))
        except Exception:
            medium_hits = 0
    else:
        high_hits = macro_risk_summary.count("[高]")
        medium_hits = macro_risk_summary.count("[中]")
    try:
        high_threshold = max(int(os.getenv("MACRO_RISK_HIGH_HITS", "2").strip() or "2"), 1)
    except Exception:
        high_threshold = 2
    try:
        medium_threshold = max(int(os.getenv("MACRO_RISK_MEDIUM_HITS", "2").strip() or "2"), 1)
    except Exception:
        medium_threshold = 2

    if high_hits >= high_threshold:
        return "high"
    if medium_hits >= medium_threshold:
        return "medium"
    return "low"
