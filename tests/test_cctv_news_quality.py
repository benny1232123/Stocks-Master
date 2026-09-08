"""CCTV 消息面质量回归测试：联播快讯拆分 / 时政过滤 / 板块匹配强度 / 整句总结。

对应修复：新闻被截成开头一段、时政新闻误挂板块（李希调研→教育、战况→物流）、
联播快讯一条命中多板块且情感互相污染、中性/宏观词负权重导致系统性偏空。
"""
import importlib.util
from pathlib import Path

import pandas as pd

_MODULE_PATH = Path(__file__).resolve().parent.parent / "smcore" / "strategies" / "cctv.py"
_spec = importlib.util.spec_from_file_location("cctv_news_quality", _MODULE_PATH)
assert _spec is not None and _spec.loader is not None
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


# —— 1. 联播快讯拆分 ——
DOMESTIC_BUNDLE = (
    "今年前7个月我国软件业务收入同比增长9.2%记者从工业和信息化部了解到，今年前7个月，"
    "我国软件业务收入89785亿元，同比增长9.2%。其中，软件产品收入占全行业收入比重为21.6%。"
    "软件业务出口406.1亿美元，同比增长12.2%。吉电入京大安火电调峰项目正式启动今天（9月4日），"
    "吉电入京大安火电调峰项目正式启动，计划2029年建成投产。该项目依托特高压直流通道，"
    "途经内蒙古、辽宁、河北等地到达北京。项目全面投运后，每年可向华北输送清洁绿电264亿千瓦时。"
)


def test_bundle_split_into_sub_news():
    subs = mod._split_bundle("国内联播快讯", DOMESTIC_BUNDLE)
    heads = [h for h, _ in subs]
    # 标题句与正文连写无标点，须在正文标志处干净切出
    assert "今年前7个月我国软件业务收入同比增长9.2%" in heads
    assert "吉电入京大安火电调峰项目正式启动" in heads
    # 子标题不得带正文起始标志残留
    assert not any(h.endswith(("记者", "记者从", "国铁集团")) for h in heads)


def test_bundle_continuation_sentences_merged():
    subs = mod._split_bundle("国内联播快讯", DOMESTIC_BUNDLE)
    texts = [t for _, t in subs]
    # 「其中，……」「该项目依托……」「项目全面投运后……」是上一条的续写句，不能单列
    joined = "".join(texts)
    assert "其中，软件产品收入占全行业收入比重为21.6%" in texts[0]
    assert any("该项目依托" in t and t.startswith("吉电入京") for t in texts)
    assert not any(t.startswith(("其中", "该项目", "项目全面")) for t in texts)


# —— 2. 时政/国际要闻过滤 ——
def test_political_news_filtered():
    assert mod._is_nonmarket_news("李希在贵州调研", "中共中央政治局常委、中央纪委书记李希1日至3日到贵州调研。他强调，纪检监察机关要坚持…")
    assert mod._is_nonmarket_news("丁薛祥出席第十一届东方经济论坛全会并致辞", "…在俄罗斯符拉迪沃斯托克出席第十一届东方经济论坛全会并致辞。")
    assert mod._is_nonmarket_news("习近平向第八届中俄能源商务论坛致贺信", "9月4日，第八届中俄能源商务论坛在俄罗斯符拉迪沃斯托克举办。")


def test_war_news_filtered():
    assert mod._is_nonmarket_news("俄称控制多个居民点 乌称袭击俄保障船", "俄罗斯国防部3日称…对乌克兰军用物流中心、船只、港口设施等目标实施集群打击")
    assert mod._is_nonmarket_news("也门政府军与胡塞武装发生交火", "也门政府官员3日说，政府军与胡塞武装当天在西南部塔伊兹省多地发生激烈交火")


def test_market_news_not_filtered():
    assert not mod._is_nonmarket_news("李强签署国务院令 公布修订后的《电力安全事故应急处置和调查处理条例》", "国务院总理李强日前签署国务院令，公布修订后的《条例》，自2027年1月1日起施行。")
    assert not mod._is_nonmarket_news("今年前7个月我国软件业务收入同比增长9.2%", "记者从工业和信息化部了解到，我国软件业务收入89785亿元，同比增长9.2%。")


# —— 3. 板块匹配强度（标题/导语强命中保留，正文深处单次弱命中剔除）——
KW = mod._BUILTIN_SECTOR_KEYWORDS


def test_deep_single_body_hit_dropped():
    # 「党纪学习教育」式误挂：教育/旅游仅在正文深处出现 1 次 → 剔除
    # （正文先铺 150+ 字与板块无关的铺垫，使命中落在导语窗口之外）
    title = "某领导出席论坛并致辞"
    filler = "论坛在莫斯科举办，中方回顾了两国合作取得的丰硕成果，系统阐述了中方关于区域合作的主张，在国际社会引起广泛共鸣，在两国元首战略引领下，地区合作取得长足进展，各领域机制化建设不断走深走实，为下一步合作奠定了坚实基础，与会各方还就共同关心的议题充分交换了意见。"
    body = filler + "双方还谈到在党纪学习教育方面的经验交流，以及旅游年框架下的人文往来。"
    sectors = [s for s, _, _ in mod._match_sectors_detailed(title, body, KW)]
    assert "教育" not in sectors
    assert "消费" not in sectors


def test_lede_hit_kept():
    title = "我国将加快建设“六张网”项目库"
    body = "记者从国家发展改革委了解到，为确保算力网、新型电网等“六张网”建设高效有序推进，我国将加快建设“六张网”项目库。"
    sectors = dict((s, k) for s, k, _ in mod._match_sectors_detailed(title, body, KW))
    assert "电力" in sectors
    assert "人工智能" in sectors


def test_multi_occurrence_body_hit_kept():
    # 商业航天报道：卫星/航空航天/船舶 在正文多次出现 → 军工保留
    title = "我国商业航天迸发新动能"
    body = ("今年以来，一系列政策举措持续发力，中国商业航天新赛道更加活跃。不久前，朱雀三号成功实现"
            "我国首次入轨级运载火箭陆地可回收。这枚火箭由商业航天企业研制，其卫星互联网组网卫星与航空航天配套、"
            "船舶导航芯片等产业链公司广泛受益，卫星应用市场空间进一步打开。")
    sectors = [s for s, _, _ in mod._match_sectors_detailed(title, body, KW)]
    assert "军工" in sectors


# —— 4. 整句总结（不截半句）——
def test_preview_complete_sentence():
    text = ("李强签署国务院令 公布修订后的《电力安全事故应急处置和调查处理条例》"
            "国务院总理李强日前签署国务院令，公布修订后的《电力安全事故应急处置和调查处理条例》"
            "（以下简称《条例》），自2027年1月1日起施行。《条例》修订的主要内容，一是明确电力企业、"
            "电力用户等应当服从统一调度。")
    p = mod._make_preview(text, title="李强签署国务院令 公布修订后的《电力安全事故应急处置和调查处理条例》")
    assert p.startswith("国务院总理李强日前签署国务院令")
    assert p.endswith("。")
    assert "…" not in p
    assert len(p) <= mod.PREVIEW_LEN


def test_preview_short_first_sentence_appends_next():
    text = "央广网北京9月4日消息 据中央广播电视总台中国之声报道。今年前7个月我国软件业务收入89785亿元，同比增长9.2%，利润总额保持稳定。"
    p = mod._make_preview(text, title="")
    assert p.endswith("。")
    assert len(p) > 40  # 首句过短时补了下一句


# —— 5. 情感打分：中性/宏观词不再压分 ——
def test_neutral_macro_words_do_not_push_negative():
    # 旧版：15 中性 + 3 宏观 → -5.1（偏空）；修正后极性分只由正/负向词决定
    text = "领导深入调研强调要把学习贯彻落实部署要求落到实处，推动地方经济社会发展项目建设和企业发展。"
    score, pos, neg, neu, macro = mod._sentiment_score(text)
    assert pos == 0 and neg == 0
    assert score == 0.0


def test_positive_news_scores_positive():
    score, pos, neg, _, _ = mod._sentiment_score("今年前7个月我国软件业务收入89785亿元，同比增长9.2%，效益持续提升。")
    assert score >= 2.0
    assert pos >= 2 and neg == 0


# —— 6. 端到端：build_sector_heat 消费拆分后的新闻 ——
def test_build_sector_heat_end_to_end():
    news_df = pd.DataFrame([
        {"title": "国内联播快讯", "content": DOMESTIC_BUNDLE},
        {"title": "李希在贵州调研", "content": "中共中央政治局常委、中央纪委书记李希1日至3日到贵州调研。他强调，要深入开展党纪学习教育。"},
    ])
    sector_df, matched_df, stats = mod.build_sector_heat(news_df, KW)
    sectors = set(sector_df["板块"])
    assert "计算机" in sectors and "电力" in sectors
    # 时政新闻不得产生任何板块行
    assert all("李希" not in t for t in matched_df["标题"])
    # 联播快讯子条各自成行，总结为完整句
    for _, r in matched_df.iterrows():
        assert r["新闻片段"].endswith("。") or r["新闻片段"].endswith("…")
        assert not r["新闻片段"].startswith("国内联播快讯")
