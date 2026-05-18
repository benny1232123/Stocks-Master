import argparse
import datetime
import re
from pathlib import Path

import akshare as ak
import pandas as pd


TOP_N = 15
MENTION_WEIGHT = 2
PREVIEW_LEN = 120
EMERGING_TOP_N = 40

POSITIVE_WORDS = ["增长", "提升", "突破", "回暖", "提振", "改善", "加速", "扩产", "景气", "超预期", "利好"]
NEGATIVE_WORDS = ["下滑", "下降", "承压", "收缩", "风险", "波动", "走弱", "放缓", "亏损", "违约", "不及预期"]
NEUTRAL_WORDS = ["推进", "建设", "部署", "召开", "会议", "发布", "落实", "调研", "强调"]
GENERIC_MACRO_WORDS = ["中国", "经济", "企业", "产业", "市场", "发展", "全国", "地方", "项目", "部门"]


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "stock_data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def parse_args():
    parser = argparse.ArgumentParser(description="CCTV 新闻舆论热门板块监测")
    parser.add_argument("--date", default="", help="指定日期，格式 YYYYMMDD")
    parser.add_argument("--top-n", type=int, default=TOP_N, help="控制台输出 Top N")
    parser.add_argument("--no-fallback", action="store_true", help="指定日期失败时不回退")
    parser.add_argument("--emerging-top-n", type=int, default=EMERGING_TOP_N)
    parser.add_argument("--backtest-days", type=int, default=0, help="回测最近N个交易信号日")
    parser.add_argument("--unit-days", type=int, default=1, help="按最近N天聚合输出榜单，默认1表示仅当日")
    parser.add_argument("--disable-extra-news", action="store_true", help="禁用补充资讯源抓取")
    parser.add_argument(
        "--extra-news-sources",
        default="cls,sina",
        help="补充资讯源，逗号分隔，例如 cls,sina,em",
    )
    parser.add_argument("--extra-news-limit", type=int, default=120, help="每个补充源最多抓取条数")
    parser.add_argument("--disable-sw-industry", action="store_true", help="禁用申万行业成分股映射")
    return parser.parse_args()


def _safe_text(value):
    return "" if value is None else str(value).strip()


def _resolve_path(p):
    path = Path(p)
    if not path.is_absolute():
        return (ROOT_DIR / path).resolve()
    return path


def _normalize_sw_code(code):
    text = _safe_text(code)
    if not text:
        return ""
    text = text.replace(".SI", "").replace(".si", "")
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits


def _load_sw_industry_index():
    frames = []
    for fn in (getattr(ak, "sw_index_third_info", None), getattr(ak, "sw_index_second_info", None), getattr(ak, "sw_index_first_info", None)):
        if fn is None:
            continue
        try:
            df = fn()
        except Exception as exc:
            print(f"获取申万行业列表失败: {exc}")
            continue
        if df is None or df.empty:
            continue
        if not {"行业代码", "行业名称"}.issubset(df.columns):
            continue
        frames.append(df[["行业代码", "行业名称"]].copy())

    if not frames:
        return []

    out = pd.concat(frames, ignore_index=True)
    out["行业代码"] = out["行业代码"].astype(str).map(_normalize_sw_code)
    out["行业名称"] = out["行业名称"].astype(str).map(_safe_text)
    out = out[(out["行业代码"].str.len() == 6) & (out["行业名称"] != "")]
    out = out.drop_duplicates(subset=["行业代码"], keep="first")
    return out[["行业代码", "行业名称"]].to_dict("records")


def _match_sw_industries(sector_name, keywords, sw_index):
    if not sw_index:
        return []
    sector_text = _safe_text(sector_name)
    kw_list = [k for k in (keywords or []) if _safe_text(k)]
    matched = []
    for item in sw_index:
        name = _safe_text(item.get("行业名称"))
        code = _safe_text(item.get("行业代码"))
        if not name or not code:
            continue
        if sector_text and sector_text in name:
            matched.append(code)
            continue
        for kw in kw_list:
            if kw in name:
                matched.append(code)
                break
    return matched


def _fetch_sw_industry_members(industry_code):
    code = _normalize_sw_code(industry_code)
    if not code:
        return pd.DataFrame()
    try:
        df = ak.index_component_sw(code)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()

    code_col = "证券代码" if "证券代码" in df.columns else None
    name_col = "证券名称" if "证券名称" in df.columns else None
    if not code_col:
        return pd.DataFrame()

    out = pd.DataFrame({
        "code": df[code_col].astype(str).str.strip(),
        "name": df[name_col].astype(str).str.strip() if name_col else "",
    })
    out = out[out["code"].str.match(r"^\d{6}$", na=False)]
    return out.reset_index(drop=True)


def _extract_title(row):
    for col in ["title", "标题", "摘要"]:
        if col in row.index and _safe_text(row[col]):
            return _safe_text(row[col])
    for v in row.values:
        if _safe_text(v):
            return _safe_text(v)[:60]
    return "(无标题)"


def _get_news_text(row):
    cols = ["title", "content", "摘要", "标题", "内容", "正文", "detail", "text"]
    parts = []
    for c in cols:
        if c in row.index and _safe_text(row[c]):
            parts.append(_safe_text(row[c]))
    if not parts:
        parts = [_safe_text(v) for v in row.values if _safe_text(v)]
    return " ".join(parts)


def _sentiment_score(text):
    txt = text.lower()
    pos = sum(txt.count(w.lower()) for w in POSITIVE_WORDS)
    neg = sum(txt.count(w.lower()) for w in NEGATIVE_WORDS)
    neutral = sum(txt.count(w.lower()) for w in NEUTRAL_WORDS)
    macro = sum(txt.count(w.lower()) for w in GENERIC_MACRO_WORDS)
    score = pos - neg - 0.3 * neutral - 0.2 * macro
    return round(score, 2), pos, neg, neutral, macro


def _match_sectors(text, sector_keywords):
    txt = text.lower()
    matched = []
    for sector, kws in sector_keywords.items():
        hits = [k for k in kws if k and k.lower() in txt]
        if hits:
            matched.append((sector, hits))
    return matched


def _normalize_news_df(df):
    if df is None or df.empty:
        return pd.DataFrame(), 0
    raw_count = len(df)
    tmp = df.copy()
    if "title" in tmp.columns:
        keys = tmp["title"].astype(str)
    elif "标题" in tmp.columns:
        keys = tmp["标题"].astype(str)
    else:
        keys = tmp.astype(str).agg("|".join, axis=1)
    tmp["_dedupe_key"] = keys.str.replace(r"\s+", "", regex=True)
    tmp = tmp.drop_duplicates(subset=["_dedupe_key"]).drop(columns=["_dedupe_key"]) 
    return tmp.reset_index(drop=True), raw_count


def fetch_cctv_news(target_date="", fallback=True):
    if target_date:
        d = datetime.datetime.strptime(target_date, "%Y%m%d").date()
        candidates = [d, d - datetime.timedelta(days=1)] if fallback else [d]
    else:
        today = datetime.date.today()
        candidates = [today, today - datetime.timedelta(days=1)] if fallback else [today]

    for d in candidates:
        ds = d.strftime("%Y%m%d")
        local_path = DATA_DIR / f"{ds}_news.csv"
        if local_path.exists():
            try:
                df = pd.read_csv(local_path, encoding="utf-8-sig")
                df, raw_count = _normalize_news_df(df)
                if not df.empty:
                    print(f"成功读取本地 CCTV 新闻：{ds}，原始{raw_count}条，去重后{len(df)}条")
                    return ds, df, raw_count
            except Exception as exc:
                print(f"读取本地 CCTV 新闻失败: {local_path}，原因: {exc}")
        try:
            df = ak.news_cctv(date=ds)
            df, raw_count = _normalize_news_df(df)
            if not df.empty:
                df.to_csv(local_path, index=False, encoding="utf-8-sig")
                print(f"成功获取 CCTV 新闻：{ds}，原始{raw_count}条，去重后{len(df)}条")
                return ds, df, raw_count
            print(f"{ds} 新闻为空，尝试前一日")
        except Exception as exc:
            print(f"获取 {ds} 新闻失败: {exc}")
    return None, pd.DataFrame(), 0


def _normalize_generic_news_df(df, source_name):
    if df is None or df.empty:
        return pd.DataFrame()

    title_cols = ["title", "标题", "新闻标题", "摘要"]
    content_cols = ["content", "内容", "正文", "detail", "text", "摘要"]
    date_cols = ["datetime", "时间", "发布时间", "date", "日期"]

    rows = []
    for _, row in df.iterrows():
        title = ""
        for c in title_cols:
            if c in row.index and _safe_text(row[c]):
                title = _safe_text(row[c])
                break

        content = ""
        for c in content_cols:
            if c in row.index and _safe_text(row[c]):
                content = _safe_text(row[c])
                break

        if not title and not content:
            txt = _safe_text(" ".join(_safe_text(v) for v in row.values if _safe_text(v)))
            if not txt:
                continue
            title = txt[:60]
            content = txt

        pub_time = ""
        for c in date_cols:
            if c in row.index and _safe_text(row[c]):
                pub_time = _safe_text(row[c])
                break

        rows.append(
            {
                "title": title,
                "content": content,
                "source": source_name,
                "pub_time": pub_time,
            }
        )

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    out["_k"] = (out["title"].fillna("") + "|" + out["content"].fillna("")).str.replace(r"\s+", "", regex=True)
    out = out.drop_duplicates(subset=["_k"]).drop(columns=["_k"]).reset_index(drop=True)
    return out


def _try_fetch_ak_news(function_names, source_name, limit=120):
    for fn in function_names:
        try:
            func = getattr(ak, fn, None)
            if func is None:
                continue
            df = func()
            out = _normalize_generic_news_df(df, source_name)
            if out.empty:
                continue
            return out.head(max(int(limit), 1)).reset_index(drop=True), fn
        except Exception:
            continue
    return pd.DataFrame(), ""


def fetch_extra_news_bundle(sources_text="cls,sina", per_source_limit=120):
    source_map = {
        "cls": ["news_cls", "stock_info_global_cls"],
        "sina": ["news_sina", "stock_info_global_sina"],
        "em": ["news_em", "stock_info_global_em"],
    }

    tags = [t.strip().lower() for t in _safe_text(sources_text).split(",") if t.strip()]
    if not tags:
        return pd.DataFrame(), []

    frames = []
    logs = []
    for tag in tags:
        fn_list = source_map.get(tag, [])
        if not fn_list:
            logs.append(f"跳过未知补充源: {tag}")
            continue
        df, fn = _try_fetch_ak_news(fn_list, source_name=tag, limit=per_source_limit)
        if df.empty:
            logs.append(f"补充源抓取失败或为空: {tag}")
            continue
        frames.append(df)
        logs.append(f"补充源抓取成功: {tag} via {fn}, rows={len(df)}")

    if not frames:
        return pd.DataFrame(), logs

    out = pd.concat(frames, ignore_index=True)
    out["_k"] = (out["title"].fillna("") + "|" + out["content"].fillna("")).str.replace(r"\s+", "", regex=True)
    out = out.drop_duplicates(subset=["_k"]).drop(columns=["_k"]).reset_index(drop=True)
    return out, logs


def build_sector_heat(news_df, sector_keywords):
    if news_df.empty:
        return pd.DataFrame(), pd.DataFrame(), {"matched_news": 0}

    rows = []
    stats = {}
    matched_news_count = 0
    for _, row in news_df.iterrows():
        text = _get_news_text(row)
        if not text:
            continue
        score, pos, neg, neutral, macro = _sentiment_score(text)
        matches = _match_sectors(text, sector_keywords)
        if not matches:
            continue
        matched_news_count += 1
        title = _extract_title(row)
        preview = re.sub(r"\s+", " ", text)[:PREVIEW_LEN]
        for sec, hit_keywords in matches:
            info = stats.setdefault(sec, {"板块": sec, "提及次数": 0, "正向词命中": 0, "负向词命中": 0, "中性词命中": 0, "宏观词命中": 0, "舆论分": 0.0})
            info["提及次数"] += 1
            info["正向词命中"] += pos
            info["负向词命中"] += neg
            info["中性词命中"] += neutral
            info["宏观词命中"] += macro
            info["舆论分"] += score
            rows.append({
                "板块": sec,
                "舆论分": score,
                "正向词命中": pos,
                "负向词命中": neg,
                "中性词命中": neutral,
                "宏观词命中": macro,
                "命中关键词": "|".join(hit_keywords),
                "标题": title,
                "新闻片段": preview,
            })

    if not stats:
        return pd.DataFrame(), pd.DataFrame(), {"matched_news": 0}

    sector_df = pd.DataFrame(stats.values())
    sector_df["热度分"] = sector_df["提及次数"] * MENTION_WEIGHT + sector_df["舆论分"]
    sector_df = sector_df.sort_values(["热度分", "提及次数", "舆论分"], ascending=False).reset_index(drop=True)
    matched_df = pd.DataFrame(rows)
    return sector_df, matched_df, {"matched_news": matched_news_count}


def build_quality_metrics(date_str, raw_news_count, dedup_news_count, matched_df, sector_df):
    matched_news = len(matched_df[["标题"]].drop_duplicates()) if not matched_df.empty else 0
    hit_rate = (matched_news / dedup_news_count) if dedup_news_count else 0
    avg_hit = (len(matched_df) / matched_news) if matched_news else 0
    return pd.DataFrame([{
        "日期": date_str,
        "原始新闻数": raw_news_count,
        "去重后新闻数": dedup_news_count,
        "命中新闻数": matched_news,
        "命中率": round(hit_rate, 4),
        "平均每条命中板块数": round(avg_hit, 2),
        "命中板块数": len(sector_df),
    }])


def extract_emerging_keywords(news_df, sector_keywords, top_n):
    known = {k.lower() for kws in sector_keywords.values() for k in kws}
    stopwords = set(
        GENERIC_MACRO_WORDS
        + NEUTRAL_WORDS
        + ["新闻", "报道", "记者", "今天", "今年", "昨日", "今日", "其中", "以及", "记者", "消息", "以来", "月份", "公司"]
    )
    counter = {}
    for _, row in news_df.iterrows():
        txt = _get_news_text(row)
        for tok in re.findall(r"[\u4e00-\u9fff]{2,8}", txt):
            if tok in stopwords or tok.lower() in known:
                continue
            counter[tok] = counter.get(tok, 0) + 1
    if not counter:
        return pd.DataFrame()
    df = pd.DataFrame([{"候选关键词": k, "出现次数": v} for k, v in counter.items()])
    return df.sort_values(["出现次数", "候选关键词"], ascending=[False, True]).head(top_n).reset_index(drop=True)


def _build_auto_sector_keywords(news_df, top_n):
    sector_keywords = {}
    sw_index = _load_sw_industry_index()
    for item in sw_index:
        name = _safe_text(item.get("行业名称"))
        if not name or name in sector_keywords:
            continue
        sector_keywords[name] = [name]

    emerging_df = extract_emerging_keywords(news_df, sector_keywords, top_n)
    return sector_keywords, emerging_df


def enrich_with_prev_change(date_str, sector_df):
    candidates = sorted(DATA_DIR.glob("CCTV-Hot-Sectors-*.csv"), reverse=True)
    prev = None
    for p in candidates:
        if date_str not in p.name:
            prev = p
            break
    if prev is None:
        sector_df["较上一期热度变化"] = "N/A"
        return sector_df
    try:
        prev_df = pd.read_csv(prev, encoding="utf-8-sig")
        prev_map = dict(zip(prev_df.get("板块", []), prev_df.get("热度分", [])))
        sector_df["较上一期热度变化"] = sector_df["板块"].apply(lambda s: "NEW" if s not in prev_map else f"{(sector_df.loc[sector_df['板块']==s,'热度分'].iloc[0]-prev_map[s]):+.1f}")
    except Exception:
        sector_df["较上一期热度变化"] = "N/A"
    return sector_df


def build_n_day_sector_board(current_date_str, unit_days):
    if unit_days <= 1:
        return pd.DataFrame(), []

    file_re = re.compile(r"^CCTV-Hot-Sectors-(\d{8})(?:-\d{6})?\.csv$")
    sources = list(DATA_DIR.glob("CCTV-Hot-Sectors-*.csv"))
    sources += list((DATA_DIR / "archive").glob("*/cctv/CCTV-Hot-Sectors-*.csv"))

    dated_files = []
    for p in sources:
        m = file_re.match(p.name)
        if not m:
            continue
        ds = m.group(1)
        if ds <= current_date_str:
            dated_files.append((ds, p))

    if not dated_files:
        return pd.DataFrame(), []

    latest_by_day = {}
    for ds, p in sorted(dated_files, key=lambda x: (x[0], x[1].name)):
        latest_by_day[ds] = p

    selected_days = sorted(latest_by_day.keys())[-unit_days:]
    if not selected_days:
        return pd.DataFrame(), []

    frames = []
    for ds in selected_days:
        p = latest_by_day[ds]
        try:
            df = pd.read_csv(p, encoding="utf-8-sig")
        except Exception:
            continue
        if df is None or df.empty:
            continue
        if "板块" not in df.columns:
            continue
        tmp = df.copy()
        for col in ["热度分", "提及次数", "舆论分"]:
            if col not in tmp.columns:
                tmp[col] = 0
            tmp[col] = pd.to_numeric(tmp[col], errors="coerce").fillna(0)
        tmp["日期"] = ds
        frames.append(tmp[["日期", "板块", "热度分", "提及次数", "舆论分"]])

    if not frames:
        return pd.DataFrame(), []

    all_df = pd.concat(frames, ignore_index=True)
    board_df = all_df.groupby("板块", as_index=False).agg(
        入榜天数=("日期", "nunique"),
        累计提及次数=("提及次数", "sum"),
        累计舆论分=("舆论分", "sum"),
        累计热度分=("热度分", "sum"),
        平均热度分=("热度分", "mean"),
    )
    board_df["平均热度分"] = board_df["平均热度分"].round(2)
    board_df["累计舆论分"] = board_df["累计舆论分"].round(2)
    board_df["累计热度分"] = board_df["累计热度分"].round(2)
    board_df = board_df.sort_values(["累计热度分", "累计提及次数", "入榜天数"], ascending=False).reset_index(drop=True)
    return board_df, selected_days


def _confidence_tier(heat_score, mention, sentiment):
    signal = heat_score + mention * 0.5 + max(sentiment, 0) * 0.3
    if signal >= 20:
        return "高"
    if signal >= 10:
        return "中"
    return "观察"


def build_sector_stock_pool(date_str, sector_df, stock_hints, sector_keywords, *, use_sw_industry=True):
    if sector_df.empty:
        return pd.DataFrame()
    try:
        base_df = ak.stock_info_a_code_name()
    except Exception as exc:
        print(f"获取A股代码名称失败，跳过个股池输出: {exc}")
        return pd.DataFrame()
    if base_df is None or base_df.empty or not {"code", "name"}.issubset(base_df.columns):
        return pd.DataFrame()

    tmp = base_df.copy()
    tmp["name"] = tmp["name"].astype(str)

    sw_index = _load_sw_industry_index() if use_sw_industry else []
    sw_member_cache = {}

    rows = []
    for _, sec_row in sector_df.iterrows():
        sector = sec_row["板块"]
        hints = stock_hints.get(sector, [])
        keywords = sector_keywords.get(sector, [])
        sw_matched = _match_sw_industries(sector, keywords, sw_index)

        if not hints and not sw_matched:
            hints = [k for k in keywords if _safe_text(k) and 1 < len(_safe_text(k)) <= 6]
        if not hints and not sw_matched:
            continue
        mask = pd.Series(False, index=tmp.index)
        for h in hints:
            mask = mask | tmp["name"].str.contains(re.escape(h), case=False, na=False)
        cand = tmp[mask]

        tier = _confidence_tier(float(sec_row["热度分"]), int(sec_row["提及次数"]), float(sec_row["舆论分"]))

        matched_rows = {}
        for _, s in cand.iterrows():
            code = str(s["code"]).strip()
            if not code:
                continue
            matched_rows[code] = {
                "日期": date_str,
                "板块": sector,
                "热度分": sec_row["热度分"],
                "置信度": tier,
                "股票代码": code,
                "股票名称": s["name"],
                "匹配线索": "|".join(hints[:4]),
            }

        for ind_code in sw_matched:
            if ind_code not in sw_member_cache:
                sw_member_cache[ind_code] = _fetch_sw_industry_members(ind_code)
            df = sw_member_cache[ind_code]
            if df is None or df.empty:
                continue
            for _, s in df.iterrows():
                code = str(s["code"]).strip()
                if not code:
                    continue
                name = str(s.get("name", "") or "")
                entry = matched_rows.get(code)
                if entry is None:
                    entry = {
                        "日期": date_str,
                        "板块": sector,
                        "热度分": sec_row["热度分"],
                        "置信度": tier,
                        "股票代码": code,
                        "股票名称": name,
                        "匹配线索": "",
                    }
                    matched_rows[code] = entry
                tags = [t for t in entry["匹配线索"].split("|") if t]
                tag = f"SW:{ind_code}"
                if tag not in tags:
                    tags.append(tag)
                entry["匹配线索"] = "|".join(tags)

        rows.extend(matched_rows.values())
    return pd.DataFrame(rows)


def _next_day_return(code, signal_date):
    try:
        start = datetime.datetime.strptime(signal_date, "%Y%m%d")
        end = start + datetime.timedelta(days=20)
        hist = ak.stock_zh_a_hist(symbol=str(code), period="daily", start_date=start.strftime("%Y%m%d"), end_date=end.strftime("%Y%m%d"), adjust="qfq")
        if hist is None or len(hist) < 2:
            return None
        c0 = float(hist.iloc[0]["收盘"])
        c1 = float(hist.iloc[1]["收盘"])
        if c0 == 0:
            return None
        return (c1 - c0) / c0
    except Exception:
        return None


def run_backtest(backtest_days=5):
    files = sorted(DATA_DIR.glob("CCTV-Sector-Stock-Pool-*.csv"))
    if not files:
        return pd.DataFrame()
    selected = files[-backtest_days:]
    rows = []
    for f in selected:
        ds = re.findall(r"(\d{8})", f.name)
        if not ds:
            continue
        date_str = ds[0]
        df = pd.read_csv(f, encoding="utf-8-sig")
        if df.empty or "股票代码" not in df.columns:
            continue
        for _, r in df.head(20).iterrows():
            code = str(r["股票代码"]).zfill(6)
            ret = _next_day_return(code, date_str)
            if ret is None:
                continue
            rows.append({"信号日期": date_str, "股票代码": code, "次日收益率": round(ret, 4)})
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    summary = out.groupby("信号日期")["次日收益率"].mean().reset_index(name="组合次日平均收益率")
    return summary


def write_markdown_report(date_str, sector_df, quality_df, emerging_df, top_n, unit_df=None, used_days=None):
    path = DATA_DIR / f"CCTV-Hot-Sectors-{date_str}.md"
    q = quality_df.iloc[0].to_dict() if not quality_df.empty else {}
    lines = [
        f"# CCTV 热门板块舆论监测日报 - {date_str}",
        "",
        f"- 原始新闻数: {q.get('原始新闻数', 'N/A')}",
        f"- 去重后新闻数: {q.get('去重后新闻数', 'N/A')}",
        f"- 命中率: {q.get('命中率', 'N/A')}",
        "",
        "## 热门板块 Top",
        "",
        "| 排名 | 板块 | 热度分 | 提及次数 | 舆论分 | 变化 |",
        "|---|---|---:|---:|---:|---|",
    ]
    for i, row in sector_df.head(top_n).reset_index(drop=True).iterrows():
        lines.append(f"| {i+1} | {row['板块']} | {row['热度分']:.1f} | {int(row['提及次数'])} | {float(row['舆论分']):.1f} | {row['较上一期热度变化']} |")

    if unit_df is not None and not unit_df.empty and used_days:
        lines += [
            "",
            f"## 最近{len(used_days)}天聚合榜单（{used_days[0]}-{used_days[-1]}）",
            "",
            "| 排名 | 板块 | 入榜天数 | 累计提及次数 | 累计舆论分 | 累计热度分 | 平均热度分 |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
        for i, row in unit_df.head(top_n).reset_index(drop=True).iterrows():
            lines.append(
                f"| {i+1} | {row['板块']} | {int(row['入榜天数'])} | {int(row['累计提及次数'])} | "
                f"{float(row['累计舆论分']):.2f} | {float(row['累计热度分']):.2f} | {float(row['平均热度分']):.2f} |"
            )
    if not emerging_df.empty:
        lines += ["", "## 新热点候选词", "", "| 词 | 次数 |", "|---|---:|"]
        for _, r in emerging_df.head(20).iterrows():
            lines.append(f"| {r['候选关键词']} | {int(r['出现次数'])} |")
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def main():
    args = parse_args()
    if args.unit_days <= 0:
        print("--unit-days 必须 >= 1")
        return
    date_str, news_df, raw_news_count = fetch_cctv_news(args.date, fallback=(not args.no_fallback))
    if news_df.empty:
        print("未获取到可用 CCTV 新闻，退出")
        return

    keyword_news_df = news_df
    extra_news_df = pd.DataFrame()
    if not args.disable_extra_news:
        extra_news_df, extra_logs = fetch_extra_news_bundle(args.extra_news_sources, args.extra_news_limit)
        for line in extra_logs:
            print(line)
        if not extra_news_df.empty:
            keyword_news_df = pd.concat([news_df, extra_news_df], ignore_index=True, sort=False)
            print(f"关键词样本扩展: CCTV={len(news_df)} + EXTRA={len(extra_news_df)} => TOTAL={len(keyword_news_df)}")
            extra_path = DATA_DIR / f"CCTV-Extra-News-{date_str}.csv"
            extra_news_df.to_csv(extra_path, index=False, encoding="utf-8-sig")
            print(f"已保存: {extra_path}")

    sector_keywords, emerging_df = _build_auto_sector_keywords(keyword_news_df, args.emerging_top_n)
    sector_df, matched_df, _ = build_sector_heat(keyword_news_df, sector_keywords)
    if sector_df.empty:
        print("未匹配到板块关键词，可扩展词库")
        return

    sector_df = enrich_with_prev_change(date_str, sector_df)

    quality_df = build_quality_metrics(date_str, raw_news_count, len(news_df), matched_df, sector_df)
    stock_pool_df = build_sector_stock_pool(
        date_str,
        sector_df,
        {},
        sector_keywords,
        use_sw_industry=(not args.disable_sw_industry),
    )

    hot_path = DATA_DIR / f"CCTV-Hot-Sectors-{date_str}.csv"
    detail_path = DATA_DIR / f"CCTV-Sector-News-Matched-{date_str}.csv"
    emerging_path = DATA_DIR / f"CCTV-Emerging-Keywords-{date_str}.csv"
    quality_path = DATA_DIR / f"CCTV-Quality-Metrics-{date_str}.csv"
    stock_pool_path = DATA_DIR / f"CCTV-Sector-Stock-Pool-{date_str}.csv"

    sector_df.to_csv(hot_path, index=False, encoding="utf-8-sig")
    matched_df.to_csv(detail_path, index=False, encoding="utf-8-sig")
    quality_df.to_csv(quality_path, index=False, encoding="utf-8-sig")
    if not emerging_df.empty:
        emerging_df.to_csv(emerging_path, index=False, encoding="utf-8-sig")
    if not stock_pool_df.empty:
        stock_pool_df.to_csv(stock_pool_path, index=False, encoding="utf-8-sig")

    unit_df = pd.DataFrame()
    used_days = []
    unit_board_path = None
    if args.unit_days > 1:
        unit_df, used_days = build_n_day_sector_board(date_str, args.unit_days)
        if not unit_df.empty and used_days:
            day_start = used_days[0]
            day_end = used_days[-1]
            unit_board_path = DATA_DIR / f"CCTV-Hot-Sectors-{date_str}-{args.unit_days}D.csv"
            unit_df.to_csv(unit_board_path, index=False, encoding="utf-8-sig")
            print(f"\n最近{len(used_days)}天聚合榜单（{day_start}-{day_end}）Top 列表：")
            print(unit_df.head(args.top_n))
            print(f"已保存: {unit_board_path}")
        else:
            print(f"\n最近{args.unit_days}天聚合榜单无可用数据，跳过输出")

    report_path = write_markdown_report(
        date_str,
        sector_df,
        quality_df,
        emerging_df,
        args.top_n,
        unit_df=unit_df,
        used_days=used_days,
    )

    if args.backtest_days > 0:
        bt = run_backtest(args.backtest_days)
        if not bt.empty:
            bt_path = DATA_DIR / f"CCTV-Backtest-{date_str}.csv"
            bt.to_csv(bt_path, index=False, encoding="utf-8-sig")
            print(f"已保存: {bt_path}")

    print("\nCCTV 舆论热门板块 Top 列表：")
    print(sector_df.head(args.top_n))
    print(f"\n已保存: {hot_path}")
    print(f"已保存: {detail_path}")
    print(f"已保存: {quality_path}")
    if not stock_pool_df.empty:
        print(f"已保存: {stock_pool_path}")
    if not emerging_df.empty:
        print(f"已保存: {emerging_path}")
    print(f"已保存: {report_path}")
    if unit_board_path is not None:
        print(f"已保存: {unit_board_path}")


if __name__ == "__main__":
    main()
