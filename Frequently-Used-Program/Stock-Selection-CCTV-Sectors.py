import argparse
import datetime
import json
import re
from pathlib import Path

import akshare as ak
import pandas as pd


TOP_N = 15
MENTION_WEIGHT = 2
PREVIEW_LEN = 120
EMERGING_TOP_N = 40
KEYWORD_CONFIG_PATH = "stock_data/cctv_sector_keywords.json"
ACCEPTED_KEYWORD_PATH = "stock_data/cctv_keyword_accepts.json"
STOCK_MAP_CONFIG_PATH = "stock_data/cctv_sector_stock_map.json"

POSITIVE_WORDS = ["增长", "提升", "突破", "回暖", "提振", "改善", "加速", "扩产", "景气", "超预期", "利好"]
NEGATIVE_WORDS = ["下滑", "下降", "承压", "收缩", "风险", "波动", "走弱", "放缓", "亏损", "违约", "不及预期"]
NEUTRAL_WORDS = ["推进", "建设", "部署", "召开", "会议", "发布", "落实", "调研", "强调"]
GENERIC_MACRO_WORDS = ["中国", "经济", "企业", "产业", "市场", "发展", "全国", "地方", "项目", "部门"]

SECTOR_KEYWORDS = {
    "人工智能": ["人工智能", "AI", "大模型", "多模态", "智能体", "AIGC", "端侧AI"],
    "算力": ["算力", "智算", "液冷", "数据中心", "服务器", "GPU"],
    "半导体": ["半导体", "芯片", "晶圆", "封测", "存储芯片", "光刻"],
    "机器人": ["机器人", "人形机器人", "工业机器人", "服务机器人", "机器视觉", "灵巧手"],
    "低空经济": ["低空经济", "无人机", "eVTOL", "飞行汽车", "通航", "低空物流"],
    "新能源车": ["新能源车", "电动车", "锂电", "充电桩", "动力电池", "智能驾驶"],
    "光伏": ["光伏", "组件", "硅料", "逆变器", "储能", "钙钛矿"],
    "医药医疗": ["医药", "创新药", "医疗器械", "中药", "生物制药", "CXO"],
    "军工": ["军工", "国防", "导弹", "舰船", "战机", "军贸"],
    "数字经济": ["数字经济", "工业互联网", "信创", "数据要素", "云计算", "网络安全"],
    "跨境出海": ["跨境电商", "出海", "海外订单", "海外市场", "一带一路"],
    "国企改革": ["国企改革", "央企", "市值管理", "并购重组", "资产注入"],
}

DEFAULT_YEARLY_HOTSPOTS = {
    "2026": {
        "AI应用": ["AI手机", "AI PC", "AI眼镜", "智能终端", "端侧模型"],
        "自主可控": ["国产替代", "自主可控", "国产算力", "国产软件", "信创"],
    }
}

DEFAULT_SECTOR_STOCK_HINTS = {
    "人工智能": ["智能", "软件", "科技", "信息"],
    "算力": ["服务器", "数据", "通信", "光模块"],
    "半导体": ["芯片", "半导体", "微电子", "集成电路"],
    "机器人": ["机器人", "自动化", "机械", "传动"],
    "低空经济": ["无人机", "航空", "航天", "飞行"],
    "新能源车": ["汽车", "电池", "充电", "新能源"],
    "光伏": ["光伏", "能源", "硅", "电力"],
    "医药医疗": ["医药", "医疗", "生物", "制药"],
    "军工": ["军工", "航天", "船舶", "电子"],
    "数字经济": ["数字", "网络", "云", "数据"],
    "跨境出海": ["跨境", "电商", "物流", "港口"],
    "国企改革": ["国资", "集团", "央企", "控股"],
}

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "stock_data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def parse_args():
    parser = argparse.ArgumentParser(description="CCTV 新闻舆论热门板块监测")
    parser.add_argument("--date", default="", help="指定日期，格式 YYYYMMDD")
    parser.add_argument("--top-n", type=int, default=TOP_N, help="控制台输出 Top N")
    parser.add_argument("--no-fallback", action="store_true", help="指定日期失败时不回退")
    parser.add_argument("--keyword-config", default=KEYWORD_CONFIG_PATH)
    parser.add_argument("--accepted-keywords", default=ACCEPTED_KEYWORD_PATH)
    parser.add_argument("--stock-map-config", default=STOCK_MAP_CONFIG_PATH)
    parser.add_argument("--emerging-top-n", type=int, default=EMERGING_TOP_N)
    parser.add_argument("--backtest-days", type=int, default=0, help="回测最近N个交易信号日")
    parser.add_argument("--unit-days", type=int, default=1, help="按最近N天聚合输出榜单，默认1表示仅当日")
    return parser.parse_args()


def _safe_text(value):
    return "" if value is None else str(value).strip()


def _resolve_path(p):
    path = Path(p)
    if not path.is_absolute():
        return (ROOT_DIR / path).resolve()
    return path


def _merge_keywords(base_map, overlay_map):
    result = {k: list(v) for k, v in base_map.items()}
    for sector, kws in overlay_map.items():
        if not isinstance(sector, str) or not isinstance(kws, list):
            continue
        cleaned = [_safe_text(x) for x in kws if _safe_text(x)]
        if not cleaned:
            continue
        existing = result.setdefault(_safe_text(sector), [])
        seen = set(existing)
        for kw in cleaned:
            if kw not in seen:
                existing.append(kw)
                seen.add(kw)
    return result


def _load_sector_keywords(config_path, accepted_path, year_str):
    result = _merge_keywords(SECTOR_KEYWORDS, DEFAULT_YEARLY_HOTSPOTS.get(year_str, {}))

    config_file = _resolve_path(config_path)
    if config_file.exists():
        try:
            data = json.loads(config_file.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                if "base" in data or "yearly" in data:
                    result = _merge_keywords(result, data.get("base", {}))
                    yearly = data.get("yearly", {})
                    if isinstance(yearly, dict):
                        result = _merge_keywords(result, yearly.get(year_str, {}))
                else:
                    result = _merge_keywords(result, data)
            print(f"已加载关键词配置: {config_file} (year={year_str})")
        except Exception as exc:
            print(f"读取关键词配置失败: {exc}")

    accepted_file = _resolve_path(accepted_path)
    if accepted_file.exists():
        try:
            accepted = json.loads(accepted_file.read_text(encoding="utf-8"))
            if isinstance(accepted, dict):
                result = _merge_keywords(result, accepted.get("base", {}))
                yearly = accepted.get("yearly", {})
                if isinstance(yearly, dict):
                    result = _merge_keywords(result, yearly.get(year_str, {}))
                print(f"已应用人工确认关键词: {accepted_file} (year={year_str})")
        except Exception as exc:
            print(f"读取人工确认关键词失败: {exc}")

    return result


def _load_sector_stock_hints(config_path):
    result = {k: list(v) for k, v in DEFAULT_SECTOR_STOCK_HINTS.items()}
    cfg = _resolve_path(config_path)
    if not cfg.exists():
        return result
    try:
        data = json.loads(cfg.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            result = _merge_keywords(result, data)
            print(f"已加载板块个股映射提示: {cfg}")
    except Exception as exc:
        print(f"读取板块个股映射提示失败: {exc}")
    return result


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
        try:
            df = ak.news_cctv(date=ds)
            df, raw_count = _normalize_news_df(df)
            if not df.empty:
                (DATA_DIR / f"{ds}_news.csv").write_text(df.to_csv(index=False, encoding="utf-8-sig"), encoding="utf-8-sig")
                print(f"成功获取 CCTV 新闻：{ds}，原始{raw_count}条，去重后{len(df)}条")
                return ds, df, raw_count
            print(f"{ds} 新闻为空，尝试前一日")
        except Exception as exc:
            print(f"获取 {ds} 新闻失败: {exc}")
    return None, pd.DataFrame(), 0


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
    stopwords = set(GENERIC_MACRO_WORDS + NEUTRAL_WORDS + ["新闻", "报道", "记者", "今天", "今年"])
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


def suggest_keyword_sector(emerging_df, sector_keywords):
    if emerging_df.empty:
        return pd.DataFrame()
    suggestions = []
    for _, row in emerging_df.iterrows():
        kw = row["候选关键词"]
        best_sector = ""
        best_score = 0
        for sec, kws in sector_keywords.items():
            score = 0
            for k in kws:
                if kw in k or k in kw:
                    score += 2
                inter = len(set(kw) & set(k))
                if inter >= 2:
                    score += 1
            if score > best_score:
                best_sector = sec
                best_score = score
        suggestions.append({
            "候选关键词": kw,
            "出现次数": int(row["出现次数"]),
            "建议板块": best_sector if best_score > 0 else "待人工判断",
            "建议置信度": "高" if best_score >= 4 else ("中" if best_score >= 2 else "低"),
        })
    return pd.DataFrame(suggestions)


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


def build_sector_stock_pool(date_str, sector_df, stock_hints):
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
    rows = []
    for _, sec_row in sector_df.head(12).iterrows():
        sector = sec_row["板块"]
        hints = stock_hints.get(sector, [])
        if not hints:
            continue
        mask = pd.Series(False, index=tmp.index)
        for h in hints:
            mask = mask | tmp["name"].str.contains(re.escape(h), case=False, na=False)
        cand = tmp[mask].head(10)
        tier = _confidence_tier(float(sec_row["热度分"]), int(sec_row["提及次数"]), float(sec_row["舆论分"]))
        for _, s in cand.iterrows():
            rows.append({
                "日期": date_str,
                "板块": sector,
                "热度分": sec_row["热度分"],
                "置信度": tier,
                "股票代码": s["code"],
                "股票名称": s["name"],
                "匹配线索": "|".join(hints[:4]),
            })
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
    summary = out.groupby("信号日期", as_index=False)["次日收益率"].mean()
    summary = summary.rename(columns={"次日收益率": "组合次日平均收益率"})
    return summary


def write_markdown_report(date_str, sector_df, quality_df, emerging_df, suggestion_df, top_n, unit_df=None, used_days=None):
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
    if not suggestion_df.empty:
        lines += ["", "## 候选词归类建议", "", "| 词 | 建议板块 | 置信度 |", "|---|---|---|"]
        for _, r in suggestion_df.head(20).iterrows():
            lines.append(f"| {r['候选关键词']} | {r['建议板块']} | {r['建议置信度']} |")
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def main():
    args = parse_args()
    if args.unit_days <= 0:
        print("--unit-days 必须 >= 1")
        return
    year = args.date[:4] if args.date else datetime.date.today().strftime("%Y")
    sector_keywords = _load_sector_keywords(args.keyword_config, args.accepted_keywords, year)
    stock_hints = _load_sector_stock_hints(args.stock_map_config)

    date_str, news_df, raw_news_count = fetch_cctv_news(args.date, fallback=(not args.no_fallback))
    if news_df.empty:
        print("未获取到可用 CCTV 新闻，退出")
        return

    sector_df, matched_df, _ = build_sector_heat(news_df, sector_keywords)
    if sector_df.empty:
        print("未匹配到板块关键词，可扩展词库")
        return

    sector_df = enrich_with_prev_change(date_str, sector_df)
    emerging_df = extract_emerging_keywords(news_df, sector_keywords, args.emerging_top_n)
    suggestion_df = suggest_keyword_sector(emerging_df, sector_keywords)
    quality_df = build_quality_metrics(date_str, raw_news_count, len(news_df), matched_df, sector_df)
    stock_pool_df = build_sector_stock_pool(date_str, sector_df, stock_hints)

    hot_path = DATA_DIR / f"CCTV-Hot-Sectors-{date_str}.csv"
    detail_path = DATA_DIR / f"CCTV-Sector-News-Matched-{date_str}.csv"
    emerging_path = DATA_DIR / f"CCTV-Emerging-Keywords-{date_str}.csv"
    suggest_path = DATA_DIR / f"CCTV-Emerging-Keyword-Suggestions-{date_str}.csv"
    quality_path = DATA_DIR / f"CCTV-Quality-Metrics-{date_str}.csv"
    stock_pool_path = DATA_DIR / f"CCTV-Sector-Stock-Pool-{date_str}.csv"

    sector_df.to_csv(hot_path, index=False, encoding="utf-8-sig")
    matched_df.to_csv(detail_path, index=False, encoding="utf-8-sig")
    quality_df.to_csv(quality_path, index=False, encoding="utf-8-sig")
    if not emerging_df.empty:
        emerging_df.to_csv(emerging_path, index=False, encoding="utf-8-sig")
    if not suggestion_df.empty:
        suggestion_df.to_csv(suggest_path, index=False, encoding="utf-8-sig")
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
        suggestion_df,
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
    if not suggestion_df.empty:
        print(f"已保存: {suggest_path}")
    print(f"已保存: {report_path}")
    if unit_board_path is not None:
        print(f"已保存: {unit_board_path}")


if __name__ == "__main__":
    main()
