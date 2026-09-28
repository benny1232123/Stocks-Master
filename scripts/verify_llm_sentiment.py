#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""预注册原型：LLM 级中文情绪分 vs 现有词典型「舆论分」——谁对板块池前向收益更有分层力。

背景（P2 FinGPT 情绪接入的可行性试点，2026-09-27）：
- CCTV 舆情策略现有情绪分 = 情感词词典命中（舆论分），已知盲区：大量新闻行舆论分=0
  （纯中性/宏观文本），情绪信号稀疏。
- FinGPT-7B 级模型 CPU 不可跑；本原型用 IDEA-CCNL/Erlangshen-Roberta-110M-Sentiment
  （中文 RoBERTa 情绪分类，通用域）作「LLM 级情绪分」的可行代理——测的是
  「模型打分 vs 词典打分」的增量，非 FinGPT 本身。

数据（全本地）：
- 文本：stock_data/CCTV-Sector-News-Matched-*.csv（~116 日，标题+片段）
- 板块池：stock_data/CCTV-Sector-Stock-Pool-{date}.csv（板块 → 个股）
- 前向收益：本地 k_data，次开盘买→持有10日开盘卖（与 walk-forward 回补同口径）

预注册声明（先打印，再算数）：
- H1（分层力对比）：按 (信号日, 板块) 池前向 10 日收益，模型情绪分三分位
  Top−Bottom 差 > 词典舆论分同口径差。
- H2（词典盲区增量）：词典舆论分=0 的 (日,板块) 中，模型分 Top1/3 − Bottom1/3 > 0。
- 判定：H1 或 H2 成立**且前后半样本方向一致** → 值得立项「模型情绪接入融合」实验
  （下一步才是生产集成）；否则 FinGPT 线以「当前无增量」关闭。
- 诚实约束：通用域情绪模型（非金融微调）；新闻联播文本以中性/宏观为主，情绪信号本底弱；
  结论为方向性（n≈千级 (日,板块) 对）；模型分数只用于研究对比，不改任何生产配置。

用法：python scripts/verify_llm_sentiment.py [--out stock_data/llm_sentiment_report.md]
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
for p in (str(ROOT), str(ROOT / "scripts")):
    if p not in sys.path:
        sys.path.insert(0, p)

from smcore.config.defaults import STOCK_DATA_DIR  # noqa: E402

MODEL_DIR = ROOT / ".workbuddy" / "models" / "erlangshen-sentiment"
SCORE_CACHE = STOCK_DATA_DIR / "cctv_llm_sentiment_scores.csv"
HORIZON = 10
BATCH = 32
MAX_TOK = 256

PRE_REGISTRATION = """
════════════════════════════════════════════════════════════════
预注册声明（先打印，再算数 —— 防数据窥探）
════════════════════════════════════════════════════════════════
H1：模型情绪分对 (信号日, 板块) 池前向 10 日收益的三分位分层差
    （Top−Bottom）> 词典舆论分同口径差。
H2：词典舆论分=0 的盲区里，模型分 Top1/3 − Bottom1/3 > 0。
判定：H1 或 H2 成立且前后半样本方向一致 → 立项接入实验；否则关闭。
诚实约束：通用域 110M 情绪模型作 LLM 级代理；新闻联播文本情绪本底弱；
只出报告不改生产。
════════════════════════════════════════════════════════════════
"""


def _kline_cache_patch():
    """进程内 LRU 缓存 read_kline_cache（与 run_ml_fuse_ab.py 同款）。"""
    import functools
    import smcore.data.kline as kline_mod
    orig = kline_mod.read_kline_cache

    @functools.lru_cache(maxsize=2048)
    def cached(code, adjust=None, base_dir=None):
        return orig(code, adjust=adjust or kline_mod.DEFAULT_ADJUST, base_dir=base_dir)

    kline_mod.read_kline_cache = cached


def infer_scores(texts: list[str]) -> np.ndarray:
    """批量推理，返回 P(Positive)−P(Negative) ∈ [-1, 1]。"""
    import torch
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    tok = AutoTokenizer.from_pretrained(str(MODEL_DIR))
    model = AutoModelForSequenceClassification.from_pretrained(str(MODEL_DIR))
    model.eval()
    out = np.empty(len(texts), dtype=float)
    with torch.no_grad():
        for i in range(0, len(texts), BATCH):
            batch = texts[i:i + BATCH]
            enc = tok(batch, return_tensors="pt", truncation=True,
                      padding=True, max_length=MAX_TOK)
            prob = model(**enc).logits.softmax(-1).numpy()
            out[i:i + len(batch)] = prob[:, 1] - prob[:, 0]
    return out


def build_score_table() -> pd.DataFrame:
    """全部新闻行的模型打分（带进程级缓存文件，重跑不重推理）。"""
    if SCORE_CACHE.exists():
        try:
            df = pd.read_csv(SCORE_CACHE, encoding="utf-8-sig")
            if len(df) > 500 and "model_score" in df.columns:
                return df
        except Exception:
            pass
    rows = []
    for p in sorted(STOCK_DATA_DIR.glob("CCTV-Sector-News-Matched-*.csv")):
        date = p.stem.rsplit("-", 1)[-1]
        try:
            d = pd.read_csv(p, encoding="utf-8-sig")
        except Exception:
            continue
        if "板块" not in d.columns:
            continue
        for _, r in d.iterrows():
            title = str(r.get("标题", "") or "")
            frag = str(r.get("新闻片段", "") or "")
            if not title and not frag:
                continue
            rows.append({"date": date, "板块": str(r["板块"]),
                         "lex_score": float(r.get("舆论分", 0) or 0),
                         "text": (title + "。" + frag)[:600]})
    if not rows:
        raise RuntimeError("无新闻文本可打分")
    df = pd.DataFrame(rows)
    print(f"推理 {len(df)} 条新闻（batch={BATCH}, max_len={MAX_TOK}）...", flush=True)
    df["model_score"] = infer_scores(df["text"].tolist())
    df.drop(columns=["text"]).to_csv(SCORE_CACHE, index=False, encoding="utf-8-sig")
    return df


def _sector_pool_fwd(date: str, sector: str, fwd_cache: dict) -> float | None:
    """该日该板块股票池的等权前向 10 日收益（%）。"""
    key = (date, sector)
    if key in fwd_cache:
        return fwd_cache[key]
    from walk_forward_validator import _forward_return_from_kdata
    p = STOCK_DATA_DIR / f"CCTV-Sector-Stock-Pool-{date}.csv"
    val = None
    if p.exists():
        try:
            d = pd.read_csv(p, encoding="utf-8-sig")
            codes = d.loc[d["板块"] == sector, "股票代码"].astype(str).str.strip().unique().tolist()
            rets = [r for c in codes if (r := _forward_return_from_kdata(c, date, HORIZON)) is not None]
            if len(rets) >= 3:
                val = float(np.mean(rets))
        except Exception:
            val = None
    fwd_cache[key] = val
    return val


def _tertile_spread(table: pd.DataFrame, score_col: str) -> dict:
    """按 score_col 三分位分组的前向收益差（Top−Bottom，pp）与各组 n。"""
    d = table.dropna(subset=["fwd"]).copy()
    if len(d) < 30:
        return {"spread": None, "n": len(d)}
    q = d[score_col].rank(pct=True)
    top, bottom = d[q >= 2 / 3], d[q < 1 / 3]
    if len(top) < 10 or len(bottom) < 10:
        return {"spread": None, "n": len(d)}
    return {"spread": float(top["fwd"].mean() - bottom["fwd"].mean()),
            "n": len(d), "top_mean": float(top["fwd"].mean()),
            "bottom_mean": float(bottom["fwd"].mean())}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(STOCK_DATA_DIR / "llm_sentiment_report.md"))
    args = ap.parse_args()
    t0 = time.time()
    print(PRE_REGISTRATION, flush=True)

    _kline_cache_patch()
    news = build_score_table()
    print(f"新闻行 {len(news)}，{news['date'].nunique()} 日（{time.time() - t0:.0f}s）", flush=True)

    # (日, 板块) 聚合 + 前向收益
    agg = (news.groupby(["date", "板块"])
           .agg(model_score=("model_score", "mean"),
                lex_score=("lex_score", "mean"), n_news=("model_score", "size"))
           .reset_index())
    fwd_cache: dict = {}
    agg["fwd"] = [_sector_pool_fwd(d, s, fwd_cache)
                  for d, s in zip(agg["date"], agg["板块"])]
    table = agg.dropna(subset=["fwd"])
    print(f"(日,板块) 样本 {len(agg)}，含前向收益 {len(table)}（{time.time() - t0:.0f}s）", flush=True)

    from scipy.stats import spearmanr
    rho, rho_p = spearmanr(table["model_score"], table["lex_score"])

    sm = _tertile_spread(table, "model_score")
    sl = _tertile_spread(table, "lex_score")
    h1 = bool(sm["spread"] is not None and sl["spread"] is not None
              and sm["spread"] > sl["spread"])

    blind = table[table["lex_score"] == 0]
    h2_res = _tertile_spread(blind, "model_score")
    h2 = bool(h2_res["spread"] is not None and h2_res["spread"] > 0)

    # 前后半样本方向一致性（按信号日排序切半）
    days = sorted(table["date"].unique())
    half = max(1, len(days) // 2)
    def _half_stats(sub):
        return (_tertile_spread(sub, "model_score")["spread"],
                _tertile_spread(sub, "lex_score")["spread"],
                _tertile_spread(sub[sub["lex_score"] == 0], "model_score")["spread"])
    sm1, sl1, h21 = _half_stats(table[table["date"].isin(days[:half])])
    sm2, sl2, h22 = _half_stats(table[table["date"].isin(days[half:])])
    h1_stable = bool(h1 and sm1 is not None and sm2 is not None
                     and sl1 is not None and sl2 is not None
                     and (sm1 - sl1) > 0 and (sm2 - sl2) > 0)
    h2_stable = bool(h2 and h21 is not None and h22 is not None and h21 > 0 and h22 > 0)

    pass_gate = h1_stable or h2_stable

    def fmt(x):
        return "--" if x is None else f"{x:+.3f}"
    lines = [
        "# LLM 级情绪分 vs 词典舆论分：板块池前向收益分层力原型（P2 预注册）",
        "",
        f"- 生成：{time.strftime('%Y-%m-%d %H:%M:%S')}；新闻行 {len(news)}（{news['date'].nunique()} 日）；"
        f"(日,板块) 样本 {len(table)}（含前向收益）",
        f"- 模型：Erlangshen-Roberta-110M-Sentiment（通用域，HF 镜像本地加载，CPU）；"
        f"分数 = P(Positive)−P(Negative)",
        f"- 模型分 vs 词典分 Spearman 秩相关：{rho:+.3f}（p={rho_p:.1e}，n={len(table)}）",
        f"- 词典盲区（舆论分=0）占比：{len(blind)}/{len(table)} = {len(blind) / max(1, len(table)):.0%}",
        "",
        "## 一、预注册判定",
        "",
        "| 检验 | 口径 | 结果 | 判定 |",
        "|---|---|---|---|",
        f"| H1 分层力对比 | 模型 Top−Bottom {fmt(sm['spread'])}pp vs 词典 {fmt(sl['spread'])}pp | "
        f"差 {fmt((sm['spread'] or 0) - (sl['spread'] or 0))}pp | {'成立' if h1 else '不成立'}"
        f"（前半 {fmt(sm1 and sm1 - sl1)} / 后半 {fmt(sm2 and sm2 - sl2)}） |",
        f"| H2 盲区增量 | 词典=0 内模型 Top−Bottom | {fmt(h2_res['spread'])}pp（n={h2_res['n']}） | "
        f"{'成立' if h2 else '不成立'}（前半 {fmt(h21)} / 后半 {fmt(h22)}） |",
        f"| **综合** | H1 或 H2 成立且两半同向 | — | **{'✅ 值得立项接入实验' if pass_gate else '❌ 关闭 FinGPT 线'}** |",
        "",
        "## 二、明细",
        "",
        "| 分组 | n | Top 前向% | Bottom 前向% | spread pp |",
        "|---|---|---|---|---|",
        f"| 模型分三分位 | {sm['n']} | {fmt(sm.get('top_mean'))} | {fmt(sm.get('bottom_mean'))} | {fmt(sm['spread'])} |",
        f"| 词典分三分位 | {sl['n']} | {fmt(sl.get('top_mean'))} | {fmt(sl.get('bottom_mean'))} | {fmt(sl['spread'])} |",
        f"| 盲区内模型分 | {h2_res['n']} | {fmt(h2_res.get('top_mean'))} | {fmt(h2_res.get('bottom_mean'))} | {fmt(h2_res['spread'])} |",
        "",
        "- 诚实约束：通用域情绪模型非金融微调，是「LLM 级打分」的可行代理而非 FinGPT 本身；"
        "新闻联播文本情绪本底弱（多数中性/宏观）；结论方向性，若立项须再做金融域模型对比与"
        "生产集成设计（OOS 门控与 ML 因子接入同构）。",
        "",
    ]
    out = Path(args.out)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"H1={h1}(stable={h1_stable}) H2={h2}(stable={h2_stable}) "
          f"rho={rho:+.3f} -> {out}", flush=True)
    print(f"DONE in {time.time() - t0:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
