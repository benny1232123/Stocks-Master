"""板块映射 + 板块轮动 + 单板块集中度控制。

设计要点
--------
- **板块映射相对静态**：由 scripts/build_sector_map.py 通过 baostock（本地可达）一次性抓取，
  缓存为 stock_data/sector_map.json 并提交仓库。云端 CI 直接读缓存 JSON，无需在线抓取
  （规避海外东财 push2 不可达、akshare spot 偶发 ConnectionError 的问题）。若缓存缺失则在
  云端静默返回空映射，融合层会安全跳过板块逻辑（fail-soft）。
- **板块轮动（确认型）**：用本轮候选股的近 20 日收益（ret20，融合已算过）聚合出「板块动量」，
  对强势板块的候选给小幅评分加成。注意：这是「在本轮已筛候选内确认强势板块」，并非全市场
  轮动信号（全市场轮动需板块指数 20 日收益，云端拿不到东财板块数据），属轻量、零额外联网的增强。
- **单板块集中度控制**：最终入选清单中单板块最多 max_per_sector 只，强制分散，避免单一行业
  黑天鹅把组合拖垮。
"""
from __future__ import annotations

import json
import os
import statistics
from collections import defaultdict
from typing import Optional

from smcore.config.defaults import PROJECT_ROOT
from smcore.utils.code import format_stock_code

SECTOR_MAP_PATH = PROJECT_ROOT / "stock_data" / "sector_map.json"

# 单板块集中度上限（最终入选清单中同一板块最多几只）
DEFAULT_MAX_PER_SECTOR = 5
# 板块动量评分加成幅度（点对综合评分，领先板块 +BONUS / 落后 -BONUS，线性插值）
SECTOR_MOMENTUM_BONUS = 6.0
# 候选数低于此值时不做板块动量加成（样本太少无统计意义）
MIN_SECTOR_MOMENTUM_SAMPLES = 20


_cache: Optional[dict] = None


def get_sector_map(refresh: bool = False) -> dict:
    """返回 {code(6位去前导零): industry}。优先读缓存 JSON，refresh 时重建（仅本地有数据源时成功）。"""
    global _cache
    if not refresh and _cache is not None:
        return _cache
    if not refresh and SECTOR_MAP_PATH.exists():
        try:
            _cache = json.loads(SECTOR_MAP_PATH.read_text(encoding="utf-8"))
            return _cache
        except Exception:
            pass
    # 缓存缺失：尝试重建（云端无数据源会返回 {}，融合层安全跳过）
    _cache = _build_and_cache()
    return _cache


def _build_and_cache() -> dict:
    # 安全闸：仅当显式允许时才在线重建（本地 build_sector_map.py 设置该环境变量）。
    # 云端 CI 必须直接读已提交的缓存 JSON，绝不允许在此自动跑 baostock 全量抓取
    # （~5000 只 × ~1s ≈ 80min，会拖垮流水线）。缓存缺失时静默返回 {}，融合层安全跳过。
    if os.environ.get("SECTOR_MAP_ALLOW_BUILD") != "1":
        return {}
    try:
        import baostock as bs

        lg = bs.login()
        if lg.error_code != "0":
            return {}
        try:
            rs = bs.query_stock_basic(code="", code_name="", type="1")
            codes: list[str] = []
            while rs.next():
                row = rs.get_row_data()
                if row and (row[0].startswith("sh.") or row[0].startswith("sz.")):
                    codes.append(row[0])
            out: dict = {}
            for code in codes:
                c6 = format_stock_code(code)
                if not c6:
                    continue
                try:
                    ir = bs.query_stock_industry(code=code)
                    ind = "未知"
                    if ir.error_code == "0":
                        while ir.next():
                            d = ir.get_row_data()
                            if len(d) >= 3 and d[2]:
                                s = d[2].strip()
                                i = 0
                                while i < len(s) and not ("\u4e00" <= s[i] <= "\u9fff"):
                                    i += 1
                                ind = s[i:].strip() or "未知"
                                break
                    out[c6] = ind
                except Exception:
                    out.setdefault(c6, "未知")
        finally:
            bs.logout()
        if out:
            try:
                SECTOR_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
                SECTOR_MAP_PATH.write_text(
                    json.dumps(out, ensure_ascii=False, indent=0), encoding="utf-8"
                )
            except Exception:
                pass
        return out
    except Exception:
        return {}


def industry_of(code, sector_map: Optional[dict] = None) -> str:
    """返回股票代码对应行业；未知/缺映射返回 '未知'。"""
    c = format_stock_code(code)
    if not c:
        return "未知"
    sm = sector_map if sector_map is not None else get_sector_map()
    return sm.get(c, "未知")


def compute_sector_momentum(
    cand_ret20: dict[str, Optional[float]],
    sector_map: Optional[dict] = None,
) -> tuple[dict[str, float], dict[str, float]]:
    """用候选股 ret20 聚合板块动量。

    Args:
        cand_ret20: {code: ret20}（ret20 为近 20 日收益率，可为 None）
        sector_map: 代码→行业映射

    Returns:
        (sector_bonus, sector_median):
          - sector_bonus: {industry: 评分加成}（领先 + / 落后 -，范围 ±SECTOR_MOMENTUM_BONUS）
          - sector_median: {industry: 中位数 ret20}（调试/报告用）
    """
    sm = sector_map if sector_map is not None else get_sector_map()
    by_ind: dict[str, list[float]] = defaultdict(list)
    for code, ret in cand_ret20.items():
        if ret is None:
            continue
        ind = sm.get(format_stock_code(code), "未知")
        by_ind[ind].append(float(ret))

    medians: dict[str, float] = {}
    for ind, vals in by_ind.items():
        if vals:
            medians[ind] = statistics.median(vals)

    sector_bonus: dict[str, float] = {}
    valid = [m for m in medians.values() if m is not None]
    if len(valid) >= 2 and len(cand_ret20) >= MIN_SECTOR_MOMENTUM_SAMPLES:
        ranked = sorted(medians.items(), key=lambda kv: kv[1])
        n = len(ranked)
        for rank, (ind, med) in enumerate(ranked):
            frac = (rank / (n - 1)) if n > 1 else 0.5  # 0..1
            sector_bonus[ind] = round(SECTOR_MOMENTUM_BONUS * (frac - 0.5) * 2, 2)
    else:
        # 样本不足：不加成（全部 0）
        for ind in medians:
            sector_bonus[ind] = 0.0
    return sector_bonus, medians


def apply_sector_cap(
    df,
    sector_map: Optional[dict] = None,
    max_per: int = DEFAULT_MAX_PER_SECTOR,
    top_n: int = 15,
):
    """对（已按评分排序的）候选 df 施加单板块集中度上限。

    规则：从高评分往低扫描，同一板块入选数达到 max_per 后跳过该板块后续候选，
    直至凑满 top_n 或扫完。若扫完仍不足 top_n（极端集中），再用剩余候选补足
    （此时可能轻微突破上限，属兜底，保证清单长度）。

    Args:
        df: 已按综合评分降序排列的 DataFrame，须含 '股票代码' 列
        sector_map: 代码→行业映射
        max_per: 单板块最多入选数
        top_n: 目标入选总数

    Returns:
        (capped_df, hit_cap: bool): 施加上限后的 DataFrame，及是否触发了上限
    """
    if df is None or df.empty or "股票代码" not in df.columns:
        return df, False
    if not sector_map:
        return df.head(top_n), False

    counts: dict[str, int] = defaultdict(int)
    keep_idx: list[int] = []
    for idx, r in df.iterrows():
        ind = industry_of(r["股票代码"], sector_map)
        # 未映射（"未知"）的股票不计入任何板块上限，避免全部塌缩进同一桶被误砍
        if ind == "未知" or counts[ind] < max_per:
            keep_idx.append(idx)
            if ind != "未知":
                counts[ind] += 1
        if len(keep_idx) >= top_n:
            break

    out = df.loc[keep_idx].reset_index(drop=True)
    hit_cap = any(c >= max_per for c in counts.values())

    # 兜底：不足 top_n 时放宽上限补满
    if len(out) < top_n:
        remaining = df.drop(index=keep_idx)
        need = top_n - len(out)
        out = pd_concat(out, remaining.head(need)).reset_index(drop=True)
    return out, hit_cap


def pd_concat(a, b):
    import pandas as pd

    return pd.concat([a, b], ignore_index=True)
