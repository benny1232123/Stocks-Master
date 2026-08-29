"""同花顺特色数据 → 策略输入适配（本项目原本缺失的增量数据源）。

覆盖（均来自 HiThink-Tech/Financial-API 的 special-data / index 端点）：
- 涨跌停池 / 炸板池 / 连板天梯（情绪与连板强度）
- 龙虎榜（机构/游资动向）
- 热榜 / 飙升榜（关注度与资金偏好）
- 同花顺概念板块目录 + 成分股（theme 策略板块发现的官方源，替代/补充 akshare 申万）

所有返回统一转换为本项目 6 位代码体系（经 smcore.utils.code.from_thscode），
与现有 5 策略代码体系对齐，可直接并入 theme / relativity 策略信号。

设计：fail-soft。HITHINK_FINANCE_API_KEY 缺失或任何调用失败返回空/None，
绝不抛异常（与你现有 baostock/akshare 回退链一致）。需联网（沙箱出网被拦截，本地/CI 联网可用）。
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from smcore.utils.code import from_thscode
from smcore.data import hithink as _hk


def _codes_from_items(items: list, code_field: str = "thscode") -> list[str]:
    out = []
    for it in items or []:
        c = from_thscode(it.get(code_field) or it.get("thscode"))
        if c:
            out.append(c)
    return out


# ───────────────────────── 涨停 / 跌停 / 炸板 / 连板 ─────────────────────────
def limit_up_codes(trade_date: str | None = None) -> list[str]:
    """涨停股 6 位代码列表。"""
    return _codes_from_items(_hk.limit_up_pool(trade_date))


def limit_down_codes(trade_date: str | None = None) -> list[str]:
    return _codes_from_items(_hk.limit_down_pool(trade_date))


def limit_break_codes(trade_date: str | None = None) -> list[str]:
    return _codes_from_items(_hk.limit_break_pool(trade_date))


def limit_up_ladder_raw() -> list:
    """连板天梯原始结构（含连板天数等），供策略自行解析。"""
    return _hk.limit_up_ladder()


# ───────────────────────── 龙虎榜 ─────────────────────────
def dragon_tiger_codes(trade_date: str | None = None, board_type: str = "all") -> list[str]:
    """龙虎榜上榜股 6 位代码列表。"""
    return _codes_from_items(_hk.dragon_tiger_list(trade_date, board_type))


def dragon_tiger_raw(trade_date: str | None = None, board_type: str = "all") -> list:
    """龙虎榜原始明细（含买卖额/机构/游资），供策略解析强弱。"""
    return _hk.dragon_tiger_list(trade_date, board_type)


# ───────────────────────── 热榜 / 飙升榜 ─────────────────────────
def hot_codes() -> list[str]:
    """A股热股榜（Top30）6 位代码列表。"""
    return _codes_from_items(_hk.hot_stock_list())


def skyrocket_codes() -> list[str]:
    """飙升榜（Top30）6 位代码列表。"""
    return _codes_from_items(_hk.skyrocket_list())


# ───────────────────────── 板块 → 成分股（theme 策略） ─────────────────────────
def concept_stocks_map(tag: str = "cn_concept", *, codes_only: bool = True) -> dict:
    """同花顺概念板块 → 成分股映射。

    codes_only=True（默认）: {concept_name: [code6, ...]}
    codes_only=False: {concept_name: {"thscode":..., "stocks":[code6,...]}}
    可作为 theme 策略板块发现的官方源；与现有 _BUILDIN_SECTOR_KEYWORDS 内置词库并列兜底。
    """
    cats = _hk.concept_list(tag) or []
    out: dict = {}
    for cat in cats:
        name = cat.get("name") or cat.get("thscode")
        ths = cat.get("thscode")
        if not name or not ths:
            continue
        stocks = _codes_from_items(_hk.concept_stocks(ths))
        if codes_only:
            out[name] = stocks
        else:
            out[name] = {"thscode": ths, "stocks": stocks}
    return out


def concept_stocks_for(concept_thscode: str) -> list[str]:
    """单个板块的成分股 6 位代码列表。"""
    return _codes_from_items(_hk.concept_stocks(concept_thscode))


# ───────────────────────── 个股异动原因（事件催化增强） ─────────────────────────
def anomaly_codes(tag_codes: str | None = None) -> list[str]:
    """异动股 6 位代码列表（按标签过滤可选）。"""
    return _codes_from_items(_hk.anomaly_list(tag_codes))


def anomaly_stock_raw(thscodes) -> list:
    """按代码批量异动原因原始明细（含 keyword_list 催化剂标签、tag_name）。"""
    return _hk.anomaly_stock(thscodes)


def anomaly_keywords_for(code6: str) -> list[str]:
    """某股当日异动关键词（催化剂标签，如 ["白酒","消费"]），供 theme 策略事件增强。去重保序。"""
    items = _hk.anomaly_stock([code6]) or []
    out: list[str] = []
    for it in items:
        for k in it.get("keyword_list") or []:
            if k not in out:
                out.append(k)
    return out


def anomaly_keywords_map(codes: list) -> dict:
    """批量 {code6: [催化剂关键词...]}，供 theme 策略一次性构建催化映射（避免逐股打 API）。

    分批 ≤50 调 anomaly_stock（已验证端点），fail-soft：无 Key/异常/空输入返回 {}。
    键统一为 6 位代码（from_thscode），与 theme 候选的 sh.600519 取尾段对齐。
    """
    if not _hk.available() or not codes:
        return {}
    try:
        uniq: list[str] = []
        seen: set[str] = set()
        for c in codes:
            c6 = from_thscode(c)
            if c6 and c6 not in seen:
                seen.add(c6)
                uniq.append(c6)
        if not uniq:
            return {}
        out: dict = {}
        for i in range(0, len(uniq), 50):
            batch = uniq[i : i + 50]
            items = _hk.anomaly_stock(batch) or []
            for it in items:
                c6 = from_thscode(it.get("thscode"))
                if not c6:
                    continue
                kws: list[str] = []
                for k in it.get("keyword_list") or []:
                    if k and k not in kws:
                        kws.append(k)
                if kws:
                    out[c6] = kws
        return out
    except Exception:
        return {}


# ───────────────────────── 板块动量（风险四层中性化源） ─────────────────────────
_SECTOR_MOM_CACHE: dict = {}


def sector_momentum_map(tag: str = "industry", window: int = 20, *, use_cache: bool = True,
                         names: Optional[list] = None) -> dict:
    """同花顺行业/概念指数近 window 交易日收益 → {板块名: 收益率}。fail-soft。

    用作风险四层中性化「板块动量」的权威源（替代/补充候选股 ret20 聚合）。
    进程内按 (tag, window, 今日) 缓存，避免每次融合重复打指数历史K。
    names: 行业名白名单（可选）。传入时只对匹配的白名单行业拉历史K，大幅减少调用数
    （候选通常 <30 个行业，而非全市场 ~320 个），利于 CI 运行时与限速。
    无 Key / 失败返回 {}。
    """
    global _SECTOR_MOM_CACHE
    today = date.today().strftime("%Y%m%d")
    cache_key = (tag, window, today, tuple(sorted(names)) if names else None)
    if use_cache and cache_key in _SECTOR_MOM_CACHE:
        return _SECTOR_MOM_CACHE[cache_key]
    if not _hk.available():
        return {}
    cats = _hk.concept_list(tag) or []
    if names:
        wl = set(names)
        cats = [
            c for c in cats
            if (c.get("name") in wl)
            or any((n in (c.get("name") or "")) or ((c.get("name") or "") in n) for n in wl)
        ]
    end = date.today()
    start = end - timedelta(days=window * 2 + 15)
    out: dict = {}
    for cat in cats:
        ths = cat.get("thscode")
        name = cat.get("name")
        if not ths or not name:
            continue
        df = _hk.fetch_index_historical(ths, start, end)
        if df is None or df.empty or len(df) < 2:
            continue
        closes = df["close"].dropna()
        if len(closes) < 2:
            continue
        base = closes.iloc[-window - 1] if len(closes) > window else closes.iloc[0]
        last = closes.iloc[-1]
        if base and base > 0:
            out[name] = round(float((last - base) / base), 4)
    _SECTOR_MOM_CACHE[cache_key] = out
    return out


def _match_ths_sector(industry: str, ths: dict) -> Optional[str]:
    """SW 行业名 → 同花顺行业指数名 的最佳匹配（精确 / 包含）。无匹配返回 None。"""
    if not industry or industry in ("未知",):
        return None
    if industry in ths:
        return industry
    for k in ths:
        if industry and (industry in k or k in industry):
            return k
    return None


# ───────────────────────── 复权因子事件流 × qfq 守卫交叉校验 ─────────────────────────
def corporate_action_explains(code6: str, break_date: str, ratio: float,
                              prev_close: float | None = None, tol: float = 0.03) -> bool:
    """该复权断层是否由真实分红/送股解释（break_date 邻近有除权事件且幅度吻合）。

    前复权序列上真实分红本不应产生断层；若出现断层且金额/送股比与同期除权事件吻合，
    说明是「缓存基准过期、需全量重拉」的预期事件，而非数据损坏。fail-soft：无 Key/异常→False。
    """
    if not _hk.available() or not code6 or not break_date:
        return False
    try:
        evs = _hk.fetch_adjustment_factors(code6)
    except Exception:
        return False
    if not evs:
        return False
    from datetime import date as _date
    try:
        brk_d = _date.fromisoformat(break_date) if "-" in break_date else None
    except Exception:
        brk_d = None
    if brk_d is None:
        return False
    for ev in evs:
        ex = _hk._ms_to_date(ev.get("ex_date_ms"))
        if not ex:
            continue
        try:
            ex_d = _date.fromisoformat(ex) if "-" in ex else None
        except Exception:
            ex_d = None
        if ex_d is None or abs((ex_d - brk_d).days) > 3:
            continue
        div = _hk._num(ev.get("dividend_per_share")) or 0.0
        bonus = _hk._num(ev.get("per_share_bonus")) or 0.0
        if prev_close and prev_close > 0:
            expected = (1.0 - div / prev_close) / (1.0 + bonus)
            if expected > 0 and abs(ratio - expected) <= tol:
                return True
        else:
            return True  # 无前收则仅按邻近除权事件存在判定（宽松）
    return False


def classify_breaks(code6: str, breaks: list) -> list:
    """给 find_price_breaks 的断层列表加 explained_by_corporate_action 标注（fail-soft）。"""
    out: list = []
    for b in breaks or []:
        prev = b.get("prev_close")
        try:
            explained = corporate_action_explains(
                code6, b.get("date", ""), float(b.get("ratio", 1.0)),
                float(prev) if prev is not None else None,
            )
        except Exception:
            explained = False
        nb = dict(b)
        nb["explained_by_corporate_action"] = explained
        out.append(nb)
    return out
