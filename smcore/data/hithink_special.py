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
