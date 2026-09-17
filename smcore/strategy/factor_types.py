#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""策略 → 因子类型映射（报告/监控层重分类用，2026-09-15）。

用户要求：报告与监控不再「按 5 策略名」分组，改为「按因子类型」分组。
本模块是**唯一**的映射来源，报告层（report.py）与监控层（factor_ic_monitor.py）
都从这里取，避免两处各写一套导致漂移。

设计原则（与「别过度工程 / 底层不动」一致）：
- 只做展示层的**标签与归并**，不触碰任何策略选股 / 融合 / 权重逻辑；
- 每策略归一个**主因子类型**（1:1），保证归并桶清晰、无歧义；
- 一个 pick 可命中多个策略（来源策略="Boll/Momentum"），因此可映射到多个因子类型——
  `factor_types_of_source` 返回去重后的类型列表，供明细展示与贡献度统计。

2026-09-15 修订：原「题材·事件」把 Theme 与 CCTV 合并，归因发现 Theme 实为拖累、
CCTV 才是正贡献，合并掩盖了内部分化 → 拆成「题材」(Theme) 与「事件·舆情」(CCTV) 两类。
"""
from __future__ import annotations

# 主因子类型映射：key 用小写策略 id（与 adaptive_weights.ALL_STRATEGIES 对齐）
STRATEGY_FACTOR_TYPE = {
    "boll": "反转·均值回归",
    "momentum": "动量",
    "relativity": "相对强度·资金流",
    "theme": "题材",
    "cctv": "事件·舆情",
    # 2026-09-17：原单一「基本面·质量价值」拆成三个正交基本面类型，便于横向对比各维 edge。
    "quality": "基本面·质量",
    "value": "基本面·估值",
    "size": "基本面·规模",
    # 2026-09-17：boll / relativity 两个「复合策略」沿价格轴拆成原子因子，各自独立竞权。
    # boll 的布林触发 4 个原子（互斥优先级，与 evaluate_boll_signal 一致）：
    "boll_oversold": "反转·超卖",
    "boll_near_lower": "反转·近下轨",
    "boll_mid_pullback": "反转·中轨回踩",
    "boll_squeeze": "反转·带宽收口",
    # relativity 的相对强度触发 2 个原子（各自独立，可同时命中）：
    "rel_up": "相对强度·上涨满足率",
    "rel_down": "相对强度·抗跌满足率",
}

# 策略 id → DAL「来源策略」/报告展示标签（唯一真相源）。
# ⚠️ 约束：label.lower() 必须 == id —— adaptive_weights._norm_strategies 把 DAL 来源策略
# 转小写后与 ALL_STRATEGIES 求交集做归因，标签大小写写错会让该策略永远归因不到 edge。
STRATEGY_LABEL = {
    "boll": "Boll",
    "relativity": "Relativity",
    "theme": "Theme",
    "cctv": "CCTV",
    "momentum": "Momentum",
    "quality": "Quality",
    "value": "Value",
    "size": "Size",
    "boll_oversold": "Boll_Oversold",
    "boll_near_lower": "Boll_Near_Lower",
    "boll_mid_pullback": "Boll_Mid_Pullback",
    "boll_squeeze": "Boll_Squeeze",
    "rel_up": "Rel_Up",
    "rel_down": "Rel_Down",
}

# 策略 id 规范顺序（供报告/权重行动态渲染；与 fusion 命中标签顺序一致）
STRATEGY_ORDER = [
    "boll", "relativity", "theme", "cctv", "momentum",
    "quality", "value", "size",
    "boll_oversold", "boll_near_lower", "boll_mid_pullback", "boll_squeeze",
    "rel_up", "rel_down",
]

# 展示顺序（与既有策略认知一致，便于阅读）：同一族的原子紧随其复合体排布
FACTOR_TYPE_ORDER = [
    "动量",
    "反转·均值回归",
    "反转·超卖",
    "反转·近下轨",
    "反转·中轨回踩",
    "反转·带宽收口",
    "相对强度·资金流",
    "相对强度·上涨满足率",
    "相对强度·抗跌满足率",
    "题材",
    "事件·舆情",
    "基本面·质量",
    "基本面·估值",
    "基本面·规模",
    "其他",
]

_DEFAULT_TYPE = "其他"


def label_of(strategy: str) -> str:
    """策略 id → DAL/报告标签（未注册时回退原样字符串，保证 fail-soft）。"""
    key = _normalize(strategy)
    return STRATEGY_LABEL.get(key, str(strategy).strip())


def _assert_label_invariant() -> None:
    """守住「标签小写 == id」这一归因硬约束。

    归因链路（adaptive_weights._norm_strategies）把 DAL「来源策略」转小写后与
    ALL_STRATEGIES 求交集；标签一旦与 id 大小写/拼写不一致，该策略会**静默**归因不到
    edge（表现为权重长期停在无证据 floor）。这里的断言让错误在导入期就暴露。
    """
    for sid, label in STRATEGY_LABEL.items():
        if label.lower() != sid:
            raise AssertionError(
                f"STRATEGY_LABEL[{sid!r}]={label!r} 的 lower() 必须等于 id，否则归因失效"
            )
    missing = [s for s in STRATEGY_ORDER if s not in STRATEGY_FACTOR_TYPE]
    if missing:
        raise AssertionError(f"STRATEGY_ORDER 含未注册因子类型的策略：{missing}")


_assert_label_invariant()


def _normalize(name: str) -> str:
    """策略名归一化：去空白、转小写，兼容 'Boll' / 'boll' / 'Momentum' 等写法。"""
    return str(name).strip().lower()


def factor_type_of(strategy: str, default: str = _DEFAULT_TYPE) -> str:
    """单个策略 → 主因子类型。"""
    return STRATEGY_FACTOR_TYPE.get(_normalize(strategy), default)


def factor_types_of_source(source: str, default: str = _DEFAULT_TYPE) -> list[str]:
    """来源策略串（如 'Boll/Momentum' 或 'cctv'）→ 去重后的因子类型列表。

    用于明细展示与贡献度统计：一个 pick 可能命中多个策略，故可能返回多个类型。
    """
    if not source:
        return [default]
    out: list[str] = []
    for part in str(source).replace("、", "/").split("/"):
        part = part.strip()
        if not part:
            continue
        t = factor_type_of(part, default)
        if t not in out:
            out.append(t)
    return out or [default]


def rollup_counts(counts_by_strategy: dict[str, int]) -> dict[str, int]:
    """{策略: 计数} → {因子类型: 计数} 归并（同类型策略计数相加）。

    归并顺序按 FACTOR_TYPE_ORDER，未列出的类型（理论不该出现）追加在末尾。
    """
    agg: dict[str, int] = {}
    for strategy, cnt in counts_by_strategy.items():
        t = factor_type_of(strategy)
        agg[t] = agg.get(t, 0) + (cnt or 0)
    ordered = {k: agg[k] for k in FACTOR_TYPE_ORDER if k in agg}
    for k in agg:  # 兜底：不在预定义顺序里的类型
        if k not in ordered:
            ordered[k] = agg[k]
    return ordered
