#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""策略 → 因子类型映射（报告/监控层重分类用，2026-09-17）。

用户要求：报告与监控不再「按策略名」分组，改为「按因子类型」分组。
本模块是**唯一**的映射来源，报告层（report.py）与监控层（factor_ic_monitor.py）
都从这里取，避免两处各写一套导致漂移。

设计原则（与「别过度工程 / 底层不动」一致）：
- 只做展示层的**标签与归并**，不触碰任何策略选股 / 融合 / 权重逻辑；
- 每策略归一个**主因子类型**（1:1），保证归并桶清晰、无歧义；
- 一个 pick 可命中多个策略（来源策略="PVCorr20/Vol20"），因此可映射到多个因子类型——
  `factor_types_of_source` 返回去重后的类型列表，供明细展示与贡献度统计。

2026-09-17 重构（本版本）：用户要求「完全去掉之前的策略，重新整一套因子候选」。
原 boll/relativity/theme/cctv/momentum 复合策略、基本面 quality/value/size 复合体、
boll/relativity 的 6 个原子、C 批基本面单指标原子（ROE/毛利率/EP/BP）与反向动量
lowmom20 全部移除，菜单收缩为 **A 批 12 个因子池（factor_zoo 预注册文法 v1）存活价格因子**：

  - pvcorr20 / pvcorr60         量价相关
  - cvamt20 / cvamt60           成交稳定
  - skew20 / skew60             收益偏度
  - vratio20_120 / vratio10_60  波动比
  - distlo10 / distlo60         位置·距低点
  - vol20                       波动
  - illiq20                     非流动性

这些因子公式 1:1 复用 `factor_zoo.compute_factor`（不改窗口、不改先验方向），保证
「已验证存活」成立。新因子候选（来自因子挖掘系统 / 开源 Alpha101 等）后续按同一注册表
协议接入：在 `factor_types` 注册 + 产出同名 `Stock-Selection-<Label>-<date>.csv` 即可，
fusion / 权重 / 报告全自动跟进（注册表驱动，零硬编码）。

⚠️ 菜单硬上限 ≈30–35：整数百分比粒度下，探索池（占 30%）摊到 N 个无证据策略会被
舍入到 0%，无法「毕业」。当前 12 个在极安全区，后续加因子须先扩粒度或改门控。
"""
from __future__ import annotations

# 主因子类型映射：key 用小写策略 id（与 adaptive_weights.ALL_STRATEGIES 对齐）
STRATEGY_FACTOR_TYPE = {
    "pvcorr20": "量价相关",
    "pvcorr60": "量价相关",
    "cvamt20": "成交稳定",
    "cvamt60": "成交稳定",
    "skew20": "收益偏度",
    "skew60": "收益偏度",
    "vratio20_120": "波动比",
    "vratio10_60": "波动比",
    "distlo10": "位置·距低点",
    "distlo60": "位置·距低点",
    "vol20": "波动",
    "illiq20": "非流动性",
}

# 策略 id → DAL「来源策略」/报告展示标签（唯一真相源）。
# ⚠️ 约束：label.lower() 必须 == id —— adaptive_weights._norm_strategies 把 DAL 来源策略
# 转小写后与 ALL_STRATEGIES 求交集做归因，标签大小写写错会让该策略永远归因不到 edge。
# 同时 label 就是 CSV 文件名里的因子名（`Stock-Selection-<label>-<date>.csv`），
# 生成脚本必须用本表取值，勿另起字符串。
STRATEGY_LABEL = {
    "pvcorr20": "PVCorr20",
    "pvcorr60": "PVCorr60",
    "cvamt20": "CVAmt20",
    "cvamt60": "CVAmt60",
    "skew20": "Skew20",
    "skew60": "Skew60",
    "vratio20_120": "VRatio20_120",
    "vratio10_60": "VRatio10_60",
    "distlo10": "DistLo10",
    "distlo60": "DistLo60",
    "vol20": "Vol20",
    "illiq20": "Illiq20",
}

# 策略 id 规范顺序（供报告/权重行动态渲染；与 fusion 命中标签顺序一致）
STRATEGY_ORDER = [
    "pvcorr20", "pvcorr60", "cvamt20", "cvamt60", "skew20", "skew60",
    "vratio20_120", "vratio10_60", "distlo10", "distlo60", "vol20", "illiq20",
]

# 展示顺序（与既有策略认知一致，便于阅读）：同一族归并桶紧邻排布
FACTOR_TYPE_ORDER = [
    "量价相关",
    "成交稳定",
    "收益偏度",
    "波动比",
    "位置·距低点",
    "波动",
    "非流动性",
    "其他",
]

# 已退役（legacy）策略名集合：boll/relativity/theme/cctv/momentum 是上一版复合策略，
# 在 2026-09-17 重构中已从活跃菜单移除（菜单收缩为 A 批 12 个因子池）。
# 该集合是这些遗留名字的**唯一真源**，供 daily_backtest.py / replay_history.py /
# backtest/engine.py / backtest/strategies.py 等遗留子系统统一引用，避免各自硬编码
# 导致「漏 momentum（STRAT_MAP）」「漏 boll（STRATS_FROZEN）」这类漂移。
# ⚠️ 活跃菜单（STRATEGY_ORDER）不含这些名字；若未来某个 legacy 名字复活，
# 须先从本集合移除并加入 STRATEGY_ORDER，不要两边同时保留。
RETIRED_STRATEGY_NAMES = ("boll", "relativity", "theme", "cctv", "momentum")

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
    unlabeled = [s for s in STRATEGY_ORDER if s not in STRATEGY_LABEL]
    if unlabeled:
        raise AssertionError(f"STRATEGY_ORDER 含未注册标签的策略：{unlabeled}")
    # 因子类型必须全部登记在 FACTOR_TYPE_ORDER 里：否则 rollup_counts 的排序兜底会把它
    # 追加到「其他」之后（展示顺序漂移，前端 FACTOR_TYPE_ORDER 也会对不上）。
    declared = {STRATEGY_FACTOR_TYPE[s] for s in STRATEGY_ORDER}
    unlisted = sorted(declared - set(FACTOR_TYPE_ORDER))
    if unlisted:
        raise AssertionError(f"以下因子类型未登记进 FACTOR_TYPE_ORDER：{unlisted}")


_assert_label_invariant()


def _normalize(name: str) -> str:
    """策略名归一化：去空白、转小写，兼容 'PVCorr20' / 'pvcorr20' 等写法。"""
    return str(name).strip().lower()


def factor_type_of(strategy: str, default: str = _DEFAULT_TYPE) -> str:
    """单个策略 → 主因子类型。"""
    return STRATEGY_FACTOR_TYPE.get(_normalize(strategy), default)


def factor_types_of_source(source: str, default: str = _DEFAULT_TYPE) -> list[str]:
    """来源策略串（如 'PVCorr20/Vol20' 或 'pvcorr20'）→ 去重后的因子类型列表。

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
