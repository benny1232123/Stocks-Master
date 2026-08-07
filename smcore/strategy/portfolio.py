"""组合优化层（P1-1）：从融合得分 + 风险估计生成「原始目标权重」。

设计原则（与项目一贯约定一致）：
- 全配置驱动，无魔法数：method / score_power / score_floor / erc_max_iter 全部来自
  risk_config.json 的 ``portfolio`` 块，由 walk-forward 调出，禁止硬编码常量。
- 纯函数、fail-soft：输入不足（分数缺失 / 波动缺失 / 名单为空）时回退等权，
  绝不抛错阻断清单生成（与风险层四层约束的 fail-soft 风格一致）。
- 只算「初始权重倾斜」，不直接改入选名单。单名上限 / 板块上限 / 行业权重上限 /
  组合 β 等风险中性化约束仍在 fusion.py 既有的四层里施加（本模块与之正交）。

提供的分配方法（``method``）：
- ``equal_weight``   : 等权 1/n，最稳健的基线。
- ``score_weighted`` : 权重 ∝ (score 平移到非负后)^score_power（默认 power=1.5），
                       分越高配越多；平移只用于避免负分无法做幂，不改变排序。
- ``risk_parity_erc``: 等风险贡献（对角近似 w_i ∝ 1/vol_i），低波动票权重高，
                       天然分散、抗集中度，适合 A 股小样本（无需估计协方差矩阵）。
"""
from __future__ import annotations

import math
from typing import Optional

from smcore.utils.code import format_stock_code


def _normalize(weights: dict) -> dict:
    """把权重归一化为和为 1；若全为 0/空则退化为等权（fail-soft）。"""
    tot = sum(weights.values())
    if tot <= 0:
        n = len(weights)
        if n == 0:
            return {}
        return {k: 1.0 / n for k in weights}
    return {k: w / tot for k, w in weights.items()}


def _shifted_scores(scores: dict) -> dict:
    """把原始分平移到 >=0（减最小值 + 极小 epsilon），保留排序，避免负分无法做幂。

    仅做平移不做缩放，故 score_power 的相对含义不变；全部相等时平移后均为 eps，
    幂后相等 → 归一化得到等权，符合直觉。
    """
    vals = [v for v in scores.values() if v is not None]
    if not vals:
        return {k: 0.0 for k in scores}
    lo = min(vals)
    eps = 1e-6
    return {k: max((v if v is not None else lo) - lo + eps, 0.0) for k, v in scores.items()}


def compute_target_weights(
    codes,
    scores: dict,
    vols: dict,
    *,
    method: str = "score_weighted",
    score_power: float = 1.5,
    score_floor: float = 0.0,
    erc_max_iter: int = 50,
    risk_parity_fallback: str = "equal_weight",
) -> dict:
    """根据方法与风险估计，计算每只票的原始目标权重（和为 1）。

    Args:
        codes: 候选股代码列表（即入选顺序；内部统一格式化为 6 位字符串）。
        scores: {code: 综合评分 / 因子分}（可含 None）。
        vols: {code: 年化波动率}（可含 None）。
        method: ``equal_weight`` | ``score_weighted`` | ``risk_parity_erc``。
        score_power: ``score_weighted`` 幂次（超参，配置驱动）。
        score_floor: ``score_weighted`` 平移后的下限（默认 0，仅防负）。
        erc_max_iter: ``risk_parity_erc`` 迭代上限；对角近似下通常 1 步收敛，
                      预留给后续全协方差 ERC 扩展，不影响当前结果。
        risk_parity_fallback: vols 全部缺失时回退的方法（默认等权）。

    Returns:
        {code(6位): weight_frac(0..1, 和为1)}；输入为空返回 {}。
    """
    codes = [format_stock_code(c) for c in codes if format_stock_code(c)]
    if not codes:
        return {}
    # 以格式化后的 code 重新对齐 scores / vols
    s = {c: scores.get(c) for c in codes}
    v = {c: vols.get(c) for c in codes}

    method = str(method or "score_weighted").lower()

    if method == "score_weighted":
        shifted = _shifted_scores(s)
        # score_floor 仅作下限夹取，不改变有效分的相对排序
        shifted = {c: max(val, score_floor) for c, val in shifted.items()}
        if all(val <= 0 for val in shifted.values()):
            return _normalize({c: 1.0 for c in codes})
        try:
            w = {c: (shifted[c] ** score_power) if shifted[c] > 0 else 0.0 for c in codes}
        except (ValueError, OverflowError):
            # 极端输入（如超大 power）防御：退化等权
            return _normalize({c: 1.0 for c in codes})
        return _normalize(w)

    if method == "risk_parity_erc":
        have = [c for c in codes if v.get(c) and v[c] > 0]
        if not have:
            # 波动全缺失 → 回退（默认等权）
            fb = str(risk_parity_fallback).lower()
            if fb == "score_weighted":
                return compute_target_weights(codes, s, v, method="score_weighted",
                                              score_power=score_power, score_floor=score_floor,
                                              erc_max_iter=erc_max_iter)
            return _normalize({c: 1.0 for c in codes})
        # 对角 ERC：w_i ∝ 1/vol_i（归一），低波动票权重高；高波动票天然降权。
        w = {c: (1.0 / v[c]) if c in have else 0.0 for c in codes}
        # erc_max_iter 预留给后续全协方差迭代；当前对角近似已收敛，仅做一次归一化。
        return _normalize(w)

    # 未知方法 → 等权（fail-soft）
    return _normalize({c: 1.0 for c in codes})
