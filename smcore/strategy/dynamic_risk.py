"""动态风险引擎：统一输出出场 + 仓位上限 + 现金/仓位规模（全连续单调函数）。

中性点 = risk_config.json / adaptive_weights_config.json 现值（通过复用
risk_rules.compute_adaptive_exit_params / compute_adaptive_risk_params 与
adaptive_weights 的 cash_from_* 函数保证回退行为不变）。

新增规则：
1. 波动率无条件钳制（regime 只影响中间带松动程度）：vol>=0.85 → scale<=0.3；
   0.75~0.85 防御/震荡 →0.3、趋势上行线性 0.45→0.3；下行防御 0.60~0.75 线性 0.5→0.3。
   覆盖 regime 误判（熊市段常判"趋势上行"）导致的满仓高波。
2. 止损高波收紧：vol>=0.70 → stop_loss <= 10%；vol>=0.85 → <= 8%。
   替代 risk_rules 现"高波放宽止损"（15% 上限 → avg_loss -15~-22%）的逆向机制。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from smcore.strategy.adaptive_weights import (
    cash_from_drawdown,
    cash_from_regime,
    cash_from_volatility,
)
from smcore.strategy.risk_rules import (
    compute_adaptive_exit_params,
    compute_adaptive_risk_params,
)

# 波动率无条件钳制（vol 为 volatility_pctile，0~1）
_VOL_CAP_HARD = 0.85           # vol>=0.85：任何 regime 都压到硬地板
_VOL_CAP_MID = 0.75            # vol>=0.75：防御/震荡直接硬地板，趋势上行走缓坡
_VOL_CAP_DEFENSE = 0.60        # vol>=0.60 且下行防御：进入缓坡
_VOL_CAP_SCALE_HARD = 0.3
_VOL_CAP_SCALE_MID = 0.45
_VOL_CAP_SCALE_DEFENSE = 0.5

# 止损收紧阈值（vol>=阈值的分位段，把 stop_loss_pct 压到对应上限）
_STOP_TIGHT_VOL_HARD = 0.85
_STOP_TIGHT_VOL = 0.70
_STOP_TIGHT_LO = 0.08
_STOP_TIGHT_MID = 0.10


@dataclass
class DynamicParams:
    """单一日/单一时点的全部风险参数（出场 + 仓位上限 + 现金/规模）。"""

    stop_loss_pct: float
    take_profit_pct: float
    trailing_stop_pct: float
    trend_exit_ma: int
    hold_days: int
    slippage: float
    max_single_weight_pct: float
    max_sector_weight_pct: float
    max_per_sector: int
    max_portfolio_beta: float
    beta_min_keep: int
    max_per_strategy: int
    dd_full: float
    dd_cash_ceiling: float
    cash_pct: float
    capital_scale: float


def _scale_cap(regime: Optional[str], vol_pctile: Optional[float]) -> Optional[float]:
    """波动率钳制：高波动时段对 capital_scale 施加上限；返回 None 表示不钳制。

    - vol>=0.85：无条件 0.3（regime 无关，防真实熊市被误判为趋势上行而满仓）
    - 0.75<=vol<0.85：下行防御/震荡轮动 → 0.3；趋势上行 → 线性 0.45→0.3
    - 0.60<=vol<0.75 且下行防御：线性 0.5→0.3
    """
    if vol_pctile is None:
        return None
    if vol_pctile >= _VOL_CAP_HARD:
        return _VOL_CAP_SCALE_HARD
    if vol_pctile >= _VOL_CAP_MID:
        if regime == "趋势上行":
            frac = (vol_pctile - _VOL_CAP_MID) / (_VOL_CAP_HARD - _VOL_CAP_MID)
            return _VOL_CAP_SCALE_MID + (_VOL_CAP_SCALE_HARD - _VOL_CAP_SCALE_MID) * frac
        return _VOL_CAP_SCALE_HARD
    if regime == "下行防御" and vol_pctile >= _VOL_CAP_DEFENSE:
        frac = (vol_pctile - _VOL_CAP_DEFENSE) / (_VOL_CAP_MID - _VOL_CAP_DEFENSE)
        return _VOL_CAP_SCALE_DEFENSE + (_VOL_CAP_SCALE_HARD - _VOL_CAP_SCALE_DEFENSE) * frac
    return None


def _tighten_stop_loss(stop_loss_pct: Optional[float], vol_pctile: Optional[float]) -> Optional[float]:
    """高波动收紧止损：阻止 risk_rules 现"高波放宽止损"把单票亏损扛到 12~15%。"""
    if stop_loss_pct is None or vol_pctile is None:
        return stop_loss_pct
    if vol_pctile >= _STOP_TIGHT_VOL_HARD:
        return min(stop_loss_pct, _STOP_TIGHT_LO)
    if vol_pctile >= _STOP_TIGHT_VOL:
        return min(stop_loss_pct, _STOP_TIGHT_MID)
    return stop_loss_pct


def compute_dynamic_risk(
    profile: Optional[object] = None,
    regime: Optional[str] = None,
    drawdown_pct: Optional[float] = None,
    n_picks: Optional[int] = None,
    n_sectors: Optional[int] = None,
) -> DynamicParams:
    """计算动态风险参数。输入与现有 risk_rules / cash_from_* 相同，输出合并为单一对象。

    现金链路：cash_from_volatility → cash_from_regime → cash_from_drawdown（只追加），
    再对高波动时段施加上限钳制；出场/仓位上限延用 risk_rules 现有自适应（中性=现值）。
    """
    exit_p = compute_adaptive_exit_params(profile, regime)
    risk_p = compute_adaptive_risk_params(regime, profile, n_picks, n_sectors)

    vol = getattr(profile, "volatility_pctile", None) if profile is not None else None
    if regime is None and profile is not None:
        regime = getattr(profile, "regime", None)

    cash_pct = cash_from_volatility(vol)
    cash_pct = cash_from_regime(regime, cash_pct)
    if drawdown_pct and drawdown_pct > 0:
        cash_pct = min(100.0, cash_pct + cash_from_drawdown(drawdown_pct))
    scale = max(0.0, 1.0 - cash_pct / 100.0)

    cap = _scale_cap(regime, vol)
    if cap is not None:
        scale = min(scale, cap)
        cash_pct = (1.0 - scale) * 100.0

    merged = {**exit_p, **risk_p}
    merged["stop_loss_pct"] = _tighten_stop_loss(merged["stop_loss_pct"], vol)
    return DynamicParams(
        **merged,
        cash_pct=round(cash_pct, 1),
        capital_scale=round(scale, 3),
    )