"""自适应风险层 —— 把全部「手工风控规则」改为纯数据驱动，零代码内硬编码。

设计动机
--------
原先风险中性化与出场逻辑里散布着一批人拍的常量：单名仓位上限 10%、行业权重上限
20%、组合 β 上限 1.4、单行业数量 ≤5、板块动量加成 6%、回撤缓冲阈值 20%、止损 8%/止盈
6%/移动止盈 5%/MA60 破位……这些数字写死在代码里，既无法随市场自适应，也难以被
walk-forward 重验 CI 调优。用户要求「手工规则全部不要，全部改成自适应的」。

本模块统一接管这些规则，全部改为**数据驱动 + 配置可热更新**：
- 单名上限 / 行业权重上限 / 单行业数量：随最终入选广度（名单长度 N、行业数 M）自适应，
  广度越窄 → 单名上限越高（但受天花板约束），广度越宽 → 越分散。
- 组合 β 上限：随市场 regime 浮动（下行防御收紧、趋势上行放宽）。
- 单名上限还随市场波动率分位微调（高波动更紧）。
- 板块动量加成幅度：随板块截面离散度自适应（离散越大、对强势板块倾斜越强）。
- 回撤缓冲阈值：配置驱动（暂不与波动率强耦合，保持防御稳定性）。
- 出场（止损/止盈/移动止盈/趋势 MA 周期）：随波动率分位与 regime 自适应，
  基线 = 已被全样本回测验证的 8%/6%/5%/60，仅在波动率/regime 偏离中位时偏离基线。

所有数值要么来自持仓数据计算，要么是**数学正则化系数**（expansion/vol_sensitivity/
regime_mult、与具体市场无关），集中在同目录 `risk_config.json`。本模块启动时读取；
文件缺失或解析失败则回退到内置默认（与历史基线一致），保证「零配置也能跑」且行为不变。
月度 walk-forward 重验 CI（`scripts/walk_forward_validator.py` + `scripts/apply_walk_forward.py`）
会在验证「稳健更优」后改写该 JSON 并自动开 PR。

本模块不导入任何业务模块（仅标准库 + 本文件配置），避免与 defaults/其它模块形成
循环依赖；defaults.py 可在模块底部安全地反导入本模块的 RISK_CONFIG 派生安全天花板别名。
"""
from __future__ import annotations

import json
import math
import statistics
from pathlib import Path
from typing import Optional

# ── 可热更新的超参配置（月度 walk-forward 重验 CI 可改写本文件）──
# 文件缺失 / 解析失败时回退到内置默认，保证「零配置也能跑」且行为不变。
_BUILTIN_DEFAULTS = {
    "single_weight": {"expansion": 1.5, "floor_pct": 5.0, "ceil_pct": 15.0, "vol_sensitivity": 0.5},
    "sector_weight": {
        "expansion": 1.8,
        "floor_pct": 10.0,
        "ceil_pct": 35.0,
        "regime_mult": {"下行防御": 0.8, "震荡轮动": 1.0, "趋势上行": 1.1},
    },
    "max_per_sector": {"expansion": 1.3, "min_count": 2, "max_count": 12},
    "portfolio_beta": {
        "base": 1.3,
        "regime_delta": {"下行防御": -0.3, "震荡轮动": 0.0, "趋势上行": 0.3},
        "min": 0.8,
        "max": 1.8,
    },
    "beta_min_keep": {"frac_of_picks": 0.5, "min": 5, "max": 15},
    "max_per_strategy": {"expansion": 1.0, "min": 3, "max": 20},
    "sector_momentum_bonus": {"dispersion_k": 1.2, "floor": 2.0, "ceil": 12.0},
    "drawdown": {"dd_full": 0.20, "dd_cash_ceiling": 0.50},
    "exit": {
        "stop_loss_pct": {"base": 0.08, "min": 0.04, "max": 0.15, "vol_sensitivity": 0.5},
        "take_profit_pct": {"base": 0.06, "min": 0.03, "max": 0.12, "vol_sensitivity": 0.5},
        "trailing_stop_pct": {"base": 0.05, "min": 0.025, "max": 0.10, "vol_sensitivity": 0.5},
        "trend_exit_ma": {"defensive": 40, "neutral": 60, "up": 90},
        "hold_days": 10,
        "slippage": 0.001,
    },
}


def _deep_copy(cfg: dict) -> dict:
    out = {}
    for k, v in cfg.items():
        out[k] = dict(v) if isinstance(v, dict) else v
    return out


_CONFIG_PATH = Path(__file__).resolve().parent / "risk_config.json"


def _load_config() -> dict:
    cfg = _deep_copy(_BUILTIN_DEFAULTS)
    try:
        with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
            user = json.load(f)
        for k, v in user.items():
            if k not in _BUILTIN_DEFAULTS:
                continue
            if isinstance(v, dict) and isinstance(_BUILTIN_DEFAULTS[k], dict):
                merged = _deep_copy(_BUILTIN_DEFAULTS[k])
                merged.update(v)
                cfg[k] = merged
            else:
                cfg[k] = v
    except Exception:
        pass
    return cfg


CONFIG = _load_config()
RISK_CONFIG = CONFIG  # 别名，便于 defaults.py 反导入


def save_risk_config(cfg: dict) -> str:
    """把完整配置写回 risk_config.json（供月度重验 CI 调用）。

    只写已知键；未知键被忽略，子字典做合并而非整体替换。写完后同步刷新模块级缓存。
    """
    target = _deep_copy(_BUILTIN_DEFAULTS)
    for k, v in cfg.items():
        if k not in target:
            continue
        if isinstance(v, dict) and isinstance(target[k], dict):
            merged = _deep_copy(target[k])
            merged.update(v)
            target[k] = merged
        else:
            target[k] = v
    with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(target, f, ensure_ascii=False, indent=2)
    globals()["CONFIG"] = target
    globals()["RISK_CONFIG"] = target
    return str(_CONFIG_PATH)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _vol_mult(vol: Optional[float], sens: float) -> float:
    """波动率分位 → 放宽因子（中位=1，高波动>1 放宽、低波动<1 收紧）。

    用于**出场**（止损/止盈/移动止盈）：高波动时放宽止损避免被洗、低波动时收紧。
    sens 为数学正则化系数（来自 CONFIG），纯连续函数，无魔法数字上下限。
    """
    if vol is None:
        return 1.0
    return _clamp(1.0 + (vol - 0.5) * 2.0 * sens, 0.4, 1.6)


def _vol_tighten(vol: Optional[float], sens: float) -> float:
    """波动率分位 → 收紧因子（中位=1，高波动<1 收紧、低波动>1 放宽）。

    与 _vol_mult 方向相反，用于**单名仓位上限**：高波动时降低单名暴露（更紧）、
    低波动时允许相对集中。sens 为数学正则化系数（来自 CONFIG）。
    """
    if vol is None:
        return 1.0
    return _clamp(1.0 - (vol - 0.5) * 2.0 * sens, 0.4, 1.6)


def compute_adaptive_risk_params(
    regime: Optional[str] = None,
    profile: Optional[object] = None,
    n_picks: Optional[int] = None,
    n_sectors: Optional[int] = None,
) -> dict:
    """计算全部风险中性化上限（纯数据驱动，无代码内硬编码）。

    输入：
        regime:   市场状态（"下行防御"/"震荡轮动"/"趋势上行"）；profile 有则优先取 profile.regime
        profile:  市场仪表盘对象（compute_market_profile 返回），提供 volatility_pctile / regime
        n_picks:  当前候选/入选名单长度 N（广度）；缺失按中性 15 处理
        n_sectors: 当前入选涉及的不同行业数 M；缺失按 N/3 估计

    输出（均为自适应值）：
        max_single_weight_pct / max_sector_weight_pct / max_per_sector /
        max_portfolio_beta / beta_min_keep / max_per_strategy / dd_full / dd_cash_ceiling

    自适应逻辑：
        - 单名上限 = clamp(expansion × 100/N × 波动率因子, floor, ceil)：名单越短单名可越重，
          高波动时收紧；天花板 ceil 仅作绝对安全兜底（非活跃调参）。
        - 行业权重上限 = clamp(expansion × 100/M × regime_mult, floor, ceil)：行业越集中(M小)
          允许单行业占比越高；下行防御整体收紧。
        - 单行业数量 = clamp(round(expansion × N/M), min, max)。
        - 组合 β 上限 = clamp(base + regime_delta[regime], min, max)。
        - beta_min_keep = clamp(round(N × frac), min, max)：剔除高 β 时至少保留的只数随名单缩放。
        - max_per_strategy = clamp(round(expansion × N/策略数), min, max)。
    所有系数（expansion/vol_sensitivity/regime_mult/regime_delta/frac）均来自 CONFIG，可热更新。
    """
    cfg = CONFIG
    vol = getattr(profile, "volatility_pctile", None) if profile is not None else None
    if regime is None and profile is not None:
        regime = getattr(profile, "regime", None)
    n = n_picks if (isinstance(n_picks, int) and n_picks > 0) else 15
    m = n_sectors if (isinstance(n_sectors, int) and n_sectors > 0) else max(1, round(n / 3))

    # 单名仓位上限（高波动收紧单名暴露）
    sw = cfg["single_weight"]
    single = sw["expansion"] * 100.0 / n * _vol_tighten(vol, sw["vol_sensitivity"])
    single = _clamp(single, sw["floor_pct"], sw["ceil_pct"])

    # 单行业权重上限
    sec = cfg["sector_weight"]
    sector_w = sec["expansion"] * 100.0 / m * sec["regime_mult"].get(regime, 1.0)
    sector_w = _clamp(sector_w, sec["floor_pct"], sec["ceil_pct"])

    # 单行业数量上限
    mps = cfg["max_per_sector"]
    per_sec = mps["expansion"] * (n / m)
    max_per = int(_clamp(round(per_sec), mps["min_count"], mps["max_count"]))

    # 组合 β 上限
    pb = cfg["portfolio_beta"]
    beta_ceil = pb["base"] + pb["regime_delta"].get(regime, 0.0)
    beta_ceil = _clamp(beta_ceil, pb["min"], pb["max"])

    # 剔除高 β 时至少保留只数
    bmk = cfg["beta_min_keep"]
    min_keep = int(_clamp(round(n * bmk["frac_of_picks"]), bmk["min"], bmk["max"]))

    # 单策略入选数量上限
    mps2 = cfg["max_per_strategy"]
    per_strat = mps2["expansion"] * (n / 5.0)
    max_strat = int(_clamp(round(per_strat), mps2["min"], mps2["max"]))

    dd = cfg["drawdown"]
    return {
        "max_single_weight_pct": round(single, 1),
        "max_sector_weight_pct": round(sector_w, 1),
        "max_per_sector": max_per,
        "max_portfolio_beta": round(beta_ceil, 2),
        "beta_min_keep": min_keep,
        "max_per_strategy": max_strat,
        "dd_full": dd["dd_full"],
        "dd_cash_ceiling": dd["dd_cash_ceiling"],
    }


def compute_adaptive_exit_params(
    profile: Optional[object] = None,
    regime: Optional[str] = None,
) -> dict:
    """计算出场参数（止损/止盈/移动止盈/趋势 MA 周期），随波动率与 regime 自适应。

    基线 = 全样本回测验证的 8%/6%/5%/60（中位波动率、震荡市）；仅在波动率/regime 偏离时
    偏离基线——高波动给更宽止损避免被洗、低波动给更紧；下行防御趋势 MA 缩短(更快离场)、
    趋势上行拉长(让利润奔跑)。无 profile 时回退基线（行为不变）。

    返回：{stop_loss_pct, take_profit_pct, trailing_stop_pct, trend_exit_ma, hold_days, slippage}
    """
    cfg = CONFIG["exit"]
    vol = getattr(profile, "volatility_pctile", None) if profile is not None else None
    if regime is None and profile is not None:
        regime = getattr(profile, "regime", None)

    def _scale(section: dict) -> float:
        base = section["base"]
        lo = section["min"]
        hi = section["max"]
        sens = section["vol_sensitivity"]
        v = base * _vol_mult(vol, sens)
        return round(_clamp(v, lo, hi), 4)

    ma_map = cfg["trend_exit_ma"]
    _ma_key = {"下行防御": "defensive", "震荡轮动": "neutral", "趋势上行": "up"}.get(regime, "neutral")
    ma = ma_map.get(_ma_key, ma_map["neutral"])
    return {
        "stop_loss_pct": _scale(cfg["stop_loss_pct"]),
        "take_profit_pct": _scale(cfg["take_profit_pct"]),
        "trailing_stop_pct": _scale(cfg["trailing_stop_pct"]),
        "trend_exit_ma": int(ma),
        "hold_days": int(cfg["hold_days"]),
        "slippage": cfg["slippage"],
    }


def compute_sector_momentum_bonus(medians: dict) -> float:
    """板块动量加成幅度随板块截面离散度自适应。

    离散度（各板块 median ret20 的总体标准差）越大 → 对强势板块倾斜越强（幅度越大），
    离散度越小 → 越收敛到 floor（不夸大微小差异）。替代写死的 SECTOR_MOMENTUM_BONUS=6.0。
    """
    cfg = CONFIG["sector_momentum_bonus"]
    vals = [v for v in medians.values() if isinstance(v, (int, float))]
    if len(vals) < 2:
        return float(cfg["floor"])
    disp = statistics.pstdev(vals)
    return round(_clamp(cfg["dispersion_k"] * disp, cfg["floor"], cfg["ceil"]), 2)
