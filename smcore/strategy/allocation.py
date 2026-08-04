"""策略仓位分配 —— 基于市场状态与信号可用性的权重计算。

从 auto_notify_boll.py 巨石抽出。纯函数，仅依赖 os.getenv 做参数覆盖，
可被两条主线复用（例如可视化界面想展示当前建议仓位）。
"""
from __future__ import annotations

import os


def env_int_percent(name: str, default: int) -> int:
    """从环境变量读 0-100 整数，缺失用默认值。"""
    text = os.getenv(name, "").strip()
    if not text:
        return int(default)
    try:
        value = int(float(text))
    except Exception:
        return int(default)
    return max(0, min(100, value))


def normalize_weight_map(weights: dict) -> dict:
    """权重归一化到和为 100 的整数 dict。"""
    normalized = {}
    for key, value in weights.items():
        try:
            normalized[key] = max(0, int(value))
        except Exception:
            normalized[key] = 0

    total = sum(normalized.values())
    if total <= 0:
        return {"boll": 40, "theme": 18, "cctv": 7, "relativity": 10, "momentum": 17, "cash": 8}

    scaled = {key: int(round(val * 100.0 / total)) for key, val in normalized.items()}
    delta = 100 - sum(scaled.values())
    if delta != 0:
        anchor = "cash" if "cash" in scaled else max(scaled, key=scaled.get)
        scaled[anchor] = max(0, scaled.get(anchor, 0) + delta)
    return scaled


def rebalance_for_signal_availability(weights, *, boll_rows_count, theme_rows_count, has_cctv_hot) -> dict:
    """根据信号可用性再平衡：无信号的策略权重转入 cash 或替代策略。"""
    adjusted = dict(weights)

    if boll_rows_count <= 0 and adjusted.get("boll", 0) > 0:
        adjusted["cash"] = adjusted.get("cash", 0) + adjusted.get("boll", 0)
        adjusted["boll"] = 0

    if theme_rows_count <= 0 and adjusted.get("theme", 0) > 0:
        adjusted["cash"] = adjusted.get("cash", 0) + adjusted.get("theme", 0)
        adjusted["theme"] = 0

    if (not has_cctv_hot) and adjusted.get("cctv", 0) > 0:
        if theme_rows_count > 0:
            adjusted["theme"] = adjusted.get("theme", 0) + adjusted.get("cctv", 0)
        else:
            adjusted["cash"] = adjusted.get("cash", 0) + adjusted.get("cctv", 0)
        adjusted["cctv"] = 0

    return normalize_weight_map(adjusted)


def format_position_units(weight, units: int = 10) -> str:
    """权重转"成"单位字符串（10成制）。"""
    return f"{weight * units / 100.0:.1f}成"


def build_strategy_allocation(regime, *, boll_rows_count, theme_rows_count, has_cctv_hot, macro_level) -> dict:
    """根据市场状态构建策略仓位分配。

    Args:
        regime: "趋势上行" / "下行防御" / 其他(震荡轮动)
        boll_rows_count/theme_rows_count: 各策略候选数
        has_cctv_hot: 是否有 CCTV 热点
        macro_level: 宏观风险等级 high/medium/low

    Returns:
        dict(base_weights, final_weights, ratio_line, unit_line, priority_line, adaption_line)
    """
    if regime == "趋势上行":
        base_weights = {
            "theme": env_int_percent("ALLOC_UP_THEME", 18),
            "cctv": env_int_percent("ALLOC_UP_CCTV", 8),
            "boll": env_int_percent("ALLOC_UP_BOLL", 30),
            "relativity": env_int_percent("ALLOC_UP_RELATIVITY", 10),
            "momentum": env_int_percent("ALLOC_UP_MOMENTUM", 18),
            "cash": env_int_percent("ALLOC_UP_CASH", 16),
        }
        priority_line = "- 执行优先级: Boll定节奏 > 动量强势确认 > 题材热度确认 > Relativity 强弱过滤"
    elif regime == "下行防御":
        base_weights = {
            "cash": env_int_percent("ALLOC_DOWN_CASH", 52),
            "boll": env_int_percent("ALLOC_DOWN_BOLL", 30),
            "relativity": env_int_percent("ALLOC_DOWN_RELATIVITY", 8),
            "theme": env_int_percent("ALLOC_DOWN_THEME", 5),
            "cctv": env_int_percent("ALLOC_DOWN_CCTV", 0),
            "momentum": env_int_percent("ALLOC_DOWN_MOMENTUM", 5),
        }
        priority_line = "- 执行优先级: 先控回撤，再做小仓位试错；题材/Relativity明显降权，动量仅留少量强势仓。"
    else:
        theme_weight = 16 if theme_rows_count >= 20 else 18
        cctv_weight = 12 if has_cctv_hot else 8
        boll_weight = 38 if boll_rows_count >= 10 else 42
        relativity_weight = 10 if macro_level != "high" else 8
        momentum_weight = 15
        cash_weight = 100 - theme_weight - cctv_weight - boll_weight - relativity_weight - momentum_weight

        base_weights = {
            "boll": env_int_percent("ALLOC_SIDE_BOLL", boll_weight),
            "theme": env_int_percent("ALLOC_SIDE_THEME", theme_weight),
            "cctv": env_int_percent("ALLOC_SIDE_CCTV", cctv_weight),
            "relativity": env_int_percent("ALLOC_SIDE_RELATIVITY", relativity_weight),
            "momentum": env_int_percent("ALLOC_SIDE_MOMENTUM", momentum_weight),
            "cash": env_int_percent("ALLOC_SIDE_CASH", cash_weight),
        }
        priority_line = "- 执行优先级: Boll定节奏，题材/CCTV找方向，Relativity做强弱确认，Momentum补强势维度。"

    normalized = normalize_weight_map(base_weights)
    final_weights = rebalance_for_signal_availability(
        normalized,
        boll_rows_count=boll_rows_count,
        theme_rows_count=theme_rows_count,
        has_cctv_hot=has_cctv_hot,
    )

    ratio_line = (
        "- 策略配比: "
        f"Boll低吸 {final_weights.get('boll', 0)}% | "
        f"题材轮动 {final_weights.get('theme', 0)}% | "
        f"CCTV跟随 {final_weights.get('cctv', 0)}% | "
        f"Relativity过滤 {final_weights.get('relativity', 0)}% | "
        f"Momentum强势 {final_weights.get('momentum', 0)}% | "
        f"现金观察 {final_weights.get('cash', 0)}%"
    )

    unit_line = (
        "- 仓位折算(10成): "
        f"Boll {format_position_units(final_weights.get('boll', 0))} | "
        f"题材 {format_position_units(final_weights.get('theme', 0))} | "
        f"CCTV {format_position_units(final_weights.get('cctv', 0))} | "
        f"Relativity {format_position_units(final_weights.get('relativity', 0))} | "
        f"Momentum {format_position_units(final_weights.get('momentum', 0))} | "
        f"现金 {format_position_units(final_weights.get('cash', 0))}"
    )

    adaption_notes = []
    if boll_rows_count <= 0:
        adaption_notes.append("Boll候选不足")
    if theme_rows_count <= 0:
        adaption_notes.append("题材候选不足")
    if not has_cctv_hot:
        adaption_notes.append("CCTV热点缺失")
    if adaption_notes:
        adaption_line = "- 动态调整: " + "，".join(adaption_notes) + "，对应仓位已自动回流至其他策略或现金。"
    else:
        adaption_line = "- 动态调整: 当前信号完整，按默认推荐比例执行。"

    return {
        "base_weights": normalized,
        "final_weights": final_weights,
        "ratio_line": ratio_line,
        "unit_line": unit_line,
        "priority_line": priority_line,
        "adaption_line": adaption_line,
    }
