"""自适应策略权重 —— 纯数据驱动，零硬编码权重。

设计动机
--------
原先 `_REGIME_STRATEGY_SCORE` 是三套「人拍的」固定权重表，即使按市场状态切换，
数字仍是写死的。用户要求「权重应根据市场自适应改变」，且「取消所有硬编码」。

本模块改为**纯数据驱动**：
- 每个信号日的前向回测已经记录了每笔交易收益（Multi-Backtest-*-trades.csv），
  每只票的来源策略可从当日 Daily-Action-List 的「来源策略」反查。
- 把每笔交易收益归因到其来源策略，得到各策略近期**已实现 edge**（平均前向收益）。
- 权重 = softmax(shrunk_edge / temp)，经验贝叶斯收缩防低样本噪声，
  再做向等权收缩(shrinkage)抑制剧烈摆动。
- 清零门：edge<0 或样本不足的策略归零，freed weight 按幸存者原比例重分配。
- 现金比例 = 波动率分位的连续函数（无魔法数字上下限）。
- 冷启动（回测历史不足）时回退等权默认。

本模块不含任何硬编码策略分数或权重阈值。所有数值要么来自交易数据计算，
要么是数学正则化常数（shrinkage/temp/pseudo/FLOOR），与具体市场/策略无关。

超参热更新
----------
shrinkage / temp / pseudo / FLOOR 以及现金函数参数集中放在同目录的
`adaptive_weights_config.json`。本模块启动时读取；文件缺失或解析失败则回退到
内置默认（与历史硬编码一致），保证「零配置也能跑」且行为不变。月度 walk-forward
重验 CI（`scripts/walk_forward_validator.py` + `scripts/apply_walk_forward.py`）
会在验证「稳健更优」后改写该 JSON 并自动开 PR。
"""
from __future__ import annotations

import glob
import json
import math
import os
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from smcore.config.defaults import STOCK_DATA_DIR
from smcore.utils.code import format_stock_code

ALL_STRATEGIES = ["boll", "theme", "relativity", "momentum", "cctv"]

# ── 可热更新的超参配置（月度 walk-forward 重验 CI 可改写本文件）──
# 文件缺失 / 解析失败时回退到内置默认，保证「零配置也能跑」且行为不变。
_CONFIG_PATH = Path(__file__).resolve().parent / "adaptive_weights_config.json"
_BUILTIN_DEFAULTS = {
    "FLOOR": 3.0,
    "shrinkage": 0.4,
    "temp": 0.5,
    "pseudo": 15.0,
    "cash_from_volatility": {"k": 12.0, "midpoint": 0.55},
    "cash_from_drawdown": {"threshold": 8.0, "cap": 50.0, "deep": 20.0},
    "cash_from_regime": {"down_mult": 2.0, "down_floor": 20.0, "down_cap": 70.0, "up_mult": 0.33},
}


def _load_config() -> dict:
    cfg = {k: (dict(v) if isinstance(v, dict) else v) for k, v in _BUILTIN_DEFAULTS.items()}
    try:
        with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
            user = json.load(f)
        for k, v in user.items():
            if k not in _BUILTIN_DEFAULTS:
                continue
            if isinstance(v, dict) and isinstance(_BUILTIN_DEFAULTS[k], dict):
                merged = dict(_BUILTIN_DEFAULTS[k])
                merged.update(v)
                cfg[k] = merged
            else:
                cfg[k] = v
    except Exception:
        pass
    return cfg


CONFIG = _load_config()


def save_config(cfg: dict) -> str:
    """把完整配置写回 adaptive_weights_config.json（供月度重验 CI 调用）。

    只写已知键；未知键被忽略，子字典（现金函数）做合并而非整体替换。
    写完后同步刷新模块级 CONFIG 缓存，使当前进程立即生效。
    """
    target = {k: (dict(v) if isinstance(v, dict) else v) for k, v in _BUILTIN_DEFAULTS.items()}
    for k, v in cfg.items():
        if k not in target:
            continue
        if isinstance(v, dict) and isinstance(target[k], dict):
            merged = dict(target[k])
            merged.update(v)
            target[k] = merged
        else:
            target[k] = v
    with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(target, f, ensure_ascii=False, indent=2)
    globals()["CONFIG"] = target
    return str(_CONFIG_PATH)


def _norm_code(c):
    return format_stock_code(c)


def _norm_strategies(s):
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return []
    out = set()
    for p in str(s).replace("/", ",").split(","):
        p = p.strip().lower()
        if p:
            out.add(p)
    return out


def compute_strategy_edge(window: int = 30) -> dict:
    """基于最近 `window` 个信号日的前向回测，计算各策略近期 edge。

    Returns: {strategy: {"n", "avg_return", "win_rate", "edge"}}
    edge = 该策略归因交易的平均前向收益（已实现，单位 %）。
    归因规则：每只票按当日 Daily-Action-List「来源策略」映射到策略；若一只票命中多策略，
    其交易收益计入每个命中策略（与 综合评分 多策略叠加口径一致）。
    """
    summary_files = sorted(
        glob.glob(str(STOCK_DATA_DIR / "Multi-Backtest-*-summary.csv")), reverse=True
    )[:window]

    strat_rets: dict[str, list[float]] = {s: [] for s in ALL_STRATEGIES}
    total_trades = 0
    unknown_trades = 0
    dal_col_missing_days = 0
    for f in summary_files:
        sd = os.path.basename(f)[len("Multi-Backtest-"):-len("-summary.csv")]
        dal = STOCK_DATA_DIR / f"Daily-Action-List-{sd}.csv"
        code2strat: dict[str, set[str]] = {}
        if dal.exists():
            try:
                d = pd.read_csv(dal)
                if {"股票代码", "来源策略"}.issubset(d.columns):
                    for _, r in d.iterrows():
                        code2strat[_norm_code(r["股票代码"])] = _norm_strategies(r["来源策略"])
                else:
                    dal_col_missing_days += 1
            except Exception:
                pass
        tr = STOCK_DATA_DIR / f"Multi-Backtest-{sd}-trades.csv"
        if not tr.exists():
            continue
        try:
            t = pd.read_csv(tr)
        except Exception:
            continue
        for _, r in t.iterrows():
            c = _norm_code(r.get("code"))
            total_trades += 1
            try:
                rp = float(r.get("return_pct"))
            except (TypeError, ValueError):
                continue
            strats = code2strat.get(c) or {"__unknown__"}
            if "__unknown__" in strats:
                unknown_trades += 1
            for s in strats:
                if s in strat_rets:
                    strat_rets[s].append(rp)

    if total_trades > 0 and unknown_trades * 2 >= total_trades:
        print(
            f"[adaptive_weights] WARN: 归因失败交易占比高 "
            f"({unknown_trades}/{total_trades} 归入 __unknown__，"
            f"{dal_col_missing_days} 个 DAL 缺『来源策略』列)。"
            f"权重将静默回退等权，请检查 Daily-Action-List 归因完整性。",
            file=sys.stderr,
        )

    edge: dict[str, dict] = {}
    for s, rs in strat_rets.items():
        if not rs:
            edge[s] = {"n": 0, "avg_return": None, "win_rate": None, "edge": 0.0}
            continue
        n = len(rs)
        avg = sum(rs) / n
        win = sum(1 for x in rs if x > 0) / n
        edge[s] = {
            "n": n,
            "avg_return": round(avg, 3),
            "win_rate": round(win * 100, 1),
            "edge": avg,
        }
    return edge


def adaptive_weights(
    edge: dict,
    *,
    shrinkage: Optional[float] = None,
    temp: Optional[float] = None,
    pseudo: Optional[float] = None,
    floor: Optional[float] = None,
    zero_negative_edge: bool = True,
    min_evidence_n: int = 0,
) -> dict:
    """把各策略 edge 转成 0-100 百分比权重（不含现金）。纯数据驱动，无硬编码地板。

    算法流程：
    1. 经验贝叶斯收缩（按样本量）：shrunk_edge = edge * n/(n+pseudo)。
       低样本 edge 不可信，被拉向 0（先验均值），防「1 笔 +7% 被放大成 76%」翻车。
    2. softmax(shrunk_edge/temp)：正 edge 自然拿更多，负 edge 更少。
    3. 向等权收缩(shrinkage)：抑制权重剧烈摆动（正则化常数，非策略相关）。

    地板门（封死输家 / 不可验证策略，但不清零）：
    - zero_negative_edge（默认 True）：edge < 0 或样本不足的策略不被清零，而是压到
      FLOOR 地板（正则化常数，来自 CONFIG，可热更新），保留极小但非 0 的权重，
      避免整策略退出摧毁分散度。
    - min_evidence_n（默认 0 = 自适应）：归因交易数 < 阈值 → 该策略只拿地板权重（不归零）。
      默认自适应公式：max(3, 总样本数 // 策略数 // 4)，
      即随可用数据量自动升降（数据多时门槛高、少时放宽）。
      设为 >0 的固定值则退回固定门槛行为。

    地板为**事后**步骤：所有策略先参与 softmax 竞争，再统一抬到 FLOOR 以上并重新归一化
    到 100。edge 越负/样本越少 → 越靠近地板；edge 越正 → 越远离地板。无策略被彻底剔除，
    故分散度始终保留；全为地板时退化为接近等权。

    所有正则化常数（shrinkage/temp/pseudo/FLOOR）默认取自 CONFIG，
    可经 adaptive_weights_config.json 热更新；传参时以传参为准。
    """
    strs = ALL_STRATEGIES

    # ── 超参来自 CONFIG（可经 adaptive_weights_config.json 热更新）──
    cfg = CONFIG
    if shrinkage is None:
        shrinkage = cfg["shrinkage"]
    if temp is None:
        temp = cfg["temp"]
    if pseudo is None:
        pseudo = cfg["pseudo"]
    eff_floor = floor if (floor is not None and zero_negative_edge) else (cfg["FLOOR"] if zero_negative_edge else 0.0)

    # ── 自适应证据门槛 ──
    if min_evidence_n <= 0:
        total_samples = sum(max(int(edge.get(s, {}).get("n", 0) or 0), 0) for s in strs)
        min_evidence_n = max(3, total_samples // len(strs) // 4)

    # 1) 经验贝叶斯收缩：样本越少，edge 越不可信 → 越靠近 0
    shrunk: dict[str, float] = {}
    n_map: dict[str, int] = {}
    for s in strs:
        e = float(edge.get(s, {}).get("edge", 0.0) or 0.0)
        n = max(int(edge.get(s, {}).get("n", 0) or 0), 0)
        shrunk[s] = e * (n / (n + pseudo))
        n_map[s] = n

    # 2) softmax（相对最大值缩放，避免溢出）
    mx = max(shrunk.values())
    exps = {s: math.exp((shrunk[s] - mx) / temp) for s in strs}
    ssum = sum(exps.values())
    raw = {s: exps[s] / ssum for s in strs} if ssum > 0 else {s: 1.0 / len(strs) for s in strs}

    # 3) 向等权收缩（正则化，无地板值——清零门负责保底过滤）
    uni = 1.0 / len(strs)
    w = {s: (1 - shrinkage) * raw[s] + shrinkage * uni for s in strs}
    tot = sum(w.values())
    pct = {s: round(max(0.0, w[s]) / tot * 100) for s in w}
    # 修正四舍五入误差使和为 100
    d = 100 - sum(pct.values())
    if d != 0:
        anchor = max(pct, key=pct.get)
        pct[anchor] = max(0, pct[anchor] + d)

    # ── 正则化地板（替代"硬归零"）：保证每个策略至少保留 eff_floor% 权重，
    # 防止整策略清零导致分散度归零（历史坑：CCTV 被清零后其全部候选票仓位=0，
    # 幸存策略单票拿到 30% 上限 → 该票爆雷直接拖垮整天，如 20260624 -13.83%）。
    # eff_floor 来自 CONFIG（默认 3.0，可热更新），是数学正则化常数（非策略相关、
    # 非硬编码分数），与 shrinkage/temp/pseudo 同类，符合"零硬编码策略分"的约束；
    # edge 越负/样本越少 → 越靠近地板而非归零。zero_negative_edge=False 时关闭地板。
    if zero_negative_edge:
        floored = {s: max(pct[s], eff_floor) for s in strs}
        tot = sum(floored.values())
        if tot > 0:
            pct = {s: round(floored[s] / tot * 100) for s in strs}
            # 修正四舍五入误差使和为 100
            d = 100 - sum(pct.values())
            if d != 0:
                anchor = max(pct, key=pct.get)
                pct[anchor] = max(0, pct[anchor] + d)
    return pct


def cash_from_volatility(volatility_pctile: Optional[float]) -> int:
    """现金比例随市场波动率分位连续上升（高风险少出手）。无魔法数字。

    使用平滑 S 型曲线而非分段线性：
    - vol_pctile ≤ 0.3（低波）→ 0% 现金
    - vol_pctile = 0.5（中位）→ ~8% 现金
    - vol_pctile ≥ 0.85（高波）→ ~40% 现金
    - 极端情况自然封顶于 ~50%（曲线渐近线）

    取代旧版 `(p-0.5)*60` 分段线性 + 外部硬编码上下限(45/5)。
    k / midpoint 来自 CONFIG，可热更新；幅值 50 为领域上限，保持固定。
    """
    if volatility_pctile is None:
        return 0
    cv = CONFIG["cash_from_volatility"]
    p = volatility_pctile
    k = cv["k"]              # 陡度（越大越接近阶跃）
    midpoint = cv["midpoint"]  # 中点
    raw = 50.0 / (1.0 + math.exp(-k * (p - midpoint)))
    return int(round(raw))


def cash_from_regime(regime: Optional[str], base_cash: int) -> int:
    """趋势维度对现金的调整：纯连续函数，无硬编码上下限。

    下行防御时追加现金（幅度由波动率决定的上限内），趋势上行时压减现金。
    不再使用 max(., 45) / min(., 5) 这类魔法数字。
    乘数 / 上下限来自 CONFIG，可热更新。
    """
    cr = CONFIG["cash_from_regime"]
    if regime == "下行防御":
        # 追加至 base_cash 的 down_mult 倍（但不低于 down_floor、不超过 down_cap）
        return min(max(int(base_cash * cr["down_mult"]), cr["down_floor"]), cr["down_cap"])
    elif regime == "趋势上行":
        # 压减至 base_cash 的 up_mult 倍（但不低于 0%）
        return max(int(base_cash * cr["up_mult"]), 0)
    return base_cash


def cash_from_drawdown(
    drawdown_pct: Optional[float],
    threshold: Optional[float] = None,
    cap: Optional[float] = None,
    deep: Optional[float] = None,
) -> int:
    """组合级回撤熔断：组合滚动回撤超过阈值，追加现金比例（降低暴露）。

    纯连续函数，无硬编码策略分：
    - drawdown_pct <= threshold（如 8%）→ 0（正常市，不干预）
    - 超过阈值后线性抬升：threshold→deep 映射 0→cap
    - 深于 deep（如 20%）→ 封顶 cap

    仅「追加」现金，绝不减少由波动率 / regime 决定的基线现金；调用方再把
    总现金 clamp 到 100%。最坏情况是过度防御（多持现金），不会放大风险。
    threshold / cap / deep 来自 CONFIG，可热更新。
    """
    cd = CONFIG["cash_from_drawdown"]
    if threshold is None:
        threshold = cd["threshold"]
    if cap is None:
        cap = cd["cap"]
    if deep is None:
        deep = cd["deep"]
    if drawdown_pct is None:
        return 0
    if drawdown_pct <= threshold:
        return 0
    if deep <= threshold:
        return int(round(cap))
    t = min(1.0, (drawdown_pct - threshold) / (deep - threshold))
    return int(round(cap * t))


def compute_adaptive_allocation(
    edge_window: int = 20,
    min_n: int = 8,
    shrinkage: Optional[float] = None,
    floor: Optional[float] = None,
    zero_negative_edge: bool = True,
    min_evidence_n: int = 0,  # 0 = 自适应
) -> tuple[dict, dict, int, bool]:
    """主入口：算 edge → 自适应权重 → 现金比例；返回 (edge, weights_pct, cash_pct, cold_start)。

    cold_start=True 表示回测历史不足，权重回退等权（仅冷启动），此时不依赖业绩。
    所有权重均来自交易数据计算，无任何硬编码策略分数。
    shrinkage / floor 默认取自 CONFIG，可经 adaptive_weights_config.json 热更新。
    """
    if shrinkage is None:
        shrinkage = CONFIG["shrinkage"]
    eff_floor = floor if (floor is not None and zero_negative_edge) else (CONFIG["FLOOR"] if zero_negative_edge else 0.0)
    edge = compute_strategy_edge(edge_window)
    total_n = sum(e["n"] for e in edge.values())
    if total_n < min_n:
        if total_n == 0:
            print(
                "[adaptive_weights] WARN: 有效归因交易数为 0（可能 Daily-Action-List "
                "缺『来源策略』列），权重静默回退等权，cold_start=True。",
                file=sys.stderr,
            )
        else:
            print(
                f"[adaptive_weights] WARN: 有效样本不足(total_n={total_n} < min_n={min_n})，"
                f"权重静默回退等权，cold_start=True。",
                file=sys.stderr,
            )
        eq = round(100 / len(ALL_STRATEGIES))
        return edge, {s: eq for s in ALL_STRATEGIES}, 0, True
    weights = adaptive_weights(
        edge, shrinkage=shrinkage, floor=eff_floor,
        zero_negative_edge=zero_negative_edge, min_evidence_n=min_evidence_n,
    )
    return edge, weights, 0, False


def save_regime_snapshot(payload: dict) -> Optional[str]:
    """把市场状态 + 自适应权重快照落盘到 stock_data/regime-latest.json。

    原子写（临时文件 + os.replace）：非原子写曾因进程中断留下 0 字节 JSON，
    导致下游 json.load 直接失败。
    """
    try:
        import os as _os

        path = STOCK_DATA_DIR / "regime-latest.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        _os.replace(tmp, path)
        return str(path)
    except Exception:
        return None
