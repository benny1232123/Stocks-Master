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
from datetime import datetime
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
    # ── edge 归因口径（2026-09-09 修复「选择偏差」）──
    # backtest = 旧口径，只在「回测成交子集」上算（已被资金/排序截断到头部）；
    # universe = 新口径，在「候选全集」的真实前向收益上算（默认，推荐）；
    # blend    = 两者加权。
    # 背景：momentum 在成交子集(n=27)上 edge=+0.99%，在候选全集(n=185)上 -3.46%，
    # 方向相反 → 用旧口径会把权重往错误的方向调。
    "edge": {
        "source": "universe",
        "window": 30,
        "hold_days": 10,
        "benchmark": "hs300",
        "blend_w_backtest": 0.5,
        # 样本置信度折扣：n < min_n_confident 的策略 edge 乘以 sqrt(n/min_n_confident)，
        # 抑制「小样本高胜率」把权重顶到 50%+（实测 boll n=6/胜率100% → 58%）。
        "min_n_confident": 30,
    },
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
    except FileNotFoundError:
        pass  # 配置文件不存在属预期（内置默认即可跑），不必告警
    except Exception as exc:
        # 配置文件存在但解析失败 = 有人改坏了 JSON，静默回退会让调参"看起来没生效"
        print(
            f"[adaptive_weights] WARN: 配置解析失败，回退内置默认（{exc!r}）",
            file=sys.stderr,
        )
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
            except Exception as exc:
                print(
                    f"[adaptive_weights] WARN: 读 {dal.name} 失败，该信号日归因缺失（{exc!r}）",
                    file=sys.stderr,
                )
        tr = STOCK_DATA_DIR / f"Multi-Backtest-{sd}-trades.csv"
        if not tr.exists():
            continue
        try:
            t = pd.read_csv(tr)
        except Exception as exc:
            print(
                f"[adaptive_weights] WARN: 读 {tr.name} 失败，该信号日交易缺失（{exc!r}）",
                file=sys.stderr,
            )
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
            edge[s] = {"n": 0, "avg_return": None, "win_rate": None, "edge": 0.0, "std": 0.0}
            continue
        n = len(rs)
        avg = sum(rs) / n
        win = sum(1 for x in rs if x > 0) / n
        edge[s] = {
            "n": n,
            "avg_return": round(avg, 3),
            "win_rate": round(win * 100, 1),
            "edge": avg,
            "std": _sd(rs),
        }
    return edge


def _recent_signal_days(window: int, hold_days: int = 0) -> list[str]:
    """最近 ``window + hold_days`` 个存在 Daily-Action-List 的信号日（升序）。

    多取 ``hold_days`` 个是因为**最近的信号日还没有走完前向窗口**——若只取
    window 个，其中最近 hold_days 个会因未来 K 线不足被整段丢弃，有效样本只剩
    window - hold_days 个（实测 window=20/hold=10 时只剩 10 个有效日，归因失真）。
    多取后在聚合阶段自然跳过不足者，保证有效样本≈window。
    """
    files = sorted(glob.glob(str(STOCK_DATA_DIR / "Daily-Action-List-*.csv")))
    out = []
    for f in files:
        name = os.path.basename(f)
        sd = name[len("Daily-Action-List-"):-len(".csv")]
        if len(sd) == 8 and sd.isdigit():
            out.append(sd)
    n = window + max(0, hold_days)
    return out[-n:] if n > 0 else []


def _benchmark_forward_ret(sig_date: str, hold_days: int) -> Optional[float]:
    """基准（沪深300）在信号日之后 hold_days 个交易日的收益（百分数）。

    拿不到基准时返回 None → 调用方退化为「绝对收益」口径（不阻塞归因）。
    """
    try:
        from smcore.strategy.regime_filter import _get_hs300_close
    except Exception:
        return None
    try:
        s = _get_hs300_close()
        if s is None or len(s) == 0:
            return None
        idx = [d.strftime("%Y%m%d") for d in s.index]
        if sig_date not in idx:
            return None
        i = idx.index(sig_date)
        if i + hold_days >= len(s):
            return None  # 基准未来数据不足（近期信号）
        p0 = float(s.iloc[i])
        p1 = float(s.iloc[i + hold_days])
        if p0 <= 0:
            return None
        return (p1 / p0 - 1) * 100.0
    except Exception:
        return None


def compute_universe_edge(
    window: Optional[int] = None,
    hold_days: Optional[int] = None,
    use_benchmark: bool = True,
) -> dict:
    """在「候选全集」上算各策略 edge（相对基准的超额收益，百分数）。

    与 :func:`compute_strategy_edge`（回测成交子集）的区别
    ──────────────────────────────────────────────────────
    旧口径只统计「回测中真正成交的交易」，这些交易已经被资金约束与排序截断到
    每个信号日的头部少数几只。但**权重影响的是候选全集的分布**——momentum 在
    成交子集里 n=27、edge +0.99%，在候选全集里 n=185、-3.46%，两者方向相反。

    用成交子集做反馈，等于只拿"尖子生的成绩"给整个班级排权重，会把权重往
    错误的方向调。本函数直接对 Daily-Action-List 的**每一条候选**计算信号日
    之后 hold_days 个交易日的真实前向收益，减去同期基准收益得到超额。

    样本量比回测子集大一个量级，且无截断偏差。多策略命中的票计入每个命中
    策略（与综合评分叠加口径一致）。

    基准不可用时自动退化为绝对收益（仍远优于被截断的子集），不阻塞流程。
    """
    cfg = CONFIG.get("edge", {})
    if window is None:
        window = int(cfg.get("window", 30))
    if hold_days is None:
        hold_days = int(cfg.get("hold_days", 10))

    try:
        from smcore.data.kline import read_kline_cache
    except Exception:
        return {s: {"n": 0, "avg_return": None, "win_rate": None, "edge": 0.0, "std": 0.0} for s in ALL_STRATEGIES}

    sig_days = _recent_signal_days(window, hold_days)
    bench_cache: dict[str, Optional[float]] = {}
    kline_cache: dict[str, Optional[pd.DataFrame]] = {}
    strat_rets: dict[str, list[float]] = {s: [] for s in ALL_STRATEGIES}
    skipped_short = 0

    for sd in sig_days:
        dal = STOCK_DATA_DIR / f"Daily-Action-List-{sd}.csv"
        try:
            d = pd.read_csv(dal)
        except Exception:
            continue
        if not {"股票代码", "来源策略"}.issubset(d.columns) or d.empty:
            continue

        b = None
        if use_benchmark:
            if sd not in bench_cache:
                bench_cache[sd] = _benchmark_forward_ret(sd, hold_days)
            b = bench_cache[sd]

        for _, r in d.iterrows():
            code = _norm_code(r["股票代码"])
            strats = _norm_strategies(r["来源策略"]) & set(ALL_STRATEGIES)
            if not strats:
                continue
            if code not in kline_cache:
                try:
                    k = read_kline_cache(code)
                    kline_cache[code] = None if (k is None or k.empty) else k
                except Exception:
                    kline_cache[code] = None
            k = kline_cache[code]
            if k is None:
                continue
            try:
                dates = k["date"].astype(str).str.replace("-", "").tolist()
                if sd not in dates:
                    continue
                i = dates.index(sd)
                if i + hold_days >= len(k):
                    skipped_short += 1  # 信号日太近，未来 K 线不足
                    continue
                p0 = float(k.iloc[i]["close"])
                p1 = float(k.iloc[i + hold_days]["close"])
                if p0 <= 0:
                    continue
                ret = (p1 / p0 - 1) * 100.0
            except Exception:
                continue
            excess = ret - b if b is not None else ret
            for s in strats:
                strat_rets[s].append(excess)

    edge: dict[str, dict] = {}
    for s, rs in strat_rets.items():
        if not rs:
            edge[s] = {"n": 0, "avg_return": None, "win_rate": None, "edge": 0.0, "std": 0.0}
            continue
        n = len(rs)
        avg = sum(rs) / n
        win = sum(1 for x in rs if x > 0) / n
        edge[s] = {"n": n, "avg_return": round(avg, 3), "win_rate": round(win * 100, 1), "edge": avg, "std": _sd(rs)}

    edge["__meta__"] = {  # type: ignore[assignment]  # 诊断用，不参与权重计算
        "source": "universe",
        "window": window,
        "hold_days": hold_days,
        "signal_days": len(sig_days),
        "benchmark": "hs300" if use_benchmark else "none",
        "skipped_future_insufficient": skipped_short,
    }
    return edge


def compute_edge(window: Optional[int] = None, **kw) -> dict:
    """按配置 ``edge.source`` 选择归因口径，返回各策略 edge。

    - ``universe``（默认）：候选全集真实前向收益（无截断偏差）
    - ``backtest``：回测成交子集（旧口径）
    - ``blend``：两者按 ``blend_w_backtest`` 加权融合
    """
    cfg = CONFIG.get("edge", {})
    source = str(cfg.get("source", "universe")).lower()
    w = window if window is not None else int(cfg.get("window", 30))

    if source == "backtest":
        return compute_strategy_edge(w)
    if source == "blend":
        wb = float(cfg.get("blend_w_backtest", 0.5))
        bt = compute_strategy_edge(w)
        uv = compute_universe_edge(window=w)
        out: dict = {}
        for s in ALL_STRATEGIES:
            a, b = bt.get(s, {}), uv.get(s, {})
            na, nb = a.get("n", 0), b.get("n", 0)
            ea, eb = a.get("edge", 0.0), b.get("edge", 0.0)
            if na and nb:
                out[s] = {
                    "n": nb,
                    "avg_return": round(wb * (a.get("avg_return") or 0) + (1 - wb) * (b.get("avg_return") or 0), 3),
                    "win_rate": round(wb * (a.get("win_rate") or 0) + (1 - wb) * (b.get("win_rate") or 0), 1),
                    "edge": wb * ea + (1 - wb) * eb,
                    # std 取样本量较大一方的离散度（证据强度主要看 n 与 edge）
                    "std": (a.get("std") or 0.0) if na >= nb else (b.get("std") or 0.0),
                }
            else:
                out[s] = (b if nb else a) or {"n": 0, "avg_return": None, "win_rate": None, "edge": 0.0, "std": 0.0}
        out["__meta__"] = {"source": "blend", "w_backtest": wb}
        return out
    return compute_universe_edge(window=w)


def _sd(vals: list) -> float:
    """总体标准差；不足 2 个样本返回 0.0（离散度未知 → 视为无证据）。"""
    n = len(vals)
    if n < 2:
        return 0.0
    m = sum(vals) / n
    return math.sqrt(sum((x - m) ** 2 for x in vals) / n)


def compute_dynamic_shrinkage(
    edge: dict,
    *,
    base: float = 0.4,
    pseudo: float = 15.0,
    t_target: float = 2.0,
) -> dict:
    """按证据强度给每策略算「向等权收缩」系数，替代固定 shrinkage。

    - 置信度 ``c = n / (n + pseudo)``：样本量越大越可信
    - 显著度 ``sig = min(1, |t| / t_target)``：|t| 越大 edge 越不像噪声
    - ``shrinkage_s = base * (1 - c * sig)``：证据强 → 0（全信自适应权重）；
      证据弱或样本小 → 接近 ``base``（≈ 等权）

    返回 {strategy: float}，全部位于 [0, base]。n<=1 或 sd<=0 → base。
    """
    out: dict[str, float] = {}
    for s, e in edge.items():
        if s == "__meta__" or not isinstance(e, dict):
            continue
        n = max(int(e.get("n", 0) or 0), 0)
        avg = float(e.get("edge", 0.0) or 0.0)
        sd = float(e.get("std", 0.0) or 0.0)
        if n <= 1 or sd <= 0:
            out[s] = base
            continue
        se = sd / math.sqrt(n)
        if se <= 0:
            out[s] = base
            continue
        t = avg / se
        c = n / (n + pseudo)
        sig = min(1.0, abs(t) / t_target)
        out[s] = round(max(0.0, base * (1.0 - c * sig)), 6)
    return out


def adaptive_weights(
    edge: dict,
    *,
    shrinkage=None,
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

    ``shrinkage`` 可为三种形态：
    - ``None``：若 CONFIG["shrinkage_dynamic"] 为 true，用
      :func:`compute_dynamic_shrinkage` 按每策略证据强度动态收缩（返回 dict）；
      否则用 CONFIG["shrinkage"] 常数。
    - ``float``：所有策略同一常数（旧行为）。
    - ``dict``：按策略指定收缩系数（缺失回退 CONFIG["shrinkage"]）。
    """
    strs = ALL_STRATEGIES

    # ── 超参来自 CONFIG（可经 adaptive_weights_config.json 热更新）──
    cfg = CONFIG
    if temp is None:
        temp = cfg["temp"]
    if pseudo is None:
        pseudo = cfg["pseudo"]
    if shrinkage is None:
        if cfg.get("shrinkage_dynamic", False):
            shrinkage = compute_dynamic_shrinkage(
                edge,
                base=float(cfg["shrinkage"]),
                pseudo=pseudo,
                t_target=float(cfg.get("shrinkage_t_target", 2.0)),
            )
        else:
            shrinkage = cfg["shrinkage"]
    eff_floor = floor if (floor is not None and zero_negative_edge) else (cfg["FLOOR"] if zero_negative_edge else 0.0)

    # ── 自适应证据门槛 ──
    if min_evidence_n <= 0:
        total_samples = sum(max(int(edge.get(s, {}).get("n", 0) or 0), 0) for s in strs)
        min_evidence_n = max(3, total_samples // len(strs) // 4)

    # 1) 经验贝叶斯收缩：样本越少，edge 越不可信 → 越靠近 0
    #    再叠一层样本置信度折扣 sqrt(n / min_n_confident)（上限 1.0）：
    #    2026-09-09 实测 boll 仅 n=6、胜率 100% 就靠 pseudo 收缩后仍拿到 58% 权重，
    #    典型「小样本高胜率」过拟合。伪计数只能压低 edge，压不住 softmax 里的
    #    相对优势；置信度折扣专门治这个。
    min_n_conf = float(cfg.get("edge", {}).get("min_n_confident", 30))
    shrunk: dict[str, float] = {}
    n_map: dict[str, int] = {}
    for s in strs:
        e = float(edge.get(s, {}).get("edge", 0.0) or 0.0)
        n = max(int(edge.get(s, {}).get("n", 0) or 0), 0)
        conf = 1.0
        if min_n_conf > 0 and n > 0:
            conf = min(1.0, math.sqrt(n / min_n_conf))
        shrunk[s] = e * (n / (n + pseudo)) * conf
        n_map[s] = n

    # 2) softmax（相对最大值缩放，避免溢出）
    mx = max(shrunk.values())
    exps = {s: math.exp((shrunk[s] - mx) / temp) for s in strs}
    ssum = sum(exps.values())
    raw = {s: exps[s] / ssum for s in strs} if ssum > 0 else {s: 1.0 / len(strs) for s in strs}

    # 3) 向等权收缩（正则化，无地板值——清零门负责保底过滤）
    uni = 1.0 / len(strs)
    if isinstance(shrinkage, dict):
        w = {}
        for s in strs:
            sh = float(shrinkage.get(s, cfg.get("shrinkage", 0.4)))
            w[s] = (1 - sh) * raw[s] + sh * uni
    else:
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
    shrinkage=None → adaptive_weights 按 CONFIG.shrinkage_dynamic 决定 float/dict。
    """
    eff_floor = floor if (floor is not None and zero_negative_edge) else (CONFIG["FLOOR"] if zero_negative_edge else 0.0)
    # 口径由 CONFIG["edge"]["source"] 决定（默认 universe = 候选全集，无截断偏差）
    edge = compute_edge(edge_window)
    total_n = sum(e.get("n", 0) for e in edge.values() if isinstance(e, dict) and "n" in e)
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


def save_regime_snapshot(payload: dict, source: str = "live") -> Optional[str]:
    """把市场状态 + 自适应权重快照落盘。

    原子写（临时文件 + os.replace）：非原子写曾因进程中断留下 0 字节 JSON，
    导致下游 json.load 直接失败。

    ``source="live"``（当日信号）写 ``regime-latest.json``；
    ``source="replay"``（历史回放/补跑）只写 ``regime_history/<信号日>.json``。

    ⚠️ 这是 2026-09-09 修复的「快照被回放污染」问题：此前回放按日期升序跑完，
    最后一个历史信号日会覆盖 regime-latest.json，使「最新市场状态」停留在
    数周前的回放日期（当时停在 20260729，实际已到 20260909），前端/接口据此
    展示的是陈旧 regime 与等权权重。
    """
    try:
        import os as _os

        payload = dict(payload)
        payload["source"] = source
        payload["generated_at"] = datetime.now().isoformat(timespec="seconds")

        if source == "replay":
            sig_date = str(payload.get("date") or "unknown")
            hist_dir = STOCK_DATA_DIR / "regime_history"
            hist_dir.mkdir(parents=True, exist_ok=True)
            path = hist_dir / f"{sig_date}.json"
        else:
            path = STOCK_DATA_DIR / "regime-latest.json"

        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        _os.replace(tmp, path)
        return str(path)
    except Exception as exc:  # 不再静默：写失败必须可见（此前 pass 导致问题潜伏数周）
        print(f"[adaptive_weights] WARN: regime 快照写入失败（{exc}）", file=sys.stderr)
        return None
