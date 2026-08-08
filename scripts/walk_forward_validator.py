#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Walk-forward 样本外验证：自适应权重到底有没有比等权多赚？

设计要点（严格因果，杜绝未来函数）：
- 对每个信号日 Ti，只用「严格早于 Ti」的历史回测 trades 计算各策略 edge，
  再经 adaptive_weights() 得到 Ti 当天的权重 —— 与生产运行时一致（生产也是用
  截至当时的历史算权重）。这就是 walk-forward：权重由过去决定，收益用未来检验。
- 在 Ti 的同一批候选票上，对比两种组合的前向收益（return_pct 已是次日开盘买入、
  T+1、含真实佣/税/滑点的已实现收益）：
    * 自适应组合：每票按「命中策略的因果权重取 max」分配；
    * 等权组合：每票 1/N。
  两者只在「权重分配」这一步不同，故差异纯粹反映权重决策是否增值。
- 样本外单调性：把所有票按 Ti 当天因果权重分高/中/低三档，看高权重档的前向收益
  是否真的高于低权重档 —— 这是「自适应信号是否灵」的核心检验。

参数敏感性网格扫描（--sweep / --recommend）：
- 在 (shrinkage × FLOOR) 网格上重算自适应组合样本外累计收益，找出最优组合。
- recommend() 额外做「稳健性」判定，杜绝把单一行情阶段的巧合当规律：
    * 改进幅度：最优组合须相对「当前配置」样本外累计多赚 ≥ MIN_IMPROVE_PP（默认 2pp）；
    * 单调性：当前配置下高权重档前向收益须 > 低权重档（信号确实灵）；
    * 稳定性：把信号日分成前后两半各扫一遍，推荐组合须在两段都进入前 3（不是
      只赌对某一半行情）。
- 仅当三项全过时 recommend() 的 robust=True，月度 CI 才会据此改写配置并开 PR。

数据来源扩展（本地缓存回补）：
- 优先使用生产回测产物 Multi-Backtest-*-trades.csv（最真实）。
- 对「有 Daily-Action-List 但无 Multi-Backtest trades」的近期信号日，用本地
  k_data/{code}_qfq_full.csv 缓存按「次开盘买入 → hold_days 后开盘卖出」做因果回补，
  扩大有效样本量。该过程不联网、不重跑回测引擎、不动生产代码。
"""
from __future__ import annotations

import glob
import json
import os
import sys
from pathlib import Path

import pandas as pd

# 允许从项目根以 `python scripts/walk_forward_validator.py` 运行
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy.adaptive_weights import (  # noqa: E402
    ALL_STRATEGIES,
    adaptive_weights,
    _norm_code,
    _norm_strategies,
)
from smcore.utils.code import format_stock_code  # noqa: E402

try:  # STOCK_DATA_DIR 直接导出则用之，否则回退到 config 默认
    from smcore.strategy.adaptive_weights import STOCK_DATA_DIR  # noqa: E402
except Exception:  # pragma: no cover
    from smcore.config.defaults import PROJECT_ROOT  # noqa: E402

    STOCK_DATA_DIR = PROJECT_ROOT / "stock_data"

from smcore.strategy.significance import significance_report, sharpe_ratio  # noqa: E402
try:  # 读全局 risk_config（含 calibration_significance / regime_robustness 守卫阈值）
    from smcore.strategy.risk_rules import (  # noqa: E402
        CONFIG as RISK_CONFIG,
        compute_adaptive_exit_params,
    )
except Exception:  # pragma: no cover
    RISK_CONFIG = {}

    def compute_adaptive_exit_params(*a, **k):  # pragma: no cover
        return {"stop_loss_pct": 0.08, "trailing_stop_pct": 0.05, "hold_days": 10}
try:  # 市场状态检测（as_of 历史切片，因果安全；新浪主源+本地缓存，离线可用）
    from smcore.strategy.market import regime_as_of as _market_regime_as_of  # noqa: E402
except Exception:  # pragma: no cover
    _market_regime_as_of = None

EDGE_WINDOW = 20          # 与生产 compute_adaptive_allocation 默认一致
MIN_N = 8                 # 总样本不足则冷启动回退等权
MIN_IMPROVE_PP = 2.0      # 推荐配置须相对当前配置至少多赚 2pp 才视为稳健改进
WF_HOLD_DAYS = int(os.environ.get("WF_HOLD_DAYS", "10"))  # 回补持有期
NO_BACKFILL = os.environ.get("WALK_FORWARD_NO_BACKFILL", "0") == "1"
# 出场参数扫描较重（全信号日 × 网格），默认关闭，仅月度重验/CI opt-in 时纳入 recommend()。
WF_BEST_EXIT = os.environ.get("WF_BEST_EXIT", "0") == "1"

# (shrinkage × FLOOR) 搜索网格
_SHRINKAGES = [0.0, 0.2, 0.4, 0.6]
_FLOORS = [0.0, 1.0, 2.0, 3.0]

# 出场参数搜索网格（P3 扩展：让出场引擎超参也接受样本外扫描，降低对单一配置的过拟合）
# 全部可配置：优先读 risk_config.json[exit_sweep]，缺省回退内置默认网格（零硬编码）。
_STOP_LOSS_GRID_DEFAULT = [0.06, 0.08, 0.10, 0.12]
_TRAILING_GRID_DEFAULT = [0.04, 0.05, 0.07]
_HOLD_GRID_DEFAULT = [7, 10, 14]
_exit_cfg = (RISK_CONFIG or {}).get("exit_sweep", {}) or {}
_STOP_LOSS_GRID = _exit_cfg.get("stop_loss_pct", _STOP_LOSS_GRID_DEFAULT)
_TRAILING_GRID = _exit_cfg.get("trailing_stop_pct", _TRAILING_GRID_DEFAULT)
_HOLD_GRID = _exit_cfg.get("hold_days", _HOLD_GRID_DEFAULT)


def _all_daily_action_lists() -> list[Path]:
    """返回所有 Daily-Action-List 文件路径（按文件名排序）。"""
    return sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))


def _parse_signal_date_from_name(name: str) -> str | None:
    """从文件名解析信号日字符串 YYYYMMDD。"""
    import re
    m = re.search(r"(\d{8})", Path(name).name)
    return m.group(1) if m else None


def _load_cached_kdata(code: str) -> pd.DataFrame:
    """只读本地 k_data 缓存，不联网；缺失或异常返回空表。"""
    cache = STOCK_DATA_DIR / "k_data" / f"{format_stock_code(code)}_qfq_full.csv"
    if not cache.exists():
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])
    try:
        df = pd.read_csv(cache)
    except Exception:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])
    for col in ("open", "high", "low", "close"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "open"]).sort_values("date").reset_index(drop=True)
    return df


def _forward_return_from_kdata(code: str, signal_date: str, hold_days: int = WF_HOLD_DAYS) -> float | None:
    """基于本地 K 线缓存，计算「信号日次开盘买入 → hold_days 后开盘卖出」的收益率。"""
    df = _load_cached_kdata(code)
    if df.empty:
        return None
    df = df[df["date"] > pd.to_datetime(signal_date)]
    if len(df) < 1:
        return None
    buy_open = float(df.iloc[0]["open"])
    if buy_open <= 0:
        return None
    sell_idx = min(hold_days, len(df) - 1)
    sell_open = float(df.iloc[sell_idx]["open"])
    return (sell_open - buy_open) / buy_open * 100.0


def _forward_return_exit_aware(code: str, signal_date: str, hold_days: int = WF_HOLD_DAYS,
                               *, stop_loss_pct: float, trailing_stop_pct: float,
                               strategy: str = "") -> float | None:
    """出场感知前向收益（P3）：信号日后首交易日开盘买入，经 simulate_position 出场引擎
    （次日开盘买 / T+1 防当天卖 / 硬止损 / 止盈 / 移动止盈 / MA60 破位 / 持有期满）持有到 end_date。

    与 naive 回补（次开盘买→持有N日开盘卖）相比，这里真实应用止损/止盈，使回补路径与
    生产 Multi-Backtest 的出场感知口径一致（审计指出的「两套 tracker 持有假设不一致」修复点）。
    """
    from datetime import timedelta

    from smcore.strategy.position_monitor import simulate_position

    df = _load_cached_kdata(code)
    if df.empty:
        return None
    sig = pd.to_datetime(signal_date)
    future = df[df["date"] > sig]
    if future.empty:
        return None
    buy_date = future.iloc[0]["date"].date()
    end_date = buy_date + timedelta(days=hold_days)
    res = simulate_position(
        code, buy_date, end_date,
        stop_loss_pct=stop_loss_pct,
        trailing_stop_pct=trailing_stop_pct,
        hold_days=hold_days,
        strategy=strategy,
    )
    rp = res.get("return_pct")
    return rp if (rp is not None and isinstance(rp, (int, float))) else None


def _read_dal_sources(dal_path: Path) -> dict[str, set[str]]:
    """读取 DAL，返回 code -> 来源策略集合。"""
    code2strat: dict[str, set[str]] = {}
    try:
        d = pd.read_csv(dal_path, encoding="utf-8-sig")
    except Exception:
        return code2strat
    if "股票代码" not in d.columns:
        return code2strat
    for _, r in d.iterrows():
        c = _norm_code(r.get("股票代码"))
        if not c:
            continue
        code2strat[c] = _norm_strategies(r.get("来源策略"))
    return code2strat


def _multi_backtest_records(sd: str) -> list[dict]:
    """读取生产 Multi-Backtest trades；返回 [] 表示缺失或空。"""
    tr = STOCK_DATA_DIR / f"Multi-Backtest-{sd}-trades.csv"
    if not tr.exists():
        return []
    try:
        t = pd.read_csv(tr, encoding="utf-8-sig")
    except Exception:
        return []
    if t.empty or "code" not in t.columns:
        return []
    dal_path = STOCK_DATA_DIR / f"Daily-Action-List-{sd}.csv"
    code2strat = _read_dal_sources(dal_path) if dal_path.exists() else {}
    rows = []
    for _, r in t.iterrows():
        c = _norm_code(r.get("code"))
        try:
            rp = float(r.get("return_pct"))
        except (TypeError, ValueError):
            continue
        rows.append({
            "code": c,
            "sources": code2strat.get(c) or {"__unknown__"},
            "return_pct": rp,
        })
    return rows


def _backfill_records(sd: str, exit_kwargs=None) -> list[dict]:
    """对缺失 Multi-Backtest trades 的信号日，用本地 k_data 做因果回补。

    exit_kwargs=None → naive 回补（次开盘买→持有N日开盘卖，保持向后兼容）；
    exit_kwargs 给定 → 出场感知回补（simulate_position，扫描出场参数用）。
    """
    if NO_BACKFILL:
        return []
    dal_path = STOCK_DATA_DIR / f"Daily-Action-List-{sd}.csv"
    if not dal_path.exists():
        return []
    code2strat = _read_dal_sources(dal_path)
    if not code2strat:
        return []
    rows = []
    for code, strats in code2strat.items():
        if exit_kwargs:
            rp = _forward_return_exit_aware(
                code, sd, WF_HOLD_DAYS,
                stop_loss_pct=exit_kwargs.get("stop_loss_pct", 0.08),
                trailing_stop_pct=exit_kwargs.get("trailing_stop_pct", 0.05),
                strategy="/".join(sorted(strats)),
            )
        else:
            rp = _forward_return_from_kdata(code, sd, WF_HOLD_DAYS)
        if rp is None:
            continue
        rows.append({
            "code": code,
            "sources": strats,
            "return_pct": rp,
            "_backfilled": True,
        })
    return rows


def _day_records(sd: str, exit_kwargs=None) -> list[dict]:
    """返回信号日 sd 的所有 (code, sources, return_pct) 记录。

    优先用生产 Multi-Backtest；缺失/空则用本地 k_data 回补（exit_kwargs 透传）。
    """
    recs = _multi_backtest_records(sd)
    if recs:
        return recs
    return _backfill_records(sd, exit_kwargs=exit_kwargs)


def _all_signal_days() -> list[str]:
    """所有存在 Daily-Action-List 且有可计算前向收益的信号日。"""
    days = set()
    for dal in _all_daily_action_lists():
        sd = _parse_signal_date_from_name(dal.name)
        if sd is None:
            continue
        # 快速判断：有 Multi-Backtest 或 DAL 里有可回补的代码
        if (STOCK_DATA_DIR / f"Multi-Backtest-{sd}-trades.csv").exists():
            days.add(sd)
            continue
        # 至少有一个票在 k_data 缓存有值才算有效
        code2strat = _read_dal_sources(dal)
        for code in code2strat:
            if not _load_cached_kdata(code).empty:
                days.add(sd)
                break
    return sorted(days)


def causal_edge(cutoff: str, window: int = EDGE_WINDOW) -> dict:
    """只用严格早于 cutoff 的信号日（取最近 window 个）算策略 edge。"""
    past = [d for d in _all_signal_days() if d < cutoff]
    past = past[-window:]
    strat_rets: dict[str, list[float]] = {s: [] for s in ALL_STRATEGIES}
    for sd in past:
        for rec in _day_records(sd):
            for s in rec["sources"]:
                if s in strat_rets:
                    strat_rets[s].append(rec["return_pct"])
    edge: dict[str, dict] = {}
    for s, rs in strat_rets.items():
        n = len(rs)
        avg = sum(rs) / n if n else 0.0
        win = (sum(1 for x in rs if x > 0) / n) if n else 0.0
        edge[s] = {"n": n, "avg_return": round(avg, 3),
                   "win_rate": round(win * 100, 1), "edge": avg}
    return edge


def _load_day_picks(sd: str, exit_kwargs=None) -> list[dict]:
    """返回 Ti 当天 (code, sources, return_pct, prod_weight) 列表。"""
    dal = STOCK_DATA_DIR / f"Daily-Action-List-{sd}.csv"
    if not dal.exists():
        return []
    try:
        d = pd.read_csv(dal, encoding="utf-8-sig")
    except Exception:
        return []
    if d.empty or "股票代码" not in d.columns:
        return []
    # 构造 return 映射
    records = _day_records(sd, exit_kwargs=exit_kwargs)
    ret_map = {r["code"]: r["return_pct"] for r in records}
    dal_map: dict[str, dict] = {}
    for _, r in d.iterrows():
        c = _norm_code(r.get("股票代码"))
        dal_map[c] = {
            "sources": _norm_strategies(r.get("来源策略")),
            "prod_weight": float(r.get("权重")) if pd.notna(r.get("权重")) else None,
        }
    picks = []
    for code, meta in dal_map.items():
        rp = ret_map.get(code)
        if rp is None:
            continue
        picks.append({
            "code": code,
            "sources": meta.get("sources", set()),
            "return_pct": rp,
            "prod_weight": meta.get("prod_weight"),
        })
    return picks


def _weights_for_day(sd: str, shrinkage=None, floor=None, zero_negative_edge=True) -> tuple[dict, bool]:
    """返回 (策略->权重%, cold_start)。"""
    edge = causal_edge(sd)
    total_n = sum(e["n"] for e in edge.values())
    if total_n < MIN_N:
        eq = round(100 / len(ALL_STRATEGIES))
        return {s: eq for s in ALL_STRATEGIES}, True
    return adaptive_weights(edge, shrinkage=shrinkage, floor=floor,
                            zero_negative_edge=zero_negative_edge), False


def _day_returns(shrinkage, floor, zero_negative_edge, sd, exit_kwargs=None) -> tuple[float, float] | None:
    """返回 (adaptive_ret, equal_ret)；无数据返回 None。"""
    weights, _ = _weights_for_day(sd, shrinkage, floor, zero_negative_edge)
    picks = _load_day_picks(sd, exit_kwargs=exit_kwargs)
    if not picks:
        return None
    wvals = []
    for p in picks:
        s = p["sources"]
        wv = max((weights[x] for x in s), default=min(weights.values()))
        wvals.append(wv)
    tot = sum(wvals) or 1.0
    allocs = [w / tot for w in wvals]
    rets = [p["return_pct"] for p in picks]
    adaptive_ret = sum(a * r for a, r in zip(allocs, rets))
    equal_ret = sum(rets) / len(rets)
    return adaptive_ret, equal_ret


def _portfolio_returns(shrinkage=None, floor=None, zero_negative_edge=True, days=None,
                       exit_kwargs=None) -> list[dict]:
    """返回每个有效信号日的 (adaptive_ret, equal_ret)。exit_kwargs 透传至回补路径。"""
    if days is None:
        days = _all_signal_days()
    rows = []
    for sd in days:
        r = _day_returns(shrinkage, floor, zero_negative_edge, sd, exit_kwargs=exit_kwargs)
        if r is None:
            continue
        ad, eq = r
        rows.append({"adaptive_ret": ad, "equal_ret": eq})
    return rows


def _cum(rows: list[dict], key: str) -> float:
    acc = 1.0
    for r in rows:
        v = r.get(key)
        if v is not None:
            acc *= (1 + v / 100.0)
    return (acc - 1) * 100


def _eff(shrinkage, floor, zero_negative_edge):
    if shrinkage is None:
        from smcore.strategy.adaptive_weights import CONFIG
        shrinkage = CONFIG["shrinkage"]
    eff_floor = floor if (floor is not None and zero_negative_edge) else (CONFIG["FLOOR"] if zero_negative_edge else 0.0)
    return shrinkage, eff_floor


def _regime_as_of(sd: str) -> str:
    """信号日 sd 当时的市场状态（因果安全：只用 <= sd 的索引数据）。

    复用 fusion 同款四维合成（market.regime_as_of，新浪主源+本地缓存），失败/数据不足回退「震荡轮动」。
    进程内缓存（market._REGIME_SERIES_CACHE）避免对同日期重复联网。
    """
    if _market_regime_as_of is None:
        return "震荡轮动"
    try:
        return _market_regime_as_of(sd)
    except Exception:
        return "震荡轮动"


def _regime_robust_gate(regime_table: dict, enabled: bool = True,
                        min_regimes: int = 2, min_days_per_regime: int = 3) -> dict:
    """机制稳健性闸门（纯函数，便于单测）。

    自适应权重须在「足够样本的不同市场状态」下都跑赢等权，而非只赌对某一种行情。
    - 仅在覆盖 >=2 个市场状态（每状态 >= min_days_per_regime 天）时才启用闸门；
      否则视为「样本不足以分状态检验」→ 真空通过（regime_robust=True），不阻断月度重验。
    - 返回 {robust, diverse, qualified, beat}。
    """
    qualified = [rg for rg, d in regime_table.items() if d.get("n_days", 0) >= min_days_per_regime]
    diverse = len(qualified) >= 2
    beat = sum(1 for rg in qualified if regime_table[rg]["adaptive_pct"] > regime_table[rg]["equal_pct"])
    robust = (not enabled) or (not diverse) or (beat >= min_regimes)
    return {"robust": robust, "diverse": diverse, "qualified": qualified, "beat": beat}


def run(shrinkage=None, floor=None, zero_negative_edge=True) -> dict:
    shrinkage, eff_floor = _eff(shrinkage, floor, zero_negative_edge)
    days = _all_signal_days()
    rows = []
    all_pairs = []  # (causal_weight, return_pct) 样本外单调性
    backfilled_days = 0
    for sd in days:
        weights, cold = _weights_for_day(sd, shrinkage, eff_floor, zero_negative_edge)
        picks = _load_day_picks(sd)
        if not picks:
            rows.append({"day": sd, "skipped": True, "n": 0,
                         "adaptive_ret": None, "equal_ret": None, "cold": cold})
            continue
        # 标记是否使用了 k_data 回补（用于报告透明度）
        is_backfilled = not _multi_backtest_records(sd)
        if is_backfilled:
            backfilled_days += 1
        wvals = []
        for p in picks:
            s = p["sources"]
            wv = max((weights[x] for x in s), default=min(weights.values()))
            p["causal_weight"] = wv
            wvals.append(wv)
        tot = sum(wvals) or 1.0
        allocs = [w / tot for w in wvals]
        rets = [p["return_pct"] for p in picks]
        adaptive_ret = sum(a * r for a, r in zip(allocs, rets))
        equal_ret = sum(rets) / len(rets)
        for p, a in zip(picks, allocs):
            all_pairs.append((p["causal_weight"], p["return_pct"], a, sd, p["code"]))
        rows.append({
            "day": sd, "skipped": False, "n": len(picks),
            "adaptive_ret": round(adaptive_ret, 3),
            "equal_ret": round(equal_ret, 3),
            "cold": cold,
            "backfilled": is_backfilled,
            "regime": _regime_as_of(sd),
        })

    def cum(rows, key):
        acc = 1.0
        for r in rows:
            v = r.get(key)
            if v is not None:
                acc *= (1 + v / 100.0)
        return (acc - 1) * 100

    adaptive_total = cum(rows, "adaptive_ret")
    equal_total = cum(rows, "equal_ret")
    valid = [r for r in rows if not r["skipped"]]
    awin = sum(1 for r in valid if r["adaptive_ret"] > 0)
    ewin = sum(1 for r in valid if r["equal_ret"] > 0)

    # 权重三档样本外单调性
    pairs_sorted = sorted(all_pairs, key=lambda x: x[0])
    n = len(pairs_sorted)
    tert = [pairs_sorted[: n // 3], pairs_sorted[n // 3: 2 * n // 3], pairs_sorted[2 * n // 3:]]
    tert_stats = []
    for label, grp in zip(["低权重档", "中权重档", "高权重档"], tert):
        if grp:
            mean_r = sum(g[1] for g in grp) / len(grp)
            win = sum(1 for g in grp if g[1] > 0) / len(grp) * 100
            tert_stats.append({"label": label, "n": len(grp),
                               "mean_ret": round(mean_r, 3), "win_rate": round(win, 1)})
        else:
            tert_stats.append({"label": label, "n": 0, "mean_ret": None, "win_rate": None})

    # 按市场状态分层：自适应 vs 等权 的样本外累计收益（稳健性检验的核心视图）
    regime_agg: dict[str, dict] = {}
    for r in valid:
        rg = r.get("regime")
        if rg is None:
            continue
        a = regime_agg.setdefault(rg, {"n": 0, "adaptive": 1.0, "equal": 1.0})
        a["n"] += 1
        if r["adaptive_ret"] is not None:
            a["adaptive"] *= (1 + r["adaptive_ret"] / 100.0)
        if r["equal_ret"] is not None:
            a["equal"] *= (1 + r["equal_ret"] / 100.0)
    regime_table = {}
    for rg, a in regime_agg.items():
        ad = (a["adaptive"] - 1) * 100
        eq = (a["equal"] - 1) * 100
        regime_table[rg] = {"n_days": a["n"], "adaptive_pct": round(ad, 2),
                            "equal_pct": round(eq, 2), "diff_pct": round(ad - eq, 2)}

    return {
        "n_days": len(days),
        "n_valid_days": len(valid),
        "n_backfilled_days": backfilled_days,
        "adaptive_total_pct": round(adaptive_total, 2),
        "equal_total_pct": round(equal_total, 2),
        "adaptive_win_days": awin,
        "equal_win_days": ewin,
        "adaptive_win_rate": round(awin / len(valid) * 100, 1) if valid else None,
        "equal_win_rate": round(ewin / len(valid) * 100, 1) if valid else None,
        "tercile": tert_stats,
        "regime_table": regime_table,
        "rows": rows,
        "pairs": [(round(w, 1), round(r, 3), round(a, 4), sd, c) for w, r, a, sd, c in all_pairs],
    }


def _grid() -> list[tuple[float, float]]:
    return [(s, f) for s in _SHRINKAGES for f in _FLOORS]


def _sweep_table(days=None, exit_kwargs=None) -> list[dict]:
    """网格扫描：每个 (shrinkage, FLOOR) 组合的样本外累计收益。exit_kwargs 透传。"""
    out = []
    for shr, fl in _grid():
        rows = _portfolio_returns(shr, floor=fl, zero_negative_edge=(fl > 0), days=days,
                                  exit_kwargs=exit_kwargs)
        ad = _cum(rows, "adaptive_ret")
        eq = _cum(rows, "equal_ret")
        out.append({"shrinkage": shr, "floor": fl, "floor_on": fl > 0,
                    "adaptive": round(ad, 2), "equal": round(eq, 2),
                    "diff": round(ad - eq, 2)})
    return out


def sweep() -> list[dict]:
    """参数敏感性扫描：放松平滑(shrinkage)与地板(floor)后，自适应组合的样本外表现。"""
    return _sweep_table()


def sweep_exits(days=None, save_path=None) -> list[dict]:
    """出场参数敏感性扫描（P3，离线、出场感知）。

    固定当前权重配置（adaptive_weights CONFIG），扫描 (止损% × trailing% × 持有期) 网格下
    的样本外累计收益，找出最优出场组合。回补路径改用 simulate_position（出场感知），使扫描
    结果真实反映止损/止盈/移动止盈/持有期满对样本外收益的边际影响。

    网格范围全部可配置（risk_config.json[exit_sweep]）；save_path 给定时把完整排序表
    落盘为 JSON 产物（供 monitor 归档 + 实验台账复查）。

    注：RS_TOL（relativity 筛选阈值）与流动性门槛需重跑策略*筛选*（relativity 依赖联网取数），
    不在离线扫描范围内，列为后续项；本函数只覆盖「持仓期出场参数」这一可离线维度。
    """
    if days is None:
        days = _all_signal_days()
    from smcore.strategy.adaptive_weights import CONFIG

    shr, fl = CONFIG["shrinkage"], CONFIG["FLOOR"]
    out = []
    for sl in _STOP_LOSS_GRID:
        for tr in _TRAILING_GRID:
            for hd in _HOLD_GRID:
                ek = {"stop_loss_pct": sl, "trailing_stop_pct": tr, "hold_days": hd}
                rows = _portfolio_returns(shr, floor=fl, zero_negative_edge=(fl > 0),
                                          days=days, exit_kwargs=ek)
                ad = _cum(rows, "adaptive_ret")
                eq = _cum(rows, "equal_ret")
                out.append({"stop_loss_pct": sl, "trailing_stop_pct": tr, "hold_days": hd,
                            "adaptive": round(ad, 2), "equal": round(eq, 2),
                            "diff": round(ad - eq, 2)})
    out.sort(key=lambda x: x["adaptive"], reverse=True)
    if save_path:
        try:
            Path(save_path).write_text(
                json.dumps({
                    "generated_at": pd.Timestamp.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    "n_days": len(days),
                    "grid": {"stop_loss_pct": _STOP_LOSS_GRID,
                             "trailing_stop_pct": _TRAILING_GRID,
                             "hold_days": _HOLD_GRID},
                    "rows": out,
                }, ensure_ascii=False, indent=2),
                encoding="utf-8")
        except Exception:
            pass
    return out


def _best_exit_params() -> dict | None:
    """P3：样本外出场参数扫描的最优组合（仅供参考，不直接改写生产配置）。

    小样本(~29 信号日)下避免过拟合，沿用权重纪律：仅「展示」最优出场，不自动采纳；
    采纳须月度重验跨 holdout 稳健。返回含 better_than_default 比较（与当前自适应基线对比）。
    """
    try:
        rows = sweep_exits()
        if not rows:
            return None
        top = rows[0]
        default = compute_adaptive_exit_params()
        def_row = None
        for r in rows:
            if (abs(r["stop_loss_pct"] - default["stop_loss_pct"]) < 1e-9
                    and abs(r["trailing_stop_pct"] - default["trailing_stop_pct"]) < 1e-9
                    and r["hold_days"] == default["hold_days"]):
                def_row = r
                break
        return {
            "stop_loss_pct": top["stop_loss_pct"],
            "trailing_stop_pct": top["trailing_stop_pct"],
            "hold_days": top["hold_days"],
            "adaptive_pct": top["adaptive"],
            "equal_pct": top["equal"],
            "diff_pct": top["diff"],
            "better_than_default": (def_row is not None
                                    and top["adaptive"] > def_row["adaptive"]),
            "default_row_adaptive_pct": (def_row["adaptive"] if def_row else None),
        }
    except Exception:
        return None


def recommend() -> dict:
    """带稳健性判定的推荐：网格最优组合 + 改进幅度/单调性/前后半段稳定性检查。

    仅当三项全过时 robust=True，月度 CI 才会据此改写配置并开 PR。
    """
    from smcore.strategy.adaptive_weights import CONFIG
    cur = run()  # 当前配置（CONFIG 默认）
    full = _sweep_table()
    best = max(full, key=lambda x: (x["adaptive"], x["diff"]))

    # 稳定性：前后半段各扫一遍，推荐组合须在两段都进入前 3
    days = _all_signal_days()
    half = max(1, len(days) // 2)
    first = _sweep_table(days[:half])
    second = _sweep_table(days[half:])

    def rank(table, key):
        ordered = sorted(table, key=lambda x: x["adaptive"], reverse=True)
        for i, c in enumerate(ordered):
            if c["shrinkage"] == key["shrinkage"] and c["floor"] == key["floor"]:
                return i + 1
        return len(table) + 1

    r_first = rank(first, best)
    r_second = rank(second, best)

    improve_pp = round(best["adaptive"] - cur["adaptive_total_pct"], 2)
    tert = cur["tercile"]
    monotonic = (tert[2]["mean_ret"] is not None and tert[0]["mean_ret"] is not None
                 and tert[2]["mean_ret"] > tert[0]["mean_ret"])
    stable = r_first <= 3 and r_second <= 3

    # 统计显著性守卫：自适应 vs 等权，经多重检验(削减夏普)校正后是否真有 edge
    _sig_cfg = (RISK_CONFIG or {}).get("calibration_significance", {})
    _sig_enabled = bool(_sig_cfg.get("enabled", True))
    valid_rows = [r for r in cur["rows"] if not r.get("skipped") and r.get("adaptive_ret") is not None]
    _ad = [r["adaptive_ret"] for r in valid_rows]
    _eq = [r["equal_ret"] for r in valid_rows]
    sig = significance_report(
        _ad, n_trials=max(1, len(full)),
        sr_benchmark=(sharpe_ratio(_eq) or 0.0),
        significance=float(_sig_cfg.get("significance", 0.05)),
        min_t_stat=float(_sig_cfg.get("min_t_stat", 3.0)),
    )

    # 机制稳健性闸门（P1-3）：自适应权重须「跨市场状态」都跑赢等权，而非只赌对某一种行情。
    # 仅在有效样本覆盖 >=2 个市场状态（每个状态 >= min_days_per_regime 天，避免小样本误判）时
    # 才启用闸门；否则视为「样本不足以分状态检验」→ 真空通过，不阻断月度重验。
    _rr_cfg = (RISK_CONFIG or {}).get("regime_robustness", {})
    _rr_enabled = bool(_rr_cfg.get("enabled", True))
    _rr_min = int(_rr_cfg.get("min_regimes", 2))
    _rr_min_days = int(_rr_cfg.get("min_days_per_regime", 3))
    _regime_table = cur.get("regime_table", {})
    _gate = _regime_robust_gate(_regime_table, enabled=_rr_enabled,
                                min_regimes=_rr_min, min_days_per_regime=_rr_min_days)
    _regime_diverse = _gate["diverse"]
    _regime_beat = _gate["beat"]
    regime_robust = _gate["robust"]

    robust = (improve_pp >= MIN_IMPROVE_PP and monotonic and stable
              and (not _sig_enabled or sig["significant"]) and regime_robust)

    return {
        "current": {"shrinkage": CONFIG["shrinkage"], "floor": CONFIG["FLOOR"]},
        "recommended": {"shrinkage": best["shrinkage"], "floor": best["floor"]},
        "improvement_pp": improve_pp,
        "robust": robust,
        "significance": sig,
        "regime_table": _regime_table,
        "checks": {
            "min_improve_pp": MIN_IMPROVE_PP,
            "improve_ok": improve_pp >= MIN_IMPROVE_PP,
            "monotonic": monotonic,
            "stable_first_half_rank": r_first,
            "stable_second_half_rank": r_second,
            "stable_ok": stable,
            "regime_robust_enabled": _rr_enabled,
                "regime_diverse": _regime_diverse,
                "regime_qualified": _gate["qualified"],
                "regime_beat_count": _regime_beat,
            "regime_min_regimes": _rr_min,
            "regime_robust_ok": regime_robust,
        },
        "current_report": {k: cur[k] for k in ("adaptive_total_pct", "equal_total_pct",
                                                "adaptive_win_rate", "equal_win_rate", "tercile")},
        "sweep": full,
        "best_exit_params": (_best_exit_params() if WF_BEST_EXIT else None),
    }

    # 实验台账：env STOCKS_LEDGER_RECORD=1 时把本次重验落一笔审计记录（默认关闭，避免
    # 干扰主流程 / 测试）。全程 try/except 包裹，失败静默，绝不抛异常。
    if os.environ.get("STOCKS_LEDGER_RECORD") == "1":
        try:
            from smcore.strategy import experiment_ledger
            experiment_ledger.record_calibration(rec, signal_date=None,
                                                author="walk_forward_validator")
        except Exception:
            pass

    return rec


def _print_report(res: dict) -> None:
    print("=" * 64)
    print("Walk-forward 样本外验证（自适应权重 vs 等权，同票对比）")
    print("=" * 64)
    print(f"信号日总数           : {res['n_days']}（有效 {res['n_valid_days']}，"
          f"其中 k_data 回补 {res.get('n_backfilled_days', 0)} 天）")
    print(f"自适应组合累计收益   : {res['adaptive_total_pct']:+.2f}%")
    print(f"等权组合累计收益     : {res['equal_total_pct']:+.2f}%")
    print(f"差值(自适应-等权)    : {res['adaptive_total_pct'] - res['equal_total_pct']:+.2f}%")
    print(f"自适应胜日率         : {res['adaptive_win_rate']}% ({res['adaptive_win_days']}/{res['n_valid_days']})")
    print(f"等权胜日率           : {res['equal_win_rate']}% ({res['equal_win_days']}/{res['n_valid_days']})")
    print("-" * 64)
    print("样本外单调性（按 Ti 当天因果权重分档，前向收益）：")
    for t in res["tercile"]:
        mr = f"{t['mean_ret']:+.3f}%" if t["mean_ret"] is not None else "n/a"
        print(f"  {t['label']:>8}  n={t['n']:>3}  均值收益={mr:>8}  胜率={t['win_rate']}%")
    print("-" * 64)
    rt = res.get("regime_table") or {}
    print("按市场状态分层（自适应 vs 等权，样本外累计收益）：")
    if rt:
        for rg, d in sorted(rt.items(), key=lambda x: -x[1]["diff_pct"]):
            flag = "✓跑赢" if d["diff_pct"] > 0 else "✗跑输"
            print(f"  {rg:>6}  n={d['n_days']:>3}  自适应={d['adaptive_pct']:+.2f}%  "
                  f"等权={d['equal_pct']:+.2f}%  差值={d['diff_pct']:+.2f}%  {flag}")
    else:
        print("  （无市场状态分层数据）")
    print("-" * 64)
    print("说明：以上为「100% 持仓、连续再投资」口径（无现金缓冲）")
    print("生产回测的 -1.63% 是「各 sleeve 独立 + 含 70% 现金」口径，两者对象不同。")
    print("相对结论（自适应 vs 等权）在两种口径下一致。")


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--sweep", action="store_true", help="额外输出参数敏感性扫描表")
    ap.add_argument("--sweep-exits", action="store_true", help="额外输出出场参数敏感性扫描表（止损%/trailing%/持有期，出场感知）")
    ap.add_argument("--recommend", action="store_true", help="输出月度重验推荐配置 JSON（含稳健性判定）")
    ap.add_argument("--emit-json", default=None, help="把 recommend/sweep 结果写到该 JSON 路径")
    ap.add_argument("--exit-sweep-json", default=None, help="把出场参数敏感性扫描表写到该 JSON 路径")
    args = ap.parse_args()

    res = run()
    _print_report(res)

    if args.sweep:
        print("=" * 64)
        print("参数敏感性扫描（网格：shrinkage × FLOOR，自适应样本外累计收益）")
        print("=" * 64)
        print(f"{'shrinkage':>10}{'FLOOR':>8}{'自适应':>10}{'等权':>10}{'差值':>10}")
        for s in sweep():
            print(f"{s['shrinkage']:>10}{s['floor']:>8}{s['adaptive']:>+10.2f}{s['equal']:>+10.2f}{s['diff']:>+10.2f}")

    if args.sweep_exits:
        print("=" * 64)
        print("出场参数敏感性扫描（固定当前权重，网格：止损% × trailing% × 持有期，出场感知回补）")
        print("=" * 64)
        print(f"{'止损%':>8}{'trailing%':>10}{'持有期':>8}{'自适应':>10}{'等权':>10}{'差值':>10}")
        for s in sweep_exits():
            print(f"{s['stop_loss_pct']*100:>7.0f}%{s['trailing_stop_pct']*100:>9.0f}%"
                  f"{s['hold_days']:>8}{s['adaptive']:>+10.2f}{s['equal']:>+10.2f}{s['diff']:>+10.2f}")

    if args.exit_sweep_json:
        try:
            sweep_exits(save_path=args.exit_sweep_json)
            print(f"\n出场参数扫描已写：{args.exit_sweep_json}")
        except Exception as e:
            print(f"写出场扫描失败：{e}")

    rec = None
    if args.recommend:
        rec = recommend()
        print("=" * 64)
        print(f"月度重验推荐：当前={rec['current']}  推荐={rec['recommended']}  "
              f"改进={rec['improvement_pp']:+.2f}pp  稳健={rec['robust']}")
        print("稳健性检查：", json.dumps(rec["checks"], ensure_ascii=False))
        out_json = json.dumps(rec, ensure_ascii=False, indent=2, default=str)
        print(out_json)

    # 落盘明细供复查
    out = ROOT / "stock_data" / "walk_forward_results.json"
    try:
        with open(out, "w", encoding="utf-8") as f:
            json.dump({k: v for k, v in res.items() if k != "pairs"}, f,
                      ensure_ascii=False, indent=2, default=str)
        print(f"\n明细已写：{out}")
    except Exception as e:  # pragma: no cover
        print(f"写明细失败：{e}")

    if args.emit_json:
        if args.recommend:
            target = rec
        else:
            target = {}
            if args.sweep:
                target["sweep"] = sweep()
            if args.sweep_exits:
                target["sweep_exits"] = sweep_exits()
            if not target:
                target = {"sweep": sweep()}
        try:
            with open(args.emit_json, "w", encoding="utf-8") as f:
                json.dump(target, f, ensure_ascii=False, indent=2, default=str)
            print(f"\n推荐/扫描结果已写：{args.emit_json}")
        except Exception as e:  # pragma: no cover
            print(f"写 JSON 失败：{e}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
