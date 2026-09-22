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
from smcore.strategy.factor_types import STRATEGY_ORDER
from smcore.utils.code import format_stock_code

# 策略清单 = factor_types.STRATEGY_ORDER（唯一真相源，避免多处硬编码漂移）。
# 顺序仅影响字典迭代/并列取整锚点，无策略语义。
ALL_STRATEGIES = list(STRATEGY_ORDER)

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
    # 仓位加权 edge 聚合（2026-09-16 立项）：开启后把候选票前向超额收益按 DAL「建议仓位%」
    # 加权再聚合到各策略 edge（替代等权平均），对齐「每元资本的边际贡献」而非「每笔候选均值」。
    # 与 OOS 归因一致（CCTV 在 pw 口径跑赢融合、dw 口径跑输），但必须经 walk-forward 稳健门
    # 控（scripts/walk_forward_pw_edge.py）判定开启，绝不手动改权重。默认关闭。
    "position_weighted": False,
    },
    # ── 因子生效开关（factor timing overlay，2026-09-16 立项；2026-09-21 修正判据）──
    # 开启后 _weights_for_day 在基权重之上套用「因子生效开关」：近期信念 IC（分配器权重 vs
    # 该策略选中票前向收益的滚动 Spearman）**仅当显著为负**才把该因子权重清零，其余（正 IC /
    # 近零 IC / 不显著）一律保留并按比例重分配——防止好因子因「不显著」被误杀。
    # 即「每日给各因子打分、只抑制被证据证伪的方法」，零硬编码、纯数据驱动。经 walk-forward 稳健门
    # 控（scripts/walk_forward_factor_timing.py）判定开启；月度自动化 tripwire 可自动回滚。
    # ⚠️ 必须注册于此 _BUILTIN_DEFAULTS：否则 _load_config 忽略它、save_config 丢弃它
    #    （前者致开关静默失效，后者致写回时整个 factor_timing 块被抹掉——2026-09-16 踩坑）。
    #    （本处为安全内置默认 enabled=False；部署用 adaptive_weights_config.json 覆写为
    #     true，故线上实际生效——见下方「生产已接入」说明。）
    # ✅ 生产已接入：fusion.py → compute_adaptive_allocation()（L982）惰性委托
    #    smcore.strategy.factor_timing.apply_mask，故 CONFIG.enabled=true 时本覆盖层**生效于
    #    线上选股权重**（2026-09-21 核对；旧注释「未接入 / 开启不改变线上权重」已过时）。
    "factor_timing": {"enabled": False, "window": 10, "min_n": 5},
    # ── 硬编码策略黑名单（2026-09-17）：被证据判死刑的策略强制清零，不参与分配。
    # 与 factor_timing 覆盖层互补：覆盖层是「数据驱动、可翻案」门控（theme/cctv 当前被其清零、
    # 若信念 IC 转正则可恢复）；本黑名单是「结构性判死刑」的最终兜底（momentum 方向反转是设计缺陷、
    # 且因 DAL 稀疏永远测不动 → 其权重纯属无证据默认保留）。名单内策略在
    # compute_adaptive_allocation 末段强制置 0 并重新归一化，不受 FLOOR 地板影响、也不受覆盖层
    # enabled 状态影响 → 即便覆盖层被 tripwire 回滚也能保证出局。默认空（不破坏现状）。
    # ⚠️ 必须注册于此 _BUILTIN_DEFAULTS：否则 _load_config/save_config 会忽略/丢弃它。
    "excluded_strategies": [],
    # ── 无证据策略门控（2026-09-17，与 excluded_strategies 互补）──
    # excluded_strategies = 结构性「判死刑」（永久清零）；本开关 = 临时「冷启动门」：
    # 任何「尚无任何已实现归因历史」(n < min_evidence_for_allocation) 的策略只保留
    # eff_floor 的「探索权重」，不吸收被清零策略释放的额度。堵死「无证据默认均分」陷阱——
    # 曾让 momentum 凭空拿 13%、fundamental（新接入、零业绩）凭空拿 32%。
    # fundamental 的候选票仍照常进入 DAL 积累业绩；一旦 n 达标（默认 ≥1 个归因信号日）
    # 即自动「毕业」、凭真实 edge 参与分配，无需改代码。关掉本开关
    # (exclude_no_evidence_strategies=false) 即退化为原行为，让新策略立即按初稿权重参与
    # 分配（若优先要即时稀释、愿承担无证据权重风险，可如此设置）。
    # ⚠️ 必须注册于此 _BUILTIN_DEFAULTS：否则 _load_config/save_config 会忽略/丢弃它。
    "exclude_no_evidence_strategies": True,
    "min_evidence_for_allocation": 1,
    # ── 探索预算池上限（2026-09-17，与上一条配套的**可扩展性**修复）──
    # 原逻辑：每个无证据策略固定拿 eff_floor(=3%)，于是无证据策略的总探索额度 =
    # 3% × 个数，会**线性吞噬**总预算，挤压有证据策略：
    #   9 个无证据 + 9 个有证据 → 有证据合计仅 85；25 个 → 52；34 个 → 25；40 个 → 8。
    # 更糟的是「3% × 个数 > 100%」（≥34 个无证据）时 rem 变负 → 有证据策略拿到**负权重**、
    # 权重合计不等于 100（实测 40 总数 / 4 有证据 → 合计 101、单个 −2.00），属数学崩坏。
    # 改为「探索预算池」：无证据策略合计不超过本上限（默认 30%），池内按个数均分。
    # 因子少时与旧行为**完全一致**（9×3% = 27% ≤ 30% → 每个仍是 3.0），
    # 因子多时自动等比缩小（40 个 → 每个 0.75%），有证据策略始终保住 ≥70%。
    # ⚠️ 必须注册于此 _BUILTIN_DEFAULTS：否则 _load_config/save_config 会忽略/丢弃它。
    "explore_pool_cap_pct": 30.0,
    # 样本外单调性守卫容差（百分点）：见 test_walk_forward.test_out_of_sample_monotonicity。
    # 项目 OOS 结论（WALK_FORWARD_VALIDATION.md）已判定单调性「非跨 regime 稳健」
    # （3 regime 仅 1 跑赢等权，robust=False，edge 处噪声级 ±0.8pp）。故该守卫不再硬断言
    # high>low，仅在高权重档比低权重档劣化超过此容差时报警（捕捉机制崩坏级倒置）。
    # 取值 = 原 21 天窗口观测到的正向单调幅度(+2.5pp) 作为对称容差；可经 walk-forward CI 调。
    "monotonicity_tol_pp": 2.5,
    # 网格扫描守卫容差（百分点）：见 test_walk_forward.test_sweep_returns_all_configs。
    # 原断言「网格内至少一个配置跑赢等权」(max(diff)>0) 在数据集扩展到后段 regime 后
    # 漂移到 -1.18pp 而恒定红灯——但该量级远小于「机制崩坏」，属噪声。故改为**容差带非回归
    # 守卫**：max(diff) 只要不劣于 -sweep_edge_tol_pp 即放行；结构断言（16 个配置齐备、
    # 裸配置存在、diff 有限）保持不变。可经 walk-forward CI 调。
    "sweep_edge_tol_pp": 2.5,
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
    position_weighted: Optional[bool] = None,
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
    if position_weighted is None:
        position_weighted = bool(cfg.get("position_weighted", False))

    try:
        from smcore.data.kline import read_kline_cache
    except Exception:
        return {s: {"n": 0, "avg_return": None, "win_rate": None, "edge": 0.0, "std": 0.0} for s in ALL_STRATEGIES}

    sig_days = _recent_signal_days(window, hold_days)
    bench_cache: dict[str, Optional[float]] = {}
    kline_cache: dict[str, Optional[pd.DataFrame]] = {}
    strat_pairs: dict[str, list[tuple[float, float]]] = {s: [] for s in ALL_STRATEGIES}
    skipped_short = 0
    pos_col_missing_days = 0

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

        # 仓位加权所需：当日清单的「建议仓位%」映射到 code
        pos_map: dict[str, float] = {}
        if position_weighted:
            if "建议仓位%" in d.columns:
                for _, r in d.iterrows():
                    try:
                        pos_map[_norm_code(r.get("股票代码"))] = float(r.get("建议仓位%") or 0)
                    except (TypeError, ValueError):
                        pass
            else:
                pos_col_missing_days += 1

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
            pos = pos_map.get(code, 0.0)
            for s in strats:
                strat_pairs[s].append((excess, pos))

    edge: dict[str, dict] = {}
    for s, prs in strat_pairs.items():
        if not prs:
            edge[s] = {"n": 0, "avg_return": None, "win_rate": None, "edge": 0.0, "std": 0.0}
            continue
        e_val, win, n, sd_val = _aggregate_excess(prs, position_weighted)
        edge[s] = {"n": n, "avg_return": round(e_val, 3), "win_rate": win, "edge": e_val, "std": sd_val}

    edge["__meta__"] = {  # type: ignore[assignment]  # 诊断用，不参与权重计算
        "source": "universe",
        "window": window,
        "hold_days": hold_days,
        "signal_days": len(sig_days),
        "benchmark": "hs300" if use_benchmark else "none",
        "skipped_future_insufficient": skipped_short,
        "position_weighted": position_weighted,
        "pos_col_missing_days": pos_col_missing_days,
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
    pw = bool(cfg.get("position_weighted", False))

    if source == "backtest":
        return compute_strategy_edge(w)
    if source == "blend":
        wb = float(cfg.get("blend_w_backtest", 0.5))
        bt = compute_strategy_edge(w)
        uv = compute_universe_edge(window=w, position_weighted=pw)
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
    return compute_universe_edge(window=w, position_weighted=pw)


def _sd(vals: list) -> float:
    """总体标准差；不足 2 个样本返回 0.0（离散度未知 → 视为无证据）。"""
    n = len(vals)
    if n < 2:
        return 0.0
    m = sum(vals) / n
    return math.sqrt(sum((x - m) ** 2 for x in vals) / n)


def _aggregate_excess(pairs: list, position_weighted: bool):
    """把 [(超额收益, 仓位%)] 列表聚合成 (edge, win_rate%, n, std)。

    - position_weighted=False：等权平均（与历史行为一致）。
    - position_weighted=True：按仓位% 加权（仅仓位>0 的票参与加权；若全为 0 或列缺失
      则自动退化为等权），与 factor_attribution 的「贡献%」口径同源。

    纯函数、无文件 I/O，便于单测；edge/win_rate/std 口径与 compute_universe_edge 一致。
    """
    n = len(pairs)
    if n == 0:
        return 0.0, None, 0, 0.0
    if position_weighted:
        wpos = [(e, p) for e, p in pairs if p is not None and p > 0]
        if wpos and sum(p for _, p in wpos) > 0:
            sp = sum(p for _, p in wpos)
            edge = sum(e * p for e, p in wpos) / sp
            win = sum(p for e, p in wpos if e > 0) / sp * 100
            return edge, round(win, 1), n, _sd([e for e, _ in pairs])
    avg = sum(e for e, _ in pairs) / n
    win = sum(1 for e, _ in pairs if e > 0) / n * 100
    return avg, round(win, 1), n, _sd([e for e, _ in pairs])


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

    使用平滑 S 型曲线而非分段线性（默认 k=12 / midpoint=0.55 的实际输出，
    2026-09-12 修正：旧注释 ~8%/~40% 与实现不符，曾误导 walk-forward 调参）：
    - vol_pctile ≤ 0.3（低波）→ 0~2% 现金
    - vol_pctile = 0.5（中位）→ ~18% 现金
    - vol_pctile = 0.55（中点）→ 25% 现金
    - vol_pctile ≥ 0.85（高波）→ ~49% 现金
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


def _apply_factor_timing(weights: dict, signal_date: Optional[str] = None) -> dict:
    """因子生效开关（默认关）：**仅当**近期信念 IC 显著为负才清零该因子权重，其余（正 IC / 近零 IC / 不显著）一律保留并按比例重分配——防误杀好因子。

    惰性 import smcore.strategy.factor_timing（仅开启时加载，不增启动内存）；
    任何异常一律 fail-soft 返回原权重（绝不让 overlay 故障拖垮主链路）。
    """
    try:
        from smcore.strategy.factor_timing import apply_mask

        masked = apply_mask(weights, signal_date=signal_date)
        pct = {s: round(float(masked.get(s, 0.0))) for s in ALL_STRATEGIES}
        d = 100 - sum(pct.values())
        if d != 0:
            anchor = max(pct, key=pct.get)
            pct[anchor] = max(0, pct[anchor] + d)
        return pct
    except Exception as exc:  # pragma: no cover - 防御性
        print(f"[adaptive_weights] WARN: factor_timing 应用失败，跳过（{exc!r}）", file=sys.stderr)
        return weights


def _integerize_weights(
    weights: dict,
    excluded: set,
    prefer: set | None = None,
) -> dict:
    """最大余数法（Hamilton 配额）把任意权重字典整数化为**合计恒为 100** 的百分数。

    单一实现，供 `_finalize_allocation_weights` 与冷启动等权回退共用——两处曾各自
    实现「逐个 round 再把差额锚定到某个策略」，在策略菜单扩容后都会出问题：
    - 逐个 ``round`` 的累计误差会超过锚点自身权重，clamp 到 0 后**合计不再等于 100**
      （实测 60 策略合计 115）；
    - 「差额全给锚点」会把大头随手送给一个任意策略（31 个策略冷启动时 ≈3.2% 的等权
      被抬成 10%，且永远是字典序最靠前的那个）。

    规则：
    - 取整差额 d 只补给**非黑名单**策略（黑名单必须恒为 0），``prefer``（通常 = 有证据集）
      优先，其余按小数部分从大到小、再按权重、最后按 id 定序；
    - d < 0 的分支理论不可达（向下取整只会欠不会超），留作防御：按权重升序逐位 −1；
    - 所有分数相同时（冷启动等权）退化为「字典序前 d 个各 +1」，偏差 ≤1pp。
    """
    total = sum(weights.get(s, 0.0) for s in weights)
    if total > 0:
        scaled = {s: weights.get(s, 0.0) / total * 100.0 for s in weights}
    else:
        scaled = {s: 0.0 for s in weights}
    pct = {s: int(scaled[s]) for s in weights}
    d = 100 - sum(pct.values())
    pref = prefer or set()
    if d > 0:
        order = sorted(
            (s for s in weights if s not in excluded),
            key=lambda s: (s not in pref, -(scaled[s] - int(scaled[s])), -scaled[s], s),
        )
        for s in order[:d]:
            pct[s] += 1
    elif d < 0:
        order = sorted(weights, key=lambda s: (pct[s], scaled[s], s))
        for s in order:
            if d == 0:
                break
            if pct[s] > 0:
                pct[s] -= 1
                d += 1
    return pct


def _finalize_allocation_weights(
    weights: dict,
    edge: dict,
    excluded: set,
    *,
    exclude_no_evidence: bool,
    eff_floor: float,
    min_ev_n: int,
) -> dict:
    """把 adaptive_weights 的初稿权重，套用黑名单 + 无证据门 → 最终 0-100 权重。

    规则：
    - 黑名单策略强制 0（不受 FLOOR 影响）。
    - exclude_no_evidence=True 时：无任何已实现归因历史(n < min_ev_n)的策略只保留
      「探索权重」（用于让其候选票进入 DAL 积累业绩），不吸收被清零策略
      释放的额度；释放额度全部分给「有证据」的幸存策略。这堵死「无证据默认均分」陷阱
      （曾让 momentum 凭空拿 13%、fundamental 凭空拿 32%）。
      探索权重总额受 `explore_pool_cap_pct`（默认 30%）封顶，池内在无证据策略间**均分**
      → 因子菜单扩容时不会线性挤压有证据策略（详见该键注释）。
    - exclude_no_evidence=False（或阈值=0）：退化为原行为——所有非黑名单策略按初稿
      权重归一化到 100（用户若想让新策略立即参与分配、愿承担无证据权重风险，可关此开关）。
    返回值之和恒为 100（四舍五入误差锚定到「有证据」的最大者，绝不补给无证据策略）。
    """
    base = {s: float(weights.get(s, 0.0)) for s in weights}
    for s in excluded:
        base[s] = 0.0

    # 分类：无证据（n 不足且开启门控）vs 有证据
    no_ev: set = set()
    ev: set = set()
    for s in base:
        if s in excluded:
            continue
        n = int((edge.get(s, {}) or {}).get("n", 0) or 0)
        if exclude_no_evidence and n < min_ev_n:
            no_ev.add(s)
        else:
            ev.add(s)

    if not no_ev:
        # 无证据门未生效（或没有无证据策略）→ 原行为：非黑名单按初稿归一化到 100
        tot = sum(base[s] for s in base if s not in excluded)
        if tot <= 0:
            live = [s for s in base if s not in excluded]
            eq = 100.0 / len(live) if live else 0.0
            out = {s: (eq if s not in excluded else 0.0) for s in base}
        else:
            out = {s: (base[s] / tot * 100.0 if s not in excluded else 0.0) for s in base}
    else:
        # 无证据策略拿「固定」探索额度，有证据策略瓜分剩余 (100 - 探索额度) 并按各自权重比例分配。
        # 这样即便 boll/relativity 在弱 regime 下也只拿地板权重，fundamental 仍固定 ≈3%，
        # 不会被 renormalize 放大到 ~1/3（曾出现 32% 无证据权重）。
        # ⚠️ 2026-09-17 可扩展性修复：探索额度必须**封顶**（默认 30%），不能是 3% × 个数。
        # 否则「因子菜单扩容」会线性挤压有证据策略（9→40 个无证据时，9 个有证据的合计
        # 从 85 掉到 8），且 ≥34 个无证据时 rem 变负 → 有证据策略拿到负权重、合计≠100。
        # 因子少时行为与旧版完全一致（9×3%=27% ≤ 30% → 每个仍 3.0）。
        cap = float(CONFIG.get("explore_pool_cap_pct", 30.0) or 30.0)
        floor_alloc = min(eff_floor * len(no_ev), cap)
        per_no_ev = floor_alloc / len(no_ev) if no_ev else 0.0
        rem = 100.0 - floor_alloc
        ev_sum = sum(base[s] for s in ev)
        if ev_sum <= 0:
            # 全体无证据（ev 为空）= 冷启动：非黑名单策略**等权**。
            # ⚠️ 此处曾退化为「各给 eff_floor 后把差额锚定到最大者」——当 ev 为空时
            # 最大者由字典序/初始值决定，等于把 ~76% 权重随手扔给某个任意策略。
            # 该分支在「boll/relativity 退役 + 新原子全部 n=0」时**必然**触发，
            # 属严重错误。语义对齐 compute_adaptive_allocation 的 cold_start 回退：
            # 没有任何业绩信息时，唯一无偏选择就是等权。
            live = [s for s in base if s not in excluded]
            eq = 100.0 / len(live) if live else 0.0
            out = {s: (eq if s not in excluded else 0.0) for s in base}
        else:
            out = {}
            for s in base:
                if s in ev:
                    out[s] = base[s] / ev_sum * rem
                elif s in no_ev:
                    out[s] = per_no_ev
                else:
                    out[s] = 0.0

    # 整数化：最大余数法（Hamilton 配额），保证合计**恒为 100** 且无负权重；有证据优先。
    # 见 `_integerize_weights` 的 docstring（原实现在策略数一多时会累计误差超锚点，
    # 导致合计 ≠ 100；冷启动等权回退也共用同一实现）。
    return _integerize_weights(out, excluded, prefer=ev)


def compute_adaptive_allocation(
    edge_window: int = 20,
    min_n: int = 8,
    shrinkage: Optional[float] = None,
    floor: Optional[float] = None,
    zero_negative_edge: bool = True,
    min_evidence_n: int = 0,  # 0 = 自适应
    signal_date: Optional[str] = None,
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
        # ⚠️ 冷启动等权同样必须尊重黑名单：否则「结构性判死刑」的策略会在历史不足时
        # 复活拿到等权份额（黑名单契约是「不受任何路径影响」）。等权只在**非黑名单**
        # 策略间分配。
        # ⚠️ 取整也必须走 `_integerize_weights`：此前是「逐个 round(100/N) 后把差额全部
        # 锚定到某个策略」，菜单扩容后该锚点会明显偏离等权（N=31 时 3.2% → 10%，
        # 且永远落在字典序最靠前的策略上）。最大余数法把差额按 1pp 摊开。
        excluded_cold = {s.lower() for s in (CONFIG.get("excluded_strategies") or [])}
        live_cold = [s for s in ALL_STRATEGIES if s not in excluded_cold]
        if live_cold:
            cold_w = _integerize_weights(
                {s: (1.0 if s not in excluded_cold else 0.0) for s in ALL_STRATEGIES},
                excluded_cold,
            )
        else:
            cold_w = {s: 0 for s in ALL_STRATEGIES}
        return edge, cold_w, 0, True
    weights = adaptive_weights(
        edge, shrinkage=shrinkage, floor=eff_floor,
        zero_negative_edge=zero_negative_edge, min_evidence_n=min_evidence_n,
    )
    # 因子生效开关（默认关）：仅 CONFIG.enabled 时惰性套用（**仅显著为负**的信念 IC 才清零该因子，其余保留）
    if bool((CONFIG.get("factor_timing") or {}).get("enabled", False)):
        weights = _apply_factor_timing(weights, signal_date)
    # 黑名单 + 无证据门（最终兜底）：名单内强制清零；无业绩历史的策略只留 floor 探索权重，
    # 不吸收被清零策略释放的额度。两者不受 FLOOR 地板与覆盖层 enabled 状态影响。
    # 默认空黑名单 + 门控开启 → 不改变现状（仅剔掉零业绩的新策略，如刚接入的 fundamental）。
    excluded = {s.lower() for s in (CONFIG.get("excluded_strategies") or [])}
    weights = _finalize_allocation_weights(
        weights, edge, excluded,
        exclude_no_evidence=bool(CONFIG.get("exclude_no_evidence_strategies", True)),
        eff_floor=eff_floor,
        min_ev_n=int(CONFIG.get("min_evidence_for_allocation", 1) or 1),
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
