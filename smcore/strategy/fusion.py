"""信号融合 —— 把四策略结果合并为"今日操作清单"。

此前四策略各自出 CSV、各自推送，用户收到四份独立报告后还要人工合并判断"今天到底买什么"。
本模块读取当日四策略结果，合并去重、打分、算止损止盈、分配仓位，输出一份操作清单。

输入（stock_data/ 下当日文件，缺失则回退最近）：
- Stock-Selection-Boll-YYYYMMDD.csv          (股票代码, 股票名称, 建议买入价)
- Stock-Selection-Relativity-YYYYMMDD.csv    (+ 上涨满足率, 抗跌满足率)
- Stock-Selection-Ashare-Theme-Turnover-*.csv (+ 综合分, 题材标签)
- CCTV-Sector-Stock-Pool-YYYYMMDD.csv        (股票代码, 股票名称, 板块, 热度分)

输出：
- stock_data/Daily-Action-List-YYYYMMDD.csv
- 日报段落文本（可追加到现有推送）

────────────────────────────────────────────────────────────
代码组织（2026-08-05 拆分）：
原 1173 行巨石按职责拆到同目录子模块，本文件仅保留融合编排 `fuse_signals`
并重新导出原内部函数以维持对外 API 兼容（10+ 处 `from smcore.strategy.fusion
import ...` 调用无需改动）。
- name_lookup.py   : 股票名称缓存与兜底查询
- regime_filter.py : 市场状态 / 沪深300 序列 / 相对强度过滤 / 动态阈值
- picks_loader.py  : 各策略 CSV 加载与回退
- boll_levels.py   : Boll 通道水位（止损止盈 / 近20日收益 / 流动性 / 波动率）
- position_sizing.py: 仓位分配与风险中性化（单名上限 / 行业分散 / 组合β）
- report.py        : 日报文本与清单落盘
────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import pandas as pd

from smcore.strategy.market import compute_market_profile
from smcore.strategy import sectors as sector_mod
from smcore.strategy.risk_rules import compute_adaptive_risk_params

# ── A 股交易约束 ─────────────────────────────────────────────────────
LOT_SIZE = 100  # A 股最小交易单位（一手 = 100 股）


def _lot_round(amount: float, price: float) -> float:
    """将建议金额向上取整到 A 股手数倍数（price * LOT_SIZE）。

    例：price=10.86, raw_amount=267 → lot_cost=1086 → 返回 1086
    若 price 无效或 ≤0，返回原值不截断。
    """
    if not (price > 0):
        return float(amount)
    lot_cost = round(price * LOT_SIZE, 2)
    if lot_cost <= 0:
        return float(amount)
    import math
    return math.ceil(float(amount) / lot_cost) * lot_cost


def _count_sectors(df, sector_map) -> int:
    """返回 df 中已映射（非"未知"）行业的不同行业数，供自适应风险参数计算广度。"""
    if df is None or df.empty or not sector_map or "股票代码" not in df.columns:
        return 1
    s = set()
    for c in df["股票代码"]:
        ind = sector_mod.industry_of(c, sector_map)
        if ind and ind != "未知":
            s.add(ind)
    return max(1, len(s))


# ── 子模块（重新导出以兼容历史调用点）─────────────────────────────────
from .name_lookup import (
    _build_stock_name_cache_from_akshare,
    _get_stock_name_map,
    _normalize_name,
    _stock_name_cache,
    lookup_stock_name,
)
from .regime_filter import (
    MIN_SIGNAL_AMOUNT,
    RS_APPLY_TO_MOMENTUM,
    RS_LOOKBACK,
    RS_TOL,
    TREND_GUARD_BELOW_MA20,
    _adaptive_multi_hit_bonus,
    _detect_market_regime,
    _dynamic_thresholds,
    _fetch_hs300_akshare,
    _fetch_hs300_baostock,
    _get_hs300_close,
    _HS300_CLOSE_CACHE,
    _index_20d_return,
    _passes_relative_strength_filter,
    _passes_trend_guard,
)
from .picks_loader import (
    _extract_date_from_filename,
    _find_strategy_csv,
    _load_boll_picks,
    _load_cctv_picks,
    _load_momentum_picks,
    _load_relativity_picks,
    _load_theme_picks,
)
from .boll_levels import _compute_boll_levels
from .position_sizing import (
    _apply_beta_cap,
    _apply_position_sizing,
    _apply_strategy_cap,
    _estimate_betas,
    _portfolio_beta,
)
from .report import (
    _build_report_text,
    _format_source_date_notes,
    save_action_list,
)

__all__ = [
    # 编排
    "fuse_signals",
    # name_lookup
    "lookup_stock_name",
    "_get_stock_name_map",
    "_build_stock_name_cache_from_akshare",
    "_normalize_name",
    "_stock_name_cache",
    # regime_filter
    "TREND_GUARD_BELOW_MA20",
    "RS_LOOKBACK",
    "RS_TOL",
    "MIN_SIGNAL_AMOUNT",
    "RS_APPLY_TO_MOMENTUM",
    "_dynamic_thresholds",
    "_adaptive_multi_hit_bonus",
    "_passes_trend_guard",
    "_detect_market_regime",
    "_HS300_CLOSE_CACHE",
    "_fetch_hs300_baostock",
    "_fetch_hs300_akshare",
    "_get_hs300_close",
    "_index_20d_return",
    "_passes_relative_strength_filter",
    # picks_loader
    "_extract_date_from_filename",
    "_find_strategy_csv",
    "_load_boll_picks",
    "_load_relativity_picks",
    "_load_theme_picks",
    "_load_cctv_picks",
    "_load_momentum_picks",
    # boll_levels
    "_compute_boll_levels",
    # position_sizing
    "_apply_strategy_cap",
    "_estimate_betas",
    "_portfolio_beta",
    "_apply_position_sizing",
    "_apply_beta_cap",
    # report
    "_build_report_text",
    "_format_source_date_notes",
    "save_action_list",
]


def fuse_signals(
    date_yyyymmdd: str,
    *,
    total_capital: float = 100000.0,
    max_picks: int = 50,
    max_per_strategy: int = None,
    fetch_levels: bool = True,
    trend_guard: bool = True,
    market_gate: bool = True,
    relative_strength_filter: bool = True,
    min_signal_amount: float = MIN_SIGNAL_AMOUNT,
    dynamic_thresholds: bool = True,
    sector_cap: bool = True,
    max_per_sector: int = None,
    sector_weight_cap: bool = True,
    max_sector_weight_pct: float = None,
    beta_neutral: bool = True,
    max_portfolio_beta: float = None,
    max_single_weight_pct: float = None,
    beta_min_keep: int = None,
    max_stale_days: int = 3,
) -> tuple[pd.DataFrame, str]:
    """融合四策略信号，输出今日操作清单。

    Args:
        date_yyyymmdd: 日期字符串 YYYYMMDD
        total_capital: 总资金（元），用于算每只票建议仓位金额
        max_picks: 最多输出几只票
        fetch_levels: 是否拉 K 线算止损止盈（批量调用网络较慢，可关闭只出清单）
        max_stale_days: 允许回退的历史策略文件最大天数（默认 3 天）

    Returns:
        (result_df, report_text)
    """
    boll, boll_date = _load_boll_picks(date_yyyymmdd, max_stale_days=max_stale_days)
    relativity, rel_date = _load_relativity_picks(date_yyyymmdd, max_stale_days=max_stale_days)
    theme, theme_date = _load_theme_picks(date_yyyymmdd, max_stale_days=max_stale_days)
    cctv, cctv_date = _load_cctv_picks(date_yyyymmdd, max_stale_days=max_stale_days)
    momentum, mom_date = _load_momentum_picks(date_yyyymmdd, max_stale_days=max_stale_days)

    source_dates = {
        "Boll": boll_date,
        "Relativity": rel_date,
        "Theme": theme_date,
        "CCTV": cctv_date,
        "Momentum": mom_date,
    }

    # 合并所有代码
    all_codes = set(boll) | set(relativity) | set(theme) | set(cctv) | set(momentum)
    if not all_codes:
        return pd.DataFrame(), "今日无任何策略命中，无可操作清单。"

    # 多维市场仪表盘：综合 趋势/波动率/宽度/量能 判定 regime（向后兼容三态）
    profile = compute_market_profile() if market_gate else None
    regime = profile.regime if profile else "震荡轮动"
    # 动态过滤阈值（RS 容忍度 / 流动性门槛随市浮动）
    rs_tol, min_amt = (RS_TOL, min_signal_amount)
    if dynamic_thresholds:
        rs_tol, min_amt = _dynamic_thresholds(regime, profile)

    # ── 自适应策略权重（核心）：纯数据驱动，零硬编码 ──
    # 权重 = softmax(近期 edge) + 贝叶斯收缩 + 向等权收缩 + 清零门，
    # 随各策略表现自动此消彼长。无 regime 权重表、无 fallback 常数。
    # regime 仍负责「趋势闸门 / RS 过滤 / 流动性门槛」这些自适应开关。
    from smcore.strategy.adaptive_weights import (
        cash_from_regime,
        cash_from_volatility,
        compute_adaptive_allocation,
        save_regime_snapshot,
    )

    edge, adaptive_pct, _, cold = compute_adaptive_allocation()
    strategy_scores = adaptive_pct  # 综合评分基础分（百分比量级）
    # 现金比例 = 波动率 S 型曲线（连续，无魔法数字上下限）
    cash_pct = cash_from_volatility(profile.volatility_pctile if profile else None)
    # 趋势维度微调：下行防御追加现金、趋势上行压减（幅度由波动率决定）
    if profile:
        cash_pct = cash_from_regime(regime, cash_pct)
    # 仓位权重 = 自适应权重按 (100-现金)% 缩放（不含现金），供单票 sizing
    weights = {s: adaptive_pct.get(s, 0) * (100 - cash_pct) / 100.0 for s in adaptive_pct}

    rows = []
    # 单名仓位上限（小数）：初值，最终在风险层用实际名单长度重算（自适应，零硬编码）
    _early_risk = compute_adaptive_risk_params(regime=regime, profile=profile, n_picks=max_picks)
    max_single_weight_frac = (
        max_single_weight_pct if max_single_weight_pct is not None else _early_risk["max_single_weight_pct"]
    ) / 100.0
    single_cap_hit = 0  # 命中单名上限的只数（最后一趟重算为准）
    # 候选股近20日收益（ret20），供板块轮动动量加成使用（融合已算过，零额外联网）
    cand_ret20: dict[str, Optional[float]] = {}
    gated_out = 0
    rs_filtered_out = 0
    liquidity_filtered_out = 0
    for code in all_codes:
        # 拉 K 线：fetch_levels 需算止损止盈；relative_strength_filter 需近20日收益（复用同一次拉取）
        levels = _compute_boll_levels(code, date_yyyymmdd) if (fetch_levels or relative_strength_filter) else {}
        cand_ret20[code] = levels.get("ret20")
        hit_strategies = []
        score = 0
        name = ""
        buy_price = None

        if code in boll:
            hit_strategies.append("Boll")
            score += strategy_scores.get("boll", 0)
            name = boll[code]["name"] or name
            buy_price = boll[code].get("buy_price")
        if code in relativity:
            hit_strategies.append("Relativity")
            score += strategy_scores.get("relativity", 0)
            name = relativity[code]["name"] or name
        if code in theme:
            hit_strategies.append("Theme")
            score += strategy_scores.get("theme", 0)
            name = theme[code]["name"] or name
            # Theme 综合分作为额外加权（综合分 0-100，按 10% 加）
            theme_score = theme[code].get("score") or 0
            score += min(theme_score * 0.1, 10)
        if code in cctv:
            hit_strategies.append("CCTV")
            score += strategy_scores.get("cctv", 0)
            name = cctv[code]["name"] or name
        if code in momentum:
            hit_strategies.append("Momentum")
            score += strategy_scores.get("momentum", 0)
            name = momentum[code]["name"] or name

        # ── 买入价兜底：非 Boll 策略无建议买入价时用信号日收盘价 ─────────
        if buy_price is None and levels:
            buy_price = levels.get("close")

        # ── 名字兜底：所有策略 CSV 都缺名字时从 stock_info / baostock 补查 ────────
        if not _normalize_name(name):
            name = lookup_stock_name(code)

        # 多策略命中加分
        if len(hit_strategies) > 1:
            score += (len(hit_strategies) - 1) * _adaptive_multi_hit_bonus(len(adaptive_pct) - list(adaptive_pct.values()).count(0))

        # 趋势闸门：下行防御时不买纯均值回归票（Boll/Relativity）。
        # 其「次日买、持有10日」在弱市必亏（实测 BASELINE 弱市 -5%~-9%），直接不出。
        if market_gate and regime == "下行防御" and set(hit_strategies) <= {"Boll", "Relativity"}:
            gated_out += 1
            continue

        # 相对大盘强度过滤：剔除「大盘涨、个股仍明显跑输」的票（针对根因 alpha 弱）。
        # rs_tol 随市浮动（趋势上行放宽、下行防御收紧），由 _dynamic_thresholds 给出。
        if relative_strength_filter:
            stock_ret = levels.get("ret20")
            index_ret = _index_20d_return(date_yyyymmdd)
            if not _passes_relative_strength_filter(hit_strategies, stock_ret, index_ret, tol=rs_tol):
                rs_filtered_out += 1
                continue

        # 流动性门槛：剔除信号日成交额过低（难出场/庄股陷阱）的票。
        # 复用 _compute_boll_levels 已拉的 K 线（amount 字段）。取值为 None 时放行，
        # 避免数据源故障把整份清单误杀。min_amt 随市浮动（下行防御抬高）。
        if min_amt and min_amt > 0:
            amt = levels.get("amount")
            if amt is not None and amt < min_amt:
                liquidity_filtered_out += 1
                continue

        # 仓位分配：按命中策略中权重最大的那个分配，单票取该策略权重的 1/N（N=该策略候选数）
        # 避免大池子策略（如 CCTV 673只）把仓位稀释到 0
        strategy_pick_counts = {
            "boll": len(boll),
            "relativity": len(relativity),
            "theme": len(theme),
            "cctv": len(cctv),
            "momentum": len(momentum),
        }
        # 取命中策略中权重最高者
        best_weight = 0
        best_raw = 0.0
        for s in hit_strategies:
            skey = s.lower()
            w = weights.get(skey, 0)
            cnt = max(strategy_pick_counts.get(skey, 1), 1)
            share = w / cnt  # 该策略仓位均分到每只票
            if share > best_weight:
                best_weight = share
            if w > best_raw:
                best_raw = w
        position_pct = min(best_weight / 100.0, max_single_weight_frac)  # 单名仓位上限（初值，最终以重算为准）
        position_amount = total_capital * position_pct

        row = {
            "股票代码": code,
            "股票名称": name,
            "命中策略数": len(hit_strategies),
            "来源策略": "/".join(hit_strategies),
            "综合评分": round(score, 1),
            "权重": round(best_raw, 1),
            "建议买入价": buy_price,
            "建议仓位%": round(position_pct * 100, 1),
            "建议金额": round(_lot_round(position_amount, buy_price or 0), 0),
        }

        if levels:
            row["最新价"] = levels.get("close")
            row["止损价(下轨)"] = levels.get("lower")
            row["止盈价(上轨)"] = levels.get("upper")
            row["MA20"] = levels.get("ma20")
            # 逐只波动率自适应止损比例（引擎 stop_pct 列优先于全局 8% 兜底），
            # 使生产清单携带与个股波动匹配的止损，供 PositionMonitor/引擎使用。
            row["stop_pct"] = levels.get("stop_pct")

        rows.append(row)

    df = pd.DataFrame(rows)
    # 趋势守卫：剔除价格远低于 MA20 的破位/下降通道股（自由落体风险）
    filtered_out = 0
    if trend_guard:
        before = len(df)
        mask = df.apply(
            lambda r: _passes_trend_guard(r.get("最新价"), r.get("MA20")),
            axis=1,
        )
        df = df[mask].reset_index(drop=True)
        filtered_out = before - len(df)
    if df.empty:
        # 全部候选被过滤（趋势闸门/RS/流动性）时返回空清单，避免空 DF sort_values 崩溃
        df = df.head(max_picks)
        sector_hit_cap = False
    else:
        # 板块轮动（确认型）：用候选 ret20 聚合板块动量，给强势板块候选小幅加成
        # 仅在本轮已筛候选内有效，零额外联网；样本不足或板块映射缺失时自动跳过（加成=0）。
        sector_hit_cap = False
        sector_map = sector_mod.ensure_industries(all_codes) if sector_cap else {}
        if sector_cap and sector_map:
            sector_bonus, _meds = sector_mod.compute_sector_momentum(cand_ret20, sector_map)
            if sector_bonus:
                df["综合评分"] = df.apply(
                    lambda r: round(
                        r["综合评分"]
                        + sector_bonus.get(sector_mod.industry_of(r["股票代码"], sector_map), 0.0),
                        1,
                    ),
                    axis=1,
                )
        df = df.sort_values("综合评分", ascending=False).reset_index(drop=True)
        # ── 自适应风险参数（随名单广度 + 行业数 + regime 实时计算，零硬编码）──
        _sectors = _count_sectors(df, sector_map) if (sector_cap and sector_map) else 1
        _risk = compute_adaptive_risk_params(
            regime=regime, profile=profile, n_picks=max(len(df), 1), n_sectors=_sectors
        )
        max_per_strategy_eff = max_per_strategy if max_per_strategy is not None else _risk["max_per_strategy"]
        max_per_sector_eff = max_per_sector if max_per_sector is not None else _risk["max_per_sector"]
        max_sector_weight_eff = max_sector_weight_pct if max_sector_weight_pct is not None else _risk["max_sector_weight_pct"]

        # 按策略分散上限：防止单策略占满最终名单、压垮分散度，
        # 保证自适应策略权重能真正生效（每个策略都有代表票进入回测）
        df = _apply_strategy_cap(df, max_per_strategy_eff)
        # 单板块集中度控制：最终入选单板块最多 max_per_sector_eff 只，强制分散（映射缺失则跳过）
        if sector_cap and sector_map:
            df, sector_hit_cap = sector_mod.apply_sector_cap(
                df, sector_map, max_per=max_per_sector_eff, top_n=max_picks
            )
        else:
            df = df.head(max_picks)

    # ── ③ 仓位稀释修复：按「最终入选清单中每策略存活票数」均分权重，
    # 而非按策略全池大小（如 CCTV 全池几百只会把每只高确定性票的仓位稀释到 ~0.03%）。
    # 这样自适应权重才能真实 deploy 到被选中的票上，而非被大池子摊没。
    if not df.empty:
        _STRATS = {"boll", "theme", "relativity", "cctv", "momentum"}
        _surv = {s: 0 for s in _STRATS}
        for _, _r in df.iterrows():
            for _s in str(_r.get("来源策略", "")).replace("/", "，").split("，"):
                _s = _s.strip().lower()
                if _s in _surv:
                    _surv[_s] += 1
        # 单名仓位上限（小数）：随名单长度自适应，且受天花板约束（零硬编码）
        max_single_eff = max_single_weight_pct if max_single_weight_pct is not None else _risk["max_single_weight_pct"]
        max_single_weight_frac = max_single_eff / 100.0
        df, single_cap_hit = _apply_position_sizing(
            df, weights, _surv, total_capital, max_single_weight_frac
        )

    # ── ④ 风险中性化 ────────────────────────────────────────────────
    # (a) 行业权重上限：任一板块总仓位 ≤ 全组合 × max_sector_weight_eff（与数量上限互补）
    beta_hit = 0
    if not df.empty and sector_map:
        if sector_weight_cap:
            df, weight_hit = sector_mod.apply_sector_weight_cap(
                df, sector_map, max_weight_pct=max_sector_weight_eff, top_n=max_picks
            )
        else:
            weight_hit = False
        # (b) 组合 β 软约束：组合对沪深300 的加权 β 超上限则逐步剔除最高 β 个股
        max_beta_eff = max_portfolio_beta if max_portfolio_beta is not None else _risk["max_portfolio_beta"]
        beta_min_keep_eff = beta_min_keep if beta_min_keep is not None else _risk["beta_min_keep"]
        if beta_neutral and len(df) > beta_min_keep_eff:
            betas = _estimate_betas(df["股票代码"].tolist(), date_yyyymmdd)
            df, beta_hit = _apply_beta_cap(df, betas, max_beta=max_beta_eff, min_keep=beta_min_keep_eff)
            df = df.reset_index(drop=True)
            # β 约束可能改变入选，用最新名单长度重算仓位金额（覆盖上面的 single_cap_hit 计数）
            if not df.empty:
                _risk2 = compute_adaptive_risk_params(
                    regime=regime, profile=profile, n_picks=max(len(df), 1), n_sectors=_sectors
                )
                max_single_eff = max_single_weight_pct if max_single_weight_pct is not None else _risk2["max_single_weight_pct"]
                max_single_weight_frac = max_single_eff / 100.0
                df, single_cap_hit = _apply_position_sizing(
                    df, weights, _surv, total_capital, max_single_weight_frac
                )
    else:
        weight_hit = False

    # 生成日报段落
    report = _build_report_text(
        df,
        date_yyyymmdd,
        len(boll),
        len(relativity),
        len(theme),
        len(cctv),
        len(momentum),
        source_dates=source_dates,
        max_stale_days=max_stale_days,
        max_single_weight_pct=max_single_eff,
    )
    if filtered_out:
        report += f"\n- 🛡️ 趋势守卫剔除 {filtered_out} 只破位/下降通道股（价格低于 MA20 超 12%）"
    if market_gate:
        if regime == "下行防御" and gated_out:
            report += f"\n- 🚦 趋势闸门触发（市场下行防御）：剔除 {gated_out} 只纯均值回归候选（Boll/Relativity），仅留顺势策略"
        else:
            cold_tag = "（冷启动等权）" if cold else "（自适应·按近期业绩）"
            report += (
                f"\n- 🚦 市场状态：{regime}（趋势闸门生效）；"
                f"策略权重{cold_tag}：Boll {adaptive_pct.get('boll')} / "
                f"Momentum {adaptive_pct.get('momentum')} / Theme {adaptive_pct.get('theme')} / "
                f"Relativity {adaptive_pct.get('relativity')} / CCTV {adaptive_pct.get('cctv')}；"
                f"现金 {cash_pct}%"
            )
    if rs_filtered_out:
        report += f"\n- 📉 相对强度过滤剔除 {rs_filtered_out} 只跑输大盘超 {rs_tol * 100:.0f}% 的票（alpha 弱，直接不治本；阈值随市浮动）"
    if liquidity_filtered_out:
        report += f"\n- 💧 流动性门槛剔除 {liquidity_filtered_out} 只信号日成交额 < ¥{min_amt / 1e8:.2f}亿 的票（难出场/庄股陷阱；门槛随市浮动）"
    if sector_cap:
        n_sec = df["股票代码"].map(lambda c: sector_mod.industry_of(c)).nunique() if not df.empty else 0
        cap_note = "（单板块上限 %d，已触发分散）" % max_per_sector_eff if sector_hit_cap else "（单板块上限 %d）" % max_per_sector_eff
        report += f"\n- 🏭 板块轮动+集中度：最终 {len(df)} 只覆盖 {n_sec} 个行业{cap_note}（强势板块候选已微调评分）"
    if not df.empty:
        wcap_note = "（单行业权重≤%g%%，已触发）" % max_sector_weight_eff if (sector_weight_cap and weight_hit) else "（单行业权重≤%g%%）" % max_sector_weight_eff
        betas = _estimate_betas(df["股票代码"].tolist(), date_yyyymmdd) if beta_neutral else {}
        pb = _portfolio_beta(df, betas) if beta_neutral else None
        beta_note = f"；组合β={pb:.2f}（上限{max_beta_eff}，剔除{beta_hit}只高β）" if beta_neutral else ""
        single_note = "（单名上限≤%g%%，已截断%d只）" % (max_single_eff, single_cap_hit) if single_cap_hit else "（单名上限≤%g%%）" % max_single_eff
        report += f"\n- ⚖️ 风险中性化{wcap_note}{single_note}{beta_note}"
    if profile is not None:
        report += f"\n- 🌡️ 市场仪表盘：{profile.summary()}"

    # 落盘市场状态 + 自适应权重快照，供前端/接口直接展示（确认权重确实在随市场自适应）
    try:
        snapshot = {
            "date": date_yyyymmdd,
            "regime": regime,
            "cash_pct": cash_pct,
            "cold_start": cold,
            "method": "fully_adaptive(softmax+ebayes_shrinkage+pseudo15+zero_negative_edge+adaptive_evidence_n+S-cash_curve+regime_cash_adj+dynamic_thresholds+sector_weight_cap+beta_neutral)",
            "adaptive_weights": adaptive_pct,
            "strategy_edge": {s: edge.get(s, {}) for s in adaptive_pct},
            "market_profile": profile.summary() if profile else None,
            "portfolio_beta": round(_portfolio_beta(df, _estimate_betas(df["股票代码"].tolist(), date_yyyymmdd)), 2) if (beta_neutral and not df.empty) else None,
            "sector_weight_cap_pct": max_sector_weight_eff if sector_weight_cap else None,
            "beta_ceiling": max_beta_eff if beta_neutral else None,
            "single_weight_cap_pct": max_single_eff,
        }
        save_regime_snapshot(snapshot)
    except Exception:
        pass
    return df, report
