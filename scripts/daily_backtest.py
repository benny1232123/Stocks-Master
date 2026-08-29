#!/usr/bin/env python3
"""每日全策略清单 → 前向信号回测（供 GitHub Actions daily-pick 调用）。

语义：对每个历史信号日（默认近 LOOKBACK_DAYS=30 天）独立做「前向信号回测」——
锁定该信号日产生的 Daily-Action-List，从信号日次日开盘买入、持有
min(HOLD_DAYS, 距今天数) 天后卖出，回测这段「往后」的真实表现。
即「从历史某天开始策略 → 往后回测」，而非在过去 N 天里重跑策略引擎重新派生信号。

每天 CI 运行时：
- 对「窗口已走完」（信号日 + 持有期 ≤ 今天）的清单做完整 HOLD_DAYS 天回测；
- 对「窗口未走完」的近期信号（如最近几天）做部分前向回测（持有到今天），
  结果随日期延长逐步更新，直到持有期满；
最终形成「从一个月前每天开始」的一系列前向回测，每天都在滚动积累。

每个信号日独立存档：stock_data/Multi-Backtest-{信号日}-{summary,equity,trades}.csv，
命名带信号日，便于追溯「这是哪天的信号、往后持有 N 天的真实结果」。

性能优化（2026-07-15）：
- 预拉阶段：在逐日回测前，一次性收集所有信号日的全部候选标的，
  每只股票仅拉一次 K 线（最宽区间），填满进程缓存 + 文件缓存，
  避免回测循环中 30天×100只 = 3000+ 次重复 akshare HTTP 请求。
- 全程进度输出：预拉 / 内联过滤 / 引擎加载均有逐条或百分比进度。
"""
from __future__ import annotations

import os
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

# 确保项目根（scripts/ 的父目录）在 sys.path，便于 `python scripts/daily_backtest.py` 直接运行
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from smcore.artifacts import PROJECT_ROOT, STOCK_DATA_DIR
from smcore.backtest import run_forward_signal_backtest
from smcore.utils.code import format_stock_code
import smcore.data.kline as kline_mod  # 用于注入进程内 K 线缓存，避免跨信号日重复抓取

# 内联过滤（复用 fusion 层的 RS 过滤与流动性门槛逻辑，确保回测输入与生产融合一致）
from smcore.strategy.fusion import (
    _passes_relative_strength_filter,
    _index_20d_return,
    _compute_boll_levels,
    _get_hs300_close,
)
# 多维市场仪表盘：波动率自适应风控的共同输入
from smcore.strategy.market import compute_market_profile
# 自适应现金：波动率分位 S 型曲线 + 趋势 regime 调整（替代硬编码 _VOL_POS_SCALE_MAP）
from smcore.strategy.adaptive_weights import cash_from_volatility, cash_from_regime, cash_from_drawdown
# 出场参数自适应（随波动率/regime 浮动，基线=已验证的 8/6/5/60）
from smcore.strategy.risk_rules import compute_adaptive_exit_params

STRAT_MAP = {
    "boll": "boll",
    "relativity": "relativity",
    "theme": "theme",
    "cctv": "cctv",
}

# 回测只取综合评分最高的前 TOP_N 只，避免信号过多把资金摊成数百个迷你仓位、
# 导致权益曲线近乎水平（「曲线不动」）。TOP_N 个等权仓位每只约 initial/TOP_N，曲线才能看出涨跌。
TOP_N = 30

# 内联过滤前的预筛选上限：先按评分粗取 N 只再做昂贵的 RS/量能计算，
# 避免对全量 600+ 候选逐只拉 K 线导致超时。
FILTER_PRE_TOP_N = 100

# 波动率自适应止损：个股近20日波动率 vol20 → 止损比例 = clamp(VOL_STOP_MULT*vol20, 6%, 15%)。
# 高波动股给更宽止损避免被洗、低波动给更紧；无 vol 数据时回退引擎全局 -8%。
VOL_STOP_MULT = 8.0
# 总仓位随市场波动率缩放（高波动留现金）：改为自适应——由 cash_from_volatility(波动率分位)
# 的 S 型曲线 + cash_from_regime(趋势) 计算，不再用硬编码 low/mid/high 映射。


class _PortfolioCurve:
    """跨信号日重建组合权益曲线，计算组合级滚动回撤（用于回撤熔断）。

    - 每个信号日的前向回测是一个独立 sleeve，从 10 万本金跑 hold_days；
    - 组合指数（日历日 d）= 当日所有「活跃」sleeve 的 total 之**均值**
      （等权跨开放 sleeve，去除「sleeve 数量随时间增减」带来的伪回撤）；
    - 滚动回撤 = (历史峰值 - 当前均值) / 历史峰值 × 100。
    因果性：sleeve T 的权益从 T+1 才开始计入，故用「信号日 T」查询回撤时，
    sleeve T 自身尚未贡献，不会用到未来信息。
    """

    def __init__(self):
        self._sleeves: dict[str, dict[date, float]] = {}

    def add_sleeve(self, signal_tag: str, series: dict[date, float]) -> None:
        """加入 / 替换某个信号日的 sleeve 权益序列（按 tag 去重）。"""
        self._sleeves[signal_tag] = dict(series)

    def drawdown_as_of(self, as_of: date) -> float:
        """返回截至 as_of（含）的组合滚动回撤（百分比，0 表示无回撤）。"""
        all_dates = sorted(
            {d for s in self._sleeves.values() for d in s if d <= as_of}
        )
        if not all_dates:
            return 0.0
        peak = 0.0
        worst = 0.0
        for d in all_dates:
            vals = [s[d] for s in self._sleeves.values() if d in s]
            if not vals:
                continue
            avg = sum(vals) / len(vals)
            if avg > peak:
                peak = avg
            if peak > 0:
                cur = (peak - avg) / peak * 100.0
                if cur > worst:
                    worst = cur
        return worst


def _load_equity_series(path: Path) -> dict[date, float]:
    """从某个 Multi-Backtest-*-equity.csv 读取 日历日→total 的序列（容错）。"""
    try:
        eq = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return {}
    if eq.empty or "date" not in eq.columns or "total" not in eq.columns:
        return {}
    series: dict[date, float] = {}
    for _, row in eq.iterrows():
        try:
            d = datetime.strptime(str(row["date"])[:10], "%Y-%m-%d").date()
            series[d] = float(row["total"])
        except Exception:
            continue
    return series


def _parse_signal_date(name: str) -> date | None:
    m = re.search(r"(\d{8})", name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d").date()
    except ValueError:
        return None


def derive_strategies(source_series: pd.Series) -> str:
    """从来源策略列（如 'Boll/Relativity/Theme'）解析启用的策略集合。"""
    enabled = set()
    for raw in source_series.dropna():
        for part in str(raw).split("/"):
            key = part.strip().lower()
            if key in STRAT_MAP:
                enabled.add(STRAT_MAP[key])
    if not enabled:
        enabled = {"boll", "relativity", "theme"}
    return ",".join(sorted(enabled))


def collect_eligible_lists(lookback_days: int) -> list[tuple[Path, date]]:
    """返回 (路径, 信号日) 列表，仅含「信号日 >= 今天 - lookback_days」的清单（旧→新排序）。"""
    cutoff = date.today() - timedelta(days=lookback_days)
    cands: list[tuple[Path, date]] = []
    for path in STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"):
        sd = _parse_signal_date(path.name)
        if sd is None:
            continue
        if sd >= cutoff:
            cands.append((path, sd))
    cands.sort(key=lambda x: x[1])
    return cands


def _backtest_one(path: Path, sd: date, hold_days: int, market_profile=None, portfolio_curve=None, dd_thr=8.0, dd_cap=50.0, dd_deep=20.0) -> dict | None:
    """对单个信号日做前向回测并落盘，返回摘要信息；无有效结果返回 None。"""
    df = pd.read_csv(path, encoding="utf-8-sig")
    if df.empty or "股票代码" not in df.columns:
        return None

    sd_yyyymmdd = sd.strftime("%Y%m%d")

    # ① 前导零归一（旧 CSV 可能丢失前导零，如 000915→915）
    df = df.copy()
    df["股票代码"] = df["股票代码"].apply(format_stock_code)
    df = df[df["股票代码"].str.len() >= 6].reset_index(drop=True)

    # K 线结果按 code 进程内缓存（内联过滤 + 波动率止损会重复调用 _compute_boll_levels，
    # 且 run_forward_signal_backtest 跨信号日会重复抓同一只票的 K 线）。单次抓取失败也容错
    # 返回 {}，不让单只票的数据源抖动把整轮回测拖垮。
    _lv_cache: dict[str, dict] = {}

    def _lv(code: str) -> dict:
        c = str(code).strip()
        if c not in _lv_cache:
            try:
                _lv_cache[c] = _compute_boll_levels(c, sd_yyyymmdd) or {}
            except Exception:
                _lv_cache[c] = {}
        return _lv_cache[c]

    # ①⑤ 预筛选：按评分粗取 FILTER_PRE_TOP_N 只，避免对 600+ 候选逐只拉 K 线
    if "综合评分" in df.columns and len(df) > FILTER_PRE_TOP_N:
        df["_pre_s"] = pd.to_numeric(df["综合评分"], errors="coerce")
        df = df.sort_values("_pre_s", ascending=False).head(FILTER_PRE_TOP_N).drop(columns=["_pre_s"]).reset_index(drop=True)

    # ② 内联 RS 过滤 + 流动性门槛（与 fusion.py 生产融合逻辑一致，
    #    确保 daily_backtest 的回测输入与「当天新跑 fusion」的输出等价）
    _inline_filter_enabled = os.environ.get("BACKTEST_INLINE_FILTER", "1") == "1"
    rs_dropped = liq_dropped = 0
    if _inline_filter_enabled and len(df) > 0:
        sd_yyyymmdd = sd.strftime("%Y%m%d")
        idx_ret = _index_20d_return(sd_yyyymmdd)
        _min_amt = float(os.environ.get("BACKTEST_MIN_AMOUNT", "100000000"))  # default ¥1亿
        _keep_mask = []
        _filter_total = len(df)
        for _fi, (_, row) in enumerate(df.iterrows()):
            if (_fi + 1) % 20 == 0 or _fi + 1 == _filter_total:
                print(f"  [过滤] {_fi+1}/{_filter_total} ...", flush=True)
            code = str(row["股票代码"]).strip()
            hit = [s.strip().lower() for s in str(row.get("来源策略", "")).split("/") if s.strip()]
            # RS 过滤（K 线失败容错：lv 为空则放行，避免数据源抖动把整份清单误杀）
            lv = _lv(code)
            stock_ret = lv.get("ret20")
            amt = lv.get("amount")
            if not _passes_relative_strength_filter(hit, stock_ret, idx_ret):
                rs_dropped += 1
                _keep_mask.append(False)
                continue
            # 流动性门槛
            if amt is not None and amt < _min_amt:
                liq_dropped += 1
                _keep_mask.append(False)
                continue
            _keep_mask.append(True)
        df = df[_keep_mask].reset_index(drop=True)
        if rs_dropped or liq_dropped:
            print(f"  [内联过滤] RS剔除={rs_dropped} 流动性剔除={liq_dropped} 保留={len(df)}")

    # 按综合评分取前 TOP_N，避免信号过多导致仓位被摊薄、权益曲线近乎不动
    if "综合评分" in df.columns:
        df = df.copy()
        df["_s"] = pd.to_numeric(df["综合评分"], errors="coerce")
        df = df.sort_values("_s", ascending=False)
        if len(df) > TOP_N:
            df = df.head(TOP_N)
        df = df.drop(columns=["_s"])
    codes = [format_stock_code(c) for c in df["股票代码"].dropna().tolist() if format_stock_code(c)]
    if not codes:
        return None

    sub = pd.DataFrame({"日期": [sd.strftime("%Y-%m-%d")] * len(codes), "代码": codes})
    if "建议买入价" in df.columns:
        sub["建议买入价"] = df["建议买入价"].values[: len(codes)]
    if "止损价(下轨)" in df.columns:
        sub["止损价(下轨)"] = df["止损价(下轨)"].values[: len(codes)]
    if "止盈价(上轨)" in df.columns:
        sub["止盈价(上轨)"] = df["止盈价(上轨)"].values[: len(codes)]
    # 置信度加权：把综合评分带入回测，按确定性分配仓位（measure_position_sizing 验证
    # 组合总收益 +1.02pp / 夏普 +0.71 / 回撤收窄 0.5pp；可按 BACKTEST_SIZE_BY="" 关闭回退等权）
    if "综合评分" in df.columns:
        sub["综合评分"] = pd.to_numeric(df["综合评分"], errors="coerce").values[: len(codes)]
    # 自适应策略权重（fusion 写入 DAL 的「权重」列）：回测按此列定仓，
    # 使自适应策略权重真正驱动收益（替代原先纯综合评分定仓）
    if "权重" in df.columns:
        sub["权重"] = pd.to_numeric(df["权重"], errors="coerce").values[: len(codes)]

    # 波动率自适应风控（market profile 驱动）
    _vol_stop_on = os.environ.get("VOL_SCALED_STOP", "1") == "1"
    _vol_pos_on = os.environ.get("VOL_POS_SCALE", "1") == "1"
    _vol_mult = float(os.environ.get("VOL_STOP_MULT", str(VOL_STOP_MULT)))
    capital_scale = 1.0
    cash_pct = 0.0
    dd_breaker_dd = 0.0
    dd_breaker_extra = 0
    # 市场仪表盘按「信号日 as-of」计算（因果安全）：旧版把「今天」的波动率分位/regime
    # 套用到窗口内全部历史信号日——既引入未来函数，又使同一信号日每天重跑结果漂移。
    # compute_market_profile 对指数全量序列做进程内缓存，逐信号日切片开销可忽略；
    # 出场参数自适应（compute_adaptive_exit_params）同样只用该 as-of profile。
    # market_profile 形参保留仅为兼容旧调用方签名，不再使用（忽略传入值）。
    _prof = None
    try:
        _prof = compute_market_profile(as_of=sd)
    except Exception:
        _prof = None
    if _prof is not None:
            # 总仓位随市场波动率缩放（高波动留现金，真正降低组合暴露）：
            # 自适应 = S 型波动率分位现金 + 趋势 regime 调整（替代硬编码 VOL_POS_SCALE_MAP）
            if _vol_pos_on:
                cash_pct = cash_from_volatility(getattr(_prof, "volatility_pctile", None))
                cash_pct = cash_from_regime(getattr(_prof, "regime", None), cash_pct)
                capital_scale = max(0.0, 1.0 - cash_pct / 100.0)
            # 逐只波动率自适应止损：无 vol 数据回退引擎全局 -8%
            if _vol_stop_on:
                _stops = []
                for code in sub["代码"]:
                    v = _lv(str(code).strip()).get("vol20")
                    if v and v > 0:
                        _stops.append(max(0.06, min(_vol_mult * v, 0.15)))
                    else:
                        _stops.append(None)
                sub["stop_pct"] = _stops
                print(f"  [波动率自适应] 市场波动={_prof.volatility_level} 总仓位缩放={capital_scale} "
                      f"逐只止损: {sum(1 for s in _stops if s)}/{len(_stops)} 只已定")

    # 组合级回撤熔断（独立于波动率维度）：跨信号日重建组合权益曲线，
    # 回撤超阈值则追加现金比例降低暴露。仅「追加」、绝不减少基线现金；
    # 最坏情况为过度防御（多持现金），不会放大风险。
    if portfolio_curve is not None:
        _dd = portfolio_curve.drawdown_as_of(sd)
        _extra = cash_from_drawdown(_dd, threshold=dd_thr, cap=dd_cap, deep=dd_deep)
        if _extra > 0:
            cash_pct = min(100, cash_pct + _extra)
            capital_scale = max(0.0, 1.0 - cash_pct / 100.0)
        dd_breaker_dd = _dd
        dd_breaker_extra = _extra

    strategies = derive_strategies(df["来源策略"]) if "来源策略" in df.columns else "boll,relativity,theme"

    size_by = os.environ.get("BACKTEST_SIZE_BY", "权重" if "权重" in df.columns else "综合评分") or None

    _exit = compute_adaptive_exit_params(_prof, regime=getattr(_prof, "regime", None))
    result = run_forward_signal_backtest(
        sub,
        hold_days=hold_days,
        initial_capital=100000.0,
        max_positions=200,
        # 出场逻辑（策略感知路由，2026-08-02 修订）：
        #  - 通用：A股 T+1 → 买入当日不可卖出（杜绝“当天买当天卖”的假止盈亏损）。
        #  - 止盈=Boll上轨(take_band)：仅当上轨真正高于入场价才生效；动量/趋势票入场常高于
        #    上轨（上轨在成本下方），命中即亏损，必须剔除，否则出现“负收益却标上轨止盈”矛盾。
        #  - 趋势/动量(theme/momentum/cctv)：固定+6% 止盈(take_pct) + 移动止盈5%(trailing)
        #    + 收盘跌破 MA60 趋势破位(trend_exit_ma=60)。
        #  - 均值回归(boll/relativity)：上轨止盈 + 固定/自适应止损 + 持有期满；**不启用 MA60 破位**
        #    （relativity 实测因 MA60 破位恶化 -9.57%→-13.86%）。
        # 全样本BASELINE实测 -6.37%→-5.09%(+1.28pct)，回撤 -7.81%→-6.69% 收窄。
        # 波动率自适应：stop_loss_pct 为全局兜底(-8%)，逐只 stop_pct 列（个股 vol20 定）优先。
        # 全局兜底也改由 compute_adaptive_exit_params 自适应（波动率/regime 浮动）。
        enable_exits=True,
        use_signal_bands=True,
        stop_loss_pct=_exit["stop_loss_pct"],
        take_profit_pct=_exit["take_profit_pct"],
        trailing_stop_pct=_exit["trailing_stop_pct"],
        trend_exit_ma=_exit["trend_exit_ma"],
        size_by=size_by,
        capital_scale=capital_scale,
    )
    if result.summary.get("error"):
        return None

    date_tag = sd.strftime("%Y%m%d")
    base = STOCK_DATA_DIR / f"Multi-Backtest-{date_tag}"

    summary = dict(result.summary)
    summary.pop("data_coverage", None)
    summary["date"] = date_tag
    summary["run_date"] = date.today().strftime("%Y%m%d")
    summary["signal_start"] = sd.strftime("%Y-%m-%d")
    summary["signal_end"] = sd.strftime("%Y-%m-%d")
    summary["hold_days"] = hold_days
    summary["exit_mode"] = "boll_upper_take+take6%+trailing5%+MA60break"
    summary["size_mode"] = f"conviction({size_by})" if size_by else "equal"
    if _vol_stop_on or _vol_pos_on:
        summary["vol_mode"] = f"scaled_stop+pos{capital_scale}"
        if dd_breaker_extra > 0:
            summary["vol_mode"] += f"+ddbreak{int(dd_breaker_extra)}"
    else:
        summary["vol_mode"] = "fixed"
    summary["dd_breaker_dd"] = round(dd_breaker_dd, 2)
    summary["dd_breaker_extra_cash"] = int(dd_breaker_extra)
    summary["capital_scale"] = round(capital_scale, 2)
    summary["cash_pct"] = round(cash_pct, 1)
    summary["size_mode"] = f"adaptive_weight({size_by})" if size_by else "equal"
    summary["signals_days"] = 1
    summary["codes_count"] = len(sub)
    summary["strategies"] = strategies
    summary["start"] = sd.strftime("%Y-%m-%d")
    summary["end"] = (sd + timedelta(days=hold_days)).strftime("%Y-%m-%d")

    pd.DataFrame([summary]).to_csv(f"{base}-summary.csv", index=False, encoding="utf-8-sig")

    # 补算回撤列（引擎返回的 equity 仅含 date/cash/holding_value/total），供前端回撤图展示
    equity = result.equity.copy()
    equity["peak"] = equity["total"].cummax()
    equity["drawdown"] = (equity["total"] - equity["peak"]) / equity["peak"] * 100
    equity.to_csv(f"{base}-equity.csv", index=False, encoding="utf-8-sig")

    result.trades.to_csv(f"{base}-trades.csv", index=False, encoding="utf-8-sig")

    return {
        "date_tag": date_tag,
        "summary": summary,
        "num_trades": int(summary.get("num_trades", 0)),
    }


def _write_status(lists, generated, skipped, incomplete_skipped=0):
    """把回测运行状态写到 stock_data/backtest_status.txt，便于在仓库/看板确认回测真的跑了
    （continue-on-error 不会再静默吞掉失败）。"""
    try:
        lines = [
            f"run_date={date.today().strftime('%Y%m%d')}",
            f"eligible_signal_days={len(lists)}",
            f"generated={len(generated)}",
            f"skipped={skipped}",
            f"incomplete_skipped={incomplete_skipped}",
        ]
        if generated:
            lines.append(f"range={generated[0]['date_tag']}~{generated[-1]['date_tag']}")
        else:
            lines.append("range=NONE")
        (STOCK_DATA_DIR / "backtest_status.txt").write_text("\n".join(lines), encoding="utf-8")
    except Exception:
        pass


def _collect_all_candidate_codes(lists: list[tuple[Path, date]], limit_per_day: int = FILTER_PRE_TOP_N) -> set[str]:
    """从所有信号日清单中收集候选代码集合（每天最多取 limit_per_day 只，按评分截断）。"""
    codes: set[str] = set()
    for path, sd in lists:
        try:
            df = pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            continue
        if df.empty or "股票代码" not in df.columns:
            continue
        # 前导零归一
        df = df.copy()
        df["股票代码"] = df["股票代码"].apply(format_stock_code)
        df = df[df["股票代码"].str.len() >= 6]
        # 按评分粗取（与 _backtest_one 的预筛选逻辑一致）
        if "综合评分" in df.columns and len(df) > limit_per_day:
            df["_pre_s"] = pd.to_numeric(df["综合评分"], errors="coerce")
            df = df.sort_values("_pre_s", ascending=False).head(limit_per_day)
        for c in df["股票代码"].dropna().unique():
            fc = format_stock_code(c)
            if fc and len(fc) >= 6:
                codes.add(fc)
    return codes


def _skip_completed(
    lists: list[tuple[Path, date]], hold_days: int, today: date
) -> tuple[list[tuple[Path, date]], int]:
    """跳过已有完整回测结果的信号日，避免重复计算。

    判定标准：Multi-Backtest-{date}-summary.csv 已存在 且 信号日+持有期 ≤ 今天（窗口走完）。
    未走完的近期信号（实际持有期 < hold_days）始终保留，以便滚动更新。
    """
    remaining: list[tuple[Path, date]] = []
    skipped_count = 0
    for path, sd in lists:
        days_since = (today - sd).days
        if days_since >= hold_days:
            # 窗口已走完 → 检查是否已存在结果
            tag = sd.strftime("%Y%m%d")
            summary_path = STOCK_DATA_DIR / f"Multi-Backtest-{tag}-summary.csv"
            if summary_path.exists():
                skipped_count += 1
                continue
        remaining.append((path, sd))
    return remaining, skipped_count


def _count_active_strategies(path: Path) -> tuple[int, set[str]]:
    """读 Daily-Action-List 的「来源策略」列，去重统计有效策略数及标识集合。"""
    try:
        df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
    except Exception:
        return 0, set()
    col = next((c for c in df.columns if "来源策略" in c or c == "策略"), None)
    if col is None or df.empty:
        return 0, set()
    tags: set[str] = set()
    for v in df[col].dropna():
        for s in str(v).split("/"):
            s = s.strip().lower()
            if s:
                tags.add(s)
    return len(tags), tags


def _filter_incomplete(
    lists: list[tuple[Path, date]], min_strategies: int
) -> tuple[list[tuple[Path, date]], int]:
    """跳过有效策略数 < min_strategies 的信号日，保证回测在统一基准上。

    某些信号日的清单只融合了部分策略（如某天 Theme/CCTV step 失败被
    continue-on-error 吞掉），用这种「残缺清单」回测会污染统计
    （组合偏小、与齐全日不可比）。
    """
    remaining: list[tuple[Path, date]] = []
    skipped = 0
    for path, sd in lists:
        n, tags = _count_active_strategies(path)
        if n < min_strategies:
            skipped += 1
            print(f"  [筛除] {sd} 有效策略={n} ({sorted(tags) if tags else '空'}) "
                  f"< {min_strategies}，跳过", flush=True)
            continue
        remaining.append((path, sd))
    return remaining, skipped


def main() -> int:
    hold_days = int(os.environ.get("HOLD_DAYS", "10"))
    lookback_days = int(os.environ.get("LOOKBACK_DAYS", "14"))  # 默认从30降到14（海外慢）
    today = date.today()
    t0 = time.time()

    # ── 阶段 0：进程内 K 线缓存（跨信号日去重）──
    _orig_fetch = kline_mod.fetch_daily_k
    _kcache: dict = {}

    def _cached_fetch(code, start, end, *a, **k):
        key = (str(code), str(start), str(end), k.get("adjust", "qfq"))
        if key not in _kcache:
            _kcache[key] = _orig_fetch(code, start, end, *a, **k)
        return _kcache[key]

    kline_mod.fetch_daily_k = _cached_fetch

    # 市场仪表盘不再在 main() 预计算：旧版此处用「今天」的 profile 统一传给所有
    # 历史信号日（未来函数）。现改为 _backtest_one 内部按信号日 as_of 各自计算。

    # ── 阶段 0.6：重建组合权益曲线（回撤熔断用）──
    # 从磁盘所有 Multi-Backtest-*-equity.csv 按日历日叠加出组合滚动回撤；
    # 回测循环中每生成一份新的 equity 就 fold 进去，使后续信号日能看到更新后的组合回撤。
    # 仅读盘，不改写任何数据；DRAWDOWN_BREAKER=0 可整体关闭。
    _dd_breaker_on = os.environ.get("DRAWDOWN_BREAKER", "1") == "1"
    _dd_thr = float(os.environ.get("DD_BREAKER_THRESHOLD", "8"))
    _dd_cap = float(os.environ.get("DD_BREAKER_CAP", "50"))
    _dd_deep = float(os.environ.get("DD_BREAKER_DEEP", "20"))
    portfolio_curve = _PortfolioCurve()
    if _dd_breaker_on:
        for _eq in STOCK_DATA_DIR.glob("Multi-Backtest-*-equity.csv"):
            _eq_tag = _parse_signal_date(_eq.name)
            if _eq_tag is None:
                continue
            portfolio_curve.add_sleeve(_eq.name, _load_equity_series(_eq))
        print(f"[回撤熔断] 已重建组合权益曲线，载入 {len(portfolio_curve._sleeves)} 个历史 sleeve "
              f"(阈值={_dd_thr}%, 封顶={_dd_cap}%, 深崩={_dd_deep}%)")

    lists = collect_eligible_lists(lookback_days)
    if not lists:
        print(f"未找到近 {lookback_days} 天的 Daily-Action-List CSV，跳过每日回测")
        return 0

    # ── 阶段 0.5：跳过已有完整结果的信号日 ──
    lists, already_done = _skip_completed(lists, hold_days, today)

    # ── 阶段 0.7：策略完整性筛选（统一回测基准）──
    # 某些信号日清单只融合了部分策略（如某天 Theme/CCTV step 失败被吞），
    # 用残缺清单回测会污染统计（组合偏小、与齐全日不可比）。
    # 跳过有效策略数 < BACKTEST_MIN_STRATEGIES 的信号日。
    _min_strat = int(os.environ.get("BACKTEST_MIN_STRATEGIES", "2"))
    lists, incomplete_skipped = _filter_incomplete(lists, _min_strat)
    print(f"[回测] 策略完整性筛选：有效策略 < {_min_strat} 的信号日跳过 {incomplete_skipped} 个")

    if not lists:
        print(f"[回测] 全部 {already_done} 个已完成 + {incomplete_skipped} 个策略不全，"
              f"无有效信号日，跳过回测")
        _write_status([], [], already_done, incomplete_skipped)
        return 0

    print(f"[回测] 找到 {len(lists)} 个待回测信号日 "
          f"(已完成跳过 {already_done} + 残缺跳过 {incomplete_skipped}) "
          f"({lists[0][1]} ~ {lists[-1][1]}), HOLD={hold_days}d")

    # 阶段 1：串行预拉全量 K 线（性能核心优化）
    # 用户要求：不用多线程，避免把 akshare 接口打崩。
    # 串行虽慢，但配合「跳过已完成」+「缩短窗口」后，日常增量只需预拉 1~3 个
    # 新信号日的标的（几十只），串行几分钟即可；冷启动（窗口内全量）由
    # PREPULL_INTERVAL 控制节奏，温和对待上游接口。
    all_codes = _collect_all_candidate_codes(lists)
    if all_codes:
        global_start = min(sd for _, sd in lists) - timedelta(days=120)  # boll 窗口前推
        global_end = today
        sorted_codes = sorted(all_codes)
        n_codes = len(sorted_codes)

        # 串行预拉时每两只之间的间隔（秒）：给上游接口喘息时间，默认 0.3s。
        # 设 0 可关闭间隔（最快，但风险高），日常 CI 建议保留默认值。
        _interval = float(os.environ.get("PREPULL_INTERVAL", "0.3"))
        print(f"[预拉K线] 共 {n_codes} 只标的，范围 {global_start} ~ {global_end}, "
              f"串行(间隔 {_interval}s)")
        t_pre = time.time()

        ok_cnt = 0

        for i, code in enumerate(sorted_codes):
            try:
                df = kline_mod.fetch_daily_k(code, global_start, global_end, adjust="qfq")
                if df is not None and not df.empty:
                    ok_cnt += 1
            except Exception:
                pass
            # 进度输出（带 ETA）
            if (i + 1) % 10 == 0 or (i + 1) == n_codes or i == 0:
                pct = (i + 1) / n_codes * 100
                elapsed = time.time() - t_pre
                eta = (elapsed / (i + 1)) * (n_codes - i - 1) if i + 1 > 0 else 0
                print(f"  [预拉 {i+1}/{n_codes}] ({pct:.0f}%) {code} "
                      f"已用 {elapsed:.0f}s 预计剩余 {eta:.0f}s", flush=True)
            # 温和间隔，保护上游接口不被打崩
            if _interval > 0 and i + 1 < n_codes:
                time.sleep(_interval)

        t_pre_done = time.time() - t_pre
        rate = n_codes / max(t_pre_done, 0.001)
        print(f"[预拉K线] 完成: {ok_cnt}/{n_codes} 只成功, "
              f"耗时 {t_pre_done:.0f}s ({rate:.1f}/s)")

    # ── 阶段 2：逐信号日回测（K 线已全部在缓存中）──
    generated: list[dict] = []
    skipped = 0
    total_signals = len(lists)
    for idx, (path, sd) in enumerate(lists):
        days_since = (today - sd).days
        actual_hold = min(hold_days, days_since)
        if actual_hold < 1:
            continue
        print(f"\n[回测] ({idx+1}/{total_signals}) 信号日 {sd} · 距今天 {days_since}d · 实际持有 {actual_hold}d",
              flush=True)
        try:
            res = _backtest_one(
                path, sd, actual_hold,
                portfolio_curve=portfolio_curve, dd_thr=_dd_thr, dd_cap=_dd_cap, dd_deep=_dd_deep,
            )
        except Exception as e:
            skipped += 1
            print(f"  → 回测异常跳过：{e}", flush=True)
            continue
        if res is None:
            skipped += 1
            print("  → 无有效结果（信号为空或 K 线拉取失败）", flush=True)
            continue
        generated.append(res)
        # 回撤熔断：把本次新生成的 equity 折回组合曲线，供后续信号日读取最新组合回撤
        if _dd_breaker_on:
            _eq = STOCK_DATA_DIR / f"Multi-Backtest-{res['date_tag']}-equity.csv"
            portfolio_curve.add_sleeve(f"Multi-Backtest-{res['date_tag']}-equity.csv", _load_equity_series(_eq))
        print(f"  → 总收益 {res['summary'].get('total_return')}%，"
              f"回撤 {res['summary'].get('max_drawdown')}%，{res['num_trades']} 笔", flush=True)

    total_skipped = skipped + already_done
    _write_status(lists, generated, total_skipped, incomplete_skipped)

    total_elapsed = time.time() - t0
    if not generated:
        print("没有生成新的每日回测结果（可能历史清单为空或 K 线全部拉取失败）")
        return 0

    print(f"\n{'='*60}")
    print(f"完成：共生成 {len(generated)} 份前向信号回测，信号日范围 "
          f"{generated[0]['date_tag']} ~ {generated[-1]['date_tag']} "
          f"(已完成{already_done} + 跳过{skipped})")
    print(f"总耗时: {total_elapsed:.0f}s ({total_elapsed/60:.1f}min)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
