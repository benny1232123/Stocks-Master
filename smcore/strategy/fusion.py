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
"""
from __future__ import annotations

import csv
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from smcore.config.defaults import DEFAULT_K, DEFAULT_WINDOW, STOCK_DATA_DIR
from smcore.data import fetch_daily_k
from smcore.indicators import calc_bollinger
from smcore.strategy import build_strategy_allocation
from smcore.strategy.market import compute_market_profile
from smcore.strategy import sectors as sector_mod
from smcore.utils.code import format_stock_code
from smcore.utils.format import fmt_num, to_float

# ── 股票名称兜底映射（当所有策略 CSV 都缺 股票名称 时使用）─────────────
_stock_name_cache: Optional[dict] = None
# baostock 登录态复用
_bs_name_logged_in = False


def _get_stock_name_map() -> dict:
    """返回 {code(6位): name} 映射，优先从 stock_info_a_code_name.csv 缓存读取。"""
    global _stock_name_cache
    if _stock_name_cache is not None:
        return _stock_name_cache
    _stock_name_cache = {}
    try:
        p = STOCK_DATA_DIR / "stock_info_a_code_name.csv"
        if p.exists():
            df = pd.read_csv(p, encoding="utf-8-sig", dtype=str)
            code_col = next((c for c in df.columns if c in ("code", "代码", "股票代码")), None)
            name_col = next((c for c in df.columns if c in ("name", "名称", "股票名称")), None)
            if code_col and name_col:
                for _, r in df.iterrows():
                    c = format_stock_code(str(r[code_col]).strip())
                    n = str(r[name_col]).strip()
                    if c and n and c not in _stock_name_cache:
                        _stock_name_cache[c] = n
    except Exception:
        pass
    return _stock_name_cache


def lookup_stock_name(code: str) -> str:
    """查询单只股票名称（缓存 → CSV → baostock 三级兜底），找不到返回空串。"""
    c6 = format_stock_code(code)
    if not c6:
        return ""
    # 1) 已有缓存
    m = _get_stock_name_map()
    if c6 in m:
        return m[c6]
    # 2) baostock 实时查（仅一次登录，~0.3s/只）
    global _bs_name_logged_in
    try:
        import baostock as bs
        bs_code = f"sh.{c6}" if c6[0] == "6" else f"sz.{c6}"
        if not _bs_name_logged_in:
            lg = bs.login()
            if getattr(lg, "error_code", "1") != "0":
                return ""
            _bs_name_logged_in = True
        rs = bs.query_stock_basic(code=bs_code, code_name="")
        found = ""
        while rs.next():
            row = rs.get_row_data()
            if len(row) >= 2 and row[1]:
                found = str(row[1]).strip()
                break
        if found:
            _stock_name_cache[c6] = found  # 写回缓存
        return found
    except Exception:
        return ""

# 各策略在综合评分中的权重（命中该策略即得基础分，多策略叠加加分）
# 根据 _detect_market_regime() 判定的市场状态动态选取，不再硬编码一套走天下。
#
# 设计思路（与 allocation.py 仓位分配联动）：
#   - 趋势上行：顺势为王 → 动量提权（强势股延续性高）、Boll 降权（超卖机会少）
#   - 下行防御：防守优先 → Boll 提权（仅存均值回归窗口，但趋势闸门会先砍纯 boll 票）、
#                   动量保留高分（仅让真强势票通过）、题材/CCTV 降权（弱市易一日游）
#   - 震荡轮动：均衡配置，沿用此前实测 edge 定稿的默认值
#
# 默认权重依据 2026-07-11 实测前向10日 edge（窗口 0606-0626，硬止损+真实成本）。
_REGIME_STRATEGY_SCORE: dict[str, dict[str, int]] = {
    "趋势上行": {
        "boll": 32,       # 牛市超卖少，均值回归降为辅
        "momentum": 38,   # 顺势为王，强势股延续性高
        "theme": 12,      # 跟风题材有溢价但不过度追
        "relativity": 10, # 牛市里抗跌属性价值有限
        "cctv": 8,        # 叙事驱动有一定作用
    },
    "下行防御": {
        "boll": 40,       # 均值回归唯一窗口（但趋势闸门会先剔除纯 boll 票）
        "momentum": 28,   # 仅留真正强势的票（RS 过滤兜底）
        "relativity": 17, # 抗跌属性在弱市有价值
        "theme": 8,       # 弱市题材容易一日游，严控
        "cctv": 7,        # 舆情驱动的票波动大
    },
    # 震荡轮动 / 兜底默认（与旧 STRATEGY_BASE_SCORE 一致，经实测验证）
    "_default": {
        "boll": 45,       # 主策略（超卖均值回归），实测最抗跌，提权
        "relativity": 15,  # 相对强弱实测最差，砍权
        "momentum": 20,    # 动量/相对强度，适度（待实测验证）
        "theme": 15,       # 题材，谨慎
        "cctv": 10,        # CCTV 舆情噪声大，最低
    },
}

# 向后兼容：保留 STRATEGY_BASE_SCORE 作为 _default regime 的别名（外部引用仍可访问）
STRATEGY_BASE_SCORE = dict(_REGIME_STRATEGY_SCORE["_default"])


def get_regime_scores(regime: str) -> dict[str, int]:
    """根据市场状态返回对应的策略评分权重。"""
    return dict(_REGIME_STRATEGY_SCORE.get(regime, _REGIME_STRATEGY_SCORE["_default"]))


def _dynamic_thresholds(regime: str) -> tuple[float, float]:
    """根据市场状态浮动 RS 容忍度与流动性门槛。

    - 趋势上行：放宽（RS_TOL=0.05 / ¥7000万），让更多顺势票过、不误杀强势股
    - 下行防御：收紧（RS_TOL=0.02 / ¥2亿），只留最强最流动的票控回撤
    - 其他：用全局默认（RS_TOL=0.03 / ¥1亿）
    """
    if regime == "趋势上行":
        return 0.05, 7e7
    if regime == "下行防御":
        return 0.02, 2e8
    return RS_TOL, MIN_SIGNAL_AMOUNT
MULTI_HIT_BONUS = 5  # 每多命中一个策略额外 +5

# 趋势守卫：价格低于 MA20 超过该比例，视作破位/下降通道自由落体股，剔除。
# 阈值设为 12%——保留 Boll 轻度超卖票（近下轨通常仅低于 MA20 几个百分点），
# 但剔除明显破位（如单日 -20%+ 的崩盘股），从信号层防尾部巨亏。
TREND_GUARD_BELOW_MA20 = 0.12

# 相对大盘强度过滤（针对根因：大盘涨、选出的超卖票仍跑输）。
# 候选近 RS_LOOKBACK 日收益若跑输沪深300 同期收益超 RS_TOL，则剔除（除非该票本身是动量票）。
# 动量票已要求 ret20>0 且 MA20 上行，豁免本过滤以免误杀强势股。
RS_LOOKBACK = 20
RS_TOL = 0.03
# 流动性门槛：信号日成交额（元）低于此值的票直接剔除。
# 头对头测量（measure_signal_quality.py，RS 宇宙样本）：¥1e8 门槛相对基线
# 平均收益 +0.92%、胜率 +5.1%、盈亏比 +0.43，为最优甜点（¥5e7 反而更弱）。
# 剔除流动性差的票可避免难出场/庄股陷阱导致的隐性亏损。
# 注意：amount 单位随数据源（akshare/baostock 均为元）；取值为 None 时放行（后端故障不误杀）。
MIN_SIGNAL_AMOUNT = 1e8
RS_APPLY_TO_MOMENTUM = False


def _passes_trend_guard(price, ma20) -> bool:
    """趋势守卫：价格远低于 MA20 则剔除（破位/下降通道）；数据缺失则保守保留。"""
    if price is None or ma20 is None:
        return True
    try:
        price = float(price)
        ma20 = float(ma20)
    except (TypeError, ValueError):
        return True
    if ma20 <= 0 or price <= 0:
        return True
    return price >= ma20 * (1 - TREND_GUARD_BELOW_MA20)


def _detect_market_regime() -> str:
    """判断市场状态（向后兼容包装，委托给多维市场仪表盘）。

    返回 "趋势上行" / "下行防御" / "震荡轮动"。现由 `compute_market_profile` 综合
    趋势/波动率/宽度/量能四维度合成，比单一 MA60 更准。
    """
    try:
        return compute_market_profile().regime
    except Exception:
        return "震荡轮动"


_HS300_CLOSE_CACHE: Optional[pd.Series] = None


def _fetch_hs300_baostock() -> Optional[pd.Series]:
    """baostock 拉沪深300 收盘价（沙箱/本地最稳，已验证可达）。失败返回 None。"""
    try:
        import baostock as bs
        from smcore.data.session import session

        with session() as ok:
            if not ok:
                return None
            end = date.today().strftime("%Y-%m-%d")
            rs = bs.query_history_k_data_plus(
                "sh.000300", "date,close",
                start_date="2020-01-01", end_date=end,
                frequency="d", adjustflag="2",
            )
            if rs.error_code != "0":
                # 指数不支持前复权，退而不复权重试
                rs = bs.query_history_k_data_plus(
                    "sh.000300", "date,close",
                    start_date="2020-01-01", end_date=end,
                    frequency="d", adjustflag="3",
                )
                if rs.error_code != "0":
                    return None
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())
            if not rows:
                return None
            df = pd.DataFrame(rows, columns=rs.fields)
            close = pd.to_numeric(df["close"], errors="coerce")
            dts = pd.to_datetime(df["date"], errors="coerce")
            s = pd.Series(close.values.astype(float), index=dts)
            s = s[~s.index.isna()].sort_index()
            return s if len(s) >= 22 else None
    except Exception:
        return None


def _fetch_hs300_akshare() -> Optional[pd.Series]:
    """akshare 拉沪深300 收盘价（云端无 baostock 时兜底）。失败返回 None。"""
    try:
        import akshare as ak
        from smcore.data.kline import _call_with_timeout

        df = _call_with_timeout(lambda: ak.stock_zh_index_daily(symbol="sh000300"), 30)
        if df is None or len(df) < 22:
            return None
        close = pd.to_numeric(df["close"], errors="coerce")
        dts = pd.to_datetime(df["date"], errors="coerce")
        s = pd.Series(close.values.astype(float), index=dts)
        s = s[~s.index.isna()].sort_index()
        return s if len(s) >= 22 else None
    except Exception:
        return None


def _get_hs300_close() -> Optional[pd.Series]:
    """缓存沪深300 收盘价序列（baostock 主源 + akshare 兜底，东财-free）。

    此前仅走 akshare，沙箱/云端指数接口偶发失败会返回 None，
    导致 RS 过滤「数据缺失一律放行」而形同虚设。改为 baostock 主源后
    沙箱稳定可取，云端退 akshare，保证相对强度过滤真正生效。
    """
    global _HS300_CLOSE_CACHE
    if _HS300_CLOSE_CACHE is not None:
        return _HS300_CLOSE_CACHE
    s = _fetch_hs300_baostock()
    if s is None:
        s = _fetch_hs300_akshare()
    if s is None or len(s) < 22:
        return None
    _HS300_CLOSE_CACHE = s
    return s


def _index_20d_return(as_of_yyyymmdd: str) -> Optional[float]:
    """沪深300 在 as_of_yyyymmdd 当日相对其前 RS_LOOKBACK 日的收益率。失败返回 None。"""
    s = _get_hs300_close()
    if s is None:
        return None
    try:
        target = pd.Timestamp(as_of_yyyymmdd)
    except Exception:
        return None
    prior = s.loc[:target]
    if len(prior) < RS_LOOKBACK + 1:
        return None
    price_now = prior.values[-1]
    price_prev = prior.values[-(RS_LOOKBACK + 1)]
    if price_prev == 0:
        return None
    return price_now / price_prev - 1


def _passes_relative_strength_filter(
    hit_strategies: list[str],
    stock_ret: Optional[float],
    index_ret: Optional[float],
    tol: float = RS_TOL,
    apply_to_momentum: bool = RS_APPLY_TO_MOMENTUM,
) -> bool:
    """相对大盘强度过滤：跑输大盘超 tol 的候选剔除（动量票豁免）。

    根因：0606-0626 窗口沪深300 其实在上行，但选出的超卖票仍亏 → 策略 alpha 弱。
    直接剔除「大盘涨、个股仍明显跑输」的票，是比趋势闸门更对症的修复。
    """
    if "Momentum" in hit_strategies and not apply_to_momentum:
        return True
    if stock_ret is None or index_ret is None:
        return True  # 数据缺失，保守保留
    return stock_ret >= index_ret - tol - 1e-9


def _extract_date_from_filename(path: Path) -> Optional[str]:
    """从文件名末尾提取 YYYYMMDD，例如 Stock-Selection-Boll-20260704.csv。"""
    suffix = path.stem.rsplit("-", 1)[-1]
    if len(suffix) == 8 and suffix.isdigit():
        return suffix
    return None


def _find_strategy_csv(
    pattern: str,
    date_yyyymmdd: str,
    *,
    max_stale_days: int = 3,
) -> Optional[tuple[Path, str]]:
    """按日期找策略结果 CSV，仅在 max_stale_days 内回退到最近一期。

    此前各策略独立回退到“各自最新文件”，可能把不同日期的信号混在一起，
    导致操作清单基于过期/错日数据。现在统一限制回退窗口并返回实际日期。
    """
    preferred = STOCK_DATA_DIR / f"{pattern}-{date_yyyymmdd}.csv"
    if preferred.exists():
        return preferred, date_yyyymmdd

    requested = datetime.strptime(date_yyyymmdd, "%Y%m%d").date()
    best: Optional[tuple[Path, str]] = None

    def _consider(paths: list[Path]) -> None:
        nonlocal best
        for path in paths:
            actual_date = _extract_date_from_filename(path)
            if not actual_date:
                continue
            actual = datetime.strptime(actual_date, "%Y%m%d").date()
            stale_days = (requested - actual).days
            if stale_days < 0 or stale_days > max_stale_days:
                continue
            if best is None or actual_date > best[1]:
                best = (path, actual_date)

    _consider(sorted(STOCK_DATA_DIR.glob(f"{pattern}-*.csv"), key=lambda p: p.name, reverse=True))

    archive = STOCK_DATA_DIR / "archive"
    if archive.exists():
        _consider(sorted(archive.rglob(f"{pattern}-*.csv"), key=lambda p: p.name, reverse=True))

    return best


def _load_boll_picks(date_yyyymmdd: str, *, max_stale_days: int = 3) -> tuple[dict, Optional[str]]:
    """读取 Boll 选股结果，返回 ({code: {...}}, 实际数据日期)。"""
    found = _find_strategy_csv("Stock-Selection-Boll", date_yyyymmdd, max_stale_days=max_stale_days)
    if not found:
        return {}, None
    path, actual_date = found
    picks = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            code = format_stock_code(row.get("股票代码", ""))
            if not code:
                continue
            picks[code] = {
                "name": (row.get("股票名称") or "").strip(),
                "buy_price": to_float(row.get("建议买入价")),
            }
    return picks, actual_date


def _load_relativity_picks(date_yyyymmdd: str, *, max_stale_days: int = 3) -> tuple[dict, Optional[str]]:
    """读取相对强弱结果，返回 ({code: {...}}, 实际数据日期)。"""
    found = _find_strategy_csv("Stock-Selection-Relativity", date_yyyymmdd, max_stale_days=max_stale_days)
    if not found:
        return {}, None
    path, actual_date = found
    picks = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            code = format_stock_code(row.get("股票代码", ""))
            if not code:
                continue
            picks[code] = {
                "name": (row.get("股票名称") or "").strip(),
                "up_ratio": to_float(row.get("上涨满足率")),
                "down_ratio": to_float(row.get("抗跌满足率")),
            }
    return picks, actual_date


def _load_theme_picks(date_yyyymmdd: str, *, max_stale_days: int = 3) -> tuple[dict, Optional[str]]:
    """读取题材策略结果，返回 ({code: {...}}, 实际数据日期)。"""
    found = _find_strategy_csv("Stock-Selection-Ashare-Theme-Turnover", date_yyyymmdd, max_stale_days=max_stale_days)
    if not found:
        return {}, None
    path, actual_date = found
    picks = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            code = format_stock_code(row.get("股票代码", ""))
            if not code:
                continue
            picks[code] = {
                "name": (row.get("股票名称") or "").strip(),
                "score": to_float(row.get("综合分")),
                "theme": (row.get("题材标签") or "").strip(),
            }
    return picks, actual_date


def _load_cctv_picks(date_yyyymmdd: str, *, max_stale_days: int = 3) -> tuple[dict, Optional[str]]:
    """读取 CCTV 股票池，返回 ({code: {...}}, 实际数据日期)。"""
    found = _find_strategy_csv("CCTV-Sector-Stock-Pool", date_yyyymmdd, max_stale_days=max_stale_days)
    if not found:
        return {}, None
    path, actual_date = found
    picks = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            code = format_stock_code(row.get("股票代码", ""))
            if not code:
                continue
            picks[code] = {
                "name": (row.get("股票名称") or "").strip(),
                "sector": (row.get("板块") or "").strip(),
                "heat": to_float(row.get("热度分")),
            }
    return picks, actual_date


def _load_momentum_picks(date_yyyymmdd: str, *, max_stale_days: int = 3) -> tuple[dict, Optional[str]]:
    """读取动量策略结果，返回 ({code: {...}}, 实际数据日期)。缺失则空（fail-soft）。"""
    found = _find_strategy_csv("Stock-Selection-Momentum", date_yyyymmdd, max_stale_days=max_stale_days)
    if not found:
        return {}, None
    path, actual_date = found
    picks = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            code = format_stock_code(row.get("股票代码", ""))
            if not code:
                continue
            picks[code] = {
                "name": (row.get("股票名称") or "").strip(),
                "momentum": to_float(row.get("动量分")),
            }
    return picks, actual_date


def _compute_boll_levels(code: str, as_of_date: Optional[str] = None) -> dict:
    """拉前复权 K 线算 Boll 水位（止损=下轨，止盈=上轨）+ 近 RS_LOOKBACK 日收益率。

    as_of_date: 指定截止日期 YYYYMMDD（默认今天）。用于历史回测/测量时点对齐。
    """
    if as_of_date:
        try:
            end = datetime.strptime(as_of_date, "%Y%m%d").date()
        except (ValueError, TypeError):
            end = date.today()
    else:
        end = date.today()
    start = end - timedelta(days=120)  # 120 天前
    df = fetch_daily_k(code, start, end, adjust="qfq")
    if len(df) < DEFAULT_WINDOW:
        return {}
    boll = calc_bollinger(df, window=DEFAULT_WINDOW, k=DEFAULT_K)
    last = boll.iloc[-1]
    close = pd.to_numeric(df["close"], errors="coerce").dropna()
    ret20 = None
    if len(close) >= RS_LOOKBACK + 1:
        prev = close.iloc[-(RS_LOOKBACK + 1)]
        if prev and not pd.isna(prev):
            ret20 = float(close.iloc[-1]) / float(prev) - 1
    # 信号日成交额（元），用于流动性门槛过滤
    amount = None
    if "amount" in df.columns:
        amt_series = pd.to_numeric(df["amount"], errors="coerce").dropna()
        if len(amt_series) > 0:
            amount = float(amt_series.iloc[-1])
    # 个股近 20 日波动率（日收益 std），供波动率自适应止损使用
    vol20 = None
    if len(close) >= 21:
        dret = close.iloc[-20:].pct_change().dropna()
        if len(dret) >= 5:
            vol20 = float(dret.std())
    return {
        "close": float(last["close"]),
        "lower": float(last["Lower"]) if pd.notna(last.get("Lower")) else None,
        "upper": float(last["Upper"]) if pd.notna(last.get("Upper")) else None,
        "ma20": float(last["MA"]) if pd.notna(last.get("MA")) else None,
        "ret20": ret20,
        "amount": amount,
        "vol20": vol20,
    }


def fuse_signals(
    date_yyyymmdd: str,
    *,
    total_capital: float = 100000.0,
    max_picks: int = 15,
    fetch_levels: bool = True,
    trend_guard: bool = True,
    market_gate: bool = True,
    relative_strength_filter: bool = True,
    min_signal_amount: float = MIN_SIGNAL_AMOUNT,
    dynamic_thresholds: bool = True,
    sector_cap: bool = True,
    max_per_sector: int = sector_mod.DEFAULT_MAX_PER_SECTOR,
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
    # 策略评分权重（根据市场状态动态选取，与仓位分配联动）
    strategy_scores = get_regime_scores(regime)
    # 动态过滤阈值（RS 容忍度 / 流动性门槛随市浮动）
    rs_tol, min_amt = (RS_TOL, min_signal_amount)
    if dynamic_thresholds:
        rs_tol, min_amt = _dynamic_thresholds(regime)
    # 策略权重分配（由真实市场状态驱动，而非硬编码震荡）
    alloc = build_strategy_allocation(
        regime,
        boll_rows_count=len(boll),
        theme_rows_count=len(theme),
        has_cctv_hot=bool(cctv),
        macro_level="low",
    )
    weights = alloc["final_weights"]

    rows = []
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
            score += strategy_scores.get("boll", 45)
            name = boll[code]["name"] or name
            buy_price = boll[code].get("buy_price")
        if code in relativity:
            hit_strategies.append("Relativity")
            score += strategy_scores.get("relativity", 15)
            name = relativity[code]["name"] or name
        if code in theme:
            hit_strategies.append("Theme")
            score += strategy_scores.get("theme", 15)
            name = theme[code]["name"] or name
            # Theme 综合分作为额外加权（综合分 0-100，按 10% 加）
            theme_score = theme[code].get("score") or 0
            score += min(theme_score * 0.1, 10)
        if code in cctv:
            hit_strategies.append("CCTV")
            score += strategy_scores.get("cctv", 10)
            name = cctv[code]["name"] or name
        if code in momentum:
            hit_strategies.append("Momentum")
            score += strategy_scores.get("momentum", 20)
            name = momentum[code]["name"] or name

        # ── 买入价兜底：非 Boll 策略无建议买入价时用信号日收盘价 ─────────
        if buy_price is None and levels:
            buy_price = levels.get("close")

        # ── 名字兜底：所有策略 CSV 都缺名字时从 stock_info / baostock 补查 ────────
        if not name:
            name = lookup_stock_name(code)

        # 多策略命中加分
        if len(hit_strategies) > 1:
            score += (len(hit_strategies) - 1) * MULTI_HIT_BONUS

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
        for s in hit_strategies:
            skey = s.lower()
            w = weights.get(skey, 0)
            cnt = max(strategy_pick_counts.get(skey, 1), 1)
            share = w / cnt  # 该策略仓位均分到每只票
            if share > best_weight:
                best_weight = share
        position_pct = min(best_weight / 100.0, 0.3)  # 单票上限 30%
        position_amount = total_capital * position_pct

        row = {
            "股票代码": code,
            "股票名称": name,
            "命中策略数": len(hit_strategies),
            "来源策略": "/".join(hit_strategies),
            "综合评分": round(score, 1),
            "建议买入价": buy_price,
            "建议仓位%": round(position_pct * 100, 1),
            "建议金额": round(position_amount, 0),
        }

        if levels:
            row["最新价"] = levels.get("close")
            row["止损价(下轨)"] = levels.get("lower")
            row["止盈价(上轨)"] = levels.get("upper")
            row["MA20"] = levels.get("ma20")

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
        # 单板块集中度控制：最终入选单板块最多 max_per_sector 只，强制分散（映射缺失则跳过）
        if sector_cap and sector_map:
            df, sector_hit_cap = sector_mod.apply_sector_cap(
                df, sector_map, max_per=max_per_sector, top_n=max_picks
            )
        else:
            df = df.head(max_picks)

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
    )
    if filtered_out:
        report += f"\n- 🛡️ 趋势守卫剔除 {filtered_out} 只破位/下降通道股（价格低于 MA20 超 12%）"
    if market_gate:
        if regime == "下行防御" and gated_out:
            report += f"\n- 🚦 趋势闸门触发（市场下行防御）：剔除 {gated_out} 只纯均值回归候选（Boll/Relativity），仅留顺势策略"
        else:
            report += f"\n- 🚦 市场状态：{regime}（趋势闸门生效；评分权重: Boll {strategy_scores.get('boll')} / Momentum {strategy_scores.get('momentum')} / Theme {strategy_scores.get('theme')} / Relativity {strategy_scores.get('relativity')} / CCTV {strategy_scores.get('cctv')}）"
    if rs_filtered_out:
        report += f"\n- 📉 相对强度过滤剔除 {rs_filtered_out} 只跑输大盘超 {rs_tol * 100:.0f}% 的票（alpha 弱，直接不治本；阈值随市浮动）"
    if liquidity_filtered_out:
        report += f"\n- 💧 流动性门槛剔除 {liquidity_filtered_out} 只信号日成交额 < ¥{min_amt / 1e8:.2f}亿 的票（难出场/庄股陷阱；门槛随市浮动）"
    if sector_cap:
        n_sec = df["股票代码"].map(lambda c: sector_mod.industry_of(c)).nunique() if not df.empty else 0
        cap_note = "（单板块上限 %d，已触发分散）" % max_per_sector if sector_hit_cap else "（单板块上限 %d）" % max_per_sector
        report += f"\n- 🏭 板块轮动+集中度：最终 {len(df)} 只覆盖 {n_sec} 个行业{cap_note}（强势板块候选已微调评分）"
    if profile is not None:
        report += f"\n- 🌡️ 市场仪表盘：{profile.summary()}"
    return df, report


def _build_report_text(
    df: pd.DataFrame,
    date_yyyymmdd: str,
    n_boll: int,
    n_relativity: int,
    n_theme: int,
    n_cctv: int,
    n_momentum: int = 0,
    *,
    source_dates: dict[str, Optional[str]] | None = None,
    max_stale_days: int = 3,
) -> str:
    """生成日报段落。"""
    if df.empty:
        stale_notes = _format_source_date_notes(date_yyyymmdd, source_dates or {}, max_stale_days=max_stale_days)
        if stale_notes:
            return "\n## 今日操作清单\n- 无候选\n" + stale_notes
        return "\n## 今日操作清单\n- 无候选"

    lines = [
        f"\n## 今日操作清单（{date_yyyymmdd}）",
        f"- 信号来源: Boll {n_boll} | Relativity {n_relativity} | Momentum {n_momentum} | Theme {n_theme} | CCTV {n_cctv}",
        f"- 融合后候选: {len(df)} 只（按综合评分排序）",
    ]
    stale_notes = _format_source_date_notes(date_yyyymmdd, source_dates or {}, max_stale_days=max_stale_days)
    if stale_notes:
        lines.append(stale_notes)
    lines.extend([
        "",
        "| 代码 | 名称 | 命中 | 评分 | 仓位% | 止损 | 止盈 |",
        "|------|------|------|------|-------|------|------|",
    ])
    for _, r in df.iterrows():
        stop = fmt_num(r.get("止损价(下轨)"), digits=2, na="-")
        take = fmt_num(r.get("止盈价(上轨)"), digits=2, na="-")
        lines.append(
            f"| {r['股票代码']} | {r['股票名称']} | {r['命中策略数']} | {r['综合评分']} | {r['建议仓位%']} | {stop} | {take} |"
        )
    lines.append("")
    lines.append("- 止损=Boll下轨，止盈=Boll上轨（前复权）；仓位为建议上限，单票不超过 30%。")
    return "\n".join(lines)


def _format_source_date_notes(
    date_yyyymmdd: str,
    source_dates: dict[str, Optional[str]],
    *,
    max_stale_days: int,
) -> str:
    """标注各策略实际使用的数据日期，过期数据显式警告。"""
    notes: list[str] = []
    for name, actual in source_dates.items():
        if not actual:
            notes.append(f"- ⚠️ {name}: 无可用数据（{max_stale_days} 天内未找到）")
            continue
        if actual != date_yyyymmdd:
            notes.append(f"- ⚠️ {name}: 使用 {actual} 的数据（非当日 {date_yyyymmdd}）")
    if not notes:
        return ""
    return "\n".join(["", "**数据日期说明**", *notes])


def save_action_list(df: pd.DataFrame, date_yyyymmdd: str) -> Optional[Path]:
    """保存操作清单 CSV，返回路径。"""
    if df.empty:
        return None
    path = STOCK_DATA_DIR / f"Daily-Action-List-{date_yyyymmdd}.csv"
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path
