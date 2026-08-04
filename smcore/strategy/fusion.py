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

from smcore.config.defaults import (
    BETA_FALLBACK,
    BETA_MIN_KEEP,
    BETA_WINDOW,
    DEFAULT_K,
    DEFAULT_WINDOW,
    MAX_SINGLE_WEIGHT_PCT,
    PORTFOLIO_BETA_CEILING,
    STOCK_DATA_DIR,
)
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


def _build_stock_name_cache_from_akshare(path: Path) -> bool:
    """用 akshare 全市场代码→名称映射构建本地缓存文件。成功返回 True。"""
    try:
        import akshare as ak
        df = ak.stock_info_a_code_name()
        if df is None or df.empty:
            return False
        df = df.copy()
        df["code"] = df["code"].astype(str).str.zfill(6)
        # 清洗名称：去除内部多余空格（如 "万 科Ａ" -> "万科Ａ"），保留全角Ａ/Ｂ
        df["name"] = (
            df["name"].astype(str).str.replace(r"\s+", "", regex=True).str.strip()
        )
        df = df[df["name"].str.len() > 0]
        df.to_csv(path, index=False, encoding="utf-8-sig")
        for _, r in df.iterrows():
            c = format_stock_code(str(r.get("code", "")).strip())
            n = str(r.get("name", "")).strip()
            if c and n and c not in _stock_name_cache:
                _stock_name_cache[c] = n
        return True
    except Exception as exc:
        print(f"[name-cache] akshare 构建失败：{type(exc).__name__}: {exc}")
        return False


def _get_stock_name_map() -> dict:
    """返回 {code(6位): name} 映射，优先从 stock_info_a_code_name.csv 缓存读取；
    文件缺失时自动用 akshare 全市场映射构建（覆盖全部 A 股，避免 baostock 兜底查不到）。"""
    global _stock_name_cache
    if _stock_name_cache is not None:
        return _stock_name_cache
    _stock_name_cache = {}
    p = STOCK_DATA_DIR / "stock_info_a_code_name.csv"
    if p.exists():
        try:
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
    else:
        # 缓存文件缺失 → 用 akshare 一次性构建并持久化（之后直接读文件，秒级）
        if _build_stock_name_cache_from_akshare(p):
            print(f"[name-cache] 已从 akshare 构建 {len(_stock_name_cache)} 只股票名称缓存")
    return _stock_name_cache


def lookup_stock_name(code: str) -> str:
    """查询单只股票名称（缓存 → akshare 单只 → baostock 兜底），找不到返回空串。"""
    c6 = format_stock_code(code)
    if not c6:
        return ""
    # 1) 已有缓存（含 stock_info_a_code_name.csv 或 akshare 全市场构建结果）
    m = _get_stock_name_map()
    if c6 in m:
        return m[c6]
    # 2) akshare 单只实时兜底（覆盖新股等未入全市场列表的代码）
    try:
        import akshare as ak
        # stock_individual_info_em 返回 item/value 两列，其中「股票简称」即名称
        info = ak.stock_individual_info_em(symbol=c6)
        if info is not None and not info.empty:
            name_row = info[info["item"].astype(str).str.contains("简称|名称", na=False)]
            if not name_row.empty:
                found = str(name_row["value"].iloc[0]).strip()
                if found:
                    _stock_name_cache[c6] = found  # 写回缓存
                    return found
    except Exception:
        pass
    # 3) baostock 兜底（仅一次登录，~0.3s/只）
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


# ── 名称归一化：把 pandas 写出的 "nan" / "None" / "--" 统一视为缺失 ──
_INVALID_NAMES = {"nan", "none", "null", "--", "", "na", "nat"}


def _normalize_name(raw: str) -> str:
    """将 CSV 中可能出现的无效名称归一化为空串。"""
    s = (raw or "").strip()
    return "" if s.lower() in _INVALID_NAMES else s


# ── 策略评分权重：纯数据驱动，无硬编码表 ──
# _REGIME_STRATEGY_SCORE 已删除（2026-08-02）：所有策略分数现在由
# adaptive_weights.compute_adaptive_allocation() 从近期回测业绩自动算出，
# 不再使用任何 regime→权重的查找表或固定默认值。
# fusion() 中 strategy_scores 直接取自适应权重（百分比量级），无 fallback 常数。


def _dynamic_thresholds(regime: str, profile=None) -> tuple[float, float]:
    """RS 容忍度与流动性门槛：根据实际市场数据连续计算，非 regime 查表。

    RS 容忍度：
    - 趋势上行 + 高强度 → 放宽（让强势票过）
    - 下行防御 / 高波动 → 收紧（只留最强控回撤）
    - 基准 = 0.03，在 [0.015, 0.07] 区间连续浮动

    流动性门槛：
    - 趋势上行 → 降低（允许中小盘参与）
    - 下行防御 / 高波动 → 抬高（只留大票防流动性危机）
    - 基准 = ¥1亿，在 [5e7, 3e8] 区间连续浮动
    """
    # 从 MarketProfile 取连续指标（无 profile 时回退中性）
    strength = getattr(profile, "regime_strength", 0.5) if profile else 0.5
    vol_pctile = getattr(profile, "volatility_pctile", 0.5) if profile else 0.5

    # RS 容忍度：趋势越强越宽、波动越大越窄
    base_tol = 0.03
    tol_adjust = (strength - 0.5) * 0.04   # 强度 0→1 贡献 [-0.02, +0.02]
    vol_adjust = -(vol_pctile - 0.5) * 0.03  # 波动率贡献 [-0.015, +0.015]（高波收紧）
    rs_tol = max(0.015, min(0.07, base_tol + tol_adjust + vol_adjust))

    # 流动性门槛：趋势强降低、波动高抬高
    base_amt = 1e8  # ¥1 亿
    amt_adjust = -(strength - 0.5) * 1e8     # 强度 0→1 贡献 [-5e7, +5e7]
    vol_amt_adj = (vol_pctile - 0.5) * 1.2e8  # 波动率贡献 [-6e7, +6e7]（高波抬高）
    min_amt = max(5e7, min(3e8, base_amt + amt_adjust + vol_amt_adj))

    return rs_tol, min_amt


def _adaptive_multi_hit_bonus(n_active_strategies: int) -> int:
    """多策略命中加分：随活跃策略数自适应。

    活跃策略少时加分高（稀缺性奖励），多时加分低（避免通胀）。
    公式：max(1, round(12 / n_active))，范围 [1, 12]。
    替代固定 MULTI_HIT_BONUS=5。
    """
    return max(1, round(12 / max(n_active_strategies, 1)))

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
                "name": _normalize_name(row.get("股票名称", "")),
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
                "name": _normalize_name(row.get("股票名称", "")),
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
                "name": _normalize_name(row.get("股票名称", "")),
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
                "name": _normalize_name(row.get("股票名称", "")),
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
                "name": _normalize_name(row.get("股票名称", "")),
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


def _apply_strategy_cap(df: pd.DataFrame, max_per: int) -> pd.DataFrame:
    """最终名单按策略分散：每个策略最多保留 max_per 只（取已排序的前 max_per）。

    避免单策略（如 CCTV）占满最终名单导致回测池同质化、自适应策略权重失效。
    多策略命中票按「来源策略」首个策略归属（与仓位分配取最高权重策略口径近似一致）。
    """
    if max_per <= 0 or "来源策略" not in df.columns or df.empty:
        return df
    keep = []
    cnt: dict[str, int] = {}
    for _, r in df.iterrows():
        parts = [s.strip().lower() for s in str(r.get("来源策略", "")).replace("/", "，").split("，") if s.strip()]
        owner = parts[0] if parts else "__none__"
        c = cnt.get(owner, 0)
        if c < max_per:
            keep.append(True)
            cnt[owner] = c + 1
        else:
            keep.append(False)
    return df[keep].reset_index(drop=True)


# ── 组合 β 软约束（风险中性化：组合对沪深300 的暴露不要过高）─────────────
def _estimate_betas(codes, as_of_yyyymmdd: str, window: int = BETA_WINDOW) -> dict[str, float]:
    """估计候选股对沪深300 的 β（本地 k_data + 沪深300 序列，零联网）。

    用近 window 个交易日的个股日收益与沪深300 日收益对齐，β = cov/var。
    个股缺本地 k_data 时回退 BETA_FALLBACK（中性 1.0），不阻断清单生成。

    Returns:
        {code(6位): beta}
    """
    idx = _get_hs300_close()
    out: dict[str, float] = {}
    if idx is None:
        return {format_stock_code(c): BETA_FALLBACK for c in codes}
    try:
        target = pd.Timestamp(as_of_yyyymmdd)
        idx_prior = idx.loc[:target]
        if len(idx_prior) < 2:
            return {format_stock_code(c): BETA_FALLBACK for c in codes}
        idx_series = idx_prior.tail(window + 1)
        idx_ret = idx_series.pct_change().dropna()
    except Exception:
        return {format_stock_code(c): BETA_FALLBACK for c in codes}

    for c in codes:
        c6 = format_stock_code(c)
        if not c6:
            continue
        try:
            p = STOCK_DATA_DIR / "k_data" / f"{c6}_qfq_full.csv"
            if not p.exists():
                out[c6] = BETA_FALLBACK
                continue
            d = pd.read_csv(p)
            if "date" not in d.columns or "close" not in d.columns or len(d) < 3:
                out[c6] = BETA_FALLBACK
                continue
            d["date"] = pd.to_datetime(d["date"], errors="coerce")
            d = d.dropna(subset=["date"]).set_index("date").sort_index()
            close = pd.to_numeric(d["close"], errors="coerce").dropna()
            close = close.loc[:target].tail(window + 1)
            sret = close.pct_change().dropna()
            joined = pd.concat([sret.rename("s"), idx_ret.rename("i")], axis=1, join="inner").dropna()
            if len(joined) < 10:
                out[c6] = BETA_FALLBACK
                continue
            cov = joined["s"].cov(joined["i"])
            var = joined["i"].var()
            out[c6] = float(cov / var) if var and var > 0 else BETA_FALLBACK
        except Exception:
            out[c6] = BETA_FALLBACK
    return out


def _portfolio_beta(df, betas: dict[str, float]) -> float:
    """按 建议仓位% 加权计算组合 β。缺 β 的票按 BETA_FALLBACK 计。"""
    if df is None or df.empty or "建议仓位%" not in df.columns:
        return BETA_FALLBACK
    tot = float(df["建议仓位%"].sum())
    if tot <= 0:
        return BETA_FALLBACK
    wsum = 0.0
    for _, r in df.iterrows():
        b = betas.get(format_stock_code(r["股票代码"]), BETA_FALLBACK)
        wsum += float(r.get("建议仓位%") or 0.0) * b
    return wsum / tot


def _apply_position_sizing(
    df,
    weights: dict[str, float],
    surv: dict[str, int],
    total_capital: float,
    max_single_weight_frac: float,
) -> tuple[pd.DataFrame, int]:
    """按「命中策略中权重最高者 / 该策略最终存活票数」重算每只票的建议仓位%与金额。

    这是风险层最后一道集中度闸：单名仓位不得超过 max_single_weight_frac
    （单名仓位上限，与行业权重上限、组合 β 上限互补——后两者管不到「单只票吃光仓位」）。
    返回 (df, n_hit)：df 已就地写入「建议仓位%」「建议金额」列；n_hit 为被上限截断的只数。
    不修改传入 df 之外的状态。
    """
    if df is None or df.empty:
        return df, 0
    new_pct, new_amt = [], []
    n_hit = 0
    for _, r in df.iterrows():
        hits = [
            s.strip().lower()
            for s in str(r.get("来源策略", "")).replace("/", "，").split("，")
            if s.strip()
        ]
        best = 0.0
        for s in hits:
            w = weights.get(s, 0)
            c = max(surv.get(s, 1), 1)
            share = w / c
            if share > best:
                best = share
        p = min(best / 100.0, max_single_weight_frac)
        if best / 100.0 > max_single_weight_frac + 1e-9:
            n_hit += 1
        new_pct.append(round(p * 100, 1))
        new_amt.append(round(total_capital * p, 0))
    df = df.copy()
    df["建议仓位%"] = new_pct
    df["建议金额"] = new_amt
    return df, n_hit


def _apply_beta_cap(df, betas: dict[str, float], max_beta: float, min_keep: int = BETA_MIN_KEEP):
    """组合 β 超上限时，逐步剔除当前 β 最高的个股，直到 ≤ max_beta 或只剩 min_keep 只。

    返回 (trimmed_df, n_trimmed)。不修改传入 df。
    """
    if df is None or df.empty:
        return df, 0
    work = df.copy()
    n_trimmed = 0
    while len(work) > min_keep:
        pb = _portfolio_beta(work, betas)
        if pb <= max_beta:
            break
        # 找当前 β 最高者剔除（降 β 最快）
        best_idx = None
        best_b = float("-inf")
        for idx, r in work.iterrows():
            b = betas.get(format_stock_code(r["股票代码"]), BETA_FALLBACK)
            if b > best_b:
                best_b = b
                best_idx = idx
        if best_idx is None:
            break
        work = work.drop(index=best_idx).reset_index(drop=True)
        n_trimmed += 1
    return work, n_trimmed


def fuse_signals(
    date_yyyymmdd: str,
    *,
    total_capital: float = 100000.0,
    max_picks: int = 50,
    max_per_strategy: int = 10,
    fetch_levels: bool = True,
    trend_guard: bool = True,
    market_gate: bool = True,
    relative_strength_filter: bool = True,
    min_signal_amount: float = MIN_SIGNAL_AMOUNT,
    dynamic_thresholds: bool = True,
    sector_cap: bool = True,
    max_per_sector: int = sector_mod.DEFAULT_MAX_PER_SECTOR,
    sector_weight_cap: bool = True,
    max_sector_weight_pct: float = sector_mod.DEFAULT_MAX_SECTOR_WEIGHT_PCT,
    beta_neutral: bool = True,
    max_portfolio_beta: float = PORTFOLIO_BETA_CEILING,
    max_single_weight_pct: float = MAX_SINGLE_WEIGHT_PCT,
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
        compute_adaptive_allocation,
        cash_from_volatility,
        cash_from_regime,
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
    # 单名仓位上限（小数），风险层最后一道集中度闸
    max_single_weight_frac = max_single_weight_pct / 100.0
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
        # 按策略分散上限：防止单策略占满最终名单、压垮分散度，
        # 保证自适应策略权重能真正生效（每个策略都有代表票进入回测）
        df = _apply_strategy_cap(df, max_per_strategy)
        # 单板块集中度控制：最终入选单板块最多 max_per_sector 只，强制分散（映射缺失则跳过）
        if sector_cap and sector_map:
            df, sector_hit_cap = sector_mod.apply_sector_cap(
                df, sector_map, max_per=max_per_sector, top_n=max_picks
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
        df, single_cap_hit = _apply_position_sizing(
            df, weights, _surv, total_capital, max_single_weight_frac
        )

    # ── ④ 风险中性化 ────────────────────────────────────────────────
    # (a) 行业权重上限：任一板块总仓位 ≤ 全组合 × max_sector_weight_pct（与数量上限互补）
    beta_hit = 0
    if not df.empty and sector_map:
        if sector_weight_cap:
            df, weight_hit = sector_mod.apply_sector_weight_cap(
                df, sector_map, max_weight_pct=max_sector_weight_pct, top_n=max_picks
            )
        else:
            weight_hit = False
        # (b) 组合 β 软约束：组合对沪深300 的加权 β 超上限则逐步剔除最高 β 个股
        if beta_neutral and len(df) > BETA_MIN_KEEP:
            betas = _estimate_betas(df["股票代码"].tolist(), date_yyyymmdd)
            df, beta_hit = _apply_beta_cap(df, betas, max_beta=max_portfolio_beta, min_keep=BETA_MIN_KEEP)
            df = df.reset_index(drop=True)
            # β 约束可能改变入选，重算仓位金额（避免建议金额与最终清单错位）；
            # 这趟重算若执行，则以它为准（覆盖上面的 single_cap_hit 计数）
            if not df.empty:
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
        max_single_weight_pct=max_single_weight_pct,
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
        cap_note = "（单板块上限 %d，已触发分散）" % max_per_sector if sector_hit_cap else "（单板块上限 %d）" % max_per_sector
        report += f"\n- 🏭 板块轮动+集中度：最终 {len(df)} 只覆盖 {n_sec} 个行业{cap_note}（强势板块候选已微调评分）"
    if not df.empty:
        wcap_note = "（单行业权重≤%g%%，已触发）" % max_sector_weight_pct if (sector_weight_cap and weight_hit) else "（单行业权重≤%g%%）" % max_sector_weight_pct
        betas = _estimate_betas(df["股票代码"].tolist(), date_yyyymmdd) if beta_neutral else {}
        pb = _portfolio_beta(df, betas) if beta_neutral else None
        beta_note = f"；组合β={pb:.2f}（上限{max_portfolio_beta}，剔除{beta_hit}只高β）" if beta_neutral else ""
        single_note = "（单名上限≤%g%%，已截断%d只）" % (max_single_weight_pct, single_cap_hit) if single_cap_hit else "（单名上限≤%g%%）" % max_single_weight_pct
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
            "sector_weight_cap_pct": max_sector_weight_pct if sector_weight_cap else None,
            "beta_ceiling": max_portfolio_beta if beta_neutral else None,
            "single_weight_cap_pct": max_single_weight_pct,
        }
        save_regime_snapshot(snapshot)
    except Exception:
        pass
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
    max_single_weight_pct: float = MAX_SINGLE_WEIGHT_PCT,
) -> str:
    """生成日报段落。"""
    # ── 策略贡献度汇总（显眼置顶，一眼看出哪几个策略在出力）──
    strat_raw = {
        "Boll": n_boll,
        "Relativity": n_relativity,
        "Theme": n_theme,
        "CCTV": n_cctv,
        "Momentum": n_momentum,
    }
    sd = source_dates or {}
    contrib_lines = []
    for name, cnt in strat_raw.items():
        actual = sd.get(name)
        if actual is None:
            status = "❌ 缺失（未找到文件）"
        elif cnt == 0:
            status = f"⚪ 产出=0（{actual} 数据为空）"
        elif actual != date_yyyymmdd:
            status = f"✅ {cnt} 只（{actual}，回退{max_stale_days}天内）"
        else:
            status = f"✅ {cnt} 只"
        contrib_lines.append(f"- {name}: {status}")
    active_count = sum(1 for c in strat_raw.values() if c > 0)

    if df.empty:
        stale_notes = _format_source_date_notes(date_yyyymmdd, sd, max_stale_days=max_stale_days)
        header = "\n## 今日操作清单\n- 无候选"
        summary = "\n### 策略贡献度\n" + "\n".join(contrib_lines) + (
            f"\n> 📊 仅 {active_count}/5 个策略有输出，清单可能不完整。" if active_count < 3 else ""
        )
        return header + ("\n" + stale_notes if stale_notes else "") + summary

    lines = [
        f"\n## 今日操作清单（{date_yyyymmdd}）",
        f"- 融合后候选: {len(df)} 只（按综合评分排序）",
        "",
        "### 策略贡献度",
        *contrib_lines,
        "" if active_count >= 3 else f"> ⚠️ 仅 {active_count}/5 个策略有输出，回测/决策参考价值有限。",
        "",
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
    lines.append(f"- 止损=Boll下轨，止盈=Boll上轨（前复权）；仓位为建议上限，单票不超过 {max_single_weight_pct:.0f}%。")
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
