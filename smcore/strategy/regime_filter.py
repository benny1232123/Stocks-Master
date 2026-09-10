"""市场状态、沪深300 序列与相对强度过滤。

从 fusion.py 抽出的「regime 检测 / HS300 抓取 / RS 过滤 / 动态阈值」职责。
纯数据驱动，无 regime→权重硬编码表（权重见 adaptive_weights）。
"""
from __future__ import annotations

import sys
from datetime import date, datetime
from typing import Optional

import pandas as pd

from smcore.strategy.market import compute_market_profile

# ── 策略评分权重：纯数据驱动，无硬编码表 ──
# _REGIME_STRATEGY_SCORE 已删除（2026-08-02）：所有策略分数现在由
# adaptive_weights.compute_adaptive_allocation() 从近期回测业绩自动算出，
# 不再使用任何 regime→权重的查找表或固定默认值。
# fusion() 中 strategy_scores 直接取自适应权重（百分比量级），无 fallback 常数。


def _rf_cfg() -> dict:
    """读取 ``risk_config.json`` 的 regime_filter 段（缺失时回退内置默认）。

    2026-09-09：原先这些阈值以字面量散落在函数体内，无法随市场调优，违反
    「权重/正则化超参必可配置」的约定。现统一由 risk_config.json 驱动，
    默认值与迁移前完全一致。
    """
    try:
        from smcore.strategy.risk_rules import RISK_CONFIG

        cfg = RISK_CONFIG.get("regime_filter")
        if isinstance(cfg, dict) and cfg:
            return cfg
    except Exception:
        pass
    return {
        "rs_tol_base": 0.03,
        "rs_tol_strength_span": 0.04,
        "rs_tol_vol_span": 0.03,
        "rs_tol_min": 0.015,
        "rs_tol_max": 0.07,
        "min_amt_base": 1e8,
        "min_amt_strength_span": 1e8,
        "min_amt_vol_span": 1.2e8,
        "min_amt_floor": 5e7,
        "min_amt_ceil": 3e8,
        "trend_guard_below_ma20": 0.12,
        "rs_lookback": 20,
        "min_signal_amount": 1e8,
        "multi_hit_bonus_cap": 12,
    }


def _dynamic_thresholds(regime: str, profile=None) -> tuple[float, float]:
    """RS 容忍度与流动性门槛：根据实际市场数据连续计算，非 regime 查表。

    RS 容忍度：
    - 趋势上行 + 高强度 → 放宽（让强势票过）
    - 下行防御 / 高波动 → 收紧（只留最强控回撤）
    - 基准 = rs_tol_base，在 [rs_tol_min, rs_tol_max] 区间连续浮动

    流动性门槛：
    - 趋势上行 → 降低（允许中小盘参与）
    - 下行防御 / 高波动 → 抬高（只留大票防流动性危机）
    - 基准 = min_amt_base，在 [min_amt_floor, min_amt_ceil] 区间连续浮动
    """
    c = _rf_cfg()
    # 从 MarketProfile 取连续指标（无 profile 时回退中性）
    strength = getattr(profile, "regime_strength", 0.5) if profile else 0.5
    vol_pctile = getattr(profile, "volatility_pctile", 0.5) if profile else 0.5

    # RS 容忍度：趋势越强越宽、波动越大越窄
    base_tol = float(c["rs_tol_base"])
    tol_adjust = (strength - 0.5) * float(c["rs_tol_strength_span"])
    vol_adjust = -(vol_pctile - 0.5) * float(c["rs_tol_vol_span"])
    rs_tol = max(float(c["rs_tol_min"]), min(float(c["rs_tol_max"]), base_tol + tol_adjust + vol_adjust))

    # 流动性门槛：趋势强降低、波动高抬高
    base_amt = float(c["min_amt_base"])
    amt_adjust = -(strength - 0.5) * float(c["min_amt_strength_span"])
    vol_amt_adj = (vol_pctile - 0.5) * float(c["min_amt_vol_span"])
    min_amt = max(float(c["min_amt_floor"]), min(float(c["min_amt_ceil"]), base_amt + amt_adjust + vol_amt_adj))

    return rs_tol, min_amt


def _adaptive_multi_hit_bonus(n_active_strategies: int) -> int:
    """多策略命中加分：随活跃策略数自适应。

    活跃策略少时加分高（稀缺性奖励），多时加分低（避免通胀）。
    公式：max(1, round(cap / n_active))，cap 来自配置（默认 12）。
    替代固定 MULTI_HIT_BONUS=5。
    """
    cap = int(_rf_cfg().get("multi_hit_bonus_cap", 12))
    return max(1, round(cap / max(n_active_strategies, 1)))


# 模块级别名：保留旧名以兼容既有 import（`from .regime_filter import RS_TOL`），
# 但取值来自 risk_config.json 的 regime_filter 段，不再写死在代码里。
# 注：这类别名在 import 时求值一次；需要热更新的地方请用 _rf_cfg() / _dynamic_thresholds()。
_RF = _rf_cfg()

# 趋势守卫：价格低于 MA20 超过该比例，视作破位/下降通道自由落体股，剔除。
# 默认 12%——保留 Boll 轻度超卖票（近下轨通常仅低于 MA20 几个百分点），
# 但剔除明显破位（如单日 -20%+ 的崩盘股），从信号层防尾部巨亏。
TREND_GUARD_BELOW_MA20 = float(_RF["trend_guard_below_ma20"])

# 相对大盘强度过滤（针对根因：大盘涨、选出的超卖票仍跑输）。
# 候选近 RS_LOOKBACK 日收益若跑输沪深300 同期收益超 RS_TOL，则剔除（除非该票本身是动量票）。
# 动量票已要求 ret20>0 且 MA20 上行，豁免本过滤以免误杀强势股。
RS_LOOKBACK = int(_RF["rs_lookback"])
RS_TOL = float(_RF["rs_tol_base"])
# 流动性门槛：信号日成交额（元）低于此值的票直接剔除。
# 头对头测量（measure_signal_quality.py，RS 宇宙样本）：¥1e8 门槛相对基线
# 平均收益 +0.92%、胜率 +5.1%、盈亏比 +0.43，为最优甜点（¥5e7 反而更弱）。
# 剔除流动性差的票可避免难出场/庄股陷阱导致的隐性亏损。
# 注意：amount 单位随数据源（akshare/baostock 均为元）；取值为 None 时放行（后端故障不误杀）。
MIN_SIGNAL_AMOUNT = float(_RF["min_signal_amount"])
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

    ⚠️ 失败时降级为"震荡轮动"是 fail-soft，但此前静默吞异常会让「真实 regime 是
    下行防御却按震荡轮动放行仓位」这类事故不可见。现在首次失败会告警。
    """
    try:
        return compute_market_profile().regime
    except Exception as exc:
        print(
            f"[regime_filter] WARN: 市场状态检测失败，降级为'震荡轮动'（{exc!r}）",
            file=sys.stderr,
        )
        return "震荡轮动"


_HS300_CLOSE_CACHE: Optional[pd.Series] = None
# 缓存获取日期。⚠️ 2026-09-09 修复：此前缓存一经建立永不失效，FastAPI 长驻进程
# 会一直用进程启动当天拉到的序列，跨天后 RS 过滤/相对强度基准全是陈旧数据。
_HS300_CACHE_DATE: Optional[str] = None


def invalidate_hs300_cache() -> None:
    """清空沪深300 缓存（测试 / 强制刷新用）。"""
    global _HS300_CLOSE_CACHE, _HS300_CACHE_DATE
    _HS300_CLOSE_CACHE = None
    _HS300_CACHE_DATE = None


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
    except Exception as exc:
        print(f"[regime_filter] WARN: baostock 拉沪深300 失败（{exc!r}）", file=sys.stderr)
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
    except Exception as exc:
        print(f"[regime_filter] WARN: akshare 拉沪深300 失败（{exc!r}）", file=sys.stderr)
        return None


def _get_hs300_close(ttl_days: int = 1) -> Optional[pd.Series]:
    """缓存沪深300 收盘价序列（baostock 主源 + akshare 兜底，东财-free）。

    此前仅走 akshare，沙箱/云端指数接口偶发失败会返回 None，
    导致 RS 过滤「数据缺失一律放行」而形同虚设。改为 baostock 主源后
    沙箱稳定可取，云端退 akshare，保证相对强度过滤真正生效。

    缓存按天失效（``ttl_days``，默认 1 天）：长驻服务（Render / FastAPI）若永不
    失效，跨天后仍用启动当天拉的序列，相对强度基准会越来越旧。
    ``ttl_days=0`` 表示不缓存（每次重拉）。
    """
    global _HS300_CLOSE_CACHE, _HS300_CACHE_DATE
    today = date.today().isoformat()
    if _HS300_CLOSE_CACHE is not None and _HS300_CACHE_DATE == today and ttl_days > 0:
        return _HS300_CLOSE_CACHE
    s = _fetch_hs300_baostock()
    if s is None:
        s = _fetch_hs300_akshare()
    if s is None or len(s) < 22:
        # 拉取失败时保留旧缓存（比"无基准导致 RS 过滤整体失效"更安全）
        if _HS300_CLOSE_CACHE is not None:
            return _HS300_CLOSE_CACHE
        return None
    _HS300_CLOSE_CACHE = s
    _HS300_CACHE_DATE = today
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
