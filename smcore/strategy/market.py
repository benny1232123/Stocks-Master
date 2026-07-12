"""多维市场仪表盘 —— 替代单一 MA60 三态判断。

此前 `_detect_market_regime` 只看沪深300 的 MA60 位置，非牛即熊或震荡。
本模块综合四个维度，产出更准的 `MarketProfile`，作为所有"市场自适应"的共同输入：

1. **趋势 (trend)**：沪深300 价格 vs MA20 / MA60，MA60 斜率方向
2. **波动率 (volatility)**：沪深300 近 20 日年化波动率，及其在近 250 日分布中的分位
3. **宽度 (breadth)**：沪深300 / 中证500 / 中证1000 近 20 日收益的一致性
   （三大宽基同步上涨=健康牛市；只有沪深300 涨、中小票跌=窄幅轮动/失真）
4. **量能 (activity)**：沪深300 近 5 日均量 / 近 60 日均量

合成：
- `regime`（向后兼容三态：趋势上行 / 下行防御 / 震荡轮动）—— 给 allocation / 评分权重 / 趋势闸门用
- `regime_strength`（0-1 连续强度）—— 给动态阈值 / 仓位强度用
- `volatility_level`（low/mid/high）—— 给波动率自适应风控用
- `breadth_score`（0-1）、`activity_ratio` —— 供看板展示与后续扩展

数据源：baostock 主源（指数代码 sh.000300 / sh.000905 / sh.000852）+ akshare 兜底，东财-free。
任一指数拉取失败时保守降级，不崩流程。
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

# 三大宽基指数（baostock 代码）
_HS300 = "sh.000300"
_ZZ500 = "sh.000905"  # 中证500
_ZZ1000 = "sh.000852"  # 中证1000


@dataclass
class MarketProfile:
    """市场状态快照。"""

    regime: str            # 趋势上行 / 下行防御 / 震荡轮动（向后兼容）
    regime_strength: float  # 0-1 连续强度
    trend: str             # up / down / side
    volatility_level: str  # low / mid / high
    volatility_pct: float   # 年化波动率（小数，如 0.18 = 18%）
    volatility_pctile: float  # 当前波动率在近 250 日中的分位 0-1
    breadth_score: float   # 0-1，宽度（三大宽基同步性）
    activity_ratio: float  # 量能比（近5日/近60日均量）
    hs300_ret20: float     # 沪深300 近20日收益

    def summary(self) -> str:
        return (
            f"regime={self.regime} strength={self.regime_strength:.2f} "
            f"trend={self.trend} vol={self.volatility_level}({self.volatility_pct*100:.1f}%, "
            f"p{self.volatility_pctile*100:.0f}) breadth={self.breadth_score:.2f} "
            f"activity={self.activity_ratio:.2f}"
        )


def _fetch_index_series(code: str, fields: str = "date,close,volume") -> pd.DataFrame | None:
    """拉单只指数日线（baostock 主源 + akshare 兜底）。返回含 date(索引)/close/volume 的 DataFrame。"""
    # baostock 主源
    try:
        import baostock as bs
        from smcore.data.session import session

        with session() as ok:
            if ok:
                rs = bs.query_history_k_data_plus(
                    code, fields, start_date="2020-01-01",
                    end_date=pd.Timestamp.today().strftime("%Y-%m-%d"),
                    frequency="d", adjustflag="3",
                )
                if rs.error_code == "0":
                    rows = []
                    while rs.next():
                        rows.append(rs.get_row_data())
                    if rows:
                        df = pd.DataFrame(rows, columns=rs.fields)
                        close = pd.to_numeric(df["close"], errors="coerce")
                        vol = pd.to_numeric(df.get("volume", pd.Series([float("nan")] * len(df))), errors="coerce")
                        dts = pd.to_datetime(df["date"], errors="coerce")
                        out = pd.DataFrame({"close": close.values, "volume": vol.values}, index=dts)
                        out = out[~out.index.isna()].sort_index()
                        if len(out) >= 22:
                            return out
    except Exception:
        pass

    # akshare 兜底
    try:
        import akshare as ak
        from smcore.data.kline import _call_with_timeout

        sym = code.replace(".", "").lower()  # sh000300
        df = _call_with_timeout(lambda: ak.stock_zh_index_daily(symbol=sym), 30)
        if df is not None and len(df) >= 22:
            close = pd.to_numeric(df["close"], errors="coerce")
            vol = pd.to_numeric(df.get("volume", pd.Series([float("nan")] * len(df))), errors="coerce")
            dts = pd.to_datetime(df["date"], errors="coerce")
            out = pd.DataFrame({"close": close.values, "volume": vol.values}, index=dts)
            out = out[~out.index.isna()].sort_index()
            if len(out) >= 22:
                return out
    except Exception:
        pass
    return None


def _safe_std(rets: pd.Series, win: int) -> float | None:
    if len(rets) < win + 1:
        return None
    return float(rets.tail(win).std())


def compute_market_profile() -> MarketProfile:
    """计算多维市场仪表盘。任何数据缺失都保守降级，不抛异常。"""
    # 默认值（数据不足时）
    default = MarketProfile(
        regime="震荡轮动", regime_strength=0.5, trend="side",
        volatility_level="mid", volatility_pct=0.0, volatility_pctile=0.5,
        breadth_score=0.5, activity_ratio=1.0, hs300_ret20=0.0,
    )

    hs = _fetch_index_series(_HS300)
    if hs is None or len(hs) < 65:
        return default

    close = pd.to_numeric(hs["close"], errors="coerce").dropna()
    if len(close) < 65:
        return default
    c = close.values.astype(float)
    price = c[-1]

    # —— 趋势 ——
    ma20 = c[-20:].mean()
    ma60 = c[-60:].mean()
    ma60_prev = c[-120:-60].mean() if len(c) >= 120 else c[-61:-1].mean()
    ma60_slope = (ma60 - ma60_prev) / ma60_prev if ma60_prev else 0.0
    if price > ma60 and ma60_slope > 0:
        trend = "up"
    elif price < ma60 and ma60_slope <= 0:
        trend = "down"
    else:
        trend = "side"

    # —— 波动率 ——
    rets = pd.Series(c).pct_change().dropna()
    vol_20 = _safe_std(rets, 20)
    if vol_20 is None or vol_20 <= 0:
        return default
    ann_vol = vol_20 * (252 ** 0.5)
    # 波动率分位（近 250 日滚动 std）
    roll = rets.rolling(20).std().dropna() * (252 ** 0.5)
    if len(roll) >= 30:
        vol_pctile = float((roll < ann_vol).mean())
    else:
        vol_pctile = 0.5
    if vol_pctile < 0.33:
        vol_level = "low"
    elif vol_pctile > 0.67:
        vol_level = "high"
    else:
        vol_level = "mid"

    # —— 宽度（三大宽基近20日收益一致性）——
    zz500 = _fetch_index_series(_ZZ500)
    zz1000 = _fetch_index_series(_ZZ1000)
    r300 = c[-1] / c[-21] - 1 if len(c) >= 21 else 0.0
    breadth = 0.5  # 默认中性
    if zz500 is not None and zz1000 is not None and len(zz500) >= 21 and len(zz1000) >= 21:
        rc = zz500["close"].values.astype(float)
        r1k = zz1000["close"].values.astype(float)
        r500 = rc[-1] / rc[-21] - 1
        r1000 = r1k[-1] / r1k[-21] - 1
        up_count = sum(1 for r in (r300, r500, r1000) if r > 0)
        base = up_count / 3.0
        small_avg = (r500 + r1000) / 2.0
        gap = abs(r300 - small_avg)
        # 沪深300 与中小票偏离 >15% 视为宽度失真，折扣
        div_penalty = min(gap / 0.15, 1.0)
        breadth = max(0.0, min(1.0, base * (1 - 0.5 * div_penalty)))

    # —— 量能 ——
    vol_series = pd.to_numeric(hs["volume"], errors="coerce").dropna()
    activity = 1.0
    if len(vol_series) >= 60:
        recent = vol_series.tail(5).mean()
        base_v = vol_series.tail(60).mean()
        if base_v and base_v > 0:
            activity = float(recent / base_v)

    # —— 合成 regime ——
    if trend == "up" and breadth >= 0.5:
        regime = "趋势上行"
    elif trend == "down" or (trend != "up" and vol_level == "high"):
        # 高波动且无明确上行 → 避险（高波动市容易急跌）
        regime = "下行防御"
    else:
        regime = "震荡轮动"

    # —— 连续强度 ——
    slope_norm = max(0.0, min(1.0, ma60_slope / 0.01))
    strength = 0.4 * slope_norm + 0.4 * breadth + 0.2 * (1 - vol_pctile)
    strength = max(0.0, min(1.0, strength))

    return MarketProfile(
        regime=regime, regime_strength=round(strength, 2), trend=trend,
        volatility_level=vol_level, volatility_pct=round(ann_vol, 4),
        volatility_pctile=round(vol_pctile, 2), breadth_score=round(breadth, 2),
        activity_ratio=round(activity, 2), hs300_ret20=round(r300, 4),
    )
