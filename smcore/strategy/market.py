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
from pathlib import Path
from typing import Optional

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


def _fetch_index_series_sina(code: str) -> pd.DataFrame | None:
    """新浪历史日K（海外可达主源）：返回含 close/volume 的 DataFrame。

    此前 compute_market_profile 只走 baostock/akshare —— 海外 CI 上 baostock 连不上
    国内券商服务器、akshare 指数接口不稳，导致 regime 静默回退默认「震荡轮动」，
    权重表永不切换（用户直观感到"没跟着市场变"）。新浪历史K线接口在海外可达，
    作为 regime 检测的首要数据源；baostock/akshare 退为其后兜底。
    """
    try:
        import json
        import urllib.request

        sym = code.replace(".", "")  # sh.000300 -> sh000300
        url = (
            "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
            "CN_MarketData.getKLineData?"
            f"symbol={sym}&scale=240&ma=5&datalen=320"
        )
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn"},
        )
        raw = urllib.request.urlopen(req, timeout=15).read().decode("utf-8", "ignore")
        if "=" in raw[:20]:  # 个别返回形如 var jsonstr=...; 剥掉前缀
            raw = raw.split("=", 1)[1].strip().rstrip(";")
        arr = json.loads(raw)
        if not arr:
            return None
        rows = []
        for r in arr:
            try:
                d = pd.Timestamp(r["day"])
                c = float(r["close"])
                v = float(r.get("volume", 0) or 0)
                rows.append((d, c, v))
            except (KeyError, ValueError, TypeError):
                continue
        if len(rows) < 65:
            return None
        df = pd.DataFrame(rows, columns=["date", "close", "volume"]).set_index("date").sort_index()
        return df
    except Exception:
        return None


def _safe_std(rets: pd.Series, win: int) -> float | None:
    if len(rets) < win + 1:
        return None
    return float(rets.tail(win).std())


_INDEX_SERIES_CACHE: dict = {}  # code -> 全量索引序列 DataFrame（date-indexed），跨 as_of 复用


def _get_index_series(code: str) -> Optional["pd.DataFrame"]:
    """取单只指数全量日线（新浪主源→baostock/akshare兜底），模块级缓存避免重复联网。"""
    if code in _INDEX_SERIES_CACHE:
        return _INDEX_SERIES_CACHE[code]
    sina = _fetch_index_series_sina(code)
    df = sina if sina is not None else _fetch_index_series(code)
    if df is None:
        return None
    _INDEX_SERIES_CACHE[code] = df
    return df


_MARKET_PROFILE_CACHE: dict = {}  # as_of -> MarketProfile（None 键=最新）


def _profile_from_full_series(hs: "pd.DataFrame", zz500: Optional["pd.DataFrame"],
                              zz1000: Optional["pd.DataFrame"],
                              as_of=None) -> Optional[MarketProfile]:
    """由（已抓取的全量/已切片）指数序列合成 MarketProfile。任何数据不足返回 None。

    抽离自原 compute_market_profile，便于「最新」与「历史 as_of」两种场景复用同一套
    趋势/波动率/宽度/量能合成逻辑，避免逻辑分叉。
    """
    # 历史切片：只保留 <= as_of 的索引数据（因果安全）
    if as_of is not None and hs is not None:
        try:
            hs = hs.loc[:pd.Timestamp(as_of)]
        except Exception:
            pass
    if hs is None or len(hs) < 65:
        return None

    close = pd.to_numeric(hs["close"], errors="coerce").dropna()
    if len(close) < 65:
        return None
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
        return None
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
    if as_of is not None:
        try:
            zz500 = zz500.loc[:pd.Timestamp(as_of)] if zz500 is not None else None
            zz1000 = zz1000.loc[:pd.Timestamp(as_of)] if zz1000 is not None else None
        except Exception:
            pass
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


def compute_market_profile(as_of=None) -> MarketProfile:
    """计算多维市场仪表盘。任何数据缺失都保守降级，不抛异常。

    as_of 给定时按该历史日切片（因果安全：只用 <= as_of 的索引数据），用于 walk-forward
    把每个信号日归类到当时的市场状态（趋势上行/下行防御/震荡轮动），避免未来函数。
    """
    cache_key = as_of
    if cache_key in _MARKET_PROFILE_CACHE:
        return _MARKET_PROFILE_CACHE[cache_key]

    # 默认值（数据不足时）
    default = MarketProfile(
        regime="震荡轮动", regime_strength=0.5, trend="side",
        volatility_level="mid", volatility_pct=0.0, volatility_pctile=0.5,
        breadth_score=0.5, activity_ratio=1.0, hs300_ret20=0.0,
    )

    # 数据源优先级：新浪历史K线（海外可达，主源）→ baostock → akshare。
    # 海外 CI 上 baostock 连不上国内券商、akshare 指数接口不稳，若只靠它们会静默回退默认 regime。
    hs = _get_index_series(_HS300)
    zz500 = _get_index_series(_ZZ500)
    zz1000 = _get_index_series(_ZZ1000)
    prof = _profile_from_full_series(hs, zz500, zz1000, as_of=as_of)
    if prof is None:
        _MARKET_PROFILE_CACHE[cache_key] = default
        return default
    _MARKET_PROFILE_CACHE[cache_key] = prof
    return prof


_REGIME_SERIES_CACHE: dict = {}  # code -> 新浪全量序列 DataFrame（进程内缓存，避免批量标注重复联网）


def _load_index_series_sina_cached(code: str, cache_dir: Path) -> Optional["pd.DataFrame"]:
    """新浪历史K线（海外可达），带本地文件缓存 + 进程内缓存；失败返回 None。"""
    if code in _REGIME_SERIES_CACHE:
        return _REGIME_SERIES_CACHE[code]
    cname = code.replace(".", "")
    cf = cache_dir / f"{cname}.csv"
    df = None
    if cf.exists():
        try:
            df = pd.read_csv(cf, index_col=0, parse_dates=True)
            if "close" not in df.columns or len(df) < 65:
                df = None
        except Exception:
            df = None
    if df is None:
        df = _fetch_index_series_sina(code)
        if df is not None:
            try:
                cache_dir.mkdir(parents=True, exist_ok=True)
                df.to_csv(cf)
            except Exception:
                pass
    _REGIME_SERIES_CACHE[code] = df
    return df


def regime_as_of(sd: str, cache_dir=None) -> str:
    """历史信号日 sd 当时的市场状态（因果安全：只用 <= sd 的索引数据）。

    用于 walk-forward 把每个信号日归类到当时的市场状态（趋势上行/下行防御/震荡轮动）。
    数据源：仅新浪历史K线（海外可达），带本地文件缓存（stock_data/index_cache）与进程内缓存，
    避免批量标注时回退到慢源（baostock/akshare）挂死。任何失败/超时回退「震荡轮动」（vacuous）。
    """
    try:
        from smcore.config.defaults import PROJECT_ROOT
        cache_dir = cache_dir or (PROJECT_ROOT / "stock_data" / "index_cache")
    except Exception:
        cache_dir = Path("stock_data") / "index_cache"
    cache_dir = Path(cache_dir)

    hs = _load_index_series_sina_cached(_HS300, cache_dir)
    if hs is None or len(hs) < 65:
        return "震荡轮动"
    zz500 = _load_index_series_sina_cached(_ZZ500, cache_dir)
    zz1000 = _load_index_series_sina_cached(_ZZ1000, cache_dir)
    prof = _profile_from_full_series(hs, zz500, zz1000, as_of=sd)
    if prof is None:
        return "震荡轮动"
    return prof.regime
