"""多策略回测引擎 —— 把项目四策略信号移植到 Backtrader。

策略（与 smcore/strategy/fusion.py 的 get_regime_scores(regime) 动态权重一一对应）：
- boll       : 布林带低吸（收盘价<下轨 或 ≤下轨×1.015），止损=下轨/止盈=上轨
- relativity : 个股 vs 上证指数 的"跟涨/抗跌"满足率（需指数数据，拉不到自动关闭）
- theme      : 量价+动量（换手/成交额放大倍数、5/20日动量、距20日高点），price 5~30
- cctv       : 题材/舆情（外部输入，默认关闭；提供 cctv_hits 后启用）

融合口径照搬 fusion.py：命中策略加权（boll40/rel25/theme20/cctv15），单票仓位
min(命中策略最高权重/100, 0.3)。技术三策略（boll/relativity/theme）纯靠行情即可
运行；cctv 是新闻因子，需外部注入命中列表。
"""
from __future__ import annotations

from collections import deque
from typing import Any, Optional

import numpy as np

import backtrader as bt


# ── 自定义行情数据（带成交额，供 Theme 策略）───────────────────────────────
class PriceData(bt.feeds.PandasData):
    """扩展 PandasData，把成交额(amount)作为单独 line 暴露。"""

    lines = ("amount",)
    params = (("amount", -1),)


# ── A股佣金/印花税 ───────────────────────────────────────────────────────
class CNCommInfo(bt.CommInfoBase):
    """佣金万2.5（最低5元）+ 卖出印花税千0.5，T+1 由 Backtrader 默认撮合保证。"""

    params = (
        ("commission", 0.00025),
        ("comm_min", 5.0),
        ("stamp_duty", 0.0005),
        ("stocklike", True),
    )

    def getcommission(self, size: float, price: float) -> float:
        if size == 0 or price <= 0:
            return 0.0
        comm = abs(size) * price * self.p.commission
        if comm > 0:
            comm = max(comm, self.p.comm_min)
        if size < 0:  # 卖出加印花税
            comm += abs(size) * price * self.p.stamp_duty
        return comm


# ── 多策略融合回测策略 ──────────────────────────────────────────────────
class MultiStrategy(bt.Strategy):
    params = (
        ("strategies", "boll,relativity,theme"),  # 启用的策略（cctv 需外部注入才有效）
        ("boll_period", 20),
        ("boll_k", 1.645),
        ("boll_near_ratio", 1.015),
        ("rel_lookback", 100),
        ("rel_min_up_days", 5),
        ("rel_min_down_days", 5),
        ("rel_up_tol", -0.025),
        ("rel_down_outperf", 0.0),
        ("rel_min_up_ratio", 0.6),
        ("rel_min_down_ratio", 0.7),
        ("theme_near_high_min", 0.9),
        ("theme_ret20_max", 0.6),
        ("theme_vol_ratio_min", 1.2),
        ("max_pos_pct", 0.3),       # 单票上限（与 fusion.py 一致）
        ("max_hold_days", 60),      # 安全退出：超期且无信号则平仓释放资金
        ("weights", None),          # 各策略仓位权重 dict；None 用默认 BASE_SCORE
        ("cctv_hits", {}),          # 外部题材命中：code -> 命中数（cctv 策略启用时生效）
    )

    # 默认策略权重（与 fusion.STRATEGY_BASE_SCORE 对应，归一化为仓位比例）
    DEFAULT_WEIGHTS = {"boll": 0.40, "relativity": 0.25, "theme": 0.20, "cctv": 0.15}

    def __init__(self) -> None:
        self.enabled = {s.strip().lower() for s in str(self.p.strategies).split(",") if s.strip()}
        self.weights = dict(self.p.weights) if self.p.weights else dict(self.DEFAULT_WEIGHTS)

        # 指数数据（用于 relativity），name='idx'
        self.idx = None
        if "relativity" in self.enabled:
            try:
                self.idx = self.getdatabyname("idx")
            except Exception:
                self.idx = None
        self.idx_ret: deque[float] = deque(maxlen=self.p.rel_lookback + 5)

        # 每只个股：布林带指标 + 滚动状态
        self.boll: dict[bt.DataBase, Any] = {}
        self.st: dict[bt.DataBase, dict[str, Any]] = {}
        for d in self.datas:
            if self.idx is not None and d is self.idx:
                continue
            self.boll[d] = bt.indicators.BollingerBands(
                d.close, period=self.p.boll_period, devfactor=self.p.boll_k
            )
            self.st[d] = {
                "close": deque(maxlen=self.p.boll_period + 1),
                "amt": deque(maxlen=6),
                "sret": deque(maxlen=self.p.rel_lookback + 5),
                "iret": deque(maxlen=self.p.rel_lookback + 5),
                "bl_streak": 0,
                "nl_streak": 0,
                "entry_bar": -999,
                "target": 0.0,
                "entry_size": 0,
                "entry_price": None,
                "exit_price": None,
                "entry_hits": [],
            }

        # 外部题材命中（cctv）：code -> 命中数
        self.cctv_hits: dict[str, int] = dict(self.p.cctv_hits) or {}

        # 订单/结果收集
        self.orders: dict[bt.DataBase, Optional[bt.Order]] = {}
        self.value_hist: list[tuple[Any, float, float]] = []  # (date, cash, total)
        self.trades: list[dict[str, Any]] = []

    # ── 工具 ──
    @staticmethod
    def _valid(x) -> bool:
        return x is not None and not (isinstance(x, float) and (np.isnan(x) or x <= 0))

    def _rolling_max(self, q: deque) -> float:
        v = [x for x in q if self._valid(x)]
        return max(v) if v else float("nan")

    def _rolling_mean(self, q: deque, n: int) -> float:
        v = [x for x in list(q)[-n:] if self._valid(x)]
        return float(np.mean(v)) if v else float("nan")

    # ── 各策略信号 ──
    def _boll_signal(self, d, st) -> bool:
        close = d.close[0]
        lower = self.boll[d].lines.bot[0]
        if not self._valid(close) or not self._valid(lower):
            return False
        below = close < lower
        near = (not below) and close <= lower * self.p.boll_near_ratio
        if below:
            st["bl_streak"] = (st["bl_streak"] + 1) if below else 0
        else:
            st["bl_streak"] = 0
        if near:
            st["nl_streak"] += 1
        else:
            st["nl_streak"] = 0
        if below and st["bl_streak"] > 1:
            return False  # 连续超卖去重
        if near and st["nl_streak"] > 1:
            return False
        return below or near

    def _relativity_signal(self, st) -> bool:
        if self.idx is None:
            return False
        sret = list(st["sret"])[-self.p.rel_lookback:]
        iret = list(st["iret"])[-self.p.rel_lookback:]
        if len(sret) < self.p.rel_lookback or len(iret) < self.p.rel_lookback:
            return False
        up_mask = [r > 0 for r in iret]
        down_mask = [r < 0 for r in iret]
        up_days = sum(up_mask)
        down_days = sum(down_mask)
        if up_days < self.p.rel_min_up_days or down_days < self.p.rel_min_down_days:
            return False
        up_ok = sum(1 for s, rm in zip(sret, up_mask) if rm and s >= self.p.rel_up_tol)
        down_ok = sum(
            1 for s, r, dm in zip(sret, iret, down_mask) if dm and (s - r) >= self.p.rel_down_outperf
        )
        up_ratio = up_ok / up_days
        down_ratio = down_ok / down_days
        return up_ratio >= self.p.rel_min_up_ratio and down_ratio >= self.p.rel_min_down_ratio

    def _theme_signal(self, d, st) -> bool:
        if len(st["close"]) < 20:
            return False
        close = d.close[0]
        if not (self.p.boll_period and 5 <= close <= 30):
            return False
        closes = list(st["close"])[-20:]
        near_high = close / self._rolling_max(deque(closes))
        if not self._valid(near_high) or near_high < self.p.theme_near_high_min:
            return False
        if len(closes) >= 21:
            ret20 = close / closes[-21] - 1
        else:
            ret20 = close / closes[0] - 1
        if not (0 < ret20 <= self.p.theme_ret20_max):
            return False
        if len(st["amt"]) >= 5:
            avg_amt5 = self._rolling_mean(st["amt"], 5)
            vol_ratio = (st["amt"][-1] / avg_amt5) if self._valid(avg_amt5) else 0.0
        else:
            vol_ratio = 0.0
        if vol_ratio < self.p.theme_vol_ratio_min:
            return False
        return True

    def _cctv_signal(self, d) -> bool:
        return bool(self.cctv_hits.get(d._name, 0))

    def _compute_hits(self, d, st) -> list[str]:
        hits: list[str] = []
        if "boll" in self.enabled and self._boll_signal(d, st):
            hits.append("boll")
        if "relativity" in self.enabled and self._relativity_signal(st):
            hits.append("relativity")
        if "theme" in self.enabled and self._theme_signal(d, st):
            hits.append("theme")
        if "cctv" in self.enabled and self._cctv_signal(d):
            hits.append("cctv")
        return hits

    # ── 订单管理 ──
    def _order_active(self, d) -> bool:
        o = self.orders.get(d)
        if o is None:
            return False
        return o.status in (bt.Order.Created, bt.Order.Submitted, bt.Order.Accepted, bt.Order.Partial)

    def _manage(self, d, st, hits, bar_idx: int) -> None:
        pos = self.getposition(d).size
        close = d.close[0]
        lower = self.boll[d].lines.bot[0]
        upper = self.boll[d].lines.top[0]

        # 持仓中：止损=下轨 / 止盈=上轨
        if pos > 0:
            if self._valid(lower) and close <= lower:
                if not self._order_active(d):
                    self.orders[d] = self.close(d)
                return
            if self._valid(upper) and close >= upper:
                if not self._order_active(d):
                    self.orders[d] = self.close(d)
                return
            # 安全退出：超期且无任何信号
            if (bar_idx - st["entry_bar"]) >= self.p.max_hold_days and not hits:
                if not self._order_active(d):
                    self.orders[d] = self.close(d)
                return
            return

        # 空仓：计算目标仓位
        target = 0.0
        if hits:
            best_w = max((self.weights.get(s, 0.0) for s in hits), default=0.0)
            target = min(best_w, self.p.max_pos_pct)
        st["target"] = target
        if target <= 0:
            return
        if self._order_active(d):
            return
        # 涨停板不追
        if self._valid(d.close[-1]) and close >= d.close[-1] * 1.095:
            return
        st["entry_hits"] = list(hits)
        st["entry_bar"] = bar_idx
        st["entry_size"] = 0
        st["entry_price"] = None
        st["exit_price"] = None
        self.orders[d] = self.order_target_percent(d, target)

    # ── 主循环 ──
    def next(self) -> None:
        dt = self.datas[0].datetime.date(0)

        # 指数收益入队
        if self.idx is not None and len(self.idx.close) >= 2:
            prev = self.idx.close[-1]
            if self._valid(prev):
                self.idx_ret.append(self.idx.close[0] / prev - 1)

        bar_idx = len(self)
        for d in self.datas:
            if self.idx is not None and d is self.idx:
                continue
            if len(d.close) < self.p.boll_period + 1:
                continue
            if not self._valid(d.close[0]):
                continue
            st = self.st[d]
            st["close"].append(d.close[0])
            if hasattr(d, "amount"):
                st["amt"].append(d.amount[0])
            # 个股日收益
            if len(d.close) >= 2 and self._valid(d.close[-1]):
                st["sret"].append(d.close[0] / d.close[-1] - 1)
            # 相对论需与指数对齐（同根 K 的收益率）
            if self.idx is not None and len(self.idx_ret) >= 1:
                st["iret"].append(self.idx_ret[-1])

            hits = self._compute_hits(d, st)
            self._manage(d, st, hits, bar_idx)

        self.value_hist.append((dt, self.broker.getcash(), self.broker.getvalue()))

    # ── 订单成交记录（用于交易明细真实手数/价格）──
    def notify_order(self, order: bt.Order) -> None:
        d = order.data
        st = self.st.get(d)
        if order.status == bt.Order.Completed:
            if order.isbuy():
                if st is not None:
                    st["entry_size"] = abs(order.executed.size)
                    st["entry_price"] = order.executed.price
            elif order.issell():
                if st is not None:
                    st["exit_price"] = order.executed.price
        if order.status in (
            bt.Order.Completed,
            bt.Order.Canceled,
            bt.Order.Rejected,
            bt.Order.Margin,
        ):
            self.orders[d] = None

    # ── 交易记录 ──
    def notify_trade(self, trade: bt.Trade) -> None:
        if not trade.isclosed:
            return
        code = trade.data._name if hasattr(trade.data, "_name") else "?"
        st = self.st.get(trade.data, {})
        entry_size = st.get("entry_size") or abs(trade.size)
        entry_price = st.get("entry_price") or float(trade.price)
        exit_price = st.get("exit_price") or entry_price
        entry_value = entry_size * entry_price
        ret = (trade.pnlcomm / entry_value * 100.0) if entry_value else 0.0
        entry_hits = st.get("entry_hits", [])
        self.trades.append(
            {
                "code": code,
                "buy_date": bt.num2date(trade.dtopen).strftime("%Y-%m-%d"),
                "sell_date": bt.num2date(trade.dtclose).strftime("%Y-%m-%d"),
                "buy_price": round(float(entry_price), 3),
                "sell_price": round(float(exit_price), 3),
                "qty": int(entry_size),
                "return_pct": round(float(ret), 2),
                "strategies": ",".join(entry_hits),
            }
        )
