#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""TWAP/VWAP 执行算法与执行质量分析（子单调度引擎 + 执行成本建模）。

设计动机
--------
选股 / 组合 / 风控层已产出目标权重（Daily-Action-List 的「建议金额」），但「如何把这笔钱拆成
一天内的若干笔子单、以什么节奏成交、成交成本几何」此前完全没有建模。本模块补齐**执行层**：

1) 子单调度（child-order schedule）：给定订单（方向 / 总股数 / 算法 / 片数），产出 N 片
   等额（TWAP）或按成交量 profile 分布（VWAP）的子单计划，可直接交给券商算法或人工执行。
2) 执行质量（execution quality）：在给定当日 K 线（OHLC + 成交量）下，用**可复现**的合成
   日内路径模拟成交，计算相对 VWAP / TWAP / 到达价的滑点、参与率（POV）、平方根市场冲击
   模型估计的冲击成本、实现缺额（implementation shortfall）。

数据约束（诚实声明）
--------------------
当前仓库只有**日频 K 线**，没有逐笔 / 分钟级真实成交。因此「日内路径」是**模型合成**（按
配置 seed / profile 由当日 OHLC 反推的几何路径），用于回测执行计划的质量与成本；VWAP / TWAP
基准同理。若未来 `stock_data/intraday/` 落地真实分钟数据，`load_intraday` 会自动优先采用真实
路径，合成路径退化为 fallback。所有结果均 fail-soft，数据不足返回中性 / None，绝不抛异常。

全部参数集中在 risk_config.json[execution]，零硬编码。配置见 DEFAULTS。
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from smcore.config.defaults import STOCK_DATA_DIR, PROJECT_ROOT
except Exception:  # pragma: no cover
    STOCK_DATA_DIR = Path("stock_data")
    PROJECT_ROOT = Path(".")

try:
    from smcore.strategy.risk_rules import CONFIG as _RISK_CONFIG
except Exception:  # pragma: no cover
    _RISK_CONFIG = {}

# ── 默认超参（config 可覆盖；与 risk_config.json[execution] 镜像）──────────
DEFAULTS = {
    "enabled": True,
    "default_algo": "VWAP",
    "n_slices": 20,
    "u_shape_alpha": 1.0,    # U 形两端权重（相对 base）
    "u_shape_sigma": 3.0,    # U 形宽度（越小越尖）
    "u_shape_base": 0.15,    # U 形底部基础权重
    "impact_coef": 1.0,      # 平方根冲击模型系数（Kyle 风格）
    "vol_window": 20,        # 日波动率估计窗口
    "min_slice_shares": 100, # 单片最小股数（不足则并入相邻片）
    "max_participation": 0.30,  # 单日最大参与率（POV）上限，超出告警
    "seeds": [0, 1, 2, 3, 4],   # 合成日内路径的多 seed 平均
}


def _cfg(user=None) -> dict:
    c = dict(DEFAULTS)
    c.update(_RISK_CONFIG.get("execution", {}) or {})
    if user:
        c.update(user)
    return c


# A 股交易时段：上午 9:30-11:30（120 分钟），下午 13:00-15:00（120 分钟），共 240 分钟。
_SESSION_MINUTES = 240


def slice_time_labels(n_slices: int):
    """生成 N 片的时间标签（A 股午休对齐）。返回 ['09:30','09:42',...] 字符串列表。"""
    if n_slices <= 0:
        return []
    labels = []
    for k in range(n_slices):
        start = int(round(k * _SESSION_MINUTES / n_slices))
        # 上午段 0-120min -> 09:30 起；下午段 120-240min -> 13:00 起
        if start < 120:
            base_min = 9 * 60 + 30 + start
        else:
            base_min = 13 * 60 + 0 + (start - 120)
        hh = base_min // 60
        mm = base_min % 60
        labels.append(f"{hh:02d}:{mm:02d}")
    return labels


def intraday_volume_profile(n_slices: int, algo: str = "VWAP", cfg: dict = None) -> np.ndarray:
    """N 片成交量分布（求和=1）。

    - TWAP：均匀 1/n
    - VWAP：U 形（开盘 / 收盘放量、午间缩量），由 u_shape_alpha/sigma/base 控制形态。
    """
    if n_slices <= 0:
        return np.array([])
    if algo.upper() == "TWAP":
        return np.full(n_slices, 1.0 / n_slices)
    c = _cfg(cfg)
    alpha = float(c["u_shape_alpha"])
    sigma = float(c["u_shape_sigma"])
    base = float(c["u_shape_base"])
    idx = np.arange(n_slices, dtype=float)
    w = alpha * np.exp(-((idx) / sigma) ** 2) \
        + alpha * np.exp(-((n_slices - 1 - idx) / sigma) ** 2) \
        + base
    s = w.sum()
    if s <= 0:
        return np.full(n_slices, 1.0 / n_slices)
    return w / s


def child_order_schedule(side: str, total_shares: float, algo: str = "VWAP",
                         n_slices: int = None, cfg: dict = None) -> list:
    """产出子单计划。返回 [{slice, time, shares}]；股数按 profile 分布，末片补差。

    fail-soft：total<=0 或 n_slices<=0 返回 []。
    """
    c = _cfg(cfg)
    if n_slices is None:
        n_slices = int(c["n_slices"])
    if total_shares is None or total_shares <= 0 or n_slices <= 0:
        return []
    profile = intraday_volume_profile(n_slices, algo, cfg=c)
    raw = total_shares * profile
    shares = np.floor(raw).astype(int)
    remainder = int(round(total_shares - shares.sum()))
    # 余数加到最大的一片（通常是开盘附近）
    if remainder != 0 and len(shares) > 0:
        shares[int(np.argmax(profile))] += remainder
    labels = slice_time_labels(n_slices)
    out = []
    for i in range(n_slices):
        sh = int(shares[i])
        if sh <= 0:
            continue
        out.append({"slice": i + 1, "time": labels[i] if i < len(labels) else "", "shares": sh})
    return out


# ── 数据读取 ──────────────────────────────────────────────────────────────
def _load_kdata(code: str) -> pd.DataFrame:
    f = STOCK_DATA_DIR / "k_data" / f"{code}_qfq_full.csv"
    if not f.exists():
        return pd.DataFrame()
    try:
        d = pd.read_csv(f)
        if "date" not in d.columns:
            return pd.DataFrame()
        d["date"] = pd.to_datetime(d["date"])
        return d.set_index("date").sort_index()
    except Exception:
        return pd.DataFrame()


def load_intraday(code: str, date_yyyymmdd: str) -> pd.DataFrame | None:
    """尝试读取真实分钟级路径 stock_data/intraday/{code}_{date}.csv。

    列需含 time, price（及可选 volume）。缺失返回 None（触发合成 fallback）。
    """
    f = STOCK_DATA_DIR / "intraday" / f"{code}_{date_yyyymmdd}.csv"
    if not f.exists():
        return None
    try:
        d = pd.read_csv(f)
        if "price" not in d.columns or "time" not in d.columns:
            return None
        return d
    except Exception:
        return None


def simulate_intraday_path(open_p, high_p, low_p, close_p, n_slices: int,
                           seed: int = 0) -> np.ndarray:
    """由 OHLC 反推一条可复现的合成日内价格路径（长度 n_slices）。

    锚定：path[0]=open, path[-1]=close；幅度缩放至覆盖 [low, high]；
    seed 控制抖动形态。fail-soft：价格非法返回全 open 的常值数组。
    """
    try:
        o, h, l, c = float(open_p), float(high_p), float(low_p), float(close_p)
    except Exception:
        return np.full(max(n_slices, 1), float("nan"))
    if n_slices <= 1 or not (h >= l >= 0) or not np.isfinite(o):
        return np.full(max(n_slices, 1), o if np.isfinite(o) else float("nan"))
    rng = np.random.default_rng(int(seed))
    z = np.cumsum(rng.standard_normal(n_slices))
    z = z - z[0]  # 起点归零
    denom = z[-1] if z[-1] != 0 else 1.0
    # 线性拉伸使终点=close
    path = o + z * ((c - o) / denom)
    pmin, pmax = float(path.min()), float(path.max())
    cur_range = pmax - pmin
    if cur_range > 1e-12:
        # 把当前 [pmin,pmax] 线性映射到 [low, high]
        path = l + (path - pmin) / cur_range * (h - l)
    else:
        path = np.full(n_slices, (h + l) / 2.0)
    path[0] = o
    path[-1] = c
    return path


def vwap_proxy_from_bar(bar: dict) -> float | None:
    """由当日 K 线估计市场 VWAP 基准：优先 amount/volume，否则 (H+L+C)/3。"""
    if not isinstance(bar, dict):
        return None
    try:
        amt = bar.get("amount")
        vol = bar.get("volume")
        if amt is not None and vol is not None and vol and float(vol) > 0 and float(amt) > 0:
            v = float(vol)
            a = float(amt)
            if np.isfinite(v) and np.isfinite(a):
                return a / v
        h = bar.get("high"); l = bar.get("low"); c = bar.get("close")
        if None not in (h, l, c):
            return (float(h) + float(l) + float(c)) / 3.0
    except Exception:
        return None
    return None


def _daily_volatility(code: str, as_of=None, window: int = 20) -> float | None:
    kd = _load_kdata(code)
    if kd.empty or "close" not in kd.columns:
        return None
    if as_of is not None:
        try:
            kd = kd.loc[:pd.Timestamp(as_of)]
        except Exception:
            pass
    close = pd.to_numeric(kd["close"], errors="coerce").dropna()
    if len(close) < window + 1:
        return None
    rets = close.pct_change().dropna().tail(window)
    if len(rets) < 5:
        return None
    sd = float(rets.std())
    return sd if np.isfinite(sd) else None


# ── 执行质量评估 ─────────────────────────────────────────────────────────
def evaluate_execution(order: dict, bar: dict = None, cfg: dict = None,
                       daily_vol: float = None) -> dict:
    """对单笔订单评估执行质量（合成日内路径 + 成本模型）。

    order: {side('buy'/'sell'), total_shares, algo, n_slices?, code?, date?, daily_volume?}
    bar:   {open,high,low,close,volume,amount}（当日 K 线；缺失则尽力而为）
    返回 fail-soft dict：含 schedule / vwap_benchmark / arrival / avg_fill_price /
    slippage_vs_vwap_bps / slippage_vs_arrival_bps / participation_rate /
    est_market_impact_bps / total_cost_bps / ok(bool)。
    """
    c = _cfg(cfg)
    side = str(order.get("side", "buy")).lower()
    total = float(order.get("total_shares") or 0.0)
    algo = str(order.get("algo") or c.get("default_algo") or "VWAP").upper()
    n = int(order.get("n_slices") or c["n_slices"])

    result = {
        "ok": False, "side": side, "algo": algo, "total_shares": total,
        "n_slices": n, "schedule": [], "code": order.get("code"),
        "date": order.get("date"),
        "vwap_benchmark": None, "arrival_price": None, "avg_fill_price": None,
        "slippage_vs_vwap_bps": None, "slippage_vs_arrival_bps": None,
        "participation_rate": None, "est_market_impact_bps": None,
        "total_cost_bps": None, "synthetic": True, "n_paths": 0,
    }
    if total <= 0 or n <= 0 or bar is None:
        return result

    try:
        o = float(bar.get("open")); h = float(bar.get("high"))
        l = float(bar.get("low")); cl = float(bar.get("close"))
    except Exception:
        return result
    if not (np.isfinite(o) and np.isfinite(h) and np.isfinite(l) and np.isfinite(cl)):
        return result

    # 1) 调度
    schedule = child_order_schedule(side, total, algo, n, cfg=c)
    if not schedule:
        return result
    result["schedule"] = schedule

    # 2) 日内路径：优先真实 intraday，否则多 seed 合成取均值
    real = None
    if order.get("code") and order.get("date"):
        real = load_intraday(order["code"], order["date"])
    if real is not None and len(real) >= 2:
        prices = pd.to_numeric(real["price"], errors="coerce").dropna().values
        if len(prices) >= n:
            # 重采样到 n 片（按索引均匀取点）
            idx = np.linspace(0, len(prices) - 1, n).astype(int)
            path = prices[idx]
            result["synthetic"] = False
        else:
            path = prices
            result["synthetic"] = False
        # 真实成交量占比（若提供）
        if "volume" in real.columns:
            vv = pd.to_numeric(real["volume"], errors="coerce").fillna(0).values
            if vv.sum() > 0:
                ridx = np.linspace(0, len(vv) - 1, n).astype(int) if len(vv) >= n else range(len(vv))
                prof = vv[list(ridx)]
                prof = prof / prof.sum() if prof.sum() > 0 else np.full(n, 1.0 / n)
            else:
                prof = intraday_volume_profile(n, algo, cfg=c)
        else:
            prof = intraday_volume_profile(n, algo, cfg=c)
    else:
        seeds = c.get("seeds") or [0]
        paths = [simulate_intraday_path(o, h, l, cl, n, s) for s in seeds]
        paths = [p for p in paths if np.all(np.isfinite(p))]
        if not paths:
            return result
        path = np.mean(paths, axis=0)
        prof = intraday_volume_profile(n, algo, cfg=c)
        result["n_paths"] = len(paths)

    # 3) 基准与成交均价
    vwap_bench = vwap_proxy_from_bar(bar)
    arrival = o
    # 加权均价：该算法按其 profile 成交，故均价 = Σ profile_i * path_i
    w = prof / prof.sum()
    avg_fill = float(np.dot(w, path))

    result["vwap_benchmark"] = round(vwap_bench, 4) if vwap_bench is not None else None
    result["arrival_price"] = round(arrival, 4)
    result["avg_fill_price"] = round(avg_fill, 4)

    # 4) 滑点（cost 为正=不利方向）
    #    buy：成交价越高成本越大；sell：成交价越低成本越大
    if vwap_bench and vwap_bench > 0:
        if side == "sell":
            sv = (1.0 - avg_fill / vwap_bench) * 1e4
        else:
            sv = (avg_fill / vwap_bench - 1.0) * 1e4
        result["slippage_vs_vwap_bps"] = round(float(sv), 2)
    if arrival and arrival > 0:
        if side == "sell":
            sa = (1.0 - avg_fill / arrival) * 1e4
        else:
            sa = (avg_fill / arrival - 1.0) * 1e4
        result["slippage_vs_arrival_bps"] = round(float(sa), 2)

    # 5) 参与率 POV 与冲击
    dv = order.get("daily_volume") if order.get("daily_volume") else bar.get("volume")
    if dv and float(dv) > 0:
        pov = total / float(dv)
        result["participation_rate"] = round(float(pov), 4)
        if daily_vol is None and order.get("code"):
            daily_vol = _daily_volatility(order["code"], order.get("date"),
                                          window=int(c["vol_window"]))
        if daily_vol and daily_vol > 0:
            impact = float(c["impact_coef"]) * np.sqrt(max(pov, 0.0)) * daily_vol * 1e4
            result["est_market_impact_bps"] = round(impact, 2)

    # 6) 总成本 = 相对市场 VWAP 基准的实现成本（headline 执行质量指标）。
    #    冲击(impact) 是模型估计的「不可避免」部分，单独列出；超额成本 = 总成本 - 冲击。
    if result["slippage_vs_vwap_bps"] is not None:
        result["total_cost_bps"] = result["slippage_vs_vwap_bps"]
        if result["est_market_impact_bps"] is not None:
            excess = result["slippage_vs_vwap_bps"] - result["est_market_impact_bps"]
            result["excess_vs_impact_bps"] = round(float(excess), 2)

    result["ok"] = True
    return result


# ── 执行计划落盘（交券商/人工）──────────────────────────────────────────
def write_execution_plan(order: dict, cfg: dict = None) -> str | None:
    """把子单计划写成 stock_data/execution_plans/{date}_{code}_{algo}.csv。"""
    c = _cfg(cfg)
    code = order.get("code", "NA")
    date = order.get("date", "NA")
    algo = str(order.get("algo") or c.get("default_algo") or "VWAP").upper()
    sched = child_order_schedule(order.get("side", "buy"),
                                 float(order.get("total_shares") or 0),
                                 algo, int(order.get("n_slices") or c["n_slices"]), cfg=c)
    if not sched:
        return None
    d = STOCK_DATA_DIR / "execution_plans"
    try:
        d.mkdir(parents=True, exist_ok=True)
    except Exception:
        return None
    fp = d / f"{date}_{code}_{algo}.csv"
    try:
        pd.DataFrame(sched).to_csv(fp, index=False, encoding="utf-8-sig")
        return str(fp)
    except Exception:
        return None


# ── 端到端报告 ───────────────────────────────────────────────────────────
def run_execution_report(orders=None, as_of=None, cfg: dict = None) -> dict:
    """对一组订单跑执行质量；orders 缺省则从最新 Daily-Action-List 的「建议金额」推导。

    每条 DAL 记录：code + 建议金额 → total_shares = 建议金额 / 当日收盘；
    side 默认 buy（建仓）。返回 {ok, n_orders, items[], warnings[]}。
    """
    c = _cfg(cfg)
    items = []
    warnings = []

    if orders is None:
        derived = []
        dals = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))
        if not dals:
            return {"ok": False, "reason": "无 Daily-Action-List", "n_orders": 0,
                    "items": [], "warnings": []}
        dal = dals[-1]
        date = None
        try:
            d = pd.read_csv(dal, encoding="utf-8-sig")
            if "股票代码" in d.columns and "建议金额" in d.columns:
                for _, r in d.iterrows():
                    code = str(r["股票代码"])
                    amt = float(r.get("建议金额") or 0)
                    if amt <= 0:
                        continue
                    kd = _load_kdata(code)
                    if kd.empty:
                        warnings.append(f"{code}: 无 K 线，跳过")
                        continue
                    close = pd.to_numeric(kd["close"], errors="coerce").dropna()
                    if close.empty or close.iloc[-1] <= 0:
                        warnings.append(f"{code}: 收盘价无效，跳过")
                        continue
                    px = float(close.iloc[-1])
                    total = int(round(amt / px / 100.0)) * 100  # 取整到百股
                    d0 = pd.Timestamp(kd.index[-1]).strftime("%Y%m%d")
                    date = date or d0
                    vol = None
                    if "volume" in kd.columns:
                        vv = pd.to_numeric(kd["volume"], errors="coerce").dropna()
                        if not vv.empty:
                            vol = float(vv.iloc[-1])
                    derived.append({"side": "buy", "total_shares": total,
                                    "code": code, "date": d0, "daily_volume": vol})
        except Exception as e:
            return {"ok": False, "reason": f"读取 DAL 失败: {e}", "n_orders": 0,
                    "items": [], "warnings": warnings}
        orders = derived

    if not orders:
        return {"ok": False, "reason": "无可用订单", "n_orders": 0,
                "items": [], "warnings": warnings}

    for od in orders:
        bar = None
        code = od.get("code")
        if code:
            kd = _load_kdata(code)
            if not kd.empty:
                row = kd.iloc[-1]
                bar = {"open": row.get("open"), "high": row.get("high"),
                       "low": row.get("low"), "close": row.get("close"),
                       "volume": row.get("volume"), "amount": row.get("amount")}
        # 显式传入的 bar 优先
        bar = od.get("bar") or bar
        dv = od.get("daily_volume") if od.get("daily_volume") else (bar or {}).get("volume")
        daily_vol = None
        if code and dv:
            daily_vol = _daily_volatility(code, od.get("date"), window=int(c["vol_window"]))
        res = evaluate_execution({**od, "daily_volume": dv}, bar=bar, cfg=cfg or c,
                                 daily_vol=daily_vol)
        if res.get("ok"):
            items.append(res)
        else:
            warnings.append(f"{code or '?'}: 执行评估失败（数据不足）")

    # 参与率超限告警
    max_pov = float(c.get("max_participation", 0.3))
    for it in items:
        pov = it.get("participation_rate")
        if pov is not None and pov > max_pov:
            warnings.append(
                f"{it.get('code')}: 参与率 {pov:.1%} 超过上限 {max_pov:.0%}，建议拆多日")

    return {"ok": len(items) > 0, "n_orders": len(items), "items": items,
            "warnings": warnings, "as_of": as_of}


def format_execution_report(res: dict) -> str:
    if not res.get("ok"):
        return ("# TWAP/VWAP 执行质量报告\n\n"
                f"⚠️ 无法生成：{res.get('reason', '')}\n")
    lines = [
        "# TWAP/VWAP 执行质量报告",
        "",
        f"- 订单数：**{res['n_orders']}**；基准=当日 VWAP 代理（amount/volume）；"
        "日内路径=合成模型（无真实分钟数据）",
        "",
        "| 标的 | 方向 | 算法 | 总股数 | 成交均价 | VWAP基准 | 滑点(vs VWAP) | 滑点(vs到达) | 参与率 | 冲击(bps) | 超额(bps) |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for it in res["items"]:
        sv = it.get("slippage_vs_vwap_bps")
        sa = it.get("slippage_vs_arrival_bps")
        pov = it.get("participation_rate")
        imp = it.get("est_market_impact_bps")
        excess = it.get("excess_vs_impact_bps")
        lines.append(
            f"| {it.get('code','?')} | {it.get('side')} | {it.get('algo')} | "
            f"{it.get('total_shares'):,.0f} | {it.get('avg_fill_price')} | "
            f"{it.get('vwap_benchmark')} | {sv if sv is not None else '—'} | "
            f"{sa if sa is not None else '—'} | "
            f"{pov if pov is not None else '—'} | {imp if imp is not None else '—'} | "
            f"{excess if excess is not None else '—'} |")
    if res.get("warnings"):
        lines.append("")
        lines.append("### 告警")
        for w in res["warnings"]:
            lines.append(f"- {w}")
    lines.append("")
    lines.append("> 总成本 = 相对市场 VWAP 基准的实现成本（bps，正=不利）；"
                 "冲击 = 平方根市场冲击模型估计的不可避免成本（coef·√POV·日波动率）；"
                 "超额 = 总成本 − 冲击（负=优于冲击预期，正=执行劣于冲击预期）。"
                 "合成路径仅供回测执行计划质量，真实落地需券商算法 / 分钟成交数据。")
    return "\n".join(lines) + "\n"
