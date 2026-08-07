#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Barra 风格风险模型（轻量离线版）。

把个股收益分解为「风格因子暴露 × 因子收益 + 特异收益」，据此估计：
- 因子协方差矩阵 F（由历史日截面 FM 回归得到因子收益序列后求协方差）
- 个股特异方差（回归残差的时序方差）
- 给定组合权重 w，预测组合波动 = sqrt(w'·B·F·B'·w + Σ w_i²·u_i)

风格因子（均可由本地数据离线计算，无需联网）：
  size       规模   = log(总市值)            (fundamental_cache.mkt_cap)
  value      价值   = -z(PE) - z(PB)        (越低估暴露越高)
  momentum   动量   = 过去 20 日收益率       (k_data close)
  volatility 波动   = 过去 20 日收益波动率    (k_data close)
  liquidity  流动性 = 过去 20 日均成交额(对数) (k_data amount)

所有计算 fail-soft：数据不足/缺失返回中性值，绝不抛异常。配置见 risk_config.json[barra]。
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from smcore.config.defaults import STOCK_DATA_DIR
except Exception:  # pragma: no cover
    STOCK_DATA_DIR = Path("stock_data")

# 风格因子（顺序即矩阵列序）
STYLE_FACTORS = ["size", "value", "momentum", "volatility", "liquidity"]

_ANNUAL = 252


# ── 数据读取 ────────────────────────────────────────────────────────────
def _load_kdata(code: str) -> pd.DataFrame:
    """本地前复权 K 线（date 索引，close/amount）。缺文件/损坏返回空。"""
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


def _load_fundamental(code: str) -> dict:
    """本地基本面缓存 {pe, pb, mkt_cap}。缺失返回 {}。"""
    f = STOCK_DATA_DIR / "fundamental_cache" / f"{code}.json"
    if not f.exists():
        return {}
    try:
        return json.loads(f.read_text(encoding="utf-8"))
    except Exception:
        return {}


# ── 因子原始值 ───────────────────────────────────────────────────────────
def compute_factor_values(code: str, as_of=None) -> dict:
    """返回单只股票在 as_of 时的各因子原始值；无法计算为 NaN。"""
    out = {f: float("nan") for f in STYLE_FACTORS}
    kd = _load_kdata(code)
    if as_of is not None and not kd.empty:
        try:
            kd = kd.loc[:pd.Timestamp(as_of)]
        except Exception:
            pass
    if not kd.empty and "close" in kd.columns:
        close = pd.to_numeric(kd["close"], errors="coerce").dropna()
        if len(close) >= 21:
            mom = close.iloc[-1] / close.iloc[-21] - 1.0
            rets = close.pct_change().dropna()
            vol = rets.tail(20).std() * np.sqrt(_ANNUAL) if len(rets) >= 20 else float("nan")
            out["momentum"] = float(mom)
            out["volatility"] = float(vol) if vol == vol else float("nan")
        if "amount" in kd.columns:
            amt = pd.to_numeric(kd["amount"], errors="coerce").dropna()
            if len(amt) >= 20:
                avg_amt = amt.tail(20).mean()
                if avg_amt and avg_amt > 0:
                    out["liquidity"] = float(np.log(avg_amt))
    fund = _load_fundamental(code)
    if fund:
        mc = fund.get("mkt_cap")
        if mc and mc == mc and mc > 0:
            out["size"] = float(np.log(mc))
        pe = fund.get("pe")
        pb = fund.get("pb")
        # 价值：低 PE / 低 PB 暴露更高 → 取负；仅当有效时可被截面 z 化
        if pe and pe == pe and pe > 0:
            out["_pe"] = float(pe)
        if pb and pb == pb and pb > 0:
            out["_pb"] = float(pb)
    return out


def compute_exposures(codes, as_of=None) -> pd.DataFrame:
    """截面 z 分数暴露。返回 DataFrame[code, factor]。缺失值按列均值填充后 z 化。

    value 因子由 -z(PE) - z(PB) 合成（需截面内有 >=2 个有效 PE/PB 才能 z 化）。
    """
    codes = [c for c in codes if c]
    if not codes:
        return pd.DataFrame(columns=STYLE_FACTORS)
    raw = {c: compute_factor_values(c, as_of) for c in codes}
    df = pd.DataFrame.from_dict(raw, orient="index")
    # value 合成：截面内对 _pe/_pb 做 z，再取负求和
    for col in ("_pe", "_pb"):
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
            mu, sd = s.mean(), s.std()
            df[col + "_z"] = (s - mu) / sd if (sd and sd == sd and sd > 0) else 0.0
    if "_pe_z" in df.columns and "_pb_z" in df.columns:
        df["value"] = -(df["_pe_z"].fillna(0) + df["_pb_z"].fillna(0)) / 2.0
    elif "_pe_z" in df.columns:
        df["value"] = -df["_pe_z"].fillna(0)
    elif "_pb_z" in df.columns:
        df["value"] = -df["_pb_z"].fillna(0)
    # 其余因子 z 化（截面）
    for f in STYLE_FACTORS:
        if f in ("value",):
            continue
        s = pd.to_numeric(df[f], errors="coerce")
        mu, sd = s.mean(), s.std()
        df[f] = (s - mu) / sd if (sd and sd == sd and sd > 0) else 0.0
    return df[STYLE_FACTORS].fillna(0.0)


# ── 风险模型估计（FM 时序回归）─────────────────────────────────────────
def estimate_risk_model(codes, window: int = 60, end=None, min_history: int = 30):
    """估计因子协方差 F 与特异方差 specific_var。

    对每个交易日 t（窗口内），用当日截面暴露 B_t 回归当日截面收益 r_t 得因子收益 f_t；
    堆叠 f_t 求协方差 F；残差 u 的时序方差即 specific_var。
    返回 dict: {factors, F(np.ndarray), specific_var(dict), n_days}；数据不足返回 None。
    """
    codes = [c for c in codes if c]
    if len(codes) < 3:
        return None
    # 对齐收盘价到共同交易日索引
    closes = {}
    for c in codes:
        kd = _load_kdata(c)
        if not kd.empty and "close" in kd.columns:
            closes[c] = pd.to_numeric(kd["close"], errors="coerce").dropna()
    if len(closes) < 3:
        return None
    panel = pd.DataFrame(closes)
    panel = panel.dropna(how="all")
    if end is not None:
        try:
            panel = panel.loc[:pd.Timestamp(end)]
        except Exception:
            pass
    if panel.shape[0] < min_history:
        return None
    if window and panel.shape[0] > window:
        panel = panel.tail(window)
    rets = panel.pct_change().dropna(how="all")
    if rets.shape[0] < min_history:
        return None

    fidx = list(STYLE_FACTORS)
    F_rows = []
    specific = {c: [] for c in rets.columns}
    for t in rets.index:
        day_codes = [c for c in rets.columns if pd.notna(rets.loc[t, c])]
        if len(day_codes) < 3:
            continue
        B = compute_exposures(day_codes, as_of=str(pd.Timestamp(t).date()))
        B = B.reindex(day_codes)
        if B.isnull().values.any():
            continue
        r = np.array([rets.loc[t, c] for c in day_codes], dtype=float)
        Bm = B.values.astype(float)
        # 岭回归稳定：Bm^T Bm + λI
        lam = 1e-6
        BtB = Bm.T @ Bm + lam * np.eye(Bm.shape[1])
        Btr = Bm.T @ r
        try:
            f = np.linalg.solve(BtB, Btr)
        except Exception:
            continue
        F_rows.append(f)
        u = r - Bm @ f
        for i, c in enumerate(day_codes):
            specific[c].append(u[i])
    if len(F_rows) < min_history // 2:
        return None
    F_mat = np.cov(np.array(F_rows), rowvar=False)
    if F_mat.ndim == 0:
        F_mat = F_mat.reshape(1, 1)
    specific_var = {}
    for c, us in specific.items():
        if len(us) >= 5:
            specific_var[c] = float(np.var(us))
    return {
        "factors": fidx,
        "F": F_mat,
        "specific_var": specific_var,
        "n_days": len(F_rows),
    }


# ── 组合风险 ───────────────────────────────────────────────────────────
def portfolio_risk(weights: dict, risk_model: dict, as_of=None) -> dict:
    """给定权重(代码->比例, 不必归一)与风险模型，预测组合波动与因子风险贡献。

    返回 fail-soft dict：pred_vol_pct / factor_exposure(dict) / factor_risk_pct /
    specific_risk_pct / risk_contrib(dict) / ok(bool)。
    """
    result = {
        "ok": False, "pred_vol_pct": None, "factor_exposure": {},
        "factor_risk_pct": None, "specific_risk_pct": None,
        "risk_contrib": {}, "n_factors": 0,
    }
    if not risk_model or "F" not in risk_model:
        return result
    codes = [c for c in weights if weights[c] and not np.isnan(weights[c])]
    if not codes:
        return result
    tot = sum(float(weights[c]) for c in codes) or 1.0
    w = np.array([float(weights[c]) / tot for c in codes], dtype=float)
    expo = compute_exposures(codes, as_of=as_of).reindex(codes)
    if expo.isnull().values.any():
        expo = expo.fillna(0.0)
    Bp = expo.values.astype(float).T @ w  # 组合因子暴露向量
    F = np.array(risk_model["F"], dtype=float)
    fac_var = float(Bp @ F @ Bp)  # 日度
    sv = risk_model.get("specific_var", {})
    spec_var = float(sum((w[i] ** 2) * sv.get(codes[i], 0.0) for i in range(len(codes))))
    if fac_var < 0:
        fac_var = 0.0
    if spec_var < 0:
        spec_var = 0.0
    daily_var = fac_var + spec_var
    if daily_var <= 0:
        return result
    pred_vol = np.sqrt(daily_var) * np.sqrt(_ANNUAL) * 100.0
    # 因子风险贡献（占组合方差比例，各因子合计 = 因子风险占比 factor_share）
    contrib = {}
    if fac_var > 0:
        FBp = F @ Bp
        for k, name in enumerate(risk_model["factors"]):
            contrib[name] = float(Bp[k] * FBp[k] / daily_var)
    result.update({
        "ok": True,
        "pred_vol_pct": round(pred_vol, 2),
        "factor_exposure": {n: round(float(Bp[k]), 3) for k, n in enumerate(risk_model["factors"])},
        "factor_risk_pct": round(np.sqrt(fac_var) * np.sqrt(_ANNUAL) * 100.0, 2),
        "specific_risk_pct": round(np.sqrt(spec_var) * np.sqrt(_ANNUAL) * 100.0, 2),
        "factor_share_pct": round(fac_var / daily_var * 100.0, 2),
        "specific_share_pct": round(spec_var / daily_var * 100.0, 2),
        "risk_contrib": {k: round(v, 3) for k, v in contrib.items()},
        "n_factors": len(risk_model["factors"]),
    })
    return result


def run_risk_model_report(codes=None, weights=None, as_of=None, window: int = 60) -> dict:
    """对给定组合（默认读最新 Daily-Action-List 的权重）跑风险模型并产出报告 dict。"""
    if codes is None:
        # 取最新 DAL
        dals = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))
        if not dals:
            return {"ok": False, "reason": "无 Daily-Action-List"}
        dal = dals[-1]
        try:
            d = pd.read_csv(dal, encoding="utf-8-sig")
            codes = d["股票代码"].astype(str).tolist()
            if weights is None and "建议金额" in d.columns:
                weights = {str(r["股票代码"]): float(r.get("建议金额") or 0)
                           for _, r in d.iterrows()}
        except Exception:
            return {"ok": False, "reason": "读取 DAL 失败"}
    if weights is None:
        weights = {c: 1.0 for c in codes}
    rm = estimate_risk_model(codes, window=window, end=as_of)
    if rm is None:
        return {"ok": False, "reason": "数据不足，无法估计风险模型", "codes": codes}
    risk = portfolio_risk(weights, rm, as_of=as_of)
    return {"ok": True, "signal_date": as_of, "n_codes": len(codes),
            "model_days": rm["n_days"], "risk": risk}


def format_risk_report(res: dict) -> str:
    if not res.get("ok"):
        return "# Barra 风格风险模型报告\n\n⚠️ 无法生成：" + str(res.get("reason", "")) + "\n"
    r = res["risk"]
    lines = [
        "# Barra 风格风险模型报告（组合预测波动）",
        "",
        f"- 标的：{res['n_codes']} 只；因子收益样本：{res['model_days']} 日",
        f"- **预测组合年化波动：{r['pred_vol_pct']:+.2f}%**"
        f"（因子风险 {r['factor_risk_pct']:+.2f}% + 特异风险 {r['specific_risk_pct']:+.2f}%）",
        "",
        "### 组合风格暴露（截面 z，正=超配该风格）",
        "",
        "| 因子 | 暴露 | 风险贡献% |",
        "|---|---|---|",
    ]
    for f in r["factor_exposure"]:
        contrib = r["risk_contrib"].get(f, 0.0) * 100.0
        lines.append(f"| {f} | {r['factor_exposure'][f]:+.3f} | {contrib:+.1f}% |")
    lines.append("")
    lines.append(f"> 暴露为截面 z 分数；因子风险贡献合计 = 因子风险占比 "
                 f"**{r['factor_share_pct']:.1f}%**（特异风险占比 {r['specific_share_pct']:.1f}%）。")
    return "\n".join(lines) + "\n"
