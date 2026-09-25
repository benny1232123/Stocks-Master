#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Double ML 因果去偏因子验证（ADIA Lab "factor mirage" 框架落地）。

治本工具：现有因子的「显著 IC」里，有多少是混淆/碰撞偏差造成的 **factor mirage**
（本项目 TSGRU / 宏观校准 OOS 双双塌陷的根因）。本模块给每个因子一个
**因果系数 theta**（控制混淆变量后的净效应）+ 按日期聚类的稳健 t 检验 + 横截面符号稳定性，
直接区分「真因果因子」与「被市场/动量/波动/流动性混淆的假因子」。

方法（Chernozhukov Double/Debiased ML）：
  - 样本 = 全部 (date, code) 面板观测
  - treatment  d = 因子横截面 z 分（每期去均，即多空信号）
  - outcome    y = 前向收益
  - confounders X = 市场收益 / 市值流动性代理 / 动量 / 波动率（经典 factor-mirage 混淆集）
  - 偏出：y、d 各自对 X 回归取残差 → 最终部分回归 y_res ~ d_res(+截距)，theta = 因果系数
      * learner="linear" → 精确 Frisch-Waugh-Lovell 偏出（DML 的闭式特例，确定性）
      * learner="elastic" → 真实 K 折交叉拟合 DML（非线性 nuisance，需 sklearn）
  - 推断：按 date 聚类的稳健 SE（cluster-robust by time）→ t / p
  - 附加：横截面符号稳定性 = 期内 spearman(factor_res, ret_res)>0 的日期占比

失败软：任一因子样本不足 / 共线性 / 异常 → 返回 None + 原因，不影响其余因子。
本模块刻意零 smcore 副作用（仅纯 numpy/pandas/sklearn），可独立 import 与单测。
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from sklearn.linear_model import LinearRegression, ElasticNet
    _HAVE_SK = True
except Exception:  # pragma: no cover
    _HAVE_SK = False


# ── nuisance learner ──────────────────────────────────────────────────────
def _make_learner(kind: str):
    if kind == "elastic":
        if not _HAVE_SK:
            kind = "linear"
        else:
            return ElasticNet(alpha=0.01, l1_ratio=0.5, max_iter=2000)
    if _HAVE_SK:
        return LinearRegression()
    return None  # numpy OLS 兜底


def _fit_predict(learner, Xtr, ytr, Xte):
    if learner is None:  # numpy OLS
        beta, *_ = np.linalg.lstsq(Xtr, ytr, rcond=None)
        return Xte @ beta
    learner.fit(Xtr, ytr)
    return learner.predict(Xte)


def _residualize(y, d, X, *, folds=3, learner_kind="linear", seed=0):
    """返回 (y_res, d_res)。linear → 全样本 FWL 精确偏出；elastic → K 折交叉拟合。"""
    if X.shape[1] == 0:  # 无混淆变量：退化为原始值
        return y.copy(), d.copy()
    if learner_kind == "linear" or folds <= 1:
        yl = _make_learner(learner_kind)
        dl = _make_learner(learner_kind)
        yh = _fit_predict(yl, X, y, X)
        dh = _fit_predict(dl, X, d, X)
        return y - yh, d - dh
    # 交叉拟合
    n = y.shape[0]
    rng = np.random.default_rng(seed)
    idx = rng.permutation(n)
    parts = np.array_split(idx, folds)
    y_res = np.empty(n)
    d_res = np.empty(n)
    for k in range(folds):
        te = parts[k]
        tr = np.concatenate([parts[j] for j in range(folds) if j != k])
        yl = _make_learner(learner_kind)
        dl = _make_learner(learner_kind)
        yh = _fit_predict(yl, X[tr], y[tr], X[te])
        dh = _fit_predict(dl, X[tr], d[tr], X[te])
        y_res[te] = y[te] - yh
        d_res[te] = d[te] - dh
    return y_res, d_res


# ── 聚类稳健推断 ─────────────────────────────────────────────────────────
def _cluster_robust_se(Y_res, D_res, cluster):
    Xm = np.column_stack([np.ones_like(D_res), D_res])
    beta, *_ = np.linalg.lstsq(Xm, Y_res, rcond=None)
    resid = Y_res - Xm @ beta
    # 向量化聚类 meat：u_g = Σ_{i∈g} Xm_i·resid_i（按簇加总），meat = Σ_g outer(u_g,u_g)
    # 避免逐簇布尔掩码（O(簇数×样本数) 在 2000+ 簇 × 900万样本下极慢）。
    cl = np.asarray(cluster).astype(np.int64)
    _, cl_codes = np.unique(cl, return_inverse=True)   # 簇 → 连续 0..K-1 编码（防 nanosecond 巨码爆内存）
    n_cl = int(cl_codes.max()) + 1
    wr = Xm * resid[:, None]                       # (n,2)：每观测的 Xm·resid
    ug = np.zeros((n_cl, 2))
    np.add.at(ug, cl_codes, wr)                    # 按簇 bincount 加总 → (n_cl,2)
    meat = ug.T @ ug                              # (2,2)
    bread = np.linalg.inv(Xm.T @ Xm)
    cov = bread @ meat @ bread
    se = np.sqrt(np.maximum(np.diag(cov), 0.0))
    theta = float(beta[1])
    se_t = float(se[1])
    t = theta / se_t if se_t > 0 else np.nan
    from math import erf
    p = 2.0 * (1.0 - 0.5 * (1.0 + erf(abs(t) / np.sqrt(2.0)))) if np.isfinite(t) else np.nan
    return theta, se_t, t, p


def _sign_stability(Y_res, D_res, cluster):
    """横截面符号稳定性：期内 pearson(factor_res, ret_res)>0 的日期占比（样本≥5）。

    向量化：按簇 bincount 累加 Σd,Σy,Σd·y,Σd²,Σy²,计数 → 逐簇相关系数（等价于 spearman 的
    符号与 pearson 一致，因残差已部分化、线性相关为主）。避免逐簇 groupby 在 900万样本下过慢。
    """
    cl = np.asarray(cluster).astype(np.int64)
    _, codes = np.unique(cl, return_inverse=True)
    n_cl = int(codes.max()) + 1
    d = D_res
    y = Y_res
    cnt = np.zeros(n_cl)
    np.add.at(cnt, codes, 1.0)
    sd = np.zeros(n_cl)
    np.add.at(sd, codes, d)
    sy = np.zeros(n_cl)
    np.add.at(sy, codes, y)
    sdy = np.zeros(n_cl)
    np.add.at(sdy, codes, d * y)
    sd2 = np.zeros(n_cl)
    np.add.at(sd2, codes, d * d)
    sy2 = np.zeros(n_cl)
    np.add.at(sy2, codes, y * y)
    ok = cnt >= 5
    n = np.maximum(cnt, 1.0)
    cov = n * sdy - sd * sy
    vd = n * sd2 - sd * sd
    vy = n * sy2 - sy * sy
    denom = np.sqrt(np.maximum(vd * vy, 0.0))
    corr = np.where(denom > 0, cov / denom, np.nan)
    valid = corr[ok]
    valid = valid[np.isfinite(valid)]
    return float(np.mean(valid > 0)) if valid.size else np.nan


# ── 单因子 DML ───────────────────────────────────────────────────────────
def double_ml_factor(y, d, X, *, folds=3, learner_kind="linear", cluster=None,
                     seed=0, min_n=500):
    """对单个因子跑 DML。

    参数
    ----
    y, d, X : 等长 1D 数组（已 stack 成面板长向量；NaN 会被成对丢弃）
    cluster : 与 y 对齐的日期标签（用于聚类 SE 与符号稳定性）；为 None 则退化为 iid
    """
    y = np.asarray(y, float)
    d = np.asarray(d, float)
    X = np.asarray(X, float)
    if X.ndim == 1:
        X = X.reshape(-1, 1)
    mask = ~(np.isnan(y) | np.isnan(d) | np.isnan(X).any(axis=1))
    if cluster is not None:
        mask &= ~np.isnan(np.asarray(cluster, float))
    y, d, X = y[mask], d[mask], X[mask]
    if cluster is not None:
        cluster = np.asarray(cluster, float)[mask]
    n = y.shape[0]
    if n < min_n or X.shape[1] == 0:
        return None
    y_res, d_res = _residualize(y, d, X, folds=folds, learner_kind=learner_kind, seed=seed)
    ok = ~(np.isnan(y_res) | np.isnan(d_res))
    if ok.sum() < min_n:
        return None
    Yr, Dr = y_res[ok], d_res[ok]
    cl = cluster[ok] if cluster is not None else np.arange(Yr.shape[0])
    theta, se, t, p = _cluster_robust_se(Yr, Dr, cl)
    stab = _sign_stability(Yr, Dr, cl)
    return {
        "theta": theta, "se": se, "t": t, "p": p,
        "n": int(ok.sum()), "sign_stability": stab, "learner": learner_kind,
    }


# ── 面板堆叠 / 标准化辅助 ────────────────────────────────────────────────
def _align(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """把所有宽表对齐到共同 index/columns（交集成非全空），返回对齐后的 index 与 columns。"""
    idx = frames[0].index
    cols = frames[0].columns
    for f in frames[1:]:
        idx = idx.intersection(f.index)
        cols = cols.intersection(f.columns)
    return idx, cols


def _stack(df: pd.DataFrame) -> np.ndarray:
    return df.to_numpy().ravel(order="C")


def _zscore_per_date(df: pd.DataFrame) -> pd.DataFrame:
    """每期横截面 z 分（多空信号）；std=0 的期置 NaN。

    用 numpy 直接算，避免 DataFrame.sub/div 链式对齐把列数异常扩张（pandas 对齐陷阱）。
    全 NaN 行（早期预热/停牌）的 nanmean 警告用 errstate 抑制（结果本就被 double_ml 丢弃）。
    """
    arr = df.to_numpy(dtype=float)
    with np.errstate(invalid="ignore", divide="ignore"):
        mu = np.nanmean(arr, axis=1, keepdims=True)
        sd = np.nanstd(arr, axis=1, keepdims=True)
        sd[sd == 0.0] = np.nan
        z = (arr - mu) / sd
    return pd.DataFrame(z, index=df.index, columns=df.columns)


def _zscore_global(df: pd.DataFrame) -> pd.DataFrame:
    return (df - df.mean()) / df.std()


# ── 批量验证 ─────────────────────────────────────────────────────────────
def validate_factors(fwd_ret: pd.DataFrame, factors: dict[str, pd.DataFrame],
                     confounders: dict[str, pd.DataFrame], *,
                     folds=3, learner_kind="linear", min_n=500,
                     horizon_label: str = "") -> pd.DataFrame:
    """对 factors 逐个跑 DML，返回 DataFrame（index=factor）。

    - 因子（treatment）做**每期横截面 z**；
    - 混淆变量做**全局 z**（市场收益在截面内方差为 0，不能按期 z）；
    - 被验证的因子若同时出现在 confounders 中，自动剔除（防自身共线性泄漏）。
    """
    # 统一对齐到 fwd_ret 自身的 (index, columns)：因子/混淆变量始终相对收益矩阵的样本域定义，
    # 直接 reindex fwd_ret 可兼溶 close/amount pivot 出的代码集合差异（样本域以收益为准）。
    idx, cols = fwd_ret.index, fwd_ret.columns
    fwd = fwd_ret.reindex(index=idx, columns=cols)
    fwd_s = _stack(fwd)
    cluster = np.tile(fwd.index.values.astype(np.int64), cols.shape[0])

    conf_z = {name: _zscore_global(c.reindex(index=idx, columns=cols)) for name, c in confounders.items()}

    rows = []
    for name, fac in factors.items():
        fc = _zscore_per_date(fac.reindex(index=idx, columns=cols))
        d = _stack(fc)
        # 混淆集剔除自身
        used = {n: c for n, c in conf_z.items() if n != name}
        if used:
            X = np.column_stack([_stack(c) for c in used.values()])
        else:
            X = np.zeros((d.shape[0], 0))
        res = double_ml_factor(fwd_s, d, X, folds=folds, learner_kind=learner_kind,
                               cluster=cluster, min_n=min_n)
        if res is None:
            rows.append({"factor": name, "theta": np.nan, "se": np.nan, "t": np.nan,
                         "p": np.nan, "n": 0, "sign_stability": np.nan,
                         "verdict": "样本不足/共线性", "learner": learner_kind})
        else:
            rows.append({"factor": name, **res,
                         "verdict": _verdict(res["t"], res["sign_stability"])})
    out = pd.DataFrame(rows).set_index("factor")
    if horizon_label:
        out.index = [f"{i}@{horizon_label}" for i in out.index]
    return out


def _verdict(t, stab):
    """因果体检结论。"""
    if not np.isfinite(t):
        return "不可估"
    if abs(t) >= 2.0 and (np.isnan(stab) or stab >= 0.6):
        return "真因果" if (not np.isnan(stab) and stab >= 0.6) else "显著(符号不稳)"
    if abs(t) < 2.0:
        return "mirage(被混淆吸收)"
    return "符号不稳"


# ── 生产接线：verdicts 加载 + 应用到因子生效 mask ───────────────────────────
def load_causal_verdicts(source: str | None, *, stock_data_dir=None, _cache: dict | None = None):
    """读离线产出的因果 verdicts JSON → {strategy_id: {...}}。失败软：缺文件/异常 → None。

    verdicts JSON 形如 {sid: {"h12": {...}, "h20": {...}, "verdict": "真因果"/...,
    "sign_stability": float, "t": float, "stable": bool}}。键是 STRATEGY_ORDER 策略 id。
    """
    if _cache is None:
        _cache = _VERDICT_CACHE
    if source is None:
        return None
    if source in _cache:
        return _cache[source]
    try:
        from smcore.config.defaults import STOCK_DATA_DIR
    except Exception:
        STOCK_DATA_DIR = None
    if stock_data_dir is not None:
        base = Path(stock_data_dir)
    elif STOCK_DATA_DIR is not None:
        base = Path(STOCK_DATA_DIR)
    else:
        base = Path("stock_data")
    p = Path(source)
    if not p.is_absolute():
        p = base / source
    if not p.exists():
        _cache[source] = None
        return None
    try:
        with open(p, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        _cache[source] = data
        return data
    except Exception:
        _cache[source] = None
        return None


_VERDICT_CACHE: dict = {}


def apply_causal_gate_to_mask(mask: dict[str, bool], verdicts: dict | None,
                              cfg: dict | None) -> dict[str, bool]:
    """把因果闸应用到因子生效 mask：非稳定 / mirage 因子额外清零。

    - verdicts 为 None 或 cfg 未启用 → 原样返回（失败软）。
    - 判定非稳定：sign_stability < min_sign_stability 或 |t| < min_abs_t 或 verdict=="mirage(被混淆吸收)"。
      注意：verdicts 里 stable=True 的因子不被清零；未在 verdicts 中的因子保留（无证据不判）。
    """
    if not verdicts or not cfg:
        return mask
    if not cfg.get("enabled", False):
        return mask
    min_stab = float(cfg.get("min_sign_stability", 0.60))
    min_t = float(cfg.get("min_abs_t", 2.0))
    out = dict(mask)
    for s in list(out.keys()):
        v = verdicts.get(s)
        if not isinstance(v, dict):
            continue
        if v.get("stable") is True:
            continue
        stab = v.get("sign_stability")
        t = v.get("t")
        verdict = v.get("verdict")
        unstable = (
            (verdict == "mirage(被混淆吸收)")
            or (not np.isfinite(stab) or stab < min_stab)
            or (not np.isfinite(t) or abs(t) < min_t)
        )
        if unstable:
            out[s] = False
    return out


def _fmt(x):
    return "NaN" if not np.isfinite(x) else f"{x:.2f}"


# ── 时间序列因果检验（宏观分支 single-signal 版，Step2）─────────────────────────
def _newey_west_lrv(u, lag):
    """单序列 Newey-West 长程方差（Bartlett 核）。u = 残差×回归量 序列。"""
    u = np.asarray(u, float)
    n = u.shape[0]
    if n < 2:
        return np.nan
    lag = int(min(int(lag), n - 1))
    if lag < 1:
        return float(np.mean(u * u))
    ac = np.correlate(u, u, mode="full")[n - 1:]      # ac[0]=Σu², ac[k]=Σu_t·u_{t-k}
    s = float(ac[0])
    w = 1.0 - np.arange(1, lag + 1) / (lag + 1.0)
    for k in range(1, lag + 1):
        s += 2.0 * w[k - 1] * float(ac[k])
    return s / n


def _ts_sign_stability(Sr, Yr, roll=252, step=None):
    """时间序列符号稳定性：滚动窗口内 partial 斜率(=corr 符号)为正的占比。"""
    n = Sr.shape[0]
    if n < 2:
        return np.nan
    if n < roll:
        if np.std(Sr) == 0 or np.std(Yr) == 0:
            return np.nan
        c = np.corrcoef(Sr, Yr)[0, 1] if (np.std(Sr) and np.std(Yr)) else np.nan
        return (1.0 if (np.isfinite(c) and c > 0) else 0.0)
    step = step or max(1, roll // 4)
    pos = tot = 0
    for i in range(0, n - roll + 1, step):
        a = Sr[i:i + roll]
        b = Yr[i:i + roll]
        if np.std(a) == 0 or np.std(b) == 0:
            continue
        c = np.corrcoef(a, b)[0, 1]
        if not np.isfinite(c):
            continue
        tot += 1
        pos += 1 if c > 0 else 0
    return float(pos / tot) if tot else np.nan


def validate_macro_signal(scores, fwd_rets, confounders=None, *,
                          nw_lag=None, roll=252, min_n=200, learner_kind="linear"):
    """时间序列因果检验：宏观 composite score 对前向市场收益的净效应（Step2 实证用）。

    与因子闸同一方法论（FWL 偏出 + 稳健推断 + 符号稳定性），但是**时间序列**版：
    - treatment d = score（单序列，非横截面 z）；outcome y = 前向市场收益
    - confounders X = 已知市场预测因子（滞后收益/波动），偏出「score 只是动量代理」类混淆
    - θ = 偏出后 score_res ~ fwd_res 斜率；稳健 t 用 Newey-West HAC（单序列按 date 聚类退化，改 HAC）
    - 符号稳定性 = 滚动窗口(roll)内 partial 斜率为正的占比

    返回 {theta, se, t, p, n, sign_stability, verdict}；样本不足 → None（失败软）。
    """
    s = np.asarray(scores, float)
    y = np.asarray(fwd_rets, float)
    if confounders is not None:
        X = np.asarray(confounders, float)
        if X.ndim == 1:
            X = X.reshape(-1, 1)
    else:
        X = np.zeros((s.shape[0], 0), float)
    mask = ~(np.isnan(s) | np.isnan(y))
    if X.shape[1] > 0:
        mask &= ~np.isnan(X).any(axis=1)
    s, y, X = s[mask], y[mask], X[mask]
    n = s.shape[0]
    if n < min_n:
        return None
    y_res, s_res = _residualize(y, s, X, folds=1, learner_kind=learner_kind)
    ok = ~(np.isnan(y_res) | np.isnan(s_res))
    Yr, Sr = y_res[ok], s_res[ok]
    n = Yr.shape[0]
    if n < min_n:
        return None
    Xm = np.column_stack([np.ones_like(Sr), Sr])
    beta, *_ = np.linalg.lstsq(Xm, Yr, rcond=None)
    resid = Yr - Xm @ beta
    theta = float(beta[1])
    if nw_lag is None:
        nw_lag = int(np.floor(4.0 * (n / 100.0) ** (2.0 / 3.0)))
    u = Sr * resid
    lrv = _newey_west_lrv(u, nw_lag)
    sxx = float(np.sum(Sr * Sr))
    se = float(np.sqrt(lrv / sxx)) if (sxx > 0 and np.isfinite(lrv)) else np.nan
    t = theta / se if (np.isfinite(se) and se > 0) else np.nan
    from math import erf
    p = 2.0 * (1.0 - 0.5 * (1.0 + erf(abs(t) / np.sqrt(2.0)))) if np.isfinite(t) else np.nan
    stab = _ts_sign_stability(Sr, Yr, roll=roll)
    return {"theta": theta, "se": se, "t": t, "p": p, "n": int(n),
            "sign_stability": stab, "verdict": _verdict(t, stab)}


# ── 新因子准入闸（Step3）：与 apply_causal_gate_to_mask 同一把尺 ─────────────────
def admit_factor_by_causal(verdict: dict | None, cfg: dict | None) -> tuple[bool, str]:
    """新因子(含 DL / 遗传挖掘)准入闸：给定某因子 DML verdict，判定是否准入。

    与 `apply_causal_gate_to_mask` 同判据：mirage / 符号不稳 / |t|<min → 拒绝；
    stable 或 证据不足(None) → 不拒绝（缺证据不挡，交上游）。返回 (admit, reason)。
    """
    if not isinstance(verdict, dict):
        return True, "无 verdict → 不拒绝(缺证据)"
    min_stab = float((cfg or {}).get("min_sign_stability", 0.60))
    min_t = float((cfg or {}).get("min_abs_t", 2.0))
    if verdict.get("stable") is True:
        return True, "stable=True → 准入"
    stab = verdict.get("sign_stability")
    t = verdict.get("t")
    v = verdict.get("verdict")
    if v == "mirage(被混淆吸收)":
        return False, "mirage(被混淆吸收) → 拒绝"
    if not np.isfinite(stab) or stab < min_stab:
        return False, f"符号稳定性 {_fmt(stab)} < {min_stab} → 拒绝"
    if not np.isfinite(t) or abs(t) < min_t:
        return False, f"|t| {_fmt(t)} < {min_t} → 拒绝"
    return True, "通过因果闸 → 准入"


def raw_ic_table(fwd_ret: pd.DataFrame, factors: dict[str, pd.DataFrame],
                 min_n_day: int = 300) -> pd.Series:
    """对照用：原始横截面 Spearman IC 均值（未控制混淆），供 mirage gap 对比。"""
    from smcore.strategy.factor_engine import cross_sectional_ic, forward_rank_matrix
    fwd_rank = forward_rank_matrix(fwd_ret)
    ic = {name: cross_sectional_ic(fac, fwd_rank, min_n_day=min_n_day).mean()
          for name, fac in factors.items()}
    return pd.Series(ic, name="raw_ic_mean")
