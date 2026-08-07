#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""ML 因子挖掘（walk-forward 堆叠，数据门控，绝不训练 29 信号日）。

把现有风格因子(及可扩展自定义因子)经「严格 walk-forward」堆叠为一个选股评分因子：
- 每个测试信号日 t 用【严格早于 t】的全部信号日训练，预测 t 当日候选股的前向收益；
- 跨测试日汇总 Rank-IC / ICIR / 正IC占比；
- 仅当「样本充足 + IC 显著 + 稳定」三件套同时满足时才「激活」ML 叠加因子，否则自动中性返回，
  防止在 29 信号日这种小样本上过拟合（与 P0-1/P1-3 同一套纪律闸门）。

默认模型 = 正则化 Ridge（闭式解，numpy only，无 sklearn 依赖，可离线 CI 跑）。
全部 fail-soft：数据不足/缺失返回中性，绝不抛异常。

依赖：本地 k_data（前向收益）+ risk_model 风格因子暴露；不联网。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from smcore.strategy import risk_model as rm
from smcore.strategy.attribution import forward_returns

# 默认超参（配置可覆盖）
_DEFAULTS = {
    "enabled": True,
    "min_train_days": 60,     # 训练所需最少历史信号日（29 日当前远不足 → 自动门控）
    "min_folds": 10,          # 至少 10 个测试日才统计 IC/IR
    "horizon": 10,            # 前向收益窗口
    "alphas": [1e-3, 1e-2, 0.1, 1.0, 10.0],
    "min_ic": 0.02,           # 平均 Rank-IC 阈值
    "min_ir": 0.5,            # ICIR 阈值
    "min_positive_frac": 0.6, # 正 IC 占比阈值（稳定性）
}


def _spearman_ic(pred: np.ndarray, y: np.ndarray) -> float:
    """Rank-IC：预测分与真实收益的横截面秩相关。"""
    if len(pred) < 3 or len(y) < 3:
        return float("nan")
    pr = pd.Series(pred).rank().values
    yr = pd.Series(y).rank().values
    if np.std(pr) == 0 or np.std(yr) == 0:
        return float("nan")
    return float(np.corrcoef(pr, yr)[0, 1])


def _standardize(X: np.ndarray):
    mu = np.nanmean(X, axis=0)
    sd = np.nanstd(X, axis=0)
    sd = np.where(sd > 0, sd, 1.0)
    return (X - mu) / sd


def _ridge_predict(Xtr, ytr, Xte, alphas):
    """Ridge 闭式：5折 CV 选 alpha（RMSE），返回测试集预测分。

    用训练集的均值/标准差标准化训练与测试（测试不得用自己的统计量，否则学到的系数尺度错位）。
    """
    Xtr = np.asarray(Xtr, dtype=float)
    ytr = np.asarray(ytr, dtype=float)
    Xte = np.asarray(Xte, dtype=float)
    mu = np.nanmean(Xtr, axis=0)
    sd = np.nanstd(Xtr, axis=0)
    sd = np.where(sd > 0, sd, 1.0)
    Ztr = (Xtr - mu) / sd
    Zte = (Xte - mu) / sd
    best_alpha, best_err = alphas[0], float("inf")
    k = min(5, len(Ztr))
    for a in alphas:
        errs = []
        idx = np.arange(len(Ztr))
        for fold in range(k):
            te = idx[fold::k]
            tr = np.array([i for i in idx if i not in set(te.tolist())])
            if len(tr) == 0 or len(te) == 0:
                continue
            Xa, ya = Ztr[tr], ytr[tr]
            Xb, yb = Ztr[te], ytr[te]
            M = Xa.T @ Xa + a * np.eye(Xa.shape[1])
            try:
                w = np.linalg.solve(M, Xa.T @ ya)
            except Exception:
                continue
            pred = Xb @ w
            errs.append(np.mean((pred - yb) ** 2))
        if errs and np.mean(errs) < best_err:
            best_err, best_alpha = np.mean(errs), a
    M = Ztr.T @ Ztr + best_alpha * np.eye(Ztr.shape[1])
    try:
        w = np.linalg.solve(M, Ztr.T @ ytr)
    except Exception:
        w = np.zeros(Ztr.shape[1])
    return Zte @ w


def _dataset(signal_days, codes, horizon):
    """构建 (sd, X, y) 序列；X=当日截面风格因子暴露(z)，y=当日候选股前向收益。"""
    out = []
    for sd in signal_days:
        try:
            expo = rm.compute_exposures(codes, as_of=sd)
        except Exception:
            continue
        if expo.shape[0] < 3:
            continue
        yd = forward_returns(codes, sd, horizon=horizon)
        yv = np.array([yd.get(c, np.nan) for c in expo.index], dtype=float)
        mask = ~np.isnan(yv)
        if mask.sum() < 3:
            continue
        out.append((sd, expo.values[mask], yv[mask]))
    return out


def walk_forward_ml(signal_days, codes, cfg: dict) -> dict:
    """walk-forward 堆叠评估。返回 {ics, mean_ic, icir, positive_frac, n_folds, ok}。"""
    c = {**_DEFAULTS, **(cfg or {})}
    horizon = int(c["horizon"])
    min_train = int(c["min_train_days"])
    data = _dataset(signal_days, codes, horizon)
    if len(data) < min_train + 1:
        return {"ok": False, "reason": "insufficient_signal_days",
                "n_available": len(data), "min_train": min_train,
                "ics": [], "mean_ic": None, "icir": None, "positive_frac": None, "n_folds": 0}
    ics = []
    for t in range(min_train, len(data)):
        train = data[:t]
        test_sd, Xte, yte = data[t]
        Xtr = np.vstack([d[1] for d in train])
        ytr = np.concatenate([d[2] for d in train])
        if Xtr.shape[0] < 5:
            continue
        pred = _ridge_predict(Xtr, ytr, Xte, c["alphas"])
        ic = _spearman_ic(pred, yte)
        if ic == ic or ic is not None:  # 非 nan
            if not np.isnan(ic):
                ics.append(ic)
    if len(ics) < int(c["min_folds"]):
        return {"ok": False, "reason": "insufficient_folds", "n_folds": len(ics),
                "ics": ics, "mean_ic": None, "icir": None, "positive_frac": None}
    ics = np.array(ics)
    mean_ic = float(np.mean(ics))
    sd = np.std(ics)
    icir = float(mean_ic / sd) if sd > 0 else 0.0
    pos = float((ics > 0).mean())
    return {"ok": True, "ics": ics.tolist(), "mean_ic": round(mean_ic, 4),
            "icir": round(icir, 3), "positive_frac": round(pos, 3), "n_folds": len(ics)}


def evaluate_ml_gate(res: dict, cfg: dict) -> dict:
    """三件套闸门：样本充足 + IC显著 + 稳定。任一不满足→不激活。"""
    c = {**_DEFAULTS, **(cfg or {})}
    if not res.get("ok"):
        return {"activate": False, "reason": res.get("reason", "not_ok"),
                "mean_ic": res.get("mean_ic"), "icir": res.get("icir"),
                "positive_frac": res.get("positive_frac"), "n_folds": res.get("n_folds", 0)}
    ok_ic = res["mean_ic"] >= float(c["min_ic"])
    ok_ir = res["icir"] >= float(c["min_ir"])
    ok_pos = res["positive_frac"] >= float(c["min_positive_frac"])
    activate = ok_ic and ok_ir and ok_pos
    return {"activate": activate, "mean_ic": res["mean_ic"], "icir": res["icir"],
            "positive_frac": res["positive_frac"], "n_folds": res["n_folds"],
            "checks": {"ic_ok": ok_ic, "ir_ok": ok_ir, "positive_ok": ok_pos}}


def run_ml_factor_report(signal_days, codes, cfg: dict = None) -> dict:
    """端到端：评估 ML 因子是否达标。返回报告 dict（含 gate 决策）。"""
    res = walk_forward_ml(signal_days, codes, cfg or {})
    gate = evaluate_ml_gate(res, cfg or {})
    return {"ok": res.get("ok", False), "gate": gate,
            "mean_ic": res.get("mean_ic"), "icir": res.get("icir"),
            "positive_frac": res.get("positive_frac"), "n_folds": res.get("n_folds", 0),
            "reason": res.get("reason")}


def format_ml_report(res: dict) -> str:
    if not res.get("ok"):
        return ("# ML 因子挖掘（walk-forward 堆叠）\n\n"
                f"⚠️ 数据门控未通过，ML 因子保持中性（不激活）：{res.get('reason')}\n"
                f"可用信号日={res.get('n_folds')}（需 ≥ min_train_days）。\n"
                "> 遵循样本外纪律：29 信号日小样本禁止训练，避免过拟合；"
                "待累积 ≥1 年信号日且 IC/IR 达标后再激活。\n")
    g = res["gate"]
    lines = [
        "# ML 因子挖掘（walk-forward 堆叠）",
        "",
        f"- 测试折数：**{res['n_folds']}**；平均 Rank-IC=**{res['mean_ic']}**；ICIR=**{res['icir']}**；正IC占比=**{res['positive_frac']}**",
        f"- 闸门决策：**{'✅ 激活 ML 叠加因子' if g['activate'] else '❌ 不激活（保持中性）'}**",
    ]
    if "checks" in g:
        lines.append(f"  - IC≥{0.02}: {g['checks']['ic_ok']} / IR≥{0.5}: {g['checks']['ir_ok']} / 正IC≥{0.6}: {g['checks']['positive_ok']}")
    lines.append("")
    lines.append("> 激活后该 ML 评分因子可经 config 叠加进融合综合评分；当前默认保持中性。")
    return "\n".join(lines) + "\n"
