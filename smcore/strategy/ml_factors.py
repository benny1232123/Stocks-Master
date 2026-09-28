#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""ML 因子挖掘（walk-forward 堆叠，数据门控，绝不训练 29 信号日）。

把现有风格因子(及可扩展自定义因子)经「严格 walk-forward」堆叠为一个选股评分因子：
- 每个测试信号日 t 用【严格早于 t】的全部信号日训练，预测 t 当日候选股的前向收益；
- 跨测试日汇总 Rank-IC / ICIR / 正IC占比；
- 仅当「样本充足 + IC 显著 + 稳定」三件套同时满足时才「激活」ML 叠加因子，否则自动中性返回，
  防止在 29 信号日这种小样本上过拟合（与 P0-1/P1-3 同一套纪律闸门）。

默认模型 = 正则化 Ridge（闭式解，numpy only，无 sklearn 依赖，可离线 CI 跑）；
内层 alpha 选择用 Purged K-Fold + Embargo（按信号日分块，剔除 label 窗口
触及验证块的样本），消除「同票相邻日」跨折的前向收益泄漏。
全部 fail-soft：数据不足/缺失返回中性，绝不抛异常。

依赖：本地 k_data（前向收益）+ risk_model 风格因子暴露；不联网。
"""
from __future__ import annotations

import json
import numpy as np
import pandas as pd

from smcore.config.defaults import STOCK_DATA_DIR
from smcore.strategy import risk_model as rm
from smcore.strategy.attribution import forward_returns

# 默认超参（配置可覆盖）
_DEFAULTS = {
    "enabled": True,
    "min_train_days": 60,     # 训练所需最少历史信号日（29 日当前远不足 → 自动门控）
    "min_folds": 10,          # 至少 10 个测试日才统计 IC/IR
    "horizon": 10,            # 前向收益窗口
    "alphas": [1e-3, 1e-2, 0.1, 1.0, 10.0],
    "cv_folds": 5,            # 内层 alpha 选择折数（按信号日分块的 purged K-Fold）
    "embargo_days": 2,        # 验证块之后额外隔离的信号日数（特征自相关泄漏）
    "min_ic": 0.02,           # 平均 Rank-IC 阈值
    "min_ir": 0.5,            # ICIR 阈值
    "min_positive_frac": 0.6, # 正 IC 占比阈值（稳定性）
}

# 融合接入（fuse_integration）默认超参：研究闸门三件套之外「另立」的接入层门控。
# 纪律：激活≠接入（2026-09-26 记忆），enabled 默认 False，须显式开启。
_FUSE_DEFAULTS = {
    "weight": 3.0,                    # 截面 z → 评分点放大系数
    "max_bonus": 10.0,                # 单票 ML 分 clamp（与 factor_scoring.max_bonus 同型）
    "monitor_window": 20,             # 衰减监控窗：最近 N 个 OOS 折
    "min_monitor_folds": 5,           # 监控窗内折数不足 → 不接入（证据不足）
    "min_recent_ic": 0.0,             # 近窗平均 Rank-IC 下限（>0 才接入）
    "min_recent_positive_frac": 0.5,  # 近窗正 IC 占比下限
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


def _purged_fold_keeps(n: int, cv_folds: int, horizon: int, embargo_days: int):
    """Purged K-Fold 的折划分：按信号日取连续块做验证折，返回 [(lo, hi, keep), ...]。

    keep 为训练可用样本索引：
      - purge：label 窗口 [t, t+horizon] 触及验证块 [lo, hi-1] 的样本剔除（t+horizon >= lo）；
      - embargo：验证块结束后 embargo_days 内的样本剔除（t <= hi-1+embargo_days）。
    """
    k = max(2, min(int(cv_folds), n))
    lo_all = [round(i * n / k) for i in range(k)] + [n]
    folds = []
    for i in range(k):
        lo, hi = lo_all[i], lo_all[i + 1]
        keep = [t for t in range(n) if t + horizon < lo or t > hi - 1 + embargo_days]
        folds.append((lo, hi, keep))
    return folds


def _purged_ridge_predict(train, Xte, alphas, cv_folds: int, embargo_days: int, horizon: int):
    """Purged K-Fold CV 选 alpha + 闭式 Ridge 拟合，返回测试集预测分。

    train: [(sd, X_d, y_d), ...] 按信号日升序 —— 每个样本的 label 是 horizon 日
    前向收益，相邻信号日的窗口互相重叠。旧实现按行交错分折（idx[fold::k]），
    会把「同票相邻日」分进训练/验证两侧，alpha 被泄漏抬高（López de Prado,
    AFML ch.7 Purged K-Fold + Embargo）。最终模型仍用全部训练样本拟合；
    验证折内标准化用折内训练统计量。
    """
    n = len(train)
    fold_sets = []
    for lo, hi, keep in _purged_fold_keeps(n, cv_folds, horizon, embargo_days):
        if not keep:
            continue
        Xtr = np.vstack([train[t][1] for t in keep])
        ytr = np.concatenate([train[t][2] for t in keep])
        Xva = np.vstack([train[t][1] for t in range(lo, hi)])
        yva = np.concatenate([train[t][2] for t in range(lo, hi)])
        if len(ytr) < 5 or len(yva) == 0:
            continue
        mu, sd = np.nanmean(Xtr, axis=0), np.nanstd(Xtr, axis=0)
        sd = np.where(sd > 0, sd, 1.0)
        fold_sets.append(((Xtr - mu) / sd, ytr, (Xva - mu) / sd, yva))

    best_alpha = None
    if fold_sets:
        best_err = float("inf")
        for a in alphas:
            errs = []
            for Ztr, ytr, Zva, yva in fold_sets:
                M = Ztr.T @ Ztr + a * np.eye(Ztr.shape[1])
                try:
                    w = np.linalg.solve(M, Ztr.T @ ytr)
                except Exception:
                    continue
                errs.append(np.mean((Zva @ w - yva) ** 2))
            if errs and np.mean(errs) < best_err:
                best_err, best_alpha = np.mean(errs), a
    if best_alpha is None:
        best_alpha = alphas[len(alphas) // 2]  # 折不足时取中间强度，避免偏端

    Xtr = np.vstack([d[1] for d in train])
    ytr = np.concatenate([d[2] for d in train])
    mu, sd = np.nanmean(Xtr, axis=0), np.nanstd(Xtr, axis=0)
    sd = np.where(sd > 0, sd, 1.0)
    Ztr = (Xtr - mu) / sd
    Zte = (Xte - mu) / sd
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


def _causal_purge_cal_days(horizon: int) -> int:
    """label 完全实现所需的最小日历距离（交易日 → 日历近似）。

    训练日 d 的 label（horizon 交易日前向收益）在 d+horizon+1 个交易日实现；
    换算日历 ≈ (horizon+1)*7/5，+2 日假期/周末缓冲。
    """
    return (int(horizon) + 1) * 7 // 5 + 2


def _causal_train_indices(ts, t: int, purge_cal: int, fallback_idx: int) -> list[int]:
    """严格因果：测试日 t 可用的训练日索引（label 窗口必须完全实现于测试日之前）。

    ts: 与 data 等长的信号日 Timestamp 序列。日期可解析 → 按日历距离 purge
    （跨周频/日频混排时唯一正确口径）；不可解析（合成测试数据）→ 按索引距离
    近似（fallback_idx 个信号日），仍严于旧口径。
    """
    test_ts = ts.iloc[t] if hasattr(ts, "iloc") else ts[t]
    if test_ts is None or pd.isna(test_ts):
        return list(range(max(0, t - max(1, fallback_idx))))
    return [i for i in range(t) if (test_ts - ts.iloc[i]).days >= purge_cal]


def walk_forward_ml(signal_days, codes, cfg: dict) -> dict:
    """walk-forward 堆叠评估。返回 {ics, mean_ic, icir, positive_frac, n_folds, ok}。

    外层为严格因果口径（2026-09-26 默认）：训练日 d 仅当其 label 窗口
    [d, d+horizon] 在测试日 t 之前完全实现（日历距离 ≥ purge 窗）才可用。
    旧口径 data[:t] 会把「相邻日 label 重叠」的未实现收益当已知，虚高 OOS IC
    （实测 horizon=10 时 IC 0.189→0.121）。causal_purge_cal_days 可覆盖，
    传 0 退回旧口径（不建议）。
    """
    c = {**_DEFAULTS, **(cfg or {})}
    horizon = int(c["horizon"])
    min_train = int(c["min_train_days"])
    purge_cfg = c.get("causal_purge_cal_days")
    purge_cal = int(purge_cfg) if purge_cfg else _causal_purge_cal_days(horizon)
    data = _dataset(signal_days, codes, horizon)
    if len(data) < min_train + 1:
        return {"ok": False, "reason": "insufficient_signal_days",
                "n_available": len(data), "min_train": min_train,
                "ics": [], "fold_records": [],
                "mean_ic": None, "icir": None, "positive_frac": None, "n_folds": 0}
    ts = pd.to_datetime(pd.Series([d[0] for d in data]), format="%Y%m%d", errors="coerce")
    ics = []
    fold_records = []  # [(test_sd, ic), ...] 按时间升序，供接入门控做因果过滤（近窗衰减监控）
    for t in range(min_train, len(data)):
        idx = _causal_train_indices(ts, t, purge_cal, horizon + 2)
        train = [data[i] for i in idx]
        test_sd, Xte, yte = data[t]
        if sum(len(d[2]) for d in train) < 5:
            continue
        pred = _purged_ridge_predict(train, Xte, c["alphas"],
                                     int(c["cv_folds"]), int(c["embargo_days"]), horizon)
        ic = _spearman_ic(pred, yte)
        if ic is not None and not np.isnan(ic):
            ics.append(ic)
            fold_records.append([str(test_sd), round(float(ic), 4)])
    if len(ics) < int(c["min_folds"]):
        return {"ok": False, "reason": "insufficient_folds", "n_folds": len(ics),
                "ics": ics, "fold_records": fold_records,
                "mean_ic": None, "icir": None, "positive_frac": None}
    ics = np.array(ics)
    mean_ic = float(np.mean(ics))
    sd = np.std(ics)
    icir = float(mean_ic / sd) if sd > 0 else 0.0
    pos = float((ics > 0).mean())
    return {"ok": True, "ics": ics.tolist(), "fold_records": fold_records,
            "mean_ic": round(mean_ic, 4),
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
    """端到端：评估 ML 因子是否达标。返回报告 dict（含 gate 决策 + 逐折记录）。"""
    res = walk_forward_ml(signal_days, codes, cfg or {})
    gate = evaluate_ml_gate(res, cfg or {})
    return {"ok": res.get("ok", False), "gate": gate,
            "mean_ic": res.get("mean_ic"), "icir": res.get("icir"),
            "positive_frac": res.get("positive_frac"), "n_folds": res.get("n_folds", 0),
            "fold_records": res.get("fold_records", []),
            "n_available": res.get("n_available"), "min_train": res.get("min_train"),
            "reason": res.get("reason")}


def format_ml_report(res: dict) -> str:
    if not res.get("ok"):
        return ("# ML 因子挖掘（walk-forward 堆叠）\n\n"
                f"⚠️ 数据门控未通过，ML 因子保持中性（不激活）：{res.get('reason')}\n"
                f"可用信号日={res.get('n_available', 'N/A')}（需 > min_train_days={res.get('min_train', 60)}），"
                f"有效测试折={res.get('n_folds')}（需 ≥ 10）。\n"
                "> 遵循样本外纪律：小样本禁止训练，避免过拟合；"
                "待累积足够信号日且 IC/IR 达标后再激活。\n")
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


# ═══════════════════════════════════════════════════════════════════════
# 融合评分接入层（2026-09-27）：研究闸门三件套之外「另立」的 OOS 接入门控。
# 纪律：激活≠接入。接入 = ①研究闸门已激活（ml_factors.json gate.activate）
# + ②近窗衰减监控（最近 N 折 IC 仍为正）+ ③配置显式开启（默认 False）
# + ④因果折过滤（回放历史日时只统计 test_sd < 信号日 的折，杜绝
# 「用今天才决定的激活结论去增强历史清单」的时间泄漏）。
# ═══════════════════════════════════════════════════════════════════════

_WF_REPORT_CACHE: dict = {}       # path -> (mtime, report_dict)
_SIGNAL_DAYS_CACHE: dict = {}     # 进程内缓存（DAL 集在单次回放期间静态）


def _default_signal_days() -> list[str]:
    """全部 DAL 信号日（YYYYMMDD 升序）。进程内缓存一次。"""
    if _SIGNAL_DAYS_CACHE:
        return _SIGNAL_DAYS_CACHE["days"]
    days = []
    for p in STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"):
        name = p.stem.replace("Daily-Action-List-", "")
        if len(name) == 8 and name.isdigit():
            days.append(name)
    days = sorted(set(days))
    _SIGNAL_DAYS_CACHE["days"] = days
    return days


def _load_wf_report(path=None) -> dict | None:
    """读 walk-forward 报告（stock_data/ml_factors.json），mtime 变化才重读。"""
    p = STOCK_DATA_DIR / (path or "ml_factors.json")
    try:
        mtime = p.stat().st_mtime
    except OSError:
        return None
    cached = _WF_REPORT_CACHE.get(str(p))
    if cached and cached[0] == mtime:
        return cached[1]
    try:
        report = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    _WF_REPORT_CACHE[str(p)] = (mtime, report)
    return report


def ml_scores_for_date(date_yyyymmdd, codes, cfg: dict = None,
                       signal_days: list[str] | None = None) -> dict | None:
    """对 date 当日候选 codes 产出 ML 截面评分（z-score），数据不足返回 None。

    严格因果：训练集 = 严格早于 date 且 label 窗口 [d, d+horizon] 完全实现
    （日历距离 ≥ purge 窗）的信号日；训练日不足 min_train_days → None（中性）。
    池 = 当日候选本身（与融合「对今日清单加分」的语义一致），训练日暴露/标签
    逐日现算（compute_exposures/forward_returns 走本地 k_data，零联网）。
    """
    c = {**_DEFAULTS, **(cfg or {})}
    horizon = int(c["horizon"])
    codes = [str(x).strip() for x in codes if str(x).strip() and str(x).strip().lower() != "nan"]
    if len(codes) < 3:
        return None
    if signal_days is None:
        signal_days = _default_signal_days()
    sds = sorted({str(s) for s in signal_days} | {str(date_yyyymmdd)})
    t = sds.index(str(date_yyyymmdd))
    purge_cfg = c.get("causal_purge_cal_days")
    purge_cal = int(purge_cfg) if purge_cfg else _causal_purge_cal_days(horizon)
    ts = pd.to_datetime(pd.Series(sds), format="%Y%m%d", errors="coerce")
    idx = _causal_train_indices(ts, t, purge_cal, horizon + 2)
    train_sds = [sds[i] for i in idx]

    train = []
    for sd in train_sds:
        try:
            expo = rm.compute_exposures(codes, as_of=sd)
        except Exception:
            continue
        if expo.shape[0] < 3:
            continue
        yd = forward_returns(codes, sd, horizon=horizon)
        yv = np.array([yd.get(cc, np.nan) for cc in expo.index], dtype=float)
        mask = ~np.isnan(yv)
        if mask.sum() < 3:
            continue
        train.append((sd, expo.values[mask], yv[mask]))
    if len(train) < int(c["min_train_days"]):
        return None
    try:
        expo_t = rm.compute_exposures(codes, as_of=str(date_yyyymmdd))
    except Exception:
        return None
    if expo_t.shape[0] < 3:
        return None
    try:
        pred = _purged_ridge_predict(train, expo_t.values, c["alphas"],
                                     int(c["cv_folds"]), int(c["embargo_days"]), horizon)
    except Exception:
        return None
    p = np.asarray(pred, dtype=float)
    if not np.all(np.isfinite(p)):
        p = np.nan_to_num(p, nan=0.0)
    if float(np.std(p)) <= 0:
        return None
    z = (p - np.mean(p)) / float(np.std(p))
    return {str(code): float(zz) for code, zz in zip(expo_t.index, z)}


def evaluate_fuse_gate(wf_report: dict, cfg: dict = None, as_of: str | None = None) -> dict:
    """接入门控（与研究闸门 evaluate_ml_gate 分离）：

    ① 研究闸门已激活（wf_report.gate.activate）；
    ② 近窗衰减监控：as_of 之前最近 monitor_window 个 OOS 折的
       mean IC ≥ min_recent_ic 且正 IC 占比 ≥ min_recent_positive_frac
       （折数 < min_monitor_folds → 证据不足，不接入）；
    ③ 因果性：as_of 给定时只统计「当时已知」的折 —— 折的 IC 用 test_sd 起
       horizon 日前向收益，须完全实现（as_of − test_sd ≥ purge 窗，与训练侧
       同口径）才对 as_of 可见。回放历史日时，今天才观测到的折不可见，
       杜绝「用今天才决定的激活结论去增强历史清单」的未来信息泄漏。

    ④ 配置开关由调用方（fusion/CONFIG）持有，不在此重复检查。
    返回 {allow, checks, n_folds_asof, recent_ic, recent_positive_frac}。
    """
    c = {**_DEFAULTS, **_FUSE_DEFAULTS, **(cfg or {})}
    checks: dict[str, bool] = {}
    research = (wf_report or {}).get("gate") or {}
    checks["research_gate"] = bool(research.get("activate"))
    folds = (wf_report or {}).get("fold_records") or []
    if as_of:
        try:
            tsv = pd.Timestamp(str(as_of))
        except Exception:
            tsv = None
        if tsv is not None and not pd.isna(tsv):
            purge_cfg = c.get("causal_purge_cal_days")
            purge_cal = int(purge_cfg) if purge_cfg else _causal_purge_cal_days(int(c["horizon"]))
            kept = []
            for sd, ic in folds:
                fd = pd.to_datetime(str(sd), format="%Y%m%d", errors="coerce")
                if pd.isna(fd) or (tsv - fd).days >= purge_cal:
                    kept.append((sd, ic))
            folds = kept
    win = max(1, int(c["monitor_window"]))
    recent = folds[-win:]
    ics = [float(ic) for _, ic in recent if ic is not None and ic == ic]
    checks["enough_recent_folds"] = len(ics) >= int(c["min_monitor_folds"])
    recent_ic = float(np.mean(ics)) if ics else None
    recent_pos = float(np.mean([1.0 if x > 0 else 0.0 for x in ics])) if ics else None
    checks["recent_ic_ok"] = bool(recent_ic is not None and recent_ic >= float(c["min_recent_ic"]))
    checks["recent_positive_ok"] = bool(
        recent_pos is not None and recent_pos >= float(c["min_recent_positive_frac"]))
    return {"allow": all(checks.values()), "checks": checks,
            "n_folds_asof": len(folds), "n_recent": len(ics),
            "recent_ic": round(recent_ic, 4) if recent_ic is not None else None,
            "recent_positive_frac": round(recent_pos, 3) if recent_pos is not None else None}


def fuse_ml_bonus(codes, date_yyyymmdd, cfg: dict = None) -> tuple[dict, str]:
    """融合层接入助手：读 walk-forward 报告 → 接入门控（因果）→ 当日 ML 评分 → 可加加分。

    返回 ({code: bonus}, note)。任何一环不满足 → ({}, 原因)（中性降级，绝不抛异常）。
    bonus = clamp(weight × z, ±max_bonus)，直接加进「综合评分」。
    """
    c = {**_FUSE_DEFAULTS, **(cfg or {})}
    try:
        report = _load_wf_report(c.get("report_path"))
    except Exception:
        report = None
    if not report:
        return {}, "ML 接入门控：无 walk-forward 报告（ml_factors.json 缺失），保持中性"
    gate = evaluate_fuse_gate(report, c, as_of=str(date_yyyymmdd))
    if not gate["allow"]:
        failed = [k for k, v in gate["checks"].items() if not v]
        return {}, f"ML 接入门控未通过（{','.join(failed)}），保持中性"
    z = ml_scores_for_date(date_yyyymmdd, codes, cfg)
    if not z:
        return {}, "ML 评分训练数据不足（min_train_days/暴露缺失），保持中性"
    w = float(c["weight"])
    cap = float(c["max_bonus"])
    bonus = {k: round(max(-cap, min(cap, w * v)), 2) for k, v in z.items()}
    note = (f"ML 叠加因子已接入：近{gate['n_recent']}折 IC={gate['recent_ic']}、"
            f"正IC占比={gate['recent_positive_frac']}；"
            f"全样本 {report.get('n_folds')}折 IC={report.get('mean_ic')}/ICIR={report.get('icir')}")
    return bonus, note
