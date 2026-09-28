"""宏观 regime 校准分支 —— 给现有「市场状态 → 总仓位」叠加一层宏观择时。

设计动机
--------
现有 adaptive 仓位只看 price-based regime（沪深300 趋势/波动/宽度/量能，
见 ``smcore/strategy/market.py``）。文献 + 本仓回测（vol_managed_overlay 实验）一致表明：
**真杠杆在「攻收益端 / timing」，不在第 13 个选股因子**；且现有风险层已饱和
（57% 现金、Sharpe 1.19、MDD -0.39%），再叠加波动管理无收益。宏观 regime 直接打
「57% 闲置现金何时部署」痛点——是现有 adaptive regime 层的**校准升级**（从纯价量
regime 增配宏观维度），非新因子、非风控叠加。

宏观变量（取自公开实证，非自测；需外部拉取，离线沙箱不可用 → 见
``.workbuddy/fetch_macro_akshare.py``）：
- 期限利差 term_spread = 10Y 国债收益率 − 1Y（或 2Y）国债收益率（正向 = 经济扩张）
- 信用利差 credit_spread = AAA 企业债收益率 − 同期限国债收益率（正向 = 信用紧缩/防御）
- M2 同比 m2_yoy（正向 = 流动性扩张）
- PMI（>50 扩张）
- 境外流动性代理 usdcny_chg = 美元兑人民币日变化（升值/USDCNY↓ = 资本流入 = 正向；
  贬值/USDCNY↑ = 资本流出 = 防御）
- 股指动量 hs300_mom = 沪深300 近 20 日收益（与 price regime 重叠，作一致性校验，降权）

合成：连续 score ∈ [-1.5, 1.5]（扩张 ↔ 防御）。各变量做近 5 年滚动 z-score 后按权重
线性合成（权重可配置、默认等权）。score ≤ hard_defensive_score（深度防御）→ 触发
「硬防御」覆盖（equity 封顶 macro_defensive_cap）；否则 equity 按 score × tilt_amp
连续微调。全部 clamp 到 equity_ratio_bounds。

fail-soft：
- 数据缺失 / 开关关 / 任一异常 → 返回原 equity，不改行为。
- 本模块零 ``smcore.strategy`` 顶层依赖，避免循环导入；``STOCK_DATA_DIR`` 在函数内懒 import。

walk-forward 门控（calibrate_macro_regime）：
- 只用 ≤ 调参日数据训练阈值；跨 holdout 稳健（均值 rankIC > 阈值 且 符号稳定）才 adopt。
- 离线沙箱无数据时不运行；``fetch_macro_akshare.py`` 拉数后由 CI/本机跑。
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# ── 宏观变量默认定义：parquet 列名 + 方向(+1 扩张 / -1 防御) + 权重 ──
# parquet 须含列：date, term_spread, credit_spread, m2_yoy, pmi, usdcny_chg, hs300_mom
DEFAULT_MACRO_VARS: dict[str, dict] = {
    "term_spread":   {"sign": +1.0, "weight": 1.00},  # 期限利差走阔 = 扩张
    "credit_spread": {"sign": -1.0, "weight": 1.00},  # 信用利差走阔 = 防御
    "m2_yoy":        {"sign": +1.0, "weight": 1.00},  # 流动性扩张 = 扩张
    "pmi":           {"sign": +1.0, "weight": 0.75},  # >50 扩张
    "usdcny_chg":    {"sign": -1.0, "weight": 0.75},  # 人民币贬值 = 资本流出 = 防御
    "hs300_mom":     {"sign": +1.0, "weight": 0.50},  # 与 price regime 重叠，降权作一致性
}

# ── 分支默认配置（内置默认关；拉到数据且 walk-forward 采纳后才置 enabled=True）──
DEFAULT_MACRO_CFG: dict = {
    "enabled": False,                       # 内置默认关：保证「零配置 → 完全无行为变化」
    "source": "macro/macro_series.parquet",
    "zscore_window": 1260,                  # ~5 年交易日滚动 z（仅用 ≤ as_of 历史，因果安全）
    "hard_defensive_score": -0.50,          # score 低于此 → 硬防御覆盖（无视 price regime 的扩张信号）
    "macro_defensive_cap": 0.40,            # 硬防御时 equity 封顶（低于 equity_ratio_bounds 下限时仍夹到下限）
    "tilt_amp": 0.10,                       # 非硬防御时 equity 按 score×tilt_amp 连续微调（最大幅度）
    # ── walk-forward 门控（calibrate_macro_regime 用）──
    "wf_min_ic": 0.02,                      # 跨折 OOS rankIC 均值须 > 此才采纳（典型因子 IC ~0.02-0.05）
    "wf_min_stable_folds": 3,               # 且 ≥ 此折数的 OOS rankIC 为正（符号稳定）才采纳
    # ── Step2 因果尺（时间序列 DML 门控，替代 OOS rankIC；需 calibrate_macro_regime 传 causal + 开启）──
    "use_causal_gate": False,               # 内置默认关：未传 causal verdict 时不启用，旧 OOS rankIC 路径照旧
    "causal_min_abs_t": 2.0,                # 因果 θ 显著阈值（与因子闸同把尺）
    "causal_min_sign_stability": 0.60,      # 滚动符号稳定性阈值（与因子闸同把尺）
}


def _as_ts(as_of) -> Optional[pd.Timestamp]:
    """把 8 位 YYYYMMDD 或日期串转成 Timestamp（与 market.py 的 as_of 口径一致）。"""
    if as_of is None:
        return None
    s = str(as_of)
    if len(s) == 8 and s.isdigit():
        s = f"{s[:4]}-{s[4:6]}-{s[6:]}"
    try:
        return pd.Timestamp(s)
    except Exception:
        return None


def load_macro_series(path: Optional[str] = None) -> Optional[pd.DataFrame]:
    """读宏观序列 parquet/csv（source-agnostic）。缺失/损坏/无 date 列 → 返回 None（fail-soft）。

    ``path`` 为 None 时默认读 ``STOCK_DATA_DIR / stock_data/macro/macro_series.parquet``。
    返回以 date 为索引、升序的 DataFrame（列见 DEFAULT_MACRO_VARS）。
    """
    try:
        if path is None:
            from smcore.config.defaults import STOCK_DATA_DIR
            path = STOCK_DATA_DIR / DEFAULT_MACRO_CFG["source"]
        p = Path(path)
        if not p.exists():
            return None
        if str(p).endswith(".parquet"):
            df = pd.read_parquet(p)
        else:
            df = pd.read_csv(p)
        if df is None or df.empty or "date" not in df.columns:
            return None
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date").set_index("date")
        return df if len(df) >= 2 else None
    except Exception:
        return None


def compute_macro_regime(
    macro_df: Optional[pd.DataFrame],
    as_of,
    cfg: Optional[dict] = None,
) -> Optional[dict]:
    """由宏观序列算 composite regime（因果安全：只用 ≤ as_of 的历史）。

    返回 ``{"regime": "扩张"|"中性"|"防御", "score": float, "z": {var: z}}``；
    数据不足（<60 交易日历史或缺列）→ 返回 None（fail-soft 由调用方处理）。
    """
    cfg = cfg or DEFAULT_MACRO_CFG
    if macro_df is None or len(macro_df) == 0:
        return None
    as_of_ts = _as_ts(as_of)
    if as_of_ts is None:
        return None
    hist = macro_df.loc[:as_of_ts]
    if len(hist) < 60:
        return None
    last = hist.iloc[-1]
    win = int(cfg.get("zscore_window", 1260) or 1260)
    window = hist.iloc[-win:] if len(hist) > win else hist

    score = 0.0
    wsum = 0.0
    z_detail: dict[str, float] = {}
    for col, vc in DEFAULT_MACRO_VARS.items():
        if col not in hist.columns:
            continue
        vals = pd.to_numeric(window[col], errors="coerce").dropna()
        cur = pd.to_numeric(last.get(col), errors="coerce")
        if vals.empty or len(vals) < 2 or pd.isna(cur):
            continue
        mu = float(vals.mean())
        sd = float(vals.std())
        if sd == 0 or pd.isna(sd):
            continue
        z = float((float(cur) - mu) / sd)
        z = max(-3.0, min(3.0, z))
        s = float(vc["sign"]) * float(vc["weight"])
        score += z * s
        wsum += float(vc["weight"])
        z_detail[col] = round(z, 3)
    if wsum <= 0:
        return None
    score = max(-1.5, min(1.5, score / wsum))
    regime_label = "扩张" if score > 0.25 else ("防御" if score < -0.25 else "中性")
    return {"regime": regime_label, "score": round(float(score), 4), "z": z_detail}


def macro_equity_tilt(
    equity: float,
    macro_state: Optional[dict],
    cfg: Optional[dict] = None,
    bounds=(0.30, 0.95),
):
    """把 price-regime 算出的 equity 按宏观 state 微调，返回 (new_equity, note)。

    - macro_state 为 None → 原样返回（fail-soft）。
    - score ≤ hard_defensive_score → 硬防御覆盖：equity 取 min(equity, cap)，再夹 bounds。
    - 否则 equity += score × tilt_amp，夹 bounds。
    """
    if macro_state is None:
        return float(equity), "macro 状态缺失 → 不倾斜"
    cfg = cfg or DEFAULT_MACRO_CFG
    try:
        score = float(macro_state.get("score", 0.0))
    except (TypeError, ValueError):
        score = 0.0
    lo, hi = float(bounds[0]), float(bounds[-1])
    hard = float(cfg.get("hard_defensive_score", -0.50))
    cap = float(cfg.get("macro_defensive_cap", 0.40))
    amp = float(cfg.get("tilt_amp", 0.10))

    if score <= hard:
        new_eq = min(float(equity), cap)
        new_eq = min(max(new_eq, lo), hi)
        return new_eq, f"宏观深度防御(score={score:.2f}) → 总仓位封顶 {cap:.2f}"
    new_eq = float(equity) + score * amp
    new_eq = min(max(new_eq, lo), hi)
    return new_eq, (
        f"宏观微调(score={score:.2f}, amp={amp:.2f}) → "
        f"总仓位 {float(equity):.2f}→{new_eq:.2f}"
    )


def _rank_ic(a, b) -> Optional[float]:
    """Spearman rank IC（纯 numpy，不依赖 scipy）。样本 <5 返回 None。"""
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    n = len(a)
    if n < 5 or len(b) != n:
        return None
    ra = a.argsort().argsort().astype(float)
    rb = b.argsort().argsort().astype(float)
    da = ra - ra.mean()
    db = rb - rb.mean()
    denom = (da ** 2).sum() ** 0.5 * (db ** 2).sum() ** 0.5
    if denom == 0:
        return None
    return float((da * db).sum() / denom)


def calibrate_macro_regime(
    pairs: list[tuple[float, float]],
    cfg: Optional[dict] = None,
    causal: Optional[dict] = None,
) -> dict:
    """walk-forward 门控：宏观 score 是否对前向收益有稳健预测力，值得采纳。

    参数 ``pairs``：已按日期升序排好的 ``(macro_score, forward_ret%)`` 列表（调用方用
    ``compute_macro_regime`` 逐信号日算出 score，再配该日之后 N 日市场收益）。
    参数 ``causal``：可选，由 ``causal_validation.validate_macro_signal`` 产出的时间序列因果
    verdict（``{t, sign_stability, verdict}``）。当提供且 ``cfg.use_causal_gate=True`` 时，
    **以因果尺替代 OOS rankIC** 作采纳判据（Step2：与因子闸同一把尺，诊断更扎实）。

    旧路径（OOS rankIC，默认）：把 pairs 切成 4 个扩张窗口折，每折后半作 OOS 算 rankIC；
    adopt 当且仅当 OOS rankIC 均值 > wf_min_ic 且 ≥ wf_min_stable_folds 折为正。

    因果尺路径（Step2）：adopt 当且仅当 非 mirage 且 |t|≥causal_min_abs_t 且
    sign_stability≥causal_min_sign_stability（与因子闸同一把尺）。

    返回 ``{"adopt", "mean_ic", "n_positive_folds", "folds", "reason", [causal]}``。
    纯函数、无文件 I/O，便于单测；离线无数据时本函数不被调用。
    """
    cfg = cfg or DEFAULT_MACRO_CFG
    min_ic = float(cfg.get("wf_min_ic", 0.02))
    min_stable = int(cfg.get("wf_min_stable_folds", 3))
    n = len(pairs)
    if n < 20:
        return {"adopt": False, "mean_ic": 0.0, "n_positive_folds": 0,
                "folds": [], "reason": f"样本不足(n={n}<20)"}
    scores = [p[0] for p in pairs]
    rets = [p[1] for p in pairs]
    n_folds = 4
    fold_ics = []
    for k in range(n_folds):
        cut0 = int(n * k / n_folds)
        cut1 = int(n * (k + 1) / n_folds)
        if cut1 - cut0 < 5:
            continue
        # 扩张窗口：in-sample = [0, cut0)，OOS = [cut0, cut1)
        oos_s = scores[cut0:cut1]
        oos_r = rets[cut0:cut1]
        ic = _rank_ic(oos_s, oos_r)
        if ic is not None:
            fold_ics.append(ic)
    mean_ic = float(np.mean(fold_ics)) if fold_ics else 0.0
    n_pos = sum(1 for x in fold_ics if x > 0)
    reason_old = (
        f"mean_OOS_IC={mean_ic:.3f}(阈值{min_ic:.3f}) "
        f"正折{n_pos}/{len(fold_ics)}(需≥{min_stable})"
    )

    # Step2 因果尺：提供 causal 且开启 use_causal_gate → 以因果尺为准
    if causal is not None and cfg.get("use_causal_gate", False):
        min_t = float(cfg.get("causal_min_abs_t", 2.0))
        min_stab = float(cfg.get("causal_min_sign_stability", 0.60))
        t = causal.get("t")
        stab = causal.get("sign_stability")
        verdict = causal.get("verdict")
        adopt = (
            (verdict != "mirage(被混淆吸收)")
            and (np.isfinite(t) and abs(t) >= min_t)
            and (np.isfinite(stab) and stab >= min_stab)
        )
        reason = (
            f"因果尺: t={t:.2f}(≥{min_t}) stab={stab:.2f}(≥{min_stab}) "
            f"verdict={verdict} → {'采纳' if adopt else '拒绝'}"
        )
        return {
            "adopt": adopt,
            "mean_ic": round(mean_ic, 4),
            "n_positive_folds": n_pos,
            "folds": [round(x, 4) for x in fold_ics],
            "reason": reason,
            "causal": {"t": t, "sign_stability": stab, "verdict": verdict},
        }
    adopt_old = (mean_ic > min_ic) and (n_pos >= min_stable)
    return {
        "adopt": adopt_old,
        "mean_ic": round(mean_ic, 4),
        "n_positive_folds": n_pos,
        "folds": [round(x, 4) for x in fold_ics],
        "reason": reason_old,
    }
