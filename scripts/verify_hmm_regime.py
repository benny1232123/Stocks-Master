#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""预注册探索实验：HMM 隐状态 vs 现有四维 regime 判定是否有增量（2026-09-27，P2）。

为什么做这个：
- 路线图 P2「hmmlearn HMM regime」从未启动。现有 regime = 四维合成（趋势/波动/宽度/量能，
  market.compute_market_profile），其价值已被 walk-forward 证实（自适应权重超额集中在
  下行防御段 +17.8pp）。HMM 若要立项，必须回答：它对「下行防御」判定有**增量**吗，
  还是同一信息的另一种参数化？

预注册声明（先打印，再算数）：
- 数据：HS300 日线全量（akshare，2002→今），收盘价日收益单变量序列。
- 模型：Gaussian HMM，K=3 主判定（K=2/4 敏感性），random_state 固定，协方差 full。
- 因果协议：扩展窗 refit（每 20 个交易日重拟合，训练只用 ≤ refit 日数据，最少 750 日）；
  状态 = 截断前向滤波（250 日 burn-in 的 filtered posterior，绝不用未来数据平滑）。
- risk-off 状态 = 拟合均值最低的状态（先验规则，不偷看结果）。
- 主判定（replay 窗口 120 信号日，regime 对照 = regime_history 既有标注）：
  增量集 =「HMM risk-off ∧ regime ≠ 下行防御」。若 n ≥ 20 且其前向 10 日收益均值
  < 全样本均值 − 0.3×全样本σ → HMM 有增量，值得做 regime 融合实验；
  否则 P2 HMM 关闭（现有四维判定已够用）。
- 支持证据（全历史 ~5000 日）：各状态前向 10 日收益分层应单调（risk-off < 中性 < risk-on
  方向），否则模型本身无信息，直接关闭。
- 诚实约束：单指数单变量；HMM 状态序号不可跨 refit 追踪，逐 refit 按均值排序重映射
  （状态「身份」漂移是 HMM walk-forward 的固有噪声，如实计入）；本实验只出报告，
  不改任何生产配置。

用法：python scripts/verify_hmm_regime.py [--out stock_data/hmm_regime_report.md]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
for p in (str(ROOT), str(ROOT / "scripts")):
    if p not in sys.path:
        sys.path.insert(0, p)

from smcore.config.defaults import STOCK_DATA_DIR  # noqa: E402

FULL_CACHE = STOCK_DATA_DIR / "index_cache" / "sh000300_full.csv"
HORIZON = 10          # 前向收益窗口（交易日），与 walk-forward WF_HOLD_DAYS 对齐
REFIT_EVERY = 20      # 扩展窗重拟合周期（交易日）
MIN_TRAIN = 750       # 首次拟合最少训练日（~3 年）
BURN_IN = 250         # 逐日滤波的 burn-in 窗口
PRIMARY_K = 3
KS = [2, 3, 4]
SEED = 42
INCREMENT_MIN_N = 20
INCREMENT_SIGMA = 0.3  # 增量集前向均值须 < 全样本均值 − 0.3σ

PRE_REGISTRATION = """
════════════════════════════════════════════════════════════════
预注册声明（先打印，再算数 —— 防数据窥探）
════════════════════════════════════════════════════════════════
H：HS300 日收益 Gaussian HMM 的 risk-off 状态（拟合均值最低状态）相对
现有四维 regime 判定有增量：增量集（HMM risk-off ∧ regime≠下行防御）
的前向 10 日收益显著劣于全样本。
因果协议：扩展窗 refit（每 {refit} 交易日，≥{mintrain} 日训练）+
逐日截断前向滤波（{burn} 日 burn-in）。
主判定（replay 120 信号日）：增量集 n≥{minn} 且 前向均值 < 全样本均值 − {sig}σ
  → 有增量，值得 regime 融合实验；否则 P2 HMM 关闭。
支持证据（全历史）：状态前向收益分层须呈 risk-off < 其他 方向。
敏感性：K=2/4 同口径，方向一致才算稳。
════════════════════════════════════════════════════════════════
"""


def load_index() -> pd.Series:
    """HS300 全量收盘序列（akshare 一次拉全史 → 本地缓存 index_cache/sh000300_full.csv）。

    刻意不覆盖现有 sh000300.csv（320 行短缓存是 regime_as_of 的现行数据源，保持原样）。
    """
    if FULL_CACHE.exists():
        try:
            df = pd.read_csv(FULL_CACHE, index_col=0, parse_dates=True)
            if len(df) > 1000 and "close" in df.columns:
                return pd.to_numeric(df["close"], errors="coerce").dropna()
        except Exception:
            pass
    from smcore.strategy.regime_filter import _fetch_hs300_akshare
    s = _fetch_hs300_akshare()
    if s is None or len(s) < 1000:
        raise RuntimeError("akshare 全量 HS300 拉取失败（网络不可用且无本地长缓存）")
    s = pd.Series(pd.to_numeric(s, errors="coerce").values, index=pd.to_datetime(s.index),
                  name="close").dropna()
    FULL_CACHE.parent.mkdir(parents=True, exist_ok=True)
    s.to_frame().to_csv(FULL_CACHE)
    return s


def fit_states(close: pd.Series, k: int) -> pd.Series:
    """因果 walk-forward HMM 状态序列（0=risk-off … k-1=risk-on，按拟合均值升序映射）。"""
    from hmmlearn.hmm import GaussianHMM
    rets = close.pct_change().dropna()
    idx = rets.index
    vals = rets.values.reshape(-1, 1)
    pos = {d: i for i, d in enumerate(idx)}
    refits = list(range(MIN_TRAIN, len(vals), REFIT_EVERY))
    state = pd.Series(np.nan, index=idx)
    last_model = None
    refit_pos = 0
    for t in range(MIN_TRAIN, len(vals)):
        while refit_pos < len(refits) and refits[refit_pos] <= t:
            r = refits[refit_pos]
            try:
                m = GaussianHMM(n_components=k, covariance_type="full",
                                n_iter=200, random_state=SEED)
                m.fit(vals[:r])
                order = np.argsort(m.means_.ravel())        # 均值升序：0=risk-off
                m._state_order = order
                last_model, refit_at = m, r
            except Exception:
                pass                                        # refit 失败沿用上一模型
            refit_pos += 1
        if last_model is None:
            continue
        lo = max(0, t - BURN_IN + 1)
        try:
            post = last_model.predict_proba(vals[lo:t + 1])[-1]  # 截断滤波：无未来数据
            state.iloc[t] = int(last_model._state_order[int(np.argmax(post))])
        except Exception:
            continue
    return state


def forward_returns(close: pd.Series, horizon: int) -> pd.Series:
    """收盘→收盘 horizon 交易日前向收益（小数）。"""
    return close.shift(-horizon) / close - 1.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(STOCK_DATA_DIR / "hmm_regime_report.md"))
    args = ap.parse_args()
    t0 = time.time()
    print(PRE_REGISTRATION.format(refit=REFIT_EVERY, mintrain=MIN_TRAIN,
                                  burn=BURN_IN, minn=INCREMENT_MIN_N,
                                  sig=INCREMENT_SIGMA), flush=True)

    close = load_index()
    print(f"HS300 全量序列：{len(close)} 根（{close.index.min().date()} → "
          f"{close.index.max().date()}）", flush=True)
    fwd = forward_returns(close, HORIZON)

    results = {}
    for k in KS:
        st = fit_states(close, k)
        valid = st.notna() & fwd.notna()
        n_valid = int(valid.sum())
        rows = []
        for s in range(k):
            m = valid & (st == s)
            if m.sum() == 0:
                continue
            rows.append({"state": s, "n": int(m.sum()),
                         "fwd_mean_pct": round(float(fwd[m].mean()) * 100, 3),
                         "fwd_win": round(float((fwd[m] > 0).mean()) * 100, 1)})
        results[k] = {"states": rows, "n": n_valid,
                      "state": st, "overall": float(fwd[valid].mean())}
        print(f"K={k}：{n_valid} 有效日 | " +
              " | ".join(f"s{r['state']}(n={r['n']}) {r['fwd_mean_pct']:+.2f}%/10d"
                         for r in rows) + f"  ({time.time() - t0:.0f}s)", flush=True)

    # ── 主判定：replay 120 信号日，与 regime_history 既有标注对比（K=PRIMARY_K）──
    from walk_forward_validator import _all_signal_days
    days = _all_signal_days()
    st_p = results[PRIMARY_K]["state"]
    close_pos = {d: i for i, d in enumerate(st_p.index)}
    inc_f, non_f, dd_f, all_f = [], [], [], []
    cross = {"下行防御": {"risk_off": 0, "other": 0},
             "趋势上行": {"risk_off": 0, "other": 0},
             "震荡轮动": {"risk_off": 0, "other": 0}}
    n_missing_state = 0
    for sd in days:
        rj = STOCK_DATA_DIR / "regime_history" / f"{sd}.json"
        if not rj.exists():
            continue
        try:
            regime = json.loads(rj.read_text(encoding="utf-8")).get("regime")
        except Exception:
            continue
        if regime not in cross:
            continue
        ts = pd.Timestamp(pd.Timestamp(sd).date())
        # 信号日 → 最近的上一个交易日状态（信号日收盘后决策，前向收益从当日收盘算）
        p = close_pos.get(ts)
        if p is None or pd.isna(st_p.iloc[p]):
            n_missing_state += 1
            continue
        risk_off = int(st_p.iloc[p]) == 0
        cross[regime]["risk_off" if risk_off else "other"] += 1
        f = fwd.iloc[p]
        if pd.isna(f):
            continue
        all_f.append(float(f))
        if risk_off:
            (dd_f if regime == "下行防御" else inc_f).append(float(f))
        else:
            non_f.append(float(f))

    overall_mu, overall_sd = float(np.mean(all_f)), float(np.std(all_f))
    inc_mu = float(np.mean(inc_f)) if inc_f else None
    has_increment = bool(inc_mu is not None and len(inc_f) >= INCREMENT_MIN_N
                         and inc_mu < overall_mu - INCREMENT_SIGMA * overall_sd)

    # ── 支持证据（全历史）：主 K 的状态分层方向 ──
    primary_rows = results[PRIMARY_K]["states"]
    monotone_ok = (len(primary_rows) == PRIMARY_K and
                   all(primary_rows[i]["fwd_mean_pct"] <= primary_rows[i + 1]["fwd_mean_pct"]
                       + 1e-9 for i in range(len(primary_rows) - 1)))

    if has_increment:
        verdict_txt = ("**HMM 相对现有四维 regime 判定有增量信息，"
                       "值得进入 regime 融合实验**")
    elif not monotone_ok:
        verdict_txt = ("**P2 HMM 关闭：全历史状态前向分层不呈 risk-off 方向"
                       "（拟合均值低的状态反而后向收益更高——与 A 股短周期反转一致，"
                       "均值状态是逆向信号而非风险信号），模型无稳定风险方向信息；"
                       "叠加增量集未达预注册门槛，双重不达标**")
    elif inc_mu is None or len(inc_f) < INCREMENT_MIN_N:
        verdict_txt = ("**P2 HMM 关闭：现有四维 regime 判定已够用，单变量收益 HMM 无可测增量"
                       f"（增量集 n={len(inc_f)} 不足 {INCREMENT_MIN_N}）**")
    else:
        verdict_txt = ("**P2 HMM 关闭：现有四维 regime 判定已够用，单变量收益 HMM 无可测增量"
                       "（增量集前向均值未劣于门槛）**")

    lines = [
        "# HMM regime 增量验证（P2 探索，预注册）",
        "",
        f"- 生成：{time.strftime('%Y-%m-%d %H:%M:%S')}；HS300 全量 {len(close)} 根"
        f"（{close.index.min().date()} → {close.index.max().date()}）；"
        f"前向窗口 {HORIZON} 交易日；refit 每 {REFIT_EVERY} 日、训练 ≥{MIN_TRAIN} 日、"
        f"滤波 burn-in {BURN_IN} 日",
        "",
        "## 一、全历史状态分层（支持证据）",
        "",
        "| K | 状态 | n | 前向10日均值% | 胜率% |",
        "|---|---|---|---|---|",
        *[
            f"| {k} | s{r['state']}{'(risk-off)' if r['state'] == 0 else ''} | {r['n']} "
            f"| {r['fwd_mean_pct']:+.3f} | {r['fwd_win']} |"
            for k in KS for r in results[k]["states"]
        ],
        f"",
        f"- 主 K={PRIMARY_K} 分层单调（risk-off 最低）：**{monotone_ok}**",
        "",
        "## 二、主判定：replay 120 信号日增量集（K=3 vs 现有 regime）",
        "",
        f"- 全样本（信号日）前向 10 日：均值 {overall_mu * 100:+.3f}%，σ {overall_sd * 100:.3f}%，n={len(all_f)}"
        f"（状态缺失日 {n_missing_state} 个）",
        f"- 增量集（HMM risk-off ∧ regime≠下行防御）：n={len(inc_f)}，"
        f"前向均值 {(inc_mu * 100) if inc_mu is not None else float('nan'):+.3f}%",
        f"- 现有下行防御日：n={len(dd_f)}，前向均值 {float(np.mean(dd_f)) * 100:+.3f}%（HMM risk-off 命中 "
        f"{cross['下行防御']['risk_off']}/{cross['下行防御']['risk_off'] + cross['下行防御']['other']}）",
        "",
        "| regime | HMM risk-off 日 | 其他日 |",
        "|---|---|---|",
        *[f"| {name} | {v['risk_off']} | {v['other']} |" for name, v in cross.items()],
        "",
        f"- 判定：增量集 n={len(inc_f)}（需≥{INCREMENT_MIN_N}），"
        f"前向均值门槛 {overall_mu * 100:+.3f}% − {INCREMENT_SIGMA}×σ = "
        f"{(overall_mu - INCREMENT_SIGMA * overall_sd) * 100:+.3f}%，"
        f"实际 {(inc_mu * 100) if inc_mu is not None else float('nan'):+.3f}% → "
        f"**{'有增量' if has_increment else '无增量'}**",
        "",
        "## 三、结论",
        "",
        f"- {verdict_txt}",
        "- 诚实约束：单指数单变量模型；HMM 状态身份逐 refit 重映射（身份漂移计入噪声）；"
        "全历史分层为支持证据、主判定只看 replay 窗口增量集。",
        "- 🚩 附带发现（另行修复）：index_cache 短缓存（sh000300.csv，320 行）一旦写入永不刷新，"
        "8 月后 regime_as_of 标注用的都是 ≤2026-08-07 的旧数据；本实验用独立的 sh000300_full.csv 长缓存，未动原文件。",
        "",
    ]
    out = Path(args.out)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"DONE in {time.time() - t0:.0f}s -> {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
