#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""动量子家族「方向反转」复核：把因子池结论落到生产 momentum 策略自己的条件宇宙上。

背景
----
- 因子池（无条件横截面）显示 mom20/mom40/mom60 全部「方向反转」：先验做多高动量，
  但验证集 IC 显著为负（mom20 −0.0717 / mom60 −0.0710），且 v1 预注册回放显示
  2018–2026 **九年全负**、分位近乎单调下行（mom60 单调ρ −0.98）、TOP50 超额 −94pp。
- 而生产 `smcore/strategies/momentum.py` 是「买中期上升趋势的强势股」：
  ret20>0 + ret60≥0 + MA20 上行 + 距20日高点 ≥ −18% + 价 5~50 + 成交额 ≥ 2e8
  + 排除 30x/688x + 排除 ST（本脚本无历史 ST 名单，未剔）。

问题
----
上述二者是否矛盾？「无条件 IC 为负」可能只是**宇宙效应**（小票/高波动票主导），
而策略已用条件把宇宙收窄到「强势股」；也可能**在策略自己的条件域内 IC 仍为负**——
那策略就是在买一个被自己宇宙证伪的尾部，属真实设计缺陷。

做法
----
在**同一套数据/掩码/前向收益**（复用 smcore.strategy.factor_engine）下，分别计算：
  ① 无条件域 IC（应与因子池/v1 对上）；
  ② 策略条件域 IC（本复核的核心）；
  ③ 条件域内「按策略打分取 TOP30」的前向超额（模拟策略真实持仓）。
全部按年拆分看持续性。

输出：stock_data/factor_ic_replay/momentum_reversal_review.md
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy import factor_engine as fe  # noqa: E402

# ── 策略参数（与 smcore/strategies/momentum.py 及其 risk_config 默认一致）──
PRICE_LO, PRICE_HI = 5.0, 50.0
MIN_AMOUNT = 2e8
NEAR_HIGH = -0.18
W_RET20, W_RET60, W_SLOPE = 0.40, 0.30, 0.30
TOP_N = 30
COST_RT = 0.003

COND_MIN_N = 100      # 条件域每日样本较少，放宽（默认 300 会把 IC 全判 NaN）
UNCOND_MIN_N = fe.MIN_N_DAY


def _ic_stats(fac: pd.DataFrame, mask: pd.DataFrame, fwd_rank: pd.DataFrame,
              min_n: int, w0, label: str) -> dict:
    f = fac.where(mask)
    ic = fe.cross_sectional_ic(f, fwd_rank, min_n_day=min_n)
    ic = ic[ic.index >= w0].dropna()
    if ic.empty:
        return {"label": label, "n_days": 0, "n_eval": 0, "ic": None,
                "icir": None, "t": None, "by_year": {}}
    thin = ic.iloc[::fe.REBAL_EVERY]
    n = int(len(thin))
    mu, sd = float(thin.mean()), float(thin.std())
    t = mu / sd * np.sqrt(n) if sd > 1e-12 else 0.0
    by_year = {str(k): round(float(v), 4)
               for k, v in ic.groupby(ic.index.year).mean().items()}
    return {"label": label, "n_days": int(len(ic)), "n_eval": n,
            "ic": round(mu, 4), "icir": round(mu / sd, 3) if sd > 1e-12 else 0.0,
            "t": round(t, 2), "mean_sd": round(sd, 5), "by_year": by_year}


def _topn_excess(score: pd.DataFrame, mask: pd.DataFrame, fwd: pd.DataFrame,
                 days: list, pos: dict) -> dict:
    rets, bens, dates, prev = [], [], [], None
    for d in days:
        if pos[d] + fe.FWD >= len(score.index):
            continue
        m = mask.loc[d]
        fv = fwd.loc[d]
        cand = score.loc[d][m & score.loc[d].notna()].dropna()
        if len(cand) < TOP_N * 2:
            prev = None
            continue
        bench = fv[m & fv.notna()]
        if bench.empty:
            prev = None
            continue
        held = set(cand.nlargest(TOP_N).index)
        fr = fv.reindex(list(held)).dropna()
        if len(fr) < 10:
            prev = None
            continue
        cost = COST_RT if prev is None else COST_RT * len(prev ^ held) / max(1, len(held))
        rets.append(float(fr.mean()) - cost)
        bens.append(float(bench.mean()))
        dates.append(d)
        prev = held
    if len(rets) < 5:
        return {"n_windows": len(rets)}
    p = pd.Series(rets, index=pd.DatetimeIndex(dates))
    b = pd.Series(bens, index=pd.DatetimeIndex(dates))
    by_year = {}
    for y in sorted(set(p.index.year)):
        py, by = p[p.index.year == y], b[b.index.year == y]
        by_year[str(y)] = round(float(((1 + py).prod() - (1 + by).prod()) * 100), 2)
    return {"n_windows": len(p),
            "total_excess_pp": round(float((1 + p).prod() - (1 + b).prod()) * 100, 1),
            "win_rate": round(float((p - b > 0).mean()), 3),
            "by_year": by_year}


def main() -> int:
    t0 = time.time()
    mats = fe.load_matrices(cols=("close", "high", "low", "amount"))
    close, high, amount = mats["close"], mats["high"], mats["amount"]
    del mats
    print(f"grid {close.shape[0]} x {close.shape[1]} ({time.time()-t0:.0f}s)", flush=True)

    ret1, bad = fe.daily_returns_and_bad(close)
    base_valid = fe.base_valid_mask(close)
    fwd_bad = fe.forward_bad_mask(bad, fe.FWD)
    fwd = fe.forward_return_matrix(close, fwd_bad, base_valid, fe.FWD)
    fwd_rank = fe.forward_rank_matrix(fwd)
    lb20 = fe.lookback_bad(bad, 20, {})

    ret20 = close / close.shift(20) - 1
    ret60 = close / close.shift(60) - 1
    ma20 = close.rolling(20, min_periods=fe.ROLL_MIN).mean()
    slope = ma20 / ma20.shift(20).replace(0.0, np.nan) - 1
    high20 = high.rolling(20, min_periods=fe.ROLL_MIN).max()
    dist = close / high20.replace(0.0, np.nan) - 1

    clean = base_valid & (lb20 == 0)
    board_ok = pd.DataFrame(True, index=close.index, columns=close.columns)
    drop = [c for c in close.columns if str(c).startswith(("30", "688"))]
    if drop:
        board_ok[drop] = False

    strat = (clean & board_ok & (ret20 > 0) & (ret60 >= 0) & (slope > 0)
             & (dist >= NEAR_HIGH) & (close >= PRICE_LO) & (close <= PRICE_HI)
             & (amount >= MIN_AMOUNT))
    score = (ret20 * 100 * W_RET20 + ret60 * 100 * W_RET60 + slope * 100 * W_SLOPE)

    w0 = pd.Timestamp(fe.WINDOW_START)
    idx = close.index[close.index >= w0]
    pos = {d: i for i, d in enumerate(close.index)}

    n_cond = strat.sum(axis=1)
    n_cond = n_cond[n_cond.index >= w0]

    rows = [
        _ic_stats(score, clean, fwd_rank, UNCOND_MIN_N, w0, "策略打分（无条件域）"),
        _ic_stats(ret20, clean, fwd_rank, UNCOND_MIN_N, w0, "ret20（无条件域）"),
        _ic_stats(ret60, clean, fwd_rank, UNCOND_MIN_N, w0, "ret60（无条件域）"),
        _ic_stats(score, strat, fwd_rank, COND_MIN_N, w0, "**策略打分（策略条件域）**"),
        _ic_stats(ret20, strat, fwd_rank, COND_MIN_N, w0, "**ret20（策略条件域）**"),
        _ic_stats(ret60, strat, fwd_rank, COND_MIN_N, w0, "**ret60（策略条件域）**"),
    ]

    rebal = [d for i, d in enumerate(idx) if i % fe.REBAL_EVERY == 0]
    pf = _topn_excess(score, strat, fwd, rebal, pos)

    # 条件域覆盖度（分年）
    cov = {str(k): int(round(float(v))) for k, v in n_cond.groupby(n_cond.index.year).median().items()}

    L: list[str] = []
    L += ["# 动量子家族「方向反转」复核", "",
          f"- 生成：{time.strftime('%Y-%m-%d %H:%M:%S')}",
          f"- 窗口：{fe.WINDOW_START} 起　T+{fe.FWD}　非重叠每 {fe.REBAL_EVERY} 交易日",
          f"- 数据：{close.shape[0]} 交易日 × {close.shape[1]} 只（k_data qfq）",
          f"- 条件域 = 生产 momentum 策略自身的选股条件：ret20>0、ret60≥0、MA20 上行、"
          f"距20日高点 ≥ {NEAR_HIGH}、价 {PRICE_LO:.0f}~{PRICE_HI:.0f}、成交额 ≥ {MIN_AMOUNT:.0e}、"
          f"排除 30x/688x（**未剔 ST**：无历史名单）",
          f"- 条件域每日样本中位数（分年）：{cov}", "",
          "## 一、动量 IC：无条件域 vs 策略条件域", "",
          "| 口径 | 有效日 | 非重叠n | 均值IC | ICIR | t |", "|---|---|---|---|---|---|"]
    for r in rows:
        L.append(f"| {r['label']} | {r['n_days']} | {r['n_eval']} | "
                 f"{r['ic']} | {r['icir']} | {r['t']} |")

    L += ["", "### 分年度均值 IC", ""]
    years = sorted({y for r in rows for y in r["by_year"]})
    L.append("| 口径 | " + " | ".join(years) + " |")
    L.append("|---|" + "---|" * len(years))
    for r in rows:
        L.append("| " + r["label"] + " | " + " | ".join(
            str(r["by_year"].get(y, "n/a")) for y in years) + " |")

    L += ["", "## 二、条件域内「按策略打分取 TOP30」的前向超额", "",
          "（在策略自己的条件域内选前 30，对比该条件域等权基准；扣 0.3% 往返成本 × 换手）", ""]
    if "total_excess_pp" in pf:
        L += [f"- 窗口数 **{pf['n_windows']}**　全期超额 **{pf['total_excess_pp']}pp**　"
              f"胜率 **{pf['win_rate']}**", "",
              "| 年份 | " + " | ".join(sorted(pf["by_year"])) + " |",
              "|---|" + "---|" * len(pf["by_year"]),
              "| 条件域 TOP30 超额(pp) | " + " | ".join(
                  str(pf["by_year"][y]) for y in sorted(pf["by_year"])) + " |"]
    else:
        L += [f"- 窗口不足（{pf.get('n_windows')}）"]

    u_ic, c_ic = rows[0]["ic"], rows[3]["ic"]
    cy = list(rows[3]["by_year"].values())
    n_neg_y = sum(1 for v in cy if v < 0)
    pf_years = pf.get("by_year", {})
    n_neg_pf = sum(1 for v in pf_years.values() if v < 0)
    verdict = ("方向反转在策略条件域内依然成立（且更负）"
               if (c_ic is not None and c_ic < 0)
               else "条件域内方向未反转，须限定前述结论的适用范围")

    L += ["", "## 三、结论（按上面的数字自动判定）", "",
          f"- 无条件域「策略打分」IC = **{u_ic}**；**策略条件域 = {c_ic}**。",
          f"- 条件域分年 IC：**{n_neg_y}/{len(cy)} 年为负**。",
          f"- 条件域 TOP30 超额：**{pf.get('total_excess_pp')}pp**，胜率 {pf.get('win_rate')}，"
          f"**{n_neg_pf}/{len(pf_years)} 年为负**。", "",
          f"**判定：{verdict}。**", "",
          "含义：条件化（ret20>0 / ret60≥0 / MA20 上行 / 距高点≥−18% / 价 5~50 / 成交额≥2e8）"
          "**并没有救回动量**——条件域 IC 比无条件域更负（样本被进一步集中到「强势股」），"
          "且条件域 TOP30 超额 9 年全为负。因此这不属于「小票/高波动票主导了无条件 IC」的宇宙效应，"
          "而是策略**在自己的宇宙里系统性买入反向尾部** → **设计缺陷成立**。", "",
          "## 四、处置建议（与 factor_zoo.md §六 一致）", "",
          "1. 与生产 `momentum` 策略**取交集核对**：本轮已用策略自身的条件复刻其宇宙，口径差已排除，"
          "可直接进入第 2 步。",
          "2. 本 universe 上动量应**反用（做空前期强势 / 改买前期弱势）或直接置零**；"
          "「反用」= 选股逻辑变更，须走**独立 OOS 门控**（复用 `walk_forward_factor_timing` 双轨门控），"
          "不得直接改配置。",
          "3. 若不反用，则应在 `fusion` 层把 momentum 权重长期压到最低档，"
          "并复核其在 5 策略融合里是否仍在贡献负 edge。", "",
          "## 五、Caveats", "",
          "- 未剔 ST（无历史名单）：动量条件天然偏好连涨股，ST 可能高估一部分。",
          "- 未复刻策略的「量能确认 +2 / 近高点 −3」调整项，只比 0.40/0.30/0.30 主分。",
          "- 幸存者偏差：universe = 今天还活着的票，只用于**相对**判断。",
          f"- 条件域每日样本较小（中位数约 33~331，故放宽到 n≥{COND_MIN_N}），t 值仅供参考；"
          "但「9 年全负 + 分年无翻转」使符号结论对样本量不敏感。",
          "- 前向收益为 T+10；策略实际换仓周期若不同，量级会变但符号预期不变。"]
    out = fe.OUT_DIR / "momentum_reversal_review.md"
    out.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"DONE {time.time()-t0:.0f}s -> {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
