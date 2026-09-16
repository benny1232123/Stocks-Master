#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""liq20 / illiq20 阶段 0 诊断（零生产改动）。

依据：``stock_data/factor_ic_replay/liq_illiq_integration_plan.md`` §三 阶段 0。
该文档要求先回答三个问题，才允许讨论接入生产（方案 A/B/C）：

  ① **增量性**：liq/illiq 的 TOP50 与现有 5 策略选票重叠多少？几乎不重叠才算新信息。
  ② **残差 alpha**：把 liq/illiq 对 vol20（及彼此）做横截面秩残差化后，IC 是否仍显著？
     —— 决定它是「独立暴露」还是「已有风险因子的另一种写法」。
  ③ **可交易性折价**：在 TOP50 上逐条施加可交易过滤（成交额 / 价格 / 停牌涨跌停锁死 / ST），
     报告超额衰减曲线 —— 这就是文档 §1.2 所述「三重折扣」的量级。

预注册通过线（文档 §三 阶段 0，**先写死再看结果**）：
  施加**全部**可交易过滤后，TOP50 全期超额仍 **> 0**，且分年 **≥ 3/4 为正**。
  不通过 → 直接终止（结论：liq/illiq 的超额不可交易）。

与既有脚本的分工（避免重复造轮子）：
  - `simple_strategy.py`        —— 单因子 TOP-N 逐换仓回测
  - `illiq20_oos_gate.py`       —— illiq20 的 OOS 闸门（净年化 / IC 显著 / 近段 > 0）
  - `tail_exclusion_ab.py`      —— 负尾剔除层 A/B
  - **本脚本**                   —— 只做上述三问（增量性 / 残差 / 可交易折价）

⚠️ 只读 k_data parquet，不写任何缓存；不改任何生产配置。
⚠️ liq20 与 illiq20 是**同一个自由度**（文档 §1.1），本脚本所有汇总**不得**把它们当两个因子加总。
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy import factor_engine as fe  # noqa: E402

# ── 预注册常数（本脚本新增，全部集中在此，禁散落 magic number）──────────────
AMT_FLOOR = 5e7          # 可交易过滤 F1：当日 20 日均成交额下限（5000 万）
TRADE_PRICE_LO = 5.0     # 可交易过滤 F2：价格下限（元）
LOCK_RATIO = 0.98        # 涨跌停锁死判定：|日收益| ≥ 板内幅度 × 0.98 且 high == low
ST_NAME_TOKEN = "ST"     # 可交易过滤 F4：名称含 ST（现名单近似，无历史名单）
POOL_MIN_N = fe.MIN_N_DAY    # 候选池下限
THIN_KEEP = 10               # 过滤后保留票数 < 该值 ⇒ 记为「薄仓」（不足以等权建仓）

FILTER_LEVELS = ("F0", "F1", "F2", "F3", "F4")
FILTER_DESC = {
    "F0": "无过滤（基线=报告口径）",
    "F1": f"剔除当日 20 日均成交额 < {AMT_FLOOR:.0e}",
    "F2": f"+ 剔除价格 < {TRADE_PRICE_LO:.0f} 元",
    "F3": "+ 剔除停牌 / 涨跌停锁死",
    "F4": f"+ 剔除名称含 {ST_NAME_TOKEN}（现名单近似）",
}
PASS_MIN_POS_FRAC = 0.75  # 分年 ≥ 3/4 为正


# ── 小工具 ──────────────────────────────────────────────────────────────
def _norm_code(c) -> str:
    """统一成 6 位数字代码（DAL / 名称表里可能是 int 或缺失前导零）。"""
    s = str(c).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit():
        return s.zfill(6)
    return s


def _load_names() -> pd.Series:
    """code -> name（现名单）。缺失则返回空 Series（ST 过滤退化为不过滤）。"""
    p = fe.STOCK_DATA_DIR / "stock_info_a_code_name.csv"
    if not p.exists():
        return pd.Series(dtype=str)
    d = pd.read_csv(p, dtype=str)
    d = d.dropna(subset=["code"])
    d["code"] = d["code"].map(_norm_code)
    return d.set_index("code")["name"].fillna("")


def _load_dal_picks() -> dict[pd.Timestamp, set[str]]:
    """Daily-Action-List-*.csv -> {信号日: 生产实际选票代码集合}。"""
    out: dict[pd.Timestamp, set[str]] = {}
    for f in sorted(fe.STOCK_DATA_DIR.glob("Daily-Action-List-*.csv")):
        stem = f.stem.replace("Daily-Action-List-", "")
        if len(stem) != 8 or not stem.isdigit():
            continue
        try:
            d = pd.read_csv(f, dtype=str)
        except Exception:
            continue
        if "股票代码" not in d.columns or d.empty:
            continue
        codes = {_norm_code(c) for c in d["股票代码"].dropna()}
        if codes:
            out[pd.Timestamp(f"{stem[:4]}-{stem[4:6]}-{stem[6:]}")] = codes
    return out


def _ic_stats(ic: pd.Series, w0) -> dict:
    """IC 序列 -> 均值 / ICIR / 非重叠 t（与因子池同口径：T+10 重叠必须抽样）。"""
    ic = ic[ic.index >= w0].dropna()
    if ic.empty:
        return {"n_days": 0, "n_eval": 0, "ic": None, "icir": None, "t": None, "by_year": {}}
    thin = ic.iloc[::fe.REBAL_EVERY]
    n = int(len(thin))
    mu, sd = float(thin.mean()), float(thin.std())
    return {"n_days": int(len(ic)), "n_eval": n,
            "ic": round(mu, 4),
            "icir": round(mu / sd, 3) if sd > 1e-12 else 0.0,
            "t": round(mu / sd * np.sqrt(n), 2) if sd > 1e-12 else 0.0,
            "by_year": {str(k): round(float(v), 4)
                        for k, v in ic.groupby(ic.index.year).mean().items()}}


def _partial_ic_series(x: pd.DataFrame, y_rank: pd.DataFrame, controls: list[pd.DataFrame],
                       min_n: int) -> pd.Series:
    """逐日「对 controls 做横截面秩残差化」后的 IC（部分相关），返回日序列。

    在**秩空间**上做 OLS 残差：先对 x 与控制变量取横截面百分位秩，
    对当日有效样本解最小二乘 x = Z·b + e，再取 corr(e, y_rank)。
    单控制时与解析式部分相关等价，多控制时即标准偏相关。
    """
    out = {}
    for d in x.index:
        xr = x.loc[d].rank(pct=True)
        yr = y_rank.loc[d]
        cols = [c.loc[d].rank(pct=True) for c in controls]
        m = xr.notna() & yr.notna()
        for c in cols:
            m &= c.notna()
        if int(m.sum()) < min_n:
            continue
        xs = xr[m].to_numpy(dtype=float)
        ys = yr[m].to_numpy(dtype=float)
        Z = np.column_stack([c[m].to_numpy(dtype=float) for c in cols])
        Z = np.column_stack([np.ones(len(Z)), Z])          # 截距
        try:
            beta, *_ = np.linalg.lstsq(Z, xs, rcond=None)
            ex = xs - Z @ beta
        except np.linalg.LinAlgError:
            continue
        ex = ex - ex.mean()
        ys = ys - ys.mean()
        den = np.sqrt(float(ex @ ex) * float(ys @ ys))
        if den <= 1e-24:
            continue
        out[d] = float(ex @ ys) / den
    return pd.Series(out).sort_index()


def _excess_by_filter(score: pd.DataFrame, side: str, fwd: pd.DataFrame,
                      uni_mask: pd.DataFrame, filt: dict[str, pd.DataFrame],
                      days: list) -> dict:
    """逐条过滤后的超额衰减曲线：**过滤选中票**（不重选），基准=全有效宇宙等权。

    side="low"（liq20：做多低流动性→取最小）/ "high"（illiq20：做多高非流动性→取最大）。
    成本与 simple_strategy / momentum_review 同口径：COST_RT × 相邻信号日换手率；
    换手率在**该过滤级自己的保留集合**上计算（集合被过滤变小后换手率上升，符合实感）。
    """
    res = {lv: {"ret": [], "ben": [], "dates": [], "kept": [], "thin": []}
           for lv in FILTER_LEVELS}
    prev: dict[str, set | None] = {lv: None for lv in FILTER_LEVELS}
    for d in days:
        m = uni_mask.loc[d]
        cand = score.loc[d][m & score.loc[d].notna()].dropna()
        bench_s = fwd.loc[d]
        bench = bench_s[m & bench_s.notna()]
        if len(cand) < fe.TOP_N * 2 or bench.empty:
            for lv in FILTER_LEVELS:
                prev[lv] = None
            continue
        base_picked = set(cand.nsmallest(fe.TOP_N).index if side == "low"
                          else cand.nlargest(fe.TOP_N).index)
        for lv in FILTER_LEVELS:
            picked = base_picked if lv == "F0" else {
                c for c in base_picked if bool(filt[lv].loc[d].get(c, False))}
            # ⚠️ 「保留票数」必须**无条件记录**：过滤把整个选票集打空本身就是核心结论，
            #    不能因为样本不足就 continue —— 那会把「一只都买不进」伪装成「无数据」。
            res[lv]["kept"].append(len(picked))
            res[lv]["thin"].append(len(picked) < THIN_KEEP)
            if not picked:
                prev[lv] = None
                continue
            fr = bench_s.reindex(sorted(picked)).dropna()
            if fr.empty:
                prev[lv] = None
                continue
            turn = 1.0 if prev[lv] is None else len(prev[lv] ^ picked) / max(1, len(picked))
            res[lv]["ret"].append(float(fr.mean()) - fe.COST_RT * turn)
            res[lv]["ben"].append(float(bench.mean()))
            res[lv]["dates"].append(d)
            prev[lv] = picked

    out = {}
    for lv in FILTER_LEVELS:
        r = res[lv]
        kept = r["kept"]
        ent: dict = {"n_windows_total": len(kept),
                     "n_windows": len(r["ret"]),
                     "n_empty": int(sum(1 for k in kept if k == 0)),
                     "n_thin": int(sum(1 for k in kept if k < THIN_KEEP)),
                     "kept_median": int(np.median(kept)) if kept else 0,
                     "kept_frac": round(float(np.mean(kept)) / fe.TOP_N, 4) if kept else 0.0}
        if len(r["ret"]) < 5:
            out[lv] = ent
            continue
        p = pd.Series(r["ret"], index=pd.DatetimeIndex(r["dates"]))
        b = pd.Series(r["ben"], index=pd.DatetimeIndex(r["dates"]))
        by_year = {str(y): round(float(((1 + p[p.index.year == y]).prod()
                                        - (1 + b[b.index.year == y]).prod()) * 100), 2)
                   for y in sorted(set(p.index.year))}
        ent.update({
            "excess_pp": round(float(((1 + p).prod() - (1 + b).prod()) * 100), 1),
            "win_rate": round(float((p - b > 0).mean()), 3),
            "net_ann": round(float(((1 + p).prod() ** (244.0 / (len(p) * fe.FWD)) - 1) * 100), 2),
            "by_year": by_year,
        })
        out[lv] = ent
    return out


def main() -> int:
    t0 = time.time()
    mats = fe.load_matrices(cols=("close", "high", "low", "amount", "volume"))
    close, high, low, amount, volume = (
        mats["close"], mats["high"], mats["low"], mats["amount"], mats["volume"])
    del mats
    print(f"grid {close.shape[0]} x {close.shape[1]} ({time.time()-t0:.0f}s)", flush=True)

    ret1, bad = fe.daily_returns_and_bad(close)
    base_valid = fe.base_valid_mask(close)
    fwd_bad = fe.forward_bad_mask(bad, fe.FWD)
    fwd = fe.forward_return_matrix(close, fwd_bad, base_valid, fe.FWD)
    fwd_rank = fe.forward_rank_matrix(fwd)

    raw = fe.build_price_factors(close, high, low, amount, ret1)
    fac, fac_valid = fe.apply_factor_validity(raw, base_valid, bad)
    del raw
    liq, illiq, vol = fac["liq20"], fac["illiq20"], fac["vol20"]

    # 用于诊断的辅助量
    amt20 = amount.rolling(20, min_periods=fe.ROLL_MIN).mean().where(lambda x: x > 0)
    lim = fe.limit_series(close.columns)
    lim_mat = pd.DataFrame(np.tile(lim.values, (close.shape[0], 1)),
                           index=close.index, columns=close.columns)

    # ── 可交易过滤掩码（逐条累进）─────────────────────────────────────
    f1 = (amt20 >= AMT_FLOOR).fillna(False)
    f2 = f1 & (close >= TRADE_PRICE_LO)
    suspended = volume.isna() | (volume <= 0)
    locked = (ret1.abs() >= lim_mat * LOCK_RATIO) & (high == low)
    f3 = f2 & (~suspended) & (~locked)
    names = _load_names()
    if len(names):
        st_codes = {c for c, n in names.items() if ST_NAME_TOKEN in str(n).upper()}
    else:
        st_codes = set()
    st_mask = pd.DataFrame(True, index=close.index, columns=close.columns)
    hit = [c for c in close.columns if str(c) in st_codes]
    if hit:
        st_mask[hit] = False
    f4 = f3 & st_mask
    filt = {"F1": f1, "F2": f2, "F3": f3, "F4": f4}
    print(f"filters ready: ST names hit={len(hit)} ({time.time()-t0:.0f}s)", flush=True)

    w0 = pd.Timestamp(fe.WINDOW_START)
    idx = close.index[close.index >= w0]
    days = [d for i, d in enumerate(idx) if i % fe.REBAL_EVERY == 0]
    uni = base_valid & (bad == 0) & (fwd_bad == 0) & fwd.notna()
    print(f"rebal days={len(days)} ({time.time()-t0:.0f}s)", flush=True)

    # ── ① 增量性：与生产选票 / 动量条件域的重叠 ──────────────────────
    dal = _load_dal_picks()
    m_ret20 = close / close.shift(20) - 1
    m_ret60 = close / close.shift(60) - 1
    ma20 = close.rolling(20, min_periods=fe.ROLL_MIN).mean()
    slope = ma20 / ma20.shift(20).replace(0.0, np.nan) - 1
    high20 = high.rolling(20, min_periods=fe.ROLL_MIN).max()
    dist = close / high20.replace(0.0, np.nan) - 1
    board_ok = pd.DataFrame(True, index=close.index, columns=close.columns)
    drop_b = [c for c in close.columns if str(c).startswith(("30", "688"))]
    if drop_b:
        board_ok[drop_b] = False
    strat_mask = (base_valid & (fe.lookback_bad(bad, 20, {}) == 0) & board_ok
                  & (m_ret20 > 0) & (m_ret60 >= 0) & (slope > 0) & (dist >= -0.18)
                  & (close >= 5.0) & (close <= 50.0) & (amount >= 2e8))

    overlap = {"dal_days_total": len(dal), "dal_days_matched": 0,
               "liq20": [], "illiq20": [], "strat_jaccard": {"liq20": [], "illiq20": []}}
    for d, codes in sorted(dal.items()):
        if d not in close.index:
            continue
        m = uni.loc[d]
        if int(m.sum()) < POOL_MIN_N:
            continue
        overlap["dal_days_matched"] += 1
        for name, f, side in (("liq20", liq, "low"), ("illiq20", illiq, "high")):
            cand = f.loc[d][m & f.loc[d].notna()].dropna()
            if len(cand) < fe.TOP_N * 2:
                continue
            top = set(cand.nsmallest(fe.TOP_N).index if side == "low"
                      else cand.nlargest(fe.TOP_N).index)
            top = {_norm_code(c) for c in top}
            inter = len(top & codes)
            overlap[name].append({"date": str(d.date()), "inter": inter,
                                  "recall": inter / max(1, len(codes)),
                                  "precision": inter / max(1, len(top)),
                                  "n_dal": len(codes)})
        for name, f, side in (("liq20", liq, "low"), ("illiq20", illiq, "high")):
            cand = f.loc[d][m & f.loc[d].notna()].dropna()
            sm = strat_mask.loc[d]
            if len(cand) < fe.TOP_N * 2 or int(sm.sum()) < 10:
                continue
            top = set(cand.nsmallest(fe.TOP_N).index if side == "low"
                      else cand.nlargest(fe.TOP_N).index)
            sset = set(sm[sm].index)
            j = len(top & sset) / max(1, len(top | sset))
            overlap["strat_jaccard"][name].append(j)

    def _agg(rows, key):
        vals = [r[key] for r in rows] if rows and isinstance(rows[0], dict) else rows
        if not vals:
            return {"mean": None, "median": None, "n": 0}
        return {"mean": round(float(np.mean(vals)), 4),
                "median": round(float(np.median(vals)), 4), "n": len(vals)}

    overlap_summary = {
        "dal_days_total": overlap["dal_days_total"],
        "dal_days_matched": overlap["dal_days_matched"],
        "liq20_recall": _agg(overlap["liq20"], "recall"),
        "illiq20_recall": _agg(overlap["illiq20"], "recall"),
        "liq20_precision": _agg(overlap["liq20"], "precision"),
        "illiq20_precision": _agg(overlap["illiq20"], "precision"),
        "liq20_jaccard_vs_strat": _agg(overlap["strat_jaccard"]["liq20"], None),
        "illiq20_jaccard_vs_strat": _agg(overlap["strat_jaccard"]["illiq20"], None),
    }
    print(f"overlap done ({time.time()-t0:.0f}s)", flush=True)

    # ── ② 残差 alpha：对 vol20 / 彼此做秩残差 ────────────────────────
    min_n_hi = fe.MIN_N_DAY
    resid = {
        "liq20_raw": _ic_stats(fe.cross_sectional_ic(liq, fwd_rank, min_n_hi), w0),
        "illiq20_raw": _ic_stats(fe.cross_sectional_ic(illiq, fwd_rank, min_n_hi), w0),
        "vol20_raw": _ic_stats(fe.cross_sectional_ic(vol, fwd_rank, min_n_hi), w0),
        "liq20__given_vol20": _ic_stats(
            _partial_ic_series(liq, fwd_rank, [vol], min_n_hi), w0),
        "illiq20__given_vol20": _ic_stats(
            _partial_ic_series(illiq, fwd_rank, [vol], min_n_hi), w0),
        "liq20__given_illiq20": _ic_stats(
            _partial_ic_series(liq, fwd_rank, [illiq], min_n_hi), w0),
        "liq20__given_vol20_illiq20": _ic_stats(
            _partial_ic_series(liq, fwd_rank, [vol, illiq], min_n_hi), w0),
    }
    # liq20 与 illiq20 的横截面秩相关（1 自由度的直接证据）
    rl, ri = liq.rank(axis=1, pct=True), illiq.rank(axis=1, pct=True)
    mm = rl.notna() & ri.notna()
    a, b = rl.where(mm), ri.where(mm)
    am, bm = a.mean(axis=1), b.mean(axis=1)
    cov = (a * b).mean(axis=1) - am * bm
    va = (a * a).mean(axis=1) - am * am
    vb = (b * b).mean(axis=1) - bm * bm
    rho = (cov / np.sqrt(np.maximum(va * vb, 1e-24)))[mm.sum(axis=1) >= min_n_hi]
    rho = rho[rho.index >= w0].dropna()
    mutual_rho = {"mean": round(float(rho.mean()), 4), "median": round(float(rho.median()), 4),
                  "n": int(len(rho))}
    print(f"residual done ({time.time()-t0:.0f}s)", flush=True)

    # ── ③ 可交易性折价曲线 ─────────────────────────────────────────
    decay = {"liq20": _excess_by_filter(liq, "low", fwd, uni, filt, days),
             "illiq20": _excess_by_filter(illiq, "high", fwd, uni, filt, days)}
    print(f"decay done ({time.time()-t0:.0f}s)", flush=True)

    # ── 判定 ───────────────────────────────────────────────────────
    verdict = {}
    for name in ("liq20", "illiq20"):
        d = decay[name]
        f0r, f1r, f4r = d.get("F0", {}), d.get("F1", {}), d.get("F4", {})
        ex = f4r.get("excess_pp")
        by = f4r.get("by_year", {})
        pos = sum(1 for v in by.values() if v > 0)
        frac = pos / len(by) if by else 0.0
        ok = (ex is not None and ex > 0 and frac >= PASS_MIN_POS_FRAC)
        verdict[name] = {
            "excess_pp_ALL_FILTERED": ex, "pos_years": pos, "n_years": len(by),
            "pos_frac": round(frac, 3), "pass": bool(ok),
            # 可交易性崩塌的三个旁证（判定「不可交易」的证据链）
            "kept_frac_F0": f0r.get("kept_frac"), "kept_frac_F1": f1r.get("kept_frac"),
            "n_empty_F1": f1r.get("n_empty"), "n_windows_total": f1r.get("n_windows_total"),
            "unassessable": ex is None,
        }

    payload = {"generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
               "window_start": fe.WINDOW_START, "fwd": fe.FWD, "rebal_every": fe.REBAL_EVERY,
               "top_n": fe.TOP_N, "cost_rt": fe.COST_RT,
               "grid": [int(close.shape[0]), int(close.shape[1])],
               "filters": FILTER_DESC, "overlap": overlap_summary,
               "mutual_rho_liq_illiq": mutual_rho, "residual_ic": resid,
               "decay": decay, "verdict": verdict}
    fe.OUT_DIR.mkdir(parents=True, exist_ok=True)
    (fe.OUT_DIR / "liq_illiq_phase0_diagnostic.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _tbl(d: dict) -> list[str]:
        L = ["| 过滤级 | 说明 | 可评估窗口/总 | 保留票中位 | 空仓窗口 | 全期超额(pp) | 净年化% | 胜率 | 分年(>0) |",
             "|---|---|---|---|---|---|---|---|---|"]
        for lv in FILTER_LEVELS:
            r = d.get(lv, {})
            head = (f"| {lv} | {FILTER_DESC[lv]} | {r.get('n_windows', 0)}/{r.get('n_windows_total', 0)} | "
                    f"{r.get('kept_median', 0)}/50 | {r.get('n_empty', 0)} | ")
            if "excess_pp" not in r:
                L.append(head + "**不可评估** | — | — | — |")
                continue
            by = r["by_year"]
            L.append(head + f"**{r['excess_pp']}** | {r['net_ann']} | {r['win_rate']} | "
                     f"{sum(1 for v in by.values() if v > 0)}/{len(by)} |")
        return L

    L: list[str] = []
    L += ["# liq20 / illiq20 阶段 0 诊断（增量性 / 残差 / 可交易性折价）", "",
          f"- 生成：{payload['generated_at']}",
          f"- 窗口：{fe.WINDOW_START} 起　T+{fe.FWD}　换仓每 {fe.REBAL_EVERY} 交易日　"
          f"TOP{fe.TOP_N}　成本 {fe.COST_RT}",
          "- 依据：`liq_illiq_integration_plan.md` §三 阶段 0（**零生产改动**）",
          "- ⚠️ liq20 与 illiq20 是**同一个自由度**（§1.1）；下表并列只为对照，**不得加总**。", ""]

    L += ["## 一、增量性：与生产选票 / 动量条件域的重叠", "",
          f"- Daily-Action-List 共 **{overlap_summary['dal_days_total']}** 天，"
          f"其中 **{overlap_summary['dal_days_matched']}** 天可与 k_data 对齐（信号日在网格内）。",
          "- `recall` = 生产选票中被 liq/illiq TOP50 覆盖的比例；"
          "`precision` = TOP50 里命中生产选票的比例；`jaccard` = 与**动量条件域**（复刻 momentum.py）的集合交并比。", "",
          "| 因子 | 对齐天数 | recall 均值 | recall 中位 | precision 均值 | Jaccard vs 动量条件域 均值 |",
          "|---|---|---|---|---|---|"]
    for nm in ("liq20", "illiq20"):
        L.append(f"| {nm} | {overlap_summary[f'{nm}_recall']['n']} | "
                 f"{overlap_summary[f'{nm}_recall']['mean']} | "
                 f"{overlap_summary[f'{nm}_recall']['median']} | "
                 f"{overlap_summary[f'{nm}_precision']['mean']} | "
                 f"{overlap_summary[f'{nm}_jaccard_vs_strat']['mean']} |")

    L += ["", "## 二、残差 alpha：对 vol20 / 彼此的秩残差", "",
          f"- liq20 与 illiq20 的横截面秩相关：**均值 {mutual_rho['mean']}**、"
          f"中位 {mutual_rho['median']}（n={mutual_rho['n']}）→ 近似镜像 ⇒ 1 个自由度。", "",
          "| 口径 | 有效日 | 非重叠n | 均值IC | ICIR | t |", "|---|---|---|---|---|---|"]
    for k, v in resid.items():
        L.append(f"| {k} | {v['n_days']} | {v['n_eval']} | {v['ic']} | {v['icir']} | {v['t']} |")

    L += ["", "## 三、可交易性折价曲线（逐条过滤选中票，不重选）", "",
          "基准 = 全有效宇宙等权（不随过滤变化）；过滤只作用于**选中的 TOP50**，"
          "即「选出来了但买不进」的部分被剔除。", ""]
    for nm in ("liq20", "illiq20"):
        L += [f"### {nm}", ""] + _tbl(decay[nm]) + [""]

    L += ["## 四、结论（预注册判据自动判定）", "",
          f"判据（文档 §三）：施加**全部**过滤（F4）后，全期超额 **> 0** 且分年 "
          f"**≥ {PASS_MIN_POS_FRAC:.0%} 为正**。", ""]
    for nm in ("liq20", "illiq20"):
        v = verdict[nm]
        k0 = v["kept_frac_F0"]
        k1 = v["kept_frac_F1"]
        trail = (f"（F0 保留率 {k0:.0%} → F1 保留率 {(k1 or 0):.0%}，"
                 f"空仓窗口 {v['n_empty_F1']}/{v['n_windows_total']}）"
                 if k0 is not None else "")
        if v["unassessable"]:
            detail = ("**F4 无可评估窗口 → 超额无法计算（即「过滤后一只都买不进」）**")
        else:
            detail = (f"F4 全期超额 **{v['excess_pp_ALL_FILTERED']}pp**，"
                      f"分年 {v['pos_years']}/{v['n_years']} 为正")
        L += [f"- **{nm}**：{detail} → **{'✅ 通过' if v['pass'] else '❌ 未通过'}** {trail}"]
    any_pass = any(v["pass"] for v in verdict.values())
    L += ["", f"**判定：{'至少一个因子通过阶段 0，可进入阶段 1（方案 A 风险中性化）讨论' if any_pass else '全部未通过 → 按文档 §三 直接终止（结论：liq/illiq 的超额不可交易）'}。**", ""]

    L += ["## 五、Caveats", "",
          "- **无历史 ST 名单**：F4 用「当前名称含 ST」近似，历史上曾被 ST 的票**未**被剔除 → 折扣量级偏小。",
          "- **无历史流通股本**：无法算真实市值分位，微盘判定只能用成交额代理。",
          "- 幸存者偏差：universe = 今天还活着的票（只影响绝对量级，相对判断可用）。",
          "- 停牌判定用「当日无成交量」；长期停牌在 k_data 里可能直接**无 bar**（已被前向坏柱窗剔除）。",
          "- 前向 T+10 与实盘换仓周期可能不同：量级会变，符号预期不变。",
          "- 本脚本**不改任何生产配置**；阶段 1/2 落地须过文档 §四 预注册门控。"]
    out = fe.OUT_DIR / "liq_illiq_phase0_diagnostic.md"
    out.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"DONE {time.time()-t0:.0f}s -> {out}", flush=True)
    print("VERDICT " + json.dumps(verdict, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
