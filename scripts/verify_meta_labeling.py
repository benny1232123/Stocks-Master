#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""预注册实验 v2：Meta-labeling 二次筛选——密集训练 + 非重叠评估（2026-09-27）。

v1 教训（当日发现）：日频信号日 + 10 交易日持有期 → 相邻日票池高度重叠，
逐日「独立组合收益」复利会把同一波行情重复计入（v1 扩样本后 A 轨 +256% 即此伪像：
2025-09/10 月度均值 +4%/日 × 重叠复利）。逐笔均值无偏，日复利轨与逐日 t 检验被污染。

v2 设计：
- 训练：全部密集信号日逐笔（≈3200 笔，扩容的意义所在）——特征/标签严格因果
  （训练日距评估日 ≥ purge 17 天，扩展窗逐日重训，LightGBM 零调参）；
- 评估：每 10 个信号日取 1 个「非重叠评估日」（10 日持有期恰好不重叠，~27 格），
  三轨 A/MF/MT 的累计、逐日差 t、G3/G4 全部在该网格上计算；
- G2 排序力：全部有模型逐笔上 P(win) 三分位（逐笔均值不受重叠偏差影响），
  另报日内 Spearman（P 与当日收益的逐日秩相关均值）作诊断。

预注册门（全过才建议立项生产集成）：
  G1 非重叠网格 cum(MF)−cum(A) ≥ +2pp；
  G2 逐笔三分位单调 低<中<高；
  G3 网格前/后半均改进；
  G4 网格上 ≥2 个 regime 改善（各 n≥3）；
  G5 平均存活率 ≥60%；
  G6 网格逐日差单侧 t ≥ 1.31（非重叠 → t 有效）。

数据口径：标签 = _day_records（生产 Multi-Backtest 优先、naive 回补兜底，与
walk-forward 全家同源）；特征 = 信号日可得信息（DAL 列 + 本地 k_data 量价 +
regime/cash_pct）；代码 join 统一 _norm_code(zfill6)——v1 曾因 DAL 整数列丢前导零
导致 00/30 开头票的特征 join 失败（与 factor_attribution 已知同坑）。

用法：python scripts/verify_meta_labeling.py [--out stock_data/meta_labeling_report.md]
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
from smcore.strategy.significance import significance_report  # noqa: E402
from walk_forward_validator import (  # noqa: E402
    _load_day_picks,
    _norm_code,
    _all_signal_days,
)

PURGE_CAL = (10 + 1) * 7 // 5 + 2
MIN_TRAIN_DAYS = 40
MIN_DAY_PICKS = 5
MIN_REGIME_N = 3
MIN_KEEP_FRAC = 0.60
MIN_IMPROVE_PP = 2.0
T_CRIT = 1.31
EVAL_EVERY = 10          # 非重叠评估网格间隔（信号日数 = 持有期交易日数）
SEED = 42

PRE_REGISTRATION = """
════════════════════════════════════════════════════════════════
预注册 v2（先打印，再算数 —— 防数据窥探）
════════════════════════════════════════════════════════════════
v1 教训：日频信号 × 10 日持有的重叠复利伪像（A 轨 +256%）。
v2：训练=全部密集逐笔（严格因果 purge）；评估=每 {ev} 信号日一格的
非重叠网格（三轨累计 / t / G3 / G4 均在网格上）；G2=全部有模型逐笔三分位。
门（全过才立项）：G1 ≥{minpp}pp ∧ G2 单调 ∧ G3 两半稳定 ∧ G4 ≥2 regime
∧ G5 存活率≥{keep:.0%} ∧ G6 t≥{tcrit}（网格非重叠，t 有效）
════════════════════════════════════════════════════════════════
"""

_KLINE: dict[str, pd.DataFrame] = {}


def _kline_cache_patch():
    import functools
    import smcore.data.kline as kline_mod
    orig = kline_mod.read_kline_cache

    @functools.lru_cache(maxsize=2048)
    def cached(code, adjust=None, base_dir=None):
        return orig(code, adjust=adjust or kline_mod.DEFAULT_ADJUST, base_dir=base_dir)

    kline_mod.read_kline_cache = cached


def _kline(code: str) -> pd.DataFrame:
    if code not in _KLINE:
        from smcore.data.kline import read_kline_cache
        try:
            d = read_kline_cache(code, base_dir=STOCK_DATA_DIR / "k_data")
        except Exception:
            d = pd.DataFrame()
        if d.empty or "date" not in d.columns or "close" not in d.columns:
            d = pd.DataFrame()
        else:
            d = d.copy()
            d["date"] = pd.to_datetime(d["date"], errors="coerce")
            for c in ("close", "amount"):
                if c in d.columns:
                    d[c] = pd.to_numeric(d[c], errors="coerce")
            d = d.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
            if d.empty:
                d = pd.DataFrame()
        _KLINE[code] = d
    return _KLINE[code]


def _stock_feats(code: str, sd: str) -> dict:
    out = {"ret20": np.nan, "ret60": np.nan, "vol20": np.nan, "amt20": np.nan}
    d = _kline(code)
    if d.empty:
        return out
    hist = d[d["date"] <= pd.Timestamp(sd)]
    close = hist["close"].reset_index(drop=True)
    if len(close) >= 21:
        out["ret20"] = float(close.iloc[-1] / close.iloc[-21] - 1.0)
    if len(close) >= 61:
        out["ret60"] = float(close.iloc[-1] / close.iloc[-61] - 1.0)
    rets = close.pct_change().dropna()
    if len(rets) >= 20:
        out["vol20"] = float(rets.tail(20).std() * np.sqrt(252))
    if "amount" in hist.columns:
        amt = hist["amount"].dropna()
        if len(amt) >= 20:
            out["amt20"] = float(amt.tail(20).mean())
    return out


def build_table(days: list[str]) -> pd.DataFrame:
    rows = []
    for sd in days:
        picks = _load_day_picks(sd)
        if len(picks) < MIN_DAY_PICKS:
            continue
        rj = STOCK_DATA_DIR / "regime_history" / f"{sd}.json"
        regime, cash_pct = "震荡轮动", None
        if rj.exists():
            try:
                meta = json.loads(rj.read_text(encoding="utf-8"))
                regime = meta.get("regime") or regime
                cash_pct = meta.get("cash_pct")
            except Exception:
                pass
        for p in picks:
            feats = _stock_feats(p["code"], sd)
            rows.append({
                "sd": sd, "code": p["code"],
                "n_sources": len(p.get("sources") or []),
                "return_pct": float(p["return_pct"]),
                "win": int(float(p["return_pct"]) > 0),
                "regime": regime, "cash_pct": cash_pct,
                "is_dd": int(regime == "下行防御"), "is_up": int(regime == "趋势上行"),
                **feats,
            })
    df = pd.DataFrame(rows)
    # DAL 特征 join：键统一 _norm_code（DAL 整数列丢前导零的已知坑，zfill6）
    dal_cols: dict[tuple[str, str], dict] = {}
    for sd in days:
        p = STOCK_DATA_DIR / f"Daily-Action-List-{sd}.csv"
        if not p.exists():
            continue
        try:
            d = pd.read_csv(p, encoding="utf-8-sig")
        except Exception:
            continue
        if "综合评分" not in d.columns:
            continue
        for _, r in d.iterrows():
            dal_cols[(sd, _norm_code(r.get("股票代码")))] = {
                "fused": float(r.get("综合评分") or 0),
                "weight": float(r.get("权重") or 0),
                "pos_pct": float(r.get("建议仓位%") or 0),
                "stop_pct": float(r.get("stop_pct")) if pd.notna(r.get("stop_pct")) else np.nan,
            }
    keyed = [dal_cols.get((r.sd, r.code), {}) for r in df.itertuples()]
    for col in ("fused", "weight", "pos_pct", "stop_pct"):
        df[col] = [k.get(col, np.nan) for k in keyed]
    return df


FEATURES = ["n_sources", "fused", "weight", "pos_pct", "stop_pct",
            "ret20", "ret60", "vol20", "amt20", "is_dd", "is_up", "cash_pct"]


def fit_predict(train: pd.DataFrame, test: pd.DataFrame) -> np.ndarray:
    ytr = train["win"].values
    if len(train) < 200 or ytr.min() == ytr.max():
        return np.full(len(test), float(ytr.mean()) if len(ytr) else 0.5)
    import lightgbm as lgb
    m = lgb.LGBMClassifier(
        n_estimators=200, learning_rate=0.05, num_leaves=15,
        min_child_samples=40, reg_lambda=1.0, random_state=SEED,
        verbose=-1, class_weight="balanced",
    )
    m.fit(train[FEATURES].fillna(0.0), ytr)
    return m.predict_proba(test[FEATURES].fillna(0.0))[:, 1]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(STOCK_DATA_DIR / "meta_labeling_report.md"))
    args = ap.parse_args()
    t0 = time.time()
    print(PRE_REGISTRATION.format(ev=EVAL_EVERY, minpp=MIN_IMPROVE_PP,
                                  keep=MIN_KEEP_FRAC, tcrit=T_CRIT), flush=True)

    _kline_cache_patch()
    days = [d for d in _all_signal_days()
            if (STOCK_DATA_DIR / f"Daily-Action-List-{d}.csv").exists()]
    table = build_table(days)
    n_labeled = len(table)
    print(f"逐笔样本 {n_labeled}（{table['sd'].nunique()} 日，{time.time() - t0:.0f}s）", flush=True)

    ts = pd.to_datetime(pd.Series(days), format="%Y%m%d", errors="coerce")
    day_index = {d: i for i, d in enumerate(days)}
    n_feats_nan = int(table[["fused", "weight", "pos_pct"]].isna().all(axis=1).sum())
    print(f"特征 join 缺失逐笔 {n_feats_nan}/{n_labeled}（DAL 行无对应）", flush=True)

    # ── 逐评估日：训练（密集）+ 预测 ──
    recs = []
    for i, sd in enumerate(days):
        day = table[table["sd"] == sd]
        if day.empty:
            continue
        t = ts.iloc[i]
        train_days = [d for j, d in enumerate(days[:i])
                      if pd.notna(t) and pd.notna(ts.iloc[j]) and (t - ts.iloc[j]).days >= PURGE_CAL]
        day = day.copy()
        if len(train_days) >= MIN_TRAIN_DAYS:
            day["p"] = fit_predict(table[table["sd"].isin(train_days)], day)
        else:
            day["p"] = np.nan
        recs.append(day)
    table = pd.concat(recs, ignore_index=True)
    print(f"P(win) 计算完成（{time.time() - t0:.0f}s）", flush=True)

    # 日级汇总（全部日，供存活率/诊断；轨道累计只在非重叠网格上）
    day_rows = []
    for sd, day in table.groupby("sd"):
        a_ret = float(day["return_pct"].mean())
        modeled = day["p"].notna().all() and day["p"].nunique() > 1
        if modeled:
            q = day["p"].rank(pct=True)
            keep = day[q >= 1 / 3]
            mf_ret = float(keep["return_pct"].mean()) if len(keep) else 0.0
            w = day["p"].clip(lower=0.01)
            mt_ret = float((w * day["return_pct"]).sum() / w.sum())
            keep_frac = len(keep) / len(day)
        else:
            mf_ret, mt_ret, keep_frac = a_ret, a_ret, 1.0
        day_rows.append({"sd": sd, "regime": day["regime"].iloc[0], "n": len(day),
                         "ret_a": a_ret, "ret_mf": mf_ret, "ret_mt": mt_ret,
                         "keep_frac": keep_frac, "has_model": bool(modeled),
                         "day_i": day_index[sd]})
    daydf = pd.DataFrame(day_rows).sort_values("day_i").reset_index(drop=True)

    # ── 非重叠评估网格 ──
    grid = daydf[daydf["day_i"] % EVAL_EVERY == 0].reset_index(drop=True)
    gridm = grid[grid["has_model"]]
    print(f"评估网格 {len(grid)} 格（有模型 {len(gridm)}）", flush=True)

    def cum(col, sub):
        acc = 1.0
        for r in sub[col]:
            acc *= (1 + r / 100.0)
        return (acc - 1) * 100.0

    ca, cmf, cmt = cum("ret_a", grid), cum("ret_mf", grid), cum("ret_mt", grid)
    diff = (gridm["ret_mf"] - gridm["ret_a"]).tolist()
    sig = significance_report(diff, n_trials=1, sr_benchmark=0.0,
                              significance=0.05, min_t_stat=T_CRIT)
    half = max(1, len(grid) // 2)
    g3_first = cum("ret_mf", grid.iloc[:half]) - cum("ret_a", grid.iloc[:half])
    g3_second = cum("ret_mf", grid.iloc[half:]) - cum("ret_a", grid.iloc[half:])

    # ── G2：全部有模型逐笔三分位 + 日内 Spearman 诊断 ──
    mtbl = table[table["p"].notna() & table.groupby("sd")["p"].transform(lambda s: s.nunique() > 1)]
    tert, spear = {}, []
    if len(mtbl) > 100:
        q = mtbl["p"].rank(pct=True)
        for name, m in (("低", q < 1 / 3), ("中", (q >= 1 / 3) & (q < 2 / 3)), ("高", q >= 2 / 3)):
            sub = mtbl[m]
            tert[name] = {"n": len(sub), "mean": float(sub["return_pct"].mean()),
                          "win": float((sub["return_pct"] > 0).mean())}
    for sd, day in mtbl.groupby("sd"):
        if len(day) >= 8:
            from scipy.stats import spearmanr
            r = spearmanr(day["p"], day["return_pct"])
            if r.statistic == r.statistic:
                spear.append(float(r.statistic))
    g2 = bool(len(tert) == 3 and tert["低"]["mean"] < tert["中"]["mean"] < tert["高"]["mean"])

    # ── G4：网格 regime 分层 ──
    by_regime: dict[str, dict] = {}
    for _, r in grid.iterrows():
        g = by_regime.setdefault(r["regime"], {"mf": [], "a": []})
        g["mf"].append(r["ret_mf"])
        g["a"].append(r["ret_a"])
    regime_rows, regime_ok_n = [], 0
    for name, g in sorted(by_regime.items()):
        if len(g["mf"]) < MIN_REGIME_N:
            continue
        acc_m = 1.0
        for r in g["mf"]:
            acc_m *= (1 + r / 100.0)
        acc_a = 1.0
        for r in g["a"]:
            acc_a *= (1 + r / 100.0)
        dpp = (acc_m - acc_a) * 100.0
        regime_rows.append((name, len(g["mf"]), dpp))
        if dpp > 0:
            regime_ok_n += 1
    g4 = regime_ok_n >= 2

    avg_keep = float(grid["keep_frac"].mean())
    g1 = bool((cmf - ca) >= MIN_IMPROVE_PP)
    g5 = bool(avg_keep >= MIN_KEEP_FRAC)
    g6 = bool(sig.get("significant")) and (np.mean(diff) > 0 if diff else False)
    robust = all([g1, g2, g3_first > 0, g3_second > 0, g4, g5, g6])

    lines = [
        "# Meta-labeling 二次筛选验证 v2（密集训练 + 非重叠评估，预注册）",
        "",
        f"- 生成：{time.strftime('%Y-%m-%d %H:%M:%S')}；逐笔样本 {n_labeled}"
        f"（{table['sd'].nunique()} 日）；评估网格 {len(grid)} 格（每 {EVAL_EVERY} 信号日一格，"
        f"10 日持有期非重叠，有模型 {len(gridm)}）；purge {PURGE_CAL} 天；LightGBM 零调参",
        f"- 特征 join 缺失逐笔 {n_feats_nan}（_norm_code zfill6 已修复 v1 的前导零坑）",
        "",
        "## 一、三轨累计前向收益（非重叠网格，%）",
        "",
        "| 轨道 | 累计 | vs A |",
        "|---|---|---|",
        f"| A 等权全清单 | {ca:+.2f} | — |",
        f"| MF 剔除 P 后 1/3 | {cmf:+.2f} | {cmf - ca:+.2f}pp |",
        f"| MT 仓位∝P | {cmt:+.2f} | {cmt - ca:+.2f}pp |",
        f"- 平均存活率（MF，网格）：{avg_keep:.0%}（门槛 {MIN_KEEP_FRAC:.0%}）",
        "",
        "## 二、排序力（全部有模型逐笔）",
        "",
        "| 组 | n | 平均收益% | 胜率% |",
        "|---|---|---|---|",
        *[f"| {k} | {v['n']} | {v['mean']:+.3f} | {v['win']:.1%} |" for k, v in tert.items()],
        f"- G2 单调（低<中<高）：**{g2}**；日内 Spearman 均值 "
        f"{float(np.mean(spear)):+.4f}（{len(spear)} 日）",
        "",
        "## 三、稳健门（预注册）",
        "",
        "| 守卫 | 值 | 通过? |",
        "|---|---|---|",
        f"| G1 网格累计改进 ≥{MIN_IMPROVE_PP}pp | {cmf - ca:+.2f}pp | {g1} |",
        f"| G2 逐笔三分位单调 | 见上 | {g2} |",
        f"| G3 网格前/后半稳定 | 前 {g3_first:+.2f} / 后 {g3_second:+.2f} | {bool(g3_first > 0 and g3_second > 0)} |",
        f"| G4 regime（{regime_ok_n} 个改善） | " + "；".join(f"{n} {d:+.1f}pp(n={c})" for n, c, d in regime_rows) + f" | {g4} |",
        f"| G5 平均存活率 ≥{MIN_KEEP_FRAC:.0%} | {avg_keep:.0%} | {g5} |",
        f"| G6 网格单侧 t ≥{T_CRIT} | t={sig.get('t_stat')} | {g6} |",
        f"| **robust** | — | **{robust}** |",
        "",
        "## 四、结论",
        "",
        (f"- **✅ robust=True：meta-labeling 值得立项生产集成**（下一步：fuse_signals 接入 "
         f"P(win) 过滤/加权的 OOS 门控设计与 ML 因子接入同构）" if robust else
         f"- **❌ robust=False：meta-labeling 未达预注册门，不进生产。**"
         + ("排序力仍缺失（G2 未过）：模型学到的是日级胜率变化，个股内排序弱。"
            "月度扩样本后以新预注册重验，或关闭该线。"
            if not g2 else
            "经济改进或稳定性不足（G1/G3/G4/G6 之一未过）。")),
        "- 诚实约束：v1 的 +256% 为重叠复利伪像（日频 × 10 日持有），v2 以非重叠网格消除；"
        "标签混合生产出场与 naive 回补口径；单模型零调参；立项前须全量 replay 确认。",
        "",
    ]
    out = Path(args.out)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nA={ca:+.2f}%  MF={cmf:+.2f}%  MT={cmt:+.2f}%  keep={avg_keep:.0%}  "
          f"G2={g2}  spear={float(np.mean(spear)) if spear else None:+.4f}  robust={robust}", flush=True)
    print(f"DONE in {time.time() - t0:.0f}s -> {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
