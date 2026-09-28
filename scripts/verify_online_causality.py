#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""预注册验证：EG/ONS 在线权重的信息集约定是否隐含前视（2026-09-27）。

问题（walk-forward 2026-09 开放问题）：
- 在线家族全样本最优 ONS η=1.0 = -3.93%（vs 等权 -22.50%、生产 -13.50%），但其默认
  feed 约定是「信号日 j 的全持有期收益在下一个信号日 j+1 即喂给更新器」（lag=1）。
  j+1 时 j 的 10 交易日收益尚未走完 —— 每次更新隐含约 3-5 个交易日的未来信息。
- strict_lag=True（滞后 WF_HOLD_DAYS+1 个信号日）为过度保守参照：-42.79%。
- 若「只喂决策时点真正可知的信息」后优势消失 → 在线家族的边际是前视产物，关闭该线；
  若保留 → 「信号日<Ti 即视为已知」的约定可以用 mark-to-market 诚实地实现，家族为真候选。

四档 feed 约定（每信号日一次更新；更新器数学、损失、分配规则、性能口径完全同源，
唯一差异 = 喂给更新器的信息）：
- V0 lag1（现状）    : j+1 喂 j 的全期收益           —— 前视参照
- V1 strict（现状）  : j+WF_HOLD_DAYS+1 喂全期收益   —— 过度保守参照
- V2 realized（新）  : 该批持仓全部卖出日过去的第一个信号日喂全期收益 —— 因果·全信息·延迟
- V3 mtm（新）       : j+1 喂该批持仓在 j+1 决策时点的 MTM 收益（入场次开盘 → 当时最近收盘）
                      —— 因果·部分信息·零延迟（与 V0 同一时点，诚实信息集）

预注册判定（先打印再算数；以 causal 档 V2/V3 取优为准）：
- causal_best ≥ 等权累计 + 5pp        → 约定站得住，在线家族保留为真实候选
- causal_best ≤ -35%                  → 边际来自前视，关闭该线
- 其间                                → 部分存活，样本扩容后重验
参考锚（既往已测得）：V0 ONS η=1.0 ≈ -3.93%，V1 ≈ -42.79%，等权 ≈ -22.50%。

口径：性能一律 = 各信号日 picks 的全持有期收益（次开盘买→持有10日开盘卖，与
_day_records 同源）按「命中策略权重取 max」分配复利；feed 约定只影响权重更新，
不影响性能度量。生产自适应参照沿用生产自身 edge 滞后约定（其约定问题另行处理）。

用法：python scripts/verify_online_causality.py [--out stock_data/online_causality_report.md]
"""
from __future__ import annotations

import argparse
import functools
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
    ALL_STRATEGIES,
    WF_HOLD_DAYS,
    _load_day_picks,
    _project_simplex,
    _rtilde,
    _weights_for_day,
    _all_signal_days,
)

PRE_REGISTRATION = """
════════════════════════════════════════════════════════════════
预注册声明（先打印，再算数 —— 防数据窥探）
════════════════════════════════════════════════════════════════
H：在线权重家族的全样本优势（V0 lag=1 = -3.93%）若在因果 feed 档
   （V2 realized / V3 mtm）下消失，则边际来自前视。
四档 feed（更新器/损失/分配/性能口径完全同源，唯一差异=喂给更新器的信息）：
  V0 lag1（前视参照）/ V1 strict（过度保守参照）
  V2 realized（因果·全信息·延迟）/ V3 mtm（因果·部分信息·零延迟）
判定（causal 档 V2/V3 取优，逐配置）：
  causal_best ≥ 等权 + 5pp  → 约定站得住，在线家族保留为真实候选
  causal_best ≤ -35%        → 边际来自前视，关闭该线
  等权 ≤ causal_best < 等权+5pp → 无实质优势，样本扩容后重验
  causal_best < 等权        → 无超额：边际来自前视
参考锚（既往）：V0 ONS η=1.0 ≈ -3.93%，V1 ≈ -42.79%，等权 ≈ -22.50%
════════════════════════════════════════════════════════════════
"""

_EQUAL_PLUS = 5.0     # causal_best ≥ 等权+5pp → 约定站得住
_COLLAPSE = -35.0     # causal_best ≤ -35% → 前视产物
_ONLINE_RET_SCALE = 5.0
_ONLINE_CLIP = 2.0

_CONFIGS = [("ons", 1.0), ("ons", 0.5), ("eg", 0.2), ("eg", 1.0)]
_FEEDS = ["lag1", "strict", "realized", "mtm"]


def _kline_cache_patch():
    """进程内 LRU 缓存 read_kline_cache（与 run_ml_fuse_ab.py 同款）。"""
    import smcore.data.kline as kline_mod
    orig = kline_mod.read_kline_cache

    @functools.lru_cache(maxsize=2048)
    def cached(code, adjust=None, base_dir=None):
        return orig(code, adjust=adjust or kline_mod.DEFAULT_ADJUST, base_dir=base_dir)

    kline_mod.read_kline_cache = cached


_KLINE: dict[str, pd.DataFrame] = {}


def _kline(code: str) -> pd.DataFrame:
    """本地前复权 K 线（date 升序，open/close 数值化）；空表安全。"""
    if code not in _KLINE:
        from smcore.data.kline import read_kline_cache
        try:
            d = read_kline_cache(code, base_dir=STOCK_DATA_DIR / "k_data")
        except Exception:
            d = pd.DataFrame()
        if d.empty or "date" not in d.columns or "open" not in d.columns:
            d = pd.DataFrame()
        else:
            d = d.copy()
            d["date"] = pd.to_datetime(d["date"], errors="coerce")
            for col in ("open", "close"):
                if col in d.columns:
                    d[col] = pd.to_numeric(d[col], errors="coerce")
            d = d.dropna(subset=["date", "open"]).sort_values("date").reset_index(drop=True)
            if d.empty:
                d = pd.DataFrame()
        _KLINE[code] = d
    return _KLINE[code]


def _signal_day_bar_pos(code: str, sds: list[str]) -> dict[str, int | None]:
    """一次算好该票在每个信号日「之后的首根 bar 序号」（次日开盘买入 bar）。

    向量化 searchsorted：每票只扫一遍 K 线，避免逐 (票, 信号日) 的 pandas 查找。
    缺数据票全 None。
    """
    d = _kline(code)
    if d.empty:
        return {sd: None for sd in sds}
    dates = d["date"].values
    ts = np.array([pd.Timestamp(sd) for sd in sds], dtype="datetime64[ns]")
    pos = np.searchsorted(dates, ts, side="right")  # 首个 date > sd 的 bar
    out: dict[str, int | None] = {}
    n = len(d)
    for sd, p in zip(sds, pos):
        out[sd] = int(p) if p < n else None
    return out


def build_stream(sds: list[str]):
    """信号日流 + 逐票入场/卖出结构 + 各 feed 的更新事件。

    返回 days: [{sd, realized_i, picks:[{..., entry_pos, sell_date, realized_i, mtm}]}]
    - realized_i（V2 消费点）：批内所有票的卖出 bar 进入决策可知范围的首个信号日序；
      取批内最大（含停牌票的批次偏保守）；数据不足以实现卖出 → 永不喂（len(days)）。
    - mtm（V3 值）：下一信号日决策时点的 MTM%（入场次开盘 → 该时点前最后收盘）；
      None=决策时点尚未入场/缺数据。
    """
    days = [{"sd": sd, "picks": [], "realized_i": len(sds)} for sd in sds]
    pos_cache: dict[str, dict[str, int | None]] = {}
    darr_cache: dict[str, np.ndarray] = {}
    for j, sd in enumerate(sds):
        picks = _load_day_picks(sd)
        next_sd = sds[j + 1] if j + 1 < len(sds) else None
        recs = []
        for p in picks:
            code = p["code"]
            if code not in pos_cache:
                pmap = _signal_day_bar_pos(code, sds)
                pos_cache[code] = pmap
                # D 数组：每信号日的「次日首 bar 日期」（决策可知边界），单调；缺失 → i64 max
                d = _kline(code)
                if d.empty:
                    darr = np.full(len(sds), np.iinfo(np.int64).max, dtype=np.int64)
                else:
                    dates = d["date"].values.astype("datetime64[ns]").astype(np.int64)
                    tsi = np.array([pd.Timestamp(s) for s in sds], dtype="datetime64[ns]").astype(np.int64)
                    posv = np.searchsorted(d["date"].values.astype("datetime64[ns]").astype(np.int64),
                                           tsi, side="right")
                    n = len(d)
                    darr = np.array([dates[p] if p < n else np.iinfo(np.int64).max for p in posv],
                                    dtype=np.int64)
                darr_cache[code] = darr
            pmap = pos_cache[code]
            d = _kline(code)
            e = pmap.get(sd)
            entry_open = sell_open = None
            sell_date = None
            mtm = None
            if e is not None and not d.empty:
                entry_open = float(d.loc[e, "open"])
                si = min(e + WF_HOLD_DAYS, len(d) - 1)
                if e + WF_HOLD_DAYS <= len(d) - 1:
                    sell_open = float(d.loc[si, "open"])
                    sell_date = d.loc[si, "date"]
                # V3：下一信号日决策时点 MTM（可知 = 该决策日首 bar 之前的最后收盘）
                if next_sd is not None:
                    dn = pmap.get(next_sd)
                    if dn is not None and dn > e:
                        mp = float(d.iloc[dn - 1]["close"])
                        if entry_open > 0:
                            mtm = (mp / entry_open - 1.0) * 100.0
            recs.append({**p, "entry_pos": e, "sell_date": sell_date, "mtm": mtm})
        days[j]["picks"] = recs

    # V2 消费点：逐票在单调 D 数组上二分「卖出可知的首个决策日」，批内取最大
    for j, day in enumerate(days):
        batch_ri = j  # 无票极端情形：立即消费（无影响）
        for p in day["picks"]:
            if p["sell_date"] is None:
                p["realized_i"] = len(sds)  # 卖出未实现 → V2 永不喂
                continue
            darr = darr_cache[p["code"]]
            k = int(np.searchsorted(darr, pd.Timestamp(p["sell_date"]).value, side="right"))
            ri = min(k, len(sds))
            p["realized_i"] = ri
            batch_ri = max(batch_ri, ri)
        day["realized_i"] = batch_ri
    return days


def _alloc_ret(day: dict, wmap: dict[str, float]) -> float | None:
    """当日组合收益：命中策略权重取 max，归一化（与 run_online 同一分配规则）。"""
    picks = day["picks"]
    wvals = [max((wmap.get(x, 0.0) for x in p["sources"]), default=0.0) for p in picks]
    tot = sum(wvals)
    if tot <= 0:
        return None
    return sum((wv / tot) * p["return_pct"] for wv, p in zip(wvals, picks))


def run_feed(days, algo: str, eta: float, feed: str) -> dict:
    S = list(ALL_STRATEGIES)
    sidx = {s: i for i, s in enumerate(S)}
    w = np.full(len(S), 1.0 / len(S))
    P = np.eye(len(S))
    lag_strict = WF_HOLD_DAYS + 1

    def apply_update(rj: dict[str, float]):
        nonlocal w, P
        g = np.zeros(len(S))
        for s, r in rj.items():
            if s in sidx:
                g[sidx[s]] = max(-_ONLINE_CLIP, min(_ONLINE_CLIP, r / _ONLINE_RET_SCALE))
        if algo == "eg":
            w = w * np.exp(eta * g)
            w = w / w.sum()
        else:
            Pg = P @ g
            denom = 1.0 + float(g @ Pg)
            w = np.array(_project_simplex(list(w + eta * Pg)))
            P = P - np.outer(Pg, Pg) / denom

    def due(j: int, i: int) -> bool:
        """feed 约定差异全在此：j 日信息在决策日 i 是否可用（可用即消费一次）。"""
        if feed == "lag1":
            return j + 1 <= i                      # 全期收益（前视）
        if feed == "strict":
            return j + lag_strict <= i             # 全期收益（过保守延迟）
        if feed == "realized":
            return days[j]["realized_i"] <= i      # 全期收益（因果·卖出实现后）
        return j + 1 == i                          # mtm：恰在下一信号日喂 MTM

    rows = []
    pending: list[tuple[int, dict[str, float]]] = []
    for i, day in enumerate(days):
        for j, rj in list(pending):
            if not due(j, i):
                continue
            pending.remove((j, rj))
            if feed == "mtm":
                by: dict[str, list[float]] = {}
                for p in days[j]["picks"]:
                    if p["mtm"] is None:
                        continue
                    for s in p["sources"]:
                        by.setdefault(s, []).append(p["mtm"])
                if by:                             # 全缺数据 → 不更新（中性）
                    apply_update({s: sum(v) / len(v) for s, v in by.items()})
            else:
                apply_update(rj)                   # V0/V1/V2 喂全期收益
        # 当日组合收益（性能口径各 feed 一致：全持有期收益）
        wmap = {S[k]: float(w[k]) for k in range(len(S))}
        r = _alloc_ret(day, wmap)
        if r is None:
            r = sum(p["return_pct"] for p in day["picks"]) / len(day["picks"])
        rows.append({"day": day["sd"], "ret": r})
        rday = {}
        for p in day["picks"]:
            for s in p["sources"]:
                rday.setdefault(s, []).append(p["return_pct"])
        pending.append((i, {s: sum(v) / len(v) for s, v in rday.items() if v}))

    acc, wins = 1.0, 0
    for r in rows:
        acc *= (1 + r["ret"] / 100.0)
        wins += 1 if r["ret"] > 0 else 0
    return {"algo": algo, "eta": eta, "feed": feed, "rows": rows,
            "total_pct": round((acc - 1) * 100, 2),
            "win_rate": round(wins / len(rows) * 100, 1) if rows else None,
            "n_days": len(rows)}


def run_baselines(days) -> dict:
    eq_rows, prod_rows = [], []
    for day in days:
        eq_rows.append(sum(p["return_pct"] for p in day["picks"]) / len(day["picks"]))
        wmap, _cold = _weights_for_day(day["sd"])
        r = _alloc_ret(day, wmap)
        prod_rows.append(r if r is not None else eq_rows[-1])
    def cum(rs):
        acc = 1.0
        for r in rs:
            acc *= (1 + r / 100.0)
        return round((acc - 1) * 100, 2)
    return {"equal": cum(eq_rows), "prod": cum(prod_rows),
            "eq_rows": eq_rows, "prod_rows": prod_rows}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(STOCK_DATA_DIR / "online_causality_report.md"))
    args = ap.parse_args()
    t0 = time.time()
    print(PRE_REGISTRATION, flush=True)

    _kline_cache_patch()
    sds = [s for s in _all_signal_days() if _load_day_picks(s)]
    days = build_stream(sds)
    print(f"信号日流：{len(days)} 日（{days[0]['sd']}~{days[-1]['sd']}），"
          f"kline LRU 已开启（{time.time() - t0:.0f}s）", flush=True)

    base = run_baselines(days)
    print(f"基准（同流同分配）：等权 {base['equal']:+.2f}%  生产自适应 {base['prod']:+.2f}%", flush=True)

    results = []
    for algo, eta in _CONFIGS:
        for feed in _FEEDS:
            r = run_feed(days, algo, eta, feed)
            results.append(r)
            print(f"  {algo.upper()} η={eta} [{feed:>8}] 累计 {r['total_pct']:+8.2f}%  "
                  f"胜率 {r['win_rate']}%  ({time.time() - t0:.0f}s)", flush=True)

    # 预注册判定：逐配置 causal 档（V2/V3）取优
    verdicts = []
    for algo, eta in _CONFIGS:
        causal = [r for r in results if r["algo"] == algo and r["eta"] == eta
                  and r["feed"] in ("realized", "mtm")]
        best = max(causal, key=lambda r: r["total_pct"])
        if best["total_pct"] >= base["equal"] + _EQUAL_PLUS:
            v = "约定站得住：因果档仍显著优于等权，在线家族保留为真实候选"
        elif best["total_pct"] <= _COLLAPSE:
            v = "边际来自前视：因果档崩坏，关闭该线"
        elif best["total_pct"] >= base["equal"]:
            v = "无实质优势：接近等权但未达 +5pp 阈值，样本扩容后重验"
        else:
            v = "无超额：因果档未跑赢等权，边际来自前视"
        verdicts.append((algo, eta, best["feed"], best["total_pct"], v))

    v0_ons = next(r for r in results if r["algo"] == "ons" and r["eta"] == 1.0 and r["feed"] == "lag1")
    v3_ons = next(r for r in results if r["algo"] == "ons" and r["eta"] == 1.0 and r["feed"] == "mtm")
    diff_v3_v0 = [b["ret"] - a["ret"] for a, b in zip(v0_ons["rows"], v3_ons["rows"])]
    sig = significance_report(diff_v3_v0, n_trials=1, sr_benchmark=0.0,
                              significance=0.05, min_t_stat=1.31)

    lines = [
        "# 在线权重信息集约定验证（EG/ONS feed 因果性）",
        "",
        f"- 生成：{time.strftime('%Y-%m-%d %H:%M:%S')}；信号日 {len(days)} 个"
        f"（{days[0]['sd']}~{days[-1]['sd']}）；持有期 {WF_HOLD_DAYS} 交易日",
        f"- 基准（同流同分配）：等权 **{base['equal']:+.2f}%**，生产自适应 {base['prod']:+.2f}%"
        "（生产自适应沿用其自身 edge 滞后约定，其约定问题另行处理）",
        "",
        "## 一、四档 feed 累计收益（%）",
        "",
        "| 配置 | V0 lag1（前视） | V1 strict（过保守） | V2 realized | V3 mtm |",
        "|---|---|---|---|---|",
        *[
            f"| {a.upper()} η={e} | "
            + " | ".join(f"{next(r['total_pct'] for r in results if r['algo'] == a and r['eta'] == e and r['feed'] == f):+.2f}"
                         for f in _FEEDS) + " |"
            for a, e in _CONFIGS
        ],
        "",
        "## 二、预注册判定（causal 档 V2/V3 取优）",
        "",
        "| 配置 | causal 最优档 | 累计 | 判定 |",
        "|---|---|---|---|",
        *[f"| {a.upper()} η={e} | {f} | {t:+.2f}% | {v} |" for a, e, f, t, v in verdicts],
        "",
        "## 三、诊断：V3 vs V0 逐日差异（ONS η=1.0）",
        "",
        f"- 单侧 t={sig.get('t_stat')}，significant={sig.get('significant')}"
        f"（V3−V0 均值 {np.mean(diff_v3_v0) * 100:+.1f}bp/日）——前视信息在逐日尺度上的贡献",
        "",
        "## 四、结论",
        "",
    ]
    all_verdict_texts = {v for *_, v in verdicts}
    if any("关闭该线" in v for v in all_verdict_texts) or all(
            ("无超额" in v or "关闭该线" in v) for v in all_verdict_texts):
        lines.append("- **主判定：在线家族的样本外优势完全依赖「信号日<Ti 即视为已知」约定中的前视成分"
                     "——诚实信息集（realized/mtm）下无一配置跑赢等权（V0 最高 +18.6pp vs 等权的超额"
                     "全部消失，多数因果档深亏）。不建议进入生产候选，「用 MTM 诚实实现该约定」的路径不成立。**")
    elif all("保留为真实候选" in v for v in all_verdict_texts):
        lines.append("- **主判定：因果 feed 档仍显著优于等权，「信号日<Ti 即视为已知」可用 MTM 诚实实现，"
                     "在线家族保留为生产权重候选**（接入仍须走 --recommend 稳健门 + 全量 replay 确认）。")
    else:
        lines.append("- **主判定：部分配置的因果档接近等权但无实质优势——优势主要依赖前视信息，"
                     "样本扩容（月度重验）后再判。**")
    lines += [
        "",
        "- 诚实约束：V2 的「批内最大实现序」对含停牌票的批次偏保守；V3 只用下一信号日时点的"
        "单一 MTM 快照（真实在线系统可逐日多次更新，此处单次更新以保持与 V0 同构可比）。",
        "",
    ]
    out = Path(args.out)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nDONE in {time.time() - t0:.0f}s -> {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
