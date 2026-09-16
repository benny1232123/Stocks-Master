#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""融合层留一分析：把某个策略从**生产融合口径**里拿掉，OOS 会怎样？

为什么需要这一层
----------------
`momentum_disposition_oos_gate.md`（策略层）已判定：现行 momentum 在**自身条件域内**显著为负
（−14.1pp, t=−5.28），反用不过门 ⇒ 处置 = 置零/压权重。但那是**策略层**结论——它没有回答
「把 momentum 从 5 策略融合里去掉，**整个组合**会不会更好」。本脚本补这一步（落地前的最后一道）。

做法（复用 walk_forward_validator 的生产口径，**不重放整套回放**）
----------------------------------------------------------------
逐信号日 sd：
  1. `wf._weights_for_day(sd, ...)` 取当日**分配器权重**（与生产同一个 `adaptive_weights`）；
  2. `wf._load_day_picks(sd)` 取当日 DAL 选票，每条带 `sources`（命中策略集合）与 `return_pct`；
  3. 复刻生产合成规则：单票权重 = 其来源策略权重中的**最大者**，再按总和归一；
  4. 对每个策略 S 计算**留一**组合：从每票 sources 里去掉 S；**若去掉后为空则该票被淘汰**
     （这正是「置零」的语义）；另算一个「压权重」变体（把 S 的权重压到分配器 floor）;
  5. 对比累计收益、逐日差值 t、前后半段、regime 分层、换手率。

预注册判据（与 `walk_forward_factor_timing.py` 同源，**先写死再看结果**）
------------------------------------------------------------------
| 判据 | 阈值 |
|---|---|
| 主判据：累计 OOS 改进 | ≥ `MIN_IMPROVE_PP`(2.0pp)，相对现行融合组合 |
| 显著性 | 逐日差值单侧 t ≥ `T_CRIT`(1.31, α=0.10, n_trials=1) 且均值 > 0 |
| 稳定性 | 前后半段**都**改善 |
| 跨 regime 稳健 | 「趋势上行」「下行防御」两段**都**改善（每段 ≥ `MIN_DAYS_PER_REGIME` 天） |
| 换手不恶化 | 提案日均换手 ≤ 现行日均换手 + `TURN_TOL` |

⚠️ **判据只服务「动量处置」这一步**（`n_trials=1`）。报告会一并列出全部 5 个策略的留一结果，
   但**只有 `momentum` 那一行进入判定**；其余行属事后观察，须另立预注册门控才可行动（见报告 §一.2）。

⚠️ 本脚本**不改任何生产配置**，也不提供 `--apply`。
⚠️ 覆盖窗口 = DAL 历史（约百余个信号日），比策略层的 2018+ 网格窄得多 ——
   这是「生产口径」的代价，报告已注明。
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for p in (str(ROOT), str(ROOT / "scripts")):
    if p not in sys.path:
        sys.path.insert(0, p)

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

import walk_forward_validator as wf  # noqa: E402
from smcore.strategy.significance import significance_report  # noqa: E402

# ── 预注册判据 ────────────────────────────────────────────────────────────
MIN_IMPROVE_PP = 2.0
T_CRIT = 1.31
MIN_DAYS_PER_REGIME = 3
MIN_REGIMES = 2
TURN_TOL = 0.10          # 提案日均换手允许比现行高出的绝对幅度
VARIANTS = ("zero", "floor")   # zero=从来源里去掉；floor=权重压到分配器 floor
OUT_NAME = "fusion_leave_one_out"
GATED_STRATEGY = "momentum"    # 本次预注册只针对它；其余行仅作透明度


def _portfolio(picks: list[dict], weights: dict, drop: str | None,
               floor_value: float | None = None) -> tuple[float | None, set]:
    """按生产规则合成组合收益，返回 (当日收益%, 持仓代码集合)。

    与 `walk_forward_validator._run_impl` 完全同构：单票权重 = 其来源策略权重中的**最大值**，
    再按总和归一（`tot = sum or 1.0`；权重全为 0 时收益为 0，不额外丢票）。

    - `drop=None`：基线。
    - `drop=S, floor_value=None`：**置零** —— 从每票 sources 去掉 S；去掉后为空则该票**淘汰**。
    - `drop=S, floor_value=f`：**压权重** —— S 的权重按 f 计，其余策略不变，再取最大值
      （`max(others, f)`），因此来源里有更强策略的票不会被误压低。
    """
    wv, rets, codes = [], [], []
    for p in picks:
        srcs = list(p["sources"])
        if drop is not None and floor_value is None:
            srcs = [s for s in srcs if s != drop]
            if not srcs:
                continue
        if srcs:
            w = max(((floor_value if (x == drop and floor_value is not None)
                      else weights.get(x, 0.0)) for x in srcs), default=0.0)
        else:
            w = min(weights.values()) if weights else 0.0
        wv.append(w)
        rets.append(float(p["return_pct"]))
        codes.append(p["code"])
    if not wv:
        return None, set()
    tot = sum(wv) or 1.0
    return sum((w / tot) * r for w, r in zip(wv, rets)), set(codes)


def _cum(vals: list[float]) -> float:
    acc = 1.0
    for v in vals:
        acc *= (1 + v / 100.0)
    return (acc - 1) * 100


def main() -> int:
    t0 = time.time()
    days = wf._all_signal_days()
    print(f"signal days = {len(days)} ({time.time()-t0:.0f}s)", flush=True)

    try:
        from smcore.strategy.adaptive_weights import CONFIG as AW
        floor_cfg = float(AW.get("floor", 0.0) or 0.0)
    except Exception:
        floor_cfg = 0.0
    shrinkage, eff_floor = wf._eff(None, None, True)
    print(f"allocator: shrinkage={shrinkage} floor={eff_floor} cfg_floor={floor_cfg}", flush=True)

    rec: list[dict] = []
    for sd in days:
        try:
            weights, cold = wf._weights_for_day(sd, shrinkage, eff_floor, True)
        except Exception as e:  # fail-soft：单日失败不中断
            print(f"  skip {sd}: {e}", flush=True)
            continue
        picks = wf._load_day_picks(sd)
        if not picks:
            continue
        base_ret, base_codes = _portfolio(picks, weights, None)
        if base_ret is None:
            continue
        row = {"day": sd, "regime": wf._regime_as_of(sd), "cold": bool(cold),
               "n_picks": len(picks), "base": base_ret, "base_codes": base_codes,
               "weights": {k: round(float(v), 3) for k, v in weights.items()}}
        # 「独家贡献」计数：只有该策略命中的票 —— 置零时会被整票淘汰，最直接解释改进幅度
        sole = {s: 0 for s in wf.ALL_STRATEGIES}
        for p in picks:
            srcs = [x for x in p["sources"] if x in wf.ALL_STRATEGIES]
            if len(srcs) == 1:
                sole[srcs[0]] += 1
        row["sole"] = sole
        for s in wf.ALL_STRATEGIES:
            z, zc = _portfolio(picks, weights, s)
            f, fc = _portfolio(picks, weights, s, floor_value=eff_floor)
            # ⚠️ 空仓日按「持币 0%」记，**不整日剔除**：否则各变体的基线累计会随之变化
            #    （如 theme 置零后某些天无票可持 → 基线从 −87.82% 变成 −80.35%），
            #    改进幅度就不可跨变体比较了。空仓日的稳健性另由 §一.3 敏感性表回答。
            row[f"zero::{s}"] = 0.0 if z is None else z
            row[f"zero::{s}_codes"] = zc
            row[f"floor::{s}"] = 0.0 if f is None else f
            row[f"floor::{s}_codes"] = fc
        rec.append(row)
    df = pd.DataFrame(rec)
    print(f"usable days = {len(df)} ({time.time()-t0:.0f}s)", flush=True)
    if len(df) < 20:
        print("too few days", flush=True)
        return 1

    idx = pd.DatetimeIndex(pd.to_datetime(df["day"], format="%Y%m%d", errors="coerce"))
    df = df.set_index(idx)
    df = df[df.index.notna()]

    def _turn(codes_series: pd.Series) -> pd.Series:
        prev, out = None, []
        for cs in codes_series:
            if prev is None:
                out.append(1.0)
            else:
                out.append(len(prev ^ cs) / max(1, len(cs)))
            prev = cs
        return pd.Series(out, index=codes_series.index)

    base_turn = _turn(df["base_codes"])

    def _stats(vcol: str, ccol: str, mask: pd.Series | None = None) -> dict:
        """在（可选的）子样本上算提案 vs 基线的全套统计。

        mask 用于 §一.3 空仓日敏感性：只保留「置零后仍持仓」的天，
        检验改进是否只是少数「无票可持（持币 0%）」天的产物。
        """
        sub = df if mask is None else df[mask]
        p = sub[vcol].astype(float)
        b = sub["base"].astype(float)
        ok = p.notna()
        p, b = p[ok], b[ok]
        if len(p) < 10:
            return {"n_days": int(len(p))}
        diff = (p - b).to_numpy(dtype=float)
        cum_p, cum_b = _cum(p.tolist()), _cum(b.tolist())
        improve = round(cum_p - cum_b, 2)
        half = max(1, len(p) // 2)
        d1, d2 = diff[:half], diff[half:]
        first_ok = float(np.sum(d1)) > 0
        second_ok = float(np.sum(d2)) > 0
        sig = significance_report(list(diff), n_trials=1, sr_benchmark=0.0,
                                  significance=0.05, min_t_stat=T_CRIT)
        mean_diff = round(float(np.mean(diff)), 4)
        sig_ok = bool(sig.get("significant")) and mean_diff > 0
        rtab = {}
        for rg, sdf in sub[ok].groupby("regime"):
            rtab[str(rg)] = {"n_days": int(len(sdf)),
                             "prop_pct": round(_cum(sdf[vcol].astype(float).tolist()), 2),
                             "base_pct": round(_cum(sdf["base"].astype(float).tolist()), 2)}
            rtab[str(rg)]["diff_pp"] = round(rtab[str(rg)]["prop_pct"]
                                             - rtab[str(rg)]["base_pct"], 2)
        qual = {k: v for k, v in rtab.items() if v["n_days"] >= MIN_DAYS_PER_REGIME}
        beat = sum(1 for v in qual.values() if v["diff_pp"] > 0)
        diverse = len(qual) >= 2
        regime_ok = (not diverse) or (beat >= MIN_REGIMES)
        prop_turn = float(_turn(sub[ok][ccol]).mean())
        base_turn_m = float(base_turn[sub[ok].index].mean())
        turn_ok = bool(prop_turn <= base_turn_m + TURN_TOL)
        # 结构性差异：与现持仓的日均对称差（透明度，非硬门）
        churn = float(np.mean([len(a ^ c) / max(1, len(c))
                               for a, c in zip(sub[ok]["base_codes"], sub[ok][ccol])]))
        robust = bool(improve >= MIN_IMPROVE_PP and first_ok and second_ok
                      and sig_ok and regime_ok and turn_ok)
        return {"n_days": int(len(p)), "cum_prop": round(cum_p, 2), "cum_base": round(cum_b, 2),
                "improve_pp": improve, "mean_daily_diff": mean_diff, "t_stat": sig.get("t_stat"),
                "significant": sig_ok, "first_half_ok": bool(first_ok),
                "second_half_ok": bool(second_ok), "regime_table": rtab,
                "regime_diverse": diverse, "regime_beat": beat, "regime_ok": bool(regime_ok),
                "turnover_prop": round(prop_turn, 4), "turnover_base": round(base_turn_m, 4),
                "turnover_ok": turn_ok, "struct_churn_vs_base": round(churn, 4),
                "robust": robust, "arith_cum_pp": round(mean_diff * len(p), 2),
                "win_rate": round(float((p > b).mean()), 3)}

    stats = {"base_cum": round(_cum(df["base"].astype(float).tolist()), 2),
             "base_turnover": round(float(base_turn.mean()), 4),
             "n_days": int(len(df))}
    for var in VARIANTS:
        for s in wf.ALL_STRATEGIES:
            stats[f"{var}::{s}"] = _stats(f"{var}::{s}", f"{var}::{s}_codes")
    # §一.3 空仓日敏感性：剔除「置零后无票可持」的天后重算（仅 zero 口径需要）
    exempty_days: dict[str, int] = {}
    for s in wf.ALL_STRATEGIES:
        m = df[f"zero::{s}_codes"].map(lambda c: len(c) > 0)
        exempty_days[s] = int((~m).sum())
        stats[f"zero::{s}__exempty"] = _stats(f"zero::{s}", f"zero::{s}_codes", mask=m)

    # ── 分配器权重与来源诊断：解释「为何改进幅度小于策略层劣势」 ──────────
    diag: dict[str, dict] = {}
    for s in wf.ALL_STRATEGIES:
        ws = df["weights"].map(lambda d: float(d.get(s, 0.0)))
        sole = df["sole"].map(lambda d: int(d.get(s, 0)))
        diag[s] = {
            "mean_weight": round(float(ws.mean()), 3),
            "zero_weight_days_frac": round(float((ws <= 1e-9).mean()), 3),
            "mean_sole_picks_per_day": round(float(sole.mean()), 2),
            "total_sole_picks": int(sole.sum()),
            "empty_days_after_zero": exempty_days.get(s, 0),
        }
    _dm = diag.get("momentum", {})
    _zf = float(_dm.get("zero_weight_days_frac", 0.0))
    _mw = float(_dm.get("mean_weight", 0.0))
    _sole = float(_dm.get("mean_sole_picks_per_day", 0.0))
    mom_s = stats.get("zero::momentum", {})
    if _zf >= 0.5:
        interp = (f"momentum 在 **{_zf:.0%}** 的信号日权重已为 0（均值 {_mw}）—— 生产分配器"
                  f"（`zero_negative_edge=True`）**本就已把它压到近零**，所以「融合层置零」接近 "
                  f"no-op；这与策略层「显著为负」**并不矛盾**：负 edge 已被分配器自动吸收。")
    else:
        interp = (f"momentum 并非长期零权重（均值 {_mw}、零权重日占比 {_zf:.0%}），"
                  f"但融合层置零仍只带来 {mom_s.get('improve_pp')}pp ⇒ 其选票与其它策略"
                  f"**高度重叠**（日均仅 {_sole} 只票是它的独家贡献），"
                  f"策略层的 −14.1pp 劣势在融合层被稀释。")

    # ── 判定：以「置零」为主，按改进幅度排序；**只有 GATED_STRATEGY 进入判定** ──
    zero_keys = [f"zero::{s}" for s in wf.ALL_STRATEGIES]
    ranked = sorted([k for k in zero_keys if "improve_pp" in stats[k]],
                    key=lambda k: -stats[k]["improve_pp"])
    mom = stats.get(f"zero::{GATED_STRATEGY}", {})
    verdict_lines = []
    for k in ranked:
        s = k.split("::", 1)[1]
        v = stats[k]
        tag = " ← **本次判定对象**" if s == GATED_STRATEGY else ""
        verdict_lines.append(
            f"- `{s}`：留一后累计 **{v['cum_prop']}%**（现行 {v['cum_base']}%）→ 改进 "
            f"**{v['improve_pp']}pp**，t={v['t_stat']}，过门 = **{'✅' if v['robust'] else '❌'}**{tag}")
    if mom and "improve_pp" in mom:
        if mom["robust"]:
            final = "可推进「把 momentum 从融合里置零」（融合层过门）"
        elif mom["improve_pp"] > 0 and mom["t_stat"] is not None and mom["t_stat"] >= T_CRIT:
            final = "方向有利但未过全部门 → 建议只做「压权重」而非置零"
        else:
            final = "融合层未支持置零 → 维持现状（叠加策略层结论：只在策略层压权重，不动融合配置）"
    else:
        final = "数据不足，无法判定"
    final = f"【{GATED_STRATEGY}】{final}"
    # 事后发现的过门策略（不在预注册范围内）→ 只登记，不作为行动建议
    flagged = [s for s in wf.ALL_STRATEGIES
               if s != GATED_STRATEGY and stats.get(f"zero::{s}", {}).get("robust")]
    print(f"VERDICT {final}", flush=True)
    if flagged:
        print(f"FLAGGED(not actionable) {flagged}", flush=True)

    payload = {"generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
               "gate": {"min_improve_pp": MIN_IMPROVE_PP, "t_crit": T_CRIT,
                        "min_regimes": MIN_REGIMES, "min_days_per_regime": MIN_DAYS_PER_REGIME,
                        "turn_tol": TURN_TOL, "gated_strategy": GATED_STRATEGY},
               "allocator": {"shrinkage": shrinkage, "floor": eff_floor, "cfg_floor": floor_cfg},
               "summary": stats, "allocator_diag": diag, "interpretation": interp,
               "verdict": final, "flagged_not_actionable": flagged,
               "note": "leave-one-out at fusion layer, production allocation口径, 未改配置"}

    wf.STOCK_DATA_DIR.joinpath("factor_ic_replay").mkdir(parents=True, exist_ok=True)
    outp_json = wf.STOCK_DATA_DIR / "factor_ic_replay" / f"{OUT_NAME}.json"
    outp_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    L: list[str] = []
    L += ["# 融合层留一分析：拿掉某个策略，OOS 会怎样？", "",
          f"- 生成：{payload['generated_at']}",
          f"- 窗口：DAL 历史 **{stats['n_days']}** 个可用信号日（生产口径；比策略层 2018+ 网格窄）",
          f"- 分配器：`shrinkage={shrinkage}`、`floor={eff_floor}`、`zero_negative_edge=True`"
          f"（与生产 `compute_adaptive_allocation` 同源）",
          "- 合成规则复刻生产：单票权重 = 来源策略权重最大值 → 归一；"
          "**「置零」= 从每票来源里去掉该策略，去空则该票淘汰**；"
          "**「压权重」= 该策略权重压到 allocator floor**",
          f"- 现行基线累计 **{stats['base_cum']}%**，日均换手 **{stats['base_turnover']}**",
          f"- 预注册判据：改进 ≥ {MIN_IMPROVE_PP}pp、t ≥ {T_CRIT}（单侧 α=0.10, n_trials=1）、"
          f"前后半段都改善、regime 两段都改善、换手不恶化（允许 +{TURN_TOL}）",
          f"- ⚠️ **本次预注册只针对 `{GATED_STRATEGY}`**：下表列出全部 5 个策略是为了透明度，"
          f"但只有 `{GATED_STRATEGY}` 那一行进入判定（见 §一.2、§四）", "",
          "## 一、留一结果（置零口径）", "",
          "| 被拿掉的策略 | 留一累计 | 现行累计 | 改进(pp) | 均值差(pp/日) | t | 显著为正 | 前半 | 后半 | regime | 换手(提案/现行) | 持仓变动 | 过门 |",
          "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for k in ranked:
        s = k.split("::", 1)[1]
        v = stats[k]
        L.append(f"| `{s}` | {v['cum_prop']}% | {v['cum_base']}% | **{v['improve_pp']}** | "
                 f"{v['mean_daily_diff']:+.3f}pp | {v['t_stat']} | {v['significant']} | "
                 f"{'✓' if v['first_half_ok'] else '✗'} | {'✓' if v['second_half_ok'] else '✗'} | "
                 f"{v['regime_beat']}/{len(v['regime_table'])} | "
                 f"{v['turnover_prop']}/{v['turnover_base']} | {v['struct_churn_vs_base']} | "
                 f"**{'✅' if v['robust'] else '❌'}** |")

    L += ["", "### 一.1 分配器权重与来源诊断（解释 §一 改进幅度的量级）", "",
          "| 策略 | 日均权重 | 零权重日占比 | 日均独家选票数 | 独家选票合计 | 置零后空仓天数 |",
          "|---|---|---|---|---|---|"]
    for s in wf.ALL_STRATEGIES:
        d = diag.get(s, {})
        L.append(f"| `{s}` | {d.get('mean_weight')} | {d.get('zero_weight_days_frac')} | "
                 f"{d.get('mean_sole_picks_per_day')} | {d.get('total_sole_picks')} | "
                 f"{d.get('empty_days_after_zero')} |")
    L += ["", f"**解读**：{interp}", "",
          f"> 口径说明：所有变体都在**同一 {stats['n_days']} 天**上评估；"
          f"「置零后无票可持」的天按**持币 0%** 计入（不整日剔除），"
          f"该口径下单调性最好（各行「现行累计」恒为 {stats['base_cum']}%），改进幅度可跨行比较；"
          f"这些空仓日的影响另见 §一.3 敏感性表。"
          f"「压权重」= 把该策略权重压到分配器实际下限 **{eff_floor}**。", "",
          "**为什么「零权重日占比」全是 0？** 生产分配器（`smcore/strategy/adaptive_weights.py`）"
          "的地板门是**事后**步骤：所有策略先参与 softmax 竞争，再把「负 edge / 样本不足」者"
          f"统一抬到 `FLOOR`({eff_floor}) 后重新归一化到 100 —— 因此 `zero_negative_edge=True` "
          "**并不清零任何策略**（模块文档原话：「无策略被彻底剔除，故分散度始终保留；"
          "全为地板时退化为接近等权」）。结论：**融合层结构上没有「置零」这个杠杆** —— "
          "本报告的「置零」变体是**人为构造的反事实**；若真要在融合层排除某策略，"
          "必须**改分配器代码**（新增排除名单），而不是改 `adaptive_weights_config.json`。", "",
          "> ⚠️ **权重 edge 的口径（2026-09-16 更正）**：生产融合的 edge 来自 "
          "`smcore/strategy/adaptive_weights.compute_universe_edge`"
          "（`edge.source=universe`、`window=30`、`hold_days=10`、`use_benchmark=True`、"
          "`benchmark=hs300`）—— 在**候选全集**上算前向收益**减同期沪深300**，是**基准相对口径**"
          "（基准不可用时才退化为绝对收益）。而本脚本为省掉整套回放，复刻的是**验证器侧** "
          "`walk_forward_validator.causal_edge`（`EDGE_WINDOW=20`，它喂给同一套 `adaptive_weights` 的"
          "是**原始 return_pct、未减基准**）。⇒ 两者共用同一套 `adaptive_weights`（同 shrinkage/floor），"
          "但 **edge 输入口径不同**（窗口 20 vs 30、绝对 vs 相对），故本报告的权重与"
          "「线上当日权重」**可能有差异**；结论层（动量日均独家选票仅 0.24 只 ⇒ 置零增益被稀释）"
          "不依赖该差异，但若要严格对齐线上，应改调 `compute_universe_edge`。", "",
          "> 更正记录（2026-09-16）：本节早期版本曾写「`causal_edge` 未减基准 ⇒ 分配器区分不了 "
          "alpha 与 beta」—— 那是**验证器侧实现**的特性，**不适用于生产**（生产走基准相对口径），"
          "故该论断已删除。", ""]

    # ── §一.2 事后发现的过门策略（登记，不作为行动建议） ──────────────────
    if flagged:
        L += ["### 一.2 ⚠️ 新发现（**不构成行动建议**）："
              + "、".join(f"`{s}`" for s in flagged) + " 置零过门", "",
              "本轮留一里，除本次判定对象外还有策略过门：", ""]
        for s in flagged:
            v = stats[f"zero::{s}"]
            e = stats.get(f"zero::{s}__exempty", {})
            L.append(f"- `{s}`：改进 **{v['improve_pp']}pp**、t=**{v['t_stat']}**、"
                     f"前后半段 {'✓/✓' if (v['first_half_ok'] and v['second_half_ok']) else '有 ✗'}、"
                     f"regime {v['regime_beat']}/{len(v['regime_table'])} → robust=True")
            _et = e.get("t_stat")
            _tail = ""
            if _et is not None and _et < T_CRIT:
                _tail = ("，**t 跌破 T_CRIT** → 该「过门」依赖那几天的持币避险，"
                         "不是全期选票质量差异")
            L.append(f"  - **但剔除「置零后无票可持」的 {exempty_days.get(s)} 天后**：改进 "
                     f"{e.get('improve_pp')}pp、t={_et}{_tail}（详见 §一.3）")
        L += ["", "**为什么不能据此动手**（三条独立理由）：", "",
              "1. **不在预注册范围内**：本次判据是为「动量处置」注册的（`n_trials=1` ＝固定规则、非多重挖掘）。",
              "   这些策略是**看到结果之后**才被注意到的 —— 事后发现（post-hoc）的证据等级低于预注册。",
              "2. **多重检验**：一轮共跑了 5 策略 × 2 变体 = **10 个变体**，未作任何多重性修正；"
              "10 个变体里冒出一个 |t|>3 并不稀奇。",
              f"3. **收益来源可疑（见 §一.3）**：窗口仅 {stats['n_days']} 个信号日；"
              "若「剔空仓日后改进」大幅缩水，说明其改良来自少数几天「置零后一只都买不进 → "
              "持币避险」的运气，而不是全期稳定的选票质量差异。",
              "",
              "→ 若要把它们当候选，须**另立预注册门控**（更宽窗口 + 明确 `n_trials` + 独立判据）后重跑，"
              "**不得直接从本表读结论**。", ""]

    # ── §一.3 空仓日敏感性 ────────────────────────────────────────────────
    L += ["### 一.3 空仓日敏感性（改进是否只是少数「无票可持」天的产物？）", "",
          "| 策略 | 全样本改进(pp) | 剔空仓日后改进(pp) | 全样本 t | 剔空仓日 t | 剔空仓日样本数 | 空仓天数 |",
          "|---|---|---|---|---|---|---|"]
    for s in wf.ALL_STRATEGIES:
        v = stats[f"zero::{s}"]
        e = stats.get(f"zero::{s}__exempty", {})
        L.append(f"| `{s}` | {v['improve_pp']} | {e.get('improve_pp')} | {v['t_stat']} | "
                 f"{e.get('t_stat')} | {e.get('n_days')} | {exempty_days.get(s)} |")
    fragile = []
    for s in wf.ALL_STRATEGIES:
        v = stats[f"zero::{s}"]
        e = stats.get(f"zero::{s}__exempty", {})
        if (exempty_days.get(s, 0) >= 3 and "improve_pp" in e
                and e["improve_pp"] < v["improve_pp"] - 3.0):
            fragile.append((s, v["improve_pp"], e["improve_pp"], exempty_days.get(s)))
    if fragile:
        _parts = [f"`{s}`（{_full}pp → {_ex}pp，空仓 {_nd} 天）"
                  for s, _full, _ex, _nd in fragile]
        L += ["", "**敏感性结论（自动判定）**：" + "、".join(_parts)
              + " 的改进**高度依赖空仓日**（剔掉这些天后大幅缩水）；"
              "其余策略无或几乎没有空仓日，两列基本一致。", ""]
    L += ["", "> 读法：若「剔空仓日后改进」比「全样本改进」**大幅缩水**，说明该策略的改进主要来自"
              "少数几天「置零后一只都买不进 → 空仓避险」的运气，而非全期稳定的选票质量差异 ——"
              "这种收益不可外推，也几乎不可能靠调权重复现。", ""]

    L += ["## 二、压权重口径（权重压到 allocator floor）", "",
          "| 被压的策略 | 压权后累计 | 现行累计 | 改进(pp) | t | 过门 |", "|---|---|---|---|---|---|"]
    for s in wf.ALL_STRATEGIES:
        v = stats.get(f"floor::{s}", {})
        if "improve_pp" not in v:
            continue
        L.append(f"| `{s}` | {v['cum_prop']}% | {v['cum_base']}% | {v['improve_pp']} | "
                 f"{v['t_stat']} | {'✅' if v['robust'] else '❌'} |")
    L += ["", "> 口径提示：「压权重」在生产合成规则下**天然弱** —— 只有当 S 是该票"
              "**权重最大的来源**时，压它的权重才会改变该票权重（来源里有更重策略则该票不受影响）。"
              "因此 §二 的改进幅度**系统性小于** §一，二者**不可直接比大小**："
              "「置零 vs 压权重」是两种强度不同的干预，不是同一实验的两个读数。", ""]

    L += ["## 三、跨 regime 明细（置零口径）", "",
          "| 策略 | regime | 天数 | 留一累计 | 现行累计 | 差(pp) |", "|---|---|---|---|---|---|"]
    for s in wf.ALL_STRATEGIES:
        v = stats.get(f"zero::{s}", {})
        for rg, d in sorted(v.get("regime_table", {}).items()):
            L.append(f"| `{s}` | {rg} | {d['n_days']} | {d['prop_pct']}% | {d['base_pct']}% | "
                     f"{d['diff_pp']} |")

    L += ["", "## 四、判定（预注册判据自动生成；**仅 "
          f"`{GATED_STRATEGY}` 行进入判定**）", "",
          f"> 本表判据是为「动量处置」这一步注册的（`n_trials=1`）。其余行一并列出仅为**透明度**，"
          f"**不得作为行动依据**（理由见 §一.2）。", ""] + verdict_lines + [
        "", f"**判定：{final}。**", "",
        "## 五、Caveats", "",
        f"- 覆盖窗口仅 **{stats['n_days']}** 个信号日（DAL 历史），远窄于策略层的 2018+ 网格 → "
        "结论只反映「生产口径下最近这段」，不能外推为长期规律。",
        "- ⚠️ **「累计」不是实现收益**：本表口径与生产 `walk_forward_validator._cum` 同构 —— "
        "把每个信号日的 **T+1 开盘买 → 10 日后开盘卖**前向收益**逐日复利拼接**。"
        "而这些信号日**前期周频、近期转日频**（同一 DAL 序列里 2025-08 起周频、2026 年转日频），"
        "日频段相邻信号日的 T+10 窗口**互相重叠约 10 倍** → **累计绝对值被显著放大，"
        "不可当作真实收益/回撤来读**。判定只依赖**逐日差值的 t**（不受复利影响）与"
        "**同口径下的相对改进**。",
        "- 复利非线性会压缩真实差异：同一实验里 momentum 的**均值差 +0.103pp/日**"
        "（算术累计约 +9.0pp）但**复利改进仅 +1.16pp**；该落差纯属路径依赖，不是数据问题。"
        "所以「改进(pp)」一列**偏保守**，`t` 更可信。",
        "- 权重用 `_weights_for_day`（因果 edge + 自适应权重），与生产一致；"
        "但**未套用 `factor_timing` 覆盖层**（`run()` 默认 `factor_timing=False`），"
        "故与「线上开着 factor_timing」的当日权重可能有差异。",
        "- regime 取 `wf._regime_as_of`（联网索引的四维状态）；失败时回退「震荡轮动」。",
        "- 「持仓变动」列 = 与现持仓的日均对称差比例（结构性差异，**非硬门**）；"
        "换手守卫只看**提案是否比现行更churn**。",
        "- 本脚本**不改任何生产配置**；任何写回须另过月度回滚 tripwire。"]
    outp_md = wf.STOCK_DATA_DIR / "factor_ic_replay" / f"{OUT_NAME}.md"
    outp_md.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(f"DONE {time.time()-t0:.0f}s -> {outp_md}", flush=True)
    for k in ranked:
        v = stats[k]
        print(f"  {k}: improve={v['improve_pp']}pp t={v['t_stat']} robust={v['robust']}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
