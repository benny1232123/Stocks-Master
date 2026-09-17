#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""价格原子因子生成器（2026-09-17）。

背景
----
用户 2026-09-17 决定把两个「复合策略」沿**价格轴**拆成原子因子，让分配器能逐因子
按已实现 edge 竞权（「因子驱动主导」），而不是被复合体的黑箱内部加权掩盖：

- 原 ``boll``（反转·均值回归）= 前置筛（EM 资金流∩基本面∩重要股东）+ 布林触发
- 原 ``relativity``（相对强度·资金流）= 同一前置筛 + 指数相对强弱触发

两个复合体的**候选宇宙都来自 EM 实时接口**（自标 ``universe_pit=False``），历史重放
不可信。故本生成器**绕开 EM 宇宙，直接用本地 kline 缓存**（按信号日切片、PIT 安全、
离线可用）复算「最后那一层触发」，得到 6 个原子因子：

布林族（互斥优先级，与 ``evaluate_boll_signal`` 一致）::

    Boll_Oversold       超卖：close < 下轨
    Boll_Near_Lower     近下轨：下轨 ≤ close ≤ 下轨×near_ratio（非超卖）
    Boll_Mid_Pullback   中轨回踩：|close-MA|/MA < mid_pullback_pct（非前两者）
    Boll_Squeeze        带宽收口：bandwidth < 近 squeeze_window 日 squeeze_pctile 分位

相对强度族（各自独立，可同时命中）::

    Rel_Up              上涨满足率：指数上涨日 个股跑赢 ≥ up_tol 的占比 ≥ min_up_ratio
    Rel_Down            抗跌满足率：指数下跌日 个股跑赢 ≥ down_outperf 的占比 ≥ min_down_ratio

刻意**不做**的事（保持原子纯度，便于同口径横向对比）
------------------------------------------------
- 不套用复合体的前置筛（资金流/基本面/重要股东/价格带/板块限制）——那只属于复合体。
- 不做连续触发抑制（``continuous_streak_cap``）：原子输出「原始信号」，暴露真实持续性。
- 不添加任何流动性/ST 过滤：只保留数据有效性约束（指标可得、RS 重合交易日足够）。

数据源与纪律
------------
- K 线：``stock_data/k_data/qfq_b*.parquet``（前复权，本地缓存，离线）。
- 基准：沪深300 收盘（``smcore.strategy.regime_filter._get_hs300_close``，主源同花顺云 API）；
  首次取到后落盘 ``stock_data/index_hs300.csv`` 供后续离线复算，避免重复联网。
- 参数：全部读 ``smcore/strategy/risk_config.json`` 的 ``boll`` / ``relativity`` 块，
  **零硬编码魔数**；缺失时用与生产一致的兜底值。
- 输出严格沿用 ``Stock-Selection-<Name>-YYYYMMDD.csv`` 契约（股票代码/股票名称/综合分），
  由 ``picks_loader._load_scored_picks`` 消费。
- 综合分映射与基本面族一致（截面 z → ``50 + 15z``，截断 [0,100]），保证跨策略可比。

⚠️ 因子纯度是刻意的：原子化后再由分配器按**已实现前向 edge** 决定权重；接入初期无归因
历史 → 无证据门控只给 floor 探索权重，跑出正 edge 才升权（见 memory 2026-09-17）。

用法::

    python scripts/price_atom_strategy.py                       # 用今天日期
    python scripts/price_atom_strategy.py --date 20260916 --top 40
    python scripts/price_atom_strategy.py --all-signal-days     # 回填全部历史信号日
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DATA_DIR = ROOT / "stock_data"
K_DATA_DIR = DATA_DIR / "k_data"
RISK_CFG_PATH = ROOT / "smcore" / "strategy" / "risk_config.json"
BENCH_CACHE_PATH = DATA_DIR / "index_hs300.csv"

BOLL_ATOMS = ("Boll_Oversold", "Boll_Near_Lower", "Boll_Mid_Pullback", "Boll_Squeeze")
RS_ATOMS = ("Rel_Up", "Rel_Down")
ATOMS = BOLL_ATOMS + RS_ATOMS

# 指标/信号窗口（非策略超参，是布林定义与最小样本要求）
_BOLL_WINDOW = 20
_RS_MIN_WINDOW_CAL_DAYS = 100  # 仅当 risk_config 缺失 rs_lookback_days 时的兜底

# 兜底参数（与生产 risk_config.json 现值一致；正常全部从 config 读）
_BOLL_FALLBACK = {
    "near_ratio": 1.015,
    "k": 1.645,
    "mid_pullback_pct": 0.02,
    "squeeze_window": 20,
    "squeeze_pctile": 0.2,
}
_RS_FALLBACK = {
    "rs_lookback_days": 100,
    "rs_min_overlap_days": 30,
    "rs_up_tol": -0.010,
    "rs_down_outperf": 0.0,
    "rs_min_up_ratio": 0.6,
    "rs_min_down_ratio": 0.7,
    "rs_min_up_days": 5,
    "rs_min_down_days": 5,
}


def _load_cfg_block(name: str, fallback: dict) -> dict:
    """读 risk_config.json 的某个策略块，缺失字段用兜底值补齐（绝不联网、绝不抛错）。"""
    cfg = dict(fallback)
    try:
        with open(RISK_CFG_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        blk = raw.get(name) or {}
        for k in fallback:
            if blk.get(k) is not None:
                cfg[k] = blk[k]
    except Exception as exc:
        print(f"[price_atom] WARN: 读 risk_config.{name} 失败，用兜底参数（{exc!r}）", file=sys.stderr)
    return cfg


# ── 数据加载 ──────────────────────────────────────────────────────────

def _signal_days() -> list[str]:
    """历史信号日 = 存在 Daily-Action-List 的日期（升序）。"""
    days = []
    for f in glob.glob(str(DATA_DIR / "Daily-Action-List-*.csv")):
        m = re.search(r"Daily-Action-List-(\d{8})\.csv", f)
        if m:
            days.append(m.group(1))
    return sorted(set(days))


def _load_kline(start_iso: str, end_iso: str) -> pd.DataFrame:
    """读全部 K 线桶的 (code, date, close)，限定日期区间。缺文件/坏文件跳过并告警。"""
    frames: list[pd.DataFrame] = []
    files = sorted(K_DATA_DIR.glob("qfq_b*.parquet"))
    if not files:
        print(f"[price_atom] WARN: 未找到 K 线桶（{K_DATA_DIR}）", file=sys.stderr)
        return pd.DataFrame(columns=["code", "date", "close"])
    for pf in files:
        try:
            sub = pd.read_parquet(pf, columns=["code", "date", "close"])
        except Exception as exc:
            print(f"[price_atom] WARN: 读 K 线桶失败 {pf.name}（{exc!r}）", file=sys.stderr)
            continue
        sub = sub[(sub["date"] >= start_iso) & (sub["date"] <= end_iso)]
        if not sub.empty:
            frames.append(sub)
    if not frames:
        return pd.DataFrame(columns=["code", "date", "close"])
    df = pd.concat(frames, ignore_index=True)
    df["code"] = df["code"].astype(str)
    df = df[df["code"].str.len() == 6]
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    # 统一日期口径为 YYYYMMDD（与信号日/DAL 一致；字符串序即日序）
    df["date"] = df["date"].astype(str).str.replace("-", "", regex=False)
    df = df.dropna(subset=["close"])
    df = df[df["close"] > 0]
    df = (
        df.sort_values(["code", "date"])
        .drop_duplicates(subset=["code", "date"], keep="last")
        .reset_index(drop=True)
    )
    return df


def _bench_close() -> pd.Series:
    """沪深300 收盘序列（index=日期字符串 YYYY-MM-DD）。

    优先实时取（主源同花顺云 API）并顺手落盘快照；取不到则回退本地快照。
    两者皆无 → 返回空 Series（RS 原子整体跳过，fail-soft）。
    """
    s = None
    try:
        from smcore.strategy.regime_filter import _get_hs300_close

        raw = _get_hs300_close()
        if raw is not None and len(raw) > 0:
            idx = [pd.Timestamp(d).strftime("%Y%m%d") for d in raw.index]
            s = pd.Series(raw.values.astype(float), index=idx).sort_index()
            try:
                pd.DataFrame({"date": s.index, "close": s.values}).to_csv(
                    BENCH_CACHE_PATH, index=False, encoding="utf-8-sig"
                )
            except Exception:
                pass  # 快照写失败不影响本次计算
    except Exception as exc:
        print(f"[price_atom] WARN: 实时取沪深300 失败（{exc!r}），回退本地快照", file=sys.stderr)

    if s is None:
        if BENCH_CACHE_PATH.exists():
            try:
                b = pd.read_csv(BENCH_CACHE_PATH, dtype={"date": str})
                s = pd.Series(
                    pd.to_numeric(b["close"], errors="coerce").values,
                    index=b["date"].astype(str).str.replace("-", "", regex=False).values,
                ).dropna().sort_index()
            except Exception as exc:
                print(f"[price_atom] WARN: 读沪深300 快照失败（{exc!r}）", file=sys.stderr)
                s = None
    if s is None or len(s) == 0:
        return pd.Series(dtype=float)
    return s


# ── 打分工具 ──────────────────────────────────────────────────────────

def _zscore(values: list[float]) -> list[float]:
    """返回与输入等长的 z 分；样本<2 或无离散度时全 0（无证据）。"""
    n = len(values)
    if n < 2:
        return [0.0] * n
    m = sum(values) / n
    var = sum((v - m) ** 2 for v in values) / n
    sd = math.sqrt(var)
    if sd <= 0:
        return [0.0] * n
    return [(v - m) / sd for v in values]


def _to_score(rows: list[tuple[str, float]], top: int) -> list[dict]:
    """[(code, raw)] → 按 raw 降序取 top，raw 截面 z 化映射到 0-100 综合分。"""
    rows = sorted(rows, key=lambda kv: kv[1], reverse=True)
    if not rows:
        return []
    zs = _zscore([r[1] for r in rows])
    out = []
    for (code, _raw), z in list(zip(rows, zs))[:top]:
        out.append({"code": code, "score": round(max(0.0, min(100.0, 50.0 + z * 15.0)), 2)})
    return out


# ── 布林族原子 ────────────────────────────────────────────────────────

def _boll_atoms_on_day(df: pd.DataFrame, days: set, params: dict) -> dict:
    """在 df（已含全历史 close）上算 4 个布林原子，返回 {atom: {day: [(code, raw)]}}。

    df 需含 code/date/close；函数内部计算 MA/STD/上轨/下轨/带宽及其滚动分位。
    """
    n = _BOLL_WINDOW
    g = df.groupby("code", sort=False)["close"]
    df = df.copy()
    df["MA"] = g.rolling(n, min_periods=n).mean().reset_index(level=0, drop=True).sort_index()
    df["STD"] = g.rolling(n, min_periods=n).std().reset_index(level=0, drop=True).sort_index()

    k = float(params["k"])
    df["Upper"] = df["MA"] + k * df["STD"]
    df["Lower"] = df["MA"] - k * df["STD"]
    df["bandwidth"] = (df["Upper"] - df["Lower"]) / df["MA"]

    sw = int(params["squeeze_window"])
    sp = float(params["squeeze_pctile"])
    df["sq_thr"] = (
        df.groupby("code", sort=False)["bandwidth"]
        .rolling(sw, min_periods=sw)
        .quantile(sp)
        .reset_index(level=0, drop=True)
        .sort_index()
    )

    valid = df["MA"].notna() & df["Lower"].notna() & df["Upper"].notna()
    near_ratio = float(params["near_ratio"])
    mid_pct = float(params["mid_pullback_pct"])

    oversold = valid & (df["close"] < df["Lower"])
    near_lower = valid & (df["close"] >= df["Lower"]) & (df["close"] <= df["Lower"] * near_ratio)
    mid_pull = (
        valid
        & ((df["close"] - df["MA"]).abs() / df["MA"] < mid_pct)
        & ~oversold
        & ~near_lower
    )
    squeeze = (
        valid
        & df["sq_thr"].notna()
        & (df["bandwidth"] < df["sq_thr"])
        & ~oversold
        & ~near_lower
        & ~mid_pull
    )

    dist_lower = (df["Lower"] - df["close"]) / df["Lower"]  # 越深越大
    dist_mid = -(df["close"] - df["MA"]).abs() / df["MA"]  # 越贴近 0 越大
    neg_bw = -df["bandwidth"]  # 越紧越大

    specs = (
        ("Boll_Oversold", oversold, dist_lower),
        ("Boll_Near_Lower", near_lower, dist_lower),
        ("Boll_Mid_Pullback", mid_pull, dist_mid),
        ("Boll_Squeeze", squeeze, neg_bw),
    )
    out: dict = {name: {} for name, _, _ in specs}
    for name, mask, score in specs:
        mask = mask & df["date"].isin(days)
        sub = pd.DataFrame({"code": df.loc[mask, "code"], "date": df.loc[mask, "date"], "raw": score[mask]})
        for day, grp in sub.groupby("date"):
            out[name].setdefault(str(day), []).extend(zip(grp["code"].tolist(), grp["raw"].tolist()))
    return out


# ── 相对强度族原子 ────────────────────────────────────────────────────

def _rs_atoms(df: pd.DataFrame, days: list[str], bench: pd.Series, params: dict) -> dict:
    """算 2 个相对强度原子，返回 {atom: {day: [(code, raw)]}}（raw = 满足率）。"""
    out: dict = {"Rel_Up": {}, "Rel_Down": {}}
    if bench is None or len(bench) == 0:
        return out

    lb = int(params["rs_lookback_days"] or _RS_MIN_WINDOW_CAL_DAYS)
    min_overlap = int(params["rs_min_overlap_days"])
    up_tol = float(params["rs_up_tol"])
    dn_out = float(params["rs_down_outperf"])
    min_up_ratio = float(params["rs_min_up_ratio"])
    min_dn_ratio = float(params["rs_min_down_ratio"])
    min_up_days = int(params["rs_min_up_days"])
    min_dn_days = int(params["rs_min_down_days"])

    d = df.sort_values(["code", "date"])
    d = d.assign(sret=d.groupby("code", sort=False)["close"].pct_change())
    d = d.dropna(subset=["sret"])

    bench_df = pd.DataFrame({
        "date": bench.index.astype(str),
        "iret": bench.pct_change().values,
    }).dropna()
    m = d.merge(bench_df, on="date", how="inner")
    if m.empty:
        return out

    m = m.assign(excess=m["sret"] - m["iret"], up=m["iret"] > 0, dn=m["iret"] < 0)

    # 统一有效性口径：**必须**在信号日本身有 K 线（当日有成交）。
    # 若放宽为「窗口内有数据即可」，则信号日恰为休市日（DAL 仍在节假日生成）时会用
    # 陈旧数据产出「当天根本买不到」的票，且与布林族「当日无 bar → 无信号」口径不一致，
    # 让跨原子对比失真。此守卫等价于复合体的停牌/陈旧剔除。
    on_day_codes = {
        str(dd): set(g["code"]) for dd, g in df[df["date"].isin(days)].groupby("date")
    }

    for day in days:
        live_codes = on_day_codes.get(day)
        if not live_codes:
            continue
        try:
            lo = (datetime.strptime(day, "%Y%m%d") - timedelta(days=lb)).strftime("%Y%m%d")
        except ValueError:
            continue
        w = m[(m["date"] > lo) & (m["date"] <= day)]
        w = w[w["code"].isin(live_codes)]
        if w.empty:
            continue
        gp = w.groupby("code")
        overlap = gp.size()
        up_days = w[w["up"]].groupby("code").size()
        dn_days = w[w["dn"]].groupby("code").size()
        up_ok = w[w["up"] & (w["excess"] >= up_tol)].groupby("code").size()
        dn_ok = w[w["dn"] & (w["excess"] >= dn_out)].groupby("code").size()

        agg = pd.DataFrame({
            "overlap": overlap,
            "up_days": up_days,
            "dn_days": dn_days,
            "up_ok": up_ok,
            "dn_ok": dn_ok,
        }).fillna(0.0)
        agg = agg[agg["overlap"] >= min_overlap]
        if agg.empty:
            continue
        up_ratio = (agg["up_ok"] / agg["up_days"]).where(agg["up_days"] > 0, 0.0)
        dn_ratio = (agg["dn_ok"] / agg["dn_days"]).where(agg["dn_days"] > 0, 0.0)

        up_sel = agg[(agg["up_days"] >= min_up_days) & (up_ratio >= min_up_ratio)]
        if not up_sel.empty:
            out["Rel_Up"][day] = list(zip(up_sel.index.tolist(), up_ratio.loc[up_sel.index].tolist()))
        dn_sel = agg[(agg["dn_days"] >= min_dn_days) & (dn_ratio >= min_dn_ratio)]
        if not dn_sel.empty:
            out["Rel_Down"][day] = list(zip(dn_sel.index.tolist(), dn_ratio.loc[dn_sel.index].tolist()))
    return out


# ── 落盘 ──────────────────────────────────────────────────────────────

def write_csv(atom: str, date_str: str, picks: list[dict], name_map: dict, out_dir: Path) -> Path:
    """按契约写 `Stock-Selection-<atom>-<date>.csv`（空表也写，保持一致契约）。"""
    out_path = Path(out_dir) / f"Stock-Selection-{atom}-{date_str}.csv"
    with open(out_path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["股票代码", "股票名称", "综合分"])
        w.writeheader()
        for p in picks:
            w.writerow({
                "股票代码": p["code"],
                "股票名称": name_map.get(p["code"], ""),
                "综合分": p["score"],
            })
    return out_path


def _name_map() -> dict:
    try:
        from smcore.strategy.name_lookup import _get_stock_name_map

        return _get_stock_name_map() or {}
    except Exception as exc:
        print(f"[price_atom] WARN: 名称映射不可用（{exc!r}）", file=sys.stderr)
        return {}


def main() -> int:
    ap = argparse.ArgumentParser(description="价格原子因子生成器（布林族 4 + 相对强度族 2）")
    ap.add_argument("--date", default=datetime.now().strftime("%Y%m%d"), help="信号日 YYYYMMDD")
    ap.add_argument("--top", type=int, default=40, help="每个原子输出候选数上限")
    ap.add_argument("--out-dir", default=str(DATA_DIR), help="CSV 输出目录")
    ap.add_argument("--all-signal-days", action="store_true", help="回填全部历史信号日")
    args = ap.parse_args()

    if args.all_signal_days:
        days = _signal_days()
    else:
        try:
            datetime.strptime(args.date, "%Y%m%d")
        except ValueError:
            print(f"[price_atom] 非法日期 {args.date}")
            return 2
        days = [args.date]
    if not days:
        print("[price_atom] 无信号日可取，退出")
        return 1

    first, last = days[0], days[-1]
    # 布林需 20 个交易日预热；RS 需回看 rs_lookback_days 个自然日 → 统一多取 130 自然日
    start_iso = (datetime.strptime(first, "%Y%m%d") - timedelta(days=130)).strftime("%Y-%m-%d")
    end_iso = f"{last[:4]}-{last[4:6]}-{last[6:]}"

    boll_p = _load_cfg_block("boll", _BOLL_FALLBACK)
    rs_p = _load_cfg_block("relativity", _RS_FALLBACK)

    df = _load_kline(start_iso, end_iso)
    if df.empty:
        print("[price_atom] K 线为空，无法计算（离线环境下请确认 k_data 缓存）")
        return 1
    print(f"[price_atom] K 线载入 {len(df)} 行 / {df['code'].nunique()} 只，区间 {start_iso}~{end_iso}")

    day_set = set(days)

    out_dir = Path(args.out_dir)
    name_map = _name_map()

    # 布林族：必须传**全历史** df（滚动指标需预热），原子函数内部只挑信号日行
    boll_raw = _boll_atoms_on_day(df, day_set, boll_p)

    rs_raw = _rs_atoms(df, days, _bench_close(), rs_p)

    all_raw = {**boll_raw, **rs_raw}
    summary = []
    for atom in ATOMS:
        per_day = all_raw.get(atom, {})
        written = 0
        for day in days:
            picks = _to_score(per_day.get(day, []), args.top)
            write_csv(atom, day, picks, name_map, out_dir)
            written += len(picks)
        n_days_hit = sum(1 for d in days if per_day.get(d))
        summary.append(f"{atom}={'/'.join([str(n_days_hit), str(written)])}")

    print(f"[price_atom] 已写 {len(days)} 个信号日 × {len(ATOMS)} 原子"
          f"（格式 命中日数/累计只数：{', '.join(summary)}）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
