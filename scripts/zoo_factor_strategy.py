#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""因子池存活价格因子 → 选股 CSV 生成器（2026-09-17，A 批 12 因子）。

背景
----
用户 2026-09-17 决定把系统推向「因子驱动主导」：很多**正交、可独立赋权**的因子，
让分配器按各自**已实现 edge** 判断谁是真 edge。前两批已把 boll/relativity 拆成 6 个
价格原子、把基本面拆成 质量/估值/规模。本脚本落第三批（A 批）：

`stock_data/factor_ic_replay/factor_zoo.md`（预注册文法 v1，103 候选 → 存活 21）
里**已验证存活**的纯价格因子，按「每族取 1–2 个代表窗口」挑出 12 个接入菜单（即 A 批）。

⚠️ **公式必须 1:1 复用 `smcore.strategy.factor_zoo.compute_factor`**——不改窗口、
不改有效域口径、不改先验方向。一旦本脚本自己重写公式，「存活」结论就不再适用
（因子池的存活判定是在**它自己的**定义与掩码下做出的）。故本脚本只做三件事：

1. 用 `factor_zoo.Candidate` 声明（名字 / kind / 参数 / 先验方向）——与文法表逐字一致；
2. 用 `factor_engine` 的共享掩码（base_valid + 坏柱回看）圈定有效域；
3. 把横截面 z 分映射成 `Stock-Selection-<Label>-<date>.csv` 的「综合分」契约。

先验方向 → 打分
---------------
因子池的先验 `prior=+1`（做多高值）/ `-1`（做多低值）。本脚本统一以
``adv = prior × raw`` 打分并 z 化，因此**高分恒等于「该因子看好」**，
下游（融合/分配器）无需知道原始方向。例：pvcorr20 先验 −1 → 量价相关越低分越高。

为什么用本地 K 线而不是生产的 EM 宇宙
------------------------------------
生产两策略的候选宇宙来自 EM 实时接口（自标 `universe_pit=False`），历史重放不可信；
因子池的结论也是在**本地 k_data 前复权**上得出的。这里直接用本地缓存（按信号日切片、
PIT 安全、离线可跑），与因子池同口径。

接入纪律
--------
- 不套任何前置筛 / 不做连续触发抑制 / 不加流动性或 ST 过滤——只保留数据有效性约束。
  原子纯度是刻意的：先由分配器按**已实现前向 edge** 决定权重。
- 新策略初期零归因历史 → 冷启动门（`exclude_no_evidence_strategies`）只给 floor
  探索权重，跑出正 edge 才升权；无需人工干预。
- ⚠️ 因子池报告 §六 的警示仍适用：存活因子的 TOP50 超额被「微盘 + 幸存者 + 不可交易
  流动性」三重折扣系统性虚高，**不能直接当 alpha**。本接入不改这一事实——把它交给
  分配器按真实前向 edge 裁决，正是「因子驱动主导」要解决的判据问题。

用法::

    python scripts/zoo_factor_strategy.py                        # 用今天日期
    python scripts/zoo_factor_strategy.py --date 20260916 --top 40
    python scripts/zoo_factor_strategy.py --all-signal-days      # 回填全部历史信号日
    python scripts/zoo_factor_strategy.py --only pvcorr20,vol20  # 只跑指定因子
"""
from __future__ import annotations

import argparse
import csv
import glob
import math
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy import factor_engine as fe  # noqa: E402
from smcore.strategy import factor_zoo as fz  # noqa: E402
from smcore.strategy.factor_types import STRATEGY_LABEL  # noqa: E402

DATA_DIR = ROOT / "stock_data"

# ── 因子规格：与 `factor_zoo.TEMPLATES` / 命名模板**逐字一致** ──────────────
# (策略 id, Candidate)。id 必须已在 factor_types.STRATEGY_ORDER 注册；
# Candidate.name 用因子池的因子名（决定 family 与坏柱回看窗，间接决定有效域）。
#
# vratio20_120 → 文法 kind "volratio2" params (20,120)
# distlo10/60  → 文法 kind "distlo"     params (10,) / (60,)
_SPECS: list[tuple[str, fz.Candidate]] = [
    # 量价相关（corr(日收益, 成交额变化)）——先验多低
    ("pvcorr20", fz.Candidate(name="pvcorr20", kind="pvcorr", prior=-1, params=(20,))),
    ("pvcorr60", fz.Candidate(name="pvcorr60", kind="pvcorr", prior=-1, params=(60,))),
    # 成交稳定性（成交额变异系数）——先验多低
    ("cvamt20", fz.Candidate(name="cvamt20", kind="cvamt", prior=-1, params=(20,))),
    ("cvamt60", fz.Candidate(name="cvamt60", kind="cvamt", prior=-1, params=(60,))),
    # 收益偏度（彩票偏好）——先验多低
    ("skew20", fz.Candidate(name="skew20", kind="skew", prior=-1, params=(20,))),
    ("skew60", fz.Candidate(name="skew60", kind="skew", prior=-1, params=(60,))),
    # 波动比（短期/长期波动 = 波动期限结构）——先验多低
    ("vratio20_120", fz.Candidate(name="vratio20_120", kind="volratio2", prior=-1, params=(20, 120))),
    ("vratio10_60", fz.Candidate(name="vratio10_60", kind="volratio2", prior=-1, params=(10, 60))),
    # 距低点（收盘相对近 N 日最低价的位置）——先验多低（越贴近低点越好）
    ("distlo10", fz.Candidate(name="distlo10", kind="distlo", prior=-1, params=(10,))),
    ("distlo60", fz.Candidate(name="distlo60", kind="distlo", prior=-1, params=(60,))),
    # 波动水平（预注册基线之一，锚在因子池对比集内）
    ("vol20", fz.Candidate(name="vol20", kind="vol", prior=-1, params=(20,))),
    # 非流动性（Amihud）——先验多高
    ("illiq20", fz.Candidate(name="illiq20", kind="illiq", prior=+1, params=(20,))),
]

ALL_IDS = [sid for sid, _ in _SPECS]

# 自适应预热：最长窗口 = vratio20_120 的 120 交易日 → 取 400 自然日（≈270 交易日）留足余量。
_WARMUP_CAL_DAYS = 400


# ── 数据准备 ──────────────────────────────────────────────────────────

def _signal_days() -> list[str]:
    """历史信号日 = 存在 Daily-Action-List 的日期（升序）。"""
    days = []
    for f in glob.glob(str(DATA_DIR / "Daily-Action-List-*.csv")):
        m = re.search(r"Daily-Action-List-(\d{8})\.csv", f)
        if m:
            days.append(m.group(1))
    return sorted(set(days))


def _name_map() -> dict:
    try:
        from smcore.strategy.name_lookup import _get_stock_name_map

        return _get_stock_name_map() or {}
    except Exception as exc:
        print(f"[zoo_factor] WARN: 名称映射不可用（{exc!r}）", file=sys.stderr)
        return {}


# ── 打分 ──────────────────────────────────────────────────────────────

def _zscore(values: list[float]) -> list[float]:
    """返回与输入等长的 z 分；样本<2 或无离散度时返回空列表（= 无信号）。"""
    n = len(values)
    if n < 2:
        return []
    m = sum(values) / n
    var = sum((v - m) ** 2 for v in values) / n
    sd = math.sqrt(var)
    if sd <= 0:
        return []  # 常数因子 → 无横截面信息，不出票（而非给所有人 50 分）
    return [(v - m) / sd for v in values]


def _picks_for_day(fac_row: pd.Series, prior: int, top: int, min_xsec: int = fe.MIN_N_DAY) -> list[dict]:
    """某信号日：先验定向 → 截面 z → 0-100 综合分 → 取前 top。

    ``min_xsec`` 是**横截面下限**（默认沿用引擎的 ``fe.MIN_N_DAY``）。低于它直接不出票：
    截面 z 分在几十只样本上毫无统计意义，产出的「高分票」纯属噪声，却会被融合进 DAL
    并被分配器当成该因子的真实业绩归因 —— 污染权重。宁可当天不出票（写入空表，对
    融合链路是 no-op），也不产噪声。
    """
    s = fac_row.dropna()
    if len(s) < max(2, int(min_xsec)):
        return []
    adv = s * prior  # prior=-1 → 做多低值 = 取 −raw 的高位
    codes = list(adv.index)
    zs = _zscore([float(v) for v in adv.values])
    if not zs:
        return []
    paired = sorted(zip(codes, zs), key=lambda kv: kv[1], reverse=True)
    return [
        {"code": str(c), "score": round(max(0.0, min(100.0, 50.0 + z * 15.0)), 2)}
        for c, z in paired[:top]
    ]


# ── 落盘 ──────────────────────────────────────────────────────────────

def write_csv(label: str, date_str: str, picks: list[dict], name_map: dict, out_dir: Path) -> Path:
    """按契约写 `Stock-Selection-<label>-<date>.csv`（空表也写，保持一致契约）。"""
    out_path = Path(out_dir) / f"Stock-Selection-{label}-{date_str}.csv"
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


def main() -> int:
    ap = argparse.ArgumentParser(description="因子池存活价格因子生成器（A 批 12 + 反向动量）")
    ap.add_argument("--date", default=datetime.now().strftime("%Y%m%d"), help="信号日 YYYYMMDD")
    ap.add_argument("--top", type=int, default=40, help="每个因子输出候选数上限")
    ap.add_argument("--out-dir", default=str(DATA_DIR), help="CSV 输出目录")
    ap.add_argument("--load-start", default=None,
                    help="K 线载入起点（默认 = 首个信号日往前 400 自然日）")
    ap.add_argument("--min-xsec", type=int, default=fe.MIN_N_DAY,
                    help=f"当日有效横截面下限（默认 {fe.MIN_N_DAY}）；低于此值不出票，避免噪声进 DAL")
    ap.add_argument("--all-signal-days", action="store_true", help="回填全部历史信号日")
    ap.add_argument("--only", default=None, help="逗号分隔的因子 id（调试用）")
    args = ap.parse_args()

    if args.all_signal_days:
        days = _signal_days()
    else:
        try:
            datetime.strptime(args.date, "%Y%m%d")
        except ValueError:
            print(f"[zoo_factor] 非法日期 {args.date}")
            return 2
        days = [args.date]
    if not days:
        print("[zoo_factor] 无信号日可取，退出")
        return 1

    specs = _SPECS
    if args.only:
        keep = {s.strip() for s in args.only.split(",") if s.strip()}
        specs = [(sid, c) for sid, c in _SPECS if sid in keep]
        if not specs:
            print(f"[zoo_factor] --only 未匹配任何因子（可选：{','.join(ALL_IDS)}）")
            return 2

    load_start = args.load_start or (
        datetime.strptime(days[0], "%Y%m%d") - timedelta(days=_WARMUP_CAL_DAYS)
    ).strftime("%Y-%m-%d")

    print(f"[zoo_factor] load matrices from {load_start} ...", flush=True)
    mats = fe.load_matrices(
        cols=("close", "high", "low", "open", "volume", "amount"), load_start=load_start
    )
    close = mats["close"]
    ctx = {
        "close": close, "high": mats["high"], "low": mats["low"], "open": mats["open"],
        "volume": mats["volume"], "amount": mats["amount"],
    }
    ret1, bad = fe.daily_returns_and_bad(close)
    ctx["ret1"] = ret1
    base_valid = fe.base_valid_mask(close)
    print(f"[zoo_factor] grid {close.shape[0]} days x {close.shape[1]} codes", flush=True)

    day_ts = {d: pd.Timestamp(f"{d[:4]}-{d[4:6]}-{d[6:]}") for d in days}
    missing_days = [d for d, ts in day_ts.items() if ts not in close.index]
    if missing_days:
        print(f"[zoo_factor] WARN: {len(missing_days)} 个信号日不在 K 线索引内"
              f"（如 {missing_days[:3]}），这些日将只写空表", flush=True)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    name_map = _name_map()
    lb_cache: dict = {}
    summary: list[str] = []

    for sid, cand in specs:
        label = STRATEGY_LABEL[sid]
        try:
            fac = fz.compute_factor(ctx, cand)
        except Exception as exc:  # fail-soft：单个因子失败不拖垮整批
            print(f"[zoo_factor] ERROR {sid}: 计算失败（{exc!r}）", file=sys.stderr)
            summary.append(f"{label}=FAIL")
            continue
        lb = cand.lookback
        fac = fac.where(base_valid & fac.notna() & (fe.lookback_bad(bad, lb, lb_cache) == 0))

        hit_days = 0
        written = 0
        thin_days = 0
        for d in days:
            ts = day_ts[d]
            if ts in fac.index:
                row = fac.loc[ts]
                picks = _picks_for_day(row, cand.prior, args.top, args.min_xsec)
                # 有效截面不足但并非「当天无数据」→ 记为退化日（数据不全，非因子无效）
                if not picks and int(row.notna().sum()) >= 2:
                    thin_days += 1
            else:
                picks = []
            write_csv(label, d, picks, name_map, out_dir)
            if picks:
                hit_days += 1
                written += len(picks)
        thin_note = f"（{thin_days} 日截面过薄已跳过）" if thin_days else ""
        summary.append(f"{label}={hit_days}日/{written}只{thin_note}")
        del fac
        print(f"[zoo_factor] {label:<14} kind={cand.kind:<10} w={cand.params} "
              f"prior={cand.prior:+d} → {hit_days} 日 / {written} 只{thin_note}", flush=True)

    print(f"[zoo_factor] 已写 {len(days)} 个信号日 × {len(specs)} 因子")
    print("[zoo_factor] " + ", ".join(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
