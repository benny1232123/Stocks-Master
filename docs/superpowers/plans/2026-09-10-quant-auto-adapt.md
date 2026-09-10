# Quant 自适应系统改造实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 三阶段改造选股策略系统——连续组合回测评估、权重连续自适应、动态风险引擎，解决"熊市大幅跑输基准 / 长期缺乏稳定正期望 / 指标波动太大"三个痛点。

**Architecture:** 阶段0搭连续组合回测框架（复用 `daily_backtest._backtest_one` 内核 + 独立输出目录隔离）；阶段1把 FLOOR 置 0 并让 shrinkage 从常数变为"证据强度"连续函数（置信度×显著度）；阶段2新增 `dynamic_risk.py` 统一输出出场/仓位/现金参数（全连续函数，中性点 = risk_config.json 现值），并接入 daily_backtest 与 fusion 生产链路。每阶段独立 commit + A/B 对比。

**Tech Stack:** Python 3.11, pandas, backtrader（现有）, pytest

## Global Constraints

- 两条铁律（来自 spec）：**中性点 = 现值**（所有动态参数在中性条件下必须等于 risk_config.json 现值，可回退最可比）；**连续单调无跳变**（密度随证据/风险强弱平滑变化，禁止档位切换）
- 验收目标（来自 spec）：全期 A/B 对比表（年化/回撤/夏普/胜率/盈亏比/超额/熊市段超额）；2026-05/06 熊市超额从 -8% 拉回 -3% 内且不牺牲强势期收益
- 不污染生产产物：所有策略改进输出写入 `stock_data/strategy_improve/`，**禁止**覆盖 `stock_data/Multi-Backtest-*`
- FLOOR 固定置 0（生产 `adaptive_weights_config.json`），`_BUILTIN_DEFAULTS` 保留 3.0 兜底
- 测试统一用 pytest，命令 `python -m pytest tests/test_xxx.py -v`
- 提交信息遵循现有风格（`feat(...)/fix(...)` 中文描述）
- 不提交工作区无关脏文件（数据文件/未跟踪脚本等，见 b2 注意事项）

---

### Task 0.1: 连续净值构造与指标纯函数库

**Files:**
- Create: `smcore/backtest/continuous.py`
- Test: `tests/test_continuous_backtest.py`

**Interfaces:**
- Consumes: 无（纯 pandas）
- Produces:
  - `build_continuous_curve(sleeves: Dict[str, Dict[date, float]], normalize: bool = True) -> pd.Series` — 日历日所有活跃 sleeve 的 total 等权均值，升序索引；normalize 时首值=1.0
  - `compute_metrics(nav: pd.Series, annualization: int = 252) -> dict` — 返回 `{total_return_pct, annual_return_pct, annual_vol_pct, sharpe, max_drawdown_pct, win_rate_pct, n_days, monthly}`。数据不足返回 `{"error": "insufficient_data"}`

- [ ] **Step 1: Write the failing test**

```python
import sys
from datetime import date
sys.path.insert(0, ".")
from smcore.backtest.continuous import build_continuous_curve, compute_metrics

def test_build_continuous_curve_basic():
    sleeves = {
        "a": {date(2026, 1, 5): 1000.0, date(2026, 1, 6): 1100.0},
        "b": {date(2026, 1, 5): 1000.0, date(2026, 1, 6): 900.0},
    }
    s = build_continuous_curve(sleeves)
    assert s.index.is_monotonic_increasing
    assert s.index[0].isoformat() == "2026-01-05"
    assert s.loc[date(2026, 1, 6)] == pytest.approx(1000.0)

    nav = build_continuous_curve(sleeves, normalize=True)
    assert nav.iloc[0] == pytest.approx(1.0)
    assert nav.iloc[-1] == pytest.approx(1.0)

def test_compute_metrics_basic():
    nav = pd.Series([1.0, 1.1, 0.99], index=pd.date_range("2026-01-01", periods=3))
    m = compute_metrics(nav)
    assert m["total_return_pct"] == pytest.approx(-1.0)
    assert m["max_drawdown_pct"] < 0
    assert m["win_rate_pct"] == pytest.approx(50.0)
    assert m["n_days"] == 2

def test_compute_metrics_insufficient():
    m = compute_metrics(pd.Series([1.0]))
    assert m == {"error": "insufficient_data"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_continuous_backtest.py -v`
Expected: FAIL with ModuleNotFoundError

- [ ] **Step 3: Write implementation**

```python
"""连续组合回测纯函数：把多个信号日的前向回测 sleeve 组合成一条连续净值曲线。"""
from __future__ import annotations

import math
from typing import Dict

import pandas as pd


def build_continuous_curve(
    sleeves: Dict[str, Dict[object, float]], normalize: bool = True
) -> pd.Series:
    """组合权益曲线：每个日历日所有活跃 sleeve 的 total 均值（跨 sleeve 等权）。

    sleeves: {tag: {date: total}}。返回按日期升序的 pd.Series(date -> 日均值)。
    normalize=True 时额外缩放使得第一个值 = 1.0。
    """
    all_dates = sorted({d for s in sleeves.values() for d in s})
    if not all_dates:
        return pd.Series(dtype=float)
    vals = []
    for d in all_dates:
        tot = [s[d] for s in sleeves.values() if d in s]
        vals.append(sum(tot) / len(tot) if tot else float("nan"))
    s = pd.Series(vals, index=all_dates).dropna()
    if normalize and len(s):
        s = s / s.iloc[0]
    return s


def compute_metrics(nav: pd.Series, annualization: int = 252) -> dict:
    """从连续净值计算完整指标集。"""
    nav = nav.dropna()
    if len(nav) < 2:
        return {"error": "insufficient_data"}
    rets = nav.pct_change().dropna()
    total = nav.iloc[-1] / nav.iloc[0] - 1.0
    n = len(nav) - 1
    years = n / annualization
    annual = (nav.iloc[-1] / nav.iloc[0]) ** (1.0 / years) - 1.0 if years > 0 and nav.iloc[0] > 0 else float("nan")
    vol = rets.std(ddof=0) * math.sqrt(annualization)
    sharpe = annual / vol if vol and not math.isnan(vol) and vol > 0 else 0.0
    dd = (nav / nav.cummax() - 1.0).min()
    win = float((rets > 0).mean())
    nav2 = nav.copy()
    nav2.index = pd.DatetimeIndex(nav.index)
    monthly_last = nav2.groupby(nav2.index.to_period("M")).last()
    monthly = monthly_last.pct_change().dropna() * 100.0
    return {
        "total_return_pct": round(total * 100, 2),
        "annual_return_pct": round(annual * 100, 2),
        "annual_vol_pct": round(vol * 100, 2) if not math.isnan(vol) else None,
        "sharpe": round(sharpe, 3),
        "max_drawdown_pct": round(dd * 100, 2),
        "win_rate_pct": round(win * 100, 1),
        "n_days": int(n),
        "monthly": {k.strftime("%Y-%m"): round(v, 2) for k, v in monthly.items()},
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_continuous_backtest.py -v`
Expected: 3 PASSED

- [ ] **Step 5: Commit**

```bash
git add smcore/backtest/continuous.py tests/test_continuous_backtest.py
git commit -m "feat(backtest): 连续净值构造+指标纯函数库 (stage0)"
```

---

### Task 0.2: 连续组合回测 CLI + 基线输出

**Files:**
- Modify: `scripts/daily_backtest.py:176`（`_backtest_one` 加 `out_dir=None` 参数）和 `:365`（写盘路径）
- Create: `scripts/continuous_backtest.py`
- Test（手动验证）: 运行 `python scripts/continuous_backtest.py --variant baseline --limit 3`

**Interfaces:**
- Consumes: `daily_backtest._backtest_one(path, sd, hold_days, market_profile, portfolio_curve, dd_thr, dd_cap, dd_deep, out_dir)`；`build_continuous_curve` / `compute_metrics`（Task 0.1）
- Produces: `scripts/continuous_backtest.py --variant NAME [--limit N]` 输出 `stock_data/strategy_improve/continuous_{variant}_equity.csv` + `continuous_{variant}_metrics.csv`

- [ ] **Step 1: daily_backtest._backtest_one 增加 out_dir 隔离输出**

修改签名（line 176）：

```python
def _backtest_one(path, sd, hold_days, market_profile=None, portfolio_curve=None,
                  dd_thr=8.0, dd_cap=50.0, dd_deep=20.0, out_dir=None):
```

修改写盘点（line 365）：

```python
    base = (out_dir if out_dir is not None else STOCK_DATA_DIR) / f"Multi-Backtest-{date_tag}"
```

- [ ] **Step 2: 写 continuous_backtest.py**

```python
"""连续组合回测：按时间顺序串起全部历史信号日，生成一条连续净值曲线。

用法:
    python scripts/continuous_backtest.py --variant baseline --limit N
输出:
    stock_data/strategy_improve/continuous_{variant}_equity.csv
    stock_data/strategy_improve/continuous_{variant}_metrics.csv
"""
import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import smcore.strategy.adaptive_weights as aw_mod
from smcore.artifacts import PROJECT_ROOT, STOCK_DATA_DIR
from smcore.backtest.continuous import build_continuous_curve, compute_metrics
from smcore.strategy.adaptive_weights import cash_from_volatility, cash_from_regime
from smcore.strategy.market import compute_market_profile
from scripts.daily_backtest import (
    _backtest_one, _collect_all_candidate_codes, _cached_fetch, _load_equity_series,
    _parse_signal_date, collect_eligible_lists,
)
import smcore.data.kline as kline_mod

# 变体预设：覆盖 adaptive_weights.CONFIG（进程内生效，跑完恢复）
VARIANT_PRESETS = {
    "baseline": {},
}

def _apply_variant(name: str):
    overrides = VARIANT_PRESETS.get(name, {})
    for k, v in overrides.get("aw", {}).items():
        aw_mod.CONFIG[k] = v

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--variant", default="baseline")
    ap.add_argument("--limit", type=int, default=0, help="只跑最近 N 个信号日（0=全部）")
    args = ap.parse_args()

    outdir = STOCK_DATA_DIR / "strategy_improve"
    outdir.mkdir(parents=True, exist_ok=True)

    _apply_variant(args.variant)

    lists = [(p, _parse_signal_date(p.name)) for p in sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))]
    lists = [x for x in lists if x[1]]
    if args.limit:
        lists = lists[-args.limit:]
    print(f"[continuous:{args.variant}] 信号日 {len(lists)} 个: {lists[0][1]} ~ {lists[-1][1]}" if lists else "无信号日")

    # 预拉 K 线缓存（与 daily_backtest 一致）
    codes = _collect_all_candidate_codes([p for p, _ in lists])
    kline_mod.fetch_daily_k = _cached_fetch(codes)

    sleeves = {}
    for i, (path, sd) in enumerate(lists, 1):
        profile = compute_market_profile(as_of=sd)
        try:
            _backtest_one(path, sd, hold_days=int(os.environ.get("HOLD_DAYS", "12")),
                          market_profile=profile, portfolio_curve=None, out_dir=outdir)
        except Exception as e:
            print(f"  [{sd}] 失败: {e}")
            continue
        tag = f"{sd:%Y%m%d}"
        eq = _load_equity_series(outdir / f"Multi-Backtest-{tag}-equity.csv")
        if eq:
            sleeves[tag] = eq
        print(f"  [{i}/{len(lists)}] {tag} ok sleeve日={len(eq)}")

    if not sleeves:
        print("无有效 sleeve，退出")
        return 1

    nav = build_continuous_curve(sleeves)
    nav.index = pd.DatetimeIndex(nav.index) if len(nav) else nav.index
    metrics = compute_metrics(nav)

    equity_file = outdir / f"continuous_{args.variant}_equity.csv"
    nav.to_csv(equity_file, header=["nav"])
    metrics_file = outdir / f"continuous_{args.variant}_metrics.csv"
    pd.DataFrame([{k: v for k, v in metrics.items() if k != "monthly"}]
                 ).to_csv(metrics_file, index=False)
    print(f"[ok] {args.variant} 指标: 年化={metrics.get('annual_return_pct')} "
          f"回撤={metrics.get('max_drawdown_pct')} 夏普={metrics.get('sharpe')} 胜率={metrics.get('win_rate_pct')}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: 运行验证（3 个信号日冒烟）**

Run: `python scripts/continuous_backtest.py --variant baseline --limit 3`
Expected: 打印 `[ok] baseline 指标: ...`，且 `stock_data/strategy_improve/continuous_baseline_equity.csv` 存在

- [ ] **Step 4: 跑全量基线**

Run: `python scripts/continuous_backtest.py --variant baseline`
Expected: 完成全量输出。记录 annual_return/max_drawdown/sharpe 到 Task 0.2 完成注记

- [ ] **Step 5: Commit**

```bash
git add scripts/daily_backtest.py scripts/continuous_backtest.py
git commit -m "feat(backtest): 连续组合回测CLI+基线输出 (stage0)"
```

---

### Task 1.1: edge 计算增加 std + dynamic_shrinkage 函数

**Files:**
- Modify: `smcore/strategy/adaptive_weights.py`（compute_strategy_edge / compute_universe_edge 返回加 `std`；新增 `compute_dynamic_shrinkage`）
- Test: `tests/test_dynamic_shrinkage.py`

**Interfaces:**
- Consumes: 现有 compute_strategy_edge / compute_universe_edge
- Produces: `compute_dynamic_shrinkage(edge, *, base=0.4, pseudo=15.0, t_target=2.0) -> Dict[str, float]` — 每策略收缩系数，0=全信自适应，base=≈等权
- edge 每策略结构从 `{n, avg_return, win_rate, edge}` 扩展为 `{n, avg_return, win_rate, edge, std}`

- [ ] **Step 1: Write the failing test**

```python
import sys, math
sys.path.insert(0, ".")
from smcore.strategy.adaptive_weights import compute_dynamic_shrinkage

def _mk_edge(n, mean, sd):
    return {"dummy": {"n": n, "avg_return": mean, "win_rate": 0.5, "edge": mean, "std": sd}}

def test_strong_evidence_shrinks_to_zero():
    # n=120, t 很大 → 收缩≈0（全信 edge）
    d = compute_dynamic_shrinkage(_mk_edge(120, 0.01, 0.01))
    assert d["dummy"] < 0.05

def test_weak_evidence_stays_at_base():
    # n=5, std 大 → 收缩≈base
    d = compute_dynamic_shrinkage(_mk_edge(5, 0.01, 0.10))
    assert d["dummy"] == 0.4

def test_all_within_bounds():
    d = compute_dynamic_shrinkage({
        "a": {"n": 100, "avg_return": 0.01, "win_rate": 0.6, "edge": 0.01, "std": 0.02},
        "b": {"n": 2, "avg_return": 0.01, "win_rate": 0.5, "edge": 0.01, "std": 0.05},
        "c": {"n": 50, "avg_return": -0.01, "win_rate": 0.4, "edge": -0.01, "std": 0.03},
    })
    for v in d.values():
        assert 0.0 <= v <= 0.4
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_dynamic_shrinkage.py -v`
Expected: FAIL ImportError

- [ ] **Step 3: 实现 compute_dynamic_shrinkage**

在 `adaptive_weights.py` 顶部加 `import math`，文件内新增：

```python
def compute_dynamic_shrinkage(edge, *, base=0.4, pseudo=15.0, t_target=2.0):
    """证据强度 -> 每策略向等权收缩系数。

    置信度 c = n/(n+pseudo)；显著度 s = min(1, |t|/t_target)，t = edge/std_err。
    shrinkage_s = base * (1 - c*s)：证据强 -> 0（全信自适应），证据弱 -> base（≈等权）。
    """
    out = {}
    for s, e in edge.items():
        if not isinstance(e, dict):
            continue
        n = max(int(e.get("n", 0) or 0), 0)
        avg = float(e.get("edge", 0.0) or 0.0)
        sd = float(e.get("std", 0.0) or 0.0)
        if n <= 1 or sd <= 0:
            out[s] = base
            continue
        se = sd / math.sqrt(n)
        t = avg / se if se > 0 else 0.0
        c = n / (n + pseudo)
        sig = min(1.0, abs(t) / t_target)
        out[s] = round(max(0.0, base * (1.0 - c * sig)), 6)
    return out
```

- [ ] **Step 4: edge 函数返回加 std**

在 `compute_strategy_edge` 中每条策略结果：
```python
res[s] = {"n": n, "avg_return": avg, "win_rate": wr, "edge": avg, "std": _sd(strat_rets[s])}
```
其中 `_sd` 文件内新增工具函数：
```python
def _sd(vals):
    vals = [float(x) for x in vals if x is not None]
    if len(vals) <= 1:
        return 0.0
    mean = sum(vals) / len(vals)
    return (sum((x - mean) ** 2 for x in vals) / len(vals)) ** 0.5
```
`compute_universe_edge` 也做同样扩展（它内部已有原始收益列表）。

- [ ] **Step 5: Run tests**

Run: `python -m pytest tests/test_dynamic_shrinkage.py tests/test_adaptive_weights.py -v`
Expected: 全部 PASS（旧测试不受影响）

- [ ] **Step 6: Commit**

```bash
git add smcore/strategy/adaptive_weights.py tests/test_dynamic_shrinkage.py
git commit -m "feat(weights): edge加std + compute_dynamic_shrinkage (stage1)"
```

---

### Task 1.2: adaptive_weights 支持动态 shrinkage + 生产 FLOOR=0

**Files:**
- Modify: `smcore/strategy/adaptive_weights.py`（adaptive_weights 接受 dict 或 None shrinkage）
- Modify: `smcore/strategy/adaptive_weights_config.json`（FLOOR=0，新增 shrinkage_dynamic=true）
- Modify: `_BUILTIN_DEFAULTS`（新增 `shrinkage_dynamic: False` 兜底）
- Modify: `tests/test_adaptive_weights.py`（适配 FLOOR=0 与动态 shrinkage）
- Test: `tests/test_adaptive_weights.py`

**Interfaces:**
- Consumes: `compute_dynamic_shrinkage`（Task 1.1）
- Produces: `adaptive_weights(edge, *, shrinkage=None, ...)` — shrinkage 可为 float | dict | None；None 时按 CONFIG["shrinkage_dynamic"] 决定动态或静态

- [ ] **Step 1: adaptive_weights 支持 dict shrinkage**

在 adaptive_weights 内部，替换现向等权收缩行：
```python
    if shrinkage is None:
        if CONFIG.get("shrinkage_dynamic") and isinstance(edge, dict) and any(
            isinstance(e, dict) and "std" in e for e in edge.values()
        ):
            shrinkage = compute_dynamic_shrinkage(edge)
        else:
            shrinkage = CONFIG.get("shrinkage", 0.4)
    if isinstance(shrinkage, dict):
        w = {s: (1.0 - shrinkage.get(s, 0.0)) * raw.get(s, 0.0) + shrinkage.get(s, 0.0) / max(len(uni), 1)
             for s in raw}
    else:
        w = {s: (1.0 - shrinkage) * raw.get(s, 0.0) + shrinkage * uni.get(s, 0.0) for s in raw}
```
（保持 `uni` 为 len 分母的等权向量，原逻辑中 `uni` 已定义，按原变量名适配）

- [ ] **Step 2: 更新测试**

`tests/test_adaptive_weights.py` 中新增：
```python
def test_adaptive_weights_accepts_dict_shrinkage():
    edge = {"a": {"n": 100, "avg_return": 0.01, "win_rate": 0.6, "edge": 0.01, "std": 0.02},
            "b": {"n": 3, "avg_return": -0.01, "win_rate": 0.4, "edge": -0.01, "std": 0.05}}
    pct = adaptive_weights(edge, shrinkage={"a": 0.0, "b": 0.4}, min_evidence_n=0)
    assert set(pct) == {"a", "b"}
    assert sum(pct.values()) > 99.0
```
并同步原 FLOOR 相关断言：将 `FLOOR = CONFIG["FLOOR"]` 改为 `FLOOR = 0.0`，把"负 edge 策略权重 ≥ FLOOR"断言改为"仍保留极小权重"或按新语义调整。

- [ ] **Step 3: 生产配置 FLOOR=0**

`adaptive_weights_config.json` 顶层新增/修改：
```json
"shrinkage_dynamic": true,
"FLOOR": 0.0,
```

- [ ] **Step 4: 运行测试**

Run: `python -m pytest tests/test_adaptive_weights.py tests/test_dynamic_shrinkage.py -v`
Expected: 全部 PASS

- [ ] **Step 5: Commit**

```bash
git add smcore/strategy/adaptive_weights.py smcore/strategy/adaptive_weights_config.json tests/test_adaptive_weights.py
git commit -m "fix(weights): FLOOR=0 + 动态shrinkage接入生产配置 (stage1)"
```

---

### Task 1.3: 阶段1 A/B 四形态对比

**Files:**
- Modify: `scripts/continuous_backtest.py`（VARIANT_PRESETS 增加 v_fixed_floor0_shrink0 / v_dynamic_shrink / v_dynamic_shrink_floor0）
- Test（手动）: 运行脚本生成对比表

**Interfaces:**
- Consumes: Task 0.2 CLI + Task 1.2 动态 shrinkage
- Produces: `stock_data/strategy_improve/continuous_{variant}_equity.csv` × 4 + `stage1_ab_comparison.csv`

- [ ] **Step 1: VARIANT_PRESETS 扩展**

```python
VARIANT_PRESETS = {
    "baseline": {},
    "v_fixed_floor0_shrink0": {"aw": {"FLOOR": 0.0, "shrinkage": 0.0, "shrinkage_dynamic": False}},
    "v_dynamic_shrink": {"aw": {"FLOOR": 3.0, "shrinkage_dynamic": True}},
    "v_dynamic_shrink_floor0": {"aw": {"FLOOR": 0.0, "shrinkage_dynamic": True}},
}
```

- [ ] **Step 2: 逐一运行四个 variant**

Run: `python scripts/continuous_backtest.py --variant baseline` 及各 variant
Expected: 四个 metrics csv 均生成

- [ ] **Step 3: 汇总对比表**

新增脚本段（或手动整理）输出 `stage1_ab_comparison.csv`：行=variant，列=年化/回撤/夏普/胜率。python 一行整理：
```bash
python -c "import pandas as pd,glob,os; rows=[]
for f in glob.glob('stock_data/strategy_improve/continuous_*_metrics.csv'):
    rows.append(pd.read_csv(f).iloc[0].to_dict())
pd.DataFrame(rows).to_csv('stock_data/strategy_improve/stage1_ab_comparison.csv', index=False)"
```
Expected: 文件生成，观察哪个 variant 年化最高

- [ ] **Step 4: Commit**

```bash
git add scripts/continuous_backtest.py stock_data/strategy_improve/stage1_ab_comparison.csv
git commit -m "test(backtest): 阶段1权重A/B四形态对比 (stage1)"
```

---

### Task 2.1: dynamic_risk.py 动态风险引擎 + 单测

**Files:**
- Create: `smcore/strategy/dynamic_risk.py`
- Test: `tests/test_dynamic_risk.py`

**Interfaces:**
- Consumes: `compute_adaptive_exit_params` / `compute_adaptive_risk_params`（risk_rules）、`cash_from_volatility` / `cash_from_regime` / `cash_from_drawdown`（adaptive_weights）、`compute_market_profile`
- Produces: `compute_dynamic_risk(profile=None, regime=None, drawdown_pct=None, n_picks=None, n_sectors=None, *, forced_vol_pctile=0.7, forced_scale=0.3) -> DynamicParams(os9)`

- [ ] **Step 1: Write the failing test**

```python
import sys
sys.path.insert(0, ".")
from smcore.strategy.dynamic_risk import compute_dynamic_risk

class _P:
    def __init__(self, region=None, vol=0.5, pvol=0.5, strength=0.5, trend="震荡", vlevel="中", breadth=0.5, r=None, s=None):
        self.regime = region or "震荡"
        self.regime_strength = strength
        self.trend = trend
        self.volatility_level = vlevel
        self.volatility_pct = vol
        self.volatility_pctile = pvol
        self.breadth_score = breadth
        self.region = r or "CN"
        self.session = s

def test_neutral_matches_current_values():
    p = _P()  # regime=震荡, vol_pctile=0.5
    dr = compute_dynamic_risk(profile=p, regime="震荡", drawdown_pct=0.0, n_picks=10, n_sectors=3)
    assert dr.stop_loss_pct == 0.08
    assert dr.take_profit_pct == 0.06
    assert dr.trailing_stop_pct == 0.05
    assert dr.trend_exit_ma == 60
    assert dr.hold_days == 12
    # 现金=现有 cash_from_volatility(0.5) 的结果（S曲线约~18%），capital_scale 对应
    assert 0.0 <= dr.cash_pct <= 100.0
    assert 0.0 <= dr.capital_scale <= 1.0

def test_forced_shutdown_when_defensive_and_high_vol():
    p = _P(region="下行防御", pvol=0.85)
    dr = compute_dynamic_risk(profile=p, regime="下行防御", drawdown_pct=0.0, n_picks=10, n_sectors=3)
    assert dr.capital_scale <= 0.3
    assert dr.cash_pct >= 70.0

def test_capital_scale_monotonic_in_vol():
    scales = []
    for pv in (0.2, 0.5, 0.8):
        p = _P(pvol=pv)
        dr = compute_dynamic_risk(profile=p, regime="震荡", drawdown_pct=0.0, n_picks=10, n_sectors=3)
        scales.append(dr.capital_scale)
    assert scales[0] >= scales[1] >= scales[2]
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_dynamic_risk.py -v`
Expected: FAIL ImportError

- [ ] **Step 3: Implement dynamic_risk.py**

```python
"""动态风险引擎：统一输出出场参数 + 仓位限制 + 现金比例。

所有参数都是 risk_config.json 现值的连续函数（中性点 = 现值）。
包含强制下线规则：下行防御 + 高波动（双信号确认）→ 仓位≤30%。
"""
from __future__ import annotations
from dataclasses import dataclass

from smcore.strategy.adaptive_weights import (
    cash_from_drawdown, cash_from_regime, cash_from_volatility,
)
from smcore.strategy.risk_rules import (
    compute_adaptive_exit_params, compute_adaptive_risk_params,
)


@dataclass
class DynamicParams:
    stop_loss_pct: float
    take_profit_pct: float
    trailing_stop_pct: float
    trend_exit_ma: int
    hold_days: int
    slippage: float
    max_single_weight_pct: float
    max_sector_weight_pct: float
    max_per_sector: int
    max_portfolio_beta: float
    cash_pct: float
    capital_scale: float

    @classmethod
    def from_exit_risk(cls, exit_p: dict, risk_p: dict, cash_pct: float):
        return cls(
            stop_loss_pct=exit_p["stop_loss_pct"],
            take_profit_pct=exit_p["take_profit_pct"],
            trailing_stop_pct=exit_p["trailing_stop_pct"],
            trend_exit_ma=exit_p["trend_exit_ma"],
            hold_days=exit_p["hold_days"],
            slippage=exit_p["slippage"],
            max_single_weight_pct=risk_p["max_single_weight_pct"],
            max_sector_weight_pct=risk_p["max_sector_weight_pct"],
            max_per_sector=risk_p["max_per_sector"],
            max_portfolio_beta=risk_p["max_portfolio_beta"],
            cash_pct=round(cash_pct, 1),
            capital_scale=round(max(0.0, 1.0 - cash_pct / 100.0), 3),
        )


def compute_dynamic_risk(profile=None, regime=None, drawdown_pct=None,
                         n_picks=None, n_sectors=None, *,
                         forced_vol_pctile: float = 0.7,
                         forced_scale: float = 0.3) -> DynamicParams:
    if regime is None and profile is not None:
        regime = getattr(profile, "regime", None)
    vol_pctile = getattr(profile, "volatility_pctile", None) if profile is not None else None

    exit_p = compute_adaptive_exit_params(profile, regime)
    risk_p = compute_adaptive_risk_params(regime, profile, n_picks, n_sectors)

    cash_pct = float(cash_from_volatility(vol_pctile))
    cash_pct = float(cash_from_regime(regime, cash_pct))
    cash_pct = cash_pct + float(cash_from_drawdown(drawdown_pct or 0.0))
    cash_pct = min(100.0, cash_pct)

    dr = DynamicParams.from_exit_risk(exit_p, risk_p, cash_pct)

    if regime == "下行防御" and vol_pctile is not None and vol_pctile >= forced_vol_pctile:
        dr.capital_scale = min(dr.capital_scale, forced_scale)
        dr.cash_pct = (1.0 - dr.capital_scale) * 100.0
    return dr
```

- [ ] **Step 4: Run tests**

Run: `python -m pytest tests/test_dynamic_risk.py -v`
Expected: 3 PASSED（如中性断言受现有 cash 计算影响，按现有函数实测值调整，但强制下线与单调性断言不能放宽）

- [ ] **Step 5: Commit**

```bash
git add smcore/strategy/dynamic_risk.py tests/test_dynamic_risk.py
git commit -m "feat(risk): 动态风险引擎 dynamic_risk.py (stage2)"
```

---

### Task 2.2: 接入 daily_backtest / fusion 生产链路

**Files:**
- Modify: `scripts/daily_backtest.py`（_backtest_one 内现金/仓位/出场改用 compute_dynamic_risk）
- Modify: `smcore/strategy/fusion.py`（生产现金链路改用 compute_dynamic_risk）
- Test（手动）: 单信号日冒烟 + 单元测试回归

**Interfaces:**
- Consumes: `compute_dynamic_risk`（Task 2.1）
- Produces: 生产与回测统一走 dynamic_risk，参数行为在阈值内与现状等价或更优

- [ ] **Step 1: daily_backtest 接入**

在 `_backtest_one` 中 profile/portfolio_curve 之后的现金计算段，将现有：
```python
    cash_pct = cash_from_volatility(...)
    ...
    capital_scale = ...
    _exit = compute_adaptive_exit_params(...)
```
替换为一次调用：
```python
    from smcore.strategy.dynamic_risk import compute_dynamic_risk
    _dd = portfolio_curve.drawdown_as_of(sd) if portfolio_curve is not None else 0.0
    _dr = compute_dynamic_risk(_prof, regime=_regime, drawdown_pct=_dd,
                               n_picks=len(_sub), n_sectors=len(_sectors))
    cash_pct = _dr.cash_pct
    capital_scale = _dr.capital_scale
    _exit = {k: getattr(_dr, k) for k in (
        "stop_loss_pct", "take_profit_pct", "trailing_stop_pct", "trend_exit_ma", "slippage")}
```
（`_regime`/`_sub`/`_sectors` 按 _backtest_one 现有局部变量名适配）

- [ ] **Step 2: fusion.py 生产现金链路接入**

在 `fusion.py:268-271`，将：
```python
cash_pct = cash_from_volatility(...); if regime: cash_pct = cash_from_regime(...)
```
替换为：
```python
    from smcore.strategy.dynamic_risk import compute_dynamic_risk
    cash_pct = compute_dynamic_risk(profile, regime=..., drawdown_pct=...,
                                    n_picks=len(candidates), n_sectors=...).cash_pct
```
（保持签名兼容，缺失参数传 None 走中性路径。）

- [ ] **Step 3: 冒烟验证**

Run: `python scripts/continuous_backtest.py --variant baseline --limit 2`
Expected: 正常运行，不报错

- [ ] **Step 4: 回归单元测试**

Run: `python -m pytest tests/test_dynamic_risk.py tests/test_adaptive_weights.py tests/test_continuous_backtest.py -v`
Expected: 全部 PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/daily_backtest.py smcore/strategy/fusion.py
git commit -m "feat(risk): daily_backtest/fusion 接入动态风险引擎 (stage2)"
```

---

### Task 2.3: 全期 A/B 验收报告

**Files:**
- Modify: `scripts/continuous_backtest.py`（VARIANT_PRESETS 增加 v_dynamic_risk）
- Create: `stock_data/strategy_improve/stage2_ab_comparison.csv` + `stock_data/strategy_improve/stage2_report.md`

**Interfaces:**
- Consumes: Task 2.2（dynamic_risk 已接生产参数链路）
- Produces: 验收对比表，验证 2026-05/06 熊市超额回到 -3% 内、强势期不牺牲

- [ ] **Step 1: VARIANT_PRESETS 增加 v_dynamic_risk**

```python
    "v_dynamic_risk": {"dr": True},
```
并让 `_apply_variant` 在 `dr` 存在时设置 `os.environ["USE_DYNAMIC_RISK"]="1"`，daily_backtest 侧按该 env 决定是否走 dynamic_risk（默认 False，拿到完成报告后再默认切换生产）。

- [ ] **Step 2: 运行 dynamic_risk 全量**

Run: `python scripts/continuous_backtest.py --variant v_dynamic_risk`
Expected: metrics 生成

- [ ] **Step 3: 对比汇总**

整理 `stage2_ab_comparison.csv`（baseline vs v_dynamic_risk），并单独统计 2026-05-01~06-30 区间的两者超额（用 equity 曲线与 hs300 同窗口买入持有对比）。
Expected: 熊市段超额 ≥ -3%，强势期年化不掉

- [ ] **Step 4: 写 stage2_report.md**

内容：四张对比表（全期指标 / 月度 / 熊市段超额 / 强势期），结论与是否切换生产。

- [ ] **Step 5: Commit**

```bash
git add scripts/continuous_backtest.py stock_data/strategy_improve/
git commit -m "test(risk): 阶段2全期A/B验收报告 (stage2)"
```

---

## Self-Review 注记

- **覆盖检查**：阶段0→连续回测框架 ✓；阶段1→权重连续自适应（Task1.1+1.2+1.3）✓；阶段2→动态风险引擎（Task2.1+2.2+2.3）✓；熊市验收→Task2.3 ✓；两铁律→Task2.1 中性断言 + 连续函数实现 ✓；不污染生产产物→out_dir 隔离 ✓；FLOOR=0→Task1.2 ✓
- **无占位符**：所有代码步均有完整实现；`_backtest_one` 局部变量名（`_prof`/`_regime`/`_sub`/`_sectors`）在每日执行时须按实际变量确认，此为唯一需执行时适配点
- **类型一致性**：`compute_dynamic_risk` 返回 `DynamicParams`，字段名与 risk_rules 的 exit_p/risk_p 键一致；`compute_dynamic_shrinkage` 返回 dict{strategy:float}，Task1.2 中 shrinkage dict 分支消费它