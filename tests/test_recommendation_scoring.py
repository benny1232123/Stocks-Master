"""持仓建议三维打分回归测试（2026-09-15 修两处缺陷）。

背景（用户实测「持仓日报建议不太对」，逐字段复算后定位到的两处**逻辑**缺陷，
数值本身可 bit 级复现，所以不是算错而是口径错）：

1. **缺失因子被当成「中等」稀释面均值**
   旧实现：`fund_scores.append(sc)` 无条件追加 → 营收增速 rg 取不到值时塞 missing(50)
   进分母。等于把「未知」当「中等」，把基本面分系统性拉向 50。
   实测：002284 基本面 75.5→被压到 70；600269 74.0→69；603187 67.5→64；603390 60.5→58。
   修法：`missing_factor_policy="exclude"`（默认）→ 缺失因子不进分母；
   整面一个因子都取不到时才回落到该面 missing 分。

2. **MACD「水下金叉」拿满分**
   旧实现只判 `dif>dea and macd_hist>0`，不区分零轴上下 → 603187（dif/dea 双深负
   −0.256、仅差 0.0005、hist≈0.00095）拿到与强势股真金叉同权的 +2。
   因技术面权重 0.40，techS±2 = techScore±12 = 综合分±4.8，足以跨 58/70 档界。
   修法：dif>0 为「水上金叉」满分（macd_golden_red）；dif<=0 为「水下金叉」减半
   （macd_golden_red_below）。

3. **`_seg_score` 的 missing 基准被资金面复用**
   旧签名 `_seg_score(segments, value)` 内部恒用 `missing_f`（基本面），资金面也走它。
   当前 fundamental.missing == capital.missing == 50 才没暴露，改配置即错。
   修法：missing 由调用方按「哪一面」显式传入。

本测试全部为**纯离线合成输入**（直接构造 analysis dict，不读网络 / 不读 k_data）。
"""
from __future__ import annotations

import copy

import pytest

from smcore.analysis import recommendation_from_analysis
from smcore.config.defaults import RECOMMENDATION_CONFIG


# ───────────────────────── 工具 ─────────────────────────

def _band(table, value, missing):
    """复刻分段取值语义（first-match-wins），用于在测试里自算期望值。"""
    if value is None:
        return missing
    for row in table:
        if "gt" in row and value > float(row["gt"]):
            return float(row["score"])
        if "lt" in row and value < float(row["lt"]):
            return float(row["score"])
    return float(table[-1]["score"])


def _js_round(x: float) -> int:
    import math
    return int(math.floor(x + 0.5))


def _analysis(latest=None, metrics=None, fund=None):
    """构造最小 analysis 结构；未给的字段一律缺省（= None，对应信号不命中）。"""
    a = {"latest": dict(latest or {}), "metrics": dict(metrics or {})}
    if fund is not None:
        a["fundamentals"] = dict(fund)
    return a


def _cfg(**over):
    c = copy.deepcopy(RECOMMENDATION_CONFIG)
    c.update(over)
    return c


# ───────────────────── 缺陷 1：缺失因子口径 ─────────────────────

# 用户实际持仓 002284 的真实基本面（rg 因 v1 缓存无该字段而恒缺失）
FUND_002284 = {"pe": 13.198986, "pb": 2.072172, "roe": 0.156261, "gross_margin": 0.208274}


def test_missing_factor_excluded_from_fundamental_mean():
    """rg 缺失时，基本面面分 = 4 个可用因子的均值，而不是带 missing=50 的 5 项均值。"""
    rec = recommendation_from_analysis(_analysis(fund=FUND_002284))
    w_f = RECOMMENDATION_CONFIG["fundamental"]
    avail_v = {"pe": FUND_002284["pe"], "pb": FUND_002284["pb"],
               "roe": FUND_002284["roe"], "gm": FUND_002284["gross_margin"]}
    scores = [_band(w_f[k], v, w_f["missing"]) for k, v in avail_v.items()]
    expect_exclude = _js_round(sum(scores) / len(scores))
    assert rec["faces"]["fundamental"] == expect_exclude
    # 旧行为（把 missing 塞进分母）必须更低 —— 这就是「被稀释」的方向
    scores_neutral = scores + [float(w_f["missing"])]
    expect_neutral = _js_round(sum(scores_neutral) / len(scores_neutral))
    assert expect_exclude > expect_neutral
    # 营收增速未参与打分 → 不该出现在 drivers 里（不能显示一个没算进去的因子）
    assert not any("营收增长" in d for d in rec["drivers"])


def test_missing_factor_policy_neutral_restores_old_behaviour():
    """missing_factor_policy=neutral 可一键回滚到旧口径（便于 A/B 与回退）。"""
    a = _analysis(fund=FUND_002284)
    new = recommendation_from_analysis(a, cfg=_cfg(missing_factor_policy="exclude"))
    old = recommendation_from_analysis(a, cfg=_cfg(missing_factor_policy="neutral"))
    w_f = RECOMMENDATION_CONFIG["fundamental"]
    avail_v = {"pe": FUND_002284["pe"], "pb": FUND_002284["pb"],
               "roe": FUND_002284["roe"], "gm": FUND_002284["gross_margin"]}
    scores = [_band(w_f[k], v, w_f["missing"]) for k, v in avail_v.items()]
    scores.append(float(w_f["missing"]))
    assert old["faces"]["fundamental"] == _js_round(sum(scores) / len(scores))
    assert new["faces"]["fundamental"] > old["faces"]["fundamental"]


def test_all_fundamental_factors_missing_falls_back_to_face_missing():
    """该面一个因子都取不到 → 回落到该面 missing 分（不是 0，也不是空）。"""
    rec = recommendation_from_analysis(_analysis(fund={}))
    assert rec["faces"]["fundamental"] == int(RECOMMENDATION_CONFIG["fundamental"]["missing"])
    assert rec["faces"]["capital"] == int(RECOMMENDATION_CONFIG["capital"]["missing"])


def test_capital_missing_uses_capital_basis_not_fundamental():
    """资金面缺失必须用 capital.missing，而不是复用 basic 的 missing_f。

    旧实现 `_seg_score` 恒用 missing_f；这里把两者设成不同值即暴露。
    """
    a = _analysis(latest={}, fund={})  # hasF=True（{} 非 error），两面因子全缺
    cfg = _cfg(missing_factor_policy="exclude")
    cfg["fundamental"] = dict(cfg["fundamental"], missing=11)
    cfg["capital"] = dict(cfg["capital"], missing=88)
    rec = recommendation_from_analysis(a, cfg=cfg)
    assert rec["faces"]["fundamental"] == 11
    assert rec["faces"]["capital"] == 88


def test_capital_partial_missing_averages_available_only():
    """资金面只有换手率可用时 → 面分 = 换手率分，不被成交额缺失拉向 50。"""
    fund = {"turnover": 3.913}  # amount_20 缺失
    rec = recommendation_from_analysis(_analysis(fund=fund))
    w_c = RECOMMENDATION_CONFIG["capital"]
    assert rec["faces"]["capital"] == _js_round(_band(w_c["turnover"], 3.913, w_c["missing"]))
    assert any("换手" in d for d in rec["drivers"])
    assert not any("日均成交" in d for d in rec["drivers"])


# ───────────────────── 缺陷 2：MACD 零轴口径 ─────────────────────

def test_macd_below_zero_golden_cross_is_reduced():
    """零轴下方（dif<=0）金叉 → 用 macd_golden_red_below，且标签标明「水下」。"""
    latest = {"dif": -0.25605923338256353, "dea": -0.25653467552032017,
              "macd_hist": 0.000950884275513264}  # 603187 2026-09-11 真实值
    rec = recommendation_from_analysis(_analysis(latest=latest))
    below = float(RECOMMENDATION_CONFIG["technical"]["macd_golden_red_below"])
    full = float(RECOMMENDATION_CONFIG["technical"]["macd_golden_red"])
    assert below < full, "水下金叉必须低于水上金叉，否则等于没修"
    base = float(RECOMMENDATION_CONFIG["tech_base"])
    step = float(RECOMMENDATION_CONFIG["tech_step"])
    assert rec["faces"]["technical"] == int(base + below * step)
    assert any("水下金叉" in d for d in rec["drivers"])


def test_macd_above_zero_golden_cross_keeps_full_score():
    """零轴上方（dif>0）金叉 → 仍拿满分（回归：别把真金叉也降权）。"""
    latest = {"dif": 0.2386194194118012, "dea": 0.15958056320848313,
              "macd_hist": 0.15807771240663615}  # 603390 2026-09-11 真实值
    rec = recommendation_from_analysis(_analysis(latest=latest))
    full = float(RECOMMENDATION_CONFIG["technical"]["macd_golden_red"])
    base = float(RECOMMENDATION_CONFIG["tech_base"])
    step = float(RECOMMENDATION_CONFIG["tech_step"])
    assert rec["faces"]["technical"] == int(base + full * step)
    assert any("金叉红柱" in d for d in rec["drivers"])
    assert not any("水下" in d for d in rec["drivers"])


def test_macd_dead_cross_unchanged():
    """死叉绿柱保持原样（本次只改金叉侧，避免无依据地扩大改动面）。"""
    latest = {"dif": 0.13736, "dea": 0.16130, "macd_hist": -0.047887}  # 002284
    rec = recommendation_from_analysis(_analysis(latest=latest))
    dead = float(RECOMMENDATION_CONFIG["technical"]["macd_dead_green"])
    base = float(RECOMMENDATION_CONFIG["tech_base"])
    step = float(RECOMMENDATION_CONFIG["tech_step"])
    assert rec["faces"]["technical"] == int(base + dead * step)
    assert any("死叉绿柱" in d for d in rec["drivers"])


def test_dif_exactly_zero_is_treated_as_below():
    """dif 恰好为 0 时归「水下」分支（边界不打满分）。"""
    latest = {"dif": 0.0, "dea": -0.01, "macd_hist": 0.02}
    rec = recommendation_from_analysis(_analysis(latest=latest))
    below = float(RECOMMENDATION_CONFIG["technical"]["macd_golden_red_below"])
    base = float(RECOMMENDATION_CONFIG["tech_base"])
    step = float(RECOMMENDATION_CONFIG["tech_step"])
    assert rec["faces"]["technical"] == int(base + below * step)


# ───────────────────── 端到端：四只真实持仓 ─────────────────────

REAL = {
    "002284": (dict(FUND_002284, amount_20=256881803.967, turnover=3.913),
               {"dif": 0.13735840456403636, "dea": 0.16130177833353976,
                "macd_hist": -0.04788674753900679, "rsi": 39.92527555731585,
                "k_val": 28.642009005875735, "d_val": 40.5748693704609},
               {"ma5": 10.074, "ma10": 10.189, "ma20": 9.9235, "close": 9.74,
                "lower": 9.2763, "upper": 10.5406}),
    "600269": (dict(pe=12.429512, pb=0.467433, roe=0.06706, gross_margin=0.37722,
                    amount_20=86327522.6055, turnover=0.8212),
               {"dif": 0.006117356346832192, "dea": -0.009405126175134051,
                "macd_hist": 0.031044965043932487, "rsi": 46.59272435015102,
                "k_val": 50.65760808218286, "d_val": 63.25557228077903},
               {"ma5": 3.952, "ma10": 3.916, "ma20": 3.859, "close": 3.89,
                "lower": 3.7868, "upper": 3.9822}),
    "603187": (dict(pe=13.221034, pb=1.011165, roe=0.091934, gross_margin=0.276339,
                    amount_20=94169779.7085, turnover=0.9661),
               {"dif": -0.25605923338256353, "dea": -0.25653467552032017,
                "macd_hist": 0.000950884275513264, "rsi": 23.601344036231595,
                "k_val": 21.64401835090524, "d_val": 28.698008568618604},
               {"ma5": 10.978, "ma10": 11.0, "ma20": 11.078, "close": 10.65,
                "lower": 10.8081, "upper": 11.3519}),
    "603390": (dict(pe=28.525771, pb=2.117511, roe=0.052695, gross_margin=0.292341,
                    amount_20=56475094.157500006, turnover=1.6393),
               {"dif": 0.2386194194118012, "dea": 0.15958056320848313,
                "macd_hist": 0.15807771240663615, "rsi": 63.663896845526565,
                "k_val": 54.80772377180344, "d_val": 57.36708278263754},
               {"ma5": 10.986, "ma10": 10.689, "ma20": 10.3865, "close": 11.05,
                "lower": 9.5989, "upper": 11.2661}),
}


@pytest.mark.parametrize("code", sorted(REAL))
def test_real_holdings_scores_are_stable_and_improved(code):
    """4 只真实持仓：新口径下基本面分不得低于旧口径，且各面落在 0-100。"""
    fund, macd, ma = REAL[code]
    a = _analysis(latest={**macd, **ma}, fund=fund)
    new = recommendation_from_analysis(a, cfg=_cfg(missing_factor_policy="exclude"))
    old = recommendation_from_analysis(a, cfg=_cfg(missing_factor_policy="neutral"))
    assert 0 <= new["faces"]["technical"] <= 100
    assert 0 <= new["faces"]["fundamental"] <= 100
    assert 0 <= new["faces"]["capital"] <= 100
    # rg 全缺失 → 新口径修复的是「被拉向 50」，所以只会抬高或持平
    assert new["faces"]["fundamental"] >= old["faces"]["fundamental"]
    # 综合分与 rating 必须自洽（档位由 rating 表派生，不是写死的）
    total = new["score"]
    gtes = [(r.get("gte"), r["label"]) for r in RECOMMENDATION_CONFIG["rating"]]
    expect = next(lbl for gte, lbl in gtes if gte is not None and total >= gte) \
        if any(g is not None and total >= g for g, _ in gtes) else gtes[-1][1]
    assert new["rating"] == expect


# ───────────────────── 缺陷 4：ROE 累计口径年化 ─────────────────────
# background：THS / baostock 的 `index_weighted_avg_roe` 是**年初至今累计**口径
# （002284 2025：Q1 3.39% → Q2 6.68% → Q3 10.73% → 年报 15.66%，单调递增），
# 而 RECOMMENDATION_CONFIG.fundamental.roe 的阈值 0.10/0.15/0.20 是按**年度** ROE 设的。
# 不做年化 → 同一只票 Q1 落「偏低」(50 分)、年报才落「良好」(82 分)，面分随报告日历漂移
# （单因子摆动 32）。修法：`fundamental.annualize_roe(roe, roe_period)` 只在**消费方**
# （analysis.py 日报 / 前端 App.jsx）套用；数据层 `_extract_for_asof` 输出 `roe` 保持源口径，
# 仅额外给出 `roe_period`。旧 v1 扁平缓存无 roe_period（其值本就是年度）→ 原样返回。


def test_annualize_roe_multipliers():
    """Q1×4 / Q2×2 / Q3×4/3 / Q4×1（用 002284 2025 真实累计路径校验）。"""
    from smcore.strategy.fundamental import annualize_roe
    assert annualize_roe(0.0339, "2025-03-31") == pytest.approx(0.0339 * 4)
    assert annualize_roe(0.0668, "2025-06-30") == pytest.approx(0.0668 * 2)
    assert annualize_roe(0.1073, "2025-09-30") == pytest.approx(0.1073 * 4 / 3)
    assert annualize_roe(0.1566, "2025-12-31") == pytest.approx(0.1566)


def test_annualize_roe_passthrough_without_period():
    """无报告期（旧 v1 扁平缓存，值本就年度）或非法期号 → 原样返回，不猜。"""
    from smcore.strategy.fundamental import annualize_roe
    assert annualize_roe(0.1566, None) == 0.1566
    assert annualize_roe(0.1566, "") == 0.1566
    assert annualize_roe(0.1566, "2025-13-31") == 0.1566
    assert annualize_roe(None, "2025-06-30") is None


def _fund_mean(roe):
    """给定 roe，按 exclude 口径算基本面期望面分（5 因子齐全）。"""
    w_f = RECOMMENDATION_CONFIG["fundamental"]
    pe, pb, gm, rg = 13.0, 2.0, 0.20, 0.10
    return _js_round(sum([
        _band(w_f["pe"], pe, w_f["missing"]),
        _band(w_f["pb"], pb, w_f["missing"]),
        _band(w_f["roe"], roe, w_f["missing"]),
        _band(w_f["gm"], gm, w_f["missing"]),
        _band(w_f["rg"], rg, w_f["missing"]),
    ]) / 5)


def test_recommendation_annualizes_roe_before_threshold():
    """同一「年度水平」ROE 无论落在 Q1 还是年报，面分应一致（不被报告日历带偏）。"""
    base = {"pe": 13.0, "pb": 2.0, "gross_margin": 0.20, "revenue_growth": 0.10}
    q1 = recommendation_from_analysis(
        _analysis(fund={**base, "roe": 0.03, "roe_period": "2025-03-31"}))  # 年化 0.12
    fy = recommendation_from_analysis(
        _analysis(fund={**base, "roe": 0.12, "roe_period": "2025-12-31"}))  # 0.12
    assert q1["faces"]["fundamental"] == fy["faces"]["fundamental"] == _fund_mean(0.12)


def test_recommendation_without_roe_period_uses_raw_roe():
    """旧 v1 扁平缓存无 roe_period（其 roe 本就是年度值）→ 不得年化。

    若误按 Q1 年化（×4），0.03 → 0.12 会跨档抬高面分，本测试守住这个回归。
    """
    base = {"pe": 13.0, "pb": 2.0, "gross_margin": 0.20, "revenue_growth": 0.10}
    rec = recommendation_from_analysis(_analysis(fund={**base, "roe": 0.03}))
    assert rec["faces"]["fundamental"] == _fund_mean(0.03)
    # 必须严格低于「误年化」的结果，证明没被年化
    assert rec["faces"]["fundamental"] < _fund_mean(0.12)


# ───────── 前后端一致性：年化系数不得漂移 ─────────

def test_frontend_annualize_multipliers_match_backend():
    """前端 useScoringConfig.js 的 ROE 年化系数必须与后端 annualize_roe 一致。"""
    import re
    from pathlib import Path
    from smcore.strategy.fundamental import _ROE_ANNUALIZE_MULT

    root = Path(__file__).resolve().parents[1]
    js = (root / "frontend" / "src" / "config" / "useScoringConfig.js").read_text(encoding="utf-8")
    block = re.search(r"ROE_ANNUALIZE_MULT\s*=\s*\{(.*?)\}", js, re.S)
    assert block, "前端缺少 ROE_ANNUALIZE_MULT 定义"
    pairs = dict(re.findall(r"['\"](\d{2}-\d{2})['\"]\s*:\s*([0-9./ ]+)", block.group(1)))
    assert set(pairs) == set(_ROE_ANNUALIZE_MULT), "前后端报告期集合不一致"
    for mmdd, mult in _ROE_ANNUALIZE_MULT.items():
        assert abs(eval(pairs[mmdd]) - mult) < 1e-9, f"{mmdd} 系数前后端不一致"

    # 且 App.jsx 必须真的调用它（否则 helper 形同虚设）
    app = (root / "frontend" / "src" / "App.jsx").read_text(encoding="utf-8")
    assert re.search(r"annualizeRoe\(\s*\w+\s*,\s*F\?\.roe_period\s*\)", app), \
        "App.jsx 未把 roe_period 传给 annualizeRoe"
