"""风险中性化单元测试：行业权重上限 + 组合 β 软约束 + 单名仓位上限。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from smcore.config.defaults import BETA_FALLBACK, MAX_SINGLE_WEIGHT_PCT
from smcore.strategy import fusion as fusion_mod
from smcore.strategy import position_sizing as ps_mod
from smcore.strategy.sectors import apply_sector_weight_cap


def _df(codes_industries_weights):
    rows = []
    for code, ind, w in codes_industries_weights:
        rows.append({"股票代码": code, "建议仓位%": w, "综合评分": 100.0})
    return pd.DataFrame(rows)


def test_weight_cap_limits_sector_total_weight():
    """同一板块总仓位不得超过组合总仓位 × max_weight_pct。"""
    sector_map = {"000001": "银行", "000002": "银行", "000003": "银行", "000004": "地产", "000005": "地产"}
    # 5 只票各 20% -> 银行 60% 远超 20% 上限，应被砍到约 20%（1~2 只）
    df = _df([
        ("000001", "银行", 20.0), ("000002", "银行", 20.0), ("000003", "银行", 20.0),
        ("000004", "地产", 20.0), ("000005", "地产", 20.0),
    ])
    out, hit = apply_sector_weight_cap(df, sector_map, max_weight_pct=20.0, top_n=5)
    assert hit is True
    bank_w = out[out["股票代码"].map(lambda c: sector_map[c]) == "银行"]["建议仓位%"].sum()
    assert bank_w <= 20.0 + 1e-6


def test_weight_cap_unknown_industry_not_capped():
    """未映射行业（"未知"）不计入板块上限，不被误砍。"""
    sector_map = {"000001": "银行"}
    df = _df([("000001", "银行", 20.0), ("000002", "未知", 20.0), ("000003", "未知", 20.0)])
    out, hit = apply_sector_weight_cap(df, sector_map, max_weight_pct=20.0, top_n=3)
    # 未知行业两只都应保留（不计入银行上限）
    assert (out["股票代码"] == "000002").any()
    assert (out["股票代码"] == "000003").any()


def test_weight_cap_no_sector_map_returns_head():
    df = _df([("000001", "x", 20.0), ("000002", "y", 20.0)])
    out, hit = apply_sector_weight_cap(df, None, top_n=1)
    assert hit is False
    assert len(out) == 1


def test_portfolio_beta_weighted():
    df = pd.DataFrame([
        {"股票代码": "000001", "建议仓位%": 50.0},
        {"股票代码": "000002", "建议仓位%": 50.0},
    ])
    betas = {"000001": 1.0, "000002": 1.5}
    assert abs(fusion_mod._portfolio_beta(df, betas) - 1.25) < 1e-9


def test_portfolio_beta_fallback_when_missing():
    df = pd.DataFrame([{"股票代码": "000001", "建议仓位%": 100.0}])
    # 缺 β 映射 -> 用 BETA_FALLBACK
    assert fusion_mod._portfolio_beta(df, {}) == BETA_FALLBACK


def test_beta_cap_trims_highest_beta():
    df = pd.DataFrame([
        {"股票代码": "000001", "建议仓位%": 25.0},
        {"股票代码": "000002", "建议仓位%": 25.0},
        {"股票代码": "000003", "建议仓位%": 25.0},
        {"股票代码": "000004", "建议仓位%": 25.0},
    ])
    # 三只 2.0 β + 一只 0.2 β -> 初始组合 β=(6.2)/4=1.55 > 1.2，应剔除高 β 至 ≤1.2
    betas = {"000001": 2.0, "000002": 2.0, "000003": 2.0, "000004": 0.2}
    out, n = fusion_mod._apply_beta_cap(df, betas, max_beta=1.2, min_keep=2)
    assert n >= 1
    assert fusion_mod._portfolio_beta(out, betas) <= 1.2 + 1e-9


def test_estimate_betas_fallback_without_index(monkeypatch):
    """沪深300 序列不可得时，所有个股回退 BETA_FALLBACK，不抛、不阻断。"""
    monkeypatch.setattr(ps_mod, "_get_hs300_close", lambda: None)
    betas = ps_mod._estimate_betas(["000001", "000002"], "20260731")
    assert all(v == BETA_FALLBACK for v in betas.values())


def test_estimate_betas_local_kdata(monkeypatch, tmp_path):
    """有本地 k_data 时返回数值 β（用合成序列验证 cov/var 计算路径不崩）。"""
    import numpy as np

    # 构造沪深300 序列
    idx = pd.Series(
        np.cumprod(1 + np.random.RandomState(0).normal(0.001, 0.01, 80)),
        index=pd.date_range("2026-03-01", periods=80, freq="B"),
    )
    monkeypatch.setattr(ps_mod, "_get_hs300_close", lambda: idx)

    # 写一只合成个股 k_data（与指数高度相关）到临时目录
    kdir = tmp_path / "k_data"
    kdir.mkdir()
    closes = np.cumprod(1 + np.random.RandomState(1).normal(0.0012, 0.012, 80))
    d = pd.DataFrame({
        "date": pd.date_range("2026-03-01", periods=80, freq="B").strftime("%Y-%m-%d"),
        "close": closes,
    })
    d.to_csv(kdir / "000001_qfq_full.csv", index=False)

    monkeypatch.setattr(ps_mod, "STOCK_DATA_DIR", tmp_path)
    betas = ps_mod._estimate_betas(["000001"], "20260731")
    assert "000001" in betas
    assert isinstance(betas["000001"], float)
    assert betas["000001"] > 0  # 正相关应得正 β


def test_max_single_weight_default_is_conservative():
    """单名仓位上限默认应为保守值（两位数 %），直接削单名尾部风险。"""
    assert 0 < MAX_SINGLE_WEIGHT_PCT <= 15.0


def test_single_weight_cap_clamps_one_dominant_name():
    """单策略权重极高且只活 1 只时，单名仓位被压到上限以下。"""
    df = pd.DataFrame([{"股票代码": "000001", "来源策略": "Boll", "综合评分": 100.0}])
    weights = {"boll": 80.0, "momentum": 0, "theme": 0, "relativity": 0, "cctv": 0}
    surv = {"boll": 1}
    out, n_hit = fusion_mod._apply_position_sizing(
        df, weights, surv, total_capital=100000.0, max_single_weight_frac=MAX_SINGLE_WEIGHT_PCT / 100.0
    )
    assert out["建议仓位%"].iloc[0] <= MAX_SINGLE_WEIGHT_PCT + 1e-9
    assert n_hit == 1


def test_single_weight_cap_not_triggered_when_small():
    """策略权重本就小于上限时，不应误截断。"""
    df = pd.DataFrame([{"股票代码": "000001", "来源策略": "Boll", "综合评分": 100.0}])
    weights = {"boll": 5.0}
    surv = {"boll": 1}
    out, n_hit = fusion_mod._apply_position_sizing(
        df, weights, surv, total_capital=100000.0, max_single_weight_frac=MAX_SINGLE_WEIGHT_PCT / 100.0
    )
    assert abs(out["建议仓位%"].iloc[0] - 5.0) < 1e-9
    assert n_hit == 0


def test_single_weight_cap_takes_best_strategy_weight():
    """多策略命中时取命中策略中权重最高者分配，仍受单名上限约束。"""
    df = pd.DataFrame([{"股票代码": "000001", "来源策略": "Boll/CCTV", "综合评分": 100.0}])
    weights = {"boll": 90.0, "cctv": 10.0}
    surv = {"boll": 1, "cctv": 1}
    out, n_hit = fusion_mod._apply_position_sizing(
        df, weights, surv, total_capital=100000.0, max_single_weight_frac=MAX_SINGLE_WEIGHT_PCT / 100.0
    )
    # 取 max(90/1, 10/1)=90% -> 被压到 ≤10%
    assert out["建议仓位%"].iloc[0] <= MAX_SINGLE_WEIGHT_PCT + 1e-9
    assert n_hit == 1
