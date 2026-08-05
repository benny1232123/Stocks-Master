"""PaperPortfolio 持仓漂移再平衡 + 回撤熔断 回归测试（Round 17）。

测试围绕「无网络」展开：β 估计与 simulate_position 均 monkeypatch，
直接构造持仓字典断言再平衡动作与权益/回撤记账。
"""
import types

import pytest

from smcore.strategy import position_monitor as pm


def _pf(**kw):
    return pm.PaperPortfolio(initial_capital=1_000_000.0, **kw)


def test_market_value_fallback_paths():
    pf = _pf()
    # 已跟踪股数：qty × last_close
    assert pf._market_value({"qty": 10.0, "last_close": 5.0}) == 50.0
    # 未跟踪股数但有买/卖价：成本 × 涨跌幅
    assert pf._market_value({"qty": 0.0, "buy_price": 10.0, "cost": 1000.0, "last_close": 11.0}) == 1100.0
    # 完全无价格：退化为成本
    assert pf._market_value({"cost": 500.0}) == 500.0


def test_current_drawdown():
    pf = _pf()
    assert pf._current_drawdown(1_000_000.0) == 0.0  # 空曲线
    pf.equity_curve = [
        {"date": "2026-01-01", "total": 1_000_000.0},
        {"date": "2026-01-02", "total": 900_000.0},
    ]
    # 峰值 1e6，当前 9e5 → 回撤 10%
    assert abs(pf._current_drawdown(900_000.0) - 0.10) < 1e-9


def test_effective_cash_frac_drawdown_buffer():
    pf = _pf(cash_frac=0.0, dd_full=0.20, dd_cash_ceiling=0.50)
    assert pf._effective_cash_frac() == 0.0  # 无曲线 → 仅静态
    # 曲线自峰值回撤 10% → 缓冲 = min(0.10/0.20,1)*0.50 = 0.25
    pf.equity_curve = [
        {"date": "2026-01-01", "total": 1_000_000.0},
        {"date": "2026-01-02", "total": 900_000.0},
    ]
    assert abs(pf._effective_cash_frac() - 0.25) < 1e-9


def test_rebalance_single_name_trim():
    pf = _pf(max_single_weight=0.10)
    pf.cash = 0.0
    # 三仓成本 1000/100/100，最后交易日无价 → 市值=成本；A 权重 83% 超限
    pf.positions = {
        "A": {"cost": 1000.0},
        "B": {"cost": 100.0},
        "C": {"cost": 100.0},
    }
    actions = pf._rebalance(__import__("datetime").date(2026, 1, 5))
    assert actions["single_trim"] == 1
    # A 被削到 10%×1200 = 120；差额 880 回现金
    assert abs(pf.positions["A"]["cost"] - 120.0) < 1e-6
    assert abs(pf.cash - 880.0) < 1e-6
    # B/C 不动
    assert pf.positions["B"]["cost"] == 100.0


def test_rebalance_beta_close(monkeypatch):
    pf = _pf(portfolio_beta_ceiling=1.4, beta_min_keep=1, max_single_weight=0.10)
    pf.cash = 0.0
    # 11 只等权（每只 ~9.09% ≤10% → 单名不触发），其中 A~H(8只) 高β、I~K(3只) 低β。
    codes = [chr(ord("A") + i) for i in range(11)]
    pf.positions = {c: {"cost": 100.0} for c in codes}

    def fake_betas(codes_arg, as_of):
        # 按代码身份固定 β（删除后索引会漂移，必须用身份而非下标）
        return {c: (2.0 if c <= "H" else 0.5) for c in codes_arg}

    monkeypatch.setattr("smcore.strategy.position_sizing._estimate_betas", fake_betas)
    actions = pf._rebalance(__import__("datetime").date(2026, 1, 5))
    # 组合 β≈1.59 >1.4 → 清高β仓。每清一只其市值回现金、稀释剩余权重（防守性降仓），
    # 故只需清 2 只即把 β 压到 ≤1.4（剩 6 高 + 3 低）。
    assert actions["beta_close"] == 2
    assert len(pf.positions) == 9
    remaining_high = [c for c in pf.positions if c <= "H"]
    assert len(remaining_high) == 6
    # 单名未触发（每只本就 ≤10%）
    assert actions["single_trim"] == 0


def test_process_day_records_drawdown_without_network(monkeypatch):
    pf = _pf(max_single_weight=0.10, portfolio_beta_ceiling=1.4, beta_min_keep=1)
    # 屏蔽 simulate_position（避免读 k_data）：返回 horizon → 永不触发出场
    monkeypatch.setattr(
        pm, "simulate_position",
        lambda *a, **k: {"return_pct": 0.0, "exit_reason": "horizon", "sell_date": None,
                          "buy_price": None, "sell_price": None},
    )
    monkeypatch.setattr(
        "smcore.strategy.position_sizing._estimate_betas",
        lambda codes, as_of: {c: 1.0 for c in codes},
    )
    # 注入一个已盯市的持仓（无股价变动 → 市值=成本）
    pf.positions = {"X": {"buy_date": "2026-01-01", "qty": 0.0, "buy_price": None,
                          "cost": 200_000.0, "last_close": None, "stop_pct": None,
                          "take_price": None, "strategy": ""}}
    pf.cash = 800_000.0  # 净值 1e6
    pf.equity_curve = [{"date": "2026-01-01", "total": 1_000_000.0}]
    out = pf.process_day(__import__("datetime").date(2026, 1, 2))
    assert "drawdown_pct" in out
    assert abs(out["drawdown_pct"] - 0.0) < 1e-9  # 净值持平 → 回撤 0
    assert out["total"] == 1_000_000.0
    assert len(pf.equity_curve) == 2
