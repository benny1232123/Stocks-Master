"""Boll 布林触发信号重构测试（中轨回踩 + 带宽收缩）。

验证 evaluate_boll_signal 的四类触发分支与连续触发抑制，
以及 risk_config.json 的 boll 段结构正确（run_boll._load_boll_config 直接读该文件）。

注意：不 import smcore.strategies.boll（其顶层 import baostock，可能未安装），
配置读取改为直接读 risk_config.json 文件验证，与 _load_boll_config 读同一源。
"""
import json
from pathlib import Path

import pandas as pd
import pytest

from smcore.indicators.boll import evaluate_boll_signal


def _df(close_last, lower, upper, middle, n=25, last_band=None):
    """构造布林 df：前 n-1 根高位中性(close=middle*1.05)，最后一根按参数设定。"""
    closes = [middle * 1.05] * (n - 1) + [close_last]
    df = pd.DataFrame({"close": closes})
    ups = [upper] * (n - 1)
    lows = [lower] * (n - 1)
    if last_band is not None:
        ups.append(last_band[0])
        lows.append(last_band[1])
    else:
        ups.append(upper)
        lows.append(lower)
    df["Upper"] = ups
    df["Lower"] = lows
    df["MA"] = [middle] * n
    return df


def _df_oversold_tail(tail, middle=100, lower=90, upper=110, n=25):
    closes = [middle * 1.05] * (n - tail) + [lower * 0.9] * tail
    df = pd.DataFrame({"close": closes})
    df["Upper"] = [upper] * n
    df["Lower"] = [lower] * n
    df["MA"] = [middle] * n
    return df


def test_oversold_selected():
    df = _df(close_last=85, lower=90, upper=110, middle=100)
    sig = evaluate_boll_signal(df)
    assert sig["selected"] is True
    assert sig["signal_type"] == "oversold"


def test_near_lower_selected():
    df = _df(close_last=90.9, lower=90, upper=110, middle=100)  # 90 < 90.9 <= 91.35
    sig = evaluate_boll_signal(df)
    assert sig["selected"] is True
    assert sig["signal_type"] == "near_lower"


def test_mid_pullback_selected():
    # close 贴近中轨(100.5, dist 0.5%) 且远离下轨(>91.35) → 中轨回踩触发
    df = _df(close_last=100.5, lower=90, upper=110, middle=100)
    sig = evaluate_boll_signal(df)
    assert sig["selected"] is True
    assert sig["signal_type"] == "mid_pullback"
    assert sig["dist_to_mid_pct"] is not None and abs(sig["dist_to_mid_pct"]) < 1.0


def test_squeeze_selected():
    # 末根带宽收窄(0.09) < 前段带宽(0.2)的20%分位 → 带宽收缩触发；close 不触其他分支
    df = _df(close_last=103, lower=90, upper=110, middle=100, last_band=(108, 99))
    sig = evaluate_boll_signal(df)
    assert sig["selected"] is True
    assert sig["signal_type"] == "squeeze"
    assert sig["is_squeeze"] is True


def test_continuous_oversold_suppressed():
    # 连续4日超卖，cap=3 → 第4日不重复触发
    df = _df_oversold_tail(tail=4)
    sig = evaluate_boll_signal(df, continuous_streak_cap=3)
    assert sig["selected"] is False
    assert sig["signal_type"] == "oversold_continuous"
    assert sig["streak"] == 4


def test_continuous_oversold_within_cap_selected():
    # 连续3日超卖，cap=3 → 3 不大于 3 → 仍触发
    df = _df_oversold_tail(tail=3)
    sig = evaluate_boll_signal(df, continuous_streak_cap=3)
    assert sig["selected"] is True
    assert sig["signal_type"] == "oversold"


def test_squeeze_disabled_falls_back():
    # 关闭带宽收缩 → 该分支不再触发（即便带宽已收窄）
    df = _df(close_last=103, lower=90, upper=110, middle=100, last_band=(108, 99))
    sig = evaluate_boll_signal(df, squeeze_enabled=False)
    assert sig["signal_type"] in ("neutral", "overbought", "near_upper")


def test_boll_config_segment():
    p = Path(__file__).resolve().parents[1] / "smcore" / "strategy" / "risk_config.json"
    user = json.load(open(p, encoding="utf-8"))
    assert "boll" in user
    b = user["boll"]
    assert b["near_ratio"] == 1.015
    assert b["mid_pullback_pct"] == 0.02
    assert b["squeeze_enabled"] is True
    assert b["squeeze_window"] == 20
    assert b["squeeze_pctile"] == 0.20
    assert b["continuous_streak_cap"] == 3
