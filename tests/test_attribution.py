"""P1-2 Brinson 绩效归因单元测试。"""
from __future__ import annotations

from smcore.strategy.attribution import (
    brinson_bhb,
    format_attribution,
    forward_returns,
    run_attribution,
)


def test_brinson_bhb_math():
    # 教科书 2 资产例子
    port_w = {"A": 0.6, "B": 0.4}
    port_ret = {"A": 0.10, "B": 0.05}
    bench_w = {"A": 0.5, "B": 0.5}
    bench_ret = {"A": 0.08, "B": 0.04}
    r = brinson_bhb(port_w, port_ret, bench_w, bench_ret)
    t = r["total"]
    assert abs(t["port_return"] - 0.08) < 1e-9
    assert abs(t["bench_return"] - 0.06) < 1e-9
    assert abs(t["active"] - 0.02) < 1e-9
    assert abs(t["allocation"] - 0.004) < 1e-9
    assert abs(t["selection"] - 0.015) < 1e-9
    assert abs(t["interaction"] - 0.001) < 1e-9
    assert abs((t["allocation"] + t["selection"] + t["interaction"]) - t["active"]) < 1e-9


def test_brinson_skips_missing_returns():
    port_w = {"A": 0.6, "B": 0.4}
    port_ret = {"A": 0.10, "B": None}  # B 缺收益 → 跳过
    bench_w = {"A": 0.5, "B": 0.5}
    bench_ret = {"A": 0.08, "B": 0.04}
    r = brinson_bhb(port_w, port_ret, bench_w, bench_ret)
    assert "B" not in r["by_code"]
    assert "A" in r["by_code"]
    # 仅 A 计入：active = 0.6*0.10 - 0.5*0.08 = 0.06 - 0.04 = 0.02
    assert abs(r["total"]["active"] - 0.02) < 1e-9


def test_forward_returns_shape_and_missing():
    # 用已提交的 k_data 代码(600519 茅台在 d839ea4 提交名单内)
    out = forward_returns(["600519", "999999"], "20260719", horizon=10)
    assert set(out.keys()) == {"600519", "999999"}
    # 999999 不存在 → None（fail-soft）
    assert out["999999"] is None
    # 600519 应算出数值或在数据不足时为 None，二者皆可，但不能抛错
    assert out["600519"] is None or isinstance(out["600519"], float)


def test_run_attribution_on_refused_date():
    # 0719 已重融合且有 k_data → 应返回归因结果字典（部分股票收益缺失时 by_code 可能不全）
    res = run_attribution("20260719", horizon=10, benchmark="equal")
    assert res is not None
    assert "total" in res
    assert "active" in res["total"]


def test_format_attribution_has_headline():
    port_w = {"A": 0.6, "B": 0.4}
    port_ret = {"A": 0.10, "B": 0.05}
    bench_w = {"A": 0.5, "B": 0.5}
    bench_ret = {"A": 0.08, "B": 0.04}
    r = brinson_bhb(port_w, port_ret, bench_w, bench_ret)
    txt = format_attribution(r, "20260719", 10, "equal")
    assert "主动收益" in txt
    assert "配置效应" in txt
    assert "选股效应" in txt
