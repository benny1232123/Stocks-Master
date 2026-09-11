"""配置驱动 + 可观测性回归测试（2026-09-09）。

1. **阈值配置化**：``risk_config.json`` 的 regime_filter / fusion / cctv 段能真正改变
   行为（不再是代码里的字面量），且默认值与迁移前完全一致。
2. **K 线读取失败可观测**：坏 parquet 不再被静默吞掉，会计数并告警。
3. **HS300 缓存按天失效**：长驻进程不会一直用启动当天的序列。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import smcore.data.kline as kl  # noqa: E402
import smcore.strategy.regime_filter as rf  # noqa: E402
from smcore.strategy import risk_rules  # noqa: E402


# ── 1. 阈值配置化 ───────────────────────────────────────────────────────


def test_regime_filter_defaults_are_config_driven():
    """模块级常量来自配置，且默认值与迁移前硬编码一致。"""
    assert rf.RS_TOL == 0.03
    assert rf.RS_LOOKBACK == 20
    assert rf.MIN_SIGNAL_AMOUNT == 1e8
    assert rf.TREND_GUARD_BELOW_MA20 == 0.12
    # 中性 profile 下动态阈值应等于基准值
    tol, amt = rf._dynamic_thresholds(None)
    assert abs(tol - 0.03) < 1e-9
    assert abs(amt - 1e8) < 1e-9


def test_regime_filter_reads_config(monkeypatch):
    """改配置的 rs_tol_base 后，_rf_cfg() 立刻反映（热更新路径）。"""
    cfg = rf._rf_cfg()
    assert cfg["rs_tol_base"] == 0.03

    patched = dict(cfg)
    patched["rs_tol_base"] = 0.05
    monkeypatch.setattr(risk_rules, "RISK_CONFIG", {"regime_filter": patched})
    assert rf._rf_cfg()["rs_tol_base"] == 0.05

    tol, _ = rf._dynamic_thresholds(None)
    assert abs(tol - 0.05) < 1e-9, "动态阈值未跟随配置"

    # 超过上限时应被 clamp（防止配置写错导致阈值失控）
    patched["rs_tol_base"] = 0.5
    monkeypatch.setattr(risk_rules, "RISK_CONFIG", {"regime_filter": patched})
    tol, _ = rf._dynamic_thresholds(None)
    assert abs(tol - cfg["rs_tol_max"]) < 1e-9, "阈值未被上限 clamp"


def test_multi_hit_bonus_cap_configurable(monkeypatch):
    cfg = rf._rf_cfg()
    patched = dict(cfg)
    patched["multi_hit_bonus_cap"] = 20
    monkeypatch.setattr(risk_rules, "RISK_CONFIG", {"regime_filter": patched})
    assert rf._adaptive_multi_hit_bonus(2) == 10  # 20/2


def test_risk_config_json_has_new_sections():
    """risk_config.json 必须含本次迁入的配置段（缺失则回退内置默认，等于没配置化）。"""
    p = Path(__file__).resolve().parents[1] / "smcore" / "strategy" / "risk_config.json"
    cfg = json.loads(p.read_text(encoding="utf-8"))
    assert "regime_filter" in cfg
    assert "fusion" in cfg
    assert cfg["cctv"]["confidence_mention_weight"] == 0.5
    assert cfg["cctv"]["confidence_sentiment_weight"] == 0.3
    assert cfg["fusion"]["theme_score_weight"] == 0.1


def test_adaptive_weights_config_has_edge_section():
    p = (
        Path(__file__).resolve().parents[1]
        / "smcore"
        / "strategy"
        / "adaptive_weights_config.json"
    )
    cfg = json.loads(p.read_text(encoding="utf-8"))
    assert cfg["edge"]["source"] in {"universe", "backtest", "blend"}
    assert cfg["edge"]["min_n_confident"] > 0


# ── 2. K 线读取失败可观测 ───────────────────────────────────────────────


def test_list_kline_codes_records_failure(tmp_path, monkeypatch, capsys):
    """坏 parquet 必须计数 + 告警，不能静默跳过（否则股票池悄悄缩水）。"""
    kl.clear_read_failures()
    good = pd.DataFrame({"code": ["600001", "600002"]})
    good.to_parquet(tmp_path / "qfq_b60.parquet", index=False)

    bad = tmp_path / "qfq_b30.parquet"
    bad.write_bytes(b"\x00\x01\x02not-a-parquet")

    codes = kl.list_kline_codes(base_dir=tmp_path)
    err = capsys.readouterr().err

    assert codes == ["600001", "600002"], "好文件应正常读出"
    assert "WARN" in err, "坏文件未告警"
    assert any("qfq_b30.parquet" in k for k in kl.get_read_failures()), "坏文件未计数"
    kl.clear_read_failures()


def test_read_kline_cache_records_failure(tmp_path, monkeypatch, capsys):
    """单票读取失败同样要留痕。"""
    kl.clear_read_failures()

    def _boom(*a, **k):
        raise RuntimeError("corrupt")

    monkeypatch.setattr(pd, "read_parquet", _boom)
    monkeypatch.setattr(kl, "_bucket_files", lambda *a, **k: [tmp_path / "qfq_b60.parquet"])

    out = kl.read_kline_cache("600519", base_dir=tmp_path)
    assert out.empty
    assert "WARN" in capsys.readouterr().err
    assert len(kl.get_read_failures()) == 1
    kl.clear_read_failures()


# ── 3. HS300 缓存按天失效 ───────────────────────────────────────────────


def test_hs300_cache_expires_daily(monkeypatch):
    """跨天后缓存必须重拉，不能一直用进程启动当天的序列。"""
    rf.invalidate_hs300_cache()
    calls = []

    idx = pd.date_range("2026-01-01", periods=60, freq="B")

    def _fake_fetch():
        calls.append(1)
        return pd.Series([100.0 + i for i in range(60)], index=idx)

    monkeypatch.setattr(rf, "_fetch_hs300_baostock", _fake_fetch)

    class _Date:
        today_iso = ["2026-09-09"]

        @staticmethod
        def today():
            class _D:
                @staticmethod
                def isoformat():
                    return _Date.today_iso[0]

            return _D()

    monkeypatch.setattr(rf, "date", _Date)

    rf._get_hs300_close()
    rf._get_hs300_close()  # 同一天 → 命中缓存
    assert len(calls) == 1, "同一天应命中缓存"

    _Date.today_iso[0] = "2026-09-10"
    rf._get_hs300_close()  # 跨天 → 应重拉
    assert len(calls) == 2, "跨天后缓存未失效"
    rf.invalidate_hs300_cache()


def test_hs300_keeps_stale_cache_on_fetch_failure(monkeypatch):
    """重拉失败时保留旧缓存——比"无基准导致 RS 过滤整体失效"更安全。"""
    rf.invalidate_hs300_cache()
    idx = pd.date_range("2026-01-01", periods=60, freq="B")
    monkeypatch.setattr(
        rf, "_fetch_hs300_baostock", lambda: pd.Series([1.0] * 60, index=idx)
    )
    first = rf._get_hs300_close()
    assert first is not None

    monkeypatch.setattr(rf, "_fetch_hs300_baostock", lambda: None)
    monkeypatch.setattr(rf, "_fetch_hs300_akshare", lambda: None)

    class _Date:
        @staticmethod
        def today():
            class _D:
                @staticmethod
                def isoformat():
                    return "2027-01-01"

            return _D()

    monkeypatch.setattr(rf, "date", _Date)
    assert rf._get_hs300_close() is not None, "重拉失败时应保留旧缓存"
    rf.invalidate_hs300_cache()
