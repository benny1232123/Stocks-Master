"""relativity 策略核心逻辑回归测试。

重点锁定 Round 17 的语义修正：
- 上涨满足率(up_ok)由「绝对收益 ≥ up_tol」改为「相对收益(个股-指数) ≥ up_tol」，
  与抗跌侧(down_outperf)对称，真正刻画指数上涨日个股是否跟上/跑赢。
- 旧的绝对口径(up_tol=-0.025)会让「个股在指数上涨日仅+1%但落后指数2%」误判为满足，
  本测试显式断言该情形现在判失败。

注意：relative_strength_pass 内部通过 _to_daily_ret 对 close 做 pct_change 得到日收益，
因此测试构造的是原始收盘价序列（首行作为 pct_change 锚点被丢弃）。
"""
import numpy as np
import pandas as pd

from smcore.strategies.relativity import relative_strength_pass


def _make_frames(stock_rets, index_rets, start="2024-01-02"):
    # 由日收益还原收盘价序列（含一个前导锚点，使 pct_change 后恰好得到目标收益序列）。
    def closes(rets):
        out = [100.0]
        for r in rets:
            out.append(out[-1] * (1.0 + r))
        return out

    dates = pd.bdate_range(start, periods=len(stock_rets) + 1)
    s = pd.DataFrame({"date": dates, "close": closes(stock_rets)})
    i = pd.DataFrame({"date": dates, "close": closes(index_rets)})
    return s, i


def test_relative_up_tol_rejects_stock_lagging_index_on_up_days():
    # 指数上涨日：个股 +1%，指数 +3% → 相对落后 -2% < -0.005 容差，不应计入满足。
    stock = [0.01] * 5 + [0.005] * 5
    index = [0.03] * 5 + [-0.01] * 5
    s, i = _make_frames(stock, index)
    passed, stats = relative_strength_pass(
        s, i,
        min_overlap_days=5, up_tol=-0.005, down_outperf=0.0,
        min_up_ratio=0.6, min_down_ratio=0.7,
        min_up_days=1, min_down_days=1,
    )
    # 旧绝对口径(up_tol=-0.025)会把 5 个 +1% 上涨日全部计入→up_ratio=1.0→误判通过；
    # 新相对口径下 5 个上涨日全部因落后指数被剔除→up_ratio=0.0→判失败。
    assert stats["up_ratio"] == 0.0
    assert passed is False


def test_relative_up_tol_keeps_stock_beating_index_on_up_days():
    # 指数上涨日：个股 +4%，指数 +3% → 相对 +1% ≥ -0.005，计入满足；
    # 下跌日个股 -0.5% 优于指数 -1% → 抗跌满足。
    stock = [0.04] * 5 + [-0.005] * 5
    index = [0.03] * 5 + [-0.01] * 5
    s, i = _make_frames(stock, index)
    passed, stats = relative_strength_pass(
        s, i,
        min_overlap_days=5, up_tol=-0.005, down_outperf=0.0,
        min_up_ratio=0.6, min_down_ratio=0.7,
        min_up_days=1, min_down_days=1,
    )
    assert stats["up_ratio"] == 1.0
    assert stats["down_ratio"] == 1.0
    assert passed is True
