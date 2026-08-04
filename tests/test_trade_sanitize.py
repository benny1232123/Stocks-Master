"""离线单元测试：backend.trade_sanitize._is_corrupt_trade。

该函数为纯标准库实现，不依赖 FastAPI / smcore / backtrader，因此可直接
import 测试，无需启动后端或安装完整依赖链。覆盖两类历史脏数据：
  ① 同日买卖（违反 A 股 T+1）；
  ② 标记为「止盈」(take_band / take_pct) 却实际亏损。
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.trade_sanitize import _is_corrupt_trade


def _rec(**kw):
    base = {
        "buy_date": "",
        "sell_date": "",
        "exit_reason": "",
        "return_pct": 0,
    }
    base.update(kw)
    return base


def test_same_day_buy_sell_is_corrupt():
    """当日买当日卖违反 T+1，应判为脏数据。"""
    assert _is_corrupt_trade(_rec(buy_date="20260801", sell_date="20260801")) is True


def test_different_day_is_clean():
    """正常隔日卖出不应被误判。"""
    assert _is_corrupt_trade(_rec(buy_date="20260801", sell_date="20260808")) is False


def test_missing_sell_date_is_clean():
    """仅有买入、尚无卖出记录的持仓不算脏数据。"""
    assert _is_corrupt_trade(_rec(buy_date="20260801", sell_date="")) is False


def test_take_band_with_loss_is_corrupt():
    """标记为布林上轨止盈却亏损，属旧引擎矛盾，判为脏数据。"""
    assert _is_corrupt_trade(_rec(exit_reason="take_band", return_pct=-3.5)) is True


def test_take_pct_with_loss_is_corrupt():
    """标记为百分比止盈却亏损，同样判为脏数据。"""
    assert _is_corrupt_trade(_rec(exit_reason="take_pct", return_pct=-1.2)) is True


def test_take_band_with_profit_is_clean():
    """标记止盈且实际盈利，属正常退出，不应误判。"""
    assert _is_corrupt_trade(_rec(exit_reason="take_band", return_pct=5.0)) is False


def test_stop_loss_with_loss_is_clean():
    """止损亏损是预期行为，不属于「止盈却亏」矛盾，不应判脏。"""
    assert _is_corrupt_trade(_rec(exit_reason="stop_loss", return_pct=-8.0)) is False


def test_malformed_return_pct_is_clean():
    """收益率为非数值时安全回退为 0，不应误判脏（除非叠加其他条件）。"""
    assert _is_corrupt_trade(_rec(exit_reason="take_band", return_pct="N/A")) is False


def test_empty_record_is_clean():
    """完全空记录（旧数据缺字段）应安全返回 False，不抛异常。"""
    assert _is_corrupt_trade({}) is False
