"""回测交易成本（佣金万2.5 最低5元 + 卖出印花税千0.5）纯函数测试。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from smcore.backtest.engine import _buy_cost, _sell_cost

_COMM_RATE = 0.00025
_COMM_MIN = 5.0
_STAMP_RATE = 0.0005


def test_buy_cost_uses_minimum_when_small():
    assert _buy_cost(1000) == _COMM_MIN


def test_buy_cost_scales_with_amount():
    assert _buy_cost(100000) == 100000 * _COMM_RATE


def test_sell_cost_adds_stamp_duty():
    # 小额：佣金封底 5 + 印花税
    assert _sell_cost(1000) == _COMM_MIN + 1000 * _STAMP_RATE
    # 大额：佣金按比例 + 印花税
    assert _sell_cost(100000) == 100000 * _COMM_RATE + 100000 * _STAMP_RATE
