"""交易记录清洗：剔除旧引擎历史脏数据。

本模块刻意保持零第三方依赖（仅标准库），以便：
  * 被后端入口 ``backend.main`` 直接引入；
  * 被离线单元测试直接 import —— 不触发 FastAPI / smcore / backtrader
    等重量级依赖链，从而可以在无网络、无完整依赖的沙箱里跑测试。

设计上刻意与 ``smcore.strategy.fusion._apply_position_sizing`` 等其它
风险层函数保持一致：把「可被独立验证的风险控制逻辑」从巨型入口模块里
抽出来，单独成文件、单独可测。
"""

from __future__ import annotations


def _is_corrupt_trade(rec: dict) -> bool:
    """剔除旧引擎历史脏数据：① 同日买卖（违反 A股 T+1）；② 止盈类退出却亏损。"""
    bd = str(rec.get("buy_date", "") or "").strip()
    sd = str(rec.get("sell_date", "") or "").strip()
    if bd and sd and bd == sd:
        return True  # 当日买当日卖：T+1 下不可能，旧引擎退出分支无最少持有保护所致
    er = str(rec.get("exit_reason", "") or "").strip()
    try:
        rp = float(rec.get("return_pct", 0) or 0)
    except (TypeError, ValueError):
        rp = 0.0
    if er in ("take_band", "take_pct") and rp < 0:
        return True  # 标记「止盈」却亏损：旧引擎把布林上轨止盈套到动量入场（上轨<成本）的矛盾
    return False
