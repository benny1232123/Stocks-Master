"""统计配对套利（协整 + 残差均值回复）。

纯数据驱动、配置可热更、零硬编码权重。复用 smcore.strategy.factor_engine 的 qfq 日线宽表。
默认长-短价差策略（标准 pairs trading）；同时产出 A 股可行的「仅多头」变体（价差便宜时只做多价差，
贵时不动——因 A 股个股难做空）。
"""
from .pairs import (
    MAIN_BOARD_PREFIXES,
    is_main_board,
    load_closes,
    load_amounts,
    load_sector_map,
    liquid_universe,
    candidate_pairs,
    cointegration_filter,
    backtest_pair,
    backtest_portfolio,
    run_pipeline,
    run_etf_pipeline,
)

__all__ = [
    "MAIN_BOARD_PREFIXES",
    "is_main_board",
    "load_closes",
    "load_amounts",
    "load_sector_map",
    "liquid_universe",
    "candidate_pairs",
    "cointegration_filter",
    "backtest_pair",
    "backtest_portfolio",
    "run_pipeline",
    "run_etf_pipeline",
]
