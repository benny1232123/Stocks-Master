"""全项目默认参数与路径 —— 单一真相源。

此前参数散落三处且存在分歧，本模块统一：
- 股价上限：命令行 30 / visualizer 35 → 统一 30（主流程口径，更保守）
- 复权方式：命令行不复权 / visualizer 前复权 → 统一前复权（不复权会导致布林带断裂）
- 财报期：<5月 命令行用年报 / data_fetcher 用三季报 → 统一三季报（年报披露中不齐全）
"""
from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STOCK_DATA_DIR = PROJECT_ROOT / "stock_data"
PLOT_DIR = STOCK_DATA_DIR / "plots"
CACHE_DIR = STOCK_DATA_DIR / "cache"
CSV_ENCODING = "utf-8-sig"

# ── Boll 指标 ──
DEFAULT_WINDOW = 20
DEFAULT_K = 1.645            # 90% 概率区间
DEFAULT_NEAR_RATIO = 1.015   # 收盘价 <= 下轨 × near_ratio 视为"接近下轨"
DEFAULT_UPPER_NEAR_RATIO = 0.985

# 复权方式：全项目统一前复权(qfq)。
# Boll 选股曾用不复权(adjustflag=3)，除权除息日布林带断裂、
# 信号失真——这是"结果不可信"的头号原因。现已统一为前复权。
DEFAULT_ADJUST = "qfq"
ADJUST_FLAG_MAP = {"hfq": "1", "qfq": "2", "bfq": "3"}

DEFAULT_DAYS_BACK = 180

# ── 基本面过滤 ──
DEFAULT_PRICE_UPPER_LIMIT = 30.0
DEFAULT_PRICE_LOWER_LIMIT = 5.0
DEFAULT_DEBT_ASSET_RATIO_LIMIT = 70.0
DEFAULT_EXCLUDE_GEM_SCI = True

# ── 资金流 ──
DEFAULT_FUND_FLOW_PERIODS = ("3日排行", "5日排行", "10日排行")

# ── 重要股东 ──
IMPORTANT_SHAREHOLDERS = (
    "香港中央结算有限公司",
    "中央汇金资产管理有限公司",
    "中央汇金投资有限责任公司",
    "香港中央结算（代理人）有限公司",
    "中国证券金融股份有限公司",
)
IMPORTANT_SHAREHOLDER_TYPES = ("社保基金",)

# ── 风险中性化（组合层约束，避免单一暴露拖垮组合）──
# β 估计窗口（交易日），用本地 k_data 与沪深300 序列对齐计算。
BETA_WINDOW = 60
# 个股 β 缺数据（无本地 k_data）时的回退值：中性 1.0，不阻断清单生成。
BETA_FALLBACK = 1.0

# 以下四个「上限/地板」不再是写死调参常量——其活跃值由 smcore.strategy.risk_rules
# 按名单广度 + regime + 波动率实时计算（见 risk_config.json）。这里仅保留它们作为
# **绝对安全天花板/地板**（取自 risk_config 的可热更新取值），防止自适应公式在极端数据下
# 越界（如单票满仓）。它们是结构性安全约束，非自选「手工规则」。A股一手=100股(交易所规则)
# 由 fusion/position_sizing 的 LOT_SIZE 另行强制，不在本层。
try:
    from smcore.strategy.risk_rules import RISK_CONFIG

    MAX_SECTOR_WEIGHT_PCT = RISK_CONFIG["sector_weight"]["ceil_pct"]
    PORTFOLIO_BETA_CEILING = RISK_CONFIG["portfolio_beta"]["max"]
    BETA_MIN_KEEP = int(RISK_CONFIG["beta_min_keep"]["min"])
    MAX_SINGLE_WEIGHT_PCT = RISK_CONFIG["single_weight"]["ceil_pct"]
except Exception:  # 兜底，保证缺 risk_config 也能跑
    MAX_SECTOR_WEIGHT_PCT = 35.0
    PORTFOLIO_BETA_CEILING = 1.8
    BETA_MIN_KEEP = 5
    MAX_SINGLE_WEIGHT_PCT = 15.0
