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
# 行业权重上限：单行业入选总仓位占组合比例上限（%），与 sectors.py 的数量上限互补。
MAX_SECTOR_WEIGHT_PCT = 20.0
# 组合 β 软上限：组合对沪深300 的加权 β 超过该值则逐步剔除高 β 个股（贴近基准、控回撤）。
PORTFOLIO_BETA_CEILING = 1.4
# β 估计窗口（交易日），用本地 k_data 与沪深300 序列对齐计算。
BETA_WINDOW = 60
# 个股 β 缺数据（无本地 k_data）时的回退值：中性 1.0，不阻断清单生成。
BETA_FALLBACK = 1.0
# 剔除高 β 个股时，清单至少保留的只数（避免为压 β 把清单砍光）。
BETA_MIN_KEEP = 8
# 单名仓位上限（%）：任一单只股票的建议仓位不得超过该值。
# 这是组合层最后一道集中度闸——行业权重上限只约束「板块」、β 上限只约束「市场暴露」，
# 二者都管不到「单只票吃光仓位」的情形（如某策略权重 40% 且只活下来 1 只，就会把 40% 押在一只票上）。
# 10% 上限保证极端情况下也至少有 ~10 只票平分组合，直接削掉单名黑天鹅尾部。
MAX_SINGLE_WEIGHT_PCT = 10.0
