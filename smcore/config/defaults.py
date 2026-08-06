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

# ── Boll 布林触发信号（重构：优质股回踩买点，破解"优质股∩超卖"内在冲突）──
# 原逻辑仅触发"超卖(close<下轨)/近下轨(close<=下轨×near_ratio)"，与前置"资金流好+基本面好+
# 国家队重仓"筛选天然冲突（优质股极少同时超卖），导致候选长期 0~4 只/天。
# 重构后新增两类与优质股特征自洽的触发：
#   ① 中轨回踩：|close-MA20|/MA20 < mid_pullback_pct（优质股常态回调买点）
#   ② 带宽收缩(volatility squeeze)：bandwidth < 近 squeeze_window 日 squeeze_pctile 分位
# 并保留超卖/近下轨作为极端兜底；连续触发抑制阈值放宽到可配置 continuous_streak_cap。
# 全部参数走 risk_config.json 的 boll 段（run_boll 直接读文件，避免循环导入），此处为 fallback。
DEFAULT_BOLL_MID_PULLBACK_PCT = 0.02     # 中轨回踩容差 |close-MA|/MA < 2%
DEFAULT_BOLL_SQUEEZE_ENABLED = True      # 启用带宽收缩触发
DEFAULT_BOLL_SQUEEZE_WINDOW = 20         # 带宽分位回看窗口（交易日）
DEFAULT_BOLL_SQUEEZE_PCTILE = 0.20       # bandwidth < 近窗口 20% 分位视为收口
DEFAULT_BOLL_CONTINUOUS_STREAK_CAP = 3   # 同一信号连续触发超过该天数则本日不重复选

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
#
# 注意：此处**直接读 risk_config.json**，不 import smcore.strategy.risk_rules —— 否则会在
# smcore.strategy 包初始化（fusion→position_sizing→defaults）期间形成循环导入，导致
# `import smcore.strategy.risk_rules` 在已导入 engine 后失败。两者读同一文件、取值一致。
def _load_risk_ceilings():
    import json

    _cfg_path = Path(__file__).resolve().parents[1] / "strategy" / "risk_config.json"
    _builtin = {
        "single_weight": {"ceil_pct": 15.0},
        "sector_weight": {"ceil_pct": 35.0},
        "portfolio_beta": {"max": 1.8},
        "beta_min_keep": {"min": 5},
    }
    try:
        with open(_cfg_path, "r", encoding="utf-8") as f:
            user = json.load(f)

        def _get(*keys, default):
            cur = user
            for k in keys:
                if not isinstance(cur, dict) or k not in cur:
                    return default
                cur = cur[k]
            return cur

        return {
            "MAX_SECTOR_WEIGHT_PCT": _get("sector_weight", "ceil_pct", default=35.0),
            "PORTFOLIO_BETA_CEILING": _get("portfolio_beta", "max", default=1.8),
            "BETA_MIN_KEEP": int(_get("beta_min_keep", "min", default=5)),
            "MAX_SINGLE_WEIGHT_PCT": _get("single_weight", "ceil_pct", default=15.0),
        }
    except Exception:
        return {
            "MAX_SECTOR_WEIGHT_PCT": 35.0,
            "PORTFOLIO_BETA_CEILING": 1.8,
            "BETA_MIN_KEEP": 5,
            "MAX_SINGLE_WEIGHT_PCT": 15.0,
        }


_CEILS = _load_risk_ceilings()
MAX_SECTOR_WEIGHT_PCT = _CEILS["MAX_SECTOR_WEIGHT_PCT"]
PORTFOLIO_BETA_CEILING = _CEILS["PORTFOLIO_BETA_CEILING"]
BETA_MIN_KEEP = _CEILS["BETA_MIN_KEEP"]
MAX_SINGLE_WEIGHT_PCT = _CEILS["MAX_SINGLE_WEIGHT_PCT"]
