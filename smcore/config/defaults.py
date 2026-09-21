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


# ── 持仓建议三维度综合打分（技术面 + 基本面 + 资金面）──
# 设计原则（符合"权重/阈值集中配置、禁散落魔数"）：
#  - 与前端 ComprehensivePanel 完全同构（2026-09-01 对齐）：三面均为 0-100 分制，
#    综合分 = 技术0.40 + 基本面0.35 + 资金0.25（有基本面时；无则仅技术面）。
#  - 技术面：五组信号累加 techS（RSI/MACD/KDJ/均线/布林），techScore = clamp(50+techS*6, 0, 100)。
#  - 基本面：PE/PB/ROE/毛利率/营收增长 5 因子分段打分后取平均。
#  - 资金面：20日成交额日均(亿) + 换手率(%) 2 因子分段打分后取平均。
#  - 档位：tech 分档 good≥70/bad≤30；fund/cap good≥65/bad<45；rating 五档；action 由 rating 映射。
#  - enable_* 可单独关闭某一面。
#  - **缺失因子口径（2026-09-15 修正）**：某因子取不到值 → **不计入该面均值**，而非塞一个
#    missing(50) 进分母。旧实现把缺失当 50 平均，等于把「未知」当成「中等」，
#    会把基本面分系统性拉向 50（实测把 002284 的 75.5 压到 70、600269 的 74.0 压到 69）。
#    只有「该面一个因子都取不到」时，整面才回落到 missing（此时面分本就是未知，不动综合分权重）。
#    ⚠️ 前端 App.jsx 必须同构，改后跑 scripts/sync_scoring_config.py。
#  - **MACD 零轴口径（2026-09-15 修正）**：旧实现只判 dif>dea 且 hist>0 就给满分，
#    不区分零轴上下 → 603187（dif/dea 双深负 −0.256、仅差 0.0005、hist≈0.001）拿到
#    与强势股真金叉同权的 +2，足以让综合分跨档（techS±2 = techScore±12 = 综合分±4.8）。
#    现按零轴拆档：dif>0 为「水上金叉」满分，dif≤0 为「水下金叉」减半（可靠性低）。
RECOMMENDATION_CONFIG = {
    "enable_technical": True,
    "enable_fundamental": True,
    "enable_capital": True,
    "face_weights": {"technical": 0.40, "fundamental": 0.35, "capital": 0.25},
    # 缺失因子口径：exclude=缺失因子不计入该面均值（默认，见上方口径说明）；
    # neutral=旧行为（把 missing 分塞进分母，等于把「未知」当「中等」，会拉向 50）。
    "missing_factor_policy": "exclude",
    "tech_base": 50,
    "tech_step": 6,
    "technical": {
        "rsi": [
            {"gt": 80, "score": -2, "label": "严重超买"},
            {"gt": 70, "score": -1, "label": "高位"},
            {"lt": 20, "score": 2, "label": "严重超卖"},
            {"lt": 30, "score": 1, "label": "超卖"},
            {"gt": 55, "score": 1, "label": "偏强"},
            {"lt": 45, "score": -1, "label": "偏弱"},
        ],
        "macd_golden_red": 2,   # dif>dea 且 macd_hist>0，且 dif>0（零轴**上方**金叉红柱：趋势转强）
        "macd_golden_red_below": 1,  # 同上前提但 dif<=0（零轴**下方**「水下金叉」：弱势反弹，可靠性低）
        "macd_dead_green": -2,  # dif<dea 且 macd_hist<0（死叉绿柱）
        "kdj_j_over": 100, "kdj_j_over_score": -2,   # J>100 极端超买
        "kdj_j_under": 0, "kdj_j_under_score": 2,    # J<0 极端超卖
        "kdj_k_gt_d": 1, "kdj_k_lt_d": -1,
        "ma_bull": 2, "ma_bear": -2,                 # MA5>MA10>MA20 多头 / 反向空头
        "ma5_gt_ma20": 1, "ma5_lt_ma20": -1,
        "boll_below_lower": 1,                       # 破下轨（超卖）
        "boll_near_lower_dist": 2.0, "boll_near_lower": 1,   # dist_to_lower_pct < 2
        "boll_near_upper_dist": -2.0, "boll_near_upper": -1, # dist_to_upper_pct > -2
    },
    "technical_cls": {"good": 70, "bad": 30},
    "fundamental": {
        "pe": [
            {"lt": 0, "score": 38, "label": "亏损"},
            {"lt": 15, "score": 90, "label": "偏低·有吸引力"},
            {"lt": 25, "score": 76, "label": "中性合理"},
            {"lt": 35, "score": 62, "label": "偏高"},
            {"lt": 50, "score": 46, "label": "高估值"},
            {"score": 32, "label": "高估值"},
        ],
        "pb": [
            {"lt": 1, "score": 90, "label": "破净·低估值"},
            {"lt": 3, "score": 76, "label": "偏低"},
            {"lt": 6, "score": 62, "label": "合理"},
            {"lt": 10, "score": 46, "label": "偏高"},
            {"score": 32, "label": "高PB"},
        ],
        "roe": [
            {"gt": 0.20, "score": 92, "label": "优秀"},
            {"gt": 0.15, "score": 82, "label": "良好"},
            {"gt": 0.10, "score": 66, "label": "一般"},
            {"gt": 0, "score": 50, "label": "偏低"},
            {"score": 28, "label": "亏损"},
        ],
        "gm": [
            {"gt": 0.5, "score": 92, "label": "高毛利"},
            {"gt": 0.4, "score": 82, "label": "较高"},
            {"gt": 0.3, "score": 66, "label": "中等"},
            {"gt": 0.2, "score": 54, "label": "较低"},
            {"score": 42, "label": "低毛利"},
        ],
        "rg": [
            {"gt": 0.3, "score": 92, "label": "高增长"},
            {"gt": 0.2, "score": 82, "label": "稳健增长"},
            {"gt": 0.1, "score": 66, "label": "微增"},
            {"gt": 0, "score": 54, "label": "持平"},
            {"score": 32, "label": "负增长"},
        ],
        "missing": 50,
    },
    "capital": {
        "liq_amt": [  # 20日成交额日均（亿元）
            {"gt": 5, "score": 92, "label": "流动性充裕"},
            {"gt": 2, "score": 76, "label": "流动性较好"},
            {"gt": 1, "score": 62, "label": "流动性中等"},
            {"gt": 0.3, "score": 48, "label": "成交偏清淡"},
            {"score": 32, "label": "成交清淡"},
        ],
        "turnover": [  # 换手率（%）
            {"gt": 5, "score": 90, "label": "高度活跃"},
            {"gt": 2, "score": 78, "label": "活跃"},
            {"gt": 1, "score": 64, "label": "一般"},
            {"gt": 0.3, "score": 52, "label": "偏低"},
            {"lt": 0.1, "score": 34, "label": "低迷"},
            {"score": 46, "label": "一般"},
        ],
        "missing": 50,
    },
    "fund_cap_cls": {"good": 65, "bad": 45},
    "rating": [
        {"gte": 70, "label": "推荐关注"},
        {"gte": 58, "label": "偏积极"},
        {"gte": 45, "label": "中性观望"},
        {"gte": 35, "label": "偏谨慎"},
        {"label": "回避"},
    ],
    "action_map": {
        "推荐关注": "加仓",
        "偏积极": "持有偏多",
        "中性观望": "持有观望",
        "偏谨慎": "减仓偏空",
        "回避": "减仓",
    },
}


# ── 仓位调整（目标权重 − 当前权重 的 delta，驱动真实加减仓）──
# ⚠️ 与上方 RECOMMENDATION_CONFIG 的「股票研判」语义**正交**：
#   - 股票研判 = 该票本身技术/基本面/资金综合健康度（"加仓"≠真加钱）；
#   - 仓位调整 = (target_weight − current_weight) 的 delta，才是用户加减仓的依据。
# 二者并列展示在持仓日报，避免"把股票健康度误读成加钱指令"。
#
# 设计原则（符合"权重/阈值集中配置、禁散落魔数"）：
#  - 单票目标权重 target_weight_pct：组合层"等权偏中枢"默认目标，per_code_targets 可逐票覆盖。
#  - hard_cap_pct：单票硬上限，超过此权重**无论研判如何**都强制减仓（与 MAX_SINGLE_WEIGHT_PCT 同口径）。
#  - add/reduce_band_pct：当前权重相对目标偏离超过 ±band 才触发"候选加仓/减仓"。
#  - health_add_min_rating / health_reduce_max_rating：健康门控，用 RECOMMENDATION_CONFIG.rating
#    五档语义（推荐关注=最强 … 回避=最弱）：
#      * 候选加仓侧：研判须 ≥ health_add_min_rating 才真加仓；低于则"欠配但暂不加(持有偏多)"，
#        避免往弱票追仓。
#      * 候选减仓侧：研判 ≤ health_reduce_max_rating 才判"减仓/减仓偏空"；高于（健康尚可）
#        只做温和再平衡（减仓偏空、理由中性），不判为看空。
#  - lot_size：A股一手=100股（交易所规则），delta_qty 取整到此。
POSITION_SIZING_CONFIG = {
    "target_weight_pct": 8.0,           # 单票目标权重（%）
    "hard_cap_pct": 15.0,               # 单票硬上限（%），超过强制减仓
    "add_band_pct": 2.0,                # 欠配超过此值 → 候选加仓
    "reduce_band_pct": 2.0,             # 超配超过此值 → 候选减仓
    "health_add_min_rating": "偏积极",  # 候选加仓侧：研判须达此档（含）才真加仓
    "health_reduce_max_rating": "偏谨慎",  # 候选减仓侧：研判达此档（含）才判减仓
    "lot_size": 100,                    # A股一手=100股
    "per_code_targets": {},             # 可选：代码→目标权重覆盖，如 {"600519": 12.0}
    # ── 组合分母口径（含现金）──────────────────────────────────────────────
    # 报告原先只用「持仓市值」作分母 → 各票权重之和≈100%，会把「满仓但结构正常」
    # 误判成普遍超配（每只都 > target 8%）。计入账户现金后 current_weight 才是真实占比。
    # 现金来源优先级：环境变量 ACCOUNT_CASH > account_cash > account_cash_pct×持仓市值。
    # 默认 0 = 未配置（退回纯持仓口径，报告会显式提示如何修正）。
    # ⚠️ 2026-09-21 用户设定：账户现金 3 万元（用于「仓位调整」的真实权重分母）。
    "account_cash": 30000.0,            # 账户可用现金（元）；>0 时计入分母
    "account_cash_pct": None,           # 或按「现金 / 总资产」比例（0~1）估算现金额
}


def is_low_memory_host() -> bool:
    """内存 ≤1.5GB 的 Linux 容器（Render free 等托管环境）判定。

    关键：容器里 /proc/meminfo 显示的是宿主机总量（Render 宿主机几十 GB），
    真实限额在 cgroup（v2: memory.max / v1: memory.limit_in_bytes）——
    必须读 cgroup，否则 512MB 容器检测失败、自动精简永不生效。
    本地 Windows 无这些文件 → False（全功能）。环境变量可显式覆盖。
    """
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            with open(path) as f:
                raw = f.read().strip()
            if raw and raw != "max":
                return int(raw) < 1_500_000_000  # 1.5GB（字节）
        except (OSError, ValueError):
            continue
    try:
        with open("/proc/meminfo", encoding="utf-8") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) < 1_500_000  # kB
    except Exception:
        pass
    return False
