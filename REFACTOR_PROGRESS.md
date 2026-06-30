# Stocks-Master 重构进度

## 诊断结论

### "不可信"根因
1. **复权方式冲突（头号元凶）**：命令行 `Stock-Selection-Boll.py` 用不复权(`adjustflag=3`)算 Boll，可视化用前复权(qfq)。同一只票两条线算出不同信号；不复权数据遇除权除息日布林带断裂，信号失真。**已修复**。
2. **Boll 逻辑 3 套独立实现**：`Stock-Selection-Boll.py` / `visualizer/core/indicators.py` / `auto_notify_boll._calc_boll_levels`，参数名、边界条件、返回结构各异。**visualizer 侧已统一到 smcore；命令行侧待任务3**。
3. **参数两套值**：股价上限命令行 30 / 可视化 35；财报期 <5月 命令行用年报 / data_fetcher 用三季报。**已统一**。

### "乱"根因
1. **3306 行巨石**：`auto_notify_boll.py` 一个文件塞了宏观分析 / 美股外汇 / Boll 计算 / 策略权重 / 市场状态 / 消息构建 / 归档清理 / 主流程 10 件事。**待任务3拆解**。
2. **两条主线零代码共享**：命令行靠 subprocess 调脚本，可视化有自己的 core 层。**可视化已接入 smcore**。
3. **数据缓存双轨**：SQLite(`stocks_data.db`) + 文件 CSV 并存。**待任务5统一**。
4. **baostock login 散落 6+ 处**，全市场扫描重复登录数千次。**smcore 已提供单例会话**。
5. **明确 bug**：`run_macro_news.py:44` 漏 `import os`。**已修复**。

## 目标架构
建立共享内核 `smcore/`（原名 core/，因与 visualizer 的 src/core 同名冲突而改名），两条主线都依赖它。

## 已完成

### 阶段 1：内核 + 止血
- [x] `smcore/utils/` — 代码标准化（统一 4 处）、财报日期、日志
- [x] `smcore/indicators/boll.py` — Boll 唯一实现（统一 3 处）
- [x] `smcore/config/defaults.py` — 统一参数（强制前复权、统一股价上限 30）
- [x] `smcore/data/session.py` — baostock 单例会话
- [x] `smcore/data/kline.py` — 前复权 K 线获取 + 缓存
- [x] 修复 `Stock-Selection-Boll.py` 复权 bug（adjustflag 3→2）
- [x] 修复 `run_macro_news.py` import os

### 阶段 2-A：可视化主线迁移（任务4）
- [x] `streamlit_app.py` 把仓库根加入 sys.path，使 smcore 可见
- [x] `boll-visualizer/src/core/indicators.py` re-export smcore.indicators
- [x] `boll-visualizer/src/utils/config.py` re-export smcore.config（股价上限 35→30）
- [x] `boll-visualizer/src/core/data_fetcher.py` 的 K线获取/代码标准化 re-export smcore
- [x] 验证：boll_strategy / full_flow_strategy / backtester 的 Boll 全部经 smcore，无独立实现
- 注：data_fetcher 的 fund_flow/universe 为 visualizer 独有功能，非 Boll 根因，留后续

### 阶段 2-B：命令行主线迁移（任务3，进行中）
- [x] **3a Boll/数据委托（"不可信"根因修复）**：`auto_notify_boll.py` 第三套 Boll 实现 `_build_indicator_levels` 重写为委托 smcore（前复权 + 单例会话 + calc_bollinger）；`_fetch_bs_latest_row`/`_fetch_bs_close_series` 的 `adjustflag="3"`→`"2"`；`_normalize_code`/`_to_bs_code`/`_to_ak_index_symbol` 委托 smcore。等价性验证通过（代码标准化 8 用例全等、Boll 水位误差 <1e-9），真实数据冒烟通过（600519/000001/601318 三票水位合理）
- [x] **3c 推送抽出**：`smcore/notify/`（email.py）落地，巨石 `send_email` 改委托，签名不变调用方零改动
- [x] **3e 缓存层抽出（任务5核心，3b前提）**：`smcore/cache.py`（cache_table_name + read_cache_df + write_cache_df + clear_cache_by_prefix + DB_PATH）；巨石 3 个缓存函数改委托；表名生成规则与原实现逐字符等价验证通过
- [x] **3b-1 外部市场风险抽出（3b 安全部分）**：`smcore/risk/external.py`（safe_float + fetch_us_market_data + fetch_fx_data + fetch_futures_data + assess_us/fx/futures_risk，~170行）；巨石 6 个函数改委托；行为等价验证通过（safe_float/阈值/期货多条件全对照）
- [x] **3b-2 宏观风险深度耦合部分抽出（~500行）**：`smcore/risk/macro.py`（9 个词库常量 + 13 个文本/NLP/事件函数：is_*/clean/extract_tokens/nlp_classify/extract_burst_tokens/collect_macro_risk_events/macro_risk_level）；巨石对应函数与常量改委托，行为逐项等价验证通过（词库成员、阈值判定、token 提取正则、风险等级反推全对照）。构造 news CSV 冒烟：联播快讯排除/中东冲突命中/无风险词跳过 均符合原逻辑。
- [x] **3d-1 可抽部分完成（格式化+策略权重+指数数据）**：`smcore/utils/format.py`（7 个格式化函数）、`smcore/strategy/allocation.py`（5 个仓位分配函数，含 build_strategy_allocation 返回完整 dict）、`smcore/data/index.py`（fetch_index_close_series + calc_index_metrics）。巨石对应函数改委托，行为等价验证通过（含策略权重和=100、趋势上行/下行防御配比、上证指数真实数据 ret5d/ret20d/vol20d）。
- [ ] **3d-2 剩余（消息构建+归档+pipeline，留巨石）**：`_build_message`/`_build_market_and_strategy_summary`/`_run_data_*`/`_run_command_with_live_output`/main。这些组装最终日报文本、依赖 pipeline 执行框架与全局 RUN_LOG 状态，耦合最深；搬动收益低风险高，属纯代码组织，不影响正确性，保留在巨石。
- 巨石 3306 → 2482 行（累计 -824 行，减 25%）；核心"不可信"早已治，3d 可抽部分已完成

## 待办
- [x] 任务3 基本完成：3a/3b-1/3b-2/3c/3d-1/3e 全部抽出；3d-2（消息构建+pipeline）保留巨石（耦合深，非 bug）。
- [x] 任务5 完成：清理 `Unnecessary-Programs/`、更新 README、端到端验证、缓存层抽出。

## 验证状态（2026-06-29）
- 全量 py_compile（smcore + 巨石 + streamlit_app + run_macro_news）通过
- smcore 全模块 import 通过
- 巨石委托链路（代码/Boll/缓存/risk/推送）通过
- 真实数据端到端（600519 前复权 Boll 信号）通过
- 等价性验证（代码标准化 8 用例、Boll 水位 <1e-9、缓存表名、risk 阈值）通过
- 无 Unnecessary-Programs 残留引用

## 风险提示
- 任务3 剩余 3b-2/3d 为纯代码组织优化，不影响正确性，可后续按需推进。
- 复权口径统一后，命令行选股结果会与历史不同（修正，非回归）。
- 股价上限统一为 30（可视化原 35），可视化选股范围略收窄。
- `app import` 失败于 `protobuf` 版本冲突（supabase 依赖），是 anaconda 环境问题，非重构引入；已用 `pip install --force-reinstall protobuf` 修复。
