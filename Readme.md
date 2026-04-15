# Stocks-Master

Stocks-Master 是一个以 A 股筛选为主的脚本集合。为避免“脚本太多找不到入口”，建议优先使用统一入口：`stocks-master.bat`。

## 统一入口（推荐）

- 运行：`stocks-master.bat`
- 一个菜单覆盖常用操作：手动执行、注册任务、触发任务、检查任务、清理数据、启动可视化、邮件配置/测试、数据索引

## 原入口（兼容保留）

1. 手动跑一次并推送结果：`run-boll-auto-notify.bat`
2. 注册每日任务（21:30，新闻联播后）：`register-boll-daily-task.bat`
3. 立即触发一次已注册任务（显示进度）：`run-boll-daily-task-now.bat`
4. 查看任务状态：`check-boll-daily-task.bat`
5. 清理历史数据（默认保留 30 天）：`clean-stock-data.bat`
6. 生成数据总览索引：`index-stock-data.bat`
7. 自动归档历史数据（默认根目录保留 7 天）：`auto-archive-stock-data.bat`
8. 运行信号样本回测：`run-backtest-signal-picks.bat`
9. 运行真实成交回测：`run-backtest-tradebook.bat`
10. 打开回测软件界面：`start-backtest-center.bat`

## 目录说明（精简版）

- `Frequently-Used-Program/`: 主程序脚本（选股、推送、清理）
- `Frequently-Used-Program/README.md`: 主程序分组导航（BOLL/CCTV/分析/工具）
- `stock_data/`: 结果与缓存数据
- `Unnecessary-Programs/`: 历史原型与低频脚本

## 首次使用

1. 创建并激活虚拟环境（可选但推荐）
2. 安装依赖：`pip install -r requirements.txt`
3. 如需邮件推送，先运行：`configure-email-smtp.bat`
4. 验证邮件配置：`test-email-notify.bat`

## 自动推送说明

- 主流程脚本：`Frequently-Used-Program/auto_notify_boll.py`
- 支持企业微信机器人（`WECOM_WEBHOOK_URL`）
- 支持 SMTP 邮件（`SMTP_HOST/PORT/USER/PASS/TO`）
- 自动任务默认启用补跑：若错过计划时间，开机后执行一次
- 自动任务默认按天分类归档结果到 `stock_data/archive/YYYYMM/分类/`（可用 `ARCHIVE_ALL_ROOT_DATED=0` 改回仅归档旧文件）

## 全流程策略原理（重点）

主流程在 `Frequently-Used-Program/auto_notify_boll.py`，按“先筛信号，再做环境判断，最后给执行建议”的思路运行。

### 1. 流程结构（每日运行）

1. Boll 主策略：执行 `Stock-Selection-Boll.py`，得到技术面候选。
2. CCTV 板块策略：执行 `Stock-Selection-CCTV-Sectors.py`，提取舆论热点方向。
3. 宏观新闻风险：从 `*_news.csv` 提取地缘、能源、外需等风险标签。
4. 题材 + 相对强弱：执行 `Stock-Selection-Ashare-Theme-Turnover.py` 与 `Stock-Selection-Relativity.py`，补充风格轮动与抗跌筛选。
5. 数据归档：按规则移动历史结果到 `stock_data/archive/`。
6. 数据清理：删除超期日志/图表/日期文件。
7. 通知输出：把结论写成企业微信/邮件日报（含 Boll/题材/相对强弱摘要）。

### 2. 市场状态判定逻辑

系统会基于指数与风险信号做“市场状态体检”：

- 指数维度：上证(sh.000001)、沪深300(sh.000300) 的 5 日/20 日收益与 20 日波动。
- 结构维度：Boll 命中数量、题材候选数量、CCTV 热点活跃度。
- 风险维度：宏观新闻中的高/中风险事件数量。

最终将市场归类为：

- 趋势上行：优先题材轮动与热点跟随，Boll 做回踩确认。
- 震荡轮动：优先低吸高抛与强弱切换，保持分散。
- 下行防御：优先降仓位、缩周期、严止损，题材策略降权。

### 3. 为什么要多策略融合

- Boll 解决“点位和节奏”（什么时候更接近低风险买点）。
- 题材策略解决“方向和弹性”（哪些方向有资金共识）。
- 相对强弱解决“风格筛选”（顺风不弱、逆风抗跌）。
- CCTV/新闻解决“叙事与预期差”（市场在关注什么、是否升温）。
- 宏观风险解决“仓位约束”（先控制回撤，再追求收益）。

这是一套“信号分层”设计：技术面给时机，题材面给方向，宏观面给风控边界。

### 4. 日报里的建议如何得出

日报不是固定模板，而是由当前市场状态动态生成：

- 主策略与辅策略（做什么）
- 参数建议（怎么调阈值）
- 失效信号（何时切换策略）
- 执行清单（仓位、止损、止盈、复盘）

## CCTV 模块与关键词自动更新

### 1. CCTV 每天跑，统计按周期看

- CCTV 抓取与板块热度计算仍是日频执行。
- 日报展示可以按窗口聚合（默认 3 天，可改 5 天）：`CCTV_STATS_DAYS=3|5`。
- 聚合口径包括：区间均值热度、区间变化、上榜次数。

### 2. 关键词库自动更新原理

脚本会先从新闻中提取“新候选词”，再做板块归类建议，然后按规则自动入库到：

- `stock_data/cctv_keyword_accepts.json`（按 `yearly` 年度管理）

自动入库条件（默认）：

- 出现次数 >= 4
- 建议置信度 >= 中
- 与现有词库不重复

可调参数：

- `CCTV_AUTO_ACCEPT_KEYWORDS=1` 开启自动入库
- `CCTV_AUTO_ACCEPT_MIN_COUNT=4` 最小出现次数
- `CCTV_AUTO_ACCEPT_MIN_CONF=中` 最低置信度（低/中/高）

### 3. 补充资讯源（用于发现新词）

除了 CCTV 主源，还可抓取补充快讯源用于“关键词发现”，失败会自动降级，不影响主流程：

- `CCTV_DISABLE_EXTRA_NEWS=0` 是否关闭补充源
- `CCTV_EXTRA_NEWS_SOURCES=cls,sina` 补充源列表
- `CCTV_EXTRA_NEWS_LIMIT=120` 每源抓取上限

## 相对强弱策略接入说明

- 策略脚本：`Frequently-Used-Program/Stock-Selection-Relativity.py`
- 主流程开关：`ENABLE_RELATIVITY_STRATEGY=1`（`0` 关闭）
- 输出文件：`stock_data/Stock-Selection-Relativity-YYYYMMDD.csv`
- 邮件行为：会自动追加“相对强弱策略”摘要，并作为附件发送（成功时）。

提速机制（已启用）：

- 自动流程会把当日 `Stock-Selection-Boll-YYYYMMDD.csv` 作为 seed 传给相对强弱策略。
- 相对强弱脚本在 seed 模式下会跳过前置的资金流/基本面/股东重复筛选，只做相对强弱评估。
- 这样可显著减少重复 API 调用，缩短整体日报耗时。

可调参数（由自动流程透传）：

- `RELATIVITY_MAX_WORKERS=1` 相对强弱评估并发
- `RELATIVITY_RESUME=1` 开启断点续跑
- `RELATIVITY_SLEEP_SECONDS=2` 慢接口节流秒数
- `RELATIVITY_DISABLE_RS=0` 关闭指数相对强弱，仅保留前置候选
- `RELATIVITY_USE_SEED=0` 是否复用当日 Boll 结果作为 seed（默认关闭，避免 Relativity 只输出 Boll 候选）

## 共享工具层（重构说明）

为减少策略脚本重复实现，新增共享模块：

- `Frequently-Used-Program/strategy_common.py`

当前已统一能力：

- 股票代码标准化
- 检查点读写
- 结果合并去重

## 策略有效性验证（回测）

目前支持两种回测路径，建议组合使用：

- 真实成交回测：复盘你实际买卖记录（最贴近实盘）。
- 信号样本回测：复盘每日选股信号在固定持有周期下的统计表现（用于验证策略是否有统计优势）。

也可直接使用软件界面（推荐）：

- 启动：`scripts\\start-backtest-center.bat`
- 功能：真实成交可页面直接录入；信号样本自动读取历史文件；填写参数后点击运行并下载结果（无需命令行）。

### 1. 真实成交回测（你自己的买卖记录）

- 脚本：`Frequently-Used-Program/backtest_tradebook.py`
- 输入方式：
	- 单文件：同一 CSV 包含买入和卖出记录（用“买/卖”字段区分）
	- 双文件：买入 CSV + 卖出 CSV
- 输出：
	- `*-summary.csv`：总收益、胜率、最大回撤等
	- `*-closed-trades.csv`：逐笔已平仓明细
	- `*-equity-curve.csv`：权益曲线

示例：

```bash
python Frequently-Used-Program/backtest_tradebook.py --trades-csv stock_data/my_trades.csv
python Frequently-Used-Program/backtest_tradebook.py --buy-csv stock_data/my_buys.csv --sell-csv stock_data/my_sells.csv
```

可先复制模板再填充真实成交：`stock_data/my_trades.template.csv`

### 2. 信号样本回测（按每日候选自动复盘）

- 脚本：`Frequently-Used-Program/backtest_signal_picks.py`
- 逻辑：
	- 读取每日选股 CSV（默认 `stock_data/Stock-Selection-Boll-*.csv`）
	- 每个信号日取前 N 只（`--top-n`）
	- 以“次日开盘买入 + 持有 N 个交易日后收盘卖出”计算毛收益
	- 支持滑点/佣金/印花税，得到净收益（更贴近实盘）
	- 汇总净胜率、净收益、回撤统计
	- 回测按交易日执行，周末和节假日自动跳过，不按自然日累加
- 输出：
	- `*-trades.csv`：逐标的回测明细
	- `*-daily.csv`：按信号日聚合表现
	- `*-summary.csv`：总体统计

成本参数（可选）：

- `--buy-slip-bps`：买入滑点（基点，默认 5）
- `--sell-slip-bps`：卖出滑点（基点，默认 5）
- `--buy-fee-rate`：买入佣金（默认 0.0003）
- `--sell-fee-rate`：卖出佣金（默认 0.0003）
- `--sell-stamp-tax-rate`：卖出印花税（默认 0.001）

示例：

```bash
python Frequently-Used-Program/backtest_signal_picks.py --signals-glob "stock_data/Stock-Selection-Boll-*.csv" --top-n 10 --hold-days 5
python Frequently-Used-Program/backtest_signal_picks.py --signals-glob "stock_data/Stock-Selection-Relativity-*.csv" --top-n 8 --hold-days 7 --start-date 20260301 --end-date 20260411
python Frequently-Used-Program/backtest_signal_picks.py --signals-glob "stock_data/Stock-Selection-Boll-*.csv" --top-n 10 --hold-days 5 --buy-slip-bps 8 --sell-slip-bps 8 --buy-fee-rate 0.00025 --sell-fee-rate 0.00025 --sell-stamp-tax-rate 0.001
```

建议先用信号样本回测找参数区间，再用真实成交回测验证执行质量。

## 推荐生产参数模板（可直接套用）

下面给出两套常用参数。建议先用“稳健版”跑一周观察，再按机器性能切到“快速版”。

### 1. 稳健版（优先稳定与可复现）

适用场景：网络一般、希望减少接口抖动、强调结果一致性。

```bat
set FAST_MODE=0
set ENABLE_THEME_STRATEGY=1
set ENABLE_RELATIVITY_STRATEGY=1

set CCTV_STATS_DAYS=3
set CCTV_AUTO_ACCEPT_KEYWORDS=1
set CCTV_AUTO_ACCEPT_MIN_COUNT=5
set CCTV_AUTO_ACCEPT_MIN_CONF=中
set CCTV_DISABLE_EXTRA_NEWS=0
set CCTV_EXTRA_NEWS_SOURCES=cls,sina
set CCTV_EXTRA_NEWS_LIMIT=80

set RELATIVITY_MAX_WORKERS=1
set RELATIVITY_RESUME=1
set RELATIVITY_SLEEP_SECONDS=3
set RELATIVITY_DISABLE_RS=0

set ENABLE_AUTO_ARCHIVE=1
set ENABLE_AUTO_CLEANUP=1
```

### 2. 快速版（优先时效）

适用场景：机器性能较好、希望更快出日报。

```bat
set FAST_MODE=1
set ENABLE_THEME_STRATEGY=1
set ENABLE_RELATIVITY_STRATEGY=1

set CCTV_STATS_DAYS=3
set CCTV_AUTO_ACCEPT_KEYWORDS=1
set CCTV_AUTO_ACCEPT_MIN_COUNT=4
set CCTV_AUTO_ACCEPT_MIN_CONF=中
set CCTV_DISABLE_EXTRA_NEWS=0
set CCTV_EXTRA_NEWS_SOURCES=cls,sina
set CCTV_EXTRA_NEWS_LIMIT=120

set RELATIVITY_MAX_WORKERS=2
set RELATIVITY_RESUME=1
set RELATIVITY_SLEEP_SECONDS=1
set RELATIVITY_DISABLE_RS=0

set ENABLE_AUTO_ARCHIVE=1
set ENABLE_AUTO_CLEANUP=1
```

### 3. 参数调整建议

- 若接口超时增多：降低 `RELATIVITY_MAX_WORKERS`，并提高 `RELATIVITY_SLEEP_SECONDS`。
- 若关键词噪声偏多：提高 `CCTV_AUTO_ACCEPT_MIN_COUNT` 到 `6`，或把 `CCTV_AUTO_ACCEPT_MIN_CONF` 调到 `高`。
- 若日报过慢：保持 `FAST_MODE=1`，并将 `CCTV_EXTRA_NEWS_LIMIT` 下调到 `60`。

## 数据清理说明

- 清理脚本：`Frequently-Used-Program/cleanup_stock_data.py`
- 默认保留 30 天（日期结果、日志、图片）
- 手动执行示例：`clean-stock-data.bat 20`

可选环境变量：

- `ENABLE_AUTO_CLEANUP=0` 关闭自动清理
- `CLEANUP_KEEP_DAYS=30` 日期文件保留天数
- `CLEANUP_LOG_KEEP_DAYS=30` 日志保留天数
- `CLEANUP_PLOTS_KEEP_DAYS=30` 图片保留天数
- `CLEANUP_DRY_RUN=1` 仅预览，不删除

## 自动归档说明

- 归档脚本：`Frequently-Used-Program/archive_stock_data.py`
- 一键入口：`auto-archive-stock-data.bat`
- 默认策略：
	- 先整理已有归档目录，再执行归档
	- `stock_data/` 根目录仅保留最近 7 天日期文件
	- 更早文件移动到 `stock_data/archive/YYYYMM/类型/`
	- 归档区默认保留 365 天，超期自动删除

二级目录类型示例：

- `stock_data/archive/202603/boll/`
- `stock_data/archive/202603/cctv/`
- `stock_data/archive/202603/theme/`
- `stock_data/archive/202603/news/`

可选环境变量（自动任务中生效）：

- `ENABLE_AUTO_ARCHIVE=1` 开关自动归档（`0` 关闭）
- `ARCHIVE_KEEP_ROOT_DAYS=7` 根目录保留天数
- `ARCHIVE_KEEP_DAYS=365` 归档区保留天数
- `ARCHIVE_DRY_RUN=1` 仅预览，不移动删除

## stock_data 快速定位

- 执行：`index-stock-data.bat`
- 作用：自动生成 `stock_data/INDEX.md`
- 你可以在 `INDEX.md` 里一眼看到：
	- 今日新增文件
	- 每类数据的最新文件
	- 最近日期文件列表
	- 子目录（如 `auto_logs`、`plots`）文件数量与占用

## 策略有效性验证（回测）

### 1. 交易流水回测（推荐）

脚本：`Frequently-Used-Program/backtest_tradebook.py`

作用：基于你真实买卖成交记录，计算胜率、单笔收益、总收益、盈亏比、最大回撤，并输出权益曲线。

运行示例（单文件流水）：

`python Frequently-Used-Program/backtest_tradebook.py --trades-csv stock_data/my_trades.csv`

运行示例（买卖分文件）：

`python Frequently-Used-Program/backtest_tradebook.py --buy-csv stock_data/my_buys.csv --sell-csv stock_data/my_sells.csv`

输出文件（默认）：

- `stock_data/Trade-Backtest-YYYYMMDD-raw-trades.csv`
- `stock_data/Trade-Backtest-YYYYMMDD-closed-trades.csv`
- `stock_data/Trade-Backtest-YYYYMMDD-summary.csv`
- `stock_data/Trade-Backtest-YYYYMMDD-equity-curve.csv`

### 2. 如何导入买入/卖出信息

支持中英文列名自动识别。常用字段如下：

- 日期：`date` / `trade_date` / `日期` / `成交日期`
- 代码：`code` / `股票代码`
- 方向：`side` / `方向`（`BUY/SELL` 或 `买入/卖出`）
- 价格：`price` / `成交价`
- 数量（可选）：`quantity` / `数量`
- 手续费（可选）：`fee` / `手续费`

说明：

- 若不提供数量，默认按 `1` 计算。
- 若不提供手续费，默认按 `0` 计算。
- 买卖配对采用 FIFO（先进先出）。

### 3. 最小可用CSV模板

```csv
date,code,side,price,quantity,fee
2026-04-01,600000,BUY,10.25,1000,5
2026-04-08,600000,SELL,10.90,1000,5
2026-04-02,000001,BUY,12.30,800,4
2026-04-15,000001,SELL,11.80,800,4
```

## CCTV 板块策略（可选）

- 脚本：`Frequently-Used-Program/Stock-Selection-CCTV-Sectors.py`
- 运行：`python Frequently-Used-Program/Stock-Selection-CCTV-Sectors.py`
- 输出到 `stock_data/` 下的 `CCTV-*` 文件

如只关注每日选股与通知，可暂时忽略该模块。
