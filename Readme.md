# Stocks-Master

Stocks-Master 是一个以 A 股筛选为主的脚本集合。为避免“脚本太多找不到入口”，建议优先使用统一入口：`stocks-master.bat`。

## 统一入口（推荐）

- 运行：`stocks-master.bat`
- 一个菜单覆盖常用操作：手动执行、注册任务、触发任务、检查任务、清理数据、启动可视化、邮件配置/测试、数据索引

## 原入口（兼容保留）

1. 手动跑一次并推送结果：`run-boll-auto-notify.bat`
2. 注册每日任务（19:00）：`register-boll-daily-task.bat`
3. 立即触发一次已注册任务（显示进度）：`run-boll-daily-task-now.bat`
4. 查看任务状态：`check-boll-daily-task.bat`
5. 清理历史数据（默认保留 30 天）：`clean-stock-data.bat`
6. 生成数据总览索引：`index-stock-data.bat`
7. 自动归档历史数据（默认根目录保留 7 天）：`auto-archive-stock-data.bat`

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

## CCTV 板块策略（可选）

- 脚本：`Frequently-Used-Program/Stock-Selection-CCTV-Sectors.py`
- 运行：`python Frequently-Used-Program/Stock-Selection-CCTV-Sectors.py`
- 输出到 `stock_data/` 下的 `CCTV-*` 文件

如只关注每日选股与通知，可暂时忽略该模块。
