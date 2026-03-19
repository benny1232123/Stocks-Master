# Boll Visualizer

基于 `Stock-Selection-Boll.py` 的全流程思路，做成可交互可视化工具，并且默认不使用东方财富接口。

## 功能

- 输入股票代码池，执行全流程筛选：
	- 同花顺资金流（3/5/10日）
	- baostock 基本面（资产负债率、净利润、现金流、增长）
	- 新浪流通股东（重点股东/股东性质）
	- Boll 信号（低于下轨/接近下轨）
- 支持两种运行模式：
	- 全流程（Selection Boll）
	- 仅Boll（跳过资金流/基本面/股东）
- 在仅Boll模式下支持“一键全市场分析”按钮（自动获取全A股代码）
- 在全流程模式下支持“一键全市场全流程分析”按钮（自动获取全A股代码）
- 运行时显示阶段进度（资金流阶段、逐股评估阶段、仅Boll阶段）
- 仅Boll全市场支持并发筛选与图表按需加载，显著降低首轮等待时间
- 仅Boll全市场支持“极速模式”（仅近N日筛选）
- 全流程支持并发评估、失败重试与请求限流（可在侧边栏高级参数中调节）
- 全流程支持“极速模式（5分钟目标）”：优先命中缓存、仅对资金流通过股票执行基本面，并跳过最慢的盈利预测接口
- 支持参数预设（保存/加载/删除），便于复用常用筛选参数
- 全市场任务支持后台异步执行，可在“后台任务中心”查看进度并加载历史结果
- 默认启用本地缓存（A股代码、资金流快照、K线单票增量缓存、全流程基本面缓存、股东缓存），重复运行显著加速
- 侧边栏提供缓存管理（统计、按范围清理、按天数清理）
- 全流程结果新增综合评分、等级与评分说明
- 输出每只股票的分步通过状态与最终命中结果
- 表格查看筛选结果，支持导出 CSV
- 选择股票查看交互式交易视图（K线 + 布林带 + 成交量 + 信号标记）
- 图表下可查看样本内历史信号回测（5/10/20日胜率、收益、回撤）
- 已抑制连续低于下轨/连续接近下轨的重复触发，避免重复入选与回测样本膨胀

## 快速开始（Windows）

### 方式1：一键启动（最简单）

在项目根目录双击运行：

`start-boll-visualizer.bat`

首次运行会自动安装依赖，随后自动打开 Streamlit 服务（默认端口 `8520`）。
如果 `8520` 被占用，脚本会自动切换到下一个可用端口（如 `8521`）。
如需忽略缓存并强制拉取最新数据，可在侧边栏勾选“强制刷新缓存（重新抓取）”。

### 方式2：链接访问（局域网 / 临时外网）

- `start-boll-visualizer.bat` 启动后会输出：
	- 本机地址：`http://localhost:端口`
	- 局域网地址：`http://你的内网IP:端口`（同一网络设备可直接访问）
- 若本机安装了 `cloudflared`，脚本会询问是否创建可分享的临时外网链接。
- 输入 `y` 后会在新窗口中显示 `https://xxxx.trycloudflare.com`，复制该链接即可分享给他人访问。

```powershell
cd c:\Users\29408\Desktop\Stocks-Master\Frequently-Used-Program\boll-visualizer
python -m pip install -r requirements.txt
streamlit run src\app.py
```

如需安装测试依赖：

```powershell
python -m pip install -r requirements-dev.txt
```

## 缓存与增量复用

- 缓存目录：`stock_data/cache/`
- 缓存内容：
	- `universe/`：全A股代码与名称（按日缓存）
	- `fund_flow/`：3/5/10日资金流快照（按日缓存）
	- `k_data/`：单票K线（按代码+复权方式缓存，全量后按请求区间增量补拉）
	- `full_flow/financial/`：全流程基本面指标缓存（按报告期缓存）
	- `full_flow/shareholder/`：全流程股东命中结果缓存（默认周级复用）
- 网络波动时会优先回退到最近可用缓存，避免整次任务失败。
- `Stock-Selection-Boll-All.py` 默认会复用当日已生成的结果文件；使用 `--force-refresh` 可强制重跑：

```powershell
cd c:\Users\29408\Desktop\Stocks-Master
python Frequently-Used-Program\Stock-Selection-Boll-All.py --force-refresh
```

- 支持断点续跑（适合全市场长任务）：

```powershell
cd c:\Users\29408\Desktop\Stocks-Master
python Frequently-Used-Program\Stock-Selection-Boll-All.py --resume --chunk-size 400
```

- 可选并发与重试参数（提高稳定性/速度）：

```powershell
cd c:\Users\29408\Desktop\Stocks-Master
python Frequently-Used-Program\Stock-Selection-Boll-All.py --resume --chunk-size 400 --max-workers 4 --max-retries 2 --retry-backoff 0.5 --request-interval 0.0
```

## 参数说明

- `window`：均线窗口，默认 `20`
- `k`：标准差倍数，默认 `1.645`
- `near_ratio`：接近下轨阈值，默认 `1.015`
- `adjust`：复权方式（`qfq` 前复权 / `hfq` 后复权）
- `price_upper_limit`：资金流环节股价上限
- `debt_asset_ratio_limit`：资产负债率上限（百分比）
- `exclude_gem_sci`：是否排除 `30*` 与 `688*`
- `max_workers`：全流程并发评估线程数
- `max_retries` / `retry_backoff`：网络失败重试次数与退避时间
- `request_interval`：请求限流间隔（秒）
- `market_fast_mode`：全流程极速模式（推荐用于全市场，目标 5 分钟量级）

## 目录结构

```text
boll-visualizer/
├─ src/
│  ├─ app.py
│  ├─ core/
│  │  ├─ data_fetcher.py
│  │  ├─ indicators.py
│  │  ├─ boll_strategy.py
│  │  ├─ full_flow_strategy.py
│  │  └─ task_manager.py
│  ├─ ui/
│  │  ├─ charts.py
│  │  └─ dashboard.py
│  └─ utils/
│     ├─ config.py
│     ├─ logger.py
│     └─ presets.py
├─ tests/
├─ requirements.txt
└─ requirements-dev.txt
```
