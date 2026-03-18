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
- 输出每只股票的分步通过状态与最终命中结果
- 表格查看筛选结果，支持导出 CSV
- 选择股票查看交互式布林带图（Plotly）

## 快速开始（Windows）

### 方式1：一键启动（最简单）

在项目根目录双击运行：

`start-boll-visualizer.bat`

首次运行会自动安装依赖，随后自动打开 Streamlit 服务（默认端口 `8520`）。
如果 `8520` 被占用，脚本会自动切换到下一个可用端口（如 `8521`）。

```powershell
cd c:\Users\29408\Desktop\Stocks-Master\Frequently-Used-Program\boll-visualizer
python -m pip install -r requirements.txt
streamlit run src\app.py
```

## 参数说明

- `window`：均线窗口，默认 `20`
- `k`：标准差倍数，默认 `1.645`
- `near_ratio`：接近下轨阈值，默认 `1.015`
- `adjust`：复权方式（`qfq` 前复权 / `hfq` 后复权）
- `price_upper_limit`：资金流环节股价上限
- `debt_asset_ratio_limit`：资产负债率上限（百分比）
- `exclude_gem_sci`：是否排除 `30*` 与 `688*`

## 目录结构

```text
boll-visualizer/
├─ src/
│  ├─ app.py
│  ├─ core/
│  │  ├─ data_fetcher.py
│  │  ├─ indicators.py
│  │  ├─ boll_strategy.py
│  │  └─ full_flow_strategy.py
│  ├─ ui/
│  │  ├─ charts.py
│  │  └─ dashboard.py
│  └─ utils/
│     ├─ config.py
│     └─ logger.py
├─ tests/
└─ requirements.txt
```
