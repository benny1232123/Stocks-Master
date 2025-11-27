# Stocks-Master Copilot 指南

欢迎来到 Stocks-Master 项目！该项目是一套用于股票数据分析和选择的 Python 脚本。以下是帮助您快速上手的关键信息。

## 项目概述

该代码库旨在通过利用各种财务指标和技术分析策略，实现对A股市场股票的筛选、分析和选择过程的自动化。它使用多个独立的 Python 脚本，每个脚本都专注于特定的分析任务。

## 核心组件

- **`Frequently-Used-Program/`**: 包含核心的、经常使用的股票筛选和分析脚本。
  - `Stock-Selectiion-Trend.py`: 基于趋势、财务指标和筹码分布进行选股。
  - `Stock-Selection-Boll.py`: 使用布林带策略进行选股。
  - `Stock-Analysis.py`: 对单个或多个股票进行深入分析。

- **`stock_data/`**: 存储从 API 获取的原始数据和分析结果的 CSV 文件。这可以作为缓存，避免重复请求。
  - 文件通常以日期或指标命名，例如 `Stock-Selection-20251103.csv` 或 `Industry-Funds-Flow-10日排行.csv`。

- **根目录脚本**: 根目录中的脚本 (`Stock-Big-Deal.py`, `Stock-Buffet-index.py` 等) 是用于特定、一次性查询或作为更复杂脚本的早期原型。

## 关键库和数据源

- **`akshare`**: 主要的数据接口，用于从网络上获取实时和历史股票数据。
- **`baostock`**: 另一个关键的数据源，提供股票历史数据和财务数据。
- **`pandas`**: 用于所有数据操作、分析和处理的核心库。
- **`scikit-learn`**: 用于数据建模，例如在 `Stock-Selectiion-Trend.py` 中进行线性回归分析。

## 开发工作流程

1.  **环境设置**:
    - 没有 `requirements.txt` 文件。您需要根据脚本中的 `import` 语句手动安装依赖项。
    - 主要依赖项包括: `pip install akshare baostock pandas scikit-learn`

2.  **运行分析**:
    - 脚本是独立运行的。您可以直接从终端执行它们。
    - 示例: `python "Frequently-Used-Program/Stock-Selectiion-Trend.py"`

3.  **配置**:
    - 大多数脚本的顶部都有一个配置区，您可以在其中调整参数，如股价限制、财务比率阈值和日期。
    - 示例 (`Stock-Selectiion-Trend.py`):
      ```python
      # --- 配置区 ---
      PRICE_UPPER_LIMIT = 30  # 股价上限
      DEBT_ASSET_RATIO_LIMIT = 70  # 资产负债率上限
      PROFIT_RATIO_LIMIT = 0.5 # 筹码获利比例上限
      ```

## 代码约定

- **数据缓存**: 脚本首先尝试从 `stock_data/` 目录加载本地 CSV 文件。如果文件不存在或已过期，它们将从 `akshare` 或 `baostock` 获取数据，并保存为新的 CSV 文件。
- **日期处理**: 脚本使用 `datetime` 模块来确定财报周期（季报、年报），并相应地获取数据。
- **模块化**: 尽管许多脚本是独立的，但它们遵循一个通用模式：设置参数 -> 获取数据（从缓存或 API）-> 使用 pandas 进行分析 -> 打印结果并保存到 CSV。
