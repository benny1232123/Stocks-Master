# Frequently-Used-Program 脚本分组说明

本目录包含常用的股票分析与自动化脚本。可按下面分组快速定位，减少“脚本太多不知道先跑哪个”的问题。

## 1）BOLL 主流程（每日优先）

- auto_notify_boll.py：每日主流程（选股 + 汇总 + 推送 + 归档/清理）
- Stock-Selection-Boll.py：核心 BOLL 选股策略
- Stock-Selection-Boll-All.py：全市场/批量 BOLL 分析
- Stock-Selection-Relativity.py：相对强弱策略（已接入 auto_notify_boll 主流程）

## 2）CCTV / 新闻 / 题材策略（可选增强）

- Stock-Selection-CCTV-Sectors.py：CCTV 舆论热点板块策略
- Stock-Selection-News.py：新闻驱动策略
- Stock-Selection-Ashare-Theme-Turnover.py：题材 + 换手率策略
- Stock-Selection-Relativity.py：相对强弱策略

## 3）个股分析工具

- Stock-Analysis.py：单票/多票分析辅助工具

## 4）维护与辅助工具

- cleanup_stock_data.py：清理 stock_data 历史文件
- test_cctv_sectors_strategy.py：CCTV 策略测试脚本
- boll-visualizer/：可选 Streamlit 可视化界面
- update：更新辅助脚本
- strategy_common.py：共享工具层（代码标准化、检查点读写、结果合并）

## 5）回测工具（验证策略有效性）

- backtest_tradebook.py：基于真实买卖记录回测（支持单文件或买卖分文件）
- backtest_signal_picks.py：基于每日选股结果回测（支持滑点/佣金/印花税，输出毛收益与净收益）
- backtest_center_app.py：回测软件界面（真实成交页面录入 + 历史信号自动读取 + 参数表单 + 一键导出）

常用示例：

```bash
python Frequently-Used-Program/backtest_tradebook.py --trades-csv stock_data/my_trades.csv
python Frequently-Used-Program/backtest_signal_picks.py --signals-glob "stock_data/Stock-Selection-Boll-*.csv" --top-n 10 --hold-days 5
python Frequently-Used-Program/backtest_signal_picks.py --signals-glob "stock_data/Stock-Selection-Boll-*.csv" --top-n 10 --hold-days 5 --buy-slip-bps 8 --sell-slip-bps 8 --buy-fee-rate 0.00025 --sell-fee-rate 0.00025 --sell-stamp-tax-rate 0.001
```

批处理入口：

```bat
scripts\run-backtest-signal-picks.bat
scripts\run-backtest-tradebook.bat
scripts\start-backtest-center.bat
```

可通过环境变量覆盖默认参数（例如 `TOP_N`、`HOLD_DAYS`、`START_DATE`、`END_DATE`、`BUY_SLIP_BPS`）。
真实成交回测可用 `TRADES_CSV` 或 `BUY_CSV`+`SELL_CSV` 覆盖输入文件。
可先参考模板：`stock_data/my_trades.template.csv`。

## 推荐日常使用路径

1. 从仓库根目录运行统一入口：stocks-master.bat。
2. 日常仅关注本目录第 1 组脚本；需要增强时再启用第 2 组。
3. 定期执行清理与归档，避免 stock_data 持续膨胀。

## 当前策略组合（简版）

- 技术面：BOLL 用于判断节奏与潜在低风险买点。
- 题材面：题材/换手率策略用于识别资金活跃方向。
- 强弱面：相对强弱策略用于筛选顺风不弱、逆风抗跌标的。
- 舆论面：CCTV 热点用于补充市场关注主题。
- 风控面：宏观风险信号用于限制仓位与回撤。

这套流程的目标是“先控风险，再做收益增强”：先判断市场状态，再给出对应策略建议与参数。

## 相对强弱阶段参数（自动流程）

- ENABLE_RELATIVITY_STRATEGY=1：是否启用相对强弱阶段
- RELATIVITY_MAX_WORKERS=1：并发评估线程数
- RELATIVITY_RESUME=1：是否启用断点续跑
- RELATIVITY_SLEEP_SECONDS=2：慢接口节流间隔
- RELATIVITY_DISABLE_RS=0：关闭指数相对强弱，仅输出前置候选
