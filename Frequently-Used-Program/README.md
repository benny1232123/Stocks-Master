# Frequently-Used-Program 脚本分组说明

本目录包含常用的股票分析与自动化脚本。可按下面分组快速定位，减少“脚本太多不知道先跑哪个”的问题。

## 1）每日主流程（编排入口）

- auto_notify_boll.py：每日主流程（选股 + 汇总 + 推送 + 归档/清理）。
  各选股策略已重构为 smcore 模块，由本脚本以子进程 `python -m smcore.strategies.<name>`（cwd=项目根）调用：
  - `smcore.strategies.boll`       （Boll 多因子，策略1）
  - `smcore.strategies.theme`      （题材 + 换手率，策略2）
  - `smcore.strategies.cctv`       （CCTV 舆论热点板块，策略3）
  - `smcore.strategies.relativity` （相对强弱，策略4；`--max-workers 1` 单线程）

## 2）个股分析工具

- Stock-Analysis.py：单票/多票分析辅助工具

## 3）维护与辅助工具

- cleanup_stock_data.py：清理 stock_data 历史文件
- compress_stock_data.py：把 auto_logs / plots / ui_uploads / checkpoints 打成 zip 并删除旧文件
- test_cctv_sectors_strategy.py：CCTV 策略单元测试（动态加载 `smcore/strategies/cctv.py`）
- boll-visualizer/：旧的 Streamlit 可视化界面，已被根目录 React + FastAPI 前端替代，当前仅保留历史参考
- update：更新辅助脚本

## 4）回测工具（验证策略有效性）

- backtest_tradebook.py：基于真实买卖记录回测（支持单文件或买卖分文件）
- backtest_signal_picks.py：基于每日选股结果回测（支持滑点/佣金/印花税，输出毛收益与净收益）
- backtest_center_app.py：回测软件界面（真实成交页面录入 + 历史信号自动读取 + 参数表单 + 一键导出）

> 注：断点续跑/代码标准化等共享工具已从 strategy_common.py 迁入 `smcore.utils.checkpoint` 与 `smcore.utils.code`。
