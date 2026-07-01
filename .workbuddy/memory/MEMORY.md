# Stocks-Master 项目记忆

## 架构决策（2026-06-28 重构）
- 建立 `smcore/` 共享内核（原名 core/，因与 visualizer src/core 同名冲突而改）作为单一真相源，两条主线（命令行 auto_notify_boll + 可视化 boll-visualizer）统一依赖。
- 强制前复权(qfq, adjustflag=2)：不复权会导致布林带断裂、信号失真（这是历史"不可信"的头号根因，命令行+可视化两侧都已修复）。
- Boll 指标/信号、代码标准化、财报日期、K线获取、baostock 会话、推送 均只在 smcore 实现一次。

## 关键参数（统一后）
- Boll: window=20, k=1.645, near_ratio=1.015
- 复权: qfq（adjustflag=2）
- 股价上限: 30（原可视化 35 已统一为命令行口径）
- 财报期 <5月: 用去年三季报(0930)，非年报（年报披露中不齐全）

## 运行环境
- `/e/Anaconda/python.exe`（含完整依赖 pandas/akshare/baostock/streamlit）；managed python 3.13 无项目依赖。
- 入口：`streamlit_app.py`（可视化）、`scripts/*.bat`（命令行定时任务）。

## 重构进度
- 阶段1（内核+止血）完成，详见 `REFACTOR_PROGRESS.md`。
- 阶段2-A（可视化主线）完成。
- 阶段2-B（命令行主线）基本完成：3a-3e 全部抽出委托 smcore，"不可信"根因（adjustflag=3 不复权）已治；3d-2(消息构建+pipeline，耦合深非bug)保留巨石。
- 巨石 auto_notify_boll.py 3306→2482 行（减25%）。
- 阶段3（功能补全）：① 信号融合、④ 持仓盈亏联动、⑥ 24h守护进程(run_daemon.py) 完成。
- 阶段4（全云端）：kline.py 加 akshare 后端(KLINE_BACKEND=akshare)；GitHub Actions workflow(daily-pick.yml) 工作日21:30选股+上传COS。完全不用开机，0元/月。
- **盘中预警已删除（2026-07-01）**：scf_alert.py/zip、build_scf_package.py、DEPLOY_SCF.md 均已删除；jobs.py 移除 job_intraday_alert+job_refresh_quotes；run_daemon.py 只保留每日选股。
- smcore 模块：utils/(code+dates+format+logging) indicators/boll config/defaults data/(session+kline+index+quote+quote_sina) cache.py notify/(wecom+email) risk/(external+macro) strategy/(allocation+fusion) portfolio/pnl scheduler/(engine+jobs) storage/cos
- 部署文档：DEPLOY_CLOUD.md（全云端）
- HF Spaces 网页版（2026-07-01）：app.py 为入口，pages/ 下 5 个功能页面，零费用全云端，详见各页面文件
