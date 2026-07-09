# Stocks-Master 项目长期记忆

## 工作优先级约定（2026-07-09 起）
- **云端是第一优先级，本地靠后**：所有改动先保证云端（GitHub Actions + 部署平台）能跑通、能部署；本地（Anaconda 直跑）可以稍后。
- 调研/选型时优先看「海外可达 + 合规 + 免费/便宜 + 能跑 FastAPI + akshare 拉得到数据」。

## 架构要点（务必先理清再动手）
- 数据链路：**GitHub Actions（ubuntu-latest，海外）** 跑 4 策略 → 融合出 `Daily-Action-List-{date}.csv` + prewarm 拉指数/市场宽度/SHIBOR 存 `daily_cache/*.pkl` → `git commit` 回仓库 → **部署平台（海外）** `uvicorn backend.main:app` 同时托管前端 `frontend/dist` 和 `/api/*`。
- **本地独立**：本机后端可走 Tdx（`TDX_ENABLED` 控制），但云端/CI 一律 `TDX_ENABLED=0`（海外连不上国内券商服务器）。
- 市场宽度：东财 `push2.eastmoney.com` 海外不可达 → 改用腾讯 `qt.gtimg.cn` 采样（已验证 63ms）。
- 回测引擎 `smcore/backtest/`（Backtrader 多策略），云端用 akshare 拉日K（海外通，已验证），指数拉不到自动降级。

## 部署平台现状（✅ 定档 Render Free，完整后端）
- 路线：用户定 Oracle → 无信用卡排除 → 短暂试 Cloudflare Pages 纯静态 → **最终"算了 render也能用"定档 Render Free**（2026-07-09）。
- Render 走**原生 Python 运行时 + 完整 FastAPI 后端**（`render.yaml`，plan:free）：`uvicorn backend.main:app --port $PORT` 同时托管前端 dist 与 /api/*；回测/选股/组合全部线上实时跑（比纯静态功能全）。
- `render.yaml` 关键项：env=python、branch=master、buildCommand 构建前端+装依赖、healthCheckPath=/、envVars 注入 `KLINE_BACKEND=akshare`/`TDX_ENABLED=0`/`TRADES_BACKEND=auto`；`SUPABASE_URL/KEY` sync:false（控制台手填）。
- 前端 `main.jsx`：纯净版，仅连真实后端（`VITE_STATIC`/`staticShim.js` 已废弃删除，无静态备选路径）。
- 免费限制：15min 无流量休眠 → 已配 `keep-alive.yml` 每 10min ping `RENDER_URL` secret 保活；RAM ~512MB（回测控标的量）。
- `daily-pick.yml`：只生成+提交 `stock_data`（Daily-Action-List + daily_cache）→ push 触发 Render 自动重新部署；**不再构建前端/生成静态 JSON**（Render 部署时自构建）。
- 已删除全部静态/容器备选与历史文档：`deploy-cf.yml`、`wrangler.toml`、`frontend/public/functions/[[path]].js`、`staticShim.js`、`scripts/generate_static_data.py`、`Dockerfile`、`docker-compose.yml`、`.dockerignore`、`.github/workflows/deploy.yml`、`DEPLOY_ORACLE.md`、`DEPLOY_CLOUDFLARE.md`、`DEPLOY_CLOUD.md`。现仅 Render 完整后端一条链路，无静态/容器回退。
- 云端 `TDX_ENABLED=0` 不变；默认分支 `master`；requirements.txt 含 `backtrader>=1.9.78`。

## 多策略选股体系（4 策略已全部迁入 smcore/strategies，2026-07-09）
- 原 `Frequently-Used-Program/` 下的 4 个独立选股巨石脚本已全部**重构成一等公民模块**，保留完整逻辑与 CSV 输出名：
  - `smcore/strategies/boll.py` → `run_boll()`（auto-boll 多因子：资金流+基本面+重要股东+布林，策略1）
  - `smcore/strategies/theme.py` → `run_theme()`（题材+换手率，策略2）
  - `smcore/strategies/cctv.py` → `run_cctv()`（CCTV 舆论板块，策略3）
  - `smcore/strategies/relativity.py` → `run_relativity()`（相对强弱，策略4；`--max-workers` 控并发）
  - `smcore/strategies/__init__.py` 统一导出以上 4 个 `run_*`。
- **已删除** `Frequently-Used-Program/` 下全部独立选股脚本：`Stock-Selection-Boll.py`(shim)、`Stock-Selection-Boll-All.py`(冗余批量)、`Stock-Selection-Relativity.py`、`Stock-Selection-Ashare-Theme-Turnover.py`、`Stock-Selection-CCTV-Sectors.py`、`Stock-Selection-News.py`(死脚本)、`strategy_common.py`(迁入 `smcore.utils.checkpoint`)。
- 各模块 `ROOT_DIR` 一律指向 `smcore.config.defaults.PROJECT_ROOT`（不再依赖 `__file__.parents[1]`），CSV 输出名不变（融合 `fusion.py` 按原名 glob）。
- 调用方统一改 `python -m smcore.strategies.<name>`（cwd=项目根使其可解析）：`daily-pick.yml` 4 步、`auto_notify_boll.py` 子进程（boll/theme/cctv/relativity）、`boll-visualizer` 三个 UI 动态加载、单测 `test_cctv_sectors_strategy.py`。
- auto-boll 多因子链路（务必保留）：资金流向(3/5/10日主力净流入 ∩ 现价[5,30]) → 基本面(资产负债率<70% ∩ 净利润>0 ∩ 经营性现金流>0) → 剔除创业板(30x)/科创板(688x) → 流通股东含重要股东(香港中央结算/汇金/社保) → 布林带(收盘<下轨 或 <=下轨×1.015近下轨，连续超卖/近下轨本日不重复触发)。
- 布林带统一走 `smcore.indicators.boll`（`calc_bollinger` + `evaluate_boll_signal`，k=1.645，前复权）；K线 baostock 前复权(`adjustflag=2`) 为主、akshare 兜底（海外/云端 baostock 不可达自动降级）。
- 后端 `smcore/selection.scan_boll_batch([])` 空候选时调 `run_boll()` 返回归一化列(代码/名称/最新价/信号)。
- 前端选股页：仅保留一个「开始选股」按钮，串联完整流程：候选池 → 完整布林多因子 → 策略融合（`/api/selection/fusion`） → 自动回测；融合完成后刷新 Overview 与日报数据。

## 前端 UI 关键约定（2026-07-10）
- 概览 Hero 里的日报预览：必须用**显式列名**读取（`股票代码`/`股票名称`/`最新价`），禁止用 `Object.keys(row)[0/1/2]` 动态列顺序，否则会把「命中策略数」等错误显示成价格。
- 选股页按钮：**只保留一个「开始选股」**，跑完整多因子 + 融合 + 回测；不再分「开始选股」与「运行完整布林选股(多因子)」两个按钮。
- 个股分析页：左侧长列表已改为顶部可搜索下拉选择器 + 代码输入框，节省空间。
- 日报/最新日报：统一用 `DailyExpandableList` 二层可展开组件。首层显示代码/名称/来源策略/综合评分/建议买入价，展开显示仓位/金额/最新价/止损/止盈/MA20。
- 数据刷新：前端在 `/api/selection/fusion` 完成后会重新拉取 `/api/artifacts/daily-action-list` 与 `/api/artifacts/daily-action-list/full`，保证 Overview 与日报页显示最新融合结果。

## 环境坑
- 本地 `stocks-master.bat` 后端用 **Anaconda**（`E:\Anaconda\python.exe app.py`），所以 backtrader **必须装进 Anaconda**（`E:\Anaconda\python.exe -m pip install backtrader`）。受管 venv 里的 backtrader 只用于验证。
