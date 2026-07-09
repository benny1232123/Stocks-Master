# Stocks-Master

A 股多策略选股系统 —— 以 Boll 布林带为主，融合题材热度 / 相对强弱 / CCTV 舆情，支持**本地运行**与**全云端自动运行**。

---

## 架构总览（云端为主）

```text
┌─────────────────────────────────────────────────────────────┐
│  GitHub Actions（免费，美国服务器）                          │
│  工作日 21:30（北京时间）cron 自动触发                       │
│    ├─ 策略1 Boll 布林带扫描      (timeout 25min)             │
│    ├─ 策略2 题材热度             (timeout 20min)             │
│    ├─ 策略3 CCTV 板块舆情        (timeout 25min)             │
│    └─ 策略4 相对强弱             (timeout 20min, 单线程)      │
│         ↓ 每个策略先 pull 缓存，命中则跳过；跑完 push 缓存   │
└───────────────────────────┬─────────────────────────────────┘
                            ↓ 写入
┌─────────────────────────────────────────────────────────────┐
│  Supabase（PostgreSQL + PostgREST，云端缓存层）              │
│  strategy_cache 表：按 (strategy, trade_date) 缓存选股结果   │
│  作用：跨环境复用、断点续跑、作为 web 看板的数据源           │
└───────────────────────────┬─────────────────────────────────┘
                            ↓ 读取
┌─────────────────────────────────────────────────────────────┐
│  Render（Web 服务，部署看板）                                │
│  uvicorn backend.main:app → FastAPI + React 看板             │
│  展示：指数 / 市场热度 / 选股结果 / 持仓 / 回测              │
└─────────────────────────────────────────────────────────────┘

（可选）腾讯云 COS + SCF：操作清单存档 + 盘中预警，与 GitHub Actions 经 COS 传数据。
```

**关键点**：
- 选股在 **GitHub Actions** 跑（不开电脑也行，0 元/月）；
- 结果缓存在 **Supabase**，避免重复调接口、支持断点续跑；
- 看板在 **Render** 部署，从 Supabase 实时读取展示；
- 任一接口挂掉时自动回退到**本地 SQLite**（`stock_data/stocks_data.db`），不会整流程崩。

---

## 目录结构

```text
Stocks-Master/
├── smcore/                     # 共享内核（命令行 + 看板唯一真相源）
│   ├── indicators/boll.py      #   Boll 计算（唯一实现）
│   ├── data/                   #   K线/行情获取（akshare / baostock 双后端 + 缓存）
│   ├── strategy/               #   信号融合 fusion.py、仓位 allocation.py
│   ├── storage/                #   cos.py（COS）、trades_repo.py（Supabase 交易）
│   ├── notify/email.py         #   邮件推送（唯一推送渠道）
│   ├── portfolio/pnl.py        #   持仓盈亏
│   └── scheduler/              #   本地定时调度引擎
│
├── backend/                    # FastAPI 后端入口（web 看板 API）
│   └── main.py                 #   启动 uvicorn，load_dotenv 读取 .env
│
├── frontend/                   # React 前端（web 看板）
│
├── Frequently-Used-Program/    # 选股主程序脚本
│   ├── auto_notify_boll.py     #   本地编排入口（含 CCTV idle 超时看门狗）
│   ├── Stock-Selection-Boll.py             # 策略1
│   ├── Stock-Selection-Ashare-Theme-Turnover.py  # 策略2
│   ├── Stock-Selection-Relativity.py        # 策略4（--max-workers 1 单线程）
│   └── Stock-Selection-CCTV-Sectors.py      # 策略3（含 API 超时 + 本地回退）
│
├── scripts/                    # 工具脚本
│   ├── strategy_cache.py       # ⭐ Supabase 缓存 pull / push
│   └── prewarm_dashboard.py    #   看板数据预热
│
├── app.py                      # 后端启动入口（兼容）
├── .github/workflows/
│   └── daily-pick.yml          # ⭐ GitHub Actions 云端选股流水线
├── render.yaml                 # ⭐ Render Web 服务部署定义
│
├── stock_data/                 # 结果输出 + 本地 SQLite 回退
│   └── stocks_data.db          #   缓存的股票基础信息（API 失败回退用）
│
├── requirements.txt            # Python 依赖（supabase 为 optional）
├── runtime.txt
└── stocks-master.bat           # Windows 一键启动（后端 + 前端）
```

---

## 快速开始

### 本地运行

```bash
# 1. 安装依赖
E:/Anaconda/python.exe -m pip install -r requirements.txt

# 2. 安装前端依赖并构建
cd frontend && npm install && npm run build && cd ..

# 3. 配置 .env（含 SUPABASE_URL / SUPABASE_KEY，已 gitignore，勿提交）
#    或直接在环境变量中设置

# 4. 启动后端 + 前端
stocks-master.bat
# 或分别：E:/Anaconda/python.exe app.py   与   cd frontend && npm run dev

# 5. 命令行选股
python Frequently-Used-Program/auto_notify_boll.py
```

### 全云端运行（不开机也跑）

详见 `DEPLOY_RENDER.md` 与 `SETUP_GUIDE.md`：

1. 推代码到 **GitHub 私有仓库**
2. 在 GitHub Secrets 配置 `SUPABASE_*` / `SMTP_*` / `COS_*`
3. 启用 GitHub Actions（工作日 21:30 自动选股 → 写 Supabase → 推送邮件）
4. 在 Render 连接仓库部署 Web 看板（读取 Supabase）

---

## Supabase 缓存层

跨环境缓存选股结果，是云端架构的数据中枢。

- **作用**：同一天重复运行直接命中缓存跳过；本地 / 云端 / 看板共享同一份结果；接口失败时回退本地不影响产出。
- **配置**：
  - GitHub Actions：仓库 Secrets 设 `SUPABASE_URL` / `SUPABASE_KEY`（`daily-pick.yml` 每个步骤已注入）；
  - 本地：`.env` 文件（已加入 `.gitignore`，请勿提交含 key 的文件）；
  - Render：`render.yaml` 中 `SUPABASE_URL` / `SUPABASE_KEY` 设为 `sync: false`，需在 **Render Dashboard → Environment** 手动填写。
- **表结构**（`scripts/strategy_cache.py` 自动建表）：
  ```sql
  CREATE TABLE strategy_cache (
      id          BIGSERIAL PRIMARY KEY,
      strategy    TEXT NOT NULL,        -- boll / theme / cctv / relativity
      trade_date  TEXT NOT NULL,        -- YYYYMMDD
      csv_content TEXT NOT NULL,
      row_count   INTEGER DEFAULT 0,
      created_at  TIMESTAMPTZ DEFAULT now(),
      UNIQUE(strategy, trade_date)
  );
  ```
- **使用**：
  ```bash
  python scripts/strategy_cache.py pull cctv 20260708   # 命中则恢复本地 CSV
  python scripts/strategy_cache.py push cctv 20260708 [csv路径]  # 上传当天结果
  ```
- **容错**：API 调用失败或返回空时，自动回退读取本地 `stock_data/stocks_data.db` 同名表。

---

## 云端运行详解

### GitHub Actions 选股（`daily-pick.yml`）

- **触发**：工作日 21:30 北京时间（cron `30 13 * * 1-5` UTC，约 5–15 分钟延迟）
- **K 线后端**：akshare 东财接口（不依赖 baostock，规避境外连国内服务器问题）
- **流程**：每个策略先 `strategy_cache.py pull` 查缓存 → 命中则 `exit 0` 跳过 → 否则跑选股 → `push` 上传
- **超时**（单 step）：Boll 25min / 题材 20min / CCTV 25min / 相对强弱 20min；总 job 90min
- **费用**：私有仓库 2000 分钟/月免费，选股约 660 分钟/月，够用（0 元）

### Render 部署 Web 看板（`render.yaml`）

- **类型**：web 服务，`uvicorn backend.main:app --host 0.0.0.0 --port $PORT`
- **构建**：`cd frontend && npm install && npm run build && cd .. && pip install -r requirements.txt`
- **环境变量**：`KLINE_BACKEND=akshare`、`TRADES_BACKEND=auto`；`SUPABASE_URL` / `SUPABASE_KEY` 为 `sync: false`，需在 Dashboard 手动填
- `backend/main.py` 启动时会 `load_dotenv(ROOT / ".env")` 读取本地环境变量

---

## 超时与加速优化（避免接口卡死）

接口无原生超时、挂起会一直等到 step 上限。已做如下加固（**均不引入多线程**，避免打崩 akshare / baostock）：

| 环境变量 | 默认值 | 作用 |
|----------|--------|------|
| `AK_API_TIMEOUT` | `30`（秒） | CCTV 主数据调用（`stock_info_a_code_name`、回测 `stock_zh_a_hist`）超时即抛异常 → 本地回退 |
| `CCTV_IDLE_TIMEOUT` | `900`（秒） | 本地编排中 CCTV 阶段无输出超过该值则自动终止跳过 |
| `CCTV_EXTRA_NEWS_TIMEOUT` | `8`（秒） | 补充资讯源（cls/sina）单源超时 |
| `BS_REQUEST_TIMEOUT_SECONDS` | `15`（秒） | 相对强弱 baostock 请求超时 |
| `RELATIVITY_MAX_WORKERS` | `1` | 相对强弱评估并发数（云端固定单线程，避免限流） |

> 设计原则：**少调用（缓存/预热）+ 不卡死（超时快速失败）**，而非并发加速。

---

## 共享内核 smcore/

为消除"命令行 + 看板两套实现结果不一致"，核心逻辑统一到 `smcore/`：

- `smcore/indicators/boll.py`：Boll 带计算与信号判定（唯一实现）
- `smcore/data/kline.py`：前复权日 K 线获取 + 文件缓存（akshare / baostock 双后端）
- `smcore/data/quote.py`：全市场实时报价（双层缓存，5 分钟 TTL）
- `smcore/strategy/fusion.py`：四策略信号融合 → 今日操作清单
- `smcore/strategy/allocation.py`：仓位分配
- `smcore/storage/cos.py`：腾讯云 COS 上传/下载
- `smcore/storage/trades_repo.py`：Supabase 交易记录读写
- `smcore/notify/email.py`：SMTP 邮件推送
- `smcore/config/defaults.py`：全项目默认参数

**关键参数（统一后）**：Boll `window=20, k=1.645, near_ratio=1.015`；复权前复权 `qfq`；股价上限 `30`；财报期 <5 月用去年三季报。

---

## 配置 Secrets

在 GitHub 仓库 **Settings → Secrets and variables → Actions** 添加：

| Secret 名 | 值 | 用途 |
|-----------|---|------|
| `SUPABASE_URL` | `https://xxxx.supabase.co` | Supabase 缓存层 |
| `SUPABASE_KEY` | `eyJ...` | Supabase anon/service key |
| `SMTP_HOST` | `smtp.qq.com` | 邮件推送 |
| `SMTP_PORT` | `465` | |
| `SMTP_USER` | 你的邮箱 | |
| `SMTP_PASS` | 邮箱授权码 | |
| `SMTP_TO` | 收件邮箱 | |
| `COS_SECRET_ID` | AKIDxxx | 操作清单存档（可选） |
| `COS_SECRET_KEY` | xxx | |
| `COS_BUCKET` | stocks-master-1250000000 | |
| `COS_REGION` | ap-guangzhou | |

未配邮件/COS 仍可跑选股（结果在 Actions Artifacts 与 Supabase 中）；`SUPABASE_*` 建议必配，否则缓存层退化为基础数据本地回退。

---

## 推送通知

**仅邮件推送**（企业微信已移除）。未配置则跳过推送，结果仍在 `stock_data/` 与 Supabase 生成。

---

## 可视化界面（Render Web）

React + FastAPI，Render 部署单一 Web 服务即可：

```bash
# 本地开发
E:/Anaconda/python.exe app.py          # 后端
cd frontend && npm run dev             # 前端 http://localhost:5173
```

访问部署后的 Web 地址，已接入：看板（指数 / 热度 / 宏观）、选股 / 分析 / 持仓 / 回测独立 API、布林扫描 / 策略融合 / 交易录入 / 回测触发等操作。

---

## 多策略融合

日报由市场状态动态生成：

1. **Boll 主策略**：技术面买卖点
2. **题材策略**：资金共识方向
3. **相对强弱**：风格筛选（顺风不弱、逆风抗跌）
4. **CCTV / 新闻**：叙事与预期差
5. **宏观风险**：仓位约束（先控回撤，再追求收益）

融合输出 `Daily-Action-List-YYYYMMDD.csv`（今日操作清单，含止损 / 止盈 / 建议买入价）。

---

## 回测

- 信号样本回测：`python Frequently-Used-Program/backtest_signal_picks.py --signals-glob "stock_data/Stock-Selection-Boll-*.csv" --top-n 10 --hold-days 5`
- 真实成交回测：`python Frequently-Used-Program/backtest_tradebook.py --trades-csv stock_data/my_trades.csv`

---

## 常见问题

**Q：Supabase 接上了吗？怎么验证？**
A：GitHub Actions 运行日志搜 `strategy_cache` 或 `Supabase`：看到 `已从云端恢复` / `已上传云端` 即接上；看到 `Supabase 未配置，跳过` 则需检查 Secrets 或 `.env`。本地可用 `python scripts/strategy_cache.py pull cctv YYYYMMDD` 验证。

**Q：选股老超时？**
A：单 step 已设 20–25min。若仍超时，多为接口挂起——已通过 `AK_API_TIMEOUT` / `CCTV_IDLE_TIMEOUT` 快速失败回退。看日志确认卡在哪个策略的网络调用。

**Q：多线程会打崩接口吗？**
A：云端相对强弱已固定 `--max-workers 1`（单线程）；CCTV 补充资讯源用单任务超时（非并发）。请勿随意调高并发数，易触发 akshare / baostock 限流。

**Q：可视化打不开？**
A：开发时先确认后端 `app.py` 已启动、前端 `npm run dev` 在 5173；生产环境确认 `frontend/dist` 已构建且由 FastAPI 托管。

**Q：全云端要付费吗？**
A：GitHub Actions 私有仓库 + COS 免费额度内，约 0 元/月。Render 免费版有休眠限制，按需升级。

---

## 参考文档

- `SETUP_GUIDE.md` —— 从零完整配置（注：其 Secrets 表待补充 `SUPABASE_*`）
- `DEPLOY_RENDER.md` —— Render Free 部署（当前主方案）
