# Stocks-Master

A 股多策略选股系统 —— 以 Boll 布林带为主，融合题材热度 / 相对强弱 / CCTV 舆情 / 动量，支持**本地运行**与**全云端自动运行**。

---

## 架构总览（云端为主）

```text
┌─────────────────────────────────────────────────────────────┐
│  GitHub Actions（免费，美国服务器）                          │
│  工作日 16:30（北京时间，cron `30 8 * * 1-5` UTC）自动触发   │
│    ├─ 策略1 Boll 布林带扫描      (timeout 25min)             │
│    ├─ 策略2 题材热度             (timeout 20min)             │
│    ├─ 策略3 CCTV 板块舆情        (timeout 25min)             │
│    ├─ 策略4 相对强弱             (timeout 20min, 单线程)      │
│    ├─ 策略5 动量/相对强度        (timeout 25min, 东财-free)   │
│    └─ 融合→操作清单→看板预热→前向回测（自动串接）            │
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

## 每日选股流程（云端自动化）

每个工作日 16:30（北京时间）GitHub Actions 自动跑完下面这条链路，全程不开电脑、0 元/月。看懂这一节，就知道"它每天是怎么选股的"。

### 一、五个独立策略各自出候选池（并行跑，先查缓存）

| 步骤 | 策略 | 选股逻辑（一句话） | 产物 |
|---|---|---|---|
| 1 | **Boll 布林带**（主策略） | 股价触及/跌破 Boll 下轨的超卖均值回归票，给建议买入价 | `Stock-Selection-Boll-*.csv` |
| 2 | **题材热度** | 近期换手/资金共识最强的题材方向里的活跃票 | `Stock-Selection-Ashare-Theme-Turnover-*.csv` |
| 3 | **CCTV 板块舆情** | 新闻/舆论热度高的板块股票池（叙事与预期差） | `CCTV-Sector-Stock-Pool-*.csv` |
| 4 | **相对强弱** | 顺风不弱、逆风抗跌的风格筛选（单线程防限流） | `Stock-Selection-Relativity-*.csv` |
| 5 | **动量/相对强度** | 近 20 日上涨、MA20 上行的强势股（与 Boll 超卖互补） | `Stock-Selection-Momentum-*.csv` |

每个策略先 `strategy_cache.py pull` 查 Supabase 当天缓存 → 命中就跳过（省接口额度）；没命中才跑 → 跑完 `push` 回 Supabase。任一策略接口挂掉/超时（`continue-on-error`）也不影响其他策略，整体不崩。

### 二、信号融合 → 一份今日操作清单（`fusion.fuse_signals`）

读取上面五个候选池，合并去重、打分、算止损止盈、分配仓位，输出 `Daily-Action-List-YYYYMMDD.csv`。这一步是选股真正的"决策中枢"，顺次过六道关：

1. **综合评分（排序与加分，动态权重）**：命中策略即按**当前市场状态**对应的权重得分——趋势上行时动量提权至 38（顺势为王）、Boll 降至 32；下行防御时 Boll 提至 40、题材降至 8；震荡轮动沿用默认（`Boll 45 / Momentum 20 / Theme 15 / Relativity 15 / CCTV 10`）。多命中一个策略额外 +5（多策略共振加分）。评分决定最终排序与仓位。
2. **趋势闸门（择时防御）**：用**沪深300 的 MA60 位置**判市场状态（上行 / 下行防御 / 震荡）。若判定"下行防御"，直接剔除**纯均值回归票**（只命中 Boll/Relativity、不含顺势策略）——其"次日买、持有 10 日"在弱市必亏。
3. **相对强度过滤（alpha 质量门，核心）**：对每只候选算其**近 20 日收益 vs 沪深300 同期收益**，跑输大盘超过 `RS_TOL=3%` 的票直接剔除（动量票豁免，因其本就要求 ret20>0 且 MA20 上行）。—— 直击"大盘涨、个股仍跑输"的根因。
4. **流动性门槛（成交质量门）**：信号日成交额 < **¥1 亿** 的票剔除（流动性差→难出场、滑点大、庄股陷阱）。头对头测量为甜点（平均收益 +0.92%、胜率 +5.1%）。
5. **趋势守卫（尾部风险）**：价格低于 MA20 超 12% 的破位/下降通道股剔除（防自由落体巨亏）。
6. **板块轮动 + 单板块集中度控制（分散风险）**：用候选股近 20 日收益聚合出「板块动量」，对强势板块候选给小幅评分加成（确认型轮动，零额外联网）；最终入选清单中**单板块最多保留 `max_per_sector=5` 只**，强制跨行业分散，避免单一行业黑天鹅把组合拖垮。板块映射（`stock_data/sector_map.json`）由 `scripts/build_sector_map.py` 经 baostock 本地一次性抓取、提交仓库，云端直接读缓存（无需在线拉行业数据）。

过完六关后，按命中策略中权重最高者分配仓位（单票上限 30%），止损价=Boll 下轨、止盈价=Boll 上轨，输出 ≤15 只的最终清单。日报里会逐条标注每道关剔除的数量。

### 三、看板预热 + 每日前向回测（自动验证）

- **看板预热**：把指数/市场热度/选股结果刷进缓存，Render 看板即日可见。
- **前向信号回测**（`scripts/daily_backtest.py`）：对近 30 天每一份历史操作清单，**从信号日次日开盘买入、持有 10 日**，回测真实表现。回测输入会**内联复用与生产融合完全一致的 RS + 流动性过滤**（避免旧清单未过滤导致数字失真），并按**综合评分加权分配仓位**（`size_by='综合评分'`，确定性高的票多给仓位）。出场规则：Boll 上轨止盈 / 固定 +6% 止盈 / 5% 移动止盈 / 收盘跌破 MA60 趋势破位即走 / −8% 硬止损（缺口感知）/ 满 10 日兜底平仓，并计入真实交易成本（佣金万 2.5 + 印花税千 0.5）。

### 四、结果落库与推送

- 操作清单 + 各策略结果 + 回测结果 `git commit` 回仓库（供历史追溯），并上传 COS（若配置）。
- 看板接入沪深300 基准对比，可直观看到策略**跑赢/跑输大盘多少**。

> 一句话总结：**五策略广撒网 → 六道质量关层层过滤 → 评分加权出清单 → 次日开盘买入持有 10 日 → 每日回测复盘**。选股的核心杠杆不在"加策略"，而在"剔除弱 alpha"（RS 过滤 + 流动性门槛 + 置信度加权 + 板块分散）。

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
├── Frequently-Used-Program/    # 本地编排与维护脚本（选股策略本身已迁入 smcore）
│   ├── auto_notify_boll.py     #   本地编排入口（含 CCTV idle 超时看门狗；子进程调用 smcore.strategies.*）
│
├── smcore/
│   ├── strategies/              # 多策略选股模块（一等公民）
│   │   ├── boll.py              #   Boll 多因子选股（策略1，run_boll）
│   │   ├── theme.py             #   题材换手策略（策略2，run_theme）
│   │   ├── relativity.py        #   相对强弱策略（策略4，run_relativity；--max-workers 控制并发）
│   │   ├── cctv.py              #   CCTV 舆论板块策略（策略3，run_cctv；含 API 超时 + 本地回退）
│   │   └── momentum.py          #   动量/相对强度策略（策略5，run_momentum；东财-free）
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

- **触发**：工作日 16:30 北京时间（cron `30 8 * * 1-5` UTC，约 5–15 分钟延迟）
- **K 线后端**：新浪 `stock_zh_a_daily` / baostock 双后端（**已全面去除东财依赖**，见下「数据链路去东财」）

> **数据链路去东财（2026-07-11）**：原动量策略快照用东财 `stock_zh_a_spot_em`、CCTV 自评估用东财 `stock_zh_a_hist`，均已替换为**新浪 `stock_zh_a_spot` + `fetch_daily_k`（baostock/新浪后端）**，沙箱/海外均可达、无需东财。动量快照偶发空加 3 次重试；`MOMENTUM_USE_EASTMONEY=1` 仅作兜底（15s 线程超时防挂）。
- **流程**：每个策略先 `strategy_cache.py pull` 查缓存 → 命中则 `exit 0` 跳过 → 否则跑选股 → `push` 上传；五个策略跑完后依次执行**融合生成操作清单 → 上传 COS → 看板预热 → 每日前向回测**
- **超时**（单 step）：Boll 25min / 题材 20min / CCTV 25min / 相对强弱 20min / 动量 25min / 回测 40min；总 job 90min
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
- `smcore/strategy/fusion.py`：五策略信号融合 → 今日操作清单（含趋势守卫过滤）
- `smcore/strategy/allocation.py`：仓位分配（三 regime：趋势上行 / 下行防御 / 震荡轮动）
- `smcore/backtest/engine.py`：前向信号回测引擎（硬止损 + 真实交易成本 + MA60 破位出场 + 移动止盈）
- `smcore/strategies/momentum.py`：动量/相对强度选股（策略5，东财-free）
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

1. **Boll 主策略**：技术面买卖点（超卖均值回归，主策略）
2. **题材策略**：资金共识方向
3. **相对强弱**：风格筛选（顺风不弱、逆风抗跌）
4. **CCTV / 新闻**：叙事与预期差
5. **动量/相对强度**：买中期上升趋势的强势股（与 Boll 超卖互补）
6. **宏观风险**：仓位约束（先控回撤，再追求收益）

各策略在综合评分中的权重（`get_regime_scores(regime)`，**根据市场状态动态选取**，与仓位分配联动；以下为「震荡轮动」默认值）：
- 趋势上行：Boll 32 / Momentum 38 / Theme 12 / Relativity 10 / CCTV 8（顺势为王，动量提权）
- 下行防御：Boll 40 / Momentum 28 / Relativity 17 / Theme 8 / CCTV 7（防守优先，题材严控）
- 震荡轮动（默认）：**Boll 45 / Momentum 20 / Theme 15 / Relativity 15 / CCTV 10**（CCTV 噪声大降权，Relativity 实测最差砍权）

融合输出 `Daily-Action-List-YYYYMMDD.csv`（今日操作清单，含止损 / 止盈 / 建议买入价）。

### 多维市场仪表盘（市场状态判定升级）

所有"市场自适应"的共同输入。此前只看沪深300 的 MA60 位置（`_detect_market_regime`，三态非牛即熊），现升级为 `smcore/strategy/market.py` 的 `compute_market_profile()`，综合四个维度：

- **趋势**：沪深300 价格 vs MA20 / MA60，MA60 斜率方向
- **波动率**：沪深300 近 20 日年化波动率，及其在近 250 日分布中的分位（low/mid/high）
- **宽度**：沪深300 / 中证500 / 中证1000 近 20 日收益的一致性（三大宽基同步涨=健康牛市；只有沪深300 涨、中小票跌=宽度失真）
- **量能**：沪深300 近 5 日均量 / 近 60 日均量

合成结果产出：向后兼容的三态 `regime`（趋势上行 / 下行防御 / 震荡轮动）、连续强度 `regime_strength`（0–1）、`volatility_level`、`breadth_score`、`activity_ratio`。**关键改进**：高波动且无明确上行时直接判为"下行防御"（避险），趋势上行还需宽度确认（避免指数失真导致的假牛市）。数据源 baostock 主源 + akshare 兜底，东财-free；任一指数拉取失败均保守降级，pipeline 不崩。日报以 `🌡️ 市场仪表盘：...` 展示快照。

### 趋势闸门（市场择时防御）
融合时由**多维市场仪表盘**判定市场状态（见上），替代此前的硬编码「震荡轮动」与单一 MA60 判断：
- **下行防御**（价格 < MA60 且 MA60 走平/下行）时，驱动 `build_strategy_allocation` 走防御 regime，**并直接剔除「纯均值回归」候选**（仅命中 Boll / Relativity、不含顺势策略）——其「次日买、持有 10 日」在弱市必亏；保留 Momentum / Theme / CCTV 等顺势策略。
- 网络/数据不足时回退「震荡轮动」（不触发闸门），保证 pipeline 不崩。
- 日报标注市场状态与闸门触发情况。

> 实测边界（2026-07-11 复盘）：0606–0626 亏损窗口沪深300 实际处于**趋势上行**，趋势闸门在该窗口几乎不触发——说明那一轮亏损根因是**策略 alpha 弱（大盘涨、个股策略仍亏）而非市场弱**，趋势闸门是防御层而非该窗口的灵药。真正杠杆在 alpha 质量，见下。

### 相对强度过滤（alpha 质量门）

针对「大盘涨、选出的超卖票仍跑输」这把根因：融合时对每只候选计算其**近 20 日收益 vs 沪深300 同期收益**，跑输大盘超过 `RS_TOL` 的候选直接剔除——除非该票本身是**动量票**（`Momentum` 命中，已要求 ret20>0 且 MA20 上行，豁免以免误杀强势股）。

- 实现：`smcore/strategy/fusion.py` 的 `_passes_relative_strength_filter` + `_index_20d_return`（沪深300 收益缓存，`_get_hs300_close` 以 **baostock 为主源 + akshare 兜底**，东财-free），`fuse_signals(..., relative_strength_filter=True)` 默认开启。
- **动态阈值**：`RS_TOL` 随市浮动（`_dynamic_thresholds`）——趋势上行放宽至 **5%**（让更多顺势票过、不误杀强势股）、下行防御收紧至 **2%**（只留最强）、震荡轮动用默认 **3%**。
- 复用候选 K 线拉取（与止损止盈计算同一次联网），不额外开销；数据缺失保守保留，pipeline 不崩。
- 日报标注剔除数量（`📉 相对强度过滤剔除 N 只跑输大盘超 X% 的票`）。
- 验证：`scripts/measure_rs_filter.py` 用改进引擎对历史融合候选「不过滤 vs 过滤」头对头重测，扫 `RS_TOL=0/0.03/0.05` 选最优。

> 与趋势闸门的区别：趋势闸门是「市场弱时不买均值回归」的择时防御层；相对强度过滤是「无论市场强弱，剔除个股 alpha 弱于大盘的票」的质量门——后者直接打击那一轮亏损的根因（大盘涨而个股亏）。

### 流动性门槛（成交质量门）

在相对强度过滤之上，进一步剔除**信号日成交额过低**的票（流动性差 → 难出场、滑点大、庄股陷阱）。头对头测量（`scripts/measure_signal_quality.py`）表明门槛 **¥1 亿**为甜点：相对 RS 基线平均收益 +0.92%、胜率 +5.1%、盈亏比 +0.43，优于相对阈值（同信号日前 50%）与更松的 ¥5000 万；相对强度排名（RSR）反而有害，未采用。

- 实现：`smcore/strategy/fusion.py` 新增 `MIN_SIGNAL_AMOUNT = 1e8`，`_compute_boll_levels` 顺带返回信号日 `amount`（复用已拉 K 线），`fuse_signals(..., min_signal_amount=1e8, dynamic_thresholds=True)` 默认开启动态浮动；`amount` 为 `None` 时保守放行（数据源故障不误杀整份清单）。
- **动态门槛**：流动性门槛随市浮动（`_dynamic_thresholds`）——趋势上行降至 **¥7000万**（放量市流动性充裕）、下行防御抬高至 **¥2亿**（只留最易出场的最强票）、震荡轮动用默认 **¥1亿**。
- 日报标注剔除数量（`💧 流动性门槛剔除 N 只信号日成交额 < ¥1亿 的票`）。
- 量级已核对：akshare 与 baostock 的 `amount` 单位均为「元」、数值一致，云端（`KLINE_BACKEND=akshare`）与测量可比。

### 置信度加权仓位（资金分配门）
在「RS + 流动性」已过滤的宇宙上，回测引擎对每日候选**按综合评分加权分配仓位**（高评分=多策略共振=确定性高→多给仓位），而非等权。头对头测量（`scripts/measure_position_sizing.py`）表明：相对等权，组合总收益 +1.02pp、夏普 +0.71、回撤还收窄 0.5pp，三维全改善；按「跑赢大盘幅度」仅小幅正、按「成交额」反而大亏（过度集中大盘股），故固定采用综合评分。

- 落地：`run_forward_signal_backtest(..., size_by='综合评分')`；`scripts/daily_backtest.py` 默认开启，环境变量 `BACKTEST_SIZE_BY=""` 可回退等权；summary 记 `size_mode` 便于追溯。

### 板块轮动 + 单板块集中度控制（分散风险）

在「评分排序」之后、「截断到 ≤15 只」之前，插入两层板块逻辑（`smcore/strategy/sectors.py`）：

- **板块轮动（确认型，零额外联网）**：用本轮候选股的近 20 日收益（`ret20`，融合拉 K 线时已算过）按行业聚合中位数，得到「板块动量」。领先板块中位动量给 **+6 分**、落后板块 **−6 分**、中间 0 分（线性插值，幅度由 `SECTOR_MOMENTUM_BONUS` 控制）。候选数 < 20 时统计意义不足，自动不加成。本质是「在本轮已筛候选内确认强势板块」，而非全市场轮动信号（全市场轮动需板块指数 20 日收益，云端拿不到东财板块数据，故未做）。
- **单板块集中度控制**：最终入选清单按评分降序扫描，**同一板块最多保留 `max_per_sector=5` 只**（`apply_sector_cap`），强制跨行业分散；极端集中导致不足 15 只时再放宽补满。未映射（"未知"）的股票不计入任何板块上限，避免被误砍。

板块映射来源：证监会行业分类，由 **baostock `query_stock_industry` 按需实时拉取**——融合时只对当天进入候选池的股票（几十只，单只 ~1s，整轮 ~1–2 分钟，对 16:30 夜跑完全可接受）查询行业，写回 `stock_data/sector_map.json` 缓存，云端每夜自动累积、越跑越全（不再依赖"预先构建全市场 5000+ 只映射"那次 ~80 分钟、易在会话切换时中断的批量抓取）。若 baostock 不可达则静默降级为仅用已有缓存 / 空映射，融合层安全跳过板块逻辑（fail-soft），pipeline 不崩。可选的全市场预热：`python scripts/build_sector_map.py`（断点续跑）。可用 `SECTOR_MAP_ONDEMAND=0` 关闭按需拉取、仅用缓存。

- 实现：`fuse_signals(..., sector_cap=True, max_per_sector=5)` 默认开启；日报标注 `🏭 板块轮动+集中度：最终 N 只覆盖 M 个行业（单板块上限 5）`。

### 波动率自适应风控（动态止损 + 总仓位）

回测引擎（`smcore/backtest/engine.py` 的 `run_forward_signal_backtest`）在「多维市场仪表盘」基础上做两层风控缩放（`scripts/daily_backtest.py` 默认开启，环境变量 `VOL_SCALED_STOP` / `VOL_POS_SCALE` 可关）：

- **逐只动态止损**：个股近 20 日波动率 `vol20`（由 `fusion._compute_boll_levels` 返回）→ 止损比例 `clamp(8 × vol20, 6%, 15%)`。高波动股给更宽止损避免被洗、低波动股给更紧止损；无 `vol20` 数据时回退引擎全局 −8%。实现为 `stop_pct` 逐行传入引擎，硬止损（缺口感知）与收盘止损均按个股比例触发。
- **总仓位随市场波动率缩放**：据 `market.compute_market_profile().volatility_level` 缩放总投入（`low=1.0` / `mid=0.85` / `high=0.6`），高波动市留现金真正降低组合暴露，而非等比缩放后满仓。

- 日报 summary 记 `vol_mode` / `capital_scale` 便于追溯；验证：`VOL_SCALED_STOP=0` / `VOL_POS_SCALE=0` 可分别对比固定止损与满仓基线。

---

## 回测与策略验证

### 前向信号回测（每日自动回测）
次日开盘买入、持有 N 日；自写循环（非 backtrader），已修正为诚实口径：
- **真实交易成本**：佣金万 2.5（最低 5 元）+ 卖出印花税千 0.5；
- **gap-aware 硬止损**：盘中触及 −8% 即离场（封顶亏损 ≈8%，不空等收盘）；
- **MA60 趋势破位出场**：收盘跌破 60 日线即走（避免弱势里死扛）；
- **移动止盈 5% + 固定止盈 6%** 锁利；
- **汇总口径**：仅聚合已走完批次（`num_trades>0`），胜率按单笔交易统计。

运行（默认对近 30 天每一份 `Daily-Action-List-*.csv` 跑前向回测，CI 即如此调用）：
```bash
# 每日自动回测（GitHub Actions daily-pick 末尾调用）
HOLD_DAYS=10 LOOKBACK_DAYS=30 python scripts/daily_backtest.py
```

关键行为（与生产融合一致，避免旧清单数字失真）：
- **内联 RS + 流动性过滤**：回测前先按评分粗取前 100 只预筛，再逐一复用 `fusion` 的 RS 过滤（`RS_TOL=3%`）与流动性门槛（¥1 亿）剔除弱 alpha 票，确保回测输入 = 当天新跑融合的输出；可用 `BACKTEST_INLINE_FILTER=0` / `BACKTEST_MIN_AMOUNT=...` 调整。
- **置信度加权仓位**：按 `综合评分` 加权分配资金（`size_by='综合评分'`，确定性高=多策略共振=多给仓位）；`BACKTEST_SIZE_BY=""` 回退等权。
- 出场规则：Boll 上轨止盈 / 固定 +6% / 移动止盈 5% / 收盘跌破 MA60 / −8% 硬止损（缺口感知）/ 满 `HOLD_DAYS` 兜底；含真实交易成本。

### 策略 edge 量化测量（定权重用）
`scripts/measure_strategy_edge.py` 读历史 `Daily-Action-List-*.csv`，按来源策略拆桶，用改进引擎跑前向 10 日回测，输出各策略收益 / 胜率 / 回撤 / 夏普 + BASELINE 对照。据其结果**数据驱动定稿**各 regime 下的评分权重（`_REGIME_STRATEGY_SCORE`）与 `allocation` 仓位权重（如 Relativity 实测最差 → 砍权，Boll 最抗跌 → 提权；趋势上行时动量提权、下行防御时题材降权）。

> 实测参考（窗口 2026-06-10~06-26，硬止损 + 真实成本）：全样本 BASELINE −5.09%、Boll −4.92%、Relativity −13.86%（MA60 破位对其单策略不利，已砍权缓解）。当前改进属「少亏」级，根因在信号 alpha 弱；**相对强度过滤（剔除跑输大盘的票）** 是下一步真正杠杆，由 `scripts/measure_rs_filter.py` 量化验证。

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
