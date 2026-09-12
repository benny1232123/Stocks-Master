# 📈 Stocks-Master

> **A 股多策略量化选股系统** —— 五策略并行扫描 → 六道质量关过滤 → 动态权重融合 → 信号级回测验证，GitHub Actions 全自动运行，零成本。

[![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)](https://www.python.org/)
[![React](https://img.shields.io/badge/React-18-61DAFB?logo=react)](https://react.dev/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104-green?logo=fastapi)](https://fastapi.tiangolo.com/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

> ⚠️ **免责声明**：本项目仅为量化策略研究与技术演示，所有选股、融合与回测结果**均不构成任何投资建议**。股市有风险，入市需谨慎；任何依据本项目输出进行的交易，后果由使用者自行承担。

---

## 核心特性

| 特性 | 说明 |
|------|------|
| 🔄 **全自动运行** | GitHub Actions 工作日 16:30 自动选股 → 融合 → 回测 → 推送，零人工干预 |
| 📊 **五策略并行** | Boll 布林带 / 题材热度 / CCTV 舆情 / 相对强弱 / 动量，互补覆盖不同市场风格 |
| 🛡️ **六道质量关** | 趋势闸门 → RS 过滤 → 流动性门槛 → 趋势守卫 → 板块轮动 → 集中度控制 |
| 🎯 **动态自适应** | 市场状态（上行/防御/震荡）实时判定，权重/仓位/止损随 regime 动态调整 |
| 📈 **信号级回测** | 每日自动回测近 30 天信号，含真实交易成本 + 缺口感知止损 |
| 💾 **云端缓存** | Supabase 跨环境复用，断点续跑，接口失败自动回退本地 |
| 🌐 **Web 看板** | React + FastAPI，Render 部署，实时展示指数/热度/选股/持仓/回测 |

---

## 系统架构

```
GitHub Actions (免费, 美国服务器)
  ┌─────────────────────────────────────────────────────┐
  │  工作日 16:30 北京时间自动触发                        │
  │    ├─ 策略1 Boll 布林带扫描      (25min)              │
  │    ├─ 策略2 题材热度             (20min)              │
  │    ├─ 策略3 CCTV 板块舆情        (25min)              │
  │    ├─ 策略4 相对强弱             (20min, 单线程)       │
  │    ├─ 策略5 动量/相对强度        (25min, 东财-free)    │
  │    └─ 融合 → 操作清单 → 看板预热 → 前向回测            │
  └─────────────────────────────────────────────────────┘
                          ↓
              Supabase (PostgreSQL, 云端缓存层)
                          ↓
              Render (FastAPI + React Web 看板)
```

---

## 每日选股流程

### 1️⃣ 五策略并行扫描

| 策略 | 选股逻辑 | 产物 |
|------|----------|------|
| **Boll 布林带** | 股价触及/跌破下轨的超卖均值回归票 | `Stock-Selection-Boll-*.csv` |
| **题材热度** | 近期换手/资金共识最强的题材方向活跃票 | `Stock-Selection-Ashare-Theme-Turnover-*.csv` |
| **CCTV 舆情** | 新闻/舆论热度高的板块股票池 | `CCTV-Sector-Stock-Pool-*.csv` |
| **相对强弱** | 顺风不弱、逆风抗跌的风格筛选 | `Stock-Selection-Relativity-*.csv` |
| **动量/相对强度** | 近 20 日上涨、MA20 上行的强势股 | `Stock-Selection-Momentum-*.csv` |

> 每个策略先查 Supabase 缓存 → 命中则跳过 → 未命中才跑 → 跑完回写。任一策略超时/失败不影响其他。

### 2️⃣ 六道质量关过滤

```
候选池 (200+ 只)
    ↓
① 综合评分排序 (动态权重, 多策略共振加分)
    ↓
② 趋势闸门 (下行防御时剔除纯均值回归票)
    ↓
③ 相对强度过滤 (跑输大盘超阈值剔除, 动量票豁免)
    ↓
④ 流动性门槛 (信号日成交额 < ¥1亿剔除)
    ↓
⑤ 趋势守卫 (价格低于 MA20 超12%剔除)
    ↓
⑥ 板块轮动 + 集中度控制 (单板块最多5只)
    ↓
最终清单 (≤15 只) → 次日开盘买入
```

### 3️⃣ 动态权重融合

市场状态实时判定（多维市场仪表盘），权重随 regime 调整：

| 市场状态 | Boll | Momentum | Theme | Relativity | CCTV |
|----------|------|----------|-------|------------|------|
| **趋势上行** | 32 | 38 | 12 | 10 | 8 |
| **下行防御** | 40 | 28 | 8 | 17 | 7 |
| **震荡轮动** | 45 | 20 | 15 | 15 | 10 |

### 4️⃣ 信号级回测验证

- 次日开盘买入，持有 N 日（`HOLD_DAYS=12`，可配置）
- 出场规则：Boll 上轨止盈 / 固定 +6% / 移动止盈 5% / MA60 破位出场 / −8% 硬止损
- 含真实交易成本（佣金万 2.5 + 印花税千 0.5）
- 置信度加权仓位（多策略共振 = 高确定性 = 多给仓位）

---

## 快速开始

### 本地运行

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 安装前端依赖并构建
cd frontend && npm install && npm run build && cd ..

# 3. 配置 .env（SUPABASE_URL / SUPABASE_KEY）
cp .env.example .env
# 编辑 .env 填入你的密钥

# 4. 启动后端 + 前端
stocks-master.bat
# 或分别启动：
#   python app.py          # 后端 http://localhost:8000
#   cd frontend && npm run dev  # 前端 http://localhost:5173

# 5. 命令行选股
python Frequently-Used-Program/auto_notify_boll.py
```

### 全云端运行（零成本）

1. 推代码到 **GitHub 私有仓库**
2. 在 GitHub Secrets 配置 `SUPABASE_URL` / `SUPABASE_KEY`
3. 启用 GitHub Actions（工作日 16:30 自动选股 → 融合 → 回测 → 推送）
4. 在 Render 连接仓库部署 Web 看板

详见 `SETUP_GUIDE.md` 和 `DEPLOY_RENDER.md`。

---

## 目录结构

```
Stocks-Master/
├── smcore/                        # 共享内核
│   ├── indicators/boll.py         #   Boll 带计算
│   ├── data/                      #   K线/行情获取 (akshare/baostock 双后端)
│   ├── strategy/                  #   融合 fusion.py, 仓位 allocation.py
│   ├── backtest/engine.py         #   前向信号回测引擎
│   └── storage/                   #   Supabase 交易记录
│
├── backend/main.py                # FastAPI 后端入口
├── frontend/                      # React 前端 (Web 看板)
├── scripts/
│   ├── strategy_cache.py          # ⭐ Supabase 缓存 pull/push
│   ├── daily_backtest.py          # ⭐ 每日自动回测
│   └── prewarm_dashboard.py       #   看板数据预热
│
├── .github/workflows/
│   └── daily-pick.yml             # ⭐ GitHub Actions 云端选股流水线
├── render.yaml                    # ⭐ Render 部署定义
├── stock_data/                    # 结果输出 + 本地 SQLite 回退
│
├── requirements.txt               # Python 依赖
├── stocks-master.bat              # Windows 一键启动
└── .env                           # 本地密钥 (已 gitignore)
```

---

## 配置

### Secrets（GitHub Actions）

| Secret | 用途 | 必需 |
|--------|------|------|
| `SUPABASE_URL` | Supabase 缓存层 | ✅ |
| `SUPABASE_KEY` | Supabase 匿钥 | ✅ |
| `SMTP_HOST` / `SMTP_PORT` / `SMTP_USER` / `SMTP_PASS` / `SMTP_TO` | 邮件推送 | ❌ |
| `COS_SECRET_ID` / `COS_SECRET_KEY` / `COS_BUCKET` / `COS_REGION` | 操作清单存档 | ❌ |

### 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `KLINE_BACKEND` | `akshare` | K线数据源 (`akshare` / `baostock`) |
| `VOL_SCALED_STOP` | `1` | 波动率自适应止损 |
| `VOL_POS_SCALE` | `1` | 总仓位随波动率缩放 |
| `BACKTEST_SIZE_BY` | `综合评分` | 回测仓位分配方式 |

---

## 技术栈

| 层 | 技术 | 部署 |
|----|------|------|
| 数据层 | akshare / baostock / Supabase | GitHub Actions |
| 策略层 | Python 3.12, numpy, pandas | GitHub Actions |
| 回测层 | 自写循环 (非 backtrader) | GitHub Actions |
| 后端 | FastAPI + uvicorn | Render |
| 前端 | React 18 + Vite | Render |
| 缓存 | Supabase (PostgreSQL) | Supabase Cloud |
| 定时 | GitHub Actions + cron-job.org | GitHub |

---

## 常见问题

**Q：全云端要付费吗？**
A：GitHub Actions 私有仓库 2000 分钟/月免费，选股约 660 分钟/月，**0 元/月**。Render 免费版有休眠限制，按需升级。

**Q：选股老超时？**
A：单策略已设 20-25min 超时，接口挂起会快速失败回退。看日志确认卡在哪个策略的网络调用。

**Q：多线程会打崩接口吗？**
A：相对强弱已固定单线程 (`--max-workers 1`)，避免触发 akshare/baostock 限流。

**Q：如何验证 Supabase 已接上？**
A：GitHub Actions 日志搜 `strategy_cache`，看到 `已从云端恢复` 即接上。本地可用 `python scripts/strategy_cache.py pull cctv YYYYMMDD` 验证。

---

## 参考文档

- [`SETUP_GUIDE.md`](SETUP_GUIDE.md) — 从零完整配置
- [`DEPLOY_RENDER.md`](DEPLOY_RENDER.md) — Render 部署指南
- [`WALK_FORWARD_VALIDATION.md`](WALK_FORWARD_VALIDATION.md) — 滚动回测验证

---

## 🤝 Partner

本项目是 [OrcaRouter](https://www.orcarouter.ai/ref/ref_3a6eb528793d4b9194322) 开源合作伙伴计划成员（AI API 网关 · Zero-token-markup · 200+ 模型统一 endpoint）。

[![Powered by OrcaRouter](https://img.shields.io/badge/Powered%20by-OrcaRouter-FF6A00?style=flat&logo=openai)](https://www.orcarouter.ai/ref/ref_3a6eb528793d4b9194322)
