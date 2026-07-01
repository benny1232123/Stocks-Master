# Stocks-Master

A 股多策略选股系统 — Boll 布林带为主，融合题材/相对强弱/CCTV 舆情，支持本地运行与全云端自动运行。

## 快速开始

### 本地运行

```bash
# 1. 安装依赖
pip install -r requirements.txt

# 2. 配置邮件推送（可选）
# 在 Frequently-Used-Program/.env 或环境变量中设置 SMTP_* 

# 3. 启动可视化界面
python streamlit_app.py
# 或
scripts\start-boll-visualizer.bat

# 4. 命令行选股
python Frequently-Used-Program/auto_notify_boll.py
```

### 全云端运行（不开机也跑）

详见 `DEPLOY_CLOUD.md`，一分钟配置：

1. Fork/Clone 本仓库到你的 GitHub
2. 在 GitHub Secrets 配置 `SMTP_*` 和 `COS_*`
3. 启用 GitHub Actions（工作日 21:30 自动选股 + 邮件推送）

---

## 目录结构

```
Stocks-Master/
├── smcore/                  # 共享内核（两条主线唯一真相源）
│   ├── indicators/boll.py   #   Boll 计算（唯一实现）
│   ├── data/                #   K线/行情/指数数据获取
│   ├── notify/email.py      #   邮件推送（唯一推送渠道）
│   ├── strategy/fusion.py   #   信号融合（四策略→操作清单）
│   ├── portfolio/pnl.py     #   持仓盈亏计算
│   └── scheduler/           #   定时任务引擎
├── Frequently-Used-Program/ # 主程序脚本
│   ├── auto_notify_boll.py  #   命令行入口（选股+推送巨石，逐步重构中）
│   ├── Stock-Selection-Boll.py      # Boll 选股独立脚本
│   └── boll-visualizer/     #   Streamlit 可视化前端
├── streamlit_app.py         # 可视化启动入口
├── run_daemon.py            # 24h 本地守护进程
├── .github/workflows/       # GitHub Actions 云端选股
└── stock_data/              # 结果输出目录
```

---

## 共享内核 smcore/

为消除"命令行 + 可视化两套实现结果不一致"的问题，所有核心逻辑统一到 `smcore/`。

| 模块 | 职责 |
|------|------|
| `smcore/indicators/boll.py` | Boll 带计算与信号判定（唯一实现） |
| `smcore/data/kline.py` | 前复权日 K 线获取 + 文件缓存（支持 baostock/akshare 双后端） |
| `smcore/data/quote.py` | 全市场实时报价（双层缓存，5 分钟 TTL） |
| `smcore/notify/email.py` | SMTP 邮件推送（唯一推送渠道） |
| `smcore/strategy/fusion.py` | 四策略信号融合 → 今日操作清单 |
| `smcore/strategy/allocation.py` | 仓位分配 |
| `smcore/portfolio/pnl.py` | 持仓盈亏计算 |
| `smcore/scheduler/engine.py` | 纯标准库定时调度器 |
| `smcore/storage/cos.py` | 腾讯云 COS 上传/下载 |
| `smcore/config/defaults.py` | 全项目默认参数 |

**关键参数（统一后）**：

- Boll: `window=20`, `k=1.645`, `near_ratio=1.015`
- 复权: 前复权 `qfq`（`adjustflag=2`）
- 股价上限: `30`
- 财报期 <5月: 用去年三季报(0930)

---

## 推送通知

**仅邮件推送**（企业微信已移除）：

| 环境变量 | 说明 |
|----------|------|
| `SMTP_HOST` | SMTP 服务器（如 `smtp.qq.com`） |
| `SMTP_PORT` | 端口（通常 465/587） |
| `SMTP_USER` | 发件邮箱 |
| `SMTP_PASS` | 邮箱授权码 |
| `SMTP_TO` | 收件邮箱 |

未配置则跳过推送，选股结果仍在 `stock_data/` 生成。

---

## 可视化界面

Streamlit 多策略选股界面：

```bash
# 方式一：直接启动（推荐）
python streamlit_app.py

# 方式二：批处理脚本（自动找空闲端口）
scripts\start-boll-visualizer.bat
```

访问 `http://localhost:8520`，功能包括：

- **策略选股**：Boll / 相对强弱 / 央视新闻 / 短线题材，Tab 切换
- **交易录入**：记录买卖操作
- **持仓总览**：实时盈亏（依赖 `stock_data/portfolio.json`）
- **交易历史**：历史成交记录

---

## 后台守护进程（本地 24h）

Streamlit 关了就不跑，守护进程独立运行：

```bash
python run_daemon.py                # 前台运行
python run_daemon.py --once daily   # 只跑一次选股（调试）
python run_daemon.py --status       # 查看任务状态
```

| 任务 | 时间 | 说明 |
|------|------|------|
| 每日选股 | 工作日 21:30 | 选股 + 邮件推送 |

---

## 全云端运行

不想开机也能跑？完全不用本地电脑：

### GitHub Actions（选股 + 推送）

- 触发：工作日 21:30 北京时间（cron `30 13 * * 1-5` UTC）
- 后端：akshare（不依赖 baostock 网络）
- 输出：邮件推送 + COS 上传操作清单
- 费用：**0 元/月**（GitHub Actions 免费额度）

配置详见 `SETUP_GUIDE.md`。

---

## 多策略融合

日报不是固定模板，而是由市场状态动态生成：

1. **Boll 主策略**：技术面买点/卖点
2. **题材策略**：资金共识方向
3. **相对强弱**：风格筛选（顺风不弱、逆风抗跌）
4. **CCTV/新闻**：叙事与预期差
5. **宏观风险**：仓位约束（先控回撤，再追求收益）

融合结果输出为 `Daily-Action-List-YYYYMMDD.csv`（今日操作清单），含止损/止盈/建议买入价。

---

## 回测

两套回测路径：

### 信号样本回测（验证策略统计优势）

```bash
python Frequently-Used-Program/backtest_signal_picks.py \
  --signals-glob "stock_data/Stock-Selection-Boll-*.csv" \
  --top-n 10 --hold-days 5
```

### 真实成交回测（验证执行质量）

```bash
python Frequently-Used-Program/backtest_tradebook.py \
  --trades-csv stock_data/my_trades.csv
```

也可用界面：`scripts\start-backtest-center.bat`

---

## 依赖

核心依赖：`akshare`, `pandas`, `streamlit`, `requests`, `baostock`

完整列表见 `requirements.txt`。

云端运行额外需要：`cos-python-sdk-v5`（COS 上传用）

---

## 常见问题

**Q：可视化打不开？**
A：不要用 `streamlit run streamlit_app.py`。正确方式是 `python streamlit_app.py`（脚本会自动启动 streamlit）。或直接用 `scripts\start-boll-visualizer.bat`。

**Q：选股结果不可信？**
A：检查是否统一用前复权（`qfq`）。本项目已统一，若发现不一致请提 issue。

**Q：云端运行需要付费吗？**
A：不需要。GitHub Actions 在免费额度内，0 元/月。

**Q：邮件推送失败？**
A：检查 SMTP 配置。QQ 邮箱需要用"授权码"而非登录密码。腾讯云/企业邮箱可能需要开启 SMTP 服务。

---

详细配置见：
- `SETUP_GUIDE.md` — 从零开始完整配置
- `DEPLOY_CLOUD.md` — 全云端部署
- `REFACTOR_PROGRESS.md` — 重构进度
