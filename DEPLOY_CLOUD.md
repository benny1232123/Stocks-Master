# 全云端部署指南

让选股 + 预警全部在云端跑，电脑关机也行。

## 架构总览

```
GitHub Actions（免费，美国服务器）
  └─ 工作日 21:30 自动触发
     ├─ 跑选股主流程（K线走 akshare 东财接口，不依赖 baostock）
     ├─ 生成操作清单
     └─ 上传操作清单到 COS
                    ↓
腾讯云 SCF（免费，国内服务器）
  └─ 盘中每 10 分钟
     ├─ 从 COS 读操作清单
     ├─ 拉新浪实时行情
     └─ 触止损/止盈 → 推企微
```

**完全不用开电脑。** 选股在 GitHub，预警在腾讯云，两者通过 COS 传递数据。

## 第一部分：GitHub Actions（选股）

### 1. 推代码到 GitHub

如果还没有 GitHub 仓库：

```bash
git init
git add .
git commit -m " Stocks-Master 全云端"
git remote add origin https://github.com/你的用户名/stocks-master.git
git push -u origin main
```

**重要**：仓库设为**私有**（私有仓库每月 2000 分钟免费额度，选股约 660 分钟/月，够用）。公开仓库会暴露你的策略。

### 2. 配置 GitHub Secrets

在仓库 Settings → Secrets and variables → Actions → New repository secret，添加：

| Secret 名 | 值 | 必填 |
|-----------|---|------|
| WECOM_WEBHOOK_URL | 企微机器人 webhook 地址 | 推送用 |
| SMTP_HOST | smtp.qq.com | 邮件用 |
| SMTP_PORT | 465 | |
| SMTP_USER | 你的邮箱 | |
| SMTP_PASS | 邮箱授权码 | |
| SMTP_TO | 收件邮箱 | |
| COS_SECRET_ID | 腾讯云 API SecretId | COS 用 |
| COS_SECRET_KEY | 腾讯云 API SecretKey | |
| COS_BUCKET | stocks-master-1250000000 | |
| COS_REGION | ap-guangzhou | |

不配推送/邮件的可以跳过对应 Secret，选股仍能跑（结果在 Actions 产物里）。
不配 COS 的可以跳过，但 SCF 预警就读不到操作清单。

### 3. 测试

在仓库 Actions 页面 → "每日选股" → Run workflow → 手动触发。

看日志确认：
- akshare K 线是否拉到（核心，必须通）
- 选股是否出结果
- COS 是否上传成功

**cron 会自动跑**：工作日 UTC 13:30（北京 21:30），可能有 5-15 分钟延迟。

### 4. 下载结果

每次跑完，结果 CSV 在 Actions 页面 → 对应 run → Artifacts 下载。保留 30 天。

---

## 第二部分：腾讯云 SCF（预警）

见 `DEPLOY_SCF.md`。

---

## 限制与风险

| 项 | 说明 |
|----|------|
| GitHub 在美国 | 访问东财/新浪接口有延迟但通常能通；baostock 服务器在中国可能连不上，所以 K 线走 akshare |
| cron 延迟 | GitHub Actions 定时触发有 5-15 分钟延迟，21:30 可能 21:45 才跑 |
| 私有仓库额度 | 2000 分钟/月，选股约 30 分钟 × 22 工作日 = 660 分钟，够用 |
| baostock 基本面 | 巨石的基本面模块（pe/pb）依赖 baostock，云端可能失败，但不影响 Boll 核心信号 |
| COS 依赖 | GitHub Actions 和 SCF 之间通过 COS 传数据，COS 免费额度充足 |

## 故障排查

| 问题 | 排查 |
|------|------|
| Actions 跑超时 | timeout-minutes 设的 30，选股正常 10-20 分钟。超时看日志卡在哪 |
| akshare 报错 | 网络问题，重跑一次。东财接口偶发限流 |
| COS 上传失败 | 检查 COS_* Secrets 是否配对 |
| SCF 预警无数据 | 检查 GitHub Actions 是否成功上传了操作清单到 COS |
| 推送没收到 | 检查 WECOM_WEBHOOK_URL / SMTP_* Secrets |

## 费用

- GitHub Actions：私有仓库 2000 分钟/月免费，够用。**0 元**
- 腾讯云 SCF：100 万次/月免费。**0 元**
- 腾讯云 COS：50GB 存储 + 100 万请求免费。**0 元**
- **总计：0 元/月**
