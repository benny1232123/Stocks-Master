# Stocks-Master 完整配置指南

从零到全云端运行，照着做就行。预计 40-60 分钟。

## 总览：你要配什么

```
┌─────────────────────────────────────────────────────┐
│  GitHub Actions（选股，免费）                         │
│  工作日 21:30 自动跑选股 → 生成操作清单               │
└──────────────────────┬──────────────────────────────┘
                       ↓ 上传操作清单
┌─────────────────────────────────────────────────────┐
│  腾讯云 COS（存数据，免费）                           │
│  存操作清单 CSV，两边传数据用                         │
└──────────────────────┬──────────────────────────────┘
                       ↓ 读取操作清单
┌─────────────────────────────────────────────────────┐
│  腾讯云 SCF（预警，免费）                             │
│  盘中每 10 分钟：读清单 → 拉行情 → 触发推送           │
└──────────────────────┬──────────────────────────────┘
                       ↓ 推送
┌─────────────────────────────────────────────────────┐
│  邮件（接收推送）                                    │
│  日报 + 盘中预警提醒                                  │
└─────────────────────────────────────────────────────┘
```

**推送渠道说明**：
- 邮件：带 CSV 附件，适合存档复盘，盘中预警也通过邮件发送

---

## 第一步：配置邮件推送

### 1. 邮箱（带附件存档）

用 QQ 邮箱最简单（SMTP 稳定）：

1. 登录 QQ 邮箱 → 设置 → 账户
2. 找到「POP3/IMAP/SMTP」→ 开启「SMTP 服务」
3. 按提示用手机发短信获取**授权码**（不是 QQ 密码！）
4. 记下：
   - SMTP_HOST = `smtp.qq.com`
   - SMTP_PORT = `465`
   - SMTP_USER = `你的QQ邮箱@qq.com`
   - SMTP_PASS = `刚才的授权码`
   - SMTP_TO = `收件邮箱`（可以发给自己）

**测试**：在项目根目录跑：
```bash
set SMTP_HOST=smtp.qq.com
set SMTP_PORT=465
set SMTP_USER=你的邮箱@qq.com
set SMTP_PASS=你的授权码
set SMTP_TO=你的邮箱@qq.com
python Frequently-Used-Program/auto_notify_boll.py --test-email-only
```
收到测试邮件就对了。

---

## 第二步：配置腾讯云 COS（存操作清单）

GitHub 和 SCF 之间传数据用，免费。

### 2.1 开通

1. 注册腾讯云账号：https://cloud.tencent.com（用微信扫码就行）
2. 实名认证（个人认证，身份证拍照，5分钟过）
3. 开通对象存储 COS：https://console.cloud.tencent.com/cos
4. 开通云函数 SCF：https://console.cloud.tencent.com/scf

### 2.2 创建存储桶

1. COS 控制台 → 存储桶列表 → 创建存储桶
2. 名称：随便起，如 `stocks-master-1250000000`（系统会加 APPID 后缀）
3. 地域：选离你近的，如 `广州` → 记下地域代码 `ap-guangzhou`
4. 访问权限：选「私有读写」
5. 创建完，记下：
   - **存储桶名**（带 APPID 的完整名字）
   - **地域**（如 ap-guangzhou）

### 2.3 获取 API 密钥

1. 访问 https://console.cloud.tencent.com/cam/capi
2. 点「新建密钥」
3. 记下 **SecretId** 和 **SecretKey**（只显示一次，妥善保存）

现在你手上应该有 4 个 COS 值：
```
COS_SECRET_ID = AKIDxxxxxxxx...
COS_SECRET_KEY = xxxxxxxx...
COS_BUCKET = stocks-master-1250000000
COS_REGION = ap-guangzhou
```

---

## 第三步：推代码到 GitHub（选股用）

### 3.1 建私有仓库

1. 登录 GitHub（没有账号先注册）
2. 右上角 + → New repository
3. 名称：`stocks-master`
4. **必须选 Private（私有）** ← 重要！公开会暴露策略
5. 勾选 Add a README → Create

### 3.2 推代码

在项目根目录（`C:\Users\29408\Desktop\Stocks-Master`）打开 Git Bash：

```bash
git init
git add .
git commit -m "stocks-master 全云端"
git branch -M main
git remote add origin https://github.com/你的用户名/stocks-master.git
git push -u origin main
```

如果没配过 GitHub 身份：
```bash
git config --global user.email "你的邮箱"
git config --global user.name "你的名字"
```

### 3.3 配置 Secrets

在仓库页面 → Settings → Secrets and variables → Actions → New repository secret

逐个添加（值填你自己的）：

| Secret 名 | 值 | 哪来的 |
|-----------|---|--------|
| `SMTP_HOST` | smtp.qq.com | 第1步 |
| `SMTP_PORT` | 465 | |
| `SMTP_USER` | 你的邮箱@qq.com | |
| `SMTP_PASS` | QQ邮箱授权码 | |
| `SMTP_TO` | 收件邮箱 | |
| `COS_SECRET_ID` | AKIDxxx | 第2.3步 |
| `COS_SECRET_KEY` | xxx | |
| `COS_BUCKET` | stocks-master-1250000000 | 第2.2步 |
| `COS_REGION` | ap-guangzhou | |

**不配邮件**：选股照跑，结果在 GitHub Actions 的 Artifacts 里下载

### 3.4 测试选股

1. 仓库页面 → Actions 标签 → 左侧「每日选股」
2. 右边「Run workflow」→ 绿色按钮手动触发
3. 等黄色圆圈变绿色（10-20 分钟）
4. 点进去看日志，确认：
   - akshare K 线拉到了
   - 选股出了结果
   - 操作清单上传 COS 成功
   - 推送成功

**如果失败**：看日志红色部分，常见问题：
- `akshare` 网络超时 → 重跑一次
- COS 上传失败 → 检查 COS_* Secrets 拼写
- 推送失败 → 检查 SMTP Secrets

跑通后，**工作日 21:30 会自动跑**（GitHub cron 有 5-15 分钟延迟，正常）。

---

## 第四步：配置腾讯云 SCF（盘中预警）

### 4.1 打包

在项目根目录：
```bash
python build_scf_package.py
```
生成 `scf_alert_package.zip`（约 46KB）。

### 4.2 创建云函数

1. https://console.cloud.tencent.com/scf → 新建函数
2. 填写：
   - 函数名称：`stocks-master-alert`
   - 运行环境：`Python 3.9`
   - 部署方式：本地上传 zip → 选 `scf_alert_package.zip`
   - 执行方法：`scf_alert.main_handler`
   - 内存：`128 MB`
   - 执行超时：`60` 秒
3. 下一步 → 完成

### 4.3 配置环境变量

函数管理 → 函数配置 → 环境变量，添加：

| Key | Value |
|-----|-------|
| `SMTP_HOST` | smtp.qq.com |
| `SMTP_PORT` | 465 |
| `SMTP_USER` | 你的邮箱@qq.com |
| `SMTP_PASS` | QQ邮箱授权码 |
| `SMTP_TO` | 收件邮箱 |
| `COS_SECRET_ID` | AKIDxxx |
| `COS_SECRET_KEY` | xxx |
| `COS_BUCKET` | stocks-master-1250000000 |
| `COS_REGION` | ap-guangzhou |

### 4.4 配置定时触发器

触发器管理 → 创建触发器：
- 类型：定时触发
- 触发周期：自定义 Cron
- Cron 表达式：`0/10 9-14 ? * MON-FRI *`
- （含义：工作日 9:00-14:59 每 10 分钟，SCF 默认北京时间）

### 4.5 测试

函数代码 → 测试 → 直接点「运行测试」

看日志：
- 如果 COS 有操作清单（第三步跑过）→ 应该返回 `triggered: 0 或 1`
- 如果 COS 没有 → 返回「无操作清单」，正常（等 GitHub Actions 跑一次就有了）

---

## 第五步：验证全链路

### 5.1 触发一次完整流程

1. GitHub Actions 手动 Run workflow（选股 + 上传 COS）
2. 等它跑完（绿色）
3. SCF 控制台手动测试预警函数
4. 看邮箱是否收到

### 5.2 日常运行（全自动）

配完后**什么都不用管**：
- 工作日 21:30 GitHub 自动选股 → 推送日报邮件 + 上传 COS
- 第二天盘中 SCF 每 10 分钟检查 → 触发发邮件预警
- 手机收邮件推送就行，电脑不用开

---

## 常见问题

**Q：只配邮件够用吗？**
A：够用。盘中预警通过邮件发送，日报也带 CSV 附件。配好 SMTP 就行。

**Q：GitHub Actions 没自动跑？**
A：cron 有延迟，等到 21:45 还没跑就手动触发。私有仓库要保证有 Actions 额度（2000分钟/月）。

**Q：SCF 预警说"无操作清单"？**
A：GitHub Actions 还没跑过，或 COS 配置错了。先手动跑一次 Actions，再去 COS 控制台看有没有 `Daily-Action-List-*.csv` 文件。

**Q：电脑完全不开机行吗？**
A：行。GitHub 选股 + SCF 预警全云端。连续几天不开机，SCF 用最后一次的操作清单预警（数据会旧，但不报错）。

**Q：想本地也跑？**
A：`scripts\start-daemon.bat` 启动本地守护进程，开机时跑。本地和云端互不冲突，谁先跑谁出结果。

**Q：费用？**
A：GitHub Actions 私有仓库 2000 分钟/月免费（选股用约 660 分钟）+ SCF 100万次/月免费 + COS 50GB 免费 = **0 元/月**。

---

## 配置清单（打勾用）

走完一遍，对照这个清单确认都配齐了：

- [ ] 邮箱 SMTP 授权码
- [ ] 腾讯云账号 + 实名认证
- [ ] COS 存储桶（记下桶名 + 地域）
- [ ] 腾讯云 API 密钥（SecretId + SecretKey）
- [ ] GitHub 私有仓库 + 代码已推上去
- [ ] GitHub Secrets 全部配齐（9 个，或按需配）
- [ ] GitHub Actions 手动跑通（选股出结果）
- [ ] `build_scf_package.py` 打包成功
- [ ] SCF 云函数创建 + 环境变量配齐（9 个）
- [ ] SCF 定时触发器配好（cron）
- [ ] SCF 手动测试通过
- [ ] 全链路验证：Actions 跑完 → SCF 预警 → 收到推送

全打勾就配完了，以后不用管，自动跑。
