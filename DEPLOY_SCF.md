# SCF 云函数部署说明

让盘中预警在腾讯云上跑，不用开机。

## 架构

```
本地 daemon（开机时跑一次）
  ├─ 21:30 选股 + 推送
  └─ 生成操作清单 → 上传 COS
                          ↓
SCF 云函数（云端，不开机也跑）
  └─ 盘中每 10 分钟：从 COS 读清单 → 拉新浪行情 → 触发推企微
```

## 前置准备

### 1. 开通腾讯云服务（免费额度内）

- 注册腾讯云账号：https://cloud.tencent.com
- 开通云函数 SCF：https://console.cloud.tencent.com/scf
- 开通对象存储 COS：https://console.cloud.tencent.com/cos

### 2. 创建 COS 存储桶

1. 进入 COS 控制台 → 创建存储桶
2. 名称随意（如 `stocks-master-1250000000`）
3. 地域选离你近的（如广州 `ap-guangzhou`）
4. 权限设为"私有读写"
5. 记下 **存储桶名** 和 **地域**

### 3. 获取 API 密钥

1. 访问 https://console.cloud.tencent.com/cam/capi
2. 新建密钥，记下 **SecretId** 和 **SecretKey**

## 部署步骤

### 步骤 1：本地配置环境变量

在本地（让 daemon 能上传 COS），设置环境变量：

```bat
set COS_SECRET_ID=你的SecretId
set COS_SECRET_KEY=你的SecretKey
set COS_BUCKET=stocks-master-1250000000
set COS_REGION=ap-guangzhou
set WECOM_WEBHOOK_URL=https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx
```

建议写到 `scripts\start-daemon.bat` 开头，或系统环境变量。

### 步骤 2：打 SCF 部署包

```bash
python build_scf_package.py
```

生成 `scf_alert_package.zip`（约 46KB，非常小）。

### 步骤 3：创建云函数

1. 登录 https://console.cloud.tencent.com/scf
2. 新建函数：
   - 名称：`stocks-master-alert`
   - 运行环境：Python 3.9
   - 部署方式：本地上传 zip
   - 上传 `scf_alert_package.zip`
3. 执行方法：`scf_alert.main_handler`
4. 内存：128MB（足够）
5. 执行超时：60 秒

### 步骤 4：配置环境变量

在函数配置 → 环境变量中添加：

| Key | Value |
|-----|-------|
| WECOM_WEBHOOK_URL | https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=你的key |
| COS_SECRET_ID | 你的SecretId |
| COS_SECRET_KEY | 你的SecretKey |
| COS_BUCKET | stocks-master-1250000000 |
| COS_REGION | ap-guangzhou |

### 步骤 5：配置定时触发器

在触发器管理 → 添加触发器：

- 类型：定时触发
- 周期：自定义 Cron
- Cron 表达式：`0/10 9-14 ? * MON-FRI *`

含义：工作日 9:00-14:59 每 10 分钟触发一次（覆盖上午+下午盘）。

> SCF Cron 用 7 段格式，`9-14` 是 UTC+8 的 9-15 点需减 8（实际写 1-7）？
> **不是**，腾讯云 SCF 默认 UTC+8，直接写北京时间：`0/10 9-14 ? * MON-FRI *`
> 9:00-14:59 每 10 分钟，覆盖 9:30-15:00 盘中时段。

### 步骤 6：测试

在 SCF 控制台点"测试"，看日志是否正常。
或本地测试：`python scf_alert.py`（用本地操作清单，不走 COS）。

## 日常使用流程

1. **开机时**：`scripts\start-daemon.bat` 启动 daemon
   - 21:30 自动跑选股 + 生成操作清单 + 上传 COS
2. **关机后**：SCF 在云端每 10 分钟检查预警，触发推企微
3. **第二天**：再开机，daemon 自动跑当天选股，更新 COS

## 费用

- SCF：每月 100 万次调用免费 + 40 万 GB-秒免费。每 10 分钟 = 每月约 5000 次，远在免费额度内。
- COS：50GB 存储免费 + 100 万次请求免费。操作清单每天一个 CSV（几 KB），远够。
- **实际费用：0 元**

## 故障排查

| 问题 | 排查 |
|------|------|
| SCF 测试报错 | 看控制台日志，检查环境变量是否配齐 |
| 预警没触发 | 检查 COS 是否有 `Daily-Action-List-*.csv`（本地 daemon 要先跑+上传） |
| 推送没收到 | 检查 `WECOM_WEBHOOK_URL` 是否正确 |
| 行情拉不到 | 新浪接口偶发超时，SCF 会跳过本次，下次重试 |
| 操作清单过期 | daemon 没跑（没开机），SCF 用旧清单。连续几天不开机会导致预警基于旧数据 |

## 限制

- **选股仍需开机**：四策略选股依赖 baostock + 巨石，不适合 SCF。开机时 daemon 跑一次。
- **预警全云上**：SCF 每 10 分钟检查，盘中触发推企微。不开机也能收到。
- 如果完全不开机：SCF 用最近一次的旧操作清单做预警，数据会过时但不会报错。
