# 部署到 Render（完整后端 · 免费 · 功能最全）

> **当前主部署方案**：后端 = **Render Free（原生 Python 运行时 + 完整 FastAPI）**，回测/选股/组合/个股分析全部线上实时运算；
> 入口 = **Cloudflare 前置代理**（自定义域名 + 全球 CDN + 免费 SSL + DDoS 防护），见下文「Cloudflare 前置代理」一节。
> 比纯静态方案功能更全，且无需信用卡（用户已确认可用）。
> 注：2026-07-09 曾因 Cloudflare **纯静态**无法跑实时后端而回退 Render；本次仅把 Cloudflare 当作**反向代理前置**，后端仍在 Render，功能不受影响。

## 为什么选它
- **完整后端**：`uvicorn` 常驻，前端直接调真实 `/api`，回测/选股/组合都是实时的，不受"预生成固定结果"限制。
- **免费**：Render Free 计划对个人项目免费，无需信用卡即可部署（用户已确认可用）。
- **自动化**：连接 GitHub 仓库后，每次 push 到 `master` 自动构建+部署。
- **海外可达 akshare**：数据由 GitHub Actions（海外 runner）拉取并预生成，提交回仓库；Render 部署时把最新数据一起打包。

## 已知限制（免费层）
- **休眠**：免费 Web 服务 15 分钟无流量会自动休眠，下次访问需冷启动（约几秒~十几秒）。
  → 已配 `.github/workflows/keep-alive.yml` 每 10 分钟 ping 一次，基本保持常驻（需配置 `RENDER_URL` secret）。
- **内存/算力有限**：约 512MB RAM。多策略回测跑小票池没问题；超大范围扫描请控制标的量。

## 架构（完整后端，非常驻前端构建）
```
GitHub Actions (海外 ubuntu-latest, 每日工作日 13:30 UTC)
  ├─ 跑 4 策略 → 融合 Daily-Action-List-*.csv
  ├─ prewarm 拉指数/市场宽度/SHIBOR → daily_cache/*.pkl
  └─ git commit stock_data/* 回 master
        │ (push 触发 Render 自动部署)
        ▼
Render (Free Web Service, 读 render.yaml)
  ├─ buildCommand: 构建 frontend/dist + pip install -r requirements.txt
  ├─ startCommand: uvicorn backend.main:app --port $PORT
  └─ 同时托管前端 dist 与 /api/*（真实后端，TDX_ENABLED=0 走 akshare）
```

## 部署步骤
1. **推送代码到我（或你）的 master**：本仓库已含 `render.yaml`，Render 会自动识别。
2. **Render 控制台新建 Web Service**：
   - New → Web Service → 连接你的 GitHub 仓库（选 `Stocks-Master`）。
   - Render 检测到根目录 `render.yaml` 会自动套用配置（名称 `stocks-master`、Free 计划、Python 运行时）。
   - 确认 Build Command / Start Command 与 `render.yaml` 一致即可，点 Create Web Service。
3. **填环境变量**（render.yaml 里 `sync: false` 的两项需在 Render 控制台手动填）：
   - `SUPABASE_URL`、`SUPABASE_KEY`：留空也能跑（仅舆情/CCTV 因子用），填了更好。
   - 其余 `KLINE_BACKEND=akshare`、`TDX_ENABLED=0`、`TRADES_BACKEND=auto` 已由 render.yaml 注入。
4. **拿到域名**：部署完成后 Render 给一个 `https://stocks-master.onrender.com`，可自定义或绑自己的域名。
5. **配置保活**：仓库 `Settings → Secrets` 加 `RENDER_URL` = 你的站点地址（如 `https://stocks-master.onrender.com`）。
   → `keep-alive.yml` 每 10 分钟 ping，避免免费实例休眠。
6. **（可选）COS 上传**：若想每日清单同步到腾讯云 COS，在 GitHub Secrets 加 `COS_SECRET_ID/KEY/BUCKET/REGION`。

## 自动更新
- `daily-pick.yml` 每个工作日跑完策略 → 提交最新 `stock_data/*` → push 触发 Render **自动重新部署**，看板每天更新。
- 你手动 push 源码同样触发部署。

## 本地开发
```bash
# 本地连真实后端调试（Anaconda 跑后端 + vite dev）
E:\Anaconda\python.exe app.py          # 另开终端
cd frontend && npm install && npm run dev
# 默认即真实后端模式（无需任何 VITE_* 开关）
```

## Cloudflare 前置代理（自定义域名 + CDN + SSL，可选但推荐）

> 后端仍跑在 Render（完整 FastAPI），Cloudflare 只做**反向代理前置**：接管你的自定义域名、全球 CDN 加速、免费 SSL、DDoS 防护。
> 前端调用的是同源 `/api/*`，**无需改任何代码**；Cloudflare 也不跑 Python，只是把入口和加速层换到 Cloudflare。

### 步骤
1. **Cloudflare 添加站点**：把你的域名（如 `yourdomain.com`）加到 Cloudflare，按提示把域名注册商的 NS 改成 Cloudflare 给的两条。
2. **DNS 记录（关键）**：
   - 类型 `CNAME`，名称 `stocks`（或 `@` 用根域 / `www`），目标 `stocks-master.onrender.com`，**代理状态 = 已代理（橙色云朵）**。
   - 用户访问 `https://stocks.yourdomain.com` → Cloudflare 边缘 → Render 源站。
3. **SSL/TLS 模式**：Cloudflare 控制台 → SSL/TLS → 概览，设为 **Full**（Cloudflare 与 Render 之间强制 HTTPS）。**不要选 Flexible**（会与 Render 的 HTTPS 形成重定向环）。Full (strict) 也可，Render 证书受信任。
4. **缓存规则（必做，否则 API 结果被冻）**：
   - 控制台 → 缓存 → 缓存规则 → 新建规则：
     - 条件：`URI 路径` 开头为 `/api/`
     - 设置：`缓存资格 = 跳过`（Cache Level: Bypass）
   - 前端 `dist/` 里带 hash 的 JS/CSS 可放心走边缘缓存，无需处理。
5. **（推荐）Render 侧认领域名**：Render 控制台 → 你的 Web Service → Settings → Custom Domains → 添加 `stocks.yourdomain.com`。Render 自动签证书并允许该 Host，避免个别 host 校验边角问题。
6. **保活指向新域名**：GitHub 仓库 Secrets 把 `RENDER_URL` 改成 `https://stocks.yourdomain.com`（走 Cloudflare → 源站，既保活又让公共域名常驻边缘）；原 `onrender.com` 地址保留也可用。
7. **安全/排障（可选）**：若 API 被 Cloudflare 拦验证码，把 Security → Settings 安全级别降到 Low，并关闭 Bot Fight Mode（避免对程序化/服务端请求误判）。

### 注意
- Cloudflare 免费版对**静态资源**加速明显；`/api/*` 动态请求每次仍回源 Render，所以后端实时计算能力不变，免费休眠 / 512MB 限制也照旧（保活照常）。
- 这不是把后端「搬」到 Cloudflare——Cloudflare 原生不支持 Python Web 服务（Workers 128MB + CPU 计时，backtrader/akshare 跑不了）。只是把入口域名与加速层换到 Cloudflare。

## 免休眠：Cloudflare Worker 保活（免费可靠，推荐）

> Render Free 的硬伤是**15 分钟无入站流量就休眠**。`.github/workflows/keep-alive.yml` 用的是 GitHub Actions scheduled cron，免费账号经常延迟十几~几十分钟才跑，一旦间隔超过 15 分钟实例就睡了。
>
> **最简做法（零代码）**：用现成免费监控 **UptimeRobot** 加一个 5 分钟 HTTP 监控指向 `https://stocks-master.onrender.com/`，纯网页表单、无需 CLI/部署（详见文末对比）。想要更「自己掌控」、不依赖第三方，再用下面的 Cloudflare Worker。

### 做法（仓库已带 `cloudflare-keepalive/`）
1. 进 `cloudflare-keepalive/` 目录。
2. 登录并部署：
   ```bash
   npx wrangler login        # 浏览器授权 Cloudflare 账号
   npx wrangler deploy       # 自动注册 Cron Trigger（每 5 分钟）
   ```
3. Worker 每 5 分钟 `GET https://stocks-master.onrender.com/`，Render 始终有入站流量，不再休眠。
4. 原 `keep-alive.yml` 可保留作兜底，或停用（Worker 更稳）。

### 成本
- 免费档：每账号 5 个 Cron Trigger（本方案用 1 个）；每天 288 次调用，远低于 10 万次/日上限；单次 CPU < 10ms，无费用。
- 注意：Worker 必须 ping **Render 源站域名**（`*.onrender.com`），不要 ping Cloudflare 前置域名（避免绕回边缘缓存、且确保直达源站）。

### 其他免休眠打法（对比，按复杂度从低到高）
- **UptimeRobot 免费监控（最简 · 零代码）**：注册免费号 → 加一个 HTTP 监控指向 `https://stocks-master.onrender.com/` → 间隔 5 分钟。纯网页表单，无需 CLI/部署/Cloudflare 账号，2 分钟搞定；免费档 50 个监控足够用。代价：多一个外部依赖。
- **Cloudflare Worker 保活（见上，自托管 · 免费）**：已写在 `cloudflare-keepalive/`，需 `wrangler deploy`，但完全自己掌控、不依赖第三方。
- **Render Starter $7/月**：付费档直接关闭休眠，确定性最强、零 hack，适合不想依赖保活 trick 的情况。
- **Oracle Always-Free**：2 台 ARM 实例（4 vCPU / 24 GB）永久免费、永不休眠，但需绑卡 + Docker 部署 + 自运维，最重。

## 备注
- 部署方案已定档 **Render Free（原生 Python 运行时 + 完整 FastAPI 后端）**，无需 Docker / 容器 / 静态拦截层。
- 早期 Oracle 容器方案（`Dockerfile`/`docker-compose.yml`/`DEPLOY_ORACLE.md`）与 Cloudflare 纯静态方案（`staticShim.js`/`scripts/generate_static_data.py`/`deploy-cf.yml`/`wrangler.toml`/`DEPLOY_CLOUDFLARE.md`）相关文件均已删除，当前仓库仅保留 Render 一条链路。
- 默认分支为 `master`。
