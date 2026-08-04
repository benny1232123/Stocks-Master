# 部署到 Render（完整后端 · 免费 · 功能最全）

> **当前主部署方案**（2026-07-09 用户定档：放弃 Cloudflare 纯静态，回到 Render）。
> Render 免费层能跑**完整 FastAPI 后端**，回测/选股/组合/个股分析全部在线上实时运算，
> 比纯静态方案功能更全，且无需信用卡（用户已确认可用）。

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

## 备注
- 部署方案已定档 **Render Free（原生 Python 运行时 + 完整 FastAPI 后端）**，无需 Docker / 容器 / 静态拦截层。
- 早期 Oracle 容器方案（`Dockerfile`/`docker-compose.yml`/`DEPLOY_ORACLE.md`）与 Cloudflare 纯静态方案（`staticShim.js`/`scripts/generate_static_data.py`/`deploy-cf.yml`/`wrangler.toml`/`DEPLOY_CLOUDFLARE.md`）相关文件均已删除，当前仓库仅保留 Render 一条链路。
- 默认分支为 `master`。
