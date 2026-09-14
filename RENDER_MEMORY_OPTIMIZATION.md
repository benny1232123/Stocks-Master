# Render 内存优化报告

> 诊断时间：2026-09-14 | 目标实例：Render Free（512MB）| 状态：**代码已改，尚未提交/部署**

---

## 一、结论摘要

启动基线 RSS 从 **163.3MB 降到 118.4MB（−44.9MB / −27%）**，且精简模式下 9 个重依赖（akshare / backtrader / talib / baostock / jieba / bs4 / lxml / matplotlib / supabase）**启动时全部不再加载**。

根因不是数据量大，而是**导入架构**：`RENDER_LITE=1` 已经把重端点全部 503 拒绝，但模块级 import 仍让这些功能在启动时把依赖全部拖进内存——等于为一批**永远不可达**的端点常年付 65MB 常驻内存。

---

## 二、诊断数据

### 2.1 启动链路逐级分解

干净进程（Windows / Anaconda 口径）导入 `backend.main`：

| 阶段 | 累计 RSS | 增量 | 拉进来的重依赖 |
|---|---:|---:|---|
| 解释器空进程 | 22.8 MB | — | — |
| `smcore.analysis` | 83.8 | +61.2 | pandas / numpy / pyarrow |
| `smcore.backtest` | 107.8 | **+85.0** | **backtrader + talib** |
| `smcore.dashboard` | 100.4 | +77.7 | requests |
| `smcore.selection` | 125.4 | **+102.6** | **akshare + jieba + bs4 + lxml + baostock** |
| `backend.main` 全量 | **163.3** | +140.7 | 以上全部 |

### 2.2 重依赖单位成本

| 依赖 | 增量 RSS | 精简模式是否需要 |
|---|---:|---|
| pandas + numpy | ~60 MB | ✅ 必需（无法避免） |
| akshare + jieba + bs4 + lxml + baostock | **~41 MB** | ❌ 端点已 503 |
| backtrader + talib | **~24 MB** | ❌ 端点已 503 |
| requests（dashboard） | ~17 MB | ✅ 必需 |
| fastapi + uvicorn + pydantic | ~13 MB | ✅ 必需 |

---

## 三、三处根因

### 根因 1 — `backend/main.py` 顶部模块级导入不可达模块
```
from smcore.analysis  import build_stock_analysis          # → pyarrow
from smcore.backtest  import run_signal_backtest, ...      # → backtrader + talib
from smcore.selection import get_candidate_codes, ...      # → akshare 全家桶
```
而 `_lite_reject()` 在 `/api/analysis/{code}`、`/api/backtests/run*`、`/api/selection/boll-scan`、`/api/selection/fusion` 上全部提前 503。
**→ 这些 import 在精简模式下永远不会被用到，却 100% 参与启动。**

### 根因 2 — `smcore/strategies/__init__.py` 聚合导入（内存放大器）
```python
from .boll import run_boll          # 只想用这个
from .cctv import run_cctv          # 却连带 jieba + akshare
from .momentum import run_momentum  # → akshare + baostock
from .relativity import run_relativity
from .theme import run_theme
```
Python 导入任意子模块前**必先执行包 `__init__`**，所以 `from smcore.strategies.boll import run_boll` 一份 Boll 的依赖，实际拉起 5 个策略的全部依赖。

### 根因 3 — `smcore/selection.py` 模块级 `run_boll` 导入
`run_boll` 实际只在 `scan_boll_batch()` 内被调用一次，却挂在模块顶部。后果是**未被精简模式拦截**的 `/api/selection/candidates`（活跃端点）首次调用就白付 41MB。

---

## 四、已实施的改动

| 文件 | 改动 |
|---|---|
| `backend/main.py` | ① 移除 3 个模块级重导入，改为 **6 处函数内懒导入**；② `lifespan` 的 `prewarm_dashboard_cache` 改为**仅当无静态快照时**才启动（原先无条件跑，构建期是主要内存峰值来源） |
| `smcore/strategies/__init__.py` | 聚合导入 → **PEP 562 惰性 `__getattr__`**：仅在实际以聚合形式取值时才导入对应子模块。**保留向后兼容**（`from smcore.strategies import run_boll` 仍可用），项目内所有调用方走子模块路径，行为与耗时无变化 |
| `smcore/selection.py` | `run_boll` 导入**下沉到 `scan_boll_batch()`** 使用点 |
| `render.yaml` | 新增 `MALLOC_ARENA_MAX=2` + `MALLOC_TRIM_THRESHOLD_=131072`（治 glibc 多线程 arena 膨胀 / RSS 只涨不落；仅 Linux 生效） |

---

## 五、验证结果

```
启动基线 RSS: 163.3 -> 118.4 MB  (增量 95.7 MB)
  akshare      ✅ not loaded      backtrader   ✅ not loaded
  talib        ✅ not loaded      baostock     ✅ not loaded
  jieba        ✅ not loaded      bs4          ✅ not loaded
  lxml         ✅ not loaded      matplotlib   ✅ not loaded
  supabase     ✅ not loaded

惰性路径可解析性: 6/6 全部可用
lifespan 启动: 成功（条件分支未报错）
端点 smoke:  /health /api/status /api/dashboard /api/config/recommendation
             /api/backtests/daily-summary 全部 200
             /api/analysis/600519 -> 503（精简模式正确拦截）
regression:  24 passed
```

另：`smcore.selection` 独立导入成本 **102.6 → 61.1 MB**；调用 `get_candidate_codes()` 后不再加载任何重依赖。

---

## 六、剩余可压的两笔（未做，按 ROI 排序）

| 优先级 | 项 | 收益 | 做法 |
|---|---|---:|---|
| P0 | **supabase-py SDK** | **~77 MB** | 首次 `/api/portfolio` 触发。SDK 依赖链极重，可用 `httpx` 直连 Supabase REST（PostgREST）替代，功能等价 |
| P1 | `/api/backtests/daily-summary` 的基准对比 | ~41 MB | 内部调 `_get_hs300_close()`（走 akshare + 联网）。建议像 dashboard 一样改为读 CI 预生成的快照 |
| P1 | `/api/backtests/daily-latest` 峰值 | 峰值↓ | 一次装 14 天 × 3 CSV 进内存。可快照化或改按需分页 |
| P2 | 部署形态 | 根治 | 若长期跑，换 Render Starter(2GB) 或按 09-14 记录的 Oracle A1 / 阿里云学生机方案 |

**不可压的硬成本**：pandas + numpy ≈ 60MB，是后端全链路的地基。

---

## 七、部署注意

1. 本次改动**尚未 commit / push**，需提交后 Render 才会重建。
2. `MALLOC_*` 环境变量需**重新部署**才生效（Render 不会热更新环境变量）。
3. 本报告数据为 **本机 Windows / Anaconda 口径**；Render（Linux + 不同 pandas/akshare 版本）绝对值会有差异，但**相对收益（−27%）成立**。
4. 回滚：4 个文件均为独立改动，`git checkout` 即可单独还原。

---

## 附：诊断脚本（留档可复用）

| 脚本 | 用途 |
|---|---|
| `.workbuddy/mem_profile.py` | 启动链路逐级 RSS 分解 + tracemalloc top 25 |
| `.workbuddy/mem_profile3.py` | 定位"哪个模块拉进了哪个重依赖" |
| `.workbuddy/mem_per_module.py` | 逐模块独立成本（干净子进程） |
| `.workbuddy/verify_lazy_import.py` | 改造后回归验证 |

> ⚠️ Windows 下用 `psapi.GetProcessMemoryInfo` 测 RSS 时，**必须显式声明 `argtypes` / `restype`**，否则静默返回 0（本报告第一版就踩了这个坑）。

---

# 第二轮：数据源压缩（2026-09-14 下半场）

## 完成情况

| # | 项目 | 效果 | 关键文件 |
|---|---|---|---|
| 1 | Supabase SDK → httpx 直连 PostgREST | **省 ~77MB 常驻** | `smcore/storage/trades_repo.py`、`requirements.txt` |
| 2 | akshare 拆 CI-only + 占位对象 fail-soft | 云端不再安装 6 个重包 | `requirements-cloud.txt`(新)、`smcore/utils/ak_compat.py`(新)、`render.yaml` |
| 3 | 指数三套实现 → 调查后判定**不合并** | 仅加边界注释，避免策略行为变更 | `data/index.py`、`strategy/market.py`、`strategy/regime_filter.py` |

## 1) Supabase 改 httpx

`SupabaseTradeBackend` 用 httpx 直连 PostgREST 四个动作（GET / POST / PATCH / DELETE），
带 `apikey` + `Authorization: Bearer` + `Prefer: return=representation`（对齐原 SDK 的
`resp.data` 语义）。接口签名、字段映射与 fail-soft 语义**完全不变**。

> ⚠️ `requirements.txt` 已加 `httpx>=0.27`。**这是必须的** —— 缺失会让云端 Supabase 后端
> 创建失败并静默回退到本地 `trades.json`，表现为"持仓消失"。

验证：用 `httpx.MockTransport` 拦截，逐方法核对 URL / 认证头 / body；`trade_date` /
`quantity` 字段映射（历史事故点）做了专项断言。

## 2) akshare 拆 CI-only

审计（`ast` 判定 try 祖先）发现 **13 处未被 try 保护**的 `import akshare`——`dashboard.py`
独占 8 处。直接拆依赖会让这些路径抛 ImportError，端点直接 500，而不是优雅降级。

新增 `smcore/utils/ak_compat.py`：akshare 缺失时返回 **MISSING 占位对象**，其属性访问得到
「调用即返回 None」的桩。调用方原样写 `df = ak.xxx()` 就得到 `None`，既有的
`if df is not None and not df.empty` 判空自然走回退分支——**调用点零改动**。15 处已替换。

`requirements-cloud.txt` 移除 6 个包（akshare / backtrader / matplotlib / supabase / pillow /
cos-python-sdk-v5），保留 9 个核心；`render.yaml` buildCommand 改指向它，CI 仍用 `requirements.txt`。

验证（屏蔽被移除的包后运行）：`backend.main` + 10 个模块导入成功；**14 个云端可达端点全部 < 500**。

## 3) 指数实现：为什么**没有**合并

原判断是"3 套重复实现"，调查后发现三者接口语义本就不一样：

| 实现 | 返回 | 缓存 | 调用方 |
|---|---|---|---|
| `data/index.py::fetch_index_close_series` | 任意区间 DataFrame | SQLite | **smcore 内零调用方**，仅遗留脚本 |
| `market.py::_get_index_series` | 全量 DataFrame（新浪，海外可达） | 进程内 | `compute_market_profile`（regime） |
| `regime_filter.py::_get_hs300_close` | 收盘 Series | 按日失效 | 归因 / 自适应权重 / 仓位β / RS过滤 **6+ 处**，**5 个测试**锁定其缓存语义 |

强行合并 = 把 regime 取数源从 baostock 换成新浪 = **策略行为变更**，须配 OOS 验证，
不能当作"消除重复"顺带做。故仅在 docstring 写明各自职责与统一入口。

> **顺带发现的既有问题**：`_get_hs300_close` 主源 baostock 在海外 CI 连不上，兜底 akshare
> 同样不可达 → 海外 regime 静默回退默认「震荡轮动」（自适应权重长期显示等权的原因之一）。
> 若要修，应立为**独立的数据源变更任务**并配 OOS 验证。

## 回归

- pytest **45 passed**（config_driven_thresholds / risk_neutral / web_endpoints_json /
  report / fusion / boll_signal / relativity）
- 行尾核对：`git diff --numstat` 行数与改动量精确吻合（如 dashboard.py 16+/8- = 8 处 import 各 +2−1），无全文重写
- 构建配置：render.yaml YAML 可解析、buildCommand 指向正确、CI workflow 未受影响

## ⚠️ 部署注意

1. 本次改动**尚未提交**，需 commit + push 后 Render 才会重建。
2. **Render 将首次使用 `requirements-cloud.txt`** —— 若云端有遗漏依赖，相关端点会 500。
   部署后建议立即检查：`/api/status`、`/api/dashboard`、`/api/portfolio`、`/api/backtests/daily-summary`。
3. 持仓写入路径已换成 httpx + PostgREST —— 部署后先做一次「录一笔 trade → 刷新持仓」的冒烟测试。
4. `stock_data/k_data` 下 3 个 parquet 是 09-13 的本地改动，**不要**混进本次提交。
