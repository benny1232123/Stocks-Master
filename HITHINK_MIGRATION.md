# 数据源统一到同花顺（hithink）—— 实施报告

> 日期：2026-09-14 · 目标：**三个运行环境（本地 / GitHub CI / Render）的数据源统一走同花顺官方
> Financial-API**，不再出现「本地 tdx、CI hithink、Render akshare」各用各的。

## 1. 改动清单

| 文件 | 改动 |
|---|---|
| `.env` | `KLINE_BACKEND=akshare` → **`hithink`**（Key 原值不动） |
| `render.yaml` | `KLINE_BACKEND: akshare` → **`hithink`**；新增 `HITHINK_FINANCE_API_KEY` (`sync:false`) |
| `backend/main.py` | `os.environ.setdefault("KLINE_BACKEND", "akshare")` → **`hithink`**（平台未设 env 时的兜底默认） |
| `smcore/dashboard.py` | `configure_runtime()` 的 `setdefault(..., "tdx")` → **`hithink`** |
| `smcore/data/quote.py` | 实时行情**首选同花顺快照**（`fetch_snapshot`）；源序 = hithink → tdx → 新浪 HTTP → 全市场快照缓存。快照无名称字段 → 用离线名称索引补名 |
| `smcore/strategy/regime_filter.py` | 新增 `_fetch_hs300_hithink`；`_get_hs300_close` 主源改为 hithink → baostock → akshare |
| `smcore/strategy/market.py` | 新增 `_fetch_index_series_hithink`；`_get_index_series` 主源改为 hithink → 新浪 → baostock/akshare |
| `smcore/data/index.py` | 遗留模块 `fetch_index_close_series` 首选加 hithink（供 `auto_notify_boll.py`） |
| `smcore/strategy/fundamental.py` | 新增 `_fetch_profit_growth_periods_hithink`（五类指标 → roe/毛利率/营收增速）；**单位换算修正**：`revenue_growth` 从 `×1.0` 改为 **`×0.01`**（见 §3.2，原值会把 1.42% 当成 142%）。`_build_fundamental_online` 质量/成长**首选 hithink**，失败回退 baostock；`FUNDAMENTAL_SOURCE=baostock` 可一键回滚 |
| `smcore/data/hithink.py` | **修复**：指数历史 K 端点改为**内部分片 + 全有或全无**（见 §4.1），此前长区间请求会静默拿不到数据、或拿到截断的旧序列 |

**未改**（`walk-forward.yml`）：该 workflow 声明「纯读历史 CSV，不联网不重跑回测」，只装 pandas+pyarrow，
根本不拉活 K 线，故无需设 `KLINE_BACKEND`。前一轮「walk-forward 会退到 baostock」的说法是错的，特此更正。

## 2. 同花顺覆盖度（现状）

| 数据类别 | 同花顺端点 | 状态 |
|---|---|---|
| K 线（日线，前复权） | `/api/a-share/prices/historical` | ✅ 全环境主源 |
| 实时快照 | `/api/a-share/prices/snapshot` | ✅ 全环境主源 |
| 指数历史 K / 快照 | `/api/a-share-index/prices/*` | ✅ 全环境主源 |
| 估值 PE/PB/PS/PCF | `/api/a-share/valuations/snapshot` | ✅ 已是主源（此前已接） |
| 质量/成长（ROE/毛利率/营收增速） | `/api/a-share/financials/indicators` | ✅ 新增主源 |
| 板块/概念目录 + 成分股 | `/api/a-share-index/*` | ✅ 已接（theme/风险中性化） |
| 复权因子事件流 | `/api/a-share/corporate-actions/adjustment-factors` | ✅ 已接（qfq 守卫交叉校验） |
| 涨停/龙虎榜/热榜/异动 | `/api/a-share/special-data/*` | ✅ 已接 |
| **总市值 mkt_cap** | ❌ 估值端点不含此字段 | 仍走腾讯 `qt.gtimg.cn` |
| **换手率 turnover** | ❌ 快照无换手率字段 | 仍走 baostock 日线（`turn`） |
| 股票名称索引 | — | 离线 JSON（`stock_names_index.json`），零联网 |

## 3. 验证结果（联网实跑，2026-09-14）

### 3.1 基础连通性

`.workbuddy/verify_hithink_all.py` → **8/8 PASS**（连跑 3 次均通过）：

| 项 | 结果 |
|---|---|
| `_backend()` | `hithink` ✅ |
| K 线 600519 前复权 | 30 行，0.75s，末行 2026-09-11 close=1275.16 ✅ |
| 实时快照 | 600519/000001/300750 价格+涨跌幅+名称均正确 ✅ |
| hs300 序列（regime_filter） | 1126 行，末值 4480.08 ✅ |
| 三大宽基指数（market） | 000300/000905/000852 各 1126 行 ✅ |
| 估值 | pe 19.62 / pb 6.36 / ps 9.22 / pcf 13.41 ✅ |
| 质量/成长 | 2026-06-30: roe 0.1675 / 毛利率 0.8956 / 营收同比 1.47% ✅ |

回归：`verify_cloud_requirements.py` **全部通过**（14 个云端端点均 <500；akshare/backtrader/matplotlib/
supabase/PIL/qcloud_cos 均未被导入）。

### 3.2 依赖 HS300 的三条链路 + 质量/成长单位（新增定向校验）

`.workbuddy/verify_index_paths.py`（信号日区间内逐点检查）：

| 链路 | 结果 |
|---|---|
| `adaptive_weights._benchmark_forward_ret`（归因/edge 基准） | 20250815→+7.01%、20260105→+0.35%、20260630→−3.67%、20260908→None（未来数据不足，因果正确）✅ |
| `market.compute_market_profile(None)` | `下行防御` strength=0.12 / hs300_ret20=−5.51%（**非默认值**，说明真在用 hithink 数据）✅ |
| `market.compute_market_profile(as_of=…)` | 20250815→`趋势上行`0.91、20260630→`趋势上行`0.76 ✅ |
| `regime_filter._index_20d_return`（RS 过滤基准） | 20250815→+3.54%、20260630→+2.79%、20260908→−2.25% ✅ |

`.workbuddy/verify_ths_quality.py`（同报告期跨源对比，3 只票）：

| 代码 | 报告期 | THS | baostock |
|---|---|---|---|
| 600519 | 2026-03-31 | roe 0.10570 / 毛利率 0.897592 / 营收增速 0.065380 | roe 0.105687 / 毛利率 0.897592 / 营收增速 **缺** |
| 600519 | 2026-06-30 | roe 0.16750 / 毛利率 0.895552 / 营收增速 0.014699 | roe 0.179543 / 毛利率 0.895552 / 营收增速 **缺** |
| 000001 | 2026-06-30 | roe 0.05220 / 营收增速 0.017756 | roe 0.046746 / 营收增速 **缺** |
| 600000 | 2026-06-30 | roe 0.03960 / 营收增速 0.035535 | roe 0.037501 / 营收增速 **缺** |

结论：**毛利率逐位一致**（0.0% 差）；**ROE 是口径差**（加权平均 vs 平均净资产），方向不固定
（茅台 −6.7%、平安 +11.7%、浦发 +5.6%），非数据错误；**营收增速是 THS 独有**（原因见 §4.3）。

### 3.3 ⚠️ 单位口径修正（本轮自己抓出来并修掉的一个真 bug）

`revenue_growth` 的规范单位在本项目里是**小数**（0.05 = 5%），依据有三处：

- `RECOMMENDATION_CONFIG["fundamental"]["rg"]` 分段 = `gt 0.3 → 92 / gt 0.2 → 82 / gt 0.1 → 66 / gt 0 → 54`；
- `analysis.py:304` 展示为 `f"{value*100:.0f}%"`；
- `tests/test_fundamental_pit.py` 用 `revenue_growth: 0.05 / 0.08`。

而 THS `calculate_operating_income_yoy_growth_ratio` 返回的是**百分数**（1.42、3.55、6.54…）。
初版映射写成 `×1.0` 直通 → 1.42 会被判成「142%」，命中 `gt 0.3 → 92 分「高增长」`，**几乎全市场通吃**，
基本面面分被系统性抬高约 `(92−50)/5 ≈ +8.4` 分。已改为 **`×0.01`**，复验后取值 0.0147/0.0178/0.0355/0.0654，
落入合理分段。`roe` / `gross_margin` 的 `×0.01` 原就正确，未动。

> 参考：`factor_scoring.py` 用的是 `roe + revenue_growth` 的**截面 z-score**，对线性缩放不变 → 不受该 100× 影响；
> 受影响的只有 `analysis.py` 的分段打分与展示（即网站 ComprehensivePanel 基本面面分）。

## 4. 过程中发现的三个既存问题

### 4.1 指数历史 K 端点长区间**静默返回空** + **间歇丢分片**（已修复）

实测（`.workbuddy/probe_hithink_index_limit.py`）：

| 请求区间 | 结果 |
|---|---|
| 近 1200 自然日（799 根） | ✅ 正常 |
| 近 2000 自然日（~1330 根） | ❌ `code=0` 但 `item=[]`（**不报错、不截断**） |
| `2020-01-01 ~ 今`（单次） | ❌ 空 |
| `2020-01-01 ~ 今`（分片 750 天） | ✅ 1126 根（**前 ~1 年无数据**：服务端可用起点 ≈ 2022-01-21) |

→ 结论：**单次请求若起点早于服务端可用窗口，整段直接返回空**（不是截断）。故 `fetch_index_historical`
改为按 750 自然日分片 + 拼接去重；首个分片为空视为「早于可用历史」不告警，其余分片为空才告警。
**个股**历史 K 端点无此限制（实测 2400 自然日 / 1598 根正常），`fetch_historical_k` 不分片。

修复前，`regime_filter` / `market` 的「2020 起」请求在本环境**全部拿不到指数数据** → regime 会静默
退化。此前该路径走 baostock/新浪所以没暴露；一旦主源切到 hithink 就会踩中——本次一并修掉了。

**补充：该端点还会间歇性丢分片（2026-09-14 实测）**——连跑中曾出现 `000905` 只取到前 3 片共 991 行、
「末收盘」8658.33 实为**6 个月前的旧值**。截断的指数序列比「取不到」更危险（会被当最新数据静默用于
regime / 相对强度基准），故分片逻辑采用**全有或全无**：任一分片（首片除外，首片空属可用历史边界）
为空即整体判失败、返回空，让调用方降级到新浪/baostock。改后连跑 3 次均 8/8 通过且三个指数均返回
完整 1126 行。

### 4.2 海外 CI 下 regime 静默退化（顺带修复）

`regime_filter._get_hs300_close` 原主源 baostock、兜底 akshare，**两者在海外 CI 均不可达** → 返回旧缓存
或 None → regime 静默退化为「震荡轮动」。hithink 作为可达主源（海外可用）**修复了这个既存问题**。
指数收盘价是交易所确定值（非复权序列），跨源数值一致，不改变 regime 判定口径。

### 4.3 baostock 成长数据实际为空 → `revenue_growth` 因子长期是「死」的（THS 顺带补活）

直接打 baostock 接口实测：

```text
bs.query_growth_data(code='sh.600519', year=2025, quarter=4).fields
→ ['code','pubDate','statDate','YOYEquity','YOYAsset','YOYNI','YOYEPSBasic','YOYPNI']
```

**没有 `YSTZ`（营业总收入同比增长率）字段**，而 `_fetch_profit_growth_periods_baostock` 读的正是
`rec["YSTZ"]` → 永远取不到 → 实测 600519 的 26 个报告期**无一含 `revenue_growth`**。
即：`rg` 因子在所有 baostock 缓存里恒为缺失，打分时吃 `missing=50` 的中性分，
「PE/PB/ROE/毛利率/营收增长 5 因子均值」实际退化为「4 真值 + 1 个常数 50」。

hithink 接上后 `revenue_growth` 才第一次有真值 → **属于行为变更（因子被激活）**，也是 §3.3 单位问题
必须在本次一并修掉的原因。若想回滚到「rg 恒 50」的旧行为，设 `FUNDAMENTAL_SOURCE=baostock`。

> 未修 baostock 侧：它现在根本不再提供营收同比字段，改了也没数可填，故保持原样（fail-soft → 缺失）。

### 4.4 指数可用历史长度变化（需知晓，当前无影响）

`.workbuddy/probe_index_window.py` 实测（沪港深三源同日同值，仅长度不同）：

| 源 | 行数 | 起始日 | 末值 |
|---|---|---|---|
| hithink | 1126 | **2022-01-21** | 4480.08 |
| 新浪 | 320 | 2025-05-27（`datalen=320` 上限） | 4480.08 |
| baostock | 1625 | 2020-01-02（按请求起点） | 4480.08 |

- 对 `market._get_index_series`：原主源是新浪（**320 根**）→ 现在 hithink（**1126 根**），**变长**。
- 对 `regime_filter._get_hs300_close`：原主源 baostock（**1625 根**）→ 现在 hithink（**1126 根**），**变短**。
- 实际影响：现有信号日区间 = **2025-08-15 ~ 2026-09-11**（203 个 Daily-Action-List），完全落在
  2022-01-21 之后 → **无影响**。若将来把回放推进到 2022-01-21 之前，`_index_20d_return` 会因基准缺失
  返回 None，RS 过滤按既有设计「数据缺失一律放行」→ 过滤静默失效。届时需把 baostock 提回主源。

## 5. ⚠️ 需要你手动操作

1. **GitHub Secrets**：Settings → Secrets and variables → Actions → `HITHINK_FINANCE_API_KEY`
   = `.env` 里的同值。（`daily-pick.yml` / `daily-holdings.yml` 已引用；不设则 CI 回退 akshare、不报错。）
2. **Render Dashboard** → Environment → 填 `HITHINK_FINANCE_API_KEY`（`render.yaml` 已声明 `sync:false`）。
   不填则 Render 上 hithink 不启用、回退旧源。

## 6. ⚠️ 遗留：k_data 复权基准混合

`stock_data/k_data/` 的 parquet 基线是 **akshare 前复权**（2026-09-09 全量重拉时的口径）；
跨源混用有 ~0.3% 的 qfq 基准偏移，**低于守卫容差 `KLINE_DRIFT_TOL=0.005`**，因此增量追加时
**不会被自动检测**，会形成「历史 akshare 基准 + 近期 hithink 基准」的混合序列。

按项目既有纪律（换源应 `force_refresh` 全量重拉保证单一基准），建议随后做一次：

```bash
KLINE_BACKEND=hithink python scripts/<全量重拉脚本>   # 4372 只，需稳定网络
```

未做之前，信号层的小幅偏差存在但不致命（0.3% 量级）。**是否现在跑，等你拍板**（耗时较长）。

## 7. 回归测试状态

`pytest` 全家桶（排除 test_walk_forward）→ **309 passed**。
`verify_cloud_requirements.py` → 全通过。

`tests/test_walk_forward.py` → **6 passed / 1 FAILED**：

```text
FAILED tests/test_walk_forward.py::test_sweep_returns_all_configs
  assert max(g["diff"] for g in grid) > 0, "walk-forward 网格无任何配置跑赢等权"
  assert -1.18 > 0
```

**已排除「本轮换源」为原因**，证据两条：

1. `conftest.py` 不加载 `.env`（全仓只有 `backend/main.py`、`scripts/notify_holdings_analysis.py` 调
   `load_dotenv`），故 pytest 进程内 `HITHINK_FINANCE_API_KEY` 为空 → `hithink.available()` 为 False →
   所有新加的 hithink 分支在第一行就 `return None`、原样落到旧源，**代码路径与改前逐字节等价**。
2. `.workbuddy/verify_benchmark_source_invariance.py` 逐日硬测：把指数源强制成 hithink vs baostock，
   对 104 个信号日 × 3 个持有期共 **312 对**基准值比对 → **max_abs_diff = 2.0e-04 个百分点**（纯浮点舍入）。
   而失败幅度是 **−1.18 个百分点**，相差约 6000 倍 → 换源在数学上无法解释该失败。

真实原因指向**数据集扩展后的既有漂移**（该测试自身 docstring 已写明：自适应权重 OOS 单调性「非跨 regime
稳健」、edge 处噪声级；09-13 起 `k_data` 被用户本地扩量 + 新增 09-09~09-11 信号日 + 当前 regime =
`下行防御`、hs300 近 20 日 −5.51%）。**如何处置（放宽/退役该断言，或调查正则化假设）等你决定**，
本轮不动。

## 8. 回滚方式

| 想回滚的东西 | 操作 |
|---|---|
| 全环境 K 线后端 | `.env` / `render.yaml` / `backend/main.py` 的 `KLINE_BACKEND` 改回 `akshare` |
| 仅基本面因子源 | 设 `FUNDAMENTAL_SOURCE=baostock`（同时会让 `rg` 回到「恒缺失」的旧行为） |
| `revenue_growth` 单位 | `_HITHINK_IND_MAP` 里改回 `1.0`（**不建议**，见 §3.3） |
| 指数端点分片 | 属纯修复，不建议回滚 |
| 报价首选源 | 删 `quote.py` 中 hithink 分支即可回到 tdx/新浪 |

## 9. 新环境变量

| 变量 | 默认 | 作用 |
|---|---|---|
| `HITHINK_FUND_YEARS` | `4` | 质量/成长逐期抓取的年数（≈ years×4 次调用/只，600519 实测 14 次）。只做实时因子、不需长历史 PIT 时可调小提速 |
| `HITHINK_TIMEOUT` | `20` | 单次请求超时（秒） |
| `FUNDAMENTAL_SOURCE` | `hithink` | 基本面质量/成长取数源，设 `baostock` 回滚 |

> ⚠️ **性能提醒**：同花顺财务指标端点是**逐报告期**取数，单只 ~14 次 HTTP 往返，而 baostock 只需 2 次
> 批量查询。全市场级 `refresh_fundamentals` 会明显变慢（基本面缓存是预填 + 低频刷新，日常选股读缓存
> 不受影响）。若发现刷新任务超时，优先调小 `HITHINK_FUND_YEARS` 或改回 `FUNDAMENTAL_SOURCE=baostock`。
>
> ⚠️ **缓存优先**：`fetch_fundamental()` 是**缓存优先**的（`stock_data/fundamental_cache/*.json`），
> 所以换源的效果**不会立刻体现**——已缓存个股（如浦发 600000 缓存里只有 `roe`、无毛利率/营收增速）
> 仍走旧缓存，直到该票缓存被刷新（`force=True` 或缓存过期）。全市场口径切换需等一轮基本面刷新。

## 10. 验证过程的工作区副作用（已还原）

出沙箱验证时 `/api/dashboard` 触发了一次基本面缓存构建，写入了 `stock_data/fundamental_cache/600000.json`；
另发现 `000001.json` 处于「已删除」状态。二者均**已 `git checkout --` 还原到 HEAD**（本轮再次复核，
`git status -- stock_data/fundamental_cache/` 已干净），工作区只保留本轮代码改动。

注意：**沙箱内实际上是通网的**（实测 `fuyao.aicubes.cn` 0.4s 返回 200），所以验证不必刻意出沙箱。

> ⚠️ `stock_data/k_data/*.parquet` 三个文件在 `git status` 里显示为已修改（b00 100MB→109MB、b60_0
> 52→57MB、b60_3 39→42MB）——这是**你 09-13 的本地扩量改动，非本轮产生**，提交时勿混入。

### 10.1 ✅ 已定位并根治「真缓存被删」的元凶

两次出现 `fundamental_cache/000001.json`、`600000.json` 被删（`git status` 显示 `D`）后，已查到根因
**不是**验证脚本，而是 `tests/test_factor_fundamentals.py`：
它的 `_seed_cache()` / `_clear_cache()` 直接读写的正是**仓库里带真实数据的目录**
`stock_data/fundamental_cache/`，写入伪造缓存后又在 teardown 里把它们删掉 →
**每跑一次该测试就删掉两个提交进 git 的真缓存**（`_clear_cache` 原先用 `DeleteFileW` 强删，
连回收站都没有）。

已修：新增 `autouse` 夹具 `_isolate_cache`，用 monkeypatch 把 `fundamental.CACHE_DIR` /
`fundamental.SPOT_FILE` 指到 pytest 的 `tmp_path`（这两个都是模块级全局量、被各函数直接引用，
故补丁有效），`_clear_cache` 也改为普通 `unlink()`。
复跑 `test_factor_fundamentals.py + test_fundamental_pit.py` → **9 passed**，且
`git status -- stock_data/fundamental_cache/` **保持干净**（验证夹具真生效）。

## 11. 另外两处「被换源暴露」的缺陷（已修）

| 文件 | 问题 | 修法 |
|---|---|---|
| `scripts/notify_holdings_analysis.py` | 持仓报告的 chips 区把 `roe`/毛利率/营收增速直接拼 `"%"`，**漏了 ×100** → 一律显示 `0.0%`（与其下方 `_fund_panel_html` 的 `roe*100` 自相矛盾）。原先 `revenue_growth` 恒缺失所以不显眼，换源激活后变成每日可见 | chips 三处补 `× 100`（ROE / 毛利 / 营收增速） |
| `smcore/strategy/fundamental.py` | `revenue_growth` 单位换算 `×1.0`（见 §3.3） | 改 `×0.01` |

> `_fund_panel_html` 里仍**没有**「营收增速」行（只有 ROE/毛利率/换手/成交额），本次未动
> （属功能增补，非缺陷）；如需在持仓面板里展示增速可另行补。
