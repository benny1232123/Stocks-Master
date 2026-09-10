# 量化策略全参数连续自适应改造设计

> 日期：2026-09-10
> 状态：已批准，待实施

## 背景与目标

聚合回测（Multi-Backtest-AGGREGATE.csv，2025-08~2026-09，60+ 信号日）显示当前策略：

- 强势期（2025-10~2026-02）胜率 80-100%、超额最高 +6.2%
- 下跌市（2026-05/06）单日亏 3-5%、超额 -6.6%~-8.5%
- 单信号日样本仅 2~26 笔，指标噪声大
- capital_scale 在熊市仍 0.84~0.98（几乎满仓扛跌）
- avg_loss 达 -10%~-18%，avg_win 仅 4%~10%，盈亏比常 <1
- walk-forward 已验证：shrinkage=0/FLOOR=0 相对等权 +5.35pp（唯一跑赢等权的配置），FLOOR 是主要拖累

**目标**：把全部风险/权重参数改为连续函数式自适应（非档位切换），统一用一个动态引擎驱动，熊市真正减仓、长期正期望、指标稳定。所有改动通过连续组合回测 A/B 验证。

## 设计约束铁律

1. **中性点=现值**：市场中性时（vol≈0.5 分位、震荡 regime、dd≈0）所有自适应函数返回 risk_config.json 现有值，改动可回退、A/B 可对比。
2. **连续单调、无跳变**：参数随市场状态连续单调变化，杜绝边界跳变和换仓抖动。
3. **同样证据，同样结论**：参数函数可复现、可测试。

## 三阶段总览

| 阶段 | 内容 | 产物 | 验证 |
|---|---|---|---|
| 0 | 连续组合回测框架 | scripts/continuous_backtest.py + 指标报告 | 已知改动跑通 A/B 差异 |
| 1 | 权重连续自适应 | adaptive_weights.py 动态 shrinkage | 四形态 A/B |
| 2 | 动态风险引擎 | smcore/strategy/dynamic_risk.py | 静态 vs 动态全期 A/B |

每阶段独立 commit + 对比报告存 `stock_data/strategy_improve/`。

## 阶段0：连续组合回测框架

**现状问题**：daily_backtest.py 每个信号日独立回测，initial_capital=10万各自起跑，无法度量连续年化/回撤/夏普。

**设计**：新增 `scripts/continuous_backtest.py`，复用 daily_backtest.py 的 `run_forward_signal_backtest` 内核：
- 按时间顺序加载历史全部信号日
- 组合漂移：当日信号票买入 + 持有满 hold_days 的旧票卖出，同一条权益曲线连续滚动
- 组合层应用与生产一致的现金逻辑（volatility + regime + dd breaker）
- 输出：`stock_data/Continuous-Backtest-{end}.csv` 连续净值 + 指标报告（年化、最大回撤、夏普、胜率、超额/基准、月度明细）
- 支持 A/B 模式：同一批信号日跑 旧参数 vs 新参数

**验收**：跑通阶段1的四形态对比，指标能反映差异。

## 阶段1：权重连续自适应

**现状**（adaptive_weights.py）：
- shrunk = edge * n/(n+pseudo=15)，再乘置信折扣 sqrt(n/min_n_confident=30)
- softmax(shrunk/temp=0.5)；向等权收缩 w=(1-shrinkage=0.4)*raw + shrinkage*uni
- FLOOR=3.0 保底后重归一

**机制变更**：
- FLOOR 固定置 0（已验证主要拖累）
- shrinkage 常数 0.4 → 连续函数由证据强度驱动：

```
置信度 c = n / (n + pseudo=15)
显著度 s = min(1, |t-stat| / t_target=2.0)
shrinkage_eff = base_shrink × (1 - c × s)
weight = (1-shrinkage_eff) × softmax(shrunk/temp) + shrinkage_eff × 等权
```

**含义**：
- 冷启动/样本不足 → shrinkage_eff≈0.5 → 自动等权（替代 n<min_n 的硬开关）
- edge 显著且样本充足 → shrinkage_eff→0 → 完全信自适应权重
- 中间平滑过渡

**验证**：A/B 四形态：现值(FLOOR3+shrink0.4) vs 固定0 vs 动态shrinkage vs 动态+FLOOR0。

## 阶段2：动态风险引擎

**新增 `smcore/strategy/dynamic_risk.py`**，统一动态风险引擎。

**输入**（市场状态，全连续量）：
- volatility_pctile（波动率分位 0~1）
- regime（趋势上行/下行防御/震荡）
- drawdown（组合回撤深）
- 市场 RS / 趋势强度

**输出** DynamicParams：
```
stop_loss_pct   take_profit_pct   trailing_stop_pct
hold_days       max_position_pct  sector_cap
beta_target     cash_pct
```

**自适应函数形式**（每参数连续单调，中性点=现值）：

| 参数 | 自变量 | 逻辑 |
|---|---|---|
| stop_loss_pct | vol_pctile, 个股平均vol20 | 波动率高→止损放宽防噪声误杀 |
| take_profit_pct | vol_pctile | 止盈止损比恒 ≥1.2 |
| hold_days | vol_pctile, dd | 高波/深跌→缩短持有期 |
| max_position_pct | vol_pctile, regime | 高波/下行→单票上限收窄 |
| cash_pct | 现三函数(f₅ vol+f₆ regime+f₇ dd) | 统一进框架，并补"双重确认急降仓"规则 |

**强制下线规则**（组合层面）：regime=下行防御 且 vol_pctile>0.7 → capital_scale ≤ 0.3。需两个独立信号同时确认才触发，避免误判。

**实现位置**：risk_rules.py / daily_backtest.py / fusion.py 的参数读取点改从 dynamic_risk.compute(params) 取，不直接读 risk_config.json 固定值。

**验证**：阶段0连续回测"静态配置 vs 动态引擎"全期 A/B，2026-05/06 熊市目标超额从 -8% 拉回 -3% 以内，且不牺牲 2025-10~2026-02 强势期收益。

## 风险与对策

- 过拟合：全部决策通过 walk-forward 样本外 + 连续回测 A/B 验证，参数扫描限 2° 网格
- 改动过大：三阶段各自独立 commit、可单独回退
- 熊市急降仓误判：强制下线需要双信号确认
- 冷启动数据不足：权重自动回等权、参数自动回中性点

## 验收总表

全部完成后汇总「改动前 vs 改动后」总对比表：年化、最大回撤、夏普、胜率、盈亏比、超额/基准、熊市段超额。