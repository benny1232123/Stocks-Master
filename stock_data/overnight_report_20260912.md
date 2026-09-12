# 连夜重建报告

- 运行时间: 2026-09-12 17:10（总耗时 435 分钟）
- 重跑口径: 修复后引擎 / HOLD_DAYS=12 / 全部历史信号日

| 步骤 | 状态 | 用时 |
|---|---|---|
| archive | ✅ | -s | moved=0 |
| hold_by_family | ❌ | 10800.0s | 超时（>10800s） |
| rebacktest | ✅ | 15002.2s |  |
| price_band | ✅ | 44.6s |  |
| boll_k | ✅ | 272.8s |  |

## 本次应用的持有分档: 无（均未通过验证）

## 产物

- 历史回测: stock_data/Multi-Backtest-*/（新口径，已回写仓库 → 网站回测 Tab）
- 旧口径归档: stock_data/archive/backtest_v1/（对照用，勿与新数字直接比较）
- hold_by_family.json: 持有期分档建议（validated=true 才建议配置）
- price_band.json: 价格带 5~30 验证结论
- boll_k.json: 分 regime 的 k 建议（填 boll.k_by_vol_regime 用）

> 注意：新数字由修复后的引擎产出（MA60 出场生效/ST 剔除/动态阈值），
> 与旧归档不可直接比较——变差是挤掉水分，不是策略退化。
