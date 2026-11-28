#!/usr/bin/env bash
set -euo pipefail

# Azure 云服务器每日收盘全自动调度脚本（北京时间 15:05 / UTC 07:05 执行）
# 替代原 GitHub Actions 定时任务：零队列延迟，准点收盘后更新全部回测数据并发送策略变动邮件

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] === Azure 每日收盘任务启动 ==="

# 1. 拉取最新代码与配置
git pull --ff-only origin master || echo "[WARN] git pull skipped"

# 2. 更新 8 大前沿策略 + JEV 综合净值曲线至今日收盘
python3 scripts/generate_all_eight_frontier_curves.py

# 3. 更新 2020~至今 穿越牛熊拯救算法全周期回测数据至今日收盘
python3 scripts/build_full_cycle_rescue_backtest.py

# 4. 检测一票一策策略切换 / 新成交基准价 / ATR波动变化并按需发送邮件
python3 scripts/send_rescue_action_plan.py

# 5. 若回测数据有更新则自动提交推送（触发静态站点同步）
git add stock_data/index_cache/sh000300_full.csv frontend/src/*_curves.json stock_data/web_data/*_curves.json frontend/src/data/rescueBacktestData.js || true
if ! git diff --cached --quiet; then
  git commit -m "chore(azure): daily close backtest update $(date +%Y%m%d)"
  git push origin master || echo "[WARN] git push failed"
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] === Azure 每日收盘任务完成 ==="
