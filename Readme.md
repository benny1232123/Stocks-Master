使用前更新：pip install akshare --upgrade -i https://pypi.org/simple

震荡市/股：布林带下线到中线
趋势市/股：斜率倾斜波动向上

---

Streamlit Community Cloud 发布（固定外网链接）

1. 将当前仓库推送到 GitHub（公开或私有均可）。
2. 打开 https://share.streamlit.io 并登录 GitHub。
3. 点击 New app，选择你的仓库与分支。
4. Main file path 选择：streamlit_app.py
5. 点击 Deploy，等待安装依赖并启动。
6. 部署完成后会得到固定访问链接（形如 https://stock-master-benny.streamlit.app）。

说明：
- 根目录 requirements.txt 已指向子项目依赖。
- 根目录 requirements-dev.txt 包含测试依赖（如 pytest）。
- runtime.txt 已锁定 Python 3.11，降低依赖兼容问题。
- 页面入口会自动转到 Frequently-Used-Program/boll-visualizer/src/app.py。

---

每日自动运行并推送结果（Windows）

已提供脚本：
- `Frequently-Used-Program/auto_notify_boll.py`：执行 `Stock-Selection-Boll.py`，读取当日结果 CSV，并推送消息。
- `run-boll-auto-notify.bat`：手动执行一次（便于先验证）。
- `register-boll-daily-task.bat`：注册 Windows 每日计划任务（中午 12:00 + 晚上 19:00），并启用“错过后尽快运行”。

推荐推送方式一：企业微信机器人（最简单）

1. 在企业微信群中添加“群机器人”，复制 Webhook 地址。
2. 在系统环境变量中新增：
	- `WECOM_WEBHOOK_URL` = 你的 webhook 地址
3. 双击 `run-boll-auto-notify.bat` 先手动验证。
4. 双击 `register-boll-daily-task.bat` 创建每日任务。

可选推送方式二：邮件（支持附带当日 CSV）

设置以下环境变量（全部都要填）：
- `SMTP_HOST`
- `SMTP_PORT`（常见 465）
- `SMTP_USER`
- `SMTP_PASS`
- `SMTP_TO`

说明：
- 日志会写入 `stock_data/auto_logs/`。
- 结果 CSV 仍在 `stock_data/Stock-Selection-Boll-YYYYMMDD.csv`。
- 若同时配置企业微信和 SMTP，会同时尝试推送。
- 若 12:00 或 19:00 时电脑关机，开机后会自动尽快补跑（StartWhenAvailable）。

后台管理与快速测试

新增管理脚本：
- `configure-email-smtp.bat`：交互式写入 SMTP 环境变量（用户级）。
- `test-email-notify.bat`：仅测试 SMTP 邮件，不跑选股。
- `check-boll-daily-task.bat`：查看中午/晚间两个计划任务状态（上次/下次运行、结果码）。
- `run-boll-daily-task-now.bat`：立即触发中午/晚间两个计划任务。

推荐测试流程：
1. 先运行 `configure-email-smtp.bat` 写入 SMTP 环境变量（SMTP_HOST/PORT/USER/PASS/TO）。
2. 运行 `test-email-notify.bat`，确认邮箱可收到测试邮件。
3. 运行 `register-boll-daily-task.bat` 注册每日任务。
4. 运行 `run-boll-daily-task-now.bat` 立即触发，验证全流程。
5. 运行 `check-boll-daily-task.bat` 检查任务结果码。

若测试失败：
- 查看 `stock_data/auto_logs/` 最新日志。
- 日志会显示缺失的 SMTP 变量名（例如缺 `SMTP_TO`）。

高级：
- 命令行测试全部推送（企业微信 + 邮件）：
	`python Frequently-Used-Program/auto_notify_boll.py --test-notify`
- 自定义测试邮件标题：
	`python Frequently-Used-Program/auto_notify_boll.py --test-email-only --subject "SMTP联调测试"`
