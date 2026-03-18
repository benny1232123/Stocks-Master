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
6. 部署完成后会得到固定访问链接（形如 https://xxx.streamlit.app）。

说明：
- 根目录 requirements.txt 已指向子项目依赖。
- runtime.txt 已锁定 Python 3.11，降低依赖兼容问题。
- 页面入口会自动转到 Frequently-Used-Program/boll-visualizer/src/app.py。
