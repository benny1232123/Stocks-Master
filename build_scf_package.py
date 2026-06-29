"""打包 SCF 部署包 —— 把 smcore + scf_alert.py 打成 zip。

用法：python build_scf_package.py
输出：scf_alert_package.zip（直接上传到腾讯云函数）

注意：新浪行情只需 requests（SCF 预装），无需 akshare/baostock，
所以包很小，直接打 smcore 源码即可。
"""
from __future__ import annotations

import zipfile
from pathlib import Path

REPO = Path(__file__).resolve().parent
OUTPUT = REPO / "scf_alert_package.zip"


def main() -> None:
    # 要打包的路径
    smcore_dir = REPO / "smcore"
    scf_entry = REPO / "scf_alert.py"

    if not smcore_dir.exists():
        print("smcore 目录不存在")
        return
    if not scf_entry.exists():
        print("scf_alert.py 不存在")
        return

    with zipfile.ZipFile(OUTPUT, "w", zipfile.ZIP_DEFLATED) as zf:
        # SCF 入口
        zf.write(scf_entry, "scf_alert.py")

        # smcore 包（只打包 .py，排除 __pycache__）
        for py in smcore_dir.rglob("*.py"):
            if "__pycache__" in py.parts:
                continue
            arcname = py.relative_to(REPO)
            zf.write(py, arcname)
            print(f"  + {arcname}")

    size_kb = OUTPUT.stat().st_size / 1024
    print(f"\n打包完成: {OUTPUT} ({size_kb:.1f} KB)")
    print("上传到腾讯云函数：")
    print("  1. 登录 https://console.cloud.tencent.com/scf")
    print("  2. 新建函数 → Python 3.9 → 上传 zip")
    print("  3. 入口：scf_alert.main_handler")
    print("  4. 环境变量：WECOM_WEBHOOK_URL / COS_SECRET_ID / COS_SECRET_KEY / COS_BUCKET / COS_REGION")
    print("  5. 触发器：定时 → cron(0/10 * 9-15 * * MON-FRI)")
    print("  详见 DEPLOY_SCF.md")


if __name__ == "__main__":
    main()
