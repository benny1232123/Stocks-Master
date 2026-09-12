#!/usr/bin/env python3
"""连夜重建编排器（2026-09-12）——一次跑完四件事，产物直接补全网站数据：

  Step1  归档旧 Multi-Backtest-*（不同代码时代的混合产物）到 stock_data/archive/backtest_v1/
  Step2  用修复后的引擎全量重跑全部历史信号日（HOLD_DAYS=12, LOOKBACK_DAYS=400）
  Step3  按策略族测持有期分档（训练/验证分离）     → measure_reports/hold_by_family.json
  Step4  价格带 5~30 验证（全样本价格桶）          → measure_reports/price_band.json
  Step5  布林 k 分 regime 敏感性                   → measure_reports/boll_k.json
  Step6  写总结报告 + git 回写仓库（Render 自动部署 → 网站回测/日报数据补全）

每步独立容错（失败不阻断后续步骤），全程日志落 stock_data/overnight_logs/。
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from smcore.artifacts import STOCK_DATA_DIR

PY = sys.executable
LOG_DIR = STOCK_DATA_DIR / "overnight_logs"
REPORT_DIR = STOCK_DATA_DIR / "measure_reports"
ARCHIVE_DIR = STOCK_DATA_DIR / "archive" / "backtest_v1"
T0 = time.time()
RESULTS: list[dict] = []


def log(msg: str) -> None:
    stamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{stamp}] {msg}"
    print(line, flush=True)


def run_step(name: str, cmd: list[str], timeout_s: int, env_extra: dict | None = None) -> dict:
    log(f"▶ {name} 开始: {' '.join(cmd[:4])}...")
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    if env_extra:
        env.update(env_extra)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{name}.log"
    t = time.time()
    rec: dict = {"step": name, "cmd": " ".join(cmd)}
    try:
        with open(log_path, "w", encoding="utf-8") as fh:
            proc = subprocess.run(cmd, cwd=str(ROOT), env=env, stdout=fh,
                                  stderr=subprocess.STDOUT, timeout=timeout_s)
        rec["rc"] = proc.returncode
        rec["ok"] = proc.returncode == 0
    except subprocess.TimeoutExpired:
        rec["rc"] = -1
        rec["ok"] = False
        rec["error"] = f"超时（>{timeout_s}s）"
    except Exception as exc:
        rec["rc"] = -1
        rec["ok"] = False
        rec["error"] = repr(exc)
    rec["seconds"] = round(time.time() - t, 1)
    rec["log"] = str(log_path)
    RESULTS.append(rec)
    log(f"■ {name} 结束: ok={rec['ok']} 用时 {rec['seconds']}s"
        + (f" 错误={rec.get('error')}" if not rec["ok"] else ""))
    return rec


def step2_apply_validated_holds() -> dict:
    """读 hold_by_family.json，把「验证通过」的分档值写进 risk_config.json。

    纪律：只采纳 recommendation.validated=true 的策略（训练段最优且验证段
    不劣于全局 12 日）；其余维持全局。写配置而非环境变量——分档值就此成为
    生产行为的一部分，重跑/实盘共用同一份。
    """
    src = REPORT_DIR / "hold_by_family.json"
    applied: dict = {}
    try:
        rep = json.loads(src.read_text(encoding="utf-8"))
    except Exception as exc:
        log(f"Step2.5 分档报告不可读（{exc}），全部维持全局 12")
        return applied
    cfg_path = ROOT / "smcore" / "strategy" / "risk_config.json"
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    by_strat = dict((cfg.get("exit", {}) or {}).get("hold_days_by_strategy") or {})
    for strat, b in rep.get("buckets", {}).items():
        rec = b.get("recommendation", {})
        if rec.get("validated") and rec.get("suggested_hold"):
            by_strat[strat] = int(rec["suggested_hold"])
            applied[strat] = int(rec["suggested_hold"])
    cfg.setdefault("exit", {})["hold_days_by_strategy"] = by_strat
    cfg_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    log(f"Step2.5 应用持有分档: {applied or '无（均未通过验证，维持全局 12）'}")
    RESULTS.append({"step": "apply_holds", "ok": True, "applied": applied})
    return applied


def step1_archive() -> None:
    moved = 0
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    for f in STOCK_DATA_DIR.glob("Multi-Backtest-*.csv"):
        shutil.move(str(f), str(ARCHIVE_DIR / f.name))
        moved += 1
    log(f"Step1 归档 {moved} 个旧回测文件 → {ARCHIVE_DIR.relative_to(ROOT)}")
    RESULTS.append({"step": "archive", "ok": True, "moved": moved})


def step6_git_push() -> None:
    # 注意：旧文件已被移入 archive（本地保留，不入库）——必须让 git 自己解析 pathspec
    # 才能把「根目录删除 + 新文件」一起暂存；shell 展开的 glob 在文件消失后匹配不到、
    # 会漏掉删除标记，导致网站出现新旧混合数据。
    paths = [
        "stock_data/Multi-Backtest-*.csv",
        "stock_data/measure_reports/",
        "stock_data/overnight_report_*.md",
    ]
    git_env = {"GIT_AUTHOR_NAME": "overnight-rebuild", "GIT_AUTHOR_EMAIL": "overnight@local",
               "GIT_COMMITTER_NAME": "overnight-rebuild", "GIT_COMMITTER_EMAIL": "overnight@local"}
    add_cmd = ["git", "add", "-A", "-f", "--"] + paths
    subprocess.run(add_cmd, cwd=str(ROOT), capture_output=True)
    diff = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=str(ROOT), capture_output=True)
    if diff.returncode == 0:
        log("Step6 无数据变更，跳过提交")
        RESULTS.append({"step": "git", "ok": True, "note": "no changes"})
        return
    subprocess.run(["git", "commit", "-m",
                    "data: 连夜重建——全量重跑历史回测（新引擎口径）+ 三项测量报告"],
                   cwd=str(ROOT), env={**os.environ, **git_env}, capture_output=True)
    pushed = False
    for i in range(1, 4):
        r1 = subprocess.run(["git", "pull", "--rebase", "--autostash", "origin", "master"],
                            cwd=str(ROOT), capture_output=True)
        r2 = subprocess.run(["git", "push", "origin", "master"], cwd=str(ROOT), capture_output=True)
        if r2.returncode == 0:
            pushed = True
            break
        time.sleep(i * 5)
    RESULTS.append({"step": "git", "ok": pushed, "note": "pushed" if pushed else "push failed（可手动重推）"})
    log(f"Step6 git 回写: {'成功' if pushed else '失败（数据已提交本地，可手动 push）'}")


def main() -> int:
    log("════ 连夜重建开始（顺序：先测出自适应分档 → 再用分档跑全量）════")
    step1_archive()

    # ① 先测量：持有期分档建议（不依赖回测归档）
    run_step("hold_by_family", [PY, "-u", str(ROOT / "scripts" / "measure_hold_by_family.py")], timeout_s=3 * 3600)

    # ② 应用验证通过的分档值到 risk_config（无通过项 = 维持全局 12）
    step2_apply_validated_holds()

    # ③ 再跑全量重回测——daily_backtest 子进程此刻读取的就是分档后的配置
    run_step("rebacktest", [PY, "-u", str(ROOT / "scripts" / "daily_backtest.py")],
             timeout_s=6 * 3600, env_extra={"HOLD_DAYS": "12", "LOOKBACK_DAYS": "400"})

    # ④ 其余两项测量（与回测归档无依赖）
    run_step("price_band", [PY, "-u", str(ROOT / "scripts" / "measure_price_band.py")], timeout_s=60 * 60)
    run_step("boll_k", [PY, "-u", str(ROOT / "scripts" / "measure_boll_k.py")], timeout_s=2 * 3600)

    # ── 总结报告 ──
    lines = [
        "# 连夜重建报告",
        "",
        f"- 运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}（总耗时 {(time.time() - T0) / 60:.0f} 分钟）",
        f"- 重跑口径: 修复后引擎 / HOLD_DAYS=12 / 全部历史信号日",
        "",
        "| 步骤 | 状态 | 用时 |",
        "|---|---|---|",
    ]
    for r in RESULTS:
        status = "✅" if r.get("ok") else "❌"
        extra = r.get("error") or r.get("note") or (f"moved={r['moved']}" if "moved" in r else "")
        lines.append(f"| {r['step']} | {status} | {r.get('seconds', '-')}s | {extra} |")
    applied = next((r.get("applied") for r in RESULTS if r["step"] == "apply_holds"), {})
    lines += ["", f"## 本次应用的持有分档: {applied or '无（均未通过验证）'}", "",
              "## 产物", "",
              "- 历史回测: stock_data/Multi-Backtest-*/（新口径，已回写仓库 → 网站回测 Tab）",
              "- 旧口径归档: stock_data/archive/backtest_v1/（对照用，勿与新数字直接比较）",
              "- hold_by_family.json: 持有期分档建议（validated=true 才建议配置）",
              "- price_band.json: 价格带 5~30 验证结论",
              "- boll_k.json: 分 regime 的 k 建议（填 boll.k_by_vol_regime 用）",
              "", "> 注意：新数字由修复后的引擎产出（MA60 出场生效/ST 剔除/动态阈值），",
              "> 与旧归档不可直接比较——变差是挤掉水分，不是策略退化。", ""]
    (STOCK_DATA_DIR / "overnight_report.md").write_text("\n".join(lines), encoding="utf-8")
    (STOCK_DATA_DIR / f"overnight_report_{datetime.now().strftime('%Y%m%d')}.md").write_text(
        "\n".join(lines), encoding="utf-8")
    log("总结报告已写 stock_data/overnight_report.md")

    step6_git_push()
    log(f"════ 连夜重建结束（总耗时 {(time.time() - T0) / 60:.0f} 分钟）════")
    return 0


if __name__ == "__main__":
    sys.exit(main())
