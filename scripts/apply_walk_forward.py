#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""月度 walk-forward 重验 CI 应用脚本。

读取 `walk_forward_validator.py recommend()` 产出的 JSON：
- 若 robust=True（改进幅度 ≥ 阈值 且 单调 且 前后半段稳定），则：
    1. 用 save_config() 把推荐 (shrinkage, FLOOR) 写回
       smcore/strategy/adaptive_weights_config.json（其余超参保持不变）；
    2. 在当前 master 上开一个独立分支，只提交该配置文件；
    3. 推送分支并开 PR（若同分支已有 open PR 则跳过创建，仅更新文件）。
- 若 robust=False：打印原因并退出 0（不开 PR），不改动任何生产文件。

设计为「幂等 + 安全」：
- 只碰 adaptive_weights_config.json 一个文件，绝不动策略/回测代码；
- 任何一步失败都打印错误并以非零码退出，让 CI 该步标红（但不影响后续）；
- --dry-run 时只改本地文件、不开分支不开 PR（便于本地演练）。
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy.adaptive_weights import CONFIG, save_config  # noqa: E402


def _run(cmd, check=True, capture=True):
    print("+ " + " ".join(cmd))
    return subprocess.run(cmd, check=check, capture_output=capture, text=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--recommend-json", required=True, help="walk_forward_validator.py --recommend 产出的 JSON")
    ap.add_argument("--branch", default=None, help="PR 分支名（默认 chore/adaptive-weights-YYYYMM）")
    ap.add_argument("--base", default="master", help="目标分支（默认 master）")
    ap.add_argument("--dry-run", action="store_true", help="只改本地配置，不开分支/PR")
    args = ap.parse_args()

    rec = json.loads(Path(args.recommend_json).read_text(encoding="utf-8"))

    if not rec.get("robust"):
        print("[apply] 推荐不稳健，跳过 PR。")
        print("[apply] 原因:", json.dumps(rec.get("checks"), ensure_ascii=False))
        print(f"[apply] 当前={rec.get('current')} 推荐={rec.get('recommended')} 改进={rec.get('improvement_pp')}pp")
        return 0

    if args.dry_run:
        print(f"[apply] --dry-run：将把配置更新为 shrinkage={rec['recommended']['shrinkage']} "
              f"FLOOR={rec['recommended']['floor']}（改进 {rec['improvement_pp']}pp），"
              f"但不写文件/不开分支/不开 PR。")
        return 0

    new_cfg = dict(CONFIG)
    new_cfg["shrinkage"] = rec["recommended"]["shrinkage"]
    new_cfg["FLOOR"] = rec["recommended"]["floor"]
    path = save_config(new_cfg)
    print(f"[apply] 已更新配置: {path}")
    print(f"[apply] shrinkage={new_cfg['shrinkage']} FLOOR={new_cfg['FLOOR']}")

    branch = args.branch or f"chore/adaptive-weights-{os.environ.get('RECOMMEND_MONTH', '')}".strip("-") or "chore/adaptive-weights-retune"

    # ── git 身份（CI 内已设，本地兜底）──
    try:
        _run(["git", "config", "user.name"], check=False)
    except Exception:
        pass
    _run(["git", "config", "user.name", "walk-forward[bot]"])
    _run(["git", "config", "user.email", "walk-forward[bot]@users.noreply.github.com"])

    # ── 分支：基于最新目标分支 ──
    _run(["git", "fetch", "origin", args.base])
    _run(["git", "checkout", "-B", branch, f"origin/{args.base}"], check=False)
    _run(["git", "add", path])
    # 若本就无变更（不应发生），直接退出
    diff = _run(["git", "diff", "--cached", "--quiet"], check=False)
    if diff.returncode == 0:
        print("[apply] 无暂存变更，跳过提交（可能已是最新配置）。")
        return 0
    msg = (f"chore(weights): 月度重验更新超参 shrinkage={new_cfg['shrinkage']} "
           f"FLOOR={new_cfg['FLOOR']} (+{rec['improvement_pp']}pp)")
    _run(["git", "commit", "-m", msg])
    _run(["git", "push", "--force-with-lease", "origin", branch])

    # ── 已有同分支 open PR 则跳过创建 ──
    existing = _run(
        ["gh", "pr", "list", "--head", branch, "--base", args.base,
         "--state", "open", "--json", "number"],
        check=False,
    )
    if existing.returncode == 0 and existing.stdout.strip().strip("[]"):
        print("[apply] 该分支已有 open PR，仅更新文件，跳过重复创建。")
        return 0

    checks = rec.get("checks", {})
    sweep = rec.get("sweep", [])
    top = sorted(sweep, key=lambda x: x["adaptive"], reverse=True)[:5]
    sweep_lines = "\n".join(
        f"| shrinkage={s['shrinkage']} FLOOR={s['floor']} | {s['adaptive']:+.2f}% | "
        f"{s['equal']:+.2f}% | {s['diff']:+.2f}% |" for s in top
    )
    body = f"""## 月度 walk-forward 重验：自适应权重超参更新

自动由 `.github/workflows/walk-forward.yml` 触发，纯样本外、因果、无未来函数。

### 推荐配置
- 当前：`shrinkage={rec['current']['shrinkage']}`, `FLOOR={rec['current']['floor']}`
- 推荐：`shrinkage={rec['recommended']['shrinkage']}`, `FLOOR={rec['recommended']['floor']}`
- 样本外累计改进：**{rec['improvement_pp']:+.2f}pp**（相对当前配置）

### 稳健性检查（三项全过才自动开 PR）
- 改进幅度 ≥ {checks.get('min_improve_pp')}pp：`{checks.get('improve_ok')}`
- 样本外单调性（高权重档 > 低权重档）：`{checks.get('monotonic')}`
- 前后半段稳定性（推荐组合两段均进前 3，rank={checks.get('stable_first_half_rank')}/{checks.get('stable_second_half_rank')}）：`{checks.get('stable_ok')}`

### 网格扫描 Top5（自适应样本外累计收益）
| 配置 | 自适应 | 等权 | 差值 |
|---|---|---|---|
{sweep_lines}

> ⚠️ 本 PR 仅改动 `smcore/strategy/adaptive_weights_config.json` 的 `shrinkage`/`FLOOR` 两项，
> 其余超参与所有策略/回测代码不变。请审阅证据后合并；合并后次日 `每日选股` 即生效。
"""
    _run([
        "gh", "pr", "create",
        "--title", f"chore(weights): 月度重验更新超参 (+{rec['improvement_pp']}pp)",
        "--body", body,
        "--base", args.base,
        "--head", branch,
        "--label", "automated,weights",
    ])
    print("[apply] 已开 PR。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
