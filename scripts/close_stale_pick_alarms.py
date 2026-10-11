#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""自动收敛 daily-pick 的过期/误报告警 issue。

背景
----
``daily-pick.yml`` 的告警步骤在「无策略产出且无候选」时**开** issue，却**没有任何关闭
路径**，于是 issue 只会越积越多（2026-10-11 实测积压 9 条，其中 #4~#9 是人工在 09-30
批量关掉的）。两类必然产生的过期告警：

  1. **休市日误报**——假期整周被当成交易日跑，链路在数据新鲜度门控处硬失败
     （2026 国庆 4 条：#10/#11/#14/#15）；
  2. **早槽失败 / 晚槽补齐**——告警已开，而当天其实后来产出了清单
     （2026-09-21 ~ 09-24 四条）。

本脚本在每次**成功**运行收尾时按标题里的信号日逐条判定并收敛：

  ============================  ==========================================
  信号日 == 今天                 跳过（当天告警留给人工与后续槽位）
  信号日为非交易日（休市）        关闭 —— 该日按定义无信号，属误报
  信号日是交易日且 DAL 已落库     关闭 —— 已补齐
  信号日是交易日但 DAL 缺失       保留 —— 真实缺口，需人工补跑
  ============================  ==========================================

用法::

    python scripts/close_stale_pick_alarms.py --today 20261011
    python scripts/close_stale_pick_alarms.py --today 20261011 --dry-run   # 只看判定

依赖 ``gh``（CI 里已登录；token 由 ``GH_TOKEN`` 注入）。**失败不致命**：本脚本属收尾
运维动作，任何异常都只打 ``::warning::`` 并 return 0，绝不因它把当天的选股链路判红。
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 标题形如「daily-pick 故障：20261007 无策略产出与候选股」
#     「daily-pick 推送校验失败：20261007 数据可能未落库」
TITLE_RE = re.compile(r"daily-pick\s*(?:故障|推送校验失败)\s*[：:]\s*(\d{8})")


def _gh(args: list[str], *, dry_run: bool = False) -> str:
    """跑一条 gh 命令，返回 stdout。``--dry-run`` 时只回显不执行（仅用于写操作）。"""
    if dry_run:
        print(f"    [dry-run] gh {' '.join(args)}")
        return ""
    proc = subprocess.run(["gh"] + args, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args)} 失败: {proc.stderr.strip()[:300]}")
    return proc.stdout


def _dal_exists(day: str) -> bool:
    """该信号日的操作清单是否已落库（用本地 checkout 判定——本步骤在 CI 里有检出）。"""
    return (ROOT / "stock_data" / f"Daily-Action-List-{day}.csv").exists()


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="收敛 daily-pick 过期/误报告警")
    ap.add_argument("--today", default="", help="今天 YYYYMMDD（默认 = 今天）")
    ap.add_argument("--repo", default="", help="owner/repo（默认由 gh 从 git remote 推断）")
    ap.add_argument("--label", default="daily-pick-failure", help="告警 label")
    ap.add_argument("--limit", type=int, default=100, help="一次拉取的 open issue 上限")
    ap.add_argument("--dry-run", action="store_true", help="只打印判定，不改动任何 issue")
    args = ap.parse_args(argv)

    today = (args.today or datetime.now().strftime("%Y%m%d")).strip()
    if not re.fullmatch(r"\d{8}", today):
        print(f"::warning::[close-alarm] 非法 --today {today!r}，跳过")
        return 0
    repo_args = ["--repo", args.repo] if args.repo else []

    try:
        from smcore.data.trading_calendar import describe_closed, is_trading_day
    except Exception as exc:  # noqa: BLE001 —— 收尾运维动作，不值得把当天链路判红
        print(f"::warning::[close-alarm] 交易日历不可用（{type(exc).__name__}: {exc}），跳过")
        return 0

    try:
        raw = _gh(["issue", "list", *repo_args, "--label", args.label, "--state", "open",
                   "--limit", str(args.limit), "--json", "number,title"])
        issues = json.loads(raw or "[]")
    except Exception as exc:  # noqa: BLE001
        print(f"::warning::[close-alarm] 拉取 issue 失败（{type(exc).__name__}: {exc}），跳过")
        return 0

    closed = kept = skipped = unmatched = 0
    for item in issues:
        title = item.get("title", "")
        m = TITLE_RE.search(title)
        if not m:
            unmatched += 1
            continue
        day = m.group(1)
        number = item["number"]

        if day >= today:
            print(f"  #{number} {day} → 保留（当日/未来，交给后续槽位与人工）")
            skipped += 1
            continue

        try:
            day_date = datetime.strptime(day, "%Y%m%d").date()
        except ValueError:
            print(f"  #{number} {day} → 保留（日期无法解析）")
            kept += 1
            continue

        if not is_trading_day(day_date):
            reason = f"该日非 A 股交易日（{describe_closed(day_date)}），当日按定义不产信号，告警为误报。"
            action = "close"
        elif _dal_exists(day):
            reason = f"该日清单已补齐落库（Daily-Action-List-{day}.csv 已存在），告警已过期。"
            action = "close"
        else:
            print(f"  #{number} {day} → 保留（交易日但清单仍缺失 = 真实缺口）")
            kept += 1
            continue

        print(f"  #{number} {day} → 关闭（{reason}）")
        if action == "close":
            _gh(["issue", "comment", str(number), *repo_args, "--body",
                 f"自动收敛：{reason}\n\n_由 daily-pick 收尾步骤 "
                 f"`scripts/close_stale_pick_alarms.py` 于 {datetime.now():%Y-%m-%d %H:%M} 处理。_"],
                dry_run=args.dry_run)
            _gh(["issue", "close", str(number), *repo_args, "--reason", "not planned"],
                dry_run=args.dry_run)
            closed += 1

    print(f"[close-alarm] 信号日={today} 扫描 {len(issues)} 条 open 告警 → "
          f"关闭 {closed} / 保留 {kept} / 跳过 {skipped} / 未匹配 {unmatched}"
          + ("（dry-run，未做任何改动）" if args.dry_run else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
