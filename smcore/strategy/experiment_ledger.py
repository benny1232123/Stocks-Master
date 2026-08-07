#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""实验台账（Experiment Ledger）—— 所有「权重/配置/策略」实验的可审计记录。

设计动机
--------
walk-forward 月度重验、手动调参、策略开关、因子激活……这些动作散落在各脚本里，缺乏统一
的「谁在何时改了什么、为什么、改前改后指标如何、结论采纳还是否决」记录。本模块提供**单一
可信台账**：

1) 追加写（append-only JSONL）：每次实验一笔记录，`append_entry()` 返回稳定 ID，绝不覆盖
   历史；文件损坏/缺失时 fail-soft（返回 None / 空列表）。
2) 记录内容：id / timestamp / author / signal_date / title / hypothesis /
   config_before / config_after / metrics_before / metrics_after / outcome /
   notes。outcome ∈ {pending, adopted, rejected, invalid}。
3) 便捷构造：
   - `record_experiment(...)` 手工/CLI 记录一条假设实验；
   - `record_calibration(rec, signal_date, author)` 由 walk-forward `recommend()`
     的产出自动落一笔「月度重验」台账（推荐配置 + 稳健性判定 + 改进幅度），是否落盘由
     env `STOCKS_LEDGER`（默认开启，`=0` 关闭）控制，且全程 try/except 包裹，
     **绝不**影响生产/验证主流程。
4) 查询与汇总：`load_ledger()` / `filter_ledger(outcome=...)` / `summarize_ledger()`
   （最近 N 条 + 采纳/否决计数 + 按 signal_date 归并），便于周报与审计。

全部 fail-soft，不依赖联网。台账落 stock_data/experiment_ledger.jsonl（已 gitignore）。
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

try:
    from smcore.config.defaults import STOCK_DATA_DIR
except Exception:  # pragma: no cover
    STOCK_DATA_DIR = Path("stock_data")

LEDGER_PATH = STOCK_DATA_DIR / "experiment_ledger.jsonl"
# 落盘总开关（默认开启；env STOCKS_LEDGER=0 关闭）。append_entry 在每次调用时实时读取，
# 故此处仅作文档性常量，不用于运行期门控。
LEDGER_ENABLED = os.environ.get("STOCKS_LEDGER", "1") != "0"

VALID_OUTCOMES = ("pending", "adopted", "rejected", "invalid")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _next_id() -> str:
    """基于已有台账序号生成稳定 ID：EXP-YYYYMMDD-NNN（NNN=当日序号+1）。"""
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    prefix = f"EXP-{today}-"
    n = 0
    try:
        if LEDGER_PATH.exists():
            with open(LEDGER_PATH, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    eid = rec.get("id", "")
                    if eid.startswith(prefix):
                        try:
                            seq = int(eid.split("-")[-1])
                            n = max(n, seq)
                        except Exception:
                            pass
    except Exception:
        pass
    return f"{prefix}{n + 1:03d}"


def _sanitize(entry: dict) -> dict:
    """规整一条记录：补全缺省字段、校验 outcome、序列化不可 JSON 对象。"""
    out = {
        "id": entry.get("id") or _next_id(),
        "timestamp": entry.get("timestamp") or _now_iso(),
        "author": entry.get("author") or "unknown",
        "signal_date": entry.get("signal_date"),
        "title": entry.get("title") or "",
        "hypothesis": entry.get("hypothesis") or "",
        "config_before": entry.get("config_before") or {},
        "config_after": entry.get("config_after") or {},
        "metrics_before": entry.get("metrics_before") or {},
        "metrics_after": entry.get("metrics_after") or {},
        "outcome": entry.get("outcome") or "pending",
        "notes": entry.get("notes") or "",
    }
    if out["outcome"] not in VALID_OUTCOMES:
        out["outcome"] = "pending"
    # 任何不可 JSON 序列化的子对象转 str，保证落盘安全
    for k in ("config_before", "config_after", "metrics_before", "metrics_after"):
        try:
            json.dumps(out[k], default=str)
        except Exception:
            out[k] = str(out[k])
    return out


def append_entry(entry: dict) -> str | None:
    """追加一笔实验记录（append-only）。成功返回 ID，失败返回 None（fail-soft）。

    落盘总开关由 env STOCKS_LEDGER 控制（默认开启，`=0` 关闭），每次调用实时读取，
    便于测试 / 运行时临时禁用而不必重启进程。
    """
    if os.environ.get("STOCKS_LEDGER", "1") == "0":
        return None
    rec = _sanitize(entry)
    try:
        LEDGER_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(LEDGER_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
        return rec["id"]
    except Exception:
        return None


def load_ledger() -> list[dict]:
    """读取全部台账记录（按文件顺序）。损坏行跳过，缺失返回 []。"""
    if not LEDGER_PATH.exists():
        return []
    out = []
    try:
        with open(LEDGER_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except Exception:
                    continue
    except Exception:
        return []
    return out


def filter_ledger(outcome: str = None, signal_date: str = None, author: str = None) -> list[dict]:
    """按 outcome / signal_date / author 过滤。"""
    recs = load_ledger()
    if outcome is not None:
        recs = [r for r in recs if r.get("outcome") == outcome]
    if signal_date is not None:
        recs = [r for r in recs if r.get("signal_date") == signal_date]
    if author is not None:
        recs = [r for r in recs if r.get("author") == author]
    return recs


def record_experiment(title: str, hypothesis: str = "", signal_date: str = None,
                      config_before: dict = None, config_after: dict = None,
                      metrics_before: dict = None, metrics_after: dict = None,
                      outcome: str = "pending", author: str = "manual",
                      notes: str = "") -> str | None:
    """手工/CLI 记录一条假设实验。返回 ID 或 None。"""
    return append_entry({
        "title": title, "hypothesis": hypothesis, "signal_date": signal_date,
        "config_before": config_before or {}, "config_after": config_after or {},
        "metrics_before": metrics_before or {}, "metrics_after": metrics_after or {},
        "outcome": outcome, "author": author, "notes": notes,
    })


def record_calibration(rec: dict, signal_date: str = None, author: str = "walk_forward_validator") -> str | None:
    """由 walk-forward `recommend()` 的产出自动落一笔「月度重验」台账。

    rec 即 recommend() 返回的 dict（含 current/recommended/improvement_pp/robust/checks）。
    outcome 由 robust 推导（True→pending：待人工/CI 采纳；False→rejected）。
    全程 try/except，调用方可安全忽略返回值。
    """
    try:
        cur = rec.get("current") or {}
        rec_cfg = rec.get("recommended") or {}
        checks = rec.get("checks") or {}
        cur_rep = rec.get("current_report") or {}
        outcome = "pending" if rec.get("robust") else "rejected"
        return append_entry({
            "author": author,
            "signal_date": signal_date,
            "title": f"月度 walk-forward 重验（推荐 shrinkage={rec_cfg.get('shrinkage')}, "
                     f"FLOOR={rec_cfg.get('floor')}）",
            "hypothesis": "网格最优(SHRINKAGE×FLOOR)样本外累计收益相对当前配置稳健更优，"
                          "且通过改进幅度/单调性/前后半段稳定性/显著性/跨状态稳健性闸门。",
            "config_before": {"shrinkage": cur.get("shrinkage"), "FLOOR": cur.get("floor")},
            "config_after": {"shrinkage": rec_cfg.get("shrinkage"), "FLOOR": rec_cfg.get("floor")},
            "metrics_before": {
                "adaptive_total_pct": cur_rep.get("adaptive_total_pct"),
                "equal_total_pct": cur_rep.get("equal_total_pct"),
            },
            "metrics_after": {"improvement_pp": rec.get("improvement_pp")},
            "outcome": outcome,
            "notes": f"robust={rec.get('robust')}; checks={json.dumps(checks, ensure_ascii=False, default=str)}",
        })
    except Exception:
        return None


def summarize_ledger(recent: int = 20) -> dict:
    """汇总台账：总数 / 各 outcome 计数 / 最近 recent 条（含关键字段）。fail-soft。"""
    recs = load_ledger()
    counts = {o: 0 for o in VALID_OUTCOMES}
    for r in recs:
        o = r.get("outcome") or "pending"
        if o in counts:
            counts[o] += 1
        else:
            counts["pending"] += 1
    recent_list = []
    for r in recs[-recent:]:
        recent_list.append({
            "id": r.get("id"),
            "timestamp": r.get("timestamp"),
            "author": r.get("author"),
            "signal_date": r.get("signal_date"),
            "title": r.get("title"),
            "outcome": r.get("outcome"),
            "improvement_pp": (r.get("metrics_after") or {}).get("improvement_pp"),
        })
    return {"total": len(recs), "counts": counts, "recent": recent_list,
            "as_of": _now_iso()}


def format_ledger_report(summary: dict = None) -> str:
    if summary is None:
        summary = summarize_ledger()
    c = summary["counts"]
    lines = [
        "# 实验台账汇总（Experiment Ledger）",
        "",
        f"- 记录总数：**{summary['total']}**；"
        f"待处理={c.get('pending',0)} / 采纳={c.get('adopted',0)} / "
        f"否决={c.get('rejected',0)} / 无效={c.get('invalid',0)}",
        f"- 生成时间：{summary['as_of']}",
        "",
        "| ID | 时间 | 作者 | 信号日 | 标题 | 结论 | 改进(pp) |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in summary["recent"]:
        lines.append(
            f"| {r.get('id')} | {r.get('timestamp')} | {r.get('author')} | "
            f"{r.get('signal_date') or '—'} | {r.get('title') or '—'} | "
            f"{r.get('outcome')} | {r.get('improvement_pp') if r.get('improvement_pp') is not None else '—'} |")
    lines.append("")
    lines.append("> 台账为 append-only 审计记录，位于 stock_data/experiment_ledger.jsonl；"
                 "每次 walk-forward 重验 / 手动调参 / 策略开关都会落一笔。")
    return "\n".join(lines) + "\n"
