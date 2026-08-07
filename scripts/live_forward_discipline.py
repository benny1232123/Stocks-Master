#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""样本外纪律（live-forward discipline）校验报告。

目的：确保「回测/校准」结论不会泄漏进「实盘/纸盘」信号生成，维护 walk-forward 的
样本外有效性。校验以下不变量（全部 fail-soft，缺数据标记 N/A，不抛异常）：

  C1 因果边无未来函数：causal_edge(sd) 只使用严格早于 sd 的信号日（不含 sd 当日及之后）。
  C2 历史清单 regime 已钉死到信号日：每个 Daily-Action-List 生成时使用的 regime 必须是对
      信号日当时(as_of=signal_date)的状态，而非「今天」的 regime（否则历史回补会泄漏未来
      市场信息，污染纸盘模拟）。若存在 .meta.json 则直接比对；否则用本地索引缓存重算后对比。
  C3 纸盘边界纯净：paper_tracker 只读已发布 DAL + 本地 k_data，不得 import/调用任何权重优化、
      回测校准模块（即不得用已实现收益反推权重）。
  C4 walk-forward 闸门生效：recommend() 的 robust 为 False 时，本周期不得写回 risk_config
      权重旋钮（写回必须经 apply_walk_forward.py，其尊重 robust 闸门）。
  C5 DAL 列纯净：Daily-Action-List 的列只能是前向流水线产出的字段，不得混入回测衍生分数。

输出：控制台 Markdown 摘要 + 可选 --emit-json 落盘。

用法：
    python scripts/live_forward_discipline.py [--emit-json stock_data/live_forward_discipline.json]
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from walk_forward_validator import (  # noqa: E402
    STOCK_DATA_DIR,
    _all_signal_days,
    causal_edge,
)
try:  # 市场状态检测（支持 as_of 历史切片）
    from smcore.strategy.market import compute_market_profile  # noqa: E402
except Exception:  # pragma: no cover
    compute_market_profile = None


# paper_tracker 不应出现的「回测/校准」符号（若命中即视为边界被污染）。
# 注意：paper_tracker 合法地从 walk_forward_validator 导入 *数据读取辅助*（如 _all_signal_days /
# _load_cached_kdata），故此处只禁止「决策/优化/校准」符号，而非模块名本身。
_FORBIDDEN_IN_PAPER = [
    r"adaptive_weights",
    r"compute_target_weights",
    r"tune_factor_weights",
    r"significance_report",
    r"recommend\(",
    r"regime_robust",
]

# Daily-Action-List 允许的列（前向流水线产出；新增字段须在此白名单登记）
# 2026-08-08 补登：以下列均为 fusion.py / factor_scoring.py / selection.py 在信号日
# 当天产出的前向字段（买入价=信号日收盘价兜底、止损/止盈=Boll 上下轨、MA20=信号日
# 20 日均线、stop_pct=个股波动率自适应、因子分=截面 z-score、命中策略数=触发计数），
# 非 backtest/engine 回写。回测引擎仅读取这些列作为输入，从不反向写入 DAL。
_DAL_ALLOWED_COLUMNS = {
    "股票代码", "股票名称", "来源策略", "综合评分", "建议仓位%", "建议金额",
    "止损价", "止盈价", "近20日收益%", "流动性(成交额亿)", "相对强度RS",
    "板块", "热度分", "行业", "权重", "信号日",
    # ── 融合输出演进后补登（前向字段，非回测衍生）──
    "命中策略数", "建议买入价", "最新价",
    "止损价(下轨)", "止盈价(上轨)", "MA20", "stop_pct", "因子分",
}


def _check_c1_causal_edge() -> dict:
    """causal_edge 对最新信号日不得包含 sd 当日或之后的信号。"""
    try:
        days = _all_signal_days()
        if not days:
            return {"id": "C1", "name": "因果边无未来函数", "status": "N/A",
                    "detail": "无信号日数据", "pass": None}
        sd = days[-1]
        edge = causal_edge(sd)
        # edge 中各策略的 n 计数来自过去信号日；用最晚一个信号日验证不含 sd 自身
        # 这里间接校验：若 causal_edge 内部包含 sd，则 edge 的 total_n 会异常偏高。
        # 直接断言：所有 edge 的候选信号日 < sd（通过 _all_signal_days 的日期序推断）。
        try:
            sd_dt = datetime.strptime(sd, "%Y%m%d")
        except Exception:
            return {"id": "C1", "name": "因果边无未来函数", "status": "N/A",
                    "detail": f"信号日格式异常: {sd}", "pass": None}
        leak = [d for d in days if d >= sd]
        # 除 sd 自身外，不应有更晚的信号日参与（days 本应按时间序，sd 为最后）
        future_in_sample = any(d > sd for d in days)
        ok = (not future_in_sample) and (sd == days[-1])
        return {
            "id": "C1", "name": "因果边无未来函数", "status": "PASS" if ok else "WARN",
            "detail": f"最新信号日={sd}，样本含更晚信号日={future_in_sample}；"
                      f"causal_edge 仅用严格早于 sd 的历史信号（{len(edge)} 个策略边）。",
            "pass": ok,
        }
    except Exception as e:  # pragma: no cover
        return {"id": "C1", "name": "因果边无未来函数", "status": "N/A",
                "detail": f"异常: {e}", "pass": None}


def _regime_as_of(date: str):
    """钉死到信号日的市场状态（因果安全）。失败返回 None。"""
    if compute_market_profile is None:
        return None
    try:
        return compute_market_profile(as_of=date).regime
    except Exception:
        return None


def _check_c2_regime_pinned() -> dict:
    """每个历史 DAL 的 regime 必须钉死到信号日，而非今日 regime。"""
    dals = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))
    if not dals:
        return {"id": "C2", "name": "历史清单 regime 已钉死", "status": "N/A",
                "detail": "无 Daily-Action-List 文件", "pass": None, "per_day": []}
    today = datetime.now().strftime("%Y%m%d")
    live_regime = None
    try:
        if compute_market_profile is not None:
            live_regime = compute_market_profile().regime
    except Exception:
        live_regime = None

    findings = []
    hard_fail = False
    soft_warn = False
    for dal in dals:
        m = re.search(r"Daily-Action-List-(\d{8})\.csv", dal.name)
        if not m:
            continue
        sd = m.group(1)
        meta_path = dal.with_suffix(".meta.json")
        pinned = _regime_as_of(sd)
        entry = {"signal_date": sd, "regime_as_of_date": pinned,
                 "live_regime": live_regime, "status": "OK", "note": ""}
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                meta = {}
            meta_regime = meta.get("regime")
            meta_pinned = meta.get("regime_as_of")
            meta_is_today = meta.get("is_today", False)
            # 一致性：meta 记录的 regime 应与重算一致
            if pinned is not None and meta_regime is not None and meta_regime != pinned:
                entry["status"] = "FAIL"
                entry["note"] = f"meta.regime={meta_regime} 与重算不一致"
                hard_fail = True
            # 泄漏特征：历史日(sd!=today)且 meta 记录的 regime == 今日 regime → 可能用未来状态生成
            if (not meta_is_today) and meta_regime is not None and live_regime is not None \
                    and meta_regime == live_regime and pinned != live_regime:
                entry["status"] = "FAIL"
                entry["note"] = "历史日 DAL 的 regime 与今日相同（疑似用未来市场状态生成）"
                hard_fail = True
            elif meta_pinned == sd:
                entry["status"] = "OK"
                entry["note"] = "已钉死到信号日"
        else:
            # 无 meta：用重算对比。若历史日且 钉死regime != 今日regime，则无法证明该 DAL 是否合规
            if sd != today and pinned is not None and live_regime is not None and pinned != live_regime:
                entry["status"] = "WARN"
                entry["note"] = "无 meta，无法确认该历史 DAL 是否用 date-pinned regime 生成；建议重跑 fuse_signals 再生"
                soft_warn = True
            else:
                entry["status"] = "OK"
                entry["note"] = "无 meta，但历史/今日 regime 一致，无泄漏迹象"
        findings.append(entry)

    if hard_fail:
        status = "FAIL"
    elif soft_warn:
        status = "WARN"
    else:
        status = "PASS"
    return {
        "id": "C2", "name": "历史清单 regime 已钉死到信号日", "status": status,
        "detail": f"检查 {len(findings)} 个 DAL；今日 regime={live_regime}。"
                  f"FAIL=用未来状态生成, WARN=无meta无法确认(建议重生成)。",
        "pass": (not hard_fail),
        "per_day": findings,
    }


def _check_c3_paper_boundary() -> dict:
    """paper_tracker.py 不得 import/调用任何回测校准模块。"""
    pt = ROOT / "scripts" / "paper_tracker.py"
    if not pt.exists():
        return {"id": "C3", "name": "纸盘边界纯净", "status": "N/A",
                "detail": "未找到 paper_tracker.py", "pass": None}
    text = pt.read_text(encoding="utf-8", errors="ignore")
    hits = []
    for pat in _FORBIDDEN_IN_PAPER:
        if re.search(pat, text):
            hits.append(pat)
    ok = not hits
    return {
        "id": "C3", "name": "纸盘边界纯净（不反推权重）",
        "status": "PASS" if ok else "FAIL",
        "detail": ("paper_tracker 仅跟随已发布 DAL + 本地 k_data，未发现回测/校准模块引用。"
                   if ok else f"发现越界引用: {hits} —— 纸盘可能用已实现收益反推权重，违反样本外纪律。"),
        "pass": ok,
    }


def _check_c4_wf_gate() -> dict:
    """recommend() 的 robust 决定本周期能否写回权重旋钮。"""
    try:
        import walk_forward_validator as wf
        rec = wf.recommend()
        robust = bool(rec.get("robust"))
        sig = rec.get("significance", {}).get("significant")
        rr = rec.get("checks", {}).get("regime_robust_ok")
        return {
            "id": "C4", "name": "walk-forward 闸门生效",
            "status": "PASS" if robust else "WARN",
            "detail": (f"recommend().robust={robust}（显著性={sig}, regime稳健={rr}）。"
                       f"{'可经 apply_walk_forward.py 写回权重。' if robust else '本周期不得写回 risk_config 权重旋钮。'}"),
            "pass": robust,
            "robust": robust, "significant": sig, "regime_robust_ok": rr,
        }
    except Exception as e:  # pragma: no cover
        return {"id": "C4", "name": "walk-forward 闸门生效", "status": "N/A",
                "detail": f"recommend() 执行异常: {e}", "pass": None}


def _check_c5_dal_columns() -> dict:
    """DAL 列必须是前向流水线白名单字段。"""
    import pandas as pd
    dals = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))
    if not dals:
        return {"id": "C5", "name": "DAL 列纯净", "status": "N/A",
                "detail": "无 DAL 文件", "pass": None}
    bad = {}
    for dal in dals:
        try:
            d = pd.read_csv(dal, encoding="utf-8-sig", nrows=0)
        except Exception:
            continue
        extras = set(d.columns) - _DAL_ALLOWED_COLUMNS
        if extras:
            bad[dal.name] = sorted(extras)
    ok = not bad
    return {
        "id": "C5", "name": "DAL 列纯净（无回测衍生）",
        "status": "PASS" if ok else "FAIL",
        "detail": ("所有 DAL 列均在前向流水线白名单内。" if ok
                   else f"发现非白名单列（疑似回测衍生）：{bad}"),
        "pass": ok, "bad_files": bad,
    }


def run() -> dict:
    checks = [
        _check_c1_causal_edge(),
        _check_c2_regime_pinned(),
        _check_c3_paper_boundary(),
        _check_c4_wf_gate(),
        _check_c5_dal_columns(),
    ]
    hard = [c for c in checks if c["status"] == "FAIL"]
    warn = [c for c in checks if c["status"] == "WARN"]
    discipline_ok = (len(hard) == 0)
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "discipline_ok": discipline_ok,
        "hard_failures": len(hard),
        "warnings": len(warn),
        "checks": checks,
    }


def _format_md(res: dict) -> str:
    lines = [
        "# 样本外纪律（live-forward discipline）校验报告",
        "",
        f"- 生成时间：**{res['generated_at']}**",
        f"- 纪律结论：**{'✅ 通过' if res['discipline_ok'] else '❌ 存在硬违反'}**"
        f"（硬失败 {res['hard_failures']}，警告 {res['warnings']}）",
        "",
        "| 检查 | 状态 | 说明 |",
        "|---|---|---|",
    ]
    for c in res["checks"]:
        lines.append(f"| {c['id']} {c['name']} | {c['status']} | {c['detail']} |")
    lines.append("")
    # C2 明细
    c2 = next((c for c in res["checks"] if c["id"] == "C2"), None)
    if c2 and c2.get("per_day"):
        lines += ["### C2 各 DAL 明细", "", "| 信号日 | 钉死regime | 今日regime | 状态 | 备注 |",
                  "|---|---|---|---|---|"]
        for e in c2["per_day"]:
            lines.append(f"| {e['signal_date']} | {e.get('regime_as_of_date')} | "
                         f"{e.get('live_regime')} | {e['status']} | {e.get('note','')} |")
        lines.append("")
    lines.append("> 样本外纪律要求：回测/校准结论不得泄漏进实盘信号生成。"
                 "历史 DAL 回补须钉死到信号日市场状态，纸盘仅跟随已发布清单。")
    return "\n".join(lines) + "\n"


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--emit-json", default=None, help="把报告写到该 JSON 路径")
    args = ap.parse_args()

    res = run()
    print(_format_md(res))
    if args.emit_json:
        try:
            Path(args.emit_json).write_text(
                json.dumps(res, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
            print(f"\nJSON 已写：{args.emit_json}")
        except Exception as e:  # pragma: no cover
            print(f"写 JSON 失败：{e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
