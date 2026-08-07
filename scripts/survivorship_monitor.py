#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""幸存者偏差 / ST·退市 监控。

为什么需要：量化回测若只以"当前在市"股票池为 universe，已退市/ST 的最差票被排除，
会系统性高估策略表现（survivorship bias）。本脚本：
1. 扫描所有 Daily-Action-List 候选，按股票名称识别 ST / *ST / 退市（名称含"退"）票；
2. 统计"候选含 ST/退市占比"，超出阈值则告警；
3. 给出选股 universe 数据源的 point-in-time 偏差结论与后续建议（回测估值改用按信号日
   baostock.get_all_stock(date) 取当时列表，正确计入退市票损失）。

阈值走 risk_config（survivorship_monitor）。纯本地、不联网、fail-soft。
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from smcore.strategy.risk_rules import CONFIG as RISK_CONFIG  # noqa: E402

STOCK_DATA_DIR = ROOT / "stock_data"


def _is_distress(name: str) -> bool:
    """名称级 ST/退市识别：A股 ST/*ST 名以 ST 开头，退市整理期名含'退'。"""
    if not name:
        return False
    n = str(name).strip()
    up = n.upper()
    if up.startswith("ST") or up.startswith("*ST"):
        return True
    if "退" in n:  # 退市整理期 / 已退市
        return True
    return False


def _load_candidates(dal_path: Path) -> list[tuple[str, str]]:
    try:
        import pandas as pd

        d = pd.read_csv(dal_path, encoding="utf-8-sig")
    except Exception:
        return []
    if "股票代码" not in d.columns:
        return []
    name_col = "股票名称" if "股票名称" in d.columns else None
    out = []
    for _, r in d.iterrows():
        code = str(r.get("股票代码", "")).strip()
        if not code:
            continue
        nm = str(r.get(name_col)) if name_col else ""
        out.append((code, nm))
    return out


def _universe_source_note() -> dict:
    """结论来自代码核查（非网络探针，避免沙箱断网）：
    - 选股预筛用 ak.stock_zh_a_spot()（新浪，当前在市快照）与 stock_info_a_code_name（当前列表）；
    - 故"前向选择"只面向当前可交易标的，无偏；
    - 但"历史 OOS 回测"若以当前列表为 universe，会系统性排除已退市/ST 最差票 → 幸存者偏差。"""
    return {
        "selection_sources": [
            "ak.stock_zh_a_spot() (current-listed snapshot)",
            "stock_info_a_code_name (current list)",
        ],
        "forward_selection_unbiased": True,
        "historical_oos_point_in_time": False,
        "recommendation": (
            "历史回测估值改用按信号日取数的 point-in-time 列表"
            "（baostock.get_all_stock(signal_date) 含当时在市/已退市），以正确计入退市票的损失。"
        ),
    }


def main() -> int:
    cfg = (RISK_CONFIG or {}).get("survivorship_monitor", {})
    max_st_ratio = float(cfg.get("max_st_ratio", 0.02))
    alert_on_st = bool(cfg.get("alert_on_st_candidate", True))

    dals = sorted(STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"))
    per_day: list[dict] = []
    tot = 0
    st = 0
    any_st_days = 0
    for dal in dals:
        m = re.search(r"(\d{8})", dal.name)
        sd = m.group(1) if m else dal.name
        cands = _load_candidates(dal)
        if not cands:
            continue
        n = len(cands)
        k = sum(1 for _, nm in cands if _is_distress(nm))
        ratio = (k / n) if n else 0.0
        per_day.append({
            "date": sd, "candidates": n, "st_candidates": k,
            "st_ratio": round(ratio, 4), "has_st": k > 0,
        })
        tot += n
        st += k
        if k > 0:
            any_st_days += 1

    overall_ratio = (st / tot) if tot else 0.0
    alert = alert_on_st and (overall_ratio > max_st_ratio)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_dal_files": len(per_day),
            "total_candidates": tot,
            "st_candidates": st,
            "overall_st_ratio": round(overall_ratio, 4),
            "days_with_st": any_st_days,
            "threshold_max_st_ratio": max_st_ratio,
            "alert": alert,
        },
        "universe_note": _universe_source_note(),
        "per_day": per_day,
    }
    out = STOCK_DATA_DIR / "survivorship_report.json"
    try:
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:  # pragma: no cover
        print(f"写报告失败：{e}")

    print(f"扫描 {len(per_day)} 个 Daily-Action-List，候选共 {tot} 只，其中 ST/退市 {st} 只 "
          f"（占比 {overall_ratio:.2%}），阈值 {max_st_ratio:.2%}")
    if st:
        sample = ", ".join(f"{d['date']}({d['st_candidates']})" for d in per_day if d["has_st"])
        print(f"⚠ 命中 ST/退市 的日期数：{any_st_days}；示例：{sample[:200]}")
    print(f"{'ALERT' if alert else 'OK'}：幸存者偏差监控 {'触发' if alert else '正常'}")
    print(f"明细已写：{out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
