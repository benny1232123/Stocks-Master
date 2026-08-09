"""K 线缓存数据质量守卫 —— 检测并修复复权基准断层。

背景（2026-08-09 事故）：
    stock_data/k_data/*_qfq_full.csv 由「缓存 + 增量追加」维护。但前复权价以
    最新交易日为锚，此后一旦分红送转，整条历史序列都会被重新缩放。旧段停留在
    过期基准、新段用新基准，接缝处出现断层 —— 实测 600900 长江电力接缝单日
    +46%（物理不可能，主板涨停仅 10%）。全库 281/2989 只股票、389 个断层点，
    且日期高度集中（2026-02-24 一天 65 只），是批量增量更新造成的。

    危害远不止回测：MA/布林/动量/相对强度只要回看窗口跨过接缝，算出来全是错的，
    而且不报错、不告警，静默给出错误选股结果。5 个策略全部读 k_data。

检测口径：
    相邻交易日收盘价比值超过该板块涨跌停上限（含余量）即判为断层。
    个股除权除息日在 qfq 序列中本就不该跳空 —— 会跳空说明复权没做对。

用法：
    python scripts/data_quality_guard.py                      # 扫描并打印报告
    python scripts/data_quality_guard.py --emit-json out.json # 供纪律校验消费
    python scripts/data_quality_guard.py --fix                # 定向重拉脏票
    python scripts/data_quality_guard.py --fix --limit 20     # 先修 20 只试水

退出码：0=无断层；1=检出断层（--fix 模式下表示修复后仍有残留）。
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import Counter
from datetime import date, datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

K_DATA_DIR = ROOT / "stock_data" / "k_data"

# ── 涨跌停上限（%）：判定「物理不可能」的边界，非策略超参，随交易所规则固定 ──
LIMIT_MAIN = float(os.getenv("DQ_LIMIT_MAIN", "10"))      # 主板 60/00/001/002
LIMIT_GROWTH = float(os.getenv("DQ_LIMIT_GROWTH", "20"))  # 创业板 300/301、科创板 688
LIMIT_BJ = float(os.getenv("DQ_LIMIT_BJ", "30"))          # 北交所 8/4
# 余量倍数：吸收停牌复牌、ST 摘帽等边缘情形，避免误报
LIMIT_MARGIN = float(os.getenv("DQ_LIMIT_MARGIN", "1.15"))
# 跳过每只股票序列开头若干根：新股上市初期不设涨跌幅限制
SKIP_HEAD_BARS = int(os.getenv("DQ_SKIP_HEAD_BARS", "10"))


def _price_limit_pct(code: str) -> float:
    c = str(code)
    if c.startswith(("300", "301", "688")):
        return LIMIT_GROWTH
    if c.startswith(("8", "4")):
        return LIMIT_BJ
    return LIMIT_MAIN


def _code_of(path: Path) -> str:
    return path.name.split("_")[0]


def scan_file(path: Path) -> dict:
    """扫描单个缓存文件，返回问题描述（无问题时 breaks/dups 为空）。"""
    code = _code_of(path)
    out = {"code": code, "file": path.name, "breaks": [], "dups": 0, "rows": 0}
    try:
        df = pd.read_csv(path, usecols=["date", "close"])
    except Exception as exc:  # noqa: BLE001
        out["error"] = f"{type(exc).__name__}: {exc}"
        return out
    if len(df) < 2:
        out["rows"] = len(df)
        return out

    df = df.dropna(subset=["date"]).copy()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"]).sort_values("date").reset_index(drop=True)
    out["rows"] = len(df)
    out["dups"] = int(df["date"].duplicated().sum())
    if len(df) < 2:
        return out

    limit = _price_limit_pct(code) * LIMIT_MARGIN / 100.0
    up, dn = 1 + limit, 1 / (1 + limit)
    ratio = df["close"] / df["close"].shift(1)
    flagged = ratio[(ratio > up) | (ratio < dn)]
    for i in flagged.index:
        if i < SKIP_HEAD_BARS:
            continue  # 新股上市初期无涨跌幅限制
        out["breaks"].append({
            "date": str(df["date"].iloc[i]),
            "prev_close": round(float(df["close"].iloc[i - 1]), 4),
            "close": round(float(df["close"].iloc[i]), 4),
            "ratio": round(float(ratio.iloc[i]), 4),
        })
    return out


def scan_all(limit_files: int = 0) -> dict:
    files = sorted(K_DATA_DIR.glob("*_qfq_full.csv"))
    if limit_files:
        files = files[:limit_files]
    results, dirty, errors = [], [], []
    break_days: Counter = Counter()
    dup_files = []
    for p in files:
        r = scan_file(p)
        results.append(r)
        if r.get("error"):
            errors.append({"code": r["code"], "error": r["error"]})
            continue
        if r["breaks"]:
            dirty.append(r)
            for b in r["breaks"]:
                break_days[b["date"]] += 1
        if r["dups"]:
            dup_files.append({"code": r["code"], "dups": r["dups"]})
    return {
        "scanned": len(files),
        "dirty_count": len(dirty),
        "break_points": sum(len(d["breaks"]) for d in dirty),
        "dirty": dirty,
        "dup_files": dup_files,
        "errors": errors,
        "top_break_days": break_days.most_common(10),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


def refetch(codes: list[str], verbose: bool = True) -> dict:
    """对指定股票全量重拉 qfq 缓存（force_refresh 绕过增量追加）。"""
    from smcore.data.kline import fetch_daily_k

    ok, failed = [], []
    total = len(codes)
    for i, code in enumerate(codes, 1):
        path = K_DATA_DIR / f"{code}_qfq_full.csv"
        # 保留原始覆盖区间，避免重拉后历史变短
        start = date(2015, 1, 1)
        try:
            old = pd.read_csv(path, usecols=["date"])
            first = str(old["date"].min())[:10]
            start = min(start, pd.to_datetime(first).date())
        except Exception:
            pass
        t0 = time.time()
        try:
            df = fetch_daily_k(
                code, start, date.today(), adjust="qfq",
                use_cache=True, force_refresh=True,
            )
            if df is None or df.empty:
                failed.append({"code": code, "reason": "empty"})
            else:
                ok.append(code)
        except Exception as exc:  # noqa: BLE001
            failed.append({"code": code, "reason": f"{type(exc).__name__}: {exc}"})
        if verbose:
            print(f"  [{i}/{total}] {code} -> "
                  f"{'ok' if code in ok else 'FAIL'} ({time.time() - t0:.1f}s)", flush=True)
    return {"ok": ok, "failed": failed}


def run(fix: bool = False, limit: int = 0, verbose: bool = True) -> dict:
    report = scan_all()
    if verbose:
        print(f"扫描 {report['scanned']} 个 qfq 缓存文件")
        print(f"  断层股票数: {report['dirty_count']}  断层点总数: {report['break_points']}")
        print(f"  重复日期文件数: {len(report['dup_files'])}")
        if report["errors"]:
            print(f"  读取失败: {len(report['errors'])}")
        if report["top_break_days"]:
            print("  断层最集中日期:")
            for d, n in report["top_break_days"]:
                print(f"    {d}  {n} 只")

    if not fix or not report["dirty"]:
        report["fixed"] = None
        return report

    codes = [d["code"] for d in report["dirty"]]
    if limit:
        codes = codes[:limit]
    if verbose:
        print(f"\n开始定向重拉 {len(codes)} 只脏票（force_refresh 全量，不走增量追加）")
    res = refetch(codes, verbose=verbose)

    # 复检：只重扫刚修过的票
    still = []
    for code in res["ok"]:
        r = scan_file(K_DATA_DIR / f"{code}_qfq_full.csv")
        if r["breaks"]:
            still.append({"code": code, "breaks": len(r["breaks"])})
    report["fixed"] = {
        "attempted": len(codes),
        "refetch_ok": len(res["ok"]),
        "refetch_failed": res["failed"],
        "still_dirty": still,
    }
    if verbose:
        print(f"\n重拉成功 {len(res['ok'])}/{len(codes)}，"
              f"复检仍有断层 {len(still)} 只，拉取失败 {len(res['failed'])} 只")
    return report


def main() -> int:
    ap = argparse.ArgumentParser(description="K 线缓存复权断层检测与修复")
    ap.add_argument("--fix", action="store_true", help="对检出的脏票全量重拉")
    ap.add_argument("--limit", type=int, default=0, help="--fix 时最多修前 N 只")
    ap.add_argument("--emit-json", default="", help="把报告写入 JSON 文件")
    ap.add_argument("--emit-csv", default="", help="把断层明细写入 CSV")
    args = ap.parse_args()

    report = run(fix=args.fix, limit=args.limit)

    if args.emit_json:
        p = Path(args.emit_json)
        p.parent.mkdir(parents=True, exist_ok=True)
        slim = {k: v for k, v in report.items() if k != "dirty"}
        slim["dirty_codes"] = [d["code"] for d in report["dirty"]]
        p.write_text(json.dumps(slim, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"报告已写入 {p}")

    if args.emit_csv:
        rows = [
            {"code": d["code"], **b}
            for d in report["dirty"] for b in d["breaks"]
        ]
        p = Path(args.emit_csv)
        p.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_csv(p, index=False, encoding="utf-8-sig")
        print(f"断层明细已写入 {p}（{len(rows)} 行）")

    residual = report["dirty_count"]
    if report.get("fixed"):
        residual = len(report["fixed"]["still_dirty"]) + len(report["fixed"]["refetch_failed"])
    return 0 if residual == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
