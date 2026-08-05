"""
Live 复核 relativity 的 up_tol 方向（生产真实宇宙，跨多信号日聚合）。

方法（忠实复刻生产链路，对齐离线扫描的聚合方法论）：
- 生产预筛选宇宙 = 各 Stock-Selection-Shared-Seed-<日期>.csv（已含 shareholder/资金流/基本面 预筛选）。
- 跨全部可用信号日聚合（单日样本太小、RS 通过率天然仅 0.5~2%）。
- 每只唯一票仅联网取一次新鲜数据（baostock，与生产 fetch_bs_daily_close 同源）；沪深300 取一次。
- 逐票复刻 _evaluate_single_code 的「停牌时效(stale>7天) + 价格(5~30) + relative_strength_pass」，
  按各信号日切片到 <= 该日（防前视）。基准 = 真实沪深300。
- 分别对 up_tol=-0.010（当前默认）与 -0.005（旧严格）汇总：通过数 + 前向10日超额(个股-指数)。
- 前向窗口：个股与指数取同一实际可用长度（上限 FORWARD_DAYS=10，下限 MIN_FWD_DAYS=3）；
  满 10 日的记作 complete（干净样本），不足者记作 partial（仍计入 all，方向对比不受截断影响）。
  近期信号日因此也能贡献样本，无需等到 08-17。

不写回被 git 跟踪的 stock_data/k_data/，数据仅驻留内存。
"""
from __future__ import annotations

import argparse
import glob
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import baostock as bs
import pandas as pd

from smcore.strategies.relativity import (  # noqa: E402
    RS_LOOKBACK_DAYS,
    RS_MIN_OVERLAP_DAYS,
    RS_DOWN_OUTPERF,
    RS_MIN_UP_RATIO,
    RS_MIN_DOWN_RATIO,
    RS_MIN_UP_DAYS,
    RS_MIN_DOWN_DAYS,
    RS_MAX_STALE_DAYS,
    PRICE_LOWER_LIMIT,
    PRICE_UPPER_LIMIT,
    relative_strength_pass,
)

STOCK_DATA_DIR = Path("stock_data")
INDEX_CODE = "000300"          # 沪深300（baostock 无前缀代码配 sh.）
UP_TOLS = [-0.010, -0.005]     # 当前默认 / 旧严格
FORWARD_DAYS = 10
FETCH_START = "2026-03-01"
FETCH_END = "2026-08-20"
_BS_FIELDS = "date,open,close,tradestatus"
_BS_INTERVAL = 0.12
_BS_RETRIES = 2


def _bs_code(code: str, is_index: bool) -> str:
    if is_index:
        return f"sh.{code}"
    if code.startswith("6"):
        return f"sh.{code}"
    return f"sz.{code}"


def _fetch_bs(code_bs: str) -> pd.DataFrame | None:
    """baostock 取 date/open/close。与生产 fetch_bs_daily_close 同源。"""
    for attempt in range(_BS_RETRIES + 1):
        time.sleep(_BS_INTERVAL)
        rs = bs.query_history_k_data_plus(
            code_bs, _BS_FIELDS,
            start_date=FETCH_START, end_date=FETCH_END,
            frequency="d", adjustflag="2",
        )
        if rs is None or rs.error_code != "0":
            if attempt < _BS_RETRIES:
                continue
            return None
        rows = []
        while rs.next():
            rows.append(rs.get_row_data())
        fields = list(getattr(rs, "fields", []) or [])
        if not rows or not fields:
            if attempt < _BS_RETRIES:
                continue
            return None
        df = pd.DataFrame(rows, columns=fields)[["date", "open", "close"]].copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        for c in ("open", "close"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df.dropna(subset=["date", "open", "close"]).sort_values("date").reset_index(drop=True)
    return None


MIN_FWD_DAYS = 3  # 前向窗口至少需 3 个交易日才有意义（近期信号日也能贡献 partial 样本）


def _forward_return_flex(full_df: pd.DataFrame, signal_date: pd.Timestamp,
                         max_days: int = FORWARD_DAYS, min_days: int = MIN_FWD_DAYS):
    """信号日后首交易日买(open)、持 max_days 交易日卖(close)，但最多取实际可用天数。

    返回 (收益率, 实际持有交易日数)；实际可用 < min_days 时返回 (None, 0)。
    max_days 可被调用方强制为某固定值（用于让指数与个股对齐同一窗口长度）。
    """
    fwd = full_df[full_df["date"] > signal_date].sort_values("date").reset_index(drop=True)
    n = min(max_days, len(fwd))
    if n < min_days:
        return None, 0
    buy_open = float(fwd["open"].iloc[0])
    sell_close = float(fwd["close"].iloc[n - 1])
    if buy_open <= 0:
        return None, 0
    return sell_close / buy_open - 1.0, n


def load_all_seed_dates() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for p in sorted(glob.glob(str(STOCK_DATA_DIR / "Stock-Selection-Shared-Seed-*.csv"))):
        stem = Path(p).stem  # Stock-Selection-Shared-Seed-20260803
        date_nodash = stem.split("-")[-1]
        date_dash = f"{date_nodash[:4]}-{date_nodash[4:6]}-{date_nodash[6:8]}"
        df = pd.read_csv(p, dtype=str)
        col = "股票代码" if "股票代码" in df.columns else df.columns[0]
        codes = [str(c).strip() for c in df[col].tolist() if str(c).strip()]
        out[date_dash] = codes
    return out


def evaluate_date(codes: list[str], index_df: pd.DataFrame, fetched: dict[str, pd.DataFrame],
                  up_tol: float, signal_date: str) -> dict:
    sig = pd.to_datetime(signal_date)
    idx_rs = index_df[index_df["date"] <= sig].sort_values("date").reset_index(drop=True)
    if idx_rs.empty:
        return {"n_passed": 0, "passed": [], "fwd_excess_all": [], "fwd_excess_complete": [], "skipped": {}}
    skipped = {"stale": 0, "price": 0, "empty": 0, "overlap": 0, "rs_fail": 0, "fetch_fail": 0}
    passed: list[str] = []
    fwd_excess_all: list[float] = []        # 所有可用窗口（≥3 日，含 partial）
    fwd_excess_complete: list[float] = []   # 仅满 FORWARD_DAYS 日的干净样本

    for code in codes:
        sdf_full = fetched.get(code)
        if sdf_full is None or sdf_full.empty:
            skipped["fetch_fail"] += 1
            continue
        sdf_rs = sdf_full[sdf_full["date"] <= sig].reset_index(drop=True)
        if sdf_rs.empty:
            skipped["empty"] += 1
            continue
        latest = sdf_rs["date"].iloc[-1]
        if (sig - latest).days > RS_MAX_STALE_DAYS:
            skipped["stale"] += 1
            continue
        latest_close = float(sdf_rs["close"].iloc[-1])
        if latest_close < PRICE_LOWER_LIMIT or latest_close > PRICE_UPPER_LIMIT:
            skipped["price"] += 1
            continue
        ok, stats = relative_strength_pass(
            sdf_rs, idx_rs,
            min_overlap_days=RS_MIN_OVERLAP_DAYS, up_tol=up_tol,
            down_outperf=RS_DOWN_OUTPERF, min_up_ratio=RS_MIN_UP_RATIO,
            min_down_ratio=RS_MIN_DOWN_RATIO, min_up_days=RS_MIN_UP_DAYS,
            min_down_days=RS_MIN_DOWN_DAYS,
        )
        if not ok:
            if stats.get("reason") in ("overlap_too_small", "insufficient_up_or_down_days"):
                skipped["overlap"] += 1
            else:
                skipped["rs_fail"] += 1
            continue
        passed.append(code)
        # 个股前向：取可用窗口（上限 FORWARD_DAYS）
        s_ret, n = _forward_return_flex(sdf_full, sig)
        if s_ret is None:
            continue
        # 指数前向：强制同一窗口长度 n，保证超额偏差不受截断影响
        i_ret, _ = _forward_return_flex(index_df, sig, max_days=n, min_days=n)
        if i_ret is None:
            continue
        excess = s_ret - i_ret
        fwd_excess_all.append(excess)
        if n >= FORWARD_DAYS:
            fwd_excess_complete.append(excess)
    return {"n_passed": len(passed), "passed": passed,
            "fwd_excess_all": fwd_excess_all, "fwd_excess_complete": fwd_excess_complete,
            "skipped": skipped}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--emit-json", default=str(STOCK_DATA_DIR / "live_reval_relativity.json"))
    args = ap.parse_args()

    date_to_codes = load_all_seed_dates()
    if not date_to_codes:
        print("[live-reval] 未发现任何 Shared-Seed CSV", file=sys.stderr)
        return 1
    print(f"[live-reval] 信号日集合({len(date_to_codes)}天): {', '.join(sorted(date_to_codes))}")

    unique_codes: set[str] = set()
    for cs in date_to_codes.values():
        unique_codes.update(cs)
    print(f"[live-reval] 唯一候选股(生产预筛选后, 去重): {len(unique_codes)} 只")

    bs.login()
    try:
        # 每只唯一票仅取一次
        print(f"[live-reval] 联网取 {len(unique_codes)} 只票 + 沪深300 ...", file=sys.stderr)
        fetched: dict[str, pd.DataFrame] = {}
        fail = 0
        for i, code in enumerate(sorted(unique_codes), 1):
            df = _fetch_bs(_bs_code(code, is_index=False))
            if df is not None and not df.empty:
                fetched[code] = df
            else:
                fail += 1
            if i % 40 == 0:
                print(f"  fetched {i}/{len(unique_codes)}", file=sys.stderr)
        index_df = _fetch_bs(_bs_code(INDEX_CODE, is_index=True))
        if index_df is None or index_df.empty:
            print("[live-reval] 沪深300 取数失败，终止", file=sys.stderr)
            return 1
        print(f"[live-reval] 取数完成: 成功 {len(fetched)}/{len(unique_codes)} (失败 {fail}) | 沪深300 行数={len(index_df)}")

        # 聚合
        agg: dict[str, dict] = {
            f"up_tol_{ut}": {"n_passed": 0, "fwd_excess_all": [], "fwd_excess_complete": [], "skipped": {}}
            for ut in UP_TOLS
        }
        per_date_records = []
        for date_dash in sorted(date_to_codes):
            codes = date_to_codes[date_dash]
            rec = {"date": date_dash, "n_universe": len(codes)}
            for ut in UP_TOLS:
                r = evaluate_date(codes, index_df, fetched, ut, date_dash)
                agg[f"up_tol_{ut}"]["n_passed"] += r["n_passed"]
                agg[f"up_tol_{ut}"]["fwd_excess_all"].extend(r["fwd_excess_all"])
                agg[f"up_tol_{ut}"]["fwd_excess_complete"].extend(r["fwd_excess_complete"])
                for k, v in r["skipped"].items():
                    agg[f"up_tol_{ut}"]["skipped"][k] = agg[f"up_tol_{ut}"]["skipped"].get(k, 0) + v
                rec[f"n_pass_{ut}"] = r["n_passed"]
                rec[f"n_fwd_all_{ut}"] = len(r["fwd_excess_all"])
                rec[f"n_fwd_complete_{ut}"] = len(r["fwd_excess_complete"])
            per_date_records.append(rec)
            print(f"  {date_dash}: 宇宙={len(codes)} | -0.010通过={rec['n_pass_-0.01']}"
                  f"(前向 all {rec['n_fwd_all_-0.01']}/complete {rec['n_fwd_complete_-0.01']}) "
                  f"-0.005通过={rec['n_pass_-0.005']}(前向 all {rec['n_fwd_all_-0.005']})")

        # 汇总（分别统计 complete 满窗 与 all 含 partial）
        def _agg_excess(ex: list[float]) -> dict:
            ser = pd.Series(ex) if ex else pd.Series(dtype=float)
            return {
                "n": len(ex),
                "mean_excess": float(ser.mean()) if len(ser) else None,
                "median_excess": float(ser.median()) if len(ser) else None,
                "win_rate": float((ser > 0).mean()) if len(ser) else None,
            }

        summary = {}
        for ut in UP_TOLS:
            summary[f"up_tol_{ut}"] = {
                "n_passed_total": agg[f"up_tol_{ut}"]["n_passed"],
                "complete": _agg_excess(agg[f"up_tol_{ut}"]["fwd_excess_complete"]),
                "all": _agg_excess(agg[f"up_tol_{ut}"]["fwd_excess_all"]),
                "skipped": agg[f"up_tol_{ut}"]["skipped"],
            }

        out = {"index": INDEX_CODE, "up_tols": UP_TOLS,
               "forward_days": FORWARD_DAYS, "min_fwd_days": MIN_FWD_DAYS,
               "summary": summary, "per_date": per_date_records}
        if args.emit_json:
            json.dump(out, open(args.emit_json, "w"), ensure_ascii=False, indent=2, default=str)
            print(f"\n[live-reval] 写出 {args.emit_json}")

        print("\n================ 结论（生产真实宇宙 · 跨信号日聚合） ================")
        a = summary["up_tol_-0.01"]
        b = summary["up_tol_-0.005"]
        print(f"唯一候选股: {len(unique_codes)} 只 | 信号日: {len(date_to_codes)} 天")
        for label, s in (("up_tol=-0.010(当前默认)", a), ("up_tol=-0.005(旧严格)", b)):
            c, al = s["complete"], s["all"]
            print(f"{label}: 总通过 {s['n_passed_total']} | "
                  f"complete(满{FORWARD_DAYS}日) n={c['n']} 均值超额"
                  f"{('%.4f' % c['mean_excess']) if c['mean_excess'] is not None else 'NA'} "
                  f"胜率{('%.3f' % c['win_rate']) if c['win_rate'] is not None else 'NA'} | "
                  f"all(含partial) n={al['n']} 均值超额"
                  f"{('%.4f' % al['mean_excess']) if al['mean_excess'] is not None else 'NA'} "
                  f"胜率{('%.3f' % al['win_rate']) if al['win_rate'] is not None else 'NA'}")
        # 判定：用 all（样本更足）做主判定，complete 作旁证
        av, bv = a["all"]["mean_excess"], b["all"]["mean_excess"]
        if av is not None and bv is not None:
            verdict = ("放宽 -0.010 优(或等)于 旧严格 -0.005 —— 离线方向在生产真实宇宙(000300+预筛选)得到验证"
                       if av >= bv
                       else "生产真实宇宙下旧严格反而更好 —— 需重新审视")
            print("判定(all):", verdict)
        ac, bc = a["complete"]["mean_excess"], b["complete"]["mean_excess"]
        if ac is not None and bc is not None:
            print("判定(complete):",
                  "一致" if (ac >= bc) == (av >= bv) else "complete 与 all 方向相反 —— 需谨慎")
        return 0
    finally:
        bs.logout()


if __name__ == "__main__":
    raise SystemExit(main())
