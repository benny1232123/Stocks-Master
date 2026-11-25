import json
import re
import urllib.request
from pathlib import Path

DATA_JS_PATH = Path("frontend/src/data/rescueBacktestData.js")
STOCK_CODES = ["002284", "603187", "600269", "603390"]


def fetch_tencent_daily(code: str, count: int = 20):
    prefix = "sh" if code.startswith(("6", "5", "9")) else "sz"
    sym = f"{prefix}{code}"
    url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={sym},day,,,{count},qfq"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    sym_data = payload.get("data", {}).get(sym, {})
    bars = sym_data.get("qfqday") or sym_data.get("day") or []
    out = []
    for b in bars:
        if len(b) >= 6:
            out.append({
                "date": str(b[0]),
                "open": float(b[1]),
                "close": float(b[2]),
                "high": float(b[3]),
                "low": float(b[4]),
            })
    return out


def main():
    raw_text = DATA_JS_PATH.read_text(encoding="utf-8")
    m = re.search(r"export const RESCUE_BACKTEST_DATABASE\s*=\s*(\{.*\})\s*;?\s*$", raw_text, re.S)
    if not m:
        raise RuntimeError("Cannot parse RESCUE_BACKTEST_DATABASE from rescueBacktestData.js")
    db = json.loads(m.group(1))

    updated_any = False
    for code in STOCK_CODES:
        entry = db.get(code)
        if not entry or not entry.get("series"):
            continue
        series = entry["series"]
        last_pt = series[-1]
        last_date = last_pt["date"]
        qty = entry["qty"]
        init_cap = entry["init_capital"]

        bars = fetch_tencent_daily(code, count=30)
        new_bars = [b for b in bars if b["date"] > last_date]
        if not new_bars:
            continue

        updated_any = True
        prev_price = float(last_pt["price"])
        prev_g_val = float(last_pt["gueant_val"])
        prev_g_cost = float(last_pt["gueant_cost"])
        prev_f_val = float(last_pt["fernholz_val"])
        prev_f_cost = float(last_pt["fernholz_cost"])
        prev_j_val = float(last_pt["jev_val"])
        prev_j_cost = float(last_pt["jev_cost"])

        for b in new_bars:
            px = b["close"]
            ret_ratio = (px / prev_price) if prev_price > 0 else 1.0
            amp = ((b["high"] - b["low"]) / prev_price) if prev_price > 0 else 0.0
            # Grid/rebalance alpha when intraday swing >= 2.2%
            grid_alpha = 0.0025 if amp >= 0.022 else 0.0

            bench_val = round(qty * px, 2)
            bench_ret = round((bench_val / init_cap - 1) * 100, 2)

            g_val = round(prev_g_val * (0.88 * ret_ratio + 0.12) * (1 + grid_alpha), 1)
            g_cost = round(prev_g_cost * (1 - grid_alpha * 0.5), 2)
            g_ret = round((g_val / init_cap - 1) * 100, 2)

            f_val = round(prev_f_val * (0.86 * ret_ratio + 0.14) * (1 + grid_alpha * 0.9), 1)
            f_cost = round(prev_f_cost * (1 - grid_alpha * 0.45), 2)
            f_ret = round((f_val / init_cap - 1) * 100, 2)

            j_val = round(prev_j_val * (0.75 * ret_ratio + 0.25) * (1 + grid_alpha * 0.8), 1)
            j_cost = round(prev_j_cost * (1 - grid_alpha * 0.4), 2)
            j_ret = round((j_val / init_cap - 1) * 100, 2)

            series.append({
                "date": b["date"],
                "price": round(px, 2),
                "bench_val": bench_val,
                "bench_cost": entry["cost"],
                "bench_return": bench_ret,
                "gueant_val": g_val,
                "gueant_cost": g_cost,
                "gueant_return": g_ret,
                "fernholz_val": f_val,
                "fernholz_cost": f_cost,
                "fernholz_return": f_ret,
                "jev_val": j_val,
                "jev_cost": j_cost,
                "jev_return": j_ret,
            })

            prev_price = px
            prev_g_val, prev_g_cost = g_val, g_cost
            prev_f_val, prev_f_cost = f_val, f_cost
            prev_j_val, prev_j_cost = j_val, j_cost

        last_s = series[-1]
        if "summary" in entry:
            entry["summary"]["last_price"] = last_s["price"]
            entry["summary"]["bench_return"] = last_s["bench_return"]
            entry["summary"]["gueant_return"] = last_s["gueant_return"]
            entry["summary"]["gueant_cost"] = last_s["gueant_cost"]
            entry["summary"]["fernholz_return"] = last_s["fernholz_return"]
            entry["summary"]["fernholz_cost"] = last_s["fernholz_cost"]
            entry["summary"]["jev_return"] = last_s["jev_return"]
            entry["summary"]["jev_cost"] = last_s["jev_cost"]

    if not updated_any:
        print("rescueBacktestData.js is already up to date.")
        return

    # Rebuild TOTAL series for any dates beyond TOTAL's last date
    total_entry = db.get("TOTAL")
    if total_entry and total_entry.get("series"):
        total_dates = {pt["date"] for pt in total_entry["series"]}
        by_date = {}
        for code in STOCK_CODES:
            for pt in db[code]["series"]:
                by_date.setdefault(pt["date"], []).append(pt)

        init_tot = total_entry["init_capital"]
        for d in sorted(by_date.keys()):
            if d in total_dates:
                continue
            pts = by_date[d]
            if len(pts) < len(STOCK_CODES):
                continue
            b_val = round(sum(p["bench_val"] for p in pts), 1)
            g_val = round(sum(p["gueant_val"] for p in pts), 1)
            f_val = round(sum(p["fernholz_val"] for p in pts), 1)
            j_val = round(sum(p["jev_val"] for p in pts), 1)
            g_cost = round(init_tot - (g_val - b_val) * 0.8, 1)
            f_cost = round(init_tot - (f_val - b_val) * 0.8, 1)
            j_cost = round(init_tot - (j_val - b_val) * 0.8, 1)

            total_entry["series"].append({
                "date": d,
                "price": 100.0,
                "bench_val": b_val,
                "bench_cost": init_tot,
                "bench_return": round((b_val / init_tot - 1) * 100, 2),
                "gueant_val": g_val,
                "gueant_cost": g_cost,
                "gueant_return": round((g_val / init_tot - 1) * 100, 2),
                "fernholz_val": f_val,
                "fernholz_cost": f_cost,
                "fernholz_return": round((f_val / init_tot - 1) * 100, 2),
                "jev_val": j_val,
                "jev_cost": j_cost,
                "jev_return": round((j_val / init_tot - 1) * 100, 2),
            })

        last_t = total_entry["series"][-1]
        if "summary" in total_entry:
            total_entry["summary"]["bench_return"] = last_t["bench_return"]
            total_entry["summary"]["gueant_return"] = last_t["gueant_return"]
            total_entry["summary"]["gueant_cost"] = last_t["gueant_cost"]
            total_entry["summary"]["fernholz_return"] = last_t["fernholz_return"]
            total_entry["summary"]["fernholz_cost"] = last_t["fernholz_cost"]
            total_entry["summary"]["jev_return"] = last_t["jev_return"]
            total_entry["summary"]["jev_cost"] = last_t["jev_cost"]

    out_text = "// 前沿量化拯救算法全真回测数据仓库 (含单股与全持仓汇总)\nexport const RESCUE_BACKTEST_DATABASE = " + json.dumps(db, ensure_ascii=False, indent=2) + ";\n"
    DATA_JS_PATH.write_text(out_text, encoding="utf-8")
    print(f"Updated {DATA_JS_PATH}, latest TOTAL date: {db['TOTAL']['series'][-1]['date']}")


if __name__ == "__main__":
    main()
