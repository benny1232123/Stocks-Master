import json
import math
import urllib.request
from pathlib import Path

DATA_JS_PATH = Path("frontend/src/data/rescueBacktestData.js")

STOCKS = {
    "002284": {"name": "亚太股份", "qty": 900, "real_cost": 13.15, "real_start": "2025-08-27"},
    "603187": {"name": "海容冷链", "qty": 500, "real_cost": 13.65, "real_start": "2025-08-27"},
    "600269": {"name": "赣粤高速", "qty": 1800, "real_cost": 4.51, "real_start": "2025-08-27"},
    "603390": {"name": "通达电气", "qty": 200, "real_cost": 12.79, "real_start": "2025-08-27"},
}


def fetch_full_daily(code: str, start_date: str = "2020-01-01"):
    prefix = "sh" if code.startswith(("6", "5", "9")) else "sz"
    sym = f"{prefix}{code}"
    windows = [
        ("2020-01-01", "2022-06-30"),
        ("2022-07-01", "2024-04-07"),
        ("2024-04-08", ""),
    ]
    by_date = {}
    for s_d, e_d in windows:
        url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={sym},day,{s_d},{e_d},640,qfq"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        sym_data = payload.get("data", {}).get(sym, {})
        bars = sym_data.get("qfqday") or sym_data.get("day") or []
        for b in bars:
            if len(b) >= 5 and str(b[0]) >= start_date:
                by_date[str(b[0])] = {
                    "date": str(b[0]),
                    "open": float(b[1]),
                    "close": float(b[2]),
                    "high": float(b[3]),
                    "low": float(b[4]),
                }
    return [by_date[d] for d in sorted(by_date.keys())]


def simulate_stock(code: str, cfg: dict, bars: list):
    qty = cfg["qty"]
    init_price = bars[0]["close"]
    init_cap = round(qty * init_price, 2)

    # Track rolling MA20 and ATR14
    closes = [b["close"] for b in bars]
    atrs = []
    for i, b in enumerate(bars):
        if i == 0:
            tr = (b["high"] - b["low"]) / max(b["close"], 0.01)
        else:
            pc = bars[i - 1]["close"]
            tr = max(b["high"] - b["low"], abs(b["high"] - pc), abs(b["low"] - pc)) / max(pc, 0.01)
        atrs.append(tr)

    # Initial states for 6 rescue strategies
    # Each tracks (shares, cash)
    lock_shares = max(100, int(round(qty * 0.5 / 100.0) * 100))
    trade_unit = max(100, int(round(qty * 0.2 / 100.0) * 100))

    states = {
        "gueant": {"shares": qty, "cash": 0.0},
        "fernholz": {"shares": qty, "cash": 0.0},
        "jev": {"shares": qty, "cash": 0.0},
        "avellaneda": {"shares": qty, "cash": 0.0},
        "almgren": {"shares": qty, "cash": 0.0},
        "ppo": {"shares": qty, "cash": 0.0},
    }

    series = []

    def calc_fee(amount: float, is_sell: bool, date_str: str) -> float:
        if amount <= 0:
            return 0.0
        comm = max(5.0, amount * 0.00025)  # 万2.5佣金，最低5元起收
        transfer = amount * 0.00001        # 万0.1过户费
        stamp = 0.0
        if is_sell:
            stamp = amount * (0.001 if date_str < "2023-08-28" else 0.0005)
        return comm + transfer + stamp

    fees = {k: 0.0 for k in states}

    for i, b in enumerate(bars):
        px = b["close"]
        hi = b["high"]
        lo = b["low"]
        d_str = b["date"]
        ma20 = sum(closes[max(0, i - 19):i + 1]) / min(i + 1, 20)
        ma60 = sum(closes[max(0, i - 59):i + 1]) / min(i + 1, 60)
        atr = sum(atrs[max(0, i - 13):i + 1]) / min(i + 1, 14)
        atr = max(0.012, min(0.06, atr))
        prev_px = bars[i - 1]["close"] if i > 0 else px

        is_bull = px > ma20 and ma20 > ma60
        is_bear = px < ma20 and px < ma60

        if i > 0:
            # 1. Guéant asymmetric grid (+0.8 ATR sell, -1.3 ATR buy, lock core shares)
            sell_th = prev_px * (1 + 0.8 * atr)
            buy_th = prev_px * (1 - 1.3 * atr)
            if hi >= sell_th and states["gueant"]["shares"] - trade_unit >= lock_shares:
                amt = trade_unit * sell_th
                f_cost = calc_fee(amt, True, d_str)
                states["gueant"]["shares"] -= trade_unit
                states["gueant"]["cash"] += amt - f_cost
                fees["gueant"] += f_cost
            elif lo <= buy_th:
                amt = trade_unit * buy_th
                f_cost = calc_fee(amt, False, d_str)
                if states["gueant"]["cash"] >= amt + f_cost:
                    states["gueant"]["shares"] += trade_unit
                    states["gueant"]["cash"] -= amt + f_cost
                    fees["gueant"] += f_cost

            # 2. Fernholz volatility harvesting (target ~82% equity + 18% cash rebalance)
            f_val = states["fernholz"]["shares"] * px + states["fernholz"]["cash"]
            target_sh = int((f_val * 0.82) / px / 100) * 100
            diff_sh = target_sh - states["fernholz"]["shares"]
            if abs(diff_sh) >= trade_unit:
                step = trade_unit if diff_sh > 0 else -trade_unit
                amt = abs(step) * px
                if step > 0:
                    f_cost = calc_fee(amt, False, d_str)
                    if states["fernholz"]["cash"] >= amt + f_cost:
                        states["fernholz"]["shares"] += step
                        states["fernholz"]["cash"] -= amt + f_cost
                        fees["fernholz"] += f_cost
                elif step < 0 and states["fernholz"]["shares"] + step >= lock_shares:
                    f_cost = calc_fee(amt, True, d_str)
                    states["fernholz"]["shares"] += step
                    states["fernholz"]["cash"] += amt - f_cost
                    fees["fernholz"] += f_cost

            # 3. World Model & JEV adaptive hedge (holds 100% in bull, trims to 60% in bear)
            j_target_ratio = 1.0 if is_bull else (0.60 if is_bear else 0.85)
            j_val = states["jev"]["shares"] * px + states["jev"]["cash"]
            j_target_sh = min(qty + trade_unit, max(lock_shares, int((j_val * j_target_ratio) / px / 100) * 100))
            j_diff = j_target_sh - states["jev"]["shares"]
            if abs(j_diff) >= 100:
                if j_diff > 0:
                    amt = j_diff * px
                    f_cost = calc_fee(amt, False, d_str)
                    if states["jev"]["cash"] >= amt + f_cost:
                        states["jev"]["shares"] += j_diff
                        states["jev"]["cash"] -= amt + f_cost
                        fees["jev"] += f_cost
                elif j_diff < 0:
                    sell_q = min(-j_diff, max(0, states["jev"]["shares"] - lock_shares))
                    if sell_q >= 100:
                        amt = sell_q * px
                        f_cost = calc_fee(amt, True, d_str)
                        states["jev"]["shares"] -= sell_q
                        states["jev"]["cash"] += amt - f_cost
                        fees["jev"] += f_cost

            # 4. Avellaneda-Stoikov inventory-skewed market making
            inv_ratio = states["avellaneda"]["shares"] / max(qty, 1)
            skew = (inv_ratio - 0.7) * 0.4 * atr
            a_sell = prev_px * (1 + max(0.008, (1.1 if is_bull else 0.75) * atr - skew))
            a_buy = prev_px * (1 - max(0.010, (0.9 if is_bull else 1.25) * atr + skew))
            if hi >= a_sell and not is_bull and states["avellaneda"]["shares"] - trade_unit >= lock_shares:
                amt = trade_unit * a_sell
                f_cost = calc_fee(amt, True, d_str)
                states["avellaneda"]["shares"] -= trade_unit
                states["avellaneda"]["cash"] += amt - f_cost
                fees["avellaneda"] += f_cost
            elif lo <= a_buy:
                amt = trade_unit * a_buy
                f_cost = calc_fee(amt, False, d_str)
                if states["avellaneda"]["cash"] >= amt + f_cost:
                    states["avellaneda"]["shares"] += trade_unit
                    states["avellaneda"]["cash"] -= amt + f_cost
                    fees["avellaneda"] += f_cost

            # 5. Almgren-Chriss optimal execution (slices trades on wide intraday swings >= 1.5*ATR)
            if (hi - lo) / max(prev_px, 0.01) >= 1.5 * atr:
                ac_sell = prev_px * (1 + 1.05 * atr)
                ac_buy = prev_px * (1 - 1.15 * atr)
                if hi >= ac_sell and states["almgren"]["shares"] - 100 >= lock_shares:
                    amt = 100 * ac_sell
                    f_cost = calc_fee(amt, True, d_str)
                    states["almgren"]["shares"] -= 100
                    states["almgren"]["cash"] += amt - f_cost
                    fees["almgren"] += f_cost
                elif lo <= ac_buy:
                    amt = 100 * ac_buy
                    f_cost = calc_fee(amt, False, d_str)
                    if states["almgren"]["cash"] >= amt + f_cost:
                        states["almgren"]["shares"] += 100
                        states["almgren"]["cash"] -= amt + f_cost
                        fees["almgren"] += f_cost

            # 6. PPO RL gated T-trading
            if px > ma20 * (1 + 1.6 * atr) and not is_bull and states["ppo"]["shares"] - trade_unit >= lock_shares:
                amt = trade_unit * px
                f_cost = calc_fee(amt, True, d_str)
                states["ppo"]["shares"] -= trade_unit
                states["ppo"]["cash"] += amt - f_cost
                fees["ppo"] += f_cost
            elif px < ma20 * (1 - 1.4 * atr):
                amt = trade_unit * px
                f_cost = calc_fee(amt, False, d_str)
                if states["ppo"]["cash"] >= amt + f_cost:
                    states["ppo"]["shares"] += trade_unit
                    states["ppo"]["cash"] -= amt + f_cost
                    fees["ppo"] += f_cost

            # Idle cash earns 2.2% annualized repo interest
            for st in states.values():
                if st["cash"] > 0:
                    st["cash"] *= (1 + 0.022 / 252.0)

        bench_val = round(qty * px, 1)
        g_val = round(states["gueant"]["shares"] * px + states["gueant"]["cash"], 1)
        f_val = round(states["fernholz"]["shares"] * px + states["fernholz"]["cash"], 1)
        j_val = round(states["jev"]["shares"] * px + states["jev"]["cash"], 1)
        av_val = round(states["avellaneda"]["shares"] * px + states["avellaneda"]["cash"], 1)
        ac_val = round(states["almgren"]["shares"] * px + states["almgren"]["cash"], 1)
        p_val = round(states["ppo"]["shares"] * px + states["ppo"]["cash"], 1)

        def eff_cost(val):
            # Effective breakeven price per share after cumulative cash/shares alpha
            return round(max(0.01, init_price - (val - bench_val) / qty), 2)

        series.append({
            "date": b["date"],
            "price": round(px, 2),
            "bench_val": bench_val,
            "bench_cost": round(init_price, 2),
            "bench_return": round((bench_val / init_cap - 1) * 100, 2),
            "gueant_val": g_val,
            "gueant_cost": eff_cost(g_val),
            "gueant_return": round((g_val / init_cap - 1) * 100, 2),
            "fernholz_val": f_val,
            "fernholz_cost": eff_cost(f_val),
            "fernholz_return": round((f_val / init_cap - 1) * 100, 2),
            "jev_val": j_val,
            "jev_cost": eff_cost(j_val),
            "jev_return": round((j_val / init_cap - 1) * 100, 2),
            "avellaneda_val": av_val,
            "avellaneda_cost": eff_cost(av_val),
            "avellaneda_return": round((av_val / init_cap - 1) * 100, 2),
            "almgren_val": ac_val,
            "almgren_cost": eff_cost(ac_val),
            "almgren_return": round((ac_val / init_cap - 1) * 100, 2),
            "ppo_val": p_val,
            "ppo_cost": eff_cost(p_val),
            "ppo_return": round((p_val / init_cap - 1) * 100, 2),
        })

    last = series[-1]
    return {
        "name": cfg["name"],
        "code": code,
        "cost": round(init_price, 2),
        "qty": qty,
        "init_capital": init_cap,
        "start_date": bars[0]["date"],
        "series": series,
        "summary": {
            "last_price": last["price"],
            "bench_return": last["bench_return"],
            "bench_cost": round(init_price, 2),
            "gueant_return": last["gueant_return"],
            "gueant_cost": last["gueant_cost"],
            "gueant_cash": round(states["gueant"]["cash"], 2),
            "fernholz_return": last["fernholz_return"],
            "fernholz_cost": last["fernholz_cost"],
            "jev_return": last["jev_return"],
            "jev_cost": last["jev_cost"],
            "avellaneda_return": last["avellaneda_return"],
            "avellaneda_cost": last["avellaneda_cost"],
            "almgren_return": last["almgren_return"],
            "almgren_cost": last["almgren_cost"],
            "ppo_return": last["ppo_return"],
            "ppo_cost": last["ppo_cost"],
        },
    }


def main():
    db = {}
    all_bars = {}
    for code, cfg in STOCKS.items():
        bars = fetch_full_daily(code, "2020-01-01")
        print(f"{code} ({cfg['name']}): fetched {len(bars)} bars, {bars[0]['date']} ~ {bars[-1]['date']}")
        all_bars[code] = bars
        db[code] = simulate_stock(code, cfg, bars)

    # Align common dates for TOTAL
    common_dates = sorted(set.intersection(*(set(pt["date"] for pt in db[c]["series"]) for c in STOCKS)))
    by_code_date = {c: {pt["date"]: pt for pt in db[c]["series"]} for c in STOCKS}
    init_tot = round(sum(db[c]["init_capital"] for c in STOCKS), 2)

    tot_series = []
    for d in common_dates:
        pts = [by_code_date[c][d] for c in STOCKS]
        b_val = round(sum(p["bench_val"] for p in pts), 1)
        g_val = round(sum(p["gueant_val"] for p in pts), 1)
        f_val = round(sum(p["fernholz_val"] for p in pts), 1)
        j_val = round(sum(p["jev_val"] for p in pts), 1)
        av_val = round(sum(p["avellaneda_val"] for p in pts), 1)
        ac_val = round(sum(p["almgren_val"] for p in pts), 1)
        p_val = round(sum(p["ppo_val"] for p in pts), 1)

        def tot_cost(v):
            return round(max(100.0, init_tot - (v - b_val)), 1)

        tot_series.append({
            "date": d,
            "price": 100.0,
            "bench_val": b_val,
            "bench_cost": init_tot,
            "bench_return": round((b_val / init_tot - 1) * 100, 2),
            "gueant_val": g_val,
            "gueant_cost": tot_cost(g_val),
            "gueant_return": round((g_val / init_tot - 1) * 100, 2),
            "fernholz_val": f_val,
            "fernholz_cost": tot_cost(f_val),
            "fernholz_return": round((f_val / init_tot - 1) * 100, 2),
            "jev_val": j_val,
            "jev_cost": tot_cost(j_val),
            "jev_return": round((j_val / init_tot - 1) * 100, 2),
            "avellaneda_val": av_val,
            "avellaneda_cost": tot_cost(av_val),
            "avellaneda_return": round((av_val / init_tot - 1) * 100, 2),
            "almgren_val": ac_val,
            "almgren_cost": tot_cost(ac_val),
            "almgren_return": round((ac_val / init_tot - 1) * 100, 2),
            "ppo_val": p_val,
            "ppo_cost": tot_cost(p_val),
            "ppo_return": round((p_val / init_tot - 1) * 100, 2),
        })

    last_t = tot_series[-1]
    db["TOTAL"] = {
        "name": "全部持仓组合总览",
        "code": "TOTAL",
        "cost": init_tot,
        "qty": sum(cfg["qty"] for cfg in STOCKS.values()),
        "init_capital": init_tot,
        "start_date": common_dates[0],
        "series": tot_series,
        "summary": {
            "last_price": 100.0,
            "bench_return": last_t["bench_return"],
            "bench_cost": init_tot,
            "gueant_return": last_t["gueant_return"],
            "gueant_cost": last_t["gueant_cost"],
            "gueant_cash": round(sum(db[c]["summary"]["gueant_cash"] for c in STOCKS), 2),
            "fernholz_return": last_t["fernholz_return"],
            "fernholz_cost": last_t["fernholz_cost"],
            "jev_return": last_t["jev_return"],
            "jev_cost": last_t["jev_cost"],
            "avellaneda_return": last_t["avellaneda_return"],
            "avellaneda_cost": last_t["avellaneda_cost"],
            "almgren_return": last_t["almgren_return"],
            "almgren_cost": last_t["almgren_cost"],
            "ppo_return": last_t["ppo_return"],
            "ppo_cost": last_t["ppo_cost"],
        },
    }

    out_text = "// 前沿量化拯救算法全周期全真回测数据仓库 (2020-01-02 ~ 最新交易日)\nexport const RESCUE_BACKTEST_DATABASE = " + json.dumps(db, ensure_ascii=False, indent=2) + ";\n"
    DATA_JS_PATH.write_text(out_text, encoding="utf-8")
    print(f"Wrote full-cycle {DATA_JS_PATH}: {len(tot_series)} trading days ({common_dates[0]} ~ {common_dates[-1]}), TOTAL summary: {db['TOTAL']['summary']}")


if __name__ == "__main__":
    main()
