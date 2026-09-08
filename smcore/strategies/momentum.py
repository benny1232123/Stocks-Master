from __future__ import annotations

"""A股动量 / 相对强度策略（smcore 多策略体系第 5 个），轻量实现。

设计定位（与现有四策略互补）：
- Boll   = 买「超卖 / 近下轨」的弱势反转
- CCTV   = 买「舆情热门」板块
- Theme  = 买「题材 + 短线动量」
- Relativity = 买「指数相对强弱（抗跌/跟涨）」
- Momentum = 买「中期上升趋势的强势股」（20/60 日收益 + MA20 上行 + 近高点）

实现要点（轻量、可控）：
1. 实盘模式：用新浪 `ak.stock_zh_a_spot()` 一次拉全市场快照做廉价预筛（价格/流动性/
   当日涨幅），避免对全市场逐只拉 K 线。默认不用东财接口（东财接口常不可用），仅在
   MOMENTUM_USE_EASTMONEY=1 时回退尝试东财快照（含 60日涨跌幅/换手率 更丰富）。
2. 重放模式（--date 或 SIGNAL_DATE 环境变量，即历史回填）：**禁止使用实时快照**——
   快照反映的是运行日市场，与信号日脱节，构成前视偏差（20260905 评审确认）。
   改为全市场代码表 + 逐只信号日 K 线计算宇宙（价格/成交额/动量全部取自信号日数据），
   首次扫描依赖 k_data 缓存预热，单进程多日期（--dates）复用内存缓存。
3. 仅对通过过滤的候选取前复权 K 线确认 20 日收益、MA20 斜率、距 20 日高点距离。
4. 严格排除创业板(30x)/科创板(688x)，价格 5~50，与 Relativity 边界一致。
5. 仅选处于上升趋势（20日收益>0 且 MA20 上行）的票，作为「买强不买弱」维度。

输出 stock_data/Stock-Selection-Momentum-{today}.csv
(股票代码, 股票名称, 建议买入价, 动量分, 20日收益%, 60日收益%, MA20斜率%, 距20日高点%)
"""
import argparse
import concurrent.futures as cf
import os
import sqlite3
import time
from datetime import datetime, timedelta

import akshare as ak
import pandas as pd

from smcore.config.defaults import STOCK_DATA_DIR
from smcore.data.kline import fetch_daily_k
from smcore.utils.code import format_stock_code


def _load_momentum_config():
    """从 risk_config.json 读取动量策略参数（消除模块级魔数）。"""
    try:
        import json
        _cfg_path = STOCK_DATA_DIR.parent / "smcore" / "strategy" / "risk_config.json"
        with open(_cfg_path, "r", encoding="utf-8") as _f:
            _cfg = json.load(_f)
        return _cfg.get("momentum", {})
    except Exception:
        return {}


# —— 预筛参数（默认值仅作 fallback；正常全部走 config）——
_MCFG = _load_momentum_config()
PRICE_UPPER_LIMIT = float(_MCFG.get("price_upper_limit", 50.0))
PRICE_LOWER_LIMIT = float(_MCFG.get("price_lower_limit", 5.0))
MIN_TURNOVER = float(_MCFG.get("min_turnover", 2e8))
MIN_TURNOVER_RATE = float(_MCFG.get("min_turnover_rate", 1.0))
MIN_60D_RETURN = float(_MCFG.get("min_60d_return", 0.0))
MAX_CANDIDATES = int(_MCFG.get("max_candidates", 80))
TOP_N = int(_MCFG.get("top_n", 30))

# 动量评分权重
W_RET20 = float(_MCFG.get("w_ret20", 0.40))
W_RET60 = float(_MCFG.get("w_ret60", 0.30))
W_MA20_SLOPE = float(_MCFG.get("w_ma20_slope", 0.30))

# 打分校正（原 +5/-8 魔数已改为与分数量纲对齐的合理值）
VOL_CONFIRM_BONUS = float(_MCFG.get("vol_confirm_bonus", 2.0))
NEAR_HIGH_PENALTY = float(_MCFG.get("near_high_penalty", 3.0))
NEAR_HIGH_THRESHOLD = float(_MCFG.get("near_high_threshold", 0.02))
FAR_FROM_HIGH_THRESHOLD = float(_MCFG.get("far_from_high_threshold", -0.18))

# 重放模式的进程级 K 线内存缓存：跨日期复用（首次填充后，同进程后续日期零成本）
_KLINE_MEM_CACHE: dict[str, pd.DataFrame] = {}


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="A股动量/相对强度策略（轻量）")
    p.add_argument("--price-upper-limit", type=float, default=PRICE_UPPER_LIMIT)
    p.add_argument("--price-lower-limit", type=float, default=PRICE_LOWER_LIMIT)
    p.add_argument("--min-turnover", type=float, default=MIN_TURNOVER)
    p.add_argument("--min-turnover-rate", type=float, default=MIN_TURNOVER_RATE)
    p.add_argument("--min-60d-return", type=float, default=MIN_60D_RETURN)
    p.add_argument("--max-candidates", type=int, default=MAX_CANDIDATES)
    p.add_argument("--top-n", type=int, default=TOP_N)
    p.add_argument("--sleep-seconds", type=float, default=0.05)
    p.add_argument("--date", default=None,
                   help="信号日 YYYY-MM-DD（历史重放用；默认今天）。K线窗口锚定该日，"
                        "避免 datetime.now() 造成的时点错配/前视偏差。")
    p.add_argument("--dates", default=None,
                   help="逗号分隔多信号日（历史重建用）：单进程内复用K线内存缓存，逐日产出。")
    return p.parse_args(argv)


def _fetch_spot_sina() -> pd.DataFrame | None:
    """新浪全市场快照（东财-free，沙箱/海外均可达）。列: 代码,名称,最新价,涨跌幅,成交额...

    新浪接口偶发返回空 df（瞬断），故重试 3 次；仍为空则返回 None 由上层降级。
    仅实盘模式使用——重放模式禁止快照（前视偏差）。
    """
    for attempt in range(3):
        try:
            df = ak.stock_zh_a_spot()
            if df is not None and not df.empty:
                return df
            print(f"[动量] 新浪快照第 {attempt + 1} 次返回空，重试...")
        except Exception as exc:  # noqa: BLE001
            print(f"[动量] 新浪快照第 {attempt + 1} 次失败（{type(exc).__name__}），重试...")
        time.sleep(2)
    print("[动量] 新浪快照重试后仍为空")
    return None


def _fetch_spot_em() -> pd.DataFrame | None:
    """东财全市场快照（列更丰富：含 60日涨跌幅/换手率）。仅在 MOMENTUM_USE_EASTMONEY=1 时尝试，
    用线程超时(15s)包裹避免东财接口挂起拖垮整策略。"""
    try:
        with cf.ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(ak.stock_zh_a_spot_em)
            return fut.result(timeout=15)
    except Exception as exc:  # noqa: BLE001
        print(f"[动量] 东财快照失败（{type(exc).__name__}），跳过")
        return None


def _fetch_spot() -> pd.DataFrame:
    """拉全市场快照：默认新浪（东财-free）；显式开关下回退东财。"""
    df = _fetch_spot_sina()
    if (df is None or df.empty) and os.getenv("MOMENTUM_USE_EASTMONEY") == "1":
        df = _fetch_spot_em()
    return df if df is not None else pd.DataFrame()


def _load_all_codes() -> list[tuple[str, str]]:
    """全市场代码表：本地 sqlite（stock_info_a_code_name）优先，失败回退 akshare。

    重放模式的宇宙来源——与快照无关，保证逐日重放时宇宙口径一致。
    """
    try:
        conn = sqlite3.connect(str(STOCK_DATA_DIR / "stocks_data.db"))
        tables = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table'", conn)["name"].tolist()
        df = pd.DataFrame()
        for t in tables:
            if "stock_info_a_code_name" in t:
                try:
                    df = pd.read_sql_query(f'SELECT * FROM "{t}"', conn)
                    break
                except Exception:
                    continue
        conn.close()
        if not df.empty and {"code", "name"}.issubset(df.columns):
            out = [(str(c).strip().zfill(6), str(n).strip())
                   for c, n in zip(df["code"], df["name"]) if str(c).strip()]
            if out:
                return out
    except Exception:
        pass
    try:
        df = ak.stock_info_a_code_name()
        return [(str(c).strip().zfill(6), str(n).strip())
                for c, n in zip(df["code"], df["name"])]
    except Exception:
        return []


def _get_kline(code: str, start: str, end: str) -> pd.DataFrame | None:
    """K线获取：重放多日期时走进程级内存缓存（一次读取，跨日期复用）。"""
    cached = _KLINE_MEM_CACHE.get(code)
    if cached is not None:
        return cached[(cached["date"] >= start) & (cached["date"] <= end)] if len(cached) else None
    return fetch_daily_k(code, start, end, adjust="qfq")


def _warm_mem_cache(code: str) -> None:
    """把该代码的全历史 K 线装入内存缓存（供多日期重放复用）。

    优先直接读 k_data 磁盘缓存：fetch_daily_k 对复权跳变股每次都会触发
    「单次全量拉取覆盖」（1216 只漂移股 × 全量拉取曾让预热耗时 1 小时+），
    直接读 CSV 则秒级完成；坏 bar 由下游数据守卫处理。
    """
    if code in _KLINE_MEM_CACHE:
        return
    cache_path = STOCK_DATA_DIR / "k_data" / f"{code}_qfq_full.csv"
    if cache_path.exists():
        try:
            df = pd.read_csv(cache_path, encoding="utf-8-sig")
            if not df.empty and "date" in df.columns and "close" in df.columns:
                df = df.copy()
                df["date"] = df["date"].astype(str)
                _KLINE_MEM_CACHE[code] = df
                return
        except Exception:
            pass
    try:
        df = fetch_daily_k(code, "2015-01-01", datetime.now().strftime("%Y-%m-%d"), adjust="qfq")
    except Exception:
        df = None
    if df is not None and not df.empty:
        if "date" not in df.columns:
            return
        df = df.copy()
        df["date"] = df["date"].astype(str)
        _KLINE_MEM_CACHE[code] = df


def _momentum_metrics(code: str, sleep_seconds: float, min_60d: float = MIN_60D_RETURN,
                      as_of_date: str | None = None) -> dict | None:
    # K线窗口锚定信号日（as_of_date），默认今天；避免 datetime.now() 造成的时点错配/前视偏差，
    # 使策略可被历史诚实重放（backfill / PositionMonitor 传信号日即可）。
    # 注意同时接受 YYYY-MM-DD 与 YYYYMMDD——解析失败静默回退 now() 会让重放全按最新数据
    # 计算（20260905 实测多个日期输出完全相同的翻车）。
    end_dt = None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        if as_of_date:
            try:
                end_dt = datetime.strptime(as_of_date.strip(), fmt)
                break
            except (ValueError, TypeError):
                continue
    if end_dt is None:
        end_dt = datetime.now()
    end = end_dt.strftime("%Y-%m-%d")
    # 窗口必须覆盖 60 日收益所需的 61+ 根交易日：80 日历天仅 ≈55 根，会让所有候选
    # 因「不足 61 根」被静默剔除（20260905 发现旧重放产物因此全为空文件）。
    start = (end_dt - timedelta(days=150)).strftime("%Y-%m-%d")
    time.sleep(max(sleep_seconds, 0.0))
    df = _get_kline(code, start, end)
    if df is None or len(df) < 25:
        return None
    close = pd.to_numeric(df["close"], errors="coerce")
    high = pd.to_numeric(df["high"], errors="coerce")
    vol = pd.to_numeric(df["volume"], errors="coerce")
    if close.isna().any() or len(close) < 25:
        return None
    last = float(close.iloc[-1])
    if last <= 0:
        return None
    # 必须至少 61 根才能可靠计算 60 日收益；不足则剔除（不再退回窗口起点 close[0]，避免失真评分）。
    if len(close) <= 61:
        return None
    try:
        ret20 = last / float(close.iloc[-21]) - 1
        ret60 = last / float(close.iloc[-61]) - 1
    except (IndexError, ZeroDivisionError):
        return None
    if ret60 < min_60d:
        return None  # 必须 60 日收益达标（中期已走强；新浪快照无 60日列时由此处把关）
    if ret20 <= 0:
        return None  # 必须 20 日收益为正（上升趋势）
    # MA20 斜率：当前 MA20 vs 20 个交易日前 MA20
    ma20_now = float(close.tail(20).mean())
    if len(close) >= 40:
        ma20_prev = float(close.iloc[-40:-20].mean())
    else:
        ma20_prev = float(close.iloc[0])
    ma20_slope = (ma20_now - ma20_prev) / ma20_prev if ma20_prev > 0 else 0.0
    if ma20_slope <= 0:
        return None  # MA20 必须上行
    high20 = float(high.tail(20).max())
    dist_from_high = last / high20 - 1 if high20 > 0 else 0.0
    # 量能确认：近 5 日均量 > 近 20 日均量
    vol_confirm = bool(vol.tail(5).mean() > vol.tail(20).mean() * 1.05) if len(vol) >= 20 else False
    # 成交额（重放宇宙的流动性过滤：快照口径在重放模式不可用，改用信号日 K 线）
    last_amount = 0.0
    if "amount" in df.columns:
        amt = pd.to_numeric(df["amount"], errors="coerce")
        if len(amt) and pd.notna(amt.iloc[-1]):
            last_amount = float(amt.iloc[-1])
    return {
        "ret20": ret20,
        "ret60": ret60,
        "ma20_slope": ma20_slope,
        "dist_from_high": dist_from_high,
        "vol_confirm": vol_confirm,
        "last": last,
        "amount": last_amount,
    }


def _score_candidate(m: dict, args: argparse.Namespace) -> float | None:
    """统一打分与近高点调整（快照/重放两条路径共用，保证口径一致）。"""
    score = (
        m["ret20"] * 100 * W_RET20
        + m["ret60"] * 100 * W_RET60
        + m["ma20_slope"] * 100 * W_MA20_SLOPE
    )
    if m["vol_confirm"]:
        score += VOL_CONFIRM_BONUS
    # 距高点过近视为追高略降分；过远视为转弱剔除（阈值均走 config）
    if m["dist_from_high"] > NEAR_HIGH_THRESHOLD:
        score -= NEAR_HIGH_PENALTY
    if m["dist_from_high"] < FAR_FROM_HIGH_THRESHOLD:
        return None
    return score


def _write_empty_momentum(date_str: str, reason: str) -> None:
    """写空动量标记文件，让 fusion 能区分「跑了但为空」vs「没跑过」。"""
    out_path = STOCK_DATA_DIR / f"Stock-Selection-Momentum-{date_str}.csv"
    pd.DataFrame(columns=["股票代码", "股票名称", "建议买入价", "动量分",
                          "20日收益%", "60日收益%", "MA20斜率%", "距20日高点%"]).to_csv(
        out_path, index=False, encoding="utf-8-sig"
    )
    print(f"[动量] 产出=0（{reason}）→ 已写入空文件 {out_path.name}")


def _save_rows(rows: list[dict], today_text: str, args: argparse.Namespace) -> None:
    if not rows:
        print("[动量] 无符合动量条件的股票")
        _write_empty_momentum(today_text, "K线确认后无符合条件的股票")
        return
    out = pd.DataFrame(rows).sort_values("动量分", ascending=False).head(args.top_n)
    out_path = STOCK_DATA_DIR / f"Stock-Selection-Momentum-{today_text}.csv"
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n{out_path.name} 已保存，共 {len(out)} 只")
    print(out.to_string(index=False))


def _run_replay(args: argparse.Namespace, today_text: str) -> None:
    """重放模式：全市场 K 线宇宙（无快照前视）。宇宙/价格/成交额/动量全部取自信号日数据。"""
    codes = _load_all_codes()
    codes = [(c, n) for c, n in codes if c[0] in "036"
             and not c.startswith("30") and not c.startswith("688")]
    print(f"[动量] 重放模式（信号日 {today_text}）：全市场 {len(codes)} 只，K线宇宙逐只确认 ...")
    rows = []
    done = 0
    total = len(codes)
    for code, name in codes:
        done += 1
        if done % 500 == 0:
            print(f"[动量] 重放扫描 {done}/{total}（内存缓存 {len(_KLINE_MEM_CACHE)} 只）")
        # 内存缓存命中 = 纯本地计算，无需 sleep 限速
        sleep_s = 0.0 if code in _KLINE_MEM_CACHE else args.sleep_seconds
        m = _momentum_metrics(code, sleep_s, args.min_60d_return, as_of_date=today_text)
        if m is None:
            continue
        if not (args.price_lower_limit <= m["last"] <= args.price_upper_limit):
            continue
        if m.get("amount", 0.0) < args.min_turnover:
            continue
        score = _score_candidate(m, args)
        if score is None:
            continue
        rows.append({
            "股票代码": code,
            "股票名称": name,
            "建议买入价": round(m["last"], 2),
            "动量分": round(score, 2),
            "20日收益%": round(m["ret20"] * 100, 2),
            "60日收益%": round(m["ret60"] * 100, 2),
            "MA20斜率%": round(m["ma20_slope"] * 100, 2),
            "距20日高点%": round(m["dist_from_high"] * 100, 2),
        })
    _save_rows(rows, today_text, args)


def _run_live(args: argparse.Namespace, today_text: str) -> None:
    """实盘模式：新浪快照廉价预筛 + K线确认（宇宙锚定今日，无前视问题）。"""
    print(f"[动量] 拉取全市场快照（新浪，东财-free）...")
    try:
        spot = _fetch_spot()
    except Exception as exc:
        print(f"[动量] 快照拉取失败（{type(exc).__name__}），跳过本策略: {exc}")
        _write_empty_momentum(today_text, f"快照拉取失败: {exc}")
        return
    if spot is None or spot.empty:
        print("[动量] 快照为空，退出")
        _write_empty_momentum(today_text, "快照为空")
        return
    spot = spot.copy()
    spot["代码"] = spot["代码"].astype(str).str.zfill(6)
    spot["最新价"] = pd.to_numeric(spot.get("最新价"), errors="coerce")
    spot["成交额"] = pd.to_numeric(spot.get("成交额"), errors="coerce")
    has_turn = "换手率" in spot.columns
    has_60 = "60日涨跌幅" in spot.columns
    if has_turn:
        spot["换手率"] = pd.to_numeric(spot.get("换手率"), errors="coerce")
    if has_60:
        spot["60日涨跌幅"] = pd.to_numeric(spot.get("60日涨跌幅"), errors="coerce")

    mask = (
        ~spot["代码"].str.startswith("30")
        & ~spot["代码"].str.startswith("688")
        & (spot["最新价"] >= args.price_lower_limit)
        & (spot["最新价"] <= args.price_upper_limit)
        & (spot["成交额"] >= args.min_turnover)
    )
    if has_60:
        mask &= (spot["60日涨跌幅"] >= args.min_60d_return)
    if has_turn:
        mask &= (spot["换手率"] >= args.min_turnover_rate)
    cand = spot[mask].copy()
    if cand.empty:
        print("[动量] 预筛后无候选")
        _write_empty_momentum(today_text, "预筛后无候选")
        return
    sort_col = "60日涨跌幅" if has_60 else "涨跌幅"
    cand = cand.sort_values(sort_col, ascending=False).head(args.max_candidates)
    print(f"[动量] 预筛候选 {len(cand)} 只（快照源={'东财' if has_60 else '新浪'}），开始拉 K 线确认动量 ...")

    rows = []
    for _, r in cand.iterrows():
        code = format_stock_code(r["代码"])
        name = str(r.get("名称", "") or "")
        m = _momentum_metrics(code, args.sleep_seconds, args.min_60d_return, as_of_date=today_text)
        if m is None:
            continue
        score = _score_candidate(m, args)
        if score is None:
            continue
        rows.append({
            "股票代码": code,
            "股票名称": name,
            "建议买入价": round(m["last"], 2),
            "动量分": round(score, 2),
            "20日收益%": round(m["ret20"] * 100, 2),
            "60日收益%": round(m["ret60"] * 100, 2),
            "MA20斜率%": round(m["ma20_slope"] * 100, 2),
            "距20日高点%": round(m["dist_from_high"] * 100, 2),
        })
        print(f"[动量] PASS {code} {name} | 动量分={score:.1f} 20日={m['ret20']*100:.1f}% MA20斜率={m['ma20_slope']*100:.1f}%")
    _save_rows(rows, today_text, args)


def _resolve_signal_date(args_date: str | None) -> tuple[str, bool]:
    """解析信号日并判定是否重放模式。

    重放 = 显式 --date/--dates，或 SIGNAL_DATE 环境变量（run_strategy_for_date 冻结回放所设）。
    冻结场景下 datetime.now() 已等于信号日，无法靠日期对比区分，必须看环境变量。
    日期同时接受 YYYY-MM-DD 与 YYYYMMDD 两种格式（--dates 列表来自文件名，为后者）。
    """
    env_date = (os.environ.get("SIGNAL_DATE") or "").strip()
    if args_date:
        d = args_date.strip()
        for fmt in ("%Y-%m-%d", "%Y%m%d"):
            try:
                return datetime.strptime(d, fmt).strftime("%Y%m%d"), True
            except (ValueError, TypeError):
                continue
        print(f"[动量] 无法解析信号日 '{d}'，忽略")
    if env_date and len(env_date) == 8 and env_date.isdigit():
        return env_date, True
    return datetime.now().strftime("%Y%m%d"), False


def run_momentum(argv=None) -> None:
    args = parse_args(argv)
    STOCK_DATA_DIR.mkdir(parents=True, exist_ok=True)

    dates_list = [d.strip() for d in args.dates.split(",") if d.strip()] if args.dates else [None]
    multi = len(dates_list) > 1
    if multi:
        codes = _load_all_codes()
        # 只留沪深个股（0/3/6 开头）；920xxx 北交所等 baostock 无数据，混入会永久缺缓存
        codes = [(c, n) for c, n in codes if c[0] in "036"
                 and not c.startswith("30") and not c.startswith("688")]
        # 预热仅读磁盘缓存（缺失代码由 scripts/warmup_missing_klines.py 独立子进程补拉）——
        # 进程内网络拉取在数据源不稳定时会无声硬崩（无 traceback，rc=1，实测多次）。
        missing = 0
        for i, (c, _n) in enumerate(codes, 1):
            if c not in _KLINE_MEM_CACHE:
                _warm_mem_cache(c)
            if c not in _KLINE_MEM_CACHE:
                missing += 1
            if i % 1000 == 0:
                print(f"[动量] 预热 {i}/{len(codes)}（内存缓存 {len(_KLINE_MEM_CACHE)} 只，缺失 {missing}）")
        print(f"[动量] 预热完成：内存缓存 {len(_KLINE_MEM_CACHE)} 只，缺缓存剔除 {missing} 只")
        codes = [(c, n) for c, n in codes if c in _KLINE_MEM_CACHE]
    for d in dates_list:
        today_text, replay = _resolve_signal_date(d)
        if multi or replay:
            print(f"[动量] ===== 信号日 {today_text}（重放={'K线宇宙' if replay else '快照'}）=====")
        if replay:
            _run_replay(args, today_text)
        else:
            _run_live(args, today_text)


if __name__ == "__main__":
    run_momentum()
