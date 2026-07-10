#!/usr/bin/env python3
"""历史时点化回填：为缺失的交易日重建「前向信号回测」。

为什么需要它：
  之前的每日流水线只在近期几个交易日生成了 Daily-Action-List（信号清单），
  更早的交易日（约 06-10 ~ 07-06 的 19 个工作日）从未产生信号，因此没有前向回测。

做法（时点化、无后视镜偏差）：
  对每个缺失的历史交易日 D，用项目既有函数 *截至 D* 的 K 线重建信号：
    - Boll 超卖 / 近下轨：calc_bollinger + evaluate_boll_signal（K 线截至 D）
    - 相对强弱：个股 N 日收益 − 指数 N 日收益（K 线均截至 D）
  生成 Daily-Action-List-{D}.csv，再复用 daily_backtest._backtest_one 产出
  Multi-Backtest-{D}-*.csv（信号日次日开盘买入、持有 min(HOLD_DAYS, 距今天数) 天）。

诚实性说明（务必知悉）：
  资金流 3/5/10 日净额排行榜、基本面、流通股东、CCTV 舆论板块等 feed 没有历史日期参数，
  无法时点化还原，故回填日的信号仅含「价格类」信号（Boll + 相对强弱），不含上述精炼过滤。
  候选股票池取自「最新资金流排行正净流入并集」（仅用于缩小扫描范围，信号本身仍按截至 D 计算）。
  这与线上实时日的完整 4 策略口径略有差异，但信号本身是时点干净的（无后视镜偏差）。

用法：
  python scripts/backfill_signals.py              # 回填全部缺失交易日
  ONLY_DATE=20260615 python scripts/backfill_signals.py   # 仅回填单日（调试）
环境变量：
  HOLD_DAYS       持有天数（默认 10）
  KLINE_BACKEND   K 线后端（默认 akshare，快且无需登录）
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

# 强制 akshare 后端：快、无需 baostock 登录会话，适合本地/云端批量回填
os.environ.setdefault("KLINE_BACKEND", "akshare")

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from smcore.config.defaults import STOCK_DATA_DIR  # noqa: E402
from smcore.data.kline import fetch_daily_k  # noqa: E402
from smcore.indicators.boll import calc_bollinger, evaluate_boll_signal  # noqa: E402
from smcore.utils.code import format_stock_code  # noqa: E402
from scripts.daily_backtest import _backtest_one  # noqa: E402

# ── 模块级缓存（只算一次）──
_UNIVERSE: list[str] | None = None
_NAME_MAP: dict[str, str] | None = None

BOLL_WINDOW = 20
BOLL_K = 1.645
BOLL_NEAR_RATIO = 1.015
REL_LOOKBACK = 20
REL_TOP_N = 12
UNIVERSE_PRICE_LOWER = 5.0
UNIVERSE_PRICE_UPPER = 30.0


def get_universe() -> list[str]:
    """候选股票池（稳健三级兜底，保证即使网络抖动也有完整候选）：

    1. 最新资金流排行（3/5/10 日）正净流入并集（最聚焦，优先）
    2. 全市场快照（价格 [5,30]）
    3. 名称映射里的全部主板代码（stock_info_a_code_name 已拉取，最可靠，绝不空）
    仅用于缩小 Boll/相对强弱扫描范围；信号仍按截至 D 的 K 线计算（时点干净）。
    """
    global _UNIVERSE
    if _UNIVERSE is not None:
        return _UNIVERSE

    codes: set[str] = set()

    # 1) 资金流排行（重试多次，该接口偶发瞬断）
    try:
        import akshare as ak

        for indicator in ("3日", "5日", "10日"):
            for attempt in range(5):
                try:
                    df = ak.stock_individual_fund_flow_rank(indicator=indicator)
                    if df is not None and not df.empty:
                        col = "股票代码" if "股票代码" in df.columns else (df.columns[0] if len(df.columns) else None)
                        if col is not None:
                            for raw in df[col].astype(str).tolist():
                                c = format_stock_code(raw)
                                if not c:
                                    continue
                                if c.startswith(("30", "688", "920")) or c.startswith(("4", "8")):
                                    continue
                                codes.add(c)
                        break
                except Exception as e:
                    if attempt == 4:
                        print(f"[universe] 资金流排行({indicator})失败: {e}")
                    else:
                        time.sleep(3.0)
    except Exception as e:
        print(f"[universe] akshare 不可用: {e}")

    # 2) 全市场快照（价格过滤），重试多次
    if not codes:
        for attempt in range(3):
            try:
                import akshare as ak

                spot = ak.stock_zh_a_spot()
                if spot is not None and not spot.empty and {"代码", "最新价"}.issubset(spot.columns):
                    for raw, price in zip(spot["代码"].tolist(), spot["最新价"].tolist()):
                        c = format_stock_code(str(raw))
                        if not c:
                            continue
                        if c.startswith(("30", "688", "920")) or c.startswith(("4", "8")):
                            continue
                        try:
                            p = float(price)
                        except Exception:
                            continue
                        if UNIVERSE_PRICE_LOWER <= p <= UNIVERSE_PRICE_UPPER:
                            codes.add(c)
                    if codes:
                        break
            except Exception as e:
                if attempt == 2:
                    print(f"[universe] spot 兜底失败: {e}")
                else:
                    time.sleep(3.0)

    # 3) 终极兜底：名称映射里的全部主板代码（绝不空，时点干净的信号仍需 K 线计算）
    if not codes:
        print("[universe] 资金流/快照均不可用，回退到全主板代码（来自名称映射）")
        nm = get_name_map()
        for c in nm.keys():
            if c.startswith(("30", "688", "920", "4", "8", "9")):
                continue
            if "ST" in (nm.get(c) or "").upper():
                continue
            codes.add(c)

    # 补充：现有 Daily-Action-List 中出现过的代码（已知关注标的）
    for p in STOCK_DATA_DIR.glob("Daily-Action-List-*.csv"):
        try:
            d = pd.read_csv(p, encoding="utf-8-sig", nrows=2000)
            if "股票代码" in d.columns:
                for raw in d["股票代码"].dropna().tolist():
                    c = format_stock_code(str(raw))
                    if c:
                        codes.add(c)
        except Exception:
            pass

    _UNIVERSE = sorted(codes)
    print(f"[universe] 候选池规模: {len(_UNIVERSE)}")
    return _UNIVERSE


def get_name_map() -> dict[str, str]:
    global _NAME_MAP
    if _NAME_MAP is not None:
        return _NAME_MAP
    m: dict[str, str] = {}
    try:
        import akshare as ak

        df = ak.stock_info_a_code_name()
        if df is not None and not df.empty and {"code", "name"}.issubset(df.columns):
            tmp = df.copy()
            tmp["code"] = tmp["code"].apply(format_stock_code)
            m = dict(zip(tmp["code"], tmp["name"]))
    except Exception as e:
        print(f"[name_map] 获取名称映射失败: {e}")
    _NAME_MAP = m
    return m


def _filter_to_date(df: pd.DataFrame, D: date) -> pd.DataFrame:
    """只保留日期 <= D 的行（时点化，避免用到未来数据）。"""
    if df is None or df.empty:
        return df
    dt = pd.to_datetime(df["date"], errors="coerce")
    mask = dt.dt.date <= D
    return df[mask.values].copy()


def _scan_universe_asof(universe: list[str], name_map: dict[str, str], D: date, fetch_end: date):
    """单次遍历候选池：对每只股票拉取宽窗口 K 线（截至 fetch_end=today）一次填充缓存，
    再按截至 D 过滤计算 Boll 与相对强弱信号。

    关键优化：所有历史日共用同一份「宽窗口」缓存（fetch_end 统一为今天），
    第 1 天拉取后，后续 16 天直接命中磁盘缓存，避免 2000+ 只 × N 天重复网络请求。
    信号本身仍严格按截至 D 的时点计算，无后视镜偏差。
    """
    end = pd.Timestamp(D)
    start = (end - pd.Timedelta(days=90)).date()
    rel_start = (end - pd.Timedelta(days=REL_LOOKBACK + 10)).date()

    # 指数也用宽窗口拉取一次，后续天复用缓存
    idx_wide = fetch_daily_k("000001", rel_start, fetch_end)
    idx = _filter_to_date(idx_wide, D)
    idx_ret = None
    if idx is not None and not idx.empty and len(idx) >= REL_LOOKBACK + 1:
        ic = pd.to_numeric(idx["close"], errors="coerce").reset_index(drop=True)
        base = ic.iloc[-1 - REL_LOOKBACK]
        if not pd.isna(base) and base != 0:
            idx_ret = ic.iloc[-1] / base - 1

    boll_hits: list[dict] = []
    rel_scored: list[tuple[str, float, float]] = []
    for code in universe:
        try:
            # 宽窗口拉取（填充缓存）；计算时严格过滤到 D 当天及之前
            kdf_wide = fetch_daily_k(code, start, fetch_end)
            if kdf_wide is None or kdf_wide.empty:
                continue
            kdf = _filter_to_date(kdf_wide, D)
            if kdf.empty:
                continue
            n = len(kdf)
            if n >= BOLL_WINDOW:
                bd = calc_bollinger(kdf, window=BOLL_WINDOW, k=BOLL_K)
                if bd is not None and not bd.empty:
                    sig = evaluate_boll_signal(bd, near_ratio=BOLL_NEAR_RATIO)
                    if sig.get("selected"):
                        boll_hits.append({
                            "股票代码": format_stock_code(code),
                            "股票名称": name_map.get(format_stock_code(code), ""),
                            "来源策略": "Boll",
                            "建议买入价": float(pd.to_numeric(bd.iloc[-1]["close"], errors="coerce")),
                        })
            if idx_ret is not None and n >= REL_LOOKBACK + 1:
                c = pd.to_numeric(kdf["close"], errors="coerce").reset_index(drop=True)
                base = c.iloc[-1 - REL_LOOKBACK]
                if not pd.isna(base) and base != 0:
                    ret = c.iloc[-1] / base - 1
                    rs = ret - idx_ret
                    rel_scored.append((format_stock_code(code), rs, float(c.iloc[-1])))
        except Exception:
            continue

    rel_scored.sort(key=lambda x: -x[1])
    rel_hits = [
        {"股票代码": code, "股票名称": name_map.get(code, ""), "来源策略": "Relativity", "建议买入价": price}
        for code, _rs, price in rel_scored[:REL_TOP_N]
        if _rs > 0
    ]
    return boll_hits, rel_hits


def build_list(D: date, fetch_end: date) -> pd.DataFrame | None:
    universe = get_universe()
    name_map = get_name_map()
    boll, rel = _scan_universe_asof(universe, name_map, D, fetch_end)
    print(f"[回填] {D} · Boll 命中 {len(boll)} · 相对强弱命中 {len(rel)}")

    by_code: dict[str, dict] = {}
    for r in boll + rel:
        c = r["股票代码"]
        if c not in by_code:
            by_code[c] = dict(r)
        else:
            existing = by_code[c]
            strats = set(existing["来源策略"].split("/")) | set(r["来源策略"].split("/"))
            existing["来源策略"] = "/".join(sorted(strats))

    if not by_code:
        return None
    df = pd.DataFrame(list(by_code.values()))
    for col in ("股票代码", "股票名称", "来源策略", "建议买入价"):
        if col not in df.columns:
            df[col] = ""
    return df[["股票代码", "股票名称", "来源策略", "建议买入价"]]


def compute_missing_days(today: date, only: str | None = None) -> list[date]:
    if only:
        d = datetime.strptime(only, "%Y%m%d").date()
        tag = d.strftime("%Y%m%d")
        if (STOCK_DATA_DIR / f"Daily-Action-List-{tag}.csv").exists():
            print(f"[回填] {tag} 已存在清单，跳过")
            return []
        return [d]
    start = today - timedelta(days=30)
    days: list[date] = []
    d = start
    while d <= today - timedelta(days=1):
        if d.weekday() < 5:
            tag = d.strftime("%Y%m%d")
            if not (STOCK_DATA_DIR / f"Daily-Action-List-{tag}.csv").exists():
                days.append(d)
        d += timedelta(days=1)
    return days


def main() -> int:
    only = os.getenv("ONLY_DATE") or (sys.argv[1] if len(sys.argv) > 1 else None)
    hold_days = int(os.getenv("HOLD_DAYS", "10"))
    today = date.today()

    days = compute_missing_days(today, only)
    if not days:
        print("没有需要回填的交易日")
        return 0

    print(f"待回填 {len(days)} 天: " + ", ".join(d.strftime("%Y%m%d") for d in days))
    ok = 0
    fetch_end = today  # 宽窗口终点统一为今天，最大化缓存复用
    for D in days:
        df = build_list(D, fetch_end)
        if df is None or df.empty:
            print(f"[回填] {D} 无信号，跳过")
            continue
        tag = D.strftime("%Y%m%d")
        path = STOCK_DATA_DIR / f"Daily-Action-List-{tag}.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"[回填] 信号日 {tag} 生成 {len(df)} 只清单")
        try:
            res = _backtest_one(path, D, hold_days)
        except Exception as e:
            print(f"[回填] {tag} 回测异常: {e}")
            res = None
        if res:
            s = res["summary"]
            print(f"  → 回测 {res['num_trades']} 笔 · 总收益 {s.get('total_return')}% · "
                  f"回撤 {s.get('max_drawdown')}%")
            ok += 1
        else:
            print("  → 回测无有效结果（K 线拉取失败？）")

    print(f"\n回填完成：{ok}/{len(days)} 天成功生成前向回测")
    return 0


if __name__ == "__main__":
    sys.exit(main())
