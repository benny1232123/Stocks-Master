"""量化测量各策略历史前向 edge —— 用于数据驱动地重定融合占比。

做法：
- 读所有 Daily-Action-List-{date}.csv，按「来源策略」列把候选股拆到对应策略桶
- 每个信号日只取该策略评分前 30 只（与当前 TOP_N 对齐，公平比较）
- 用改进后的前向回测引擎（硬止损+真实成本）对每策略分别跑前向 10 日回测
- 聚合每策略：平均总收益 / 胜率 / 最大回撤 / 夏普 / 交易笔数
- 同时跑「当前融合 top-30」作为基线对照

仅做研究，不修改任何生产文件。结果打印到 stdout。
"""
from __future__ import annotations

import glob
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

# 确保项目根在 sys.path（直接 `python scripts/xxx.py` 时脚本目录会遮蔽包根）
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

# 用 baostock 拉 K 线（沙箱/本地直连、快且稳，东财-free）。
# 云端(CI)若 baostock 不可达，fetch_daily_k 会自动回退到 akshare(新浪) 后端。
os.environ.setdefault("KLINE_BACKEND", "baostock")

from smcore.backtest.engine import run_forward_signal_backtest
import smcore.data.kline as kline_mod
from smcore.utils.code import format_stock_code

STUDY_DIR = "stock_data"
TODAY = date.today()
HOLD_DAYS = 10
TOP_N = 30
STRATS = ["Boll", "Relativity", "Theme", "CCTV"]


def _signal_date_from_name(path: str) -> date | None:
    name = os.path.basename(path)
    suffix = name.replace("Daily-Action-List-", "").replace(".csv", "")
    if len(suffix) == 8 and suffix.isdigit():
        return date(int(suffix[:4]), int(suffix[4:6]), int(suffix[6:8]))
    return None


def main() -> int:
    files = sorted(glob.glob(os.path.join(STUDY_DIR, "Daily-Action-List-*.csv")))
    # 只取已完成 10 日窗口的信号日（距今 >= 15 天）
    study = []
    for f in files:
        sd = _signal_date_from_name(f)
        if sd and (TODAY - sd).days >= 15:
            study.append((sd, f))
    if not study:
        print("无已完成窗口的信号日可供测量")
        return 1
    print(f"测量信号日数: {len(study)}  ({study[0][0]} ~ {study[-1][0]})")

    # 1) 拆桶：strategy -> signal_date -> [row dicts]
    buckets: dict[str, dict[date, list[dict]]] = {s: {} for s in STRATS}
    combined_rows: list[dict] = []
    unique_codes: set[str] = set()

    for sd, f in study:
        df = pd.read_csv(f)
        sd_str = sd.strftime("%Y-%m-%d")
        for _, r in df.iterrows():
            code = format_stock_code(r.get("股票代码", ""))
            if not code:
                continue
            unique_codes.add(code)
            src = str(r.get("来源策略", "") or "")
            score = float(r.get("综合评分", 0) or 0)
            rec = {
                "日期": sd_str,
                "代码": code,
                "综合评分": score,
                "建议买入价": r.get("建议买入价"),
                "止损价(下轨)": r.get("止损价(下轨)"),
                "止盈价(上轨)": r.get("止盈价(上轨)"),
            }
            combined_rows.append(rec)
            for s in src.split("/"):
                s = s.strip()
                if s in buckets:
                    buckets[s].setdefault(sd, []).append(rec)

    # 2) 预拉全量 K 线到缓存（每只股票一次，宽窗口覆盖所有信号日）
    print(f"预拉 K 线: {len(unique_codes)} 只唯一标的 ...")
    earliest = min(sd for sd, _ in study) - timedelta(days=5)
    latest = max(sd for sd, _ in study) + timedelta(days=HOLD_DAYS + 20)
    cache: dict[str, pd.DataFrame] = {}

    def _real_fetch(code6, start, end, adjust="qfq"):
        return kline_mod.fetch_daily_k(code6, start, end, adjust)

    def cached_fetch(code6, start, end, adjust="qfq"):
        if code6 not in cache or cache[code6] is None or cache[code6].empty:
            cache[code6] = _real_fetch(code6, earliest, latest, adjust)
        df = cache[code6]
        if df is None or df.empty:
            return df
        mask = (df["date"] >= str(start)) & (df["date"] <= str(end))
        return df.loc[mask].copy()

    # 执行预拉
    t0 = time.time()
    done = 0
    for code in sorted(unique_codes):
        cached_fetch(code, earliest, latest)
        done += 1
        if done % 25 == 0:
            print(f"  已拉 {done}/{len(unique_codes)}  ({(time.time()-t0):.0f}s)")
    print(f"  K线预拉完成 {(time.time()-t0):.0f}s")

    # 打补丁：让引擎用缓存
    kline_mod.fetch_daily_k = cached_fetch
    import smcore.backtest.engine as eng_mod
    eng_mod.fetch_daily_k = cached_fetch

    def run_set(rows: list[dict]):
        """对一组候选行跑前向回测，返回 (summary_list, n_batches)。"""
        if not rows:
            return [], 0
        sdf = pd.DataFrame(rows)
        res = run_forward_signal_backtest(
            sdf,
            hold_days=HOLD_DAYS,
            initial_capital=100000.0,
            max_positions=200,
            slippage=0.001,
            enable_exits=True,
            use_signal_bands=True,
            stop_loss_pct=0.08,
            take_profit_pct=0.06,       # +6% 固定止盈，锁定利润
            trailing_stop_pct=0.05,     # 移动止盈收紧至 5%（原 8% 太宽）
            trend_exit_ma=60,           # 收盘跌破 MA60 即走（截停下行市继续下跌）
        )
        if "error" in res.summary:
            return [], 0
        return [res.summary], 1

    def aggregate(summaries: list[dict]) -> dict:
        if not summaries:
            return {"n": 0}
        rets = [s["total_return"] for s in summaries]
        wrs = [s["win_rate"] for s in summaries]
        dds = [s["max_drawdown"] for s in summaries]
        shps = [s["sharpe"] for s in summaries]
        tr = [s["num_trades"] for s in summaries]
        return {
            "n": len(summaries),
            "avg_return": round(sum(rets) / len(rets), 2),
            "avg_win": round(sum(wrs) / len(wrs), 1),
            "avg_dd": round(sum(dds) / len(dds), 2),
            "avg_sharpe": round(sum(shps) / len(shps), 2),
            "total_trades": sum(tr),
        }

    # 3) 逐策略测量
    print(f"[出场配置] 硬止损=-8% 固定止盈=+6% 移动止盈=-5% 趋势破位=MA60 持有={HOLD_DAYS}日")
    results = {}
    for s in STRATS:
        rows_for_s: list[dict] = []
        for sd, recs in buckets[s].items():
            top = sorted(recs, key=lambda x: float(x.get("综合评分", 0) or 0), reverse=True)[:TOP_N]
            rows_for_s.extend(top)
        summ, nb = run_set(rows_for_s)
        results[s] = aggregate(summ)
        print(f"  {s:10s} 批次={results[s].get('n')} 收益={results[s].get('avg_return')}% "
              f"胜率={results[s].get('avg_win')}% 回撤={results[s].get('avg_dd')}% "
              f"夏普={results[s].get('avg_sharpe')} 交易={results[s].get('total_trades')}")

    # 4) 基线：当前融合 top-30
    baseline_rows: list[dict] = []
    for sd, f in study:
        df = pd.read_csv(f)
        top = df.sort_values("综合评分", ascending=False).head(TOP_N)
        sd_str = sd.strftime("%Y-%m-%d")
        for _, r in top.iterrows():
            baseline_rows.append({
                "日期": sd_str,
                "代码": str(r.get("股票代码", "")).strip(),
                "综合评分": float(r.get("综合评分", 0) or 0),
                "建议买入价": r.get("建议买入价"),
                "止损价(下轨)": r.get("止损价(下轨)"),
                "止盈价(上轨)": r.get("止盈价(上轨)"),
            })
    bsumm, bnb = run_set(baseline_rows)
    baseline = aggregate(bsumm)
    print(f"  {'BASELINE':10s} 批次={baseline.get('n')} 收益={baseline.get('avg_return')}% "
          f"胜率={baseline.get('avg_win')}% 回撤={baseline.get('avg_dd')}% "
          f"夏普={baseline.get('avg_sharpe')} 交易={baseline.get('total_trades')}")

    # 5) 输出建议权重
    print("\n===== 各策略 edge 对比 =====")
    print(f"{'策略':10s} {'收益%':>8s} {'胜率%':>8s} {'回撤%':>8s} {'夏普':>7s} {'交易':>7s}")
    for s in STRATS:
        r = results[s]
        print(f"{s:10s} {r.get('avg_return',0):8.2f} {r.get('avg_win',0):8.1f} "
              f"{r.get('avg_dd',0):8.2f} {r.get('avg_sharpe',0):7.2f} {r.get('total_trades',0):7d}")
    rb = baseline
    print(f"{'BASELINE':10s} {rb.get('avg_return',0):8.2f} {rb.get('avg_win',0):8.1f} "
          f"{rb.get('avg_dd',0):8.2f} {rb.get('avg_sharpe',0):7.2f} {rb.get('total_trades',0):7d}")

    # 简单权重建议：按 avg_return 排名分配（收益高者权重高），cctv 若为负则进一步压低
    ranked = sorted(STRATS, key=lambda s: results[s].get("avg_return", -999), reverse=True)
    print("\n按 10 日收益排序:", " > ".join(ranked))
    return 0


if __name__ == "__main__":
    sys.exit(main())
