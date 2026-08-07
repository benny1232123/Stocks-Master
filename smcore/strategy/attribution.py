"""绩效归因（P1-2）：Brinson 模型把组合收益相对基准分解为
配置效应(asset allocation) / 选股效应(stock selection) / 交互效应(interaction)。

设计原则（与项目一致）：
- 纯函数、fail-soft：输入缺失(缺收益/缺基准)时回退中性，绝不抛错。
- 全配置驱动：归因窗口 horizon、基准类型由调用方/配置决定，无魔法数硬编码。
- 基准两种：
  * ``equal``（默认）：等权持有「相同入选名单」。此时 selection≡0，allocation 捕获
    组合优化层(score/vol 倾斜)相对等权的增益 —— 直接回答「优化层有没有用」。
  * ``market``：相对沪深300。此时 allocation 在等权代理下≈0，selection 捕获
    「选出的票相对大盘的超额」，interaction 捕获权重倾斜与个股超额的叠加。
  两种基准互补：equal 看优化层贡献，market 看选股贡献。

BHB(Brinson-Hood-Beebower)公式（逐资产 i）：
  allocation_i = (wp_i - wb_i) * rb_i
  selection_i  = wb_i * (rp_i - rb_i)
  interaction_i= (wp_i - wb_i) * (rp_i - rb_i)
  total_active = Σ wp_i·rp_i - Σ wb_i·rb_i = allocation + selection + interaction
"""
from __future__ import annotations

from typing import Optional

import pandas as pd

from smcore.config.defaults import STOCK_DATA_DIR
from smcore.utils.code import format_stock_code


def brinson_bhb(
    port_w: dict,
    port_ret: dict,
    bench_w: dict,
    bench_ret: dict,
) -> dict:
    """标准 BHB Brinson 归因（逐资产）。

    Args:
        port_w:   {code: 组合权重(小数, 合计可为1或≤1)}
        port_ret: {code: 个股区间收益(小数, 可含 None)}
        bench_w:  {code: 基准权重(小数)}
        bench_ret:{code: 基准个股收益(小数, 可含 None)}

    Returns:
        {
          "total": {"port_return","bench_return","active","allocation","selection","interaction"},
          "by_code": {code: {wp,wb,rp,rb,allocation,selection,interaction}},
        }
        缺失收益的股票跳过(不计入)，保证 fail-soft。
    """
    codes = set(port_w) | set(bench_w)
    by_code = {}
    alloc = sel = inter = 0.0
    rp_sum = rb_sum = 0.0
    for c in codes:
        wp = float(port_w.get(c, 0.0) or 0.0)
        wb = float(bench_w.get(c, 0.0) or 0.0)
        rp = port_ret.get(c)
        rb = bench_ret.get(c)
        if rp is None or rb is None:
            # 任一侧收益缺失 → 跳过该资产，避免污染归因（fail-soft）
            continue
        rp = float(rp)
        rb = float(rb)
        a = (wp - wb) * rb
        s = wb * (rp - rb)
        it = (wp - wb) * (rp - rb)
        alloc += a
        sel += s
        inter += it
        rp_sum += wp * rp
        rb_sum += wb * rb
        by_code[c] = {
            "wp": wp, "wb": wb, "rp": rp, "rb": rb,
            "allocation": a, "selection": s, "interaction": it,
        }
    return {
        "total": {
            "port_return": rp_sum,
            "bench_return": rb_sum,
            "active": rp_sum - rb_sum,
            "allocation": alloc,
            "selection": sel,
            "interaction": inter,
        },
        "by_code": by_code,
    }


def forward_returns(codes, as_of_yyyymmdd: str, horizon: int = 10) -> dict:
    """从本地 k_data 计算信号日后 horizon 个交易日的前向收益(小数)。

    零联网；缺文件/缺数据返回 None（由调用方按中性处理）。
    """
    out: dict[str, Optional[float]] = {}
    for c in codes:
        c6 = format_stock_code(c)
        if not c6:
            out[c6] = None
            continue
        p = STOCK_DATA_DIR / "k_data" / f"{c6}_qfq_full.csv"
        if not p.exists():
            out[c6] = None
            continue
        try:
            d = pd.read_csv(p)
            if "date" not in d.columns or "close" not in d.columns:
                out[c6] = None
                continue
            d["date"] = pd.to_datetime(d["date"], errors="coerce")
            d = d.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
            target = pd.Timestamp(as_of_yyyymmdd)
            idx = d.index[d["date"] >= target]
            if len(idx) == 0:
                out[c6] = None
                continue
            i0 = int(idx[0])
            i1 = i0 + horizon
            if i1 >= len(d):
                out[c6] = None
                continue
            c0 = pd.to_numeric(d.loc[i0, "close"], errors="coerce")
            c1 = pd.to_numeric(d.loc[i1, "close"], errors="coerce")
            if not (c0 and c1 and c0 > 0):
                out[c6] = None
                continue
            out[c6] = float(c1 / c0 - 1.0)
        except Exception:
            out[c6] = None
    return out


def _market_return(as_of_yyyymmdd: str, horizon: int = 10) -> Optional[float]:
    """沪深300 同期前向收益(小数)，供 market 基准的 rb_i 使用。"""
    try:
        from .regime_filter import _get_hs300_close

        idx = _get_hs300_close()
        if idx is None:
            return None
        target = pd.Timestamp(as_of_yyyymmdd)
        idx_prior = idx.loc[:target]
        if len(idx_prior) < 2:
            return None
        i0 = int(idx_prior.index[-1])
        i1 = i0 + horizon
        if i1 >= len(idx):
            return None
        c0 = float(idx.iloc[i0])
        c1 = float(idx.iloc[i1])
        if not (c0 and c1 and c0 > 0):
            return None
        return c1 / c0 - 1.0
    except Exception:
        return None


def run_attribution(signal_date: str, horizon: int = 10, benchmark: str = "equal") -> Optional[dict]:
    """对某个信号日的 Daily-Action-List 做 Brinson 归因。

    Args:
        signal_date: YYYYMMDD，对应 stock_data/Daily-Action-List-{date}.csv
        horizon: 前向收益窗口(交易日)
        benchmark: ``equal``(等权同名单) | ``market``(沪深300)

    Returns:
        brinson_bhb 的结果字典；缺文件/空清单返回 None。
    """
    path = STOCK_DATA_DIR / f"Daily-Action-List-{signal_date}.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path)
    if df.empty or "股票代码" not in df.columns or "建议仓位%" not in df.columns:
        return None
    codes = df["股票代码"].tolist()
    raw_w = {format_stock_code(c): float(p) / 100.0 for c, p in zip(codes, df["建议仓位%"])}
    tot = sum(raw_w.values())
    if tot <= 0:
        return None
    port_w = {k: v / tot for k, v in raw_w.items()}  # 归一化到已投资本(排除现金)
    ret = forward_returns(codes, signal_date, horizon)
    port_ret = {k: ret.get(k) for k in port_w}

    if benchmark == "market":
        r_m = _market_return(signal_date, horizon)
        if r_m is None:
            return None
        # 等权代理：基准权重 1/N，基准个股收益=市场收益（同一 rb_i）
        n = len(port_w)
        bench_w = {k: 1.0 / n for k in port_w}
        bench_ret = {k: r_m for k in port_w}
    else:  # equal：等权持有相同名单
        n = len(port_w)
        bench_w = {k: 1.0 / n for k in port_w}
        bench_ret = port_ret  # 相同个股收益 → selection≡0，allocation 捕获优化层倾斜

    return brinson_bhb(port_w, port_ret, bench_w, bench_ret)


def format_attribution(result: dict, signal_date: str, horizon: int, benchmark: str) -> str:
    """把归因结果格式化为可读报告（百分比展示）。"""
    t = result["total"]
    lines = [
        f"# 绩效归因 Brinson（信号日 {signal_date}，窗口 {horizon} 日，基准={benchmark}）",
        "",
        f"- 组合收益：{t['port_return']*100:+.2f}%",
        f"- 基准收益：{t['bench_return']*100:+.2f}%",
        f"- 主动收益：{t['active']*100:+.2f}%",
        f"  - 配置效应(allocation)：{t['allocation']*100:+.2f}%",
        f"  - 选股效应(selection)： {t['selection']*100:+.2f}%",
        f"  - 交互效应(interaction)：{t['interaction']*100:+.2f}%",
        "",
        "## 逐股贡献（按 |主动| 降序）",
        "",
        "| 代码 | wp% | wb% | rp% | rb% | 配置% | 选股% | 交互% |",
        "|------|-----|-----|-----|-----|-------|-------|-------|",
    ]
    rows = sorted(
        result["by_code"].items(),
        key=lambda kv: abs(kv[1]["allocation"] + kv[1]["selection"] + kv[1]["interaction"]),
        reverse=True,
    )
    for c, d in rows:
        lines.append(
            f"| {c} | {d['wp']*100:.1f} | {d['wb']*100:.1f} | {d['rp']*100:+.1f} | "
            f"{d['rb']*100:+.1f} | {d['allocation']*100:+.2f} | {d['selection']*100:+.2f} | "
            f"{d['interaction']*100:+.2f} |"
        )
    return "\n".join(lines)
