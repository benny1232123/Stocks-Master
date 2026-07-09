"""auto-boll 多因子选股策略模块（smcore 多策略体系之一），由 Frequently-Used-Program/Stock-Selection-Boll.py 重构而来。

完整保留实战验证过的筛选链路：
  1) 资金流向（3/5/10 日主力净流入，且现价在 [5,30] 区间）
  2) 基本面（资产负债率 < 70% ∩ 净利润 > 0 ∩ 经营性现金流 > 0，可选盈利预测）
  3) 剔除创业板(30x)/科创板(688x)
  4) 流通股东含重要股东（香港中央结算 / 汇金 / 社保 等）→ 持股稳定
  5) 布林带技术面：收盘价 < 下轨（超卖）或 <= 下轨×1.015（近下轨）触发，
     连续超卖/连续近下轨本日不重复触发。

输出：
  - stock_data/Stock-Selection-Boll-{today}.csv      (股票代码, 股票名称, 建议买入价)
  - stock_data/Stock-Selection-Shared-Seed-{today}.csv  (共享候选池，供 Relativity 复用)
  - 返回选中的 DataFrame

布林带计算统一委托 smcore.indicators.boll.calc_bollinger（全项目唯一实现）。
K 线优先用 baostock 前复权（与原始脚本一致）；baostock 不可达时（如海外/云端）
自动降级到 akshare 前复权，保证云端也能产出信号。
"""
from __future__ import annotations

import os
import re
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import baostock as bs
import pandas as pd

try:
    import akshare as ak
except Exception:  # pragma: no cover - akshare 为运行期依赖
    ak = None

from smcore.config.defaults import (
    DEFAULT_ADJUST,
    DEFAULT_DEBT_ASSET_RATIO_LIMIT,
    DEFAULT_K,
    DEFAULT_NEAR_RATIO,
    DEFAULT_PRICE_LOWER_LIMIT,
    DEFAULT_PRICE_UPPER_LIMIT,
    STOCK_DATA_DIR,
)
from smcore.data import fetch_daily_k
from smcore.indicators.boll import calc_bollinger
from smcore.utils.code import format_stock_code

# ── 默认参数（与原始脚本一致）──
PRICE_UPPER_LIMIT = DEFAULT_PRICE_UPPER_LIMIT
PRICE_LOWER_LIMIT = DEFAULT_PRICE_LOWER_LIMIT
DEBT_ASSET_RATIO_LIMIT = DEFAULT_DEBT_ASSET_RATIO_LIMIT
BOLL_STD_MULTIPLIER = DEFAULT_K
BOLL_NEAR_LOWER_MARGIN = DEFAULT_NEAR_RATIO
PLOT_ONLY_SELECTED = True
PLOT_MAX_COUNT = 50
PLOT_SAVE_DIR = STOCK_DATA_DIR / "plots"

IMPORTANT_SHAREHOLDERS = (
    "香港中央结算有限公司",
    "中央汇金资产管理有限公司",
    "中央汇金投资有限责任公司",
    "香港中央结算（代理人）有限公司",
    "中国证券金融股份有限公司",
)
IMPORTANT_SHAREHOLDER_TYPES = ("社保基金",)


# ── 工具函数 ──
def convert_fund_flow(value):
    """将资金流字符串（如 '1.2亿'）转换为浮点数。"""
    if isinstance(value, str):
        if "亿" in value:
            return float(value.replace("亿", "")) * 1e8
        elif "万" in value:
            return float(value.replace("万", "")) * 1e4
        elif value == "-":
            return 0.0
        return float(value)
    return value


def add_market_prefix(code: str) -> str:
    """小写市场前缀（sh/sz），用于 akshare 流通股东接口。"""
    code = format_stock_code(code)
    return f"sh{code}" if code.startswith("6") else f"sz{code}"


def add_market_prefix_dotted(code: str) -> str:
    """带点的小写市场前缀（sh./sz.），用于 baostock K 线接口。"""
    code = format_stock_code(code)
    return f"sh.{code}" if code.startswith("6") else f"sz.{code}"


def safe_filename_component(s: str, max_len: int = 30) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r'[\\/:*?"<>|]', "_", s)
    s = re.sub(r"\s+", "", s)
    return s[:max_len]


def _count_trailing_true(mask_series) -> int:
    flags = pd.Series(mask_series).fillna(False).astype(bool).tolist()
    count = 0
    for item in reversed(flags):
        if not item:
            break
        count += 1
    return count


def _sanitize_table_name(name: str) -> str:
    name = str(name)
    name = name.replace("stock_data/", "").replace(".csv", "").replace("-", "_")
    name = re.sub(r"[^0-9a-zA-Z_]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    if not name:
        name = "table"
    if name[0].isdigit():
        name = f"t_{name}"
    return name


def fetch_data_with_fallback(api_func, table_name: str, *args, **kwargs):
    """通用数据获取：优先 API，失败/空时回退本地 SQLite（stock_data/stocks_data.db）。"""
    db_path = str(STOCK_DATA_DIR / "stocks_data.db")
    conn = sqlite3.connect(db_path)
    try:
        try:
            df_api = api_func(*args, **kwargs)
            if isinstance(df_api, pd.DataFrame) and not df_api.empty:
                df_api.to_sql(table_name, conn, if_exists="replace", index=False)
                return df_api
        except Exception:
            pass
        try:
            df_local = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
            if not df_local.empty:
                return df_local
        except Exception:
            pass
        return pd.DataFrame()
    finally:
        conn.close()


def _compute_report_dates():
    """根据当前月份确定最近的财报日期（与原始脚本一致）。"""
    current_month = datetime.now().month
    current_year = datetime.now().year
    last_year = current_year - 1
    if current_month < 5:
        report_date_profit = f"{last_year}1231"
        report_date_holder = f"{last_year}1231"
    elif current_month < 9:
        report_date_profit = f"{current_year}0331"
        report_date_holder = f"{current_year}0331"
    elif current_month < 11:
        report_date_profit = f"{current_year}0630"
        report_date_holder = f"{current_year}0630"
    else:
        report_date_profit = f"{current_year}0930"
        report_date_holder = f"{current_year}0930"

    if current_month < 5:
        zcfz1, zcfz2 = f"{last_year}1231", f"{last_year}0630"
    elif current_month < 9:
        zcfz1, zcfz2 = f"{current_year}0331", f"{last_year}1231"
    elif current_month < 11:
        zcfz1, zcfz2 = f"{current_year}0630", f"{current_year}0331"
    else:
        zcfz1, zcfz2 = f"{current_year}0930", f"{current_year}0630"
    return {
        "report_date_profit": report_date_profit,
        "report_date_holder": report_date_holder,
        "zcfz_dates": [zcfz1, zcfz2],
        "current_year": current_year,
    }


def _fetch_kline_baostock(fncode_dotted: str, start_date: str, end_date: str) -> pd.DataFrame:
    """用 baostock 拉前复权日线，返回含 close 的 DataFrame。失败返回空。"""
    rs = bs.query_history_k_data_plus(
        fncode_dotted,
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="2",  # 前复权
    )
    if rs is None or rs.error_code != "0":
        return pd.DataFrame()
    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    if not data_list or not rs.fields:
        return pd.DataFrame()
    df = pd.DataFrame(data_list, columns=rs.fields)
    required = ["date", "code", "open", "high", "low", "close", "preclose"]
    if not set(required).issubset(df.columns):
        return pd.DataFrame()
    df = df[required]
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"]).reset_index(drop=True)
    return df


def _fetch_kline_akshare(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """用 akshare 拉前复权日线（baostock 兜底）。返回含 close 的 DataFrame。"""
    try:
        df = fetch_daily_k(code, start_date, end_date, adjust=DEFAULT_ADJUST)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty or "close" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"]).reset_index(drop=True)
    return df


def _plot_bollinger(result_df, fncode, k, today, save_dir, show, stock_name):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    needed = {"date", "close", "MA20", "Upper", "Lower"}
    if result_df.empty or not needed.issubset(result_df.columns):
        return
    os.makedirs(save_dir, exist_ok=True)
    dfp = result_df.copy()
    try:
        dfp["date"] = pd.to_datetime(dfp["date"])
    except Exception:
        pass
    name_part = safe_filename_component(stock_name)
    display_name = stock_name.strip() if isinstance(stock_name, str) else ""
    title_name = f"{fncode} {display_name}" if display_name else fncode
    plt.figure(figsize=(10, 5))
    plt.plot(dfp["date"], dfp["close"], label="Close", linewidth=1.4)
    plt.plot(dfp["date"], dfp["MA20"], label="MA20", linewidth=1.1)
    plt.plot(dfp["date"], dfp["Upper"], label=f"Upper({k})", linestyle="--", linewidth=1.0)
    plt.plot(dfp["date"], dfp["Lower"], label=f"Lower({k})", linestyle="--", linewidth=1.0)
    plt.title(f"{title_name} Bollinger Bands")
    plt.legend(loc="best")
    plt.grid(alpha=0.25)
    plt.xticks(rotation=30)
    out_name = f"{fncode}_{name_part}_BOLL_{today}.png" if name_part else f"{fncode}_BOLL_{today}.png"
    plt.tight_layout()
    plt.savefig(os.path.join(save_dir, out_name), dpi=160)
    if show:
        plt.show()
    plt.close()


def run_boll(
    today: Optional[str] = None,
    *,
    enable_visualization: Optional[bool] = None,
    k: float = BOLL_STD_MULTIPLIER,
    near_ratio: float = BOLL_NEAR_LOWER_MARGIN,
    price_upper: float = PRICE_UPPER_LIMIT,
    price_lower: float = PRICE_LOWER_LIMIT,
    debt_limit: float = DEBT_ASSET_RATIO_LIMIT,
    exclude_gem_sci: bool = True,
    days_back: int = 60,
) -> pd.DataFrame:
    """运行 auto-boll 多因子选股，写出 CSV 并返回选中 DataFrame。

    Args:
        today: 日期字符串 YYYYMMDD，默认今天。
        enable_visualization: 是否画布林带图；None 时读环境变量 ENABLE_VISUALIZATION（默认关）。
        k / near_ratio: 布林带标准差倍数 / 近下轨容差。
        price_upper / price_lower: 现价上下限（原始 5~30）。
        debt_limit: 资产负债率上限。
        exclude_gem_sci: 是否剔除创业板(30x)/科创板(688x)。
        days_back: 布林 K 线回看天数。
    """
    if ak is None:
        raise RuntimeError("akshare 未安装，无法进行多因子选股")

    today = today or date.today().strftime("%Y%m%d")
    if enable_visualization is None:
        enable_visualization = os.getenv("ENABLE_VISUALIZATION", "0") != "0"

    dates = _compute_report_dates()
    report_date_profit = dates["report_date_profit"]
    report_date_holder = dates["report_date_holder"]
    zcfz_dates = dates["zcfz_dates"]
    current_year = dates["current_year"]
    do_plot = bool(enable_visualization)

    print(f"[boll] 开始 auto-boll 多因子选股 {today}")

    # ── 1) 资金流向 ──
    all_fund_flow_codes: dict[str, list] = {}
    for period in ["3日排行", "5日排行", "10日排行"]:
        period_name = period.split("日")[0]
        df = fetch_data_with_fallback(
            ak.stock_fund_flow_individual,
            f"ak_fund_flow_{period_name}d",
            symbol=period,
        )
        if not df.empty and "最新价" in df.columns and "股票代码" in df.columns:
            df["资金流入净额"] = df["资金流入净额"].apply(convert_fund_flow)
            positive = df[
                (df["资金流入净额"] > 0)
                & (df["最新价"] < price_upper)
                & (df["最新价"] >= price_lower)
            ]
            codes = positive["股票代码"].apply(format_stock_code).tolist()
            all_fund_flow_codes[f"format_{period_name}_days_positive_funds_codes"] = codes
        else:
            all_fund_flow_codes[f"format_{period_name}_days_positive_funds_codes"] = []
    f3 = set(all_fund_flow_codes.get("format_3_days_positive_funds_codes", []))
    f5 = set(all_fund_flow_codes.get("format_5_days_positive_funds_codes", []))
    f10 = set(all_fund_flow_codes.get("format_10_days_positive_funds_codes", []))
    fund_flow_union = f3 | f5 | f10
    print(f"[boll] 资金流向候选(3/5/10日净流入并集): {len(fund_flow_union)}")

    # ── 2) 基本面 ──
    zcfz_codes: list = []
    for d in zcfz_dates:
        s = fetch_data_with_fallback(ak.stock_zcfz_em, f"ak_zcfz_em_{d}", date=d)
        if not s.empty and "资产负债率" in s.columns and "股票代码" in s.columns:
            good = s[s["资产负债率"] < debt_limit]
            zcfz_codes.extend(good["股票代码"].tolist())
    zcfz_codes = list(set(zcfz_codes))

    profit_codes: list = []
    profit_df = fetch_data_with_fallback(ak.stock_lrb_em, f"ak_lrb_em_{report_date_profit}", date=report_date_profit)
    if not profit_df.empty and "净利润" in profit_df.columns and "股票代码" in profit_df.columns:
        profit_codes = profit_df[profit_df["净利润"] > 0]["股票代码"].tolist()

    cashflow_codes: list = []
    cash_df = fetch_data_with_fallback(ak.stock_xjll_em, f"ak_xjll_em_{report_date_profit}", date=report_date_profit)
    if not cash_df.empty and "经营性现金流-现金流量净额" in cash_df.columns and "股票代码" in cash_df.columns:
        cashflow_codes = cash_df[cash_df["经营性现金流-现金流量净额"] > 0]["股票代码"].tolist()

    profit_forecast_codes: list = []
    use_profit_forecast_filter = False
    pf_df = fetch_data_with_fallback(ak.stock_profit_forecast_em, "ak_profit_forecast_em")
    if not pf_df.empty:
        forecast_col = f"{current_year}预测每股收益"
        if forecast_col in pf_df.columns and "代码" in pf_df.columns:
            series = pd.to_numeric(pf_df[forecast_col], errors="coerce")
            profit_forecast_codes = pf_df[series > 0]["代码"].tolist()
            use_profit_forecast_filter = True

    fundamental_sets = [set(zcfz_codes), set(profit_codes), set(cashflow_codes)]
    if use_profit_forecast_filter:
        fundamental_sets.append(set(profit_forecast_codes))
    fundamental_intersection = set.intersection(*fundamental_sets) if fundamental_sets else set()
    print(f"[boll] 基本面交集: {len(fundamental_intersection)}")

    common_codes = fundamental_intersection & fund_flow_union
    print(f"[boll] 基本面∩资金流: {len(common_codes)}")

    if exclude_gem_sci:
        common_codes = {c for c in common_codes if not (str(c).startswith("30") or str(c).startswith("688"))}
    print(f"[boll] 剔除创业板/科创板后: {len(common_codes)}")

    # ── 3) 流通股东（重要股东）──
    code_name_map: dict[str, str] = {}
    try:
        name_df = fetch_data_with_fallback(ak.stock_info_a_code_name, "ak_stock_info_a_code_name")
        if not name_df.empty and {"code", "name"}.issubset(name_df.columns):
            tmp = name_df.copy()
            tmp["code"] = tmp["code"].apply(format_stock_code)
            code_name_map = dict(zip(tmp["code"], tmp["name"]))
    except Exception as e:
        print(f"[boll] 获取股票名称映射失败: {e}")

    final_candidate_codes: list = []
    if common_codes:
        for code in common_codes:
            try:
                new_code = add_market_prefix(code)
                sh_df = ak.stock_gdfx_free_top_10_em(symbol=new_code, date=report_date_holder)
                has_important = False
                if not sh_df.empty:
                    top5_names = sh_df["股东名称"].head(5).tolist()
                    top5_types = sh_df["股东性质"].head(5).tolist()
                    if any(any(imp in n for n in top5_names) for imp in IMPORTANT_SHAREHOLDERS):
                        has_important = True
                    if not has_important and any(
                        any(t in str(tt) for tt in top5_types) for t in IMPORTANT_SHAREHOLDER_TYPES
                    ):
                        has_important = True
                if has_important:
                    final_candidate_codes.append(code)
            except Exception as e:
                print(f"[boll] 获取 {code} 流通股东数据出错: {e}，跳过")
    print(f"[boll] 重要股东过滤后候选: {len(final_candidate_codes)}")

    if not final_candidate_codes:
        print("[boll] 没有符合所有条件的股票")
        return pd.DataFrame(columns=["股票代码", "股票名称", "建议买入价"])

    # 写出共享候选池（供 Relativity 复用）
    shared_seed_df = pd.DataFrame({
        "股票代码": [format_stock_code(c) for c in final_candidate_codes],
        "股票名称": [code_name_map.get(format_stock_code(c), "") for c in final_candidate_codes],
    })
    shared_path = STOCK_DATA_DIR / f"Stock-Selection-Shared-Seed-{today}.csv"
    shared_seed_df.to_csv(shared_path, index=False, encoding="utf-8-sig")

    # ── 4) 布林带技术面 ──
    end_date = date.today()
    start_date = (end_date - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end_date_text = end_date.strftime("%Y-%m-%d")

    bs_login_ok = False
    try:
        lg = bs.login()
        bs_login_ok = lg.error_code == "0"
    except Exception:
        bs_login_ok = False

    boll_selected_codes: list = []
    boll_selected_buy: dict = {}
    plot_saved_count = 0

    for fncode in final_candidate_codes:
        stock_name = code_name_map.get(format_stock_code(fncode), "")
        kdf = pd.DataFrame()
        if bs_login_ok:
            try:
                kdf = _fetch_kline_baostock(add_market_prefix_dotted(fncode), start_date, end_date_text)
            except Exception:
                kdf = pd.DataFrame()
        if kdf.empty:
            kdf = _fetch_kline_akshare(format_stock_code(fncode), start_date, end_date_text)
        if kdf.empty or len(kdf) < 20:
            continue

        boll_df = calc_bollinger(kdf, window=20, k=k)
        if boll_df.empty:
            continue
        latest = boll_df.iloc[-1]
        if pd.isna(latest.get("Lower")) or pd.isna(latest.get("Upper")):
            continue

        close_series = pd.to_numeric(boll_df["close"], errors="coerce")
        lower_series = pd.to_numeric(boll_df["Lower"], errors="coerce")
        oversold_mask = (close_series < lower_series)
        selected_zone_mask = (close_series <= lower_series * near_ratio)
        near_lower_only_mask = selected_zone_mask & ~oversold_mask
        oversold_streak = _count_trailing_true(oversold_mask)
        near_lower_streak = _count_trailing_true(near_lower_only_mask)

        selected = False
        if float(latest["close"]) < float(latest["Lower"]):
            if oversold_streak > 1:
                print(f"{fncode} {stock_name} 连续{oversold_streak}日低于布林带下轨，本日不重复触发")
            else:
                print(f"{fncode} {stock_name} 价格低于布林带下轨 (95%概率)，超卖")
                suggested_buy = round(min(float(latest["close"]), float(latest["Lower"])), 2)
                boll_selected_codes.append(fncode)
                boll_selected_buy[format_stock_code(fncode)] = suggested_buy
                selected = True
        elif float(latest["close"]) <= float(latest["Lower"]) * near_ratio:
            if near_lower_streak > 1:
                print(f"{fncode} {stock_name} 连续{near_lower_streak}日处于下轨附近，本日不重复触发")
            else:
                print(f"{fncode} {stock_name} 价格接近布林带下轨 (95%概率)，关注")
                suggested_buy = round(min(float(latest["close"]), float(latest["Lower"])), 2)
                boll_selected_codes.append(fncode)
                boll_selected_buy[format_stock_code(fncode)] = suggested_buy
                selected = True

        if do_plot and plot_saved_count < PLOT_MAX_COUNT and (not PLOT_ONLY_SELECTED or selected):
            _plot_bollinger(
                result_df=boll_df, fncode=fncode, k=k, today=today,
                save_dir=str(PLOT_SAVE_DIR), show=False, stock_name=stock_name,
            )
            plot_saved_count += 1

    if bs_login_ok:
        try:
            bs.logout()
        except Exception:
            pass

    if boll_selected_codes:
        out_df = pd.DataFrame({
            "股票代码": [format_stock_code(c) for c in boll_selected_codes],
            "股票名称": [code_name_map.get(format_stock_code(c), "") for c in boll_selected_codes],
            "建议买入价": [boll_selected_buy.get(format_stock_code(c), "") for c in boll_selected_codes],
        })
        out_path = STOCK_DATA_DIR / f"Stock-Selection-Boll-{today}.csv"
        out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"[boll] Stock-Selection-Boll-{today}.csv 已保存，{len(out_df)} 只")
        return out_df
    else:
        print("[boll] 没有选出符合布林带策略的股票")
        return pd.DataFrame(columns=["股票代码", "股票名称", "建议买入价"])


if __name__ == "__main__":
    run_boll()
