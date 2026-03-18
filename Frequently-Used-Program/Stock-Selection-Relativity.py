import akshare as ak
import pandas as pd
import numpy as np
import time
import sqlite3
from datetime import datetime, timedelta
import baostock as bs


'''0.准备工作'''
# --- 配置区 ---
PRICE_UPPER_LIMIT = 35          # 股价上限
DEBT_ASSET_RATIO_LIMIT = 70     # 资产负债率上限
CURRENT_YEAR = datetime.now().year
LAST_YEAR = CURRENT_YEAR - 1

# === 相对强弱策略参数（核心）===
ENABLE_RELATIVE_STRENGTH = True

# 指数代码（baostock）：上证 sh.000001；沪深300 sh.000300；中证500 sh.000905
RS_INDEX_CODE = "sh.000001"
RS_LOOKBACK_DAYS = 100          # 回看天数（越大越稳但更慢）
RS_MIN_OVERLAP_DAYS = 30        # 对齐交易日不足则不计算该股

# 大盘涨（iret>0）：个股不跌/少跌 —— stock_ret >= RS_UP_TOL
# 注意：这里比较的是“日收益率”（pct_change），单位是小数：0.01=1%，-0.002=-0.2%
RS_UP_TOL = -0.025                 # 0=不跌；可放宽为 -0.002（允许跌0.2%）

# 大盘跌（iret<0）：个股跌得少于大盘 —— stock_ret - index_ret >= RS_DOWN_OUTPERF
RS_DOWN_OUTPERF = 0.0           # 0=不比大盘更差；可改 0.002 要求至少强0.2%

# “大部分时间满足”比例阈值（建议 0.6~0.75）
RS_MIN_UP_RATIO = 0.6
RS_MIN_DOWN_RATIO = 0.6

# 样本期内上涨/下跌天数太少时避免误判
RS_MIN_UP_DAYS = 5
RS_MIN_DOWN_DAYS = 5
# --- 配置区结束 ---

# 参数自检：避免把“百分比”误写成“小数”导致条件永远不满足
if abs(RS_UP_TOL) >= 0.2:
    print(f"[参数警告] RS_UP_TOL={RS_UP_TOL} 看起来过大（日收益率小数制：0.01=1%）。这会导致上涨满足率几乎恒为0。")
if abs(RS_DOWN_OUTPERF) >= 0.2:
    print(f"[参数警告] RS_DOWN_OUTPERF={RS_DOWN_OUTPERF} 看起来过大（日收益率小数制：0.01=1%）。")


today = time.strftime("%Y%m%d", time.localtime())

# pandas显示设置
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def format_stock_code(code):
    """将股票代码格式化为6位数"""
    if isinstance(code, str):
        code = ''.join(filter(str.isdigit, code))
        return code.zfill(6)
    elif isinstance(code, int):
        return f"{code:06d}"
    return code


def add_market_prefix(code):
    """为股票代码添加小写市场前缀（用于 akshare 股东接口：sh600000/sz000001）"""
    formatted_code = format_stock_code(code)
    return f"sh{formatted_code}" if formatted_code.startswith('6') else f"sz{formatted_code}"


def add_market_prefix_dotted(code):
    """为股票代码添加小写市场前缀（用于 baostock：sh.600000/sz.000001）"""
    formatted_code = format_stock_code(code)
    return f"sh.{formatted_code}" if formatted_code.startswith('6') else f"sz.{formatted_code}"


def convert_fund_flow(value):
    """将资金流字符串（如'1.2亿'）转换为浮点数"""
    if isinstance(value, str):
        if '亿' in value:
            return float(value.replace('亿', '')) * 1e8
        elif '万' in value:
            return float(value.replace('万', '')) * 1e4
        elif value == '-':
            return 0.0
        return float(value)
    return value


def fetch_data_with_fallback(api_func, file_path, *args, **kwargs):
    """
    通用数据获取函数，优先从API获取并存入本地数据库，失败则从数据库读取。
    数据库文件: stock_data/stocks_data.db
    """
    db_path = "stock_data/stocks_data.db"
    table_name = file_path.replace("stock_data/", "").replace(".csv", "").replace("-", "_")

    conn = sqlite3.connect(db_path)
    try:
        df = api_func(*args, **kwargs)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"API调用成功。数据已保存至数据库表: {table_name}")
        conn.close()
        return df
    except Exception as e:
        print(f"API调用失败或超时: {e}。正在尝试读取本地数据库缓存...")
        try:
            df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
            conn.close()
            print(f"成功读取本地数据库表: {table_name}")
            return df
        except Exception as e2:
            conn.close()
            print(f"读取本地数据库失败 {table_name}: {e2}")
            return pd.DataFrame()


# 根据当前月份确定最近的财报日期
current_month = datetime.now().month
if current_month < 5:
    REPORT_DATE_PROFIT = f"{LAST_YEAR}0930"
    REPORT_DATE_HOLDER = f"{LAST_YEAR}0930"
elif current_month < 9:
    REPORT_DATE_PROFIT = f"{CURRENT_YEAR}0331"
    REPORT_DATE_HOLDER = f"{CURRENT_YEAR}0331"
elif current_month < 11:
    REPORT_DATE_PROFIT = f"{CURRENT_YEAR}0630"
    REPORT_DATE_HOLDER = f"{CURRENT_YEAR}0630"
else:
    REPORT_DATE_PROFIT = f"{CURRENT_YEAR}0930"
    REPORT_DATE_HOLDER = f"{CURRENT_YEAR}0930"

if current_month < 5:
    ZCFZ_DATE1 = f"{LAST_YEAR}0930"
    ZCFZ_DATE2 = f"{LAST_YEAR}0630"
elif current_month < 9:
    ZCFZ_DATE1 = f"{CURRENT_YEAR}0331"
    ZCFZ_DATE2 = f"{LAST_YEAR}1231"
elif current_month < 11:
    ZCFZ_DATE1 = f"{CURRENT_YEAR}0630"
    ZCFZ_DATE2 = f"{CURRENT_YEAR}0331"
else:
    ZCFZ_DATE1 = f"{CURRENT_YEAR}0930"
    ZCFZ_DATE2 = f"{CURRENT_YEAR}0630"

ZCFZ_DATES = [ZCFZ_DATE1, ZCFZ_DATE2]

IMPORTANT_SHAREHOLDERS = [
    "香港中央结算有限公司", "中央汇金资产管理有限公司", "中央汇金投资有限责任公司",
    "香港中央结算（代理人）有限公司", "中国证券金融股份有限公司"
]
IMPORTANT_SHAREHOLDER_TYPES = ["社保基金"]


'''1.技术面选股'''
# 此脚本仅做相对强弱策略，技术面（均线/形态等）可后续补充


'''2.资金流向选股'''
all_fund_flow_codes = {}
for period in ["3日排行", "5日排行", "10日排行"]:
    period_name = period.split('日')[0]
    df = fetch_data_with_fallback(
        ak.stock_fund_flow_individual,
        f"stock_data/{period_name}-days-positive-funds.csv",
        symbol=period
    )
    if not df.empty:
        df['资金流入净额'] = df['资金流入净额'].apply(convert_fund_flow)
        positive_df = df[(df['资金流入净额'] > 0) & (df['最新价'] < PRICE_UPPER_LIMIT)]
        codes = positive_df['股票代码'].apply(format_stock_code).tolist()
        all_fund_flow_codes[f'format_{period_name}_days_positive_funds_codes'] = codes
    else:
        all_fund_flow_codes[f'format_{period_name}_days_positive_funds_codes'] = []
    time.sleep(3)

format_three_days_positive_funds_codes = all_fund_flow_codes.get('format_3_days_positive_funds_codes', [])
format_five_days_positive_funds_codes = all_fund_flow_codes.get('format_5_days_positive_funds_codes', [])
format_ten_days_positive_funds_codes = all_fund_flow_codes.get('format_10_days_positive_funds_codes', [])


'''3.基本面选股'''
'''3.1.资产负债率'''
zcfz_codes_list = []
for date_str in ZCFZ_DATES:
    s_zcfz_df = fetch_data_with_fallback(
        ak.stock_zcfz_em,
        f"stock_data/stock_zcfz_em_{date_str}.csv",
        date=date_str
    )
    if not s_zcfz_df.empty:
        s_good_zcfz_df = s_zcfz_df[s_zcfz_df['资产负债率'] < DEBT_ASSET_RATIO_LIMIT]
        zcfz_codes_list.extend(s_good_zcfz_df['股票代码'].tolist())
    time.sleep(3)
zcfz_codes = list(set(zcfz_codes_list))

'''3.2.利润表'''
profit_df = fetch_data_with_fallback(
    ak.stock_lrb_em,
    f"stock_data/stock_lrb_em_{REPORT_DATE_PROFIT}.csv",
    date=REPORT_DATE_PROFIT
)
profit_codes = []
if not profit_df.empty:
    good_profit_df = profit_df[(profit_df['净利润'] > 0)]
    profit_codes = good_profit_df['股票代码'].tolist()
time.sleep(3)

'''3.3.现金流量表'''
cashflow_df = fetch_data_with_fallback(
    ak.stock_xjll_em,
    f"stock_data/stock_xjll_em_{REPORT_DATE_PROFIT}.csv",
    date=REPORT_DATE_PROFIT
)
cashflow_codes = []
if not cashflow_df.empty:
    good_cashflow_df = cashflow_df[cashflow_df['经营性现金流-现金流量净额'] > 0]
    cashflow_codes = good_cashflow_df['股票代码'].tolist()
time.sleep(3)

'''3.4.盈利预测'''
profit_forecast_df = fetch_data_with_fallback(
    ak.stock_profit_forecast_em,
    "stock_data/stock_profit_forecast_em.csv"
)
profit_forecast_codes = []
if not profit_forecast_df.empty:
    forecast_col = f'{CURRENT_YEAR}预测每股收益'
    if forecast_col in profit_forecast_df.columns:
        good_profit_forecast_df = profit_forecast_df[profit_forecast_df[forecast_col] > 0]
        profit_forecast_codes = good_profit_forecast_df['代码'].tolist()
    else:
        print(f"'{forecast_col}' not found in profit forecast data.")
time.sleep(3)


'''4.数据处理'''
print("\n各条件股票数量:")
print(f"  现金流: {len(cashflow_codes)}")
print(f"  利润表: {len(profit_codes)}")
print(f"  资产负债率: {len(zcfz_codes)}")
print(f"  盈利预测: {len(profit_forecast_codes)}")
print(f"  3日资金: {len(format_three_days_positive_funds_codes)}")
print(f"  5日资金: {len(format_five_days_positive_funds_codes)}")
print(f"  10日资金: {len(format_ten_days_positive_funds_codes)}")

fundamental_intersection = set(cashflow_codes) & set(profit_codes) & set(zcfz_codes) & set(profit_forecast_codes)
print(f"基本面条件交集: {len(fundamental_intersection)}")

set_3d = set(format_three_days_positive_funds_codes)
set_5d = set(format_five_days_positive_funds_codes)
set_10d = set(format_ten_days_positive_funds_codes)
fund_flow_union = set_3d | set_5d | set_10d
print(f"资金流向条件(至少满足一个)交集: {len(fund_flow_union)}")

common_codes_set = fundamental_intersection & fund_flow_union
print(f"所有条件交集后: {len(common_codes_set)}")

filtered_codes = [code for code in common_codes_set if not (str(code).startswith('30') or str(code).startswith('688'))]
print(f"排除创业板和科创板后: {len(filtered_codes)}")

candidate_codes = filtered_codes


'''5.流通股东分析（保留 Boll 脚本逻辑）'''
final_candidate_codes = []
if candidate_codes:
    for code in candidate_codes:
        try:
            new_code = add_market_prefix(code)
            share_holders_df = ak.stock_gdfx_free_top_10_em(symbol=new_code, date=REPORT_DATE_HOLDER)

            has_important = False
            if not share_holders_df.empty:
                top5_names = share_holders_df["股东名称"].head(5).tolist()
                top5_types = share_holders_df["股东性质"].head(5).tolist()

                if any(any(imp in name for name in top5_names) for imp in IMPORTANT_SHAREHOLDERS):
                    has_important = True
                if (not has_important) and any(any(imp_type in str(t) for t in top5_types) for imp_type in IMPORTANT_SHAREHOLDER_TYPES):
                    has_important = True

            if has_important:
                print(f"{code}：大股东持股稳定，符合条件")
                final_candidate_codes.append(code)
            else:
                print(f"{code}：无重要股东持股")
        except Exception as e:
            print(f"获取 {code} 流通股东数据时出错: {e}. 默认保留该股票。")
            final_candidate_codes.append(code)
else:
    print("没有候选股票进行流通股东分析")


'''6.相对强弱（核心策略）'''
def fetch_bs_daily_close(code_bs: str, start_date: str, end_date: str) -> pd.DataFrame:
    """拉取 baostock 日线 close，返回 date/close"""
    rs = bs.query_history_k_data_plus(
        code_bs,
        "date,close,tradestatus",
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="2"
    )
    data_list = []
    while (rs.error_code == "0") and rs.next():
        data_list.append(rs.get_row_data())
    if rs.error_code != "0":
        return pd.DataFrame()

    df = pd.DataFrame(data_list, columns=rs.fields)
    if df.empty:
        return df

    if "tradestatus" in df.columns:
        df = df[df["tradestatus"].astype(str) == "1"]

    df = df[["date", "close"]].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "close"]).sort_values("date")
    return df


def _to_daily_ret(df_close: pd.DataFrame, col_name: str) -> pd.DataFrame:
    if df_close is None or df_close.empty:
        return pd.DataFrame()
    t = df_close[["date", "close"]].copy()
    t["ret"] = t["close"].pct_change()
    t = t.dropna(subset=["ret"])
    return t[["date"]].assign(**{col_name: t["ret"].values})


def relative_strength_pass(
    stock_close_df: pd.DataFrame,
    index_close_df: pd.DataFrame,
) -> tuple[bool, dict]:
    s = _to_daily_ret(stock_close_df, "sret")
    i = _to_daily_ret(index_close_df, "iret")
    if s.empty or i.empty:
        return False, {"reason": "empty_ret"}

    m = pd.merge(s, i, on="date", how="inner")
    if len(m) < RS_MIN_OVERLAP_DAYS:
        return False, {"reason": "overlap_too_small", "overlap_days": int(len(m))}

    up_mask = m["iret"] > 0
    down_mask = m["iret"] < 0
    up_days = int(up_mask.sum())
    down_days = int(down_mask.sum())

    # 个股自身上涨/下跌天数（基于对齐交易日的 sret 正负）
    stock_up_days = int((m["sret"] > 0).sum())
    stock_down_days = int((m["sret"] < 0).sum())

    if up_days < RS_MIN_UP_DAYS or down_days < RS_MIN_DOWN_DAYS:
        return False, {
            "reason": "insufficient_up_or_down_days",
            "up_days": up_days,
            "down_days": down_days,
            "stock_up_days": stock_up_days,
            "stock_down_days": stock_down_days,
            "overlap_days": int(len(m)),
        }

    up_ok = int((m.loc[up_mask, "sret"] >= RS_UP_TOL).sum())
    down_ok = int(((m.loc[down_mask, "sret"] - m.loc[down_mask, "iret"]) >= RS_DOWN_OUTPERF).sum())

    up_ratio = up_ok / up_days if up_days else 0.0
    down_ratio = down_ok / down_days if down_days else 0.0

    passed = (up_ratio >= RS_MIN_UP_RATIO) and (down_ratio >= RS_MIN_DOWN_RATIO)
    return passed, {
        "overlap_days": int(len(m)),
        "up_days": up_days,
        "down_days": down_days,
        "stock_up_days": stock_up_days,
        "stock_down_days": stock_down_days,
        "up_ratio": float(up_ratio),
        "down_ratio": float(down_ratio),
    }


# baostock 登录
lg = bs.login()
print('login respond error_code:' + lg.error_code)
print('login respond  error_msg:' + lg.error_msg)

# 拉取一次代码-名称映射
code_name_map: dict[str, str] = {}
try:
    code_name_df = fetch_data_with_fallback(
        ak.stock_info_a_code_name,
        "stock_data/stock_info_a_code_name.csv",
    )
    if not code_name_df.empty and {"code", "name"}.issubset(code_name_df.columns):
        tmp = code_name_df.copy()
        tmp["code"] = tmp["code"].apply(format_stock_code)
        code_name_map = dict(zip(tmp["code"], tmp["name"]))
except Exception as e:
    print(f"获取股票名称映射失败: {e}（将仅输出股票代码）")

end_date = datetime.now().strftime("%Y-%m-%d")
start_date = (datetime.now() - timedelta(days=RS_LOOKBACK_DAYS)).strftime("%Y-%m-%d")

selected = []
if ENABLE_RELATIVE_STRENGTH and final_candidate_codes:
    index_close_df = fetch_bs_daily_close(RS_INDEX_CODE, start_date=start_date, end_date=end_date)
    if index_close_df.empty:
        print(f"[相对强弱] 指数数据为空：{RS_INDEX_CODE}，无法执行相对强弱筛选。")
    else:
        for code in final_candidate_codes:
            stock_name = code_name_map.get(format_stock_code(code), "")
            stock_close_df = fetch_bs_daily_close(add_market_prefix_dotted(code), start_date=start_date, end_date=end_date)

            passed, stats = relative_strength_pass(stock_close_df, index_close_df)
            if passed:
                print(
                    f"[相对强弱] PASS {code} {stock_name} | "
                    f"上涨满足率={stats['up_ratio']:.2f} 抗跌满足率={stats['down_ratio']:.2f} 对齐交易日={stats['overlap_days']}"
                )
                selected.append({
                    "股票代码": format_stock_code(code),
                    "股票名称": stock_name,
                    "上涨满足率": stats["up_ratio"],      # 指数上涨日：个股不跌/少跌 的比例
                    "抗跌满足率": stats["down_ratio"],    # 指数下跌日：个股跌得少于大盘 的比例
                    "对齐交易日": stats["overlap_days"],
                    "指数上涨日数(对齐后)": stats["up_days"],
                    "指数下跌日数(对齐后)": stats["down_days"],
                    "个股上涨日数(对齐后)": stats["stock_up_days"],
                    "个股下跌日数(对齐后)": stats["stock_down_days"],
                })
            else:
                print(f"[相对强弱] FAIL {code} {stock_name} | {stats}")

bs.logout()

# 输出
if selected:
    out_df = pd.DataFrame(selected).sort_values(["抗跌满足率", "上涨满足率"], ascending=False)
    out_df.to_csv(
        f"stock_data/Stock-Selection-Relativity-{today}.csv",
        index=False,
        encoding='utf-8-sig'
    )
    print(f"\nStock-Selection-Relativity-{today}.csv 文件已保存")
    print(out_df)
else:
    print("\n没有选出符合相对强弱策略的股票。")