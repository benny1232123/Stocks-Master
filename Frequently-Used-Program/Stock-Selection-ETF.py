import os
import time
from datetime import datetime, timedelta

import akshare as ak
import pandas as pd


'''0.准备工作'''
# --- 配置区 ---
DAYS_BACK = 120                   # 拉取历史天数（>=BOLL_WINDOW即可，建议>=80）
BOLL_WINDOW = 20                  # 布林带窗口
K_STD = 1.645                     # 90%概率对应约1.645倍标准差
NEAR_LOWER_MULT = 1.015           # 接近下轨阈值：close <= lower * 1.015
SLEEP_SECONDS = 1.0               # 接口节流
OUTPUT_DIR = "stock_data"         # 只用于保存最终结果
# --- 配置区结束 ---

today = time.strftime("%Y%m%d", time.localtime())
os.makedirs(OUTPUT_DIR, exist_ok=True)

pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


def fetch_data(api_func, *args, **kwargs) -> pd.DataFrame:
    """
    仅从API获取数据；不做任何本地缓存读写（按要求：只保存最终筛选CSV）。
    """
    try:
        df = api_func(*args, **kwargs)
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def to_float_safe(x):
    try:
        if pd.isna(x):
            return None
        if isinstance(x, str):
            x = x.replace("%", "").replace(",", "").strip()
        return float(x)
    except Exception:
        return None


def yi_from_amount(amount) -> float:
    """
    将成交额（可能为数值或字符串）转为“亿元”。
    akshare的ETF现货字段通常是数值（元），这里兼容处理。
    """
    val = to_float_safe(amount)
    if val is None:
        return 0.0
    return val / 1e8


'''1.获取ETF实时列表（用于筛选候选池）'''
spot_df = fetch_data(ak.fund_etf_spot_em)
if spot_df.empty:
    raise SystemExit("ETF实时数据为空：请检查网络/akshare接口。")

# 统一字段名（不同版本 akshare 可能略有差异）
col_code = "代码" if "代码" in spot_df.columns else None
col_name = "名称" if "名称" in spot_df.columns else None
col_price = "最新价" if "最新价" in spot_df.columns else ("最新" if "最新" in spot_df.columns else None)
col_amount = "成交额" if "成交额" in spot_df.columns else None

missing = [c for c in [col_code, col_name, col_price] if c is None]
if missing:
    raise SystemExit(f"ETF现货字段缺失，无法继续：spot columns={list(spot_df.columns)}")

spot_df = spot_df.copy()
spot_df[col_price] = spot_df[col_price].apply(to_float_safe)

if col_amount is not None:
    spot_df["_turnover_yi"] = spot_df[col_amount].apply(yi_from_amount)
else:
    spot_df["_turnover_yi"] = 0.0

candidate_df = spot_df[
    (spot_df[col_price].notna()) &
    (spot_df[col_price] > 0)&
    (spot_df["基金折价率"].apply(to_float_safe) < 0)  &
    ((spot_df["超大单净流入-净额"].apply(to_float_safe) + spot_df["大单净流入-净额"].apply(to_float_safe) + spot_df["中单净流入-净额"].apply(to_float_safe) + spot_df["小单净流入-净额"].apply(to_float_safe)) > 0)
].copy()

candidate_df[col_code] = candidate_df[col_code].astype(str)
candidate_codes = candidate_df[col_code].tolist()
print(f"候选ETF数量：{len(candidate_codes)}")

if not candidate_codes:
    raise SystemExit("候选ETF为空：请放宽 PRICE_UPPER_LIMIT / MIN_TURNOVER_YI。")


'''2.逐个获取历史K线 + 计算布林带筛选'''
start_date = (datetime.now() - timedelta(days=DAYS_BACK)).strftime("%Y%m%d")

results = []
total = len(candidate_codes)

for idx, code in enumerate(candidate_codes, start=1):
    name = ""
    if col_name is not None:
        try:
            name = candidate_df.loc[candidate_df[col_code] == str(code), col_name].iloc[0]
        except Exception:
            name = ""

    # 进度显示（覆盖同一行）
    print(f"[{idx}/{total}] 正在处理: {code} {name}", end="\r", flush=True)

    hist_df = fetch_data(
        ak.fund_etf_hist_em,
        symbol=str(code),
        period="daily",
        start_date=start_date,
        end_date=today,
        adjust=""
    )
    if hist_df.empty:
        time.sleep(SLEEP_SECONDS)
        continue

    if "日期" not in hist_df.columns or "收盘" not in hist_df.columns:
        time.sleep(SLEEP_SECONDS)
        continue

    df = hist_df[["日期", "收盘"]].copy()
    df["收盘"] = pd.to_numeric(df["收盘"], errors="coerce")
    df = df.dropna(subset=["收盘"]).sort_values("日期")
    if len(df) < BOLL_WINDOW:
        time.sleep(SLEEP_SECONDS)
        continue

    df["MA"] = df["收盘"].rolling(window=BOLL_WINDOW).mean()
    df["STD"] = df["收盘"].rolling(window=BOLL_WINDOW).std()
    df["Upper"] = df["MA"] + K_STD * df["STD"]
    df["Lower"] = df["MA"] - K_STD * df["STD"]

    latest = df.iloc[-1]
    close = float(latest["收盘"])
    lower = float(latest["Lower"])
    upper = float(latest["Upper"])

    status = None
    if close < lower:
        status = "低于下轨(超卖)"
    elif close <= lower * NEAR_LOWER_MULT:
        status = "接近下轨(关注)"

    if status:
        pct_to_lower = (close / lower - 1.0) * 100 if lower != 0 else None
        row = {
            "代码": str(code),
            "名称": name,
            "日期": str(latest["日期"]),
            "收盘": round(close, 4),
            "下轨": round(lower, 4),
            "上轨": round(upper, 4),
            "距离下轨(%)": round(pct_to_lower, 2) if pct_to_lower is not None else None,
            "状态": status
        }
        results.append(row)

        # 命中时打印（先换行避免覆盖进度行）
        print()
        print(f"{row['代码']} {row['名称']} {row['状态']} 距离下轨{row['距离下轨(%)']}%")

    time.sleep(SLEEP_SECONDS)

# 循环结束后补一个换行，避免光标停在同一行
print()


'''3.只保存选出来的csv（不保存任何缓存csv）'''
out_path = os.path.join(OUTPUT_DIR, f"ETF-Selection-Boll-{today}.csv")
if results:
    out_df = pd.DataFrame(results).sort_values(by=["状态", "距离下轨(%)"], ascending=[True, True])
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
# else: 按要求不输出任何“未选出”的提示