import akshare as ak
import pandas as pd
import numpy as np
import time
import datetime
from sklearn.linear_model import TheilSenRegressor
from sklearn.metrics import r2_score
import baostock as bs
import pandas as pd

'''0.准备工作'''
# --- 配置区 ---
PRICE_UPPER_LIMIT = 30  # 股价上限
DEBT_ASSET_RATIO_LIMIT = 70  # 资产负债率上限
PROFIT_RATIO_LIMIT = 0.5 # 筹码获利比例上限
CURRENT_YEAR = datetime.datetime.now().year
LAST_YEAR = CURRENT_YEAR - 1
SKIP_CHIPS_ANALYSIS = False # 是否跳过筹码分析

# 根据当前月份确定最近的财报日期
# 5月前用去年年报，5-8月用一季报，9-10月用中报，11月后用三季报
current_month = datetime.datetime.now().month
current_day = datetime.datetime.now().day

if current_month < 5:
    REPORT_DATE_PROFIT = f"{LAST_YEAR}0930" # 利润表/现金流量表使用去年年报
    REPORT_DATE_HOLDER = f"{LAST_YEAR}0930" # 股东信息使用去年年报
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

UNFAMILIAR_INDUSTRY = [
    "钢铁行业", "化学制品", "房地产开发", "纺织服装", "水泥建材", "燃气",
    "航运港口", "化学原料", "美容护理", "农药兽药", "化纤行业", "采掘行业",
    "化肥行业", "酿酒行业", "商业百货", "中药", "化学制药", "医药商业", "生物制品",
    "工业金属", "钢铁", "港口航运", "造纸", "包装印刷", "食品加工制造", 
    "环境治理", "服装家纺", "养殖业", "建筑材料", "农产品加工", "纺织制造",
    "乳胶制品", "零售", "机场航运", "公路铁路运输", "化学纤维", "电池", 
    "生物制品", "光学光电子", "种植业与林业", "农化制品", "金属新材料", "饮料制造",
    "小金属", "建筑装饰", "石油加工贸易", "橡胶制品", "油气开采及服务","医疗服务",
    "电子化学品", "煤炭开采加工"


]
IMPORTANT_SHAREHOLDERS = [
    "香港中央结算有限公司", "中央汇金资产管理有限公司", "中央汇金投资有限责任公司",
    "香港中央结算（代理人）有限公司", "中国证券金融股份有限公司"
]
IMPORTANT_SHAREHOLDER_TYPES = ["社保基金"]
# --- 配置区结束 ---


today = time.strftime("%Y%m%d", time.localtime())

# pandas显示设置
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def add_market_prefix_upper(code):
    """为股票代码添加大写市场前缀 (SH/SZ)"""
    formatted_code = format_stock_code(code)
    return f"SH{formatted_code}" if formatted_code.startswith('6') else f"SZ{formatted_code}"

def format_stock_code(code):
    """将股票代码格式化为6位数"""
    if isinstance(code, str):
        code = ''.join(filter(str.isdigit, code))
        return code.zfill(6)
    elif isinstance(code, int):
        return f"{code:06d}"
    return code

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
    通用数据获取函数，支持从API获取，失败则从本地CSV读取。
    :param api_func: akshare的数据获取函数。
    :param file_path: 本地缓存文件的路径。
    :param args, kwargs: 传递给api_func的参数。
    :return: pandas DataFrame。
    """
    try:
        df = api_func(*args, **kwargs)
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
        print(f"API call for {file_path} was successful. Data saved.")
        return df
    except Exception as e:
        print(f"API call for {file_path} failed: {e}. Trying to read local file...")
        try:
            df = pd.read_csv(file_path)
            print(f"Successfully read local file: {file_path}")
            return df
        except Exception as e2:
            print(f"Failed to read local file {file_path}: {e2}")
            return pd.DataFrame()

'''1.技术面选股'''
# 待补充

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
'''3.1.业绩快报'''
# 待补充

'''3.2.资产负债率'''
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


'''3.3.利润表'''
profit_df = fetch_data_with_fallback(
    ak.stock_lrb_em,
    f"stock_data/stock_lrb_em_{REPORT_DATE_PROFIT}.csv",
    date=REPORT_DATE_PROFIT
)
profit_codes = []
if not profit_df.empty:
    good_profit_df = profit_df[(profit_df['净利润'] > 0) & (profit_df['净利润同比'] > 0)]
    profit_codes = good_profit_df['股票代码'].tolist()
time.sleep(3)

'''3.4.现金流量表'''
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

'''3.5.盈利预测'''
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
'''4.1.取出条件代码'''
print("\n各条件股票数量:")
print(f"  现金流: {len(cashflow_codes)}")
print(f"  利润表: {len(profit_codes)}")
print(f"  资产负债率: {len(zcfz_codes)}")
print(f"  盈利预测: {len(profit_forecast_codes)}")
print(f"  3日资金: {len(format_three_days_positive_funds_codes)}")
print(f"  5日资金: {len(format_five_days_positive_funds_codes)}")
print(f"  10日资金: {len(format_ten_days_positive_funds_codes)}")

# 分步计算
fundamental_intersection = set(cashflow_codes) & set(profit_codes) & set(zcfz_codes) & set(profit_forecast_codes)
print(f"基本面条件交集: {len(fundamental_intersection)}")

set_3d = set(format_three_days_positive_funds_codes)
set_5d = set(format_five_days_positive_funds_codes)
set_10d = set(format_ten_days_positive_funds_codes)
# 至少满足3、5、10日资金流入中的两个
fund_flow_union = (set_3d & set_5d) | (set_3d & set_10d) | (set_5d & set_10d)
print(f"资金流向条件(至少满足两者)交集: {len(fund_flow_union)}")

common_codes_set = fundamental_intersection & fund_flow_union
print(f"所有条件交集后: {len(common_codes_set)}")

filtered_codes = [code for code in common_codes_set if not (str(code).startswith('30') or str(code).startswith('688'))]
print(f"排除创业板和科创板后: {len(filtered_codes)}")


'''4.2.获取股票信息'''
# 优化：先收集数据再创建DataFrame，效率更高
stock_info_list = []
if filtered_codes:
    print("正在合并股票信息...")
    for code in filtered_codes:
        try:
            # 使用新的接口和带大写前缀的代码
            prefixed_code = add_market_prefix_upper(code)
            stock_info_df = ak.stock_individual_basic_info_xq(symbol=prefixed_code)
            info_dict = dict(zip(stock_info_df['item'], stock_info_df['value']))

            # 使用新的字段名
            stock_name = info_dict.get('org_short_name_cn', '未知')
            
            # 解析行业信息
            industry_str = info_dict.get('affiliate_industry', '{}')
            try:
                industry=industry_str['ind_name']
            except (ValueError, SyntaxError):
                industry = '未知'

            stock_info_list.append({
                '股票代码': code,
                '股票名称': stock_name,
                '行业': industry
            })
            print(f"正在处理 {code} - {stock_name}")
            time.sleep(1)
        except Exception as e:
            print(f"获取 {code} 个股信息时出错: {e}")
    summary_df = pd.DataFrame(stock_info_list)
else:
    print("没有筛选出的股票可供合并")
    summary_df = pd.DataFrame()


'''5.去除最近涨幅大的行业'''
hot_industry = set()
cold_industry = set()
for period in ["3日排行", "5日排行", "10日排行"]:
    period_name = period.split('日')[0]
    df = fetch_data_with_fallback(
        ak.stock_fund_flow_industry,
        f"stock_data/Industry-Funds-Flow-{period}.csv",
        symbol=period
    )
    if not df.empty:
        hot_industry.update(df['行业'].head(2).tolist())
        cold_industry.update(df['行业'].tail(2).tolist())
    time.sleep(3)

if not summary_df.empty:
    filter_summary_df = summary_df[
        (~summary_df['行业'].isin(hot_industry)) &
        (~summary_df['行业'].isin(cold_industry)) &
        (~summary_df['行业'].isin(UNFAMILIAR_INDUSTRY))
    ]
    filter_summary_codes = filter_summary_df['股票代码'].tolist()
else:
    filter_summary_df = pd.DataFrame()
    filter_summary_codes = []


'''6.股票筹码分析'''
def filter_by_chips(codes):
    """根据筹码分布筛选股票，出错时直接跳过。"""
    chips_filtered_codes = []
    for code in codes:
        try:
            print(f"正在获取 {code} 的筹码数据...")
            chips_df = ak.stock_cyq_em(symbol=code, adjust="qfq")
            if not chips_df.empty:
                profit_ratio = chips_df.iloc[-1]['获利比例']
                if profit_ratio < PROFIT_RATIO_LIMIT:
                    chips_filtered_codes.append(code)
                    print(f"{code}: 获利比例={profit_ratio:.3f} - 符合筹码条件")
                else:
                    print(f"{code}: 获利比例={profit_ratio:.3f} - 不符合筹码条件")
            else:
                print(f"未获取到 {code} 的筹码数据，跳过。")
        except Exception as e:
            print(f"获取 {code} 筹码数据时出错: {e}，已跳过。")
    return chips_filtered_codes

if SKIP_CHIPS_ANALYSIS:
    print("\n--- 开始进行第6步：股票筹码分析 ---")
    candidate_codes = filter_by_chips(filter_summary_codes) if filter_summary_codes else []
    print(f"筹码筛选后剩余股票数量: {len(candidate_codes)}")
else:
    print("\n--- 已跳过第6步：股票筹码分析 ---")
    candidate_codes = filter_summary_codes # 跳过筹码分析，直接使用上一环节的结果


'''7.流通股东分析'''
def add_market_prefix(code):
    """为股票代码添加小写市场前缀"""
    formatted_code = format_stock_code(code)
    return f"sh{formatted_code}" if formatted_code.startswith('6') else f"sz{formatted_code}"

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
                if not has_important and any(any(imp_type in str(t) for t in top5_types) for imp_type in IMPORTANT_SHAREHOLDER_TYPES):
                    has_important = True

            if has_important:
                print(f"{code}：大股东持股稳定，符合条件")
                final_candidate_codes.append(code)
            else:
                print(f"{code}：无重要股东持股")
        except Exception as e:
            print(f"获取 {code} 流通股东数据时出错: {e}. 默认保留该股票。")
            final_candidate_codes.append(code) # 出错时默认保留
else:
    print("没有候选股票进行流通股东分析")

if not filter_summary_df.empty:
    final_df = filter_summary_df[filter_summary_df['股票代码'].isin(final_candidate_codes)]
else:
    print("\n没有符合所有条件的股票。")

'''8.趋势选股'''
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

def format_stock_code(code):
    """将股票代码格式化为6位数"""
    if isinstance(code, str):
        code = ''.join(filter(str.isdigit, code))
        return code.zfill(6)
    elif isinstance(code, int):
        return f"{code:06d}"
    return code

def add_market_prefix_dotted(code):
    """为股票代码添加小写市场前缀"""
    formatted_code = format_stock_code(code)
    return f"sh.{formatted_code}" if formatted_code.startswith('6') else f"sz.{formatted_code}"

#### 获取沪深A股历史K线数据 ####
# 详细指标参数，参见“历史行情指标参数”章节；“分钟线”参数与“日线”参数不同。“分钟线”不包含指数。
# 分钟线指标：date,time,code,open,high,low,close,volume,amount,adjustflag
# 周月线指标：date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg
# 1：后复权；2：前复权 ；3：不复权

days_back = 20 # 修改：增加回溯天数，以便更好地进行滤波和平滑趋势分析
start_date = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime('%Y-%m-%d')
trend_data=[]

for fncode in final_candidate_codes:
    rs = bs.query_history_k_data_plus(add_market_prefix_dotted(fncode),
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
        start_date=start_date, 
        end_date=f"{CURRENT_YEAR}-{current_month}-{current_day}",
        frequency="d", adjustflag="2")

    print('query_history_k_data_plus respond error_code:'+rs.error_code)
    print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)
      
    data_list = []
    while (rs.error_code == '0') & rs.next():
       data_list.append(rs.get_row_data())

    result_df = pd.DataFrame(data_list, columns=rs.fields)
    result_df=result_df[['date','code','open','high','low','close','preclose']]  

    # 将收盘价转换为数值类型
    result_df['close'] = pd.to_numeric(result_df['close'], errors='coerce')
    result_df = result_df.dropna(subset=['close'])
    
    if len(result_df) < 3: # 数据点太少，无法判断趋势
        print(f"股票 {fncode} 的数据不足，跳过趋势分析。")
        continue

    # --- 新增：滤波处理 ---
    # 使用指数加权移动平均 (EWMA) 滤除短期回调噪音，提取平滑趋势
    # span=5 对应约 3 日的平滑窗口，既能滤掉单日剧烈回调，又能保留上升形态
    result_df['close_smooth'] = result_df['close'].ewm(span=5, adjust=False).mean()
    
    y = result_df['close_smooth'].values # 使用平滑后的数据进行拟合
    x = np.arange(len(y)).reshape(-1, 1)
        
    # 拟合 Theil-Sen 回归模型，它对异常值更不敏感
    model = TheilSenRegressor(random_state=42)
    model.fit(x, y)
        
    # 获取斜率和 R² 值
    slope = model.coef_[0]
    y_pred = model.predict(x)
    r_squared = r2_score(y, y_pred) # 使用 r2_score 计算 R²

    # --- 优化：斜率归一化 ---
    # 将斜率除以平均股价，得到一个相对变化率
    normalized_slope = slope / np.mean(y) if np.mean(y) != 0 else 0
    
    # --- 优化：结合 R² 和归一化斜率进行判断 ---
    signal = "stand" # 默认状态
    
    # 修改阈值说明：
    # 1. R² > 0.25: 放宽线性度要求，允许更深的回调或波动
    # 2. Slope > 0.001: 日均涨幅只要大于 0.1% 即可（捕捉缓慢爬升的股票）
    if r_squared > 0.25: 
        if 0.001 < normalized_slope <= 0.05: # 日均涨幅在 0.1% 到 5% 之间
            signal = "buy"
            print(f"  [选中] {fncode}: 日均涨幅={normalized_slope*100:.2f}%, 趋势强度R2={r_squared:.2f}")
        elif normalized_slope > 0.05: # 日均涨幅超过 5%
            signal = "overheated"
        elif normalized_slope < -0.001: # 日均跌幅超过 0.1%
            signal = "sell"
    
    # 调试打印：如果觉得还是选不出，可以取消下面这行的注释，看看具体数值
    # else: print(f"  [淘汰] {fncode}: 斜率={normalized_slope:.4f}, R2={r_squared:.4f}")

    trend_data.append({
            '股票代码': fncode,
            'slope': normalized_slope,
            'r_squared': r_squared, # 新增：保存 R² 值
            'signal': signal
        })

trend_df = pd.DataFrame(trend_data)
# ... (后续代码)

merged_df = pd.merge(final_df, trend_df, on='股票代码', how='inner')

# 只保留 signal 为 'buy' 的股票
buy_df = merged_df[merged_df['signal'] == 'buy']

buy_df.to_csv(f"stock_data/Stock-Selection-{today}T.csv", index=False, encoding='utf-8-sig')
print(f"\nStock-Selection-{today}T.csv 文件已保存，共选出 {len(buy_df)} 只标记为'buy'的股票")
#### 登出系统 ####
bs.logout()