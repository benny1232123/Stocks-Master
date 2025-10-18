import akshare as ak
import pandas as pd
import numpy as np
import time
import requests

'''0.准备工作'''
today = time.strftime("%Y%m%d", time.localtime())

# 列名与数据对其显示
pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Edg/139.0.0.0",
    "Connection": "keep-alive",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache"
}


# 创建会话对象，复用连接
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Edg/139.0.0.0",
    "Connection": "keep-alive"
})

# 可以尝试设置 akshare 使用这个 session
# 注意：这取决于 akshare 是否支持自定义 session
def format_stock_code(code):
    """将股票代码格式化为6位数"""
    if isinstance(code, str):
        # 移除可能存在的前缀（如SH、SZ等）
        code = code.replace('SH', '').replace('SZ', '').replace('sh', '').replace('sz', '')
        # 格式化为6位数，不足6位的前面补0
        return code.zfill(6)
    elif isinstance(code, int):
        return f"{code:06d}"
    return code

def convert_fund_flow(value):
    if isinstance(value, str):
        # 去除亿、万等单位，并转换为数值
        if '亿' in value:
            return float(value.replace('亿', '')) * 100000000
        elif '万' in value:
            return float(value.replace('万', '')) * 10000
        else:
            return float(value)
    return value



'''1.技术面选股'''
'''1.1.量价齐升'''



'''2.资金流向选股'''
'''2.1.三日资金流入为正'''
try:
    three_days_funds_df=ak.stock_fund_flow_individual("3日排行")
    three_days_funds_df['资金流入净额'] = three_days_funds_df['资金流入净额'].apply(convert_fund_flow)
    three_days_positive_funds_df = three_days_funds_df[(three_days_funds_df['资金流入净额'] > 0) & (three_days_funds_df['最新价'] < 30)]
    three_days_positive_funds_df.to_csv(f"stock_data/three-days-positive-funds.csv", index=False, encoding="utf-8")
    print("The api call was successful. Data saved to three-days-positive-funds.csv")
    three_days_positive_funds_codes = three_days_positive_funds_df['股票代码'].tolist() if not three_days_positive_funds_df.empty else []
    format_three_days_positive_funds_codes = [format_stock_code(code) for code in three_days_positive_funds_codes]
    time.sleep(3)

except Exception as e:
    try:
        print(f"Error occurred while processing three_days_funds_df: {e},trying to read local file...")
        three_days_positive_funds_df = pd.read_csv(f"stock_data/three-days-positive-funds.csv")
        print("Successfully read local three-days-positive-funds.csv")
        three_days_positive_funds_codes = three_days_positive_funds_df['股票代码'].tolist() if not three_days_positive_funds_df.empty else []
        format_three_days_positive_funds_codes = [format_stock_code(code) for code in three_days_positive_funds_codes]
        time.sleep(3)

    except Exception as e2:
        print(f"Failed to read local file: {e2}")
        three_days_positive_funds_df=pd.DataFrame()

'''2.2.五日资金流入为正'''

try:
    five_days_funds_df=ak.stock_fund_flow_individual("5日排行")
    five_days_funds_df['资金流入净额'] = five_days_funds_df['资金流入净额'].apply(convert_fund_flow)
    five_days_positive_funds_df = five_days_funds_df[(five_days_funds_df['资金流入净额'] > 0) & (five_days_funds_df['最新价'] < 30)]
    five_days_positive_funds_df.to_csv(f"stock_data/five-days-positive-funds.csv", index=False, encoding="utf-8")
    print("The api call was successful. Data saved to five-days-positive-funds.csv")
    five_days_positive_funds_codes = five_days_positive_funds_df['股票代码'].tolist() if not five_days_positive_funds_df.empty else []
    format_five_days_positive_funds_codes = [format_stock_code(code) for code in five_days_positive_funds_codes]
    time.sleep(3)

except Exception as e:
    try:
        print(f"Error occurred while processing five_days_funds_df: {e},trying to read local file...")
        five_days_positive_funds_df = pd.read_csv(f"stock_data/five-days-positive-funds.csv")
        print("Successfully read local five-days-positive-funds.csv")
        five_days_positive_funds_codes = five_days_positive_funds_df['股票代码'].tolist() if not five_days_positive_funds_df.empty else []
        format_five_days_positive_funds_codes = [format_stock_code(code) for code in five_days_positive_funds_codes]
        time.sleep(3)

    except Exception as e2:
        print(f"Failed to read local file: {e2}")
        five_days_positive_funds_df=pd.DataFrame()



'''2.3.十日资金流入为正'''

try:
    ten_days_funds_df = ak.stock_fund_flow_individual("10日排行")
    ten_days_funds_df['资金流入净额'] = ten_days_funds_df['资金流入净额'].apply(convert_fund_flow)
    ten_days_positive_funds_df = ten_days_funds_df[(ten_days_funds_df['资金流入净额'] > 0) & (ten_days_funds_df['最新价'] < 30)]
    ten_days_positive_funds_df.to_csv(f"stock_data/ten-days-positive-funds.csv", index=False, encoding="utf-8")
    print("The api call was successful. Data saved to ten-days-positive-funds.csv")
    ten_days_positive_funds_codes = ten_days_positive_funds_df['股票代码'].tolist() if not ten_days_positive_funds_df.empty else []
    format_ten_days_positive_funds_codes = [format_stock_code(code) for code in ten_days_positive_funds_codes]
    time.sleep(3)

except Exception as e:
    try:
        print(f"Error occurred while processing ten_days_funds_df: {e},trying to read local file...")
        ten_days_positive_funds_df = pd.read_csv(f"stock_data/ten-days-positive-funds.csv")
        print("Successfully read local ten-days-positive-funds.csv")
        ten_days_positive_funds_codes = ten_days_positive_funds_df['股票代码'].tolist() if not ten_days_positive_funds_df.empty else []
        format_ten_days_positive_funds_codes = [format_stock_code(code) for code in ten_days_positive_funds_codes]
        time.sleep(3)

    except Exception as e2:
        print(f"Failed to read local file: {e2}")
        ten_days_positive_funds_df=pd.DataFrame()





'''3.基本面选股'''
'''3.1.业绩快报'''


'''3.2.资产负债率'''
# 一季度
try:
    s1_zcfz_df=ak.stock_zcfz_em(date="20250331")    #0331 or 0630 or 0930 or 1231
    s1_zcfz_df.to_csv(f"stock_data/s1_stock_zcfz_em.csv", index=False, encoding='utf-8')
    s1_good_zcfz_df = s1_zcfz_df[s1_zcfz_df['资产负债率'] < 70]  #资产负债率小于70%
    s1_zcfz_codes = s1_good_zcfz_df['股票代码'].tolist()
    print("一季度资产负债率数据获取成功")
    time.sleep(3)

except Exception as e:
    try:
        print(f"获取资产负债率数据失败: {e}，尝试读取本地文件...")
        s1_zcfz_df = pd.read_csv(f"stock_data/s1_stock_zcfz_em.csv")
        s1_good_zcfz_df = s1_zcfz_df[s1_zcfz_df['资产负债率'] < 70]
        s1_zcfz_codes = s1_good_zcfz_df['股票代码'].tolist()
        print("成功读取本地资产负债率数据")
    except Exception as e2:
        print(f"读取本地资产负债率数据失败: {e2}")
        s1_zcfz_codes = []

# 二季度
try:
    s2_zcfz_df=ak.stock_zcfz_em(date="20250630")
    s2_zcfz_df.to_csv(f"stock_data/s2_stock_zcfz_em.csv", index=False, encoding='utf-8')
    s2_good_zcfz_df = s2_zcfz_df[s2_zcfz_df['资产负债率'] < 70]
    s2_zcfz_codes = s2_good_zcfz_df['股票代码'].tolist()
    print("中报资产负债率数据获取成功")
    time.sleep(3)

except Exception as e:
    try:
        print(f"获取资产负债率数据失败: {e}，尝试读取本地文件...")
        s2_zcfz_df = pd.read_csv(f"stock_data/s2_stock_zcfz_em.csv")
        s2_good_zcfz_df = s2_zcfz_df[s2_zcfz_df['资产负债率'] < 70]
        s2_zcfz_codes = s2_good_zcfz_df['股票代码'].tolist()
        print("成功读取本地资产负债率数据")

    except Exception as e2:
        print(f"读取本地资产负债率数据失败: {e2}")
        s2_zcfz_codes = []

zcfz_codes=list(set(s1_zcfz_codes) | set(s2_zcfz_codes))



'''3.3.利润表'''
try:
    profit_df=ak.stock_lrb_em(date="20250331")  #0331 or 0630 or 0930 or 1231
    profit_df.to_csv(f"stock_data/stock_lrb_em.csv", index=False, encoding='utf-8')
    good_profit_df = profit_df[(profit_df['净利润'] > 0) & (profit_df['净利润同比'] > 0)]
    profit_codes = good_profit_df['股票代码'].tolist()
    print("利润表数据获取成功")
except Exception as e:
    try:
        print(f"获取利润表数据失败: {e}，尝试读取本地文件...")
        profit_df = pd.read_csv(f"stock_data/stock_lrb_em.csv")
        good_profit_df = profit_df[(profit_df['净利润'] > 0) & (profit_df['净利润同比'] > 0)]
        profit_codes = good_profit_df['股票代码'].tolist()
        print("成功读取本地利润表数据")
    except Exception as e2:
        print(f"读取本地利润表数据失败: {e2}")
        profit_codes = []

#print(profit_codes)
time.sleep(3)

'''3.4.现金流量表'''
try:
    cashflow_df=ak.stock_xjll_em(date="20250331")  #0331 or 0630 or 0930 or 1231
    cashflow_df.to_csv(f"stock_data/stock_xjll_em.csv", index=False, encoding='utf-8')
    good_cashflow_df= cashflow_df[(cashflow_df['经营性现金流-现金流量净额'] > 0) ]
    cashflow_codes = good_cashflow_df['股票代码'].tolist()
    print("现金流量表数据获取成功")
except Exception as e:
    try:
        print(f"获取现金流量表数据失败: {e}，尝试读取本地文件...")
        cashflow_df = pd.read_csv(f"stock_data/stock_xjll_em.csv")
        good_cashflow_df= cashflow_df[(cashflow_df['经营性现金流-现金流量净额'] > 0) ]
        cashflow_codes = good_cashflow_df['股票代码'].tolist()
        print("成功读取本地现金流量表数据")
    except Exception as e2:
        print(f"读取本地现金流量表数据失败: {e2}")
        cashflow_codes = []

#print(good_profit_codes)
time.sleep(3)

'''3.5.盈利预测'''
try:
    profit_forecast_df=ak.stock_profit_forecast_em()  
    profit_forecast_df.to_csv(f"stock_data/stock_profit_forecast_em.csv", index=False, encoding='utf-8')
    good_profit_forecast_df = profit_forecast_df[profit_forecast_df['2025预测每股收益'] > 0]
    profit_forecast_codes = good_profit_forecast_df['代码'].tolist()
    print("盈利预测数据获取成功")
except Exception as e:
    try:
        print(f"获取盈利预测数据失败: {e}，尝试读取本地文件...")
        profit_forecast_df = pd.read_csv(f"stock_data/stock_profit_forecast_em.csv")
        good_profit_forecast_df = profit_forecast_df[profit_forecast_df['2025预测每股收益'] > 0]
        profit_forecast_codes = good_profit_forecast_df['代码'].tolist()
        print("成功读取本地盈利预测数据")
    except Exception as e2:
        print(f"读取本地盈利预测数据失败: {e2}")
        profit_forecast_codes = []

#print(profit_forecast_codes)
time.sleep(3)

'''4.数据处理'''
'''4.1.取出条件代码'''

print("各条件股票数量:")
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

fund_flow_union = (set(format_three_days_positive_funds_codes) & set(format_ten_days_positive_funds_codes)) | (set(format_five_days_positive_funds_codes) & set(format_ten_days_positive_funds_codes) | (set(format_three_days_positive_funds_codes)& set(format_five_days_positive_funds_codes)))
print(f"资金流向条件并集: {len(fund_flow_union)}")

common_codes_set = list(fundamental_intersection & fund_flow_union)
print(f"所有条件交集后: {len(common_codes_set)}")

filtered_codes = [code for code in common_codes_set if not (code.startswith('30') or code.startswith('688'))]
print(f"排除创业板和科创板后: {len(filtered_codes)}")

'''4.2.获取股票信息'''

summary_df = pd.DataFrame(columns=['股票代码', '股票名称', '行业'])

# 遍历筛选出的股票代码
if filtered_codes:
    print("正在合并股票信息...")
    for code in filtered_codes:
        try:
            # 获取个股详细信息
            stock_info_df = ak.stock_individual_info_em(symbol=code,timeout=10)
            
            # 提取股票名称
            name_record = stock_info_df[stock_info_df['item'] == '股票简称']
            stock_name = name_record['value'].iloc[0] if not name_record.empty else '未知'
            
            # 提取行业信息
            industry_record = stock_info_df[stock_info_df['item'] == '行业']
            industry = industry_record['value'].iloc[0] if not industry_record.empty else '未知'
            
            # 将信息添加到结果 DataFrame 中
            new_row = pd.DataFrame({
                '股票代码': [code],
                '股票名称': [stock_name],
                '行业': [industry]
            })
            
            summary_df = pd.concat([summary_df, new_row], ignore_index=True)
            print(f"正在处理 {code} - {stock_name}")
            time.sleep(1)  

        except Exception as e:
            print(f"获取 {code} 个股信息时出错: {e}")

else:
    print("没有筛选出的股票可供合并")

'''5.去除最近涨幅大的行业'''
'''5.1.获取3日行业资金'''

try:
    three_days_industry_funds_df=ak.stock_fund_flow_industry(symbol="3日排行")
    three_days_industry_funds_df.to_csv(f"stock_data/Industry-Funds-Flow-3日排行.csv", index=False, encoding='utf-8-sig')
    three_days_hot_industry=three_days_industry_funds_df['行业'].iloc[0:2].tolist()
    three_days_cold_industry=three_days_industry_funds_df['行业'].iloc[-2:].tolist()
    time.sleep(3)

except Exception as e:
    try:
        print(f"获取3日数据失败: {e}，正在使用本地数据")
        three_days_industry_funds_df=pd.read_csv(f"stock_data/Industry-Funds-Flow-3日排行.csv")
        three_days_hot_industry=three_days_industry_funds_df['行业'].iloc[0:2].tolist()
        three_days_cold_industry=three_days_industry_funds_df['行业'].iloc[-2:].tolist()

    except Exception as e2:
        print(f"获取本地数据失败：{e2}，请检查数据文件")
        three_days_industry_funds_df=[]
        

'''5.2.获取5日行业资金'''

try:
    five_days_industry_funds_df=ak.stock_fund_flow_industry(symbol="5日排行")
    five_days_industry_funds_df.to_csv(f"stock_data/Industry-Funds-Flow-5日排行.csv", index=False, encoding='utf-8-sig')
    five_days_hot_industry=five_days_industry_funds_df['行业'].iloc[0:2].tolist()
    five_days_cold_industry=five_days_industry_funds_df['行业'].iloc[-2:].tolist()
    time.sleep(3)

except Exception as e:
    try:
        print(f"获取5日数据失败: {e}，正在使用本地数据")
        five_days_industry_funds_df=pd.read_csv(f"stock_data/Industry-Funds-Flow-5日排行.csv")
        five_days_hot_industry=five_days_industry_funds_df['行业'].iloc[0:2].tolist()
        five_days_cold_industry=five_days_industry_funds_df['行业'].iloc[-2:].tolist()

    except Exception as e2:
        print(f"获取本地数据失败：{e2}，请检查数据文件")
        five_days_industry_funds_df=[]


'''5.3.获取10日行业资金'''

try:
    ten_days_industry_funds_df=ak.stock_fund_flow_industry(symbol="10日排行")
    ten_days_industry_funds_df.to_csv(f"stock_data/Industry-Funds-Flow-10日排行.csv", index=False, encoding='utf-8')
    ten_days_hot_industry=ten_days_industry_funds_df['行业'].iloc[0:2].tolist()
    ten_days_cold_industry=ten_days_industry_funds_df['行业'].iloc[-2:].tolist()
    time.sleep(3)

except Exception as e:
    try:
        print(f"获取10日行业资金失败,错误信息为：{e},正在使用本地数据")
        ten_days_industry_funds_df=pd.read_csv(f"stock_data/Industry-Funds-Flow-10日排行.csv", index=False, encoding='utf-8')
        ten_days_hot_industry=ten_days_industry_funds_df['行业'].iloc[0:2].tolist()
        ten_days_cold_industry=ten_days_industry_funds_df['行业'].iloc[-2:].tolist()

    except Exception as e2:
        print(f"获取本地数据失败！错误信息为：{e2},请检查数据文件")
        ten_days_industry_funds_df=[]

hot_industry=list(set(ten_days_hot_industry) | set(five_days_hot_industry) | set(three_days_hot_industry))
cold_industry=list(set(ten_days_cold_industry) | set(five_days_cold_industry) | set(three_days_cold_industry))
unfamiliar_industry=["钢铁行业","化学制品","房地产开发","纺织服装",
                     "水泥建材","燃气","航运港口","化学原料",
                     "美容护理","农药兽药","化纤行业","采掘行业",
                     "化肥行业","酿酒行业","商业百货","中药",
                     "化学制药","医药商业","生物制品"]

filter_summary_df=summary_df[(~summary_df['行业'].isin(hot_industry)) & (~summary_df['行业'].isin(cold_industry)) & (~summary_df['行业'].isin(unfamiliar_industry))] # 使用~取反，排除热门和冷门行业

filter_summary_codes=filter_summary_df['股票代码'].tolist()
#filter_summary_df.to_csv(f"stock_data/Filtered-Stock-Selection.csv", index=False, encoding='utf-8')
#print("Data saved to Filtered-Industry-Stock-Selection.csv")


'''6.股票筹码分析'''

def filter_by_chips(codes, max_retries=3):
    """根据筹码分布筛选股票"""
    chips_filtered_codes = []
    
    for code in codes:
        retry_count = 0
        while retry_count < max_retries:
            try:
                print(f"Fetching data for {code}... (尝试 {retry_count + 1}/{max_retries})")
                chips_df = ak.stock_cyq_em(symbol=code, adjust="qfq")
                
                if not chips_df.empty:
                    latest_data = chips_df.iloc[-1]
                    profit_ratio = latest_data['获利比例']
                    
                    if  profit_ratio < 0.5:
                        chips_filtered_codes.append(code)
                        print(f"{code}:获利比例={profit_ratio:.3f}-符合筹码条件")
                
                break  # 成功获取数据后跳出重试循环
                
            except Exception as e:
                retry_count += 1
                print(f"获取 {code} 筹码数据时出错: {e}")
                if retry_count < max_retries:
                    print(f"等待5秒后重试...")
                    time.sleep(5)
                else:
                    print(f"已达最大重试次数，跳过 {code}")
    
    return chips_filtered_codes

# 只有当有候选股票时才进行筹码分析
if filter_summary_codes:
    candidate_codes = filter_by_chips(filter_summary_codes)
    print(f"筹码筛选后剩余股票数量: {len(candidate_codes)}")
else:
    candidate_codes = []
    print("没有候选股票进行筹码分析")


'''7.流通股东分析'''
'''7.流通股东分析'''
def add_market_prefix(code):
    """为股票代码添加小写市场前缀"""
    if isinstance(code, str):
        # 先格式化为6位数
        formatted_code = format_stock_code(code)
        # 根据代码添加市场前缀
        if formatted_code.startswith('6'):
            return f"sh{formatted_code}"
        else:
            return f"sz{formatted_code}"
    elif isinstance(code, int):
        formatted_code = f"{code:06d}"
        if formatted_code.startswith('6'):
            return f"sh{formatted_code}"
        else:
            return f"sz{formatted_code}"
    return code

format_candidate_codes = []
# 定义重要股东名单
important_shareholders = [
    "香港中央结算有限公司",
    "中央汇金资产管理有限公司", 
    "中央汇金投资有限责任公司",
    "香港中央结算（代理人）有限公司",
    "中国证券金融股份有限公司"
]

important_shareholder_types = [
    "社保基金"
]

# 只有当有候选股票时才进行流通股东分析
if candidate_codes:
    for code in candidate_codes:
        try:
            new_code = add_market_prefix(code)
            share_holders_df = ak.stock_gdfx_free_top_10_em(symbol=new_code, date="20250630")  # 0331 or 0630 or 0930 or 1231
            
            has_important_shareholder = False
            
            # 检查是否有重要股东
            if not share_holders_df.empty:
                # 检查股东名称是否包含重要股东
                for shareholder in important_shareholders:
                    if any(shareholder in name for name in share_holders_df["股东名称"].iloc[0:5].tolist()):
                        has_important_shareholder = True
                        break
                
                # 如果还没找到重要股东，检查股东性质
                if not has_important_shareholder:
                    for shareholder_type in important_shareholder_types:
                        if any(shareholder_type in type_name for type_name in share_holders_df["股东性质"].iloc[0:5].tolist()):
                            has_important_shareholder = True
                            break
            
            if has_important_shareholder:
                print(f"{code}：大股东持股稳定，符合条件")
                format_candidate_codes.append(code)
            else:
                print(f"{code}：无重要股东持股")
                
        except Exception as e:
            print(f"获取 {code} 流通股东数据时出错: {e}")
            # 出错时仍然保留该股票，避免因数据获取问题遗漏好股票
            format_candidate_codes.append(code)
else:
    print("没有候选股票进行流通股东分析")

final_df = filter_summary_df[filter_summary_df['股票代码'].isin(format_candidate_codes)]
final_df.to_csv(f"stock_data/Stock-Selection-{today}.csv", index=False, encoding='utf-8')
print(f"Stock-Selection.csv 文件已保存，共选出 {len(format_candidate_codes)} 只股票")
