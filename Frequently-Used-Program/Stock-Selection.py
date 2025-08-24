import akshare as ak
import pandas as pd
import numpy as np
import time
import os


'''0.准备工作'''
today = time.strftime("%Y%m%d", time.localtime())

headers = {
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36 Edg/139.0.0.0",
    "connection": "keep-alive"

}
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

'''
ljqs_df=ak.stock_rank_ljqs_ths()
#print(ljqs_df)
ljqs_codes = ljqs_df['股票代码'].tolist() #tolist()变成一个列表
#print(codes)
time.sleep(3)
'''

'''1.2.连续放量'''
'''
cxfl_df=ak.stock_rank_cxfl_ths()
#print(cxfl_df)
cxfl_codes = cxfl_df['股票代码'].tolist()
time.sleep(3)
'''

'''1.3.连续上涨'''
'''
lxsz_df=ak.stock_rank_lxsz_ths()
#print(lxsz_df)
lxsz_codes = lxsz_df['股票代码'].tolist()
time.sleep(3)
'''








'''2.资金流向选股'''
'''2.1.五日资金流入为正'''

try:
    five_days_funds_df=ak.stock_fund_flow_individual("5日排行")
    five_days_funds_df['资金流入净额'] = five_days_funds_df['资金流入净额'].apply(convert_fund_flow)
    five_days_positive_funds_df = five_days_funds_df[(five_days_funds_df['资金流入净额'] > 0) & five_days_funds_df['最新价'] < 40]
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



'''2.2.十日资金流入为正'''

try:
    ten_days_funds_df = ak.stock_fund_flow_individual("10日排行")
    ten_days_funds_df['资金流入净额'] = ten_days_funds_df['资金流入净额'].apply(convert_fund_flow)
    ten_days_positive_funds_df = ten_days_funds_df[(ten_days_funds_df['资金流入净额'] > 0) & (ten_days_funds_df['最新价'] < 40)]
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
#yjbb_df=ak.stock_yjbb_em(date="20250331") #0331 or 0630 or 0930 or 1231
#yjbb_df.to_csv(f"stock_data/stock_yjbb_em-{today}.csv", index=False, encoding='utf-8')
#time.sleep(3)

'''3.2.资产负债率'''
try:
    zcfz_df=ak.stock_zcfz_em(date="20250331")    #0331 or 0630 or 0930 or 1231
    zcfz_df.to_csv(f"stock_data/stock_zcfz_em.csv", index=False, encoding='utf-8')
    good_zcfz_df = zcfz_df[zcfz_df['资产负债率'] < 70]  #资产负债率小于70%
    zcfz_codes = good_zcfz_df['股票代码'].tolist()
    print("资产负债率数据获取成功")

except Exception as e:
    try:
        print(f"获取资产负债率数据失败: {e}，尝试读取本地文件...")
        zcfz_df = pd.read_csv(f"stock_data/stock_zcfz_em.csv")
        good_zcfz_df = zcfz_df[zcfz_df['资产负债率'] < 70]
        zcfz_codes = good_zcfz_df['股票代码'].tolist()
        print("成功读取本地资产负债率数据")
    except Exception as e2:
        print(f"读取本地资产负债率数据失败: {e2}")
        zcfz_codes = []


time.sleep(3)

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
    good_cashflow_df= cashflow_df[(cashflow_df['经营性现金流-现金流量净额'] > 0) & (cashflow_df["净现金流-净现金流"]>0)]
    cashflow_codes = good_cashflow_df['股票代码'].tolist()
    print("现金流量表数据获取成功")
except Exception as e:
    try:
        print(f"获取现金流量表数据失败: {e}，尝试读取本地文件...")
        cashflow_df = pd.read_csv(f"stock_data/stock_xjll_em.csv")
        good_cashflow_df= cashflow_df[(cashflow_df['经营性现金流-现金流量净额'] > 0) & (cashflow_df["净现金流-净现金流"]>0)]
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
'''4.1.取出代码交集'''

if five_days_positive_funds_codes ==[]:
    common_codes_set=list(set(cashflow_codes) & set(profit_codes) & set(zcfz_codes) & set(profit_forecast_codes) & set(format_ten_days_positive_funds_codes))

elif ten_days_positive_funds_codes ==[]:
    common_codes_set = list(set(cashflow_codes) & set(profit_codes) & set(zcfz_codes) & set(profit_forecast_codes) & set(format_five_days_positive_funds_codes))

elif five_days_positive_funds_codes ==[] and ten_days_positive_funds_codes ==[]:
    common_codes_set = list(set(cashflow_codes) & set(profit_codes) & set(zcfz_codes) & set(profit_forecast_codes))

else : common_codes_set = list(set(cashflow_codes) & set(zcfz_codes) & set(profit_codes) & set(profit_forecast_codes) & (set(format_five_days_positive_funds_codes) | set(format_ten_days_positive_funds_codes)))

#common_codes_set = list(set(cashflow_codes) & set(profit_codes) & set(zcfz_codes) & set(profit_forecast_codes))
filtered_codes = [code for code in common_codes_set if not (code.startswith('30') or code.startswith('688'))]  #去掉创业板和科创板
print("Filtered Codes:", filtered_codes)

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
'''5.1.获取5日行业资金'''

try:
    five_days_industry_funds_df=ak.stock_fund_flow_industry(symbol="5日排行")
    five_days_industry_funds_df.to_csv(f"stock_data/Industry-Funds-Flow-5日排行.csv", index=False, encoding='utf-8-sig')
    five_days_hot_industry=five_days_industry_funds_df['行业'].iloc[0:5].tolist()
    time.sleep(3)

except Exception as e:
    try:
        print(f"获取5日数据失败: {e}，正在使用本地数据")
        five_days_industry_funds_df=pd.read_csv(f"stock_data/Industry-Funds-Flow-5日排行.csv")
        five_days_hot_industry=five_days_industry_funds_df['行业'].iloc[0:5].tolist()

    except Exception as e2:
        print(f"获取本地数据失败：{e2}，请检查数据文件")
        five_days_industry_funds_df=[]


'''5.2.获取10日行业资金'''

try:
    ten_days_industry_funds_df=ak.stock_fund_flow_industry(symbol="10日排行")
    ten_days_industry_funds_df.to_csv(f"stock_data/Industry-Funds-Flow-10日排行.csv", index=False, encoding='utf-8')
    ten_days_hot_industry=ten_days_industry_funds_df['行业'].iloc[0:5].tolist()
    time.sleep(3)

except Exception as e:
    try:
        print(f"获取10日行业资金失败,错误信息为：{e},正在使用本地数据")
        ten_days_industry_funds_df=pd.read_csv(f"stock_data/Industry-Funds-Flow-10日排行.csv", index=False, encoding='utf-8')
        ten_days_hot_industry=ten_days_industry_funds_df['行业'].iloc[0:5].tolist()
    
    except Exception as e2:
        print(f"获取本地数据失败！错误信息为：{e2},请检查数据文件")
        ten_days_industry_funds_df=[]

hot_industry=list(set(ten_days_hot_industry+five_days_hot_industry))
filter_summary_df=summary_df[~summary_df['行业'].isin(hot_industry)] # 使用~取反，排除热门行业
filter_summary_codes=filter_summary_df['股票代码'].tolist()
#filter_summary_df.to_csv(f"stock_data/Filtered-Stock-Selection.csv", index=False, encoding='utf-8')
#print("Data saved to Filtered-Industry-Stock-Selection.csv")


'''6.股票筹码分析'''

def filter_by_chips(codes):
    """根据筹码分布筛选股票"""
    chips_filtered_codes = []
    
    for code in codes:
        try:
            print(f"Fetching data for {code}...")
            # 获取筹码分布数据
            chips_df = ak.stock_cyq_em(symbol=code, adjust="qfq")

            if not chips_df.empty:
                latest_data = chips_df.iloc[-1]
                
                profit_ratio = latest_data['获利比例']

                # 您要求的筛选条件
                if profit_ratio < 0.4 and profit_ratio > 0.2 :
                    chips_filtered_codes.append(code)
                    print(f"{code}:获利比例={profit_ratio:.3f}-符合筹码条件")

            time.sleep(1)

        except Exception as e:
            print(f"获取 {code} 筹码数据时出错: {e}")
    
    return chips_filtered_codes

# 只有当有候选股票时才进行筹码分析
if filter_summary_codes:
    candidate_codes = filter_by_chips(filter_summary_codes)
    print(f"筹码筛选后剩余股票数量: {len(candidate_codes)}")
else:
    candidate_codes = []
    print("没有候选股票进行筹码分析")

final_df = filter_summary_df[filter_summary_df['股票代码'].isin(candidate_codes)]
final_df.to_csv(f"stock_data/Stock-Selection-{today}.csv", index=False, encoding='utf-8')
print("Stock-Selection.csv 文件已保存")





