import akshare as ak
import pandas as pd
import numpy as np
import time

'''0.前期准备'''
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
                    
                    if profit_ratio < 0.5:
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
def add_stock_prefix(code):
        """为股票代码添加前缀"""
        if isinstance(code, int):
            code = f"{code:06d}"
        elif isinstance(code, str):
            # 先格式化为6位数
            code = code.zfill(6)
        
        # 根据规则添加前缀
        if code.startswith(('6', '5')):  # 6开头和5开头的股票（上海）
            return f"SH{code}"
        elif code.startswith(('0', '3', '1')):  # 0、3、1开头的股票（深圳）
            return f"SZ{code}"
        else:
            return code  # 如果不符合规则，返回原代码

pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)

# 主循环
while True:
    print("请输入股票代码（输入'quit'退出）：")
    code = input()
    stock_df=ak.stock_individual_info_em(symbol=code,timeout=10)
    time.sleep(3)
    stock_name = stock_df[stock_df['item'] == '股票简称']['value'].iloc[0]
    print(f"股票名称: {stock_name}")
    if code.lower() == 'quit':
        break

    '''1.技术面判断'''
    '''1.1.持续放量/持续缩量'''
    cxfl_df=ak.stock_rank_cxfl_ths()
    cxfl_codes = cxfl_df['股票代码'].tolist()
    cxfl_codes = [format_stock_code(code) for code in cxfl_codes]
    time.sleep(3)

    cxsl_df=ak.stock_rank_cxsl_ths()
    cxsl_codes = cxsl_df['股票代码'].tolist()
    cxsl_codes = [format_stock_code(code) for code in cxsl_codes]
    time.sleep(3)

    if code in cxfl_codes:
        print("成交量：持续放量")
    elif code in cxsl_codes:
        print("成交量：持续缩量")
    else:
        print("成交量：正常")

    '''1.2.量价齐升/量价齐跌'''
    ljqs_df=ak.stock_rank_ljqs_ths()
    ljqs_codes = ljqs_df['股票代码'].tolist()
    ljqs_codes = [format_stock_code(code) for code in ljqs_codes]
    time.sleep(3)

    ljqd_df=ak.stock_rank_ljqd_ths()
    ljqd_codes = ljqd_df['股票代码'].tolist()
    ljqd_codes = [format_stock_code(code) for code in ljqd_codes]
    time.sleep(3)

    if code in ljqs_codes:
        print("量价关系：量价齐升")
    elif code in ljqd_codes:
        print("量价关系：量价齐跌")
    else:
        print("量价关系：正常")

    '''2.市场反应'''
    '''2.1.历史评分'''
    historical_evaluation_df=ak.stock_comment_detail_zhpj_lspf_em(symbol=code)
    historical_evaluation_df=historical_evaluation_df.iloc[-5:]
    print("历史评分:")
    print(historical_evaluation_df.to_string(index=False,justify="center"))
    time.sleep(3)

    '''2.2.机构参与度'''
    institute_participation_df=ak.stock_comment_detail_zlkp_jgcyd_em(symbol=code)
    institute_participation_df=institute_participation_df.iloc[-5:]
    print("机构参与度:")
    print(institute_participation_df.to_string(index=False,justify="center"))
    time.sleep(3)

    '''2.3.热门关键词'''
    complete_code = add_stock_prefix(code)
    hot_keywords_df=ak.stock_hot_keyword_em(symbol=complete_code)
    hot_keywords_df=hot_keywords_df.iloc[:5]
    print("热门关键词:")
    print(hot_keywords_df.to_string(index=False,justify="center"))
    time.sleep(3)

    '''2.4.涨跌投票'''
    '''
    vote_df=ak.stock_zh_vote_baidu(symbol=code,indicator="股票")
    print("涨跌投票:")
    print(vote_df.to_string(index=False,justify="center"))
    time.sleep(3)
    '''
    
    '''3.1.三日资金流向'''
    try:
        three_days_funds_df = ak.stock_fund_flow_individual("3日排行")
        three_days_funds_df['资金流入净额'] = three_days_funds_df['资金流入净额'].apply(convert_fund_flow)
        three_days_positive_funds_df = three_days_funds_df[three_days_funds_df['资金流入净额'] > 0]
        three_days_positive_funds_df.to_csv("stock_data/three-days-positive-funds.csv")
    
    # 修复：正确判断股票是否在正资金流向列表中
        matching_stocks = three_days_positive_funds_df[three_days_positive_funds_df['股票代码'].apply(format_stock_code) == code]
        if not matching_stocks.empty:
            print("三日资金流向：三日资金流入为正")
        else:
            print("三日资金流向：三日资金流入为负")
    except Exception as e:
        print(f"三日资金流向：获取数据失败 - {e}")
    time.sleep(3)

    '''3.2.五日资金流向'''
    try:
        five_days_funds_df = ak.stock_fund_flow_individual("5日排行")
        five_days_funds_df['资金流入净额'] = five_days_funds_df['资金流入净额'].apply(convert_fund_flow)
        five_days_positive_funds_df = five_days_funds_df[five_days_funds_df['资金流入净额'] > 0]
        five_days_positive_funds_df.to_csv("stock_data/five-days-positive-funds.csv")
    
        # 修复：正确判断股票是否在正资金流向列表中
        matching_stocks = five_days_positive_funds_df[five_days_positive_funds_df['股票代码'].apply(format_stock_code) == code]
        if not matching_stocks.empty:
            print("五日资金流向：五日资金流入为正")
        else:
            print("五日资金流向：五日资金流入为负")
    except Exception as e:
        print(f"五日资金流向：获取数据失败 - {e}")
    time.sleep(3)

    '''3.3.十日资金流向'''
    try:
        ten_days_funds_df = ak.stock_fund_flow_individual("10日排行")
        ten_days_funds_df['资金流入净额'] = ten_days_funds_df['资金流入净额'].apply(convert_fund_flow)
        ten_days_positive_funds_df = ten_days_funds_df[ten_days_funds_df['资金流入净额'] > 0]
        ten_days_positive_funds_df.to_csv("stock_data/ten-days-positive-funds.csv")
    
        # 这部分逻辑是正确的
        if code in ten_days_positive_funds_df['股票代码'].apply(format_stock_code).tolist():
            print("十日资金流向：十日资金流入为正")
        else:
            print("十日资金流向：十日资金流入为负")
    except Exception as e:
        print(f"十日资金流向：获取数据失败 - {e}")
    time.sleep(3)



    '''4.筹码分布'''
    chips_df=ak.stock_cyq_em(symbol=code, adjust="qfq")
    chips_df=chips_df.iloc[-5:]
    print("筹码分布:")
    print(chips_df.to_string(index=False,justify="center"))
    time.sleep(3)

print("程序已退出")