import baostock as bs
import pandas as pd
import time
import datetime
from sklearn.linear_model import LinearRegression
import numpy as np

today = time.strftime("%Y%m%d", time.localtime())
now = datetime.datetime.now()
CURRENT_YEAR = now.year
current_month = now.month
current_day = now.day


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
fianl_df=[]
days_back = 10
start_date = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime('%Y-%m-%d')
final_candidate_codes=["600900", "000001", "000002"]  # 示例股票代码列表
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
    

    
    y = result_df['close'].values
    x = np.arange(len(y)).reshape(-1, 1)
        
        # 拟合线性回归模型
    model = LinearRegression()
    model.fit(x, y)
        
        # 获取斜率
    slope = model.coef_[0]

    if slope > 0.05:
        signal = "buy"
    elif slope < -0.05:
        signal = "sell"
    else:
        signal = "stand"

    fianl_df.append({
         'code': fncode,
         'slope': slope,
         "signal": signal
    })
        # 生成交易信号
    if slope > 0.05:  # 上升趋势较强
            print(f"slope is {slope}, {fncode} buy")   # 买入信号
    elif slope < -0.05:  # 下降趋势较强
            print(f"slope is {slope}, {fncode} sell")  # 卖出信号
    else:
            print(f"slope is {slope}, {fncode} stand")   # 持有/观望
    
    #### 结果集输出到csv文件 ####
    result_df.to_csv(f"stock_data/history_A_stock_k_data-{fncode}.csv", index=False)

print(pd.DataFrame(fianl_df))
#### 登出系统 ####
bs.logout()