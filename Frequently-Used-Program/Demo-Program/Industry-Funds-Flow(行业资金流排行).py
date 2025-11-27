import akshare as ak
import pandas as pd
import numpy as np
import time

today = time.strftime("%Y%m%d", time.localtime())

#selection="即时"
selection="3日排行"
#selection="5日排行"
#selection="10日排行"
#selection="20日排行"
df=ak.stock_fund_flow_industry(symbol=selection)
df.to_csv(f"stock_data/Industry-Funds-Flow-{selection}.csv", index=False, encoding='utf-8')