import akshare as ak
import pandas as pd
import numpy as np
import time

today=time.strftime("%Y%m%d", time.localtime())


df=ak.stock_fund_flow_individual()
df.to_csv(f"Frequently-Used-Program/Real-Time-Funds-{today}.csv", index=False, encoding='utf-8')
print("Data saved to Real-Time-Funds.csv")
