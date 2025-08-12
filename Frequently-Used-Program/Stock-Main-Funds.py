import akshare as ak
import pandas as pd
import numpy as np
import time

today=time.strftime("%Y%m%d", time.localtime())
df=ak.stock_main_fund_flow(symbol="沪深A股")
df.to_csv(f"Frequently-Used-Program/Stock-Main-Funds-{today}.csv", index=False, encoding='utf-8')
print("Data saved to Stock-Main-Funds.csv")