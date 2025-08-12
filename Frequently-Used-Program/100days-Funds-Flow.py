import akshare as ak
import pandas as pd
import numpy as np
import time

today=time.strftime("%Y%m%d", time.localtime())
codes=["600900","600027","601728"]
for code in codes:
    
    df=ak.stock_individual_fund_flow(stock=code,market="sh")
    df.to_csv(f"Frequently-Used-Program/100days-Funds-Flow-{code}-{today}.csv", index=False, encoding='utf-8')
    print("Data saved to 100days-Funds-Flow.csv")
