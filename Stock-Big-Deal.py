import akshare as ak
import pandas as pd
import numpy as np
import time

today = time.strftime("%Y%m%d", time.localtime())
df=ak.stock_fund_flow_big_deal()
df.to_csv(f"Stock-Big-Deal-{today}.csv", index=False, encoding='utf-8')
print("Data saved to Stock-Big-Deal.csv")