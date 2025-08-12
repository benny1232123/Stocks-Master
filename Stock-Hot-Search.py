import akshare as ak
import pandas as pd
import numpy as np
import time

today = time.strftime("%Y%m%d", time.localtime())
df=ak.stock_hot_search_baidu(symbol="A股",date="20250812",time="今日")
df.to_csv(f"Stock-Hot-Search-{today}.csv", index=False, encoding='utf-8')
print("Data saved to Stock-Hot-Search.csv")