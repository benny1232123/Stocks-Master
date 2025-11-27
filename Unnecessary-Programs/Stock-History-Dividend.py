import akshare as ak
import pandas as pd  
import numpy as np
import time

today = time.strftime("%Y%m%d", time.localtime())
df=ak.stock_history_dividend()
df.to_csv(f"Stock-History-Dividend-{today}.csv", index=False, encoding='utf-8')
print("Data saved to Stock-History-Dividend.csv")