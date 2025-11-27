import akshare as ak
import pandas as pd 
import numpy as np
import time

today = time.strftime("%Y%m%d", time.localtime())
df=ak.stock_board_change_em()
df.to_csv(f"stock_data/Stock-Board-Change-{today}.csv", index=False, encoding='utf-8')
print("Data saved to Stock-Board-Change.csv")
