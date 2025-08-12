import akshare
import pandas as pd
import numpy as np
import time

today = time.strftime("%Y%m%d", time.localtime())
df=akshare.stock_zh_a_spot()
df.to_csv(f"All-Stock-{today}.csv", index=False, encoding='utf-8')
print("Data saved to Stock1.csv")