import akshare as ak
import pandas as pd
import numpy as np
import time

today = time.strftime("%Y%m%d", time.localtime())
codes=["600900"]

for code in codes:
    df=ak.stock_profile_cninfo(symbol=code)
    df.to_csv(f"Stock-Profile-info-{code}-{today}.csv",index=False,encoding="utf-8")
    print("Data saved to stock_profile_info.csv")