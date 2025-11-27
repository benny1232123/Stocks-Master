import akshare as ak
import pandas as pd
import numpy as np

date="20241"
#2024开始 1季度报
codes=["600900","600027","601728"]
for code in codes:
    df=ak.stock_institute_hold(symbol=date)
    new_df=df[df["证券代码"]== code]
    new_df.to_csv(f"stock_data/Institute-Hold-{code}-{date}.csv", index=False, encoding='utf-8')
    print(f"Data saved to Institute-Hold-{code}-{date}.csv")