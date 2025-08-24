import akshare as ak
import pandas as pd
import numpy as np
import time

codes=["600900","600027","601728"]
for code in codes:
    df=ak.stock_institute_recommend_detail(symbol=code)
    df.to_csv(f"stock_data/Institute-Recommend-{code}.csv", index=False, encoding='utf-8')
    print(f"Data saved to Institute-Recommend-{code}.csv")