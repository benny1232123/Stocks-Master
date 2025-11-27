import akshare as ak
import pandas as pd
import numpy as np

codes=["600900","600027","601728"]
for code in codes:
    df=ak.stock_zh_vote_baidu(symbol=code,indicator="股票")
    df.to_csv(f"stock_data/Stock-Vote-{code}.csv", index=False, encoding='utf-8')
    print(f"Data saved to Stock-Vote-{code}.csv")