import akshare as ak
import pandas as pd
import numpy as np

codes=["SH600900","SH600027","SH601728"]
for code in codes:
    df=ak.stock_hot_keyword_em(symbol=code)
    df.to_csv(f"stock_data/Hot-Keyword-{code}.csv", index=False, encoding='utf-8')
    print(f"Data saved to Hot-Keyword-{code}.csv")