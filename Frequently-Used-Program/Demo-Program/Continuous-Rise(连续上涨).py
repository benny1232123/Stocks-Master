import akshare as ak
import pandas as pd
import numpy as np

df=ak.stock_rank_lxsz_ths()
df.to_csv("stock_data/Continuous-Rise.csv", index=False, encoding='utf-8')
print("Data saved to Continuous-Rise.csv")