import akshare as ak
import pandas as pd
import numpy as np

df=ak.stock_rank_cxfl_ths()
df.to_csv("stock_data/Continuous-Increased-Deals.csv", index=False, encoding='utf-8')
print("Data saved to Continuous-Increased-Deals.csv")