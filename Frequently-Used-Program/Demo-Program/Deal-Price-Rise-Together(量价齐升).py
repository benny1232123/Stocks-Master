import akshare as ak
import pandas as pd
import numpy as np

df=ak.stock_rank_ljqs_ths()
df.to_csv("stock_data/Deal-Price-Rise-Together.csv", index=False, encoding='utf-8')
print("Data saved to Deal-Price-Rise-Together.csv")