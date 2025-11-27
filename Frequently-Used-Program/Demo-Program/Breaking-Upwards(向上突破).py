import akshare as ak
import pandas as pd
import numpy as np

selection="5日均线"
# selection="10日均线"
# selection="20日均线"
# selection="30日均线"  
# selection="60日均线"
# selection="90日均线"
# selection="250日均线"
# selection="500日均线"

df=ak.stock_rank_xstp_ths(symbol=selection)
df.to_csv(f"stock_data/Breaking-Upwards-{selection}.csv", index=False, encoding='utf-8')
print(f"Data saved to Breaking-Upwards-{selection}.csv")