import akshare as ak
import pandas as pd
import numpy as np
df=ak.stock_gdfx_free_holding_change_em(date="20250630")
df.to_csv("Stock-Free-Holding-Change.csv",index=False)
print("Data saved to Stock_Free_Holding_Change.csv")

#something wrong with the date, need to check