import akshare as ak
import pandas as pd
import numpy as np
df=ak.stock_comment_em()
print(df)
#df.to_csv("Stock-Comments.csv", index=False, encoding='utf-8')
#print("Data saved to Stock-Comments.csv")

#something wrong with the akshare package, it does not return the data correctly