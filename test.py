import akshare as ak
import pandas as pd
import numpy as np

df=ak.stock_fund_flow_industry(symbol="即时")
df.to_csv("Industry-Funds-Flow-即时.csv", index=False, encoding='utf-8')