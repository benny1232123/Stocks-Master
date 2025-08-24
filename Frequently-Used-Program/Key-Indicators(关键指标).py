import akshare as ak
import pandas as pd
import numpy as np
import time

today = time.strftime("%Y%m%d", time.localtime())

codes = ["600900", "600027", "601728"]
for code in codes:
    print(f"Fetching data for {code}...")
    # Fetching financial abstract data
    period = "按报告期"
    # period = "按年度"
    # period = "按单季度"
    df=ak.stock_financial_abstract_ths(symbol=code,indicator=period)
    df.to_csv(f"stock_data/Key-Indicators-{code}-{today}.csv", index=False, encoding='utf-8')
    print(f"Data saved to Key-Indicators-{code}-{today}.csv")