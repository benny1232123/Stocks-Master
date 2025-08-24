import akshare as ak
import pandas as pd
import numpy as np
import time

today = time.strftime("%Y%m%d", time.localtime())

codes=["600900","600027","601728"]
for code in codes:
    print(f"Fetching data for {code}...")
    # Fetching historical evaluation data
    df=ak.stock_comment_detail_zhpj_lspf_em(symbol=code)
    df.to_csv(f"stock_data/Historical-Evaluation-{code}-{today}.csv", index=False, encoding='utf-8')
    print("Data saved to Historical-Evaluation.csv")
