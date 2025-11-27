import akshare as ak
import pandas as pd
import numpy as np
import time

today = time.strftime("%Y%m%d", time.localtime())
codes=["600900","600027","601728"]
for code in codes:
    df=ak.stock_comment_detail_scrd_desire_daily_em(symbol=code)
    print(f"正在获取 {code} 的市场参与意愿数据...")
    df.to_csv(f"stock_data/Daily-Market-Participation-Willingness-{code}-{today}.csv", index=False, encoding='utf-8')
    print(f"数据已保存到 Daily-Market-Participation-Willingness-{code}-{today}.csv")