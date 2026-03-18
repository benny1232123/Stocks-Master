import baostock as bs
import pandas as pd
import time
import datetime
import numpy as np
import akshare as ak

today = time.strftime("%Y%m%d", time.localtime())
now = datetime.datetime.now()
CURRENT_YEAR = now.year
current_month = now.month
current_day = now.day


df=ak.news_cctv(date=today)
df.to_csv(f"{today}_cctv.csv", index=False)