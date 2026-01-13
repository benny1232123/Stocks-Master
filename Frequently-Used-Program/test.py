import baostock as bs
import pandas as pd
import time
import datetime
from sklearn.linear_model import LinearRegression
import numpy as np
import akshare as ak

today = time.strftime("%Y%m%d", time.localtime())
now = datetime.datetime.now()
CURRENT_YEAR = now.year
current_month = now.month
current_day = now.day


df=ak.fund_etf_spot_em()
df.to_csv("ETF基金.csv", encoding="utf-8-sig")