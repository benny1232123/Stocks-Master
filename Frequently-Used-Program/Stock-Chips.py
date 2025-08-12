import akshare as ak
import pandas as pd
import numpy as np
import time

today = time.strftime("%Y%m%d", time.localtime())
codes=["600900","600027","601728"]

for code in codes:
    print(f"Fetching data for {code}...")
    df=ak.stock_cyq_em(symbol=code,adjust="qfq")
    df.to_csv(f"Frequently-Used-Program/Stock-Chips-{code}-{today}.csv", index=False, encoding='utf-8')
    print(f"Data saved to Stock-Chips-{code}.csv")

#获利比例：当前价格下，持仓盈利的筹码占比（越高说明大部分人赚钱，越低说明大部分人亏钱）
#平均成本：市场主力资金的平均持仓成本
#90成本-低/高：90%筹码的成本区间（低/高），反映主力大资金的主要建仓区间
#70成本-低/高：70%筹码的成本区间（低/高），反映次主力或大部分散户的主要建仓区间
#90集中度：90%筹码的集中度，数值越小说明筹码越集中，越大说明分散
#70集中度：70%筹码的集中度，含义同上