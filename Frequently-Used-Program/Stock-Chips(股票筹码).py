import akshare as ak
import pandas as pd
import numpy as np
import time

today = time.strftime("%Y%m%d", time.localtime())
codes=["600900","600027","601728"]

for code in codes:
    print(f"Fetching data for {code}...")
    df=ak.stock_cyq_em(symbol=code,adjust="qfq")
    df.to_csv(f"stock_data/Stock-Chips-{code}-{today}.csv", index=False, encoding='utf-8')
    print(f"Data saved to Stock-Chips-{code}.csv")

#获利比例：当前价格下，持仓盈利的筹码占比（越高说明大部分人赚钱，越低说明大部分人亏钱）
#0.2-0.4：大部分投资者亏损，预示着股价接近阶段性底部
#0.6-0.8：大部分投资者盈利，预示着股价接近阶段性顶部
#理想买入区间：获利比例在0.2以下 理想卖出区间：获利比例在0.8以上


#平均成本：市场主力资金的平均持仓成本
#90成本-低/高：90%筹码的成本区间（低/高），反映主力大资金的主要建仓区间
#70成本-低/高：70%筹码的成本区间（低/高），反映次主力或大部分散户的主要建仓区间

#90集中度：90%筹码的集中度，理想值在0.05-0.1之间，数值越小说明筹码越集中，越大说明分散
#70集中度：70%筹码的集中度，理想值在0.03-0.06之间，数值越小说明筹码越集中，越大说明分散