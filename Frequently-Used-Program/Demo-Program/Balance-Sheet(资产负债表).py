import akshare as ak
import pandas as pd
import numpy as np

newdate=input("请输入日期：")
df=ak.stock_zcfz_em(date=newdate)
df.to_csv(f"stock_data/Balance-Sheet-{newdate}.csv", index=False, encoding="utf-8")
print("Data saved to Stock_Balance_Sheet.csv")

#date="xxxx0331"or "xxxx0630" or "xxxx0930" or "xxxx1231"

#资产-总资产同比: 总资产与去年同期相比的增长率（百分比）
#资产负债率: 总负债占总资产的比例，反映公司的财务杠杆水平
#股东权益合计: 公司资产扣除负债后由所有者享有的剩余权益



