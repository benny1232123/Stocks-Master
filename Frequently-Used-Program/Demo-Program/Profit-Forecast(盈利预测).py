import akshare as ak
import pandas as pd
import numpy as np

codes=["600900","600027","601728"]
for code in codes:
    df=ak.stock_profit_forecast_em(symbol="")
    new_df=df[df["代码"]== code]
    new_df.to_csv(f"stock_data/Profit-Forecast-{code}.csv", index=False, encoding='utf-8')
    print(f"Data saved to Profit-Forecast-{code}.csv")