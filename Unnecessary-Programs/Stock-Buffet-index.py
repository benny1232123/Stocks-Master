import akshare as ak
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl

#设置中文字体和负号显示
mpl.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
data=ak.stock_buffett_index_lg()
data.to_csv("Stock-Buffet-Index.csv", index=False, encoding="utf-8")
print("Data saved to Stock_Buffet_Index.csv")

plt.figure(figsize=(12, 6))
plt.plot(data["日期"], data["总历史分位数"],linewidth=2.5,color='navy')
plt.axhline(y=0.7, color='red', linestyle='--', label='泡沫警戒线')
plt.axhline(y=0.3, color='green', linestyle='--', label='抄底信号线')
current_value = "{:.0f}%".format(data.iloc[-1]["总历史分位数"] * 100)
plt.title(f'巴菲特指数历史分位数走势 (当前值: {current_value})', fontsize=14)
plt.ylabel("历史分位数", fontsize=12)
plt.grid(True)
plt.legend()
plt.savefig("Stock-Buffet-Index-Trend.png", dpi=300, bbox_inches='tight')
plt.show()

print("Data saved to Stock_Buffet_Index_Trend.png")

#巴菲特指数 = 股票市场总市值 / 国内生产总值（GDP）
#巴菲特指数是衡量股市估值水平的指标，通常用于判断市场是否被高估或低估。

#指数 < 0.5:    严重低估
#指数 0.5-0.75: 轻度低估
#指数 0.75-0.9: 估值合理
#指数 0.9-1.15: 轻度高估
#指数 > 1.15:   严重高估



