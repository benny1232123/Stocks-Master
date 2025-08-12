import akshare as ak
import pandas as pd
import numpy as np
import time

today = time.strftime("%Y%m%d", time.localtime())

df=ak.stock_zh_a_spot_em()
df.to_csv(f"Frequently-Used-Program/Real-Time-Stock-Flow-{today}.csv", index=False, encoding='utf-8')
print("Data saved to Real-Time-Stock-Flow.csv")

#量比 = 当前成交量 / 过去5日同一时段平均成交量
#反映当前成交活跃度。量比>1说明今天成交比过去活跃，量比<1说明成交低迷。短线资金常用来判断异动。

#换手率 = 当日成交股数 / 流通股本 × 100%
#反映股票流通性和活跃度。换手率高说明交易活跃，主力进出频繁；换手率低说明筹码锁定，交易清淡。

#市盈率 = 股价 / 每股盈利（最近12个月）
#反映市场对公司未来盈利的预期。市盈率高说明市场看好成长性，市盈率低可能被低估或成长性差。不同板块市盈率可比性强。

#市净率 = 股价 / 每股净资产
#反映公司股价与账面价值的关系。市净率低说明股价接近或低于净资产，通常被认为安全边际高；市净率高说明市场对公司资产溢价高。

