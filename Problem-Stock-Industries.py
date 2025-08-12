import adata 
import pandas as pd

stocks=adata.stock.info.all_code().head(1)
def fetch_info(func,stock_code,col_name):
    try:
        info=func(stock_code=stock_code)
        return ','.join(info[col_name].tolist()) if not info.empty else 'N/A'
    except:
        return "获取失败"
data=[
    {"股票代码": row["stock_code"],
     "股票名称": row["short_name"],
     "行业": fetch_info(adata.stock.info.get_industry_sw, row["stock_code"], '行业'),
     "概念": fetch_info(adata.stock.info.get_concept_ths, row["stock_code"], '概念')}
     for _, row in stocks.iterrows()
     ]
pd.DataFrame(data).to_csv("Stock-Industries.csv", index=False, encoding='utf-8')
print("Data saved to Stock-Industries.csv")

#something wrong with the response, need to check

