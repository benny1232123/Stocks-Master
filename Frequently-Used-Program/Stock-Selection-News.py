import akshare as ak
import datetime
import os
import sqlite3

# --- 配置区 ---
TODAY = datetime.date.today()
YESTERDAY = TODAY - datetime.timedelta(days=1)
DATE_STR = TODAY.strftime("%Y%m%d")
YESTERDAY_STR = YESTERDAY.strftime("%Y%m%d")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "stock_data")
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

DB_FILE = os.path.join(DATA_DIR, "news_events.db")

def get_cctv_news():
    """获取新闻联播文本"""
    print(f"正在获取 {DATE_STR} 的《新闻联播》文本...")
    try:
        df_news = ak.news_cctv(date=DATE_STR)
        df_news.to_csv(os.path.join(DATA_DIR, f"{DATE_STR}_news.csv"), index=False)
        if df_news.empty:
            raise ValueError("今日新闻为空")
        current_date = DATE_STR
    except Exception as e:
        print(f"获取今日新闻失败 ({e})，尝试获取昨日 {YESTERDAY_STR} 的新闻...")
        try:
            df_news = ak.news_cctv(date=YESTERDAY_STR)
            df_news.to_csv(os.path.join(DATA_DIR, f"{YESTERDAY_STR}_news.csv"), index=False)
            current_date = YESTERDAY_STR
        except Exception as e2:
            print(f"获取昨日新闻也失败: {e2}")
            return None, None
    
    if df_news.empty:
        return None, None
    
    print(f"成功获取 {current_date} 新闻联播，共 {len(df_news)} 条新闻。")
    return current_date, df_news

get_cctv_news()