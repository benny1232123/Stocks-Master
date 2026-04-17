import akshare as ak
import datetime
import os
import sqlite3
from pathlib import Path
import pandas as pd

# --- 配置区 ---
TODAY = datetime.date.today()
YESTERDAY = TODAY - datetime.timedelta(days=1)
DATE_STR = TODAY.strftime("%Y%m%d")
YESTERDAY_STR = YESTERDAY.strftime("%Y%m%d")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "stock_data")
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

DB_FILE = os.path.join(DATA_DIR, "news_events.db")
DATA_PATH = Path(DATA_DIR)

def get_cctv_news():
    """获取新闻联播文本"""
    print(f"正在获取 {DATE_STR} 的《新闻联播》文本...")
    for current_date in (DATE_STR, YESTERDAY_STR):
        local_path = DATA_PATH / f"{current_date}_news.csv"
        if local_path.exists():
            try:
                df_news = pd.read_csv(local_path, encoding="utf-8-sig")
                if not df_news.empty:
                    print(f"成功读取本地新闻联播 CSV: {local_path}")
                    return current_date, df_news
            except Exception as exc:
                print(f"读取本地新闻联播失败: {local_path}，原因: {exc}")

        try:
            df_news = ak.news_cctv(date=current_date)
            df_news.to_csv(local_path, index=False, encoding="utf-8-sig")
            if df_news.empty:
                raise ValueError("新闻为空")
            print(f"成功获取 {current_date} 新闻联播，共 {len(df_news)} 条新闻。")
            return current_date, df_news
        except Exception as exc:
            print(f"获取 {current_date} 新闻失败: {exc}")
    
    return None, None

if __name__ == "__main__":
    get_cctv_news()