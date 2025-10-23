import akshare as ak
import pandas as pd
import numpy as np
import time
import os
import glob

# --- 全局配置 ---
FEE = 0.00025
TODAY = time.strftime("%Y%m%d", time.localtime())
PORTFOLIO_FILE = "stock_data/my_stocks.csv"
OUTPUT_DIR = "stock_data"

# --- 函数定义 ---

def get_stock_info(stock_code, real_time_df=None):
    """
    获取股票名称和当前价格。
    优先从传入的DataFrame中查找，如果没有再调用API。
    """
    try:
        # 优化：如果传入了实时行情DataFrame，直接从中查找
        if real_time_df is not None:
            stock_data = real_time_df[real_time_df['代码'] == stock_code]
            if not stock_data.empty:
                stock_name = stock_data['名称'].iloc[0]
                current_price = stock_data['最新价'].iloc[0]
                # 不再打印，因为批量模式下信息会过多
                # print(f"自动获取到 {stock_name}({stock_code}) 当前价格: {current_price}")
                return stock_name, current_price
        
        # 如果没有传入DataFrame或在其中没找到，再单独调用API（主要服务于交互模式）
        print("正在单独调用API获取实时数据...")
        spot_df = ak.stock_zh_a_spot_em()
        stock_data = spot_df[spot_df['代码'] == stock_code]
        if not stock_data.empty:
            stock_name = stock_data['名称'].iloc[0]
            current_price = stock_data['最新价'].iloc[0]
            print(f"自动获取到 {stock_name}({stock_code}) 当前价格: {current_price}")
            return stock_name, current_price
        else:
            # 如果实时行情中没有，再尝试用个股信息接口（作为备用）
            info_df = ak.stock_individual_info_em(symbol=stock_code)
            stock_name = info_df[info_df['item'] == '股票简称']['value'].iloc[0]
            print(f"股票名称: {stock_name}。无法自动获取当前价，请手动输入。")
            return stock_name, None
            
    except Exception as e:
        print(f"获取股票 {stock_code} 信息失败: {e}")
        return None, None

def analyze_stock(stock_code, buy_price, quantity, startdate, current_price):
    """对单只股票进行止盈止损分析"""
    try:
        # 1. 获取历史价格
        hist_df = ak.stock_zh_a_hist(
            symbol=stock_code, period="daily", start_date=startdate, end_date=TODAY, adjust="qfq"
        )
        if hist_df.empty:
            print(f"未能获取 {stock_code} 从 {startdate} 开始的历史数据。")
            return None

        lowest_price = hist_df['最低'].min()
        highest_price = hist_df['最高'].max()

        # 2. 设置动态回撤比例
        if 0 < current_price < 10:
            X = 6
        elif 10 <= current_price < 20:
            X = 5
        else: # 20元及以上
            X = 4
        print(f"动态回撤比例: {X}%")

        # 3. 计算核心指标
        float_profit_or_loss = (current_price - buy_price) * quantity
        result = {
            '股票代码': stock_code, '买入价格': buy_price, '买入日期': startdate,
            '当前价格': current_price, '持仓数量': quantity, '最低价格': lowest_price,
            '最高价格': highest_price, '当前盈亏': float_profit_or_loss, '信号': '无'
        }

        if float_profit_or_loss > 0:
            # 浮盈情况
            highest_float_profit = (highest_price - buy_price) * quantity
            stop_earn_profit = highest_float_profit * 0.9
            stop_earn_price = buy_price + stop_earn_profit / quantity
            
            print(f"当前浮盈: {float_profit_or_loss:.2f}, 最高浮盈: {highest_float_profit:.2f}")
            print(f"目标浮盈: {stop_earn_profit:.2f}, 止盈价格: {stop_earn_price:.2f}")
            
            signal = "已触发止盈" if current_price >= stop_earn_price else "未触发止盈"
            result.update({
                '止盈价格': stop_earn_price, '信号': signal
            })
        else:
            # 浮亏情况
            stop_loss_price = buy_price * (1 - X / 100)
            print(f"当前浮亏: {abs(float_profit_or_loss):.2f}, 止损价格: {stop_loss_price:.2f}")

            signal = "已触发止损" if current_price <= stop_loss_price else "未触发止损"
            add_price = buy_price * (1 - (X / 2) / 100)
            result.update({
                '止损价格': stop_loss_price - add_price * quantity * FEE, '信号': signal, '加仓价格': add_price - add_price * quantity * FEE 
            })
        
        print(f"分析结果: {result['信号']}")
        return pd.DataFrame([result])

    except Exception as e:
        print(f"分析股票 {stock_code} 时发生错误: {e}")
        return None

def merge_all_stock_files():
    """合并所有股票分析文件"""
    csv_files = glob.glob(os.path.join(OUTPUT_DIR, "Stock-Earn-or-Loss-*.csv"))
    if not csv_files:
        return
        
    merged_df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
    # 统一列顺序，方便查看
    cols = ['股票代码', '股票名称', '买入价格', '买入日期', '当前价格', '持仓数量', '当前盈亏', 
            '信号', '止盈价格', '止损价格', '加仓价格', '最高价格', '最低价格']
    merged_df = merged_df.reindex(columns=[c for c in cols if c in merged_df.columns])
    
    merged_df.to_csv(os.path.join(OUTPUT_DIR, "Stocks-Combined-Analysis.csv"), index=False, encoding='utf-8-sig')
    print("\n所有股票分析结果已合并更新至 Stocks-Combined-Analysis.csv")

def run_interactive_mode():
    """交互模式，手动输入单只股票信息"""
    while True:
        stock_code = input("\n请输入股票代码 (输入 q 退出): ")
        if stock_code.lower() == 'q':
            break

        stock_name, auto_price = get_stock_info(stock_code)
        if not stock_name:
            continue

        try:
            buy_price = float(input("买入价格: "))
            current_price = auto_price if auto_price is not None else float(input("当前价格: "))
            quantity = int(input("持仓数量: "))
            startdate = input("买入日期 (格式 YYYYMMDD): ")
        except ValueError:
            print("输入无效，价格和数量必须是数字。请重新输入。")
            continue

        result_df = analyze_stock(stock_code, buy_price, quantity, startdate, current_price)
        
        if result_df is not None:
            # 补充股票名称并保存
            result_df.insert(1, '股票名称', stock_name)
            output_path = os.path.join(OUTPUT_DIR, f"Stock-Earn-or-Loss-{stock_code}.csv")
            result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"分析结果已保存至 {output_path}")
            merge_all_stock_files()

def run_batch_mode():
    """批量模式，从CSV文件读取持仓列表"""
    if not os.path.exists(PORTFOLIO_FILE):
        print(f"未找到持仓文件 {PORTFOLIO_FILE}。")
        # 创建一个示例文件
        print("已为您创建一个示例文件，请填写您的持仓信息后重新运行。")
        sample_df = pd.DataFrame([
            {"stock_code": "000001", "buy_price": 10.0, "quantity": 100, "startdate": "20230101"}
        ])
        sample_df.to_csv(PORTFOLIO_FILE, index=False, encoding='utf-8-sig')
        return

    portfolio_df = pd.read_csv(PORTFOLIO_FILE, dtype={'stock_code': str, 'startdate': str})
    print(f"成功读取 {len(portfolio_df)} 条持仓记录。")

    # --- API调用优化关键点 ---
    # 在循环外，一次性获取所有A股的实时行情数据
    print("\n正在获取所有A股实时行情数据，请稍候...")
    try:
        all_stocks_spot_df = ak.stock_zh_a_spot_em()
        print("实时行情数据获取成功！")
    except Exception as e:
        print(f"获取全部实时行情失败: {e}。将尝试为每只股票单独获取。")
        all_stocks_spot_df = None
    # --- 优化结束 ---

    for index, row in portfolio_df.iterrows():
        stock_code = row['stock_code']
        print(f"\n--- 正在分析 {stock_code} ---")
        
        # 将预先获取的DataFrame传入，避免重复API调用
        stock_name, current_price = get_stock_info(stock_code, real_time_df=all_stocks_spot_df)
        
        if not stock_name or current_price is None:
            print(f"跳过股票 {stock_code} 因为无法获取信息。")
            continue
        
        result_df = analyze_stock(
            stock_code, row['buy_price'], row['quantity'], row['startdate'], current_price
        )
        
        if result_df is not None:
            result_df.insert(1, '股票名称', stock_name)
            output_path = os.path.join(OUTPUT_DIR, f"Stock-Earn-or-Loss-{stock_code}.csv")
            result_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"分析结果已保存至 {output_path}")
    
    merge_all_stock_files()


# --- 主程序入口 ---
if __name__ == "__main__":
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    mode = input("请选择运行模式: \n 1: 交互模式 (手动输入)\n 2: 批量模式 (从 my_stocks.csv 读取)\n请输入 1 或 2: ")
    
    if mode == '1':
        run_interactive_mode()
    elif mode == '2':
        run_batch_mode()
    else:
        print("无效的选择。")

    print("\n程序结束！")