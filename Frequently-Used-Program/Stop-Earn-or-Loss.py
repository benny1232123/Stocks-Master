import akshare as ak
import pandas as pd
import numpy as np
import time

# 添加循环
while True:
    '''0.前期准备'''
    today=time.strftime("%Y%m%d", time.localtime())
    fee=0.025

    # 输入持仓信息
    print("请输入持仓信息:")
    stock_code = input("股票代码: ")
    stock_df = ak.stock_individual_info_em(symbol=stock_code, timeout=10)
    stock_name = stock_df[stock_df['item'] == '股票简称']['value'].iloc[0]
    print(f"股票名称: {stock_name}")
    buy_price = float(input("买入价格: "))
    current_price = float(input("当前价格: "))
    quantity = int(input("持仓数量: "))
    startdate = input("买入日期：")

    '''1.设置回撤比例'''

    if 0 < current_price <10:
        X = 6

    elif 10 <= current_price < 20:
        X = 5

    elif 20 <= current_price < 30:
        X = 4 

    print(f"动态回撤比例: {X}%")

    '''2.计算浮盈/亏'''
    historical_price_df = ak.stock_zh_a_hist(
        symbol=stock_code, 
        period="daily", 
        start_date=startdate, 
        end_date=today, 
        adjust="qfq", 
        timeout=15
    )

    lowest_price_list = historical_price_df['最低'].tolist()
    lowest_price = min(lowest_price_list)
    highest_price_list = historical_price_df['最高'].tolist()
    highest_price = max(highest_price_list)
    float_profit_or_Loss = (current_price - buy_price) * quantity

    # 准备保存到CSV的数据
    result_data = {
        '股票代码': [stock_code],
        '股票名称': [stock_name],
        '买入价格': [buy_price],
        '买入日期': [startdate],
        '当前价格': [current_price],
        '持仓数量': [quantity],
        '最低价格': [lowest_price],
        '最高价格': [highest_price],
        '当前盈亏': [float_profit_or_Loss]
    }

    # 根据盈亏情况添加相应数据
    if float_profit_or_Loss>0:
        '''2.1.计算浮盈'''
        float_profit = float_profit_or_Loss
        highest_float_profit = (highest_price - buy_price) * quantity
        stop_earn_profit = highest_float_profit * 0.9
        stop_earn_price = buy_price + stop_earn_profit / quantity
        
        print(f"当前浮盈: {float_profit:.3f}")
        print(f"最高浮盈: {highest_float_profit:.3f}")
        print(f"目标浮盈：{stop_earn_profit:.3f}")
        print(f"止盈价格：{stop_earn_price:.3f}")
        
        if current_price >= stop_earn_price:
            print("已触发止盈！")
            signal = "已触发止盈"
        else:
            print("未触发止盈！")
            signal = "未触发止盈"
        
        # 添加浮盈相关数据
        result_data.update({
            '当前浮盈': [float_profit],
            '最高浮盈': [highest_float_profit],
            '目标浮盈': [stop_earn_profit],
            '止盈价格': [stop_earn_price],
            '信号': [signal]
        })


        
    else:
        '''2.2.计算浮亏'''
        float_Loss = abs(float_profit_or_Loss)
        highest_float_Loss = (buy_price - lowest_price) * quantity
        stop_loss_price = buy_price * (1 - X / 100)
        stop_loss_profit = (buy_price - stop_loss_price) * quantity
        
        print(f"当前浮亏: {float_Loss:.3f}")
        print(f"最大浮亏: {highest_float_Loss:.3f}")
        print(f"目标浮亏：{stop_loss_profit:.3f}")
        print(f"止损价格：{stop_loss_price:.3f}")
        
        if current_price <= stop_loss_price:
            print("已触发止损！")
            signal = "已触发止损"
        else:
            print("未触发止损！")
            signal = "未触发止损"
        
        # 添加浮亏相关数据
        result_data.update({
            '当前浮亏': [float_Loss],
            '最大浮亏': [highest_float_Loss],
            '目标浮亏': [stop_loss_profit],
            '止损价格': [stop_loss_price],
            '信号': [signal]
        })
    
        '''3.计算加仓价格'''
        Y=X/2
        add_price=buy_price * (1 - Y / 100)
        print("是否有加仓空间：y or n")
        answer = input()
        if answer == "y":
            print(f"加仓价格为: {add_price-fee}") 
            result_data.update({'加仓价格': [add_price-fee]})

    '''4.保存结果'''
    # 在你代码的最后部分，替换最后几行：
    result_df = pd.DataFrame(result_data)
    result_df.to_csv(f"stock_data/Stock-Earn-or-Loss-{stock_code}.csv", index=False, encoding='utf-8-sig')
    print(f"分析结果已保存至 Stock-Earn-or-Loss-{stock_code}.csv")

    # 自动合并所有股票分析文件
    def merge_all_stock_files():
        import glob
        csv_files = glob.glob("stock_data/Stock-Earn-or-Loss-*.csv")
        if csv_files:
            dataframes = []
            for file in csv_files:
                df = pd.read_csv(file, encoding='utf-8-sig')
                dataframes.append(df)
            
            merged_df = pd.concat(dataframes, ignore_index=True)
            merged_df.to_csv("stock_data/Stocks-Combined-Analysis.csv", index=False, encoding='utf-8-sig')
            print("所有股票分析结果已合并更新")

    merge_all_stock_files()
    
    # 询问是否继续
    continue_input = input("\n是否继续分析其他股票？(回车继续，输入任意字符退出): ")
    if continue_input.strip() != "":
        break

print("程序结束！")