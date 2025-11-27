import akshare as ak
import pandas as pd
import numpy as np
import time
import os
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import warnings
import random
warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

class StockAnalyzer:
    def __init__(self):
        self.today = time.strftime("%Y%m%d", time.localtime())
        self.data_dir = "stock_data"  # 修改为 stock_data
        os.makedirs(self.data_dir, exist_ok=True)
        
        # 配置请求头
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        # 配置访问间隔时间（秒）
        self.min_delay = 0.5  # 最小延迟
        self.max_delay = 1.5  # 最大延迟
        
        
    def random_delay(self):
        """随机延迟，避免频繁请求"""
        delay = random.uniform(self.min_delay, self.max_delay)
        print(f"等待 {delay:.1f} 秒...")
        time.sleep(delay)
    
    def fetch_real_time_data(self):
        """获取实时股票数据"""
        print("正在获取实时股票数据...")
        try:
            df = ak.stock_zh_a_spot_em()
            filename = f"{self.data_dir}/Real-Time-Stock-Flow-{self.today}.csv"
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"实时数据已保存至 {filename}")
            self.random_delay()  # 添加延迟
            return df
        except Exception as e:
            print(f"获取实时股票数据失败: {e}")
            return None
    
    def fetch_chip_data(self, codes=["600900", "600027", "601728"]):
        """获取筹码分布数据"""
        print("正在获取筹码分布数据...")
        chip_data = {}
        for code in codes:
            print(f"正在获取 {code} 的筹码数据...")
            try:
                df = ak.stock_cyq_em(symbol=code, adjust="qfq")
                filename = f"{self.data_dir}/Stock-Chips-{code}-{self.today}.csv"
                df.to_csv(filename, index=False, encoding='utf-8')
                chip_data[code] = df
                print(f"{code} 筹码数据已保存至 {filename}")
                self.random_delay()  # 添加延迟
            except Exception as e:
                print(f"获取 {code} 筹码数据失败: {e}")
        return chip_data
    
    def fetch_main_funds_data(self):
        """获取主力资金流向数据"""
        print("正在获取主力资金流向数据...")
        try:
            df = ak.stock_main_fund_flow(symbol="沪深A股")
            filename = f"{self.data_dir}/Stock-Main-Funds-{self.today}.csv"
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"主力资金数据已保存至 {filename}")
            self.random_delay()  # 添加延迟
            return df
        except Exception as e:
            print(f"获取主力资金数据失败: {e}")
            return None
    
    def fetch_historical_data(self, symbol, period=30):
        """获取个股历史数据"""
        print(f"正在获取 {symbol} 的历史数据...")
        try:
            # 获取最近period天的数据
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", 
                                   start_date=(datetime.now() - timedelta(days=period*2)).strftime("%Y%m%d"),
                                   end_date=self.today, adjust="qfq")
            self.random_delay()  # 添加延迟
            return df
        except Exception as e:
            print(f"获取 {symbol} 历史数据失败: {e}")
            return None
    
    def calculate_moving_averages(self, df, windows=[5, 20, 60]):
        """计算移动平均线"""
        for window in windows:
            df[f'MA_{window}'] = df['收盘'].rolling(window=window).mean()
        return df
    #短期：5日均线
    #中期：20日均线
    #长期：60日均线
    #买卖点：5日均线向上突破20日/60日均线为买入信号，5日均线向下跌破20日/60日均线为卖出信号
    
    def calculate_price_change_rate(self, df, period=20):
        """计算价格变化率"""
        df[f'ROC_{period}'] = (df['收盘'] - df['收盘'].shift(period)) / df['收盘'].shift(period) * 100
        return df
    #ROC = (当前收盘价 - period周期前的收盘价) / period周期前的收盘价 × 100%
    #上涨/下跌幅度：ROC > 0 为上涨，ROC < 0 为下跌
    
    def calculate_volatility(self, df, window=20):
        """计算波动率"""
        df['volatility'] = df['涨跌幅'].rolling(window=window).std() * (252 ** 0.5)
        return df
    #波动率 = 涨跌幅的标准差 × √252
    #波动率越高，风险越大，反之亦然
    
    def volume_price_analysis(self, df):
        """量价关系分析"""
        # 计算成交量移动平均
        df['Volume_MA'] = df['成交量'].rolling(window=20).mean()
        
        # 判断量价关系
        price_change = df['收盘'].pct_change()
        volume_change = df['成交量'].pct_change()
        # pct_change() 计算当前值相对于前一个值的百分比变化
        
        # 量价齐升/齐跌信号
        df['量价齐升'] = (price_change > 0) & (volume_change > 0)
        df['量价背离'] = (price_change > 0) & (volume_change < 0)
        # 量价齐升：价格上涨且成交量增加 股价可能继续上涨
        # 量价背离：价格上涨但成交量减少 股价可能回调
        
        return df  # 添加返回语句
    
    def identify_trend(self, df):
        """判断趋势方向"""
        # 添加更详细的调试信息
        if df is None:
            print("数据为空")
            return "数据不足"
        
        print(f"数据行数: {len(df)}")
    
        # 检查必要的列是否存在
        required_columns = ['MA_5', 'MA_20', 'MA_60']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"缺少列: {missing_columns}")
            return "数据不足"
    
        # 检查非空数据数量
        non_null_counts = df[required_columns].count()
        print(f"非空数据数量: {non_null_counts.to_dict()}")
        
        # 只检查最近几行数据是否为空（用于趋势判断）
        recent_data = df[required_columns].iloc[-3:]  # 检查最近3行
        if recent_data.isnull().any().any():
            print("最近数据存在空值，无法判断趋势")
            return "数据不足"
        
        if len(df) < 3:  # 至少需要3行数据来比较最新的均线
            return "数据不足"
        
        # 使用iloc[-1]确保获取最后一行有效数据
        last_row = df.iloc[-1]
        if last_row['MA_5'] > last_row['MA_20'] > last_row['MA_60']:
            return "上升趋势"
        elif last_row['MA_5'] < last_row['MA_20'] < last_row['MA_60']:
            return "下降趋势"
        else:
            return "震荡趋势"
    
    def analyze_chip_distribution_trend(self, chip_files):
        """分析筹码分布变化趋势"""
        if not chip_files:
            return None
            
        chip_data_list = []
        for file in chip_files:
            try:
                df = pd.read_csv(file)
                date = file.split('-')[-1].split('.')[0]  # 提取日期
                df['date'] = date
                chip_data_list.append(df)
            except Exception as e:
                print(f"读取 {file} 失败: {e}")
        
        if not chip_data_list:
            return None
            
        combined_df = pd.concat(chip_data_list)
        # concat() 函数将多个 DataFrame 拼接在一起
        
        # 分析筹码集中度变化趋势
        trend_analysis = combined_df.groupby('date').agg({
            '90集中度': 'mean',
            '70集中度': 'mean',
            '获利比例': 'mean'
        }).reset_index()
        # groupby() 函数按日期分组，agg() 函数计算每个组的平均值
        # reset_index() 将dataframe的索引恢复成列
        
        return trend_analysis
    
    def analyze_fund_flow_trend(self, fund_files):
        """分析主力资金流向趋势"""
        fund_data_list = []
        for file in fund_files:
            try:
                df = pd.read_csv(file)
                date = file.split('-')[-1].split('.')[0]
                df['date'] = date
                fund_data_list.append(df)
            except Exception as e:
                print(f"读取 {file} 失败: {e}")
        
        if not fund_data_list:
            return None
            
        combined_df = pd.concat(fund_data_list)
        
        # 分析资金流入流出趋势
        trend_analysis = combined_df.groupby('date').agg({
            '主力净流入-净额': 'sum',
            '超大单净流入-净额': 'sum',
            '大单净流入-净额': 'sum'
        }).reset_index()
        
        return trend_analysis
    
    def comprehensive_analysis(self, symbol):
        """对单个股票进行综合分析"""
        print(f"\n开始对 {symbol} 进行综合分析...")
        
        # 获取历史数据
        hist_df = self.fetch_historical_data(symbol, period=60)
        if hist_df is None or hist_df.empty:
            print(f"无法获取 {symbol} 的历史数据")
            return
        
        # 技术指标计算
        hist_df = self.calculate_moving_averages(hist_df)
        hist_df = self.calculate_price_change_rate(hist_df)
        hist_df = self.calculate_volatility(hist_df)
        hist_df = self.volume_price_analysis(hist_df)
        
        # 趋势判断
        trend = self.identify_trend(hist_df)
        
        # 输出分析结果
        latest_data = hist_df.iloc[-1]
        print(f"\n{symbol} 综合分析结果:")
        print(f"当前价格: {latest_data['收盘']:.2f}")
        print(f"趋势判断: {trend}")
        print(f"20日涨跌幅: {latest_data['ROC_20']:.2f}%")
        print(f"波动率: {latest_data['volatility']:.4f}")
        print(f"量价关系: {'齐升' if latest_data['量价齐升'] else ('背离' if latest_data['量价背离'] else '正常')}")
        
        # 可视化
        self.visualize_analysis(hist_df, symbol)
    
    def visualize_analysis(self, df, symbol):
        """可视化分析结果"""
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle(f'{symbol} 趋势分析图', fontsize=16)
        
        # 价格趋势图
        axes[0,0].plot(df['日期'], df['收盘'], label='收盘价', linewidth=1)
        if 'MA_5' in df.columns:
            axes[0,0].plot(df['日期'], df['MA_5'], label='5日均线', alpha=0.7)
        if 'MA_20' in df.columns:
            axes[0,0].plot(df['日期'], df['MA_20'], label='20日均线', alpha=0.7)
        axes[0,0].set_title('价格趋势')
        axes[0,0].legend()
        axes[0,0].tick_params(axis='x', rotation=45)
        
        # 成交量趋势图
        axes[0,1].bar(df['日期'], df['成交量'], alpha=0.7)
        axes[0,1].set_title('成交量趋势')
        axes[0,1].tick_params(axis='x', rotation=45)
        
        # 涨跌幅图
        axes[1,0].plot(df['日期'], df['涨跌幅'], color='orange')
        axes[1,0].set_title('每日涨跌幅')
        axes[1,0].tick_params(axis='x', rotation=45)
        
        # 波动率图
        if 'volatility' in df.columns:
            axes[1,1].plot(df['日期'], df['volatility'], color='red')
            axes[1,1].set_title('波动率趋势')
            axes[1,1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig(f"{self.data_dir}/{symbol}-analysis-{self.today}.png", dpi=300, bbox_inches='tight')
        print(f"分析图表已保存至 {self.data_dir}/{symbol}-analysis-{self.today}.png")
        plt.show()
    
    def run_all_analysis(self):
        """运行所有分析"""
        print("开始股票综合分析系统...")
        
        # 1. 获取实时数据
        self.fetch_real_time_data()
        
        # 2. 获取筹码数据
        chip_codes = ["600900", "600027", "601728"]
        chip_data = self.fetch_chip_data(chip_codes)
        
        # 3. 获取主力资金数据
        self.fetch_main_funds_data()
        
        # 4. 对重点股票进行综合分析
        for code in chip_codes:
            self.comprehensive_analysis(code)
        
        print("\n所有分析已完成！")

# 主程序入口
if __name__ == "__main__":
    analyzer = StockAnalyzer()
    analyzer.run_all_analysis()