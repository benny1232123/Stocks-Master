import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
import os
import time
import random
warnings.filterwarnings('ignore')

class BottomPredictor:
    def __init__(self):
        pass
    
    def detect_bottom_patterns(self, df):
        """检测底部形态"""
        # 计算各种技术指标
        df = self.calculate_indicators(df)
        
        # 检测底部信号
        df['bottom_signal'] = self.identify_bottom_signals(df)
        
        return df
    
    def calculate_indicators(self, df):
        """计算技术指标"""
        # RSI指标
        df['rsi'] = self.calculate_rsi(df['收盘'], 14)
        # 数值范围：0-100
        # 超买区域：RSI > 70，可能价格过高，有回调风险
        # 超卖区域：RSI < 30，可能价格过低，有反弹机会
        # 中性区域：30-70，价格相对平衡
        
        # MACD指标
        df['macd'], df['signal'], df['histogram'] = self.calculate_macd(df['收盘'])
        # MACD线：快线（12日EMA）- 慢线（26日EMA）
        #信号线（Signal）：MACD线的9日EMA
        #柱状图（Histogram）：MACD线 - 信号线
        #金叉：MACD线上穿信号线，买入信号
        #死叉：MACD线下穿信号线，卖出信号
        #零轴上方：多头市场
        #零轴下方：空头市场
        #柱状图变化：反映趋势强弱变化

        # 布林带
        df['upper_band'], df['middle_band'], df['lower_band'] = self.calculate_bollinger_bands(df['收盘'])
        # 布林带上轨：中轨 + 2倍20日标准差
        # 布林带中轨：20日简单移动平均线
        # 布林带下轨：中轨 - 2倍20日标准差
        # 价格触及上轨：可能超买，回调风险
        # 价格触及下轨：可能超卖，反弹机会
        
        # 成交量比率
        df['volume_ratio'] = df['成交量'] / df['成交量'].rolling(20).mean()
        
        return df
        # 当前成交量 / 过去N日平均成交量
        # 比率>1: 成交量高于平均成交量，市场关注度高
        # 比率<1: 成交量低于平均成交量，市场关注度低
        # 比率>1.5: 成交量显著放大，可能有重要信息或事件影响
        # 比率<0.5: 成交量显著缩量，市场缺乏方向

    def calculate_rsi(self, prices, window=14):
        """计算RSI指标"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    def calculate_macd(self, prices, fast=12, slow=26, signal=9):
        """计算MACD指标"""
        ema_fast = prices.ewm(span=fast).mean()
        ema_slow = prices.ewm(span=slow).mean()
        macd = ema_fast - ema_slow
        signal_line = macd.ewm(span=signal).mean()
        histogram = macd - signal_line
        return macd, signal_line, histogram
    
    def calculate_bollinger_bands(self, prices, window=20, num_std=2):
        """计算布林带"""
        middle_band = prices.rolling(window=window).mean()
        std = prices.rolling(window=window).std()
        upper_band = middle_band + (std * num_std)
        lower_band = middle_band - (std * num_std)
        return upper_band, middle_band, lower_band
    
    def identify_bottom_signals(self, df):
        """识别底部信号"""
        signals = pd.Series(0, index=df.index)
        
        # RSI超卖信号 (通常<30为超卖)
        rsi_oversold = (df['rsi'] < 30) & (df['rsi'].shift(1) >= 30)
        
        # MACD金叉信号
        macd_cross = (df['macd'] > df['signal']) & (df['macd'].shift(1) <= df['signal'].shift(1))
        
        # 价格触及布林带下轨
        price_touch_lower = df['收盘'] <= df['lower_band']
        
        # 成交量放大
        volume_increase = df['volume_ratio'] > 1.5
        
        # 综合信号判断
        signals = (rsi_oversold | macd_cross | price_touch_lower) & volume_increase
        
        return signals.astype(int)

# 新增：数据获取类
class DataFetcher:
    def __init__(self):
        self.today = time.strftime("%Y%m%d", time.localtime())
        self.data_dir = "stock_data"
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
        
    def fetch_stock_data(self, symbol, period=120):

        try:
            print(f"正在获取 {symbol} 的历史数据...")
            # 获取最近period天的数据
            start_date = (datetime.now() - timedelta(days=period*2)).strftime("%Y%m%d")
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", 
                                   start_date=start_date,
                                   end_date=self.today, adjust="qfq")
            
            if df.empty:
                print(f"获取 {symbol} 数据失败：返回空数据")
                return None
                
            self.random_delay()  # 添加延迟
            return df
        
        except Exception as e:
            print(f"获取 {symbol} 历史数据失败: {e}")
            return None

# 新增：结果分析和展示类
class ResultAnalyzer:
    def __init__(self):
        pass
    
    def analyze_bottom_signals(self, df, symbol):
        """
        分析底部信号并输出结果
        :param df: 包含信号的数据框
        :param symbol: 股票代码
        """
        if df is None or df.empty:
            print(f"{symbol} 数据为空，无法分析")
            return
            
        # 获取有底部信号的记录
        bottom_signals = df[df['bottom_signal'] == 1]
        
        print(f"\n======== {symbol} 底部信号分析结果 ========")
        print(f"分析期间: {df['日期'].iloc[0]} 至 {df['日期'].iloc[-1]}")
        print(f"总交易日数: {len(df)}")
        print(f"检测到底部信号次数: {len(bottom_signals)}")
        
        if len(bottom_signals) > 0:
            print("\n检测到的底部信号:")
            for idx, row in bottom_signals.iterrows():
                print(f"  日期: {row['日期']}")
                print(f"    收盘价: {row['收盘']:.2f}")
                print(f"    RSI: {row['rsi']:.2f}")
                print(f"    MACD: {row['macd']:.4f}")
                print(f"    成交量比率: {row['volume_ratio']:.2f}")
                print(f"    布林带位置: 下轨={row['lower_band']:.2f}, 当前价={row['收盘']:.2f}")
                print()
        else:
            print("在分析期间未检测到明显的底部信号")
            
        # 输出最近的数据状态
        if not df.empty:
            latest = df.iloc[-1]
            print(f"\n最新数据 ({latest['日期']}):")
            print(f"  收盘价: {latest['收盘']:.2f}")
            print(f"  RSI: {latest['rsi']:.2f} ({'超卖' if latest['rsi'] < 30 else '超买' if latest['rsi'] > 70 else '中性'})")
            print(f"  MACD: {latest['macd']:.4f}")
            print(f"  信号线: {latest['signal']:.4f}")
            print(f"  成交量比率: {latest['volume_ratio']:.2f} ({'放量' if latest['volume_ratio'] > 1.5 else '缩量' if latest['volume_ratio'] < 0.5 else '正常'})")
            print(f"  布林带位置: 上轨={latest['upper_band']:.2f}, 中轨={latest['middle_band']:.2f}, 下轨={latest['lower_band']:.2f}")
            
            # 判断当前是否有底部信号
            if latest['bottom_signal'] == 1:
                print("  >>> 当前可能为底部信号 <<<")
            else:
                print("  >>> 当前无底部信号 <<<")
        
        return bottom_signals


# 新增：主程序类
class StockBottomAnalyzer:
    def __init__(self):
        self.predictor = BottomPredictor()
        self.fetcher = DataFetcher()
        self.analyzer = ResultAnalyzer()
        
        self.stock_list = []  
    
    def analyze_single_stock(self, symbol):
        """
        分析单只股票
        :param symbol: 股票代码
        """
        # 获取数据
        df = self.fetcher.fetch_stock_data(symbol)
        if df is None:
            return
            
        # 检测底部形态
        df_with_signals = self.predictor.detect_bottom_patterns(df)
        
        # 分析结果
        self.bottom_signals = self.analyzer.analyze_bottom_signals(df_with_signals, symbol)
        

        
        return df_with_signals
    
    def run_analysis(self):
        """
        运行完整分析
        """
        print("开始股票底部形态分析...")
        print("=" * 50)
        
        results = {}
        for symbol in self.stock_list:
            try:
                result = self.analyze_single_stock(symbol)
                results[symbol] = result
                print("-" * 50)
            except Exception as e:
                print(f"分析 {symbol} 时出错: {e}")
                print("-" * 50)
                continue
                
        print("\n分析完成！")
        return results

# 主程序入口
if __name__ == "__main__":
    # 创建分析器实例
    analyzer = StockBottomAnalyzer()
    
    # 可以修改要分析的股票列表
    analyzer.stock_list = ["600900", "600027", "601728" ]  # 自定义股票列表
    
    # 运行分析
    results = analyzer.run_analysis()