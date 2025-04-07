#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
日线数据获取器示例
演示如何使用BaseFetcher基类实现一个完整的股票日线数据获取器
"""
import os
import sys
import pandas as pd
import numpy as np
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union, Tuple

# 导入基类
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from core.base_fetcher import BaseFetcher
from core.config_manager import config_manager
from core.mongodb_handler import mongodb_handler
from core import utils

class DailyFetcherExample(BaseFetcher):
    """
    股票日线数据获取器示例
    演示如何从Tushare获取股票日线行情并保存到MongoDB
    """
    
    def __init__(self, api_name="daily", config_path=None, silent=False):
        """
        初始化日线数据获取器
        
        Args:
            api_name: 接口名称，默认为'daily'
            config_path: 配置文件路径
            silent: 是否静默模式
        """
        super().__init__(api_name=api_name, config_path=config_path, silent=silent)
        
        # 获取tushare配置
        self.tushare_config = config_manager.get_tushare_config()
        
        # 加载tushare
        try:
            import tushare as ts
            self.ts = ts
            ts.set_token(self.tushare_config.get('token', ''))
            self.pro = ts.pro_api()
            self.logger.info("Tushare API初始化成功")
        except Exception as e:
            self.logger.error(f"Tushare初始化失败: {str(e)}")
            self.pro = None
            
        # 设置批量获取参数
        self.batch_size = 1000  # 每批次获取的股票数量
        
    def fetch_data(self, trade_date=None, ts_code=None, start_date=None, end_date=None, **kwargs):
        """
        从Tushare获取股票日线行情数据
        
        Args:
            trade_date: 交易日期，YYYYMMDD格式，二选一
            ts_code: 股票代码，如'000001.SZ'
            start_date: 开始日期，YYYYMMDD格式，与end_date同时使用
            end_date: 结束日期，YYYYMMDD格式，与start_date同时使用
            **kwargs: 其他参数
            
        Returns:
            pd.DataFrame: 获取的数据
        """
        if self.pro is None:
            self.logger.error("Tushare API未初始化")
            return pd.DataFrame()
            
        try:
            # 构建查询参数
            params = {}
            
            if ts_code:
                params['ts_code'] = ts_code
                
            if trade_date:
                params['trade_date'] = trade_date
            elif start_date and end_date:
                params['start_date'] = start_date
                params['end_date'] = end_date
            else:
                # 默认获取前一个交易日数据
                yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
                params['trade_date'] = yesterday
                    
            # 查询数据
            self.logger.debug(f"正在从Tushare获取日线数据，参数：{params}")
            df = self.pro.daily(**params)
            
            # 添加延时，避免频繁请求
            time.sleep(0.6)
            
            if df is None or df.empty:
                self.logger.warning(f"未获取到数据，参数: {params}")
                return pd.DataFrame()
                
            self.logger.info(f"成功获取 {len(df)} 条日线数据")
            return df
            
        except Exception as e:
            self.logger.error(f"获取数据失败: {str(e)}")
            return pd.DataFrame()
            
    def process_data(self, data):
        """
        处理获取的原始数据
        
        Args:
            data: 原始数据
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        if data.empty:
            return data
            
        try:
            # 转换数值列
            numeric_cols = ['open', 'high', 'low', 'close', 'pre_close', 
                           'change', 'pct_chg', 'vol', 'amount']
            
            for col in numeric_cols:
                if col in data.columns:
                    data[col] = pd.to_numeric(data[col], errors='coerce')
                    
            # 确保日期列是字符串格式
            if 'trade_date' in data.columns:
                data['trade_date'] = data['trade_date'].astype(str)
                
            # 计算是否涨停和跌停
            if all(col in data.columns for col in ['close', 'pre_close']):
                # 计算涨跌幅度（百分比）
                data['pct_change'] = data['close'] / data['pre_close'] - 1
                
                # 涨停（通常为10%）和跌停（通常为-10%）
                # 注：科创板、创业板等涨跌幅限制可能不同，这里简化处理
                data['is_limit_up'] = data['pct_change'] > 0.099  # 接近10%
                data['is_limit_down'] = data['pct_change'] < -0.099  # 接近-10%
                
            # 添加更新时间字段
            data['update_time'] = datetime.now().strftime('%Y%m%d%H%M%S')
            
            return data
            
        except Exception as e:
            self.logger.error(f"处理数据失败: {str(e)}")
            return data
            
    def get_collection_name(self):
        """
        获取数据存储的集合名称
        
        Returns:
            str: 集合名称
        """
        return "stock_daily"
        
    def get_stock_list(self):
        """
        获取股票列表
        
        Returns:
            List[str]: 股票代码列表
        """
        try:
            if self.pro is None:
                self.logger.error("Tushare API未初始化")
                return []
                
            # 查询股票列表
            df = self.pro.stock_basic(exchange='', list_status='L', 
                              fields='ts_code,symbol,name,area,industry,list_date')
            
            if df is not None and not df.empty:
                return df['ts_code'].tolist()
            else:
                return []
                
        except Exception as e:
            self.logger.error(f"获取股票列表失败: {str(e)}")
            return []
            
    def _run_recent_mode(self):
        """
        获取最近交易日的所有股票数据
        
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.info("运行最近数据获取模式")
        
        # 获取最近交易日期
        trade_date = utils.get_last_trade_date()
        
        # 获取股票列表
        stock_list = self.get_stock_list()
        
        if not stock_list:
            return {
                'status': 'error',
                'message': '无法获取股票列表'
            }
            
        self.logger.info(f"获取到 {len(stock_list)} 只股票")
        
        # 分批获取数据
        total_saved = 0
        batches = list(utils.chunks(stock_list, self.batch_size))
        
        for i, batch in enumerate(batches):
            self.logger.info(f"处理批次 {i+1}/{len(batches)}, 包含 {len(batch)} 只股票")
            
            # 构建批量获取参数
            batch_str = ",".join(batch)
            
            # 获取数据
            data = self.fetch_data(trade_date=trade_date, ts_code=batch_str)
            
            if not data.empty:
                # 处理数据
                processed_data = self.process_data(data)
                
                # 保存数据
                saved = self.save_data(processed_data)
                total_saved += saved
                
                self.logger.info(f"批次 {i+1} 获取成功, 保存 {saved} 条记录")
            else:
                self.logger.warning(f"批次 {i+1} 无数据获取")
                
        return {
            'status': 'success',
            'saved_count': total_saved,
            'message': f"成功获取并保存 {total_saved} 条 {trade_date} 的股票日线数据"
        }
        
    def _run_date_mode(self, date):
        """
        获取指定日期的所有股票数据
        
        Args:
            date: 指定日期
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.info(f"运行指定日期数据获取模式: {date}")
        
        # 获取股票列表
        stock_list = self.get_stock_list()
        
        if not stock_list:
            return {
                'status': 'error',
                'message': '无法获取股票列表'
            }
            
        self.logger.info(f"获取到 {len(stock_list)} 只股票")
        
        # 分批获取数据
        total_saved = 0
        batches = list(utils.chunks(stock_list, self.batch_size))
        
        for i, batch in enumerate(batches):
            self.logger.info(f"处理批次 {i+1}/{len(batches)}, 包含 {len(batch)} 只股票")
            
            # 构建批量获取参数
            batch_str = ",".join(batch)
            
            # 获取数据
            data = self.fetch_data(trade_date=date, ts_code=batch_str)
            
            if not data.empty:
                # 处理数据
                processed_data = self.process_data(data)
                
                # 保存数据
                saved = self.save_data(processed_data)
                total_saved += saved
                
                self.logger.info(f"批次 {i+1} 获取成功, 保存 {saved} 条记录")
            else:
                self.logger.warning(f"批次 {i+1} 无数据获取")
                
        return {
            'status': 'success',
            'saved_count': total_saved,
            'message': f"成功获取并保存 {total_saved} 条 {date} 的股票日线数据"
        }
        
    def _run_date_range_mode(self, start_date, end_date):
        """
        获取日期范围内的数据
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.info(f"运行日期范围数据获取模式: {start_date} 至 {end_date}")
        
        # 获取股票列表
        stock_list = self.get_stock_list()
        
        if not stock_list:
            return {
                'status': 'error',
                'message': '无法获取股票列表'
            }
            
        self.logger.info(f"获取到 {len(stock_list)} 只股票")
        
        # 分批获取数据
        total_saved = 0
        
        # 按日期和股票批次分批获取
        dates = utils.get_workdays(start_date, end_date)
        self.logger.info(f"将获取 {len(dates)} 个交易日的数据")
        
        for date in dates:
            self.logger.info(f"处理日期: {date}")
            
            # 按股票批次处理
            batches = list(utils.chunks(stock_list, self.batch_size))
            
            for i, batch in enumerate(batches):
                self.logger.info(f"处理批次 {i+1}/{len(batches)}, 包含 {len(batch)} 只股票")
                
                # 构建批量获取参数
                batch_str = ",".join(batch)
                
                # 获取数据
                data = self.fetch_data(trade_date=date, ts_code=batch_str)
                
                if not data.empty:
                    # 处理数据
                    processed_data = self.process_data(data)
                    
                    # 保存数据
                    saved = self.save_data(processed_data)
                    total_saved += saved
                    
                    self.logger.info(f"日期 {date} 批次 {i+1} 获取成功, 保存 {saved} 条记录")
                else:
                    self.logger.warning(f"日期 {date} 批次 {i+1} 无数据获取")
                    
        return {
            'status': 'success',
            'saved_count': total_saved,
            'message': f"成功获取并保存 {total_saved} 条从 {start_date} 至 {end_date} 的股票日线数据"
        }
        
    def _run_stock_mode(self, ts_code, start_date, end_date):
        """
        获取指定股票在日期范围内的数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.info(f"运行股票数据获取模式: {ts_code} 从 {start_date} 至 {end_date}")
        
        # 获取数据
        data = self.fetch_data(ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        if data.empty:
            return {
                'status': 'error',
                'message': f'未获取到股票 {ts_code} 的数据'
            }
            
        # 处理数据
        processed_data = self.process_data(data)
        
        # 保存数据
        saved_count = self.save_data(processed_data)
        
        return {
            'status': 'success',
            'saved_count': saved_count,
            'message': f"成功获取并保存 {saved_count} 条 {ts_code} 的日线数据"
        }
        
    def _run_full_mode(self):
        """
        获取全量历史数据
        
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.info("运行全量历史数据获取模式")
        
        # 获取股票列表
        stock_list = self.get_stock_list()
        
        if not stock_list:
            return {
                'status': 'error',
                'message': '无法获取股票列表'
            }
            
        # 设置默认起始日期（从1990年开始）
        start_date = '19900101'
        end_date = datetime.now().strftime('%Y%m%d')
        
        # 构建任务列表（按股票获取）
        tasks = []
        for ts_code in stock_list:
            task = {
                'ts_code': ts_code,
                'start_date': start_date,
                'end_date': end_date
            }
            tasks.append(task)
            
        self.logger.info(f"创建了 {len(tasks)} 个获取任务")
        
        # 并行执行任务
        total_saved = self.parallel_fetch(tasks, thread_count=5)
        
        return {
            'status': 'success',
            'saved_count': total_saved,
            'message': f"成功获取并保存 {total_saved} 条全量历史日线数据"
        }


# 如果直接运行此脚本，则执行示例
if __name__ == "__main__":
    # 设置日志级别
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 创建获取器实例
    fetcher = DailyFetcherExample(silent=False)
    
    # 默认执行最近模式
    result = fetcher.run(mode='recent')
    
    # 输出结果
    if result.get('status') == 'success':
        print(f"运行成功: {result.get('message')}")
        print(f"共保存 {result.get('saved_count')} 条记录")
        print(f"执行时间: {result.get('execution_time', 0):.2f} 秒")
    else:
        print(f"运行失败: {result.get('message')}")