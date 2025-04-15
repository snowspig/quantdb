#!/usr/bin/env python
"""
基本面指标数据获取器 - 从Tushare获取股票基本面指标数据并保存到MongoDB

该脚本用于从湘财Tushare获取股票的基本面指标数据，包括市盈率、市净率等关键财务指标，
通过继承BaseFetcher类实现，可被gorepe.py调度执行。

使用方法：
    python basic_indicator_fetcher.py --recent  # 获取最近一个交易日数据
    python basic_indicator_fetcher.py --date 20230601  # 获取特定日期数据
    python basic_indicator_fetcher.py --start-date 20230101 --end-date 20230601  # 获取日期范围数据
    python basic_indicator_fetcher.py --full  # 获取所有历史数据
"""
import os
import sys
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

# 添加项目根目录到路径
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

# 导入核心模块
from core import BaseFetcher
from core import config_manager, mongodb_handler, network_manager
import pymongo

class BasicIndicatorFetcher(BaseFetcher):
    """
    股票基本面指标数据获取器类
    
    获取股票的基本面指标数据，包括市盈率、市净率、总市值等关键财务指标
    """
    
    def __init__(self, config_path=None, silent=False):
        """初始化基本面指标数据获取器
        
        Args:
            config_path: 配置文件路径，默认为None，使用系统默认配置
            silent: 是否静默模式，默认为False
        """
        # 调用父类初始化方法，接口名称为'daily_basic'
        super().__init__('daily_basic', config_path, silent)
        
        # 设置数据获取的批量大小
        self.batch_size = 5000
        
        # 设置集合索引字段
        self.index_fields = ['ts_code', 'trade_date']
        
        # 初始化已获取的股票代码集合
        self.fetched_stocks = set()
        
        # 初始化初始日期，用于全量数据获取
        self.start_date = '20100101'  # 默认从2010年开始获取数据
        
        self.logger.info(f"基本面指标数据获取器初始化完成，批量大小: {self.batch_size}")
        
    def fetch_data(self, **kwargs) -> pd.DataFrame:
        """
        从Tushare获取基本面指标数据
        
        Args:
            **kwargs: 查询参数，支持以下参数：
                - ts_code: 股票代码
                - trade_date: 交易日期
                - start_date: 开始日期
                - end_date: 结束日期
                
        Returns:
            pd.DataFrame: 获取的基本面数据
        """
        try:
            # 从接口配置中获取API参数
            api_args = {}
            
            # 处理股票代码参数
            if 'ts_code' in kwargs and kwargs['ts_code']:
                api_args['ts_code'] = kwargs['ts_code']
                
            # 处理日期相关参数
            if 'trade_date' in kwargs and kwargs['trade_date']:
                api_args['trade_date'] = kwargs['trade_date']
            
            if 'start_date' in kwargs and kwargs['start_date']:
                api_args['start_date'] = kwargs['start_date']
                
            if 'end_date' in kwargs and kwargs['end_date']:
                api_args['end_date'] = kwargs['end_date']
            
            # 如果是在多WAN模式下运行，使用网络管理器获取客户端
            if self.wan_enabled and network_manager.is_enabled():
                self.logger.info(f"使用多WAN模式获取基本面指标数据: {api_args}")
                # 获取一个可用的Tushare客户端
                ts_client = network_manager.get_tushare_client()
                # 调用API
                result = ts_client.query(self.api_name, fields=self.field_names, **api_args)
            else:
                # 使用标准模式获取数据
                self.logger.info(f"使用标准模式获取基本面指标数据: {api_args}")
                # 从配置管理器获取Tushare客户端
                from data_fetcher.tushare_client import TushareClient
                ts_client = TushareClient()
                # 调用API
                result = ts_client.query(self.api_name, fields=self.field_names, **api_args)
            
            # 检查结果
            if result is None or not isinstance(result, pd.DataFrame) or result.empty:
                self.logger.warning(f"未获取到基本面指标数据: {api_args}")
                return pd.DataFrame()
            
            self.logger.info(f"成功获取 {len(result)} 条基本面指标数据")
            return result
            
        except Exception as e:
            self.logger.error(f"获取基本面指标数据失败: {str(e)}")
            return pd.DataFrame()
    
    def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        处理获取的基本面指标数据
        
        Args:
            data: 原始数据
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        if data.empty:
            return data
        
        try:
            # 确保日期字段是字符串格式
            if 'trade_date' in data.columns:
                data['trade_date'] = data['trade_date'].astype(str)
            
            # 处理可能的NaN值
            numeric_columns = ['pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'dv_ratio', 
                              'dv_ttm', 'total_mv', 'circ_mv']
            
            for col in numeric_columns:
                if col in data.columns:
                    # 将无效值替换为None
                    data[col] = data[col].replace([float('inf'), -float('inf')], None)
            
            # 添加创建时间字段
            data['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 将浮点数精确到4位小数
            float_cols = data.select_dtypes(include=['float']).columns
            for col in float_cols:
                if col in data.columns:
                    data[col] = data[col].round(4)
            
            self.logger.info(f"数据处理完成，记录数: {len(data)}")
            return data
            
        except Exception as e:
            self.logger.error(f"处理基本面指标数据失败: {str(e)}")
            return data
    
    def get_collection_name(self) -> str:
        """
        获取MongoDB集合名称
        
        Returns:
            str: 集合名称
        """
        return "stock_daily_basic"
    
    def get_stock_list(self) -> List[str]:
        """
        获取股票列表
        
        Returns:
            List[str]: 股票代码列表
        """
        try:
            # 从股票基本信息集合中获取股票列表
            collection = mongodb_handler.get_collection("stock_basic")
            stock_list = collection.distinct("ts_code")
            
            if not stock_list:
                self.logger.warning("未找到股票列表，尝试从Tushare获取")
                # 如果数据库中没有股票列表，则从Tushare获取
                from data_fetcher.tushare_client import TushareClient
                ts_client = TushareClient()
                result = ts_client.query('stock_basic', 
                                         fields='ts_code,name,list_date,delist_date')
                
                if not result.empty:
                    stock_list = result['ts_code'].tolist()
                else:
                    self.logger.error("无法获取股票列表")
                    return []
            
            self.logger.info(f"获取到 {len(stock_list)} 只股票")
            return stock_list
            
        except Exception as e:
            self.logger.error(f"获取股票列表失败: {str(e)}")
            return []
    
    def get_trade_dates(self, start_date: str, end_date: str) -> List[str]:
        """
        获取交易日期列表
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            List[str]: 交易日期列表
        """
        try:
            # 从交易日历集合中获取交易日期
            collection = mongodb_handler.get_collection("trade_cal")
            
            # 构建查询条件
            query = {
                "cal_date": {"$gte": start_date, "$lte": end_date},
                "is_open": 1
            }
            
            # 从小到大排序
            cursor = collection.find(query).sort("cal_date", pymongo.ASCENDING)
            
            # 提取交易日期列表
            dates = [doc["cal_date"] for doc in cursor]
            
            if not dates:
                self.logger.warning(f"未找到交易日期 ({start_date} - {end_date})，使用日期范围")
                # 如果数据库中没有交易日历，则使用日期范围
                dates = self.get_date_range(start_date, end_date)
            
            self.logger.info(f"获取到 {len(dates)} 个交易日期 ({start_date} - {end_date})")
            return dates
            
        except Exception as e:
            self.logger.error(f"获取交易日期列表失败: {str(e)}")
            # 出错时使用日期范围
            return self.get_date_range(start_date, end_date)
    
    def _run_recent_mode(self) -> Dict[str, Any]:
        """
        运行最近数据模式
        
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.info("运行最近数据获取模式")
        
        # 获取最近交易日
        today = datetime.now().strftime('%Y%m%d')
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
        
        # 尝试获取最近交易日的数据
        task = {'trade_date': yesterday}
        saved_count = self.fetch_and_save(**task)
        
        if saved_count == 0:
            # 如果昨天数据为空，可能是非交易日，尝试获取最近7天数据
            self.logger.info(f"未获取到 {yesterday} 的数据，尝试获取最近7天数据")
            task = {'start_date': week_ago, 'end_date': today}
            saved_count = self.fetch_and_save(**task)
        
        return {
            'status': 'success',
            'saved_count': saved_count,
            'message': f"成功获取并保存了 {saved_count} 条最近的基本面指标数据"
        }
    
    def _run_full_mode(self) -> Dict[str, Any]:
        """
        运行全量数据模式，获取所有股票的历史基本面指标数据
        
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.info("运行全量数据获取模式")
        
        # 获取股票列表
        stock_list = self.get_stock_list()
        if not stock_list:
            return {
                'status': 'error',
                'message': '无法获取股票列表'
            }
        
        # 获取当前日期
        end_date = datetime.now().strftime('%Y%m%d')
        
        # 准备批量任务
        tasks = []
        for ts_code in stock_list:
            task = {
                'ts_code': ts_code,
                'start_date': self.start_date,
                'end_date': end_date
            }
            tasks.append(task)
        
        # 并行获取数据
        self.logger.info(f"开始获取 {len(tasks)} 只股票的历史基本面指标数据")
        saved_count = self.parallel_fetch(tasks, max_workers=10)
        
        return {
            'status': 'success',
            'saved_count': saved_count,
            'message': f"成功获取并保存了 {saved_count} 条全量基本面指标数据"
        }
    
    def _run_date_mode(self, date: str) -> Dict[str, Any]:
        """
        运行指定日期模式
        
        Args:
            date: 日期 (YYYYMMDD格式)
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.info(f"运行指定日期 {date} 数据获取模式")
        
        # 创建任务
        task = {'trade_date': date}
        
        # 执行获取
        saved_count = self.fetch_and_save(**task)
        
        return {
            'status': 'success',
            'saved_count': saved_count,
            'message': f"成功获取并保存了 {saved_count} 条 {date} 日期的基本面指标数据"
        }
    
    def _run_date_range_mode(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        运行日期范围模式
        
        Args:
            start_date: 开始日期 (YYYYMMDD格式)
            end_date: 结束日期 (YYYYMMDD格式)
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.info(f"运行日期范围 {start_date} - {end_date} 数据获取模式")
        
        # 获取交易日期
        trade_dates = self.get_trade_dates(start_date, end_date)
        
        if not trade_dates:
            return {
                'status': 'error',
                'message': f'无法获取交易日期 ({start_date} - {end_date})'
            }
        
        # 准备批量任务
        tasks = []
        for date in trade_dates:
            task = {'trade_date': date}
            tasks.append(task)
        
        # 并行获取数据
        self.logger.info(f"开始获取 {len(tasks)} 个交易日的基本面指标数据")
        saved_count = self.parallel_fetch(tasks)
        
        return {
            'status': 'success',
            'saved_count': saved_count,
            'message': f"成功获取并保存了 {saved_count} 条从 {start_date} 至 {end_date} 的基本面指标数据"
        }
    
    def _run_stock_mode(self, ts_code: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        运行指定股票模式
        
        Args:
            ts_code: 股票代码 (如 000001.SZ)
            start_date: 开始日期 (YYYYMMDD格式)
            end_date: 结束日期 (YYYYMMDD格式)
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.info(f"运行指定股票 {ts_code} ({start_date} - {end_date}) 数据获取模式")
        
        # 创建任务
        task = {
            'ts_code': ts_code,
            'start_date': start_date,
            'end_date': end_date
        }
        
        # 执行获取
        saved_count = self.fetch_and_save(**task)
        
        return {
            'status': 'success',
            'saved_count': saved_count,
            'message': f"成功获取并保存了 {saved_count} 条 {ts_code} 从 {start_date} 至 {end_date} 的基本面指标数据"
        }


if __name__ == "__main__":
    # 创建数据获取器并运行
    fetcher = BasicIndicatorFetcher()
    result = fetcher.run()
    
    # 输出结果
    if result['status'] == 'success':
        print(f"执行成功：{result.get('message')}")
        print(f"保存记录数：{result.get('saved_count', 0)}")
    else:
        print(f"执行失败：{result.get('message')}")