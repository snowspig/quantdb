#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
QuantDB 主入口程序
演示如何使用核心模块进行数据获取和处理
"""
import os
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta

# 导入核心模块
from core import config_manager, mongodb_handler, network_manager
from core import BaseFetcher
import core.utils as utils


def setup_logging():
    """设置日志配置"""
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 获取日志配置
    log_config = config_manager.get_log_config()
    log_level = log_config.get('level', 'INFO')
    log_file = log_config.get('file', 'logs/quantdb.log')

    # 确保日志目录存在
    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 设置日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # 文件处理器
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level))
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    return root_logger


def check_system_status():
    """检查系统状态"""
    logger = logging.getLogger("main.check_system_status")
    status = {
        "config": False,
        "database": False,
        "network": False
    }

    # 检查配置
    try:
        config = config_manager.get_all_config()
        status["config"] = bool(config)
        logger.info(f"配置状态: {'正常' if status['config'] else '异常'}")
    except Exception as e:
        logger.error(f"配置检查失败: {str(e)}")

    # 检查数据库
    try:
        if mongodb_handler.is_connected():
            status["database"] = True
            logger.info(f"数据库状态: 正常, 数据库: {mongodb_handler.db.name}")
        else:
            logger.warning("数据库连接失败")
    except Exception as e:
        logger.error(f"数据库检查失败: {str(e)}")

    # 检查网络
    try:
        if network_manager and network_manager.check_connectivity():
            status["network"] = True
            # 获取接口状态
            interfaces_status = network_manager.get_interfaces_status()
            logger.info(f"网络状态: 正常, 多WAN模式: {interfaces_status['enabled']}, 活跃接口: {interfaces_status['active_interfaces']}/{interfaces_status['total_interfaces']}")
        else:
            logger.warning("网络连接异常")
    except Exception as e:
        logger.error(f"网络检查失败: {str(e)}")

    return status


class ExampleFetcher(BaseFetcher):
    """
    示例数据获取器
    演示如何实现BaseFetcher的子类
    """
    
    def __init__(self):
        """初始化示例获取器"""
        super().__init__(api_name="example", silent=False)
    
    def fetch_data(self, **kwargs):
        """
        从数据源获取数据
        
        Args:
            **kwargs: 查询参数
            
        Returns:
            pd.DataFrame: 获取的数据
        """
        import pandas as pd
        import numpy as np
        
        # 这里只是一个演示，实际应用中应该从真实数据源获取数据
        self.logger.info(f"正在获取数据，参数: {kwargs}")
        
        # 创建一个示例数据框
        dates = pd.date_range(start=kwargs.get('start_date', '20230101'), 
                             end=kwargs.get('end_date', '20230105'))
        
        # 生成随机股票数据
        n_stocks = kwargs.get('n_stocks', 5)
        stock_codes = [f"60000{i}.SH" for i in range(1, n_stocks+1)]
        
        data = []
        for date in dates:
            date_str = date.strftime('%Y%m%d')
            for ts_code in stock_codes:
                # 生成模拟数据
                open_price = np.random.uniform(10, 100)
                close = open_price * (1 + np.random.uniform(-0.05, 0.05))
                high = max(open_price, close) * (1 + np.random.uniform(0, 0.03))
                low = min(open_price, close) * (1 - np.random.uniform(0, 0.03))
                vol = np.random.uniform(1000, 10000)
                amount = vol * close
                
                data.append({
                    'ts_code': ts_code,
                    'trade_date': date_str,
                    'open': open_price,
                    'high': high,
                    'low': low,
                    'close': close,
                    'vol': vol,
                    'amount': amount,
                    'change': close - open_price,
                    'pct_chg': (close - open_price) / open_price * 100
                })
        
        # 创建DataFrame
        df = pd.DataFrame(data)
        
        # 模拟网络请求延迟
        time.sleep(0.5)
        
        self.logger.info(f"成功获取 {len(df)} 条数据记录")
        return df
    
    def process_data(self, data):
        """
        处理获取的原始数据
        
        Args:
            data: 原始数据
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        self.logger.info("处理数据...")
        
        # 确保日期列是字符串格式
        if 'trade_date' in data.columns and not data['trade_date'].dtype == 'object':
            data['trade_date'] = data['trade_date'].astype(str)
        
        # 添加计算列
        data['ma5'] = data.groupby('ts_code')['close'].rolling(window=5, min_periods=1).mean().reset_index(drop=True)
        data['ma10'] = data.groupby('ts_code')['close'].rolling(window=10, min_periods=1).mean().reset_index(drop=True)
        
        # 计算涨跌标志
        data['up_down'] = data['change'].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        
        self.logger.info(f"数据处理完成，记录数: {len(data)}")
        return data
    
    def get_collection_name(self):
        """
        获取数据存储的集合名称
        
        Returns:
            str: 集合名称
        """
        return "example_daily_data"
    
    def _run_recent_mode(self):
        """
        运行最近数据模式
        
        Returns:
            Dict: 执行结果
        """
        self.logger.info("运行最近数据获取模式")
        
        # 获取今天和昨天的日期
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')
        
        # 创建一个获取任务
        task = {
            'start_date': start_date,
            'end_date': end_date,
            'n_stocks': 10
        }
        
        # 执行获取任务
        saved_count = self.fetch_and_save(**task)
        
        return {
            'status': 'success',
            'saved_count': saved_count,
            'message': f"成功获取并保存了 {saved_count} 条最近数据"
        }
    
    def _run_date_mode(self, date):
        """
        运行指定日期模式
        
        Args:
            date: 日期字符串
            
        Returns:
            Dict: 执行结果
        """
        self.logger.info(f"运行指定日期数据获取模式: {date}")
        
        # 创建任务
        task = {
            'start_date': date,
            'end_date': date,
            'n_stocks': 10
        }
        
        # 执行任务
        saved_count = self.fetch_and_save(**task)
        
        return {
            'status': 'success',
            'saved_count': saved_count,
            'message': f"成功获取并保存了 {saved_count} 条 {date} 日期的数据"
        }
    
    def _run_date_range_mode(self, start_date, end_date):
        """
        运行日期范围模式
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            Dict: 执行结果
        """
        self.logger.info(f"运行日期范围数据获取模式: {start_date} 至 {end_date}")
        
        # 创建任务
        task = {
            'start_date': start_date,
            'end_date': end_date,
            'n_stocks': 10
        }
        
        # 执行任务
        saved_count = self.fetch_and_save(**task)
        
        return {
            'status': 'success',
            'saved_count': saved_count,
            'message': f"成功获取并保存了 {saved_count} 条从 {start_date} 至 {end_date} 的数据"
        }


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='QuantDB 数据获取和管理系统')
    
    parser.add_argument('--check', action='store_true', help='检查系统状态')
    parser.add_argument('--demo', action='store_true', help='运行示例数据获取器')
    parser.add_argument('--mode', type=str, choices=['recent', 'date', 'range', 'full'], 
                        default='recent', help='数据获取模式')
    parser.add_argument('--date', type=str, help='指定日期 (YYYYMMDD格式)')
    parser.add_argument('--start-date', type=str, help='起始日期 (YYYYMMDD格式)')
    parser.add_argument('--end-date', type=str, help='结束日期 (YYYYMMDD格式)')
    
    return parser.parse_args()


def main():
    """主函数"""
    # 设置日志
    logger = setup_logging()
    logger.info("QuantDB 系统启动")
    
    # 解析命令行参数
    args = parse_arguments()
    
    # 检查系统状态
    if args.check:
        status = check_system_status()
        all_ok = all(status.values())
        logger.info(f"系统状态检查: {'正常' if all_ok else '异常'}")
        for component, status in status.items():
            logger.info(f"  - {component}: {'正常' if status else '异常'}")
    
    # 运行示例数据获取器
    if args.demo:
        logger.info("运行示例数据获取器")
        fetcher = ExampleFetcher()
        
        # 根据模式运行
        if args.mode == 'recent':
            result = fetcher.run(mode='recent')
        elif args.mode == 'date':
            if not args.date:
                logger.error("缺少日期参数，请使用 --date 指定日期")
                return
            result = fetcher.run(mode='date', date=args.date)
        elif args.mode == 'range':
            if not args.start_date or not args.end_date:
                logger.error("缺少日期范围参数，请使用 --start-date 和 --end-date 指定日期范围")
                return
            result = fetcher.run(mode='range', start_date=args.start_date, end_date=args.end_date)
        elif args.mode == 'full':
            result = fetcher.run(mode='full')
        
        # 输出结果
        if result.get('status') == 'success':
            logger.info(f"示例运行成功: {result.get('message')}")
            logger.info(f"保存记录数: {result.get('saved_count', 0)}")
            logger.info(f"执行时间: {result.get('execution_time', 0):.2f} 秒")
        else:
            logger.error(f"示例运行失败: {result.get('message')}")
    
    logger.info("QuantDB 系统运行完成")


if __name__ == "__main__":
    main()