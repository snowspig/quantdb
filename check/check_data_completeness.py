#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
数据完整性检查工具
检查最近一个月内数据不完整的股票，并为其调度数据获取任务
"""
import os
import sys
import time
import logging
import argparse
import calendar
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import concurrent.futures
from pathlib import Path
import subprocess

# 导入配置管理器
from config import config_manager

# 导入MongoDB处理器
from storage.mongodb_handler import mongodb_handler

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join('logs', 'check_completeness.log'))
    ]
)

logger = logging.getLogger('check_completeness')

class DataCompletenessChecker:
    """
    数据完整性检查器
    检查股票数据是否完整，并针对不完整的数据调度抓取任务
    """
    
    def __init__(self, days=30, threshold=0.8, concurrent=5, silent=False):
        """
        初始化数据完整性检查器
        
        Args:
            days: 检查最近多少天的数据
            threshold: 完整性阈值（小于此阈值的股票会被标记为不完整）
            concurrent: 并发执行的数据获取任务数量
            silent: 静默模式，不输出日志
        """
        self.days = days
        self.threshold = threshold
        self.concurrent = concurrent
        self.silent = silent
        
        # 设置日志级别
        if silent:
            logger.setLevel(logging.WARNING)
        else:
            logger.setLevel(logging.INFO)
            
        # 缓存最近交易日列表
        self.trading_dates = None
        self.trading_days_count = 0
        
        # 提前计算日期范围
        self.end_date = datetime.now().strftime('%Y%m%d')
        self.start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        
        logger.info(f"初始化数据完整性检查器，检查范围: {self.start_date} 至 {self.end_date}")
        
    def get_recent_trading_dates(self):
        """
        获取最近交易日列表
        
        Returns:
            list: 交易日列表
        """
        if self.trading_dates is not None:
            return self.trading_dates
            
        try:
            # 从数据库获取交易日历数据
            cal_data = mongodb_handler.find(
                'trade_cal', 
                {'cal_date': {'$gte': self.start_date, '$lte': self.end_date}, 
                 'is_open': 1},
                sort=[('cal_date', 1)]
            )
            
            if not cal_data:
                logger.warning("未找到交易日历数据，尝试从数据获取")
                # 如果没有交易日历数据，尝试从每日行情数据中提取日期
                daily_data = mongodb_handler.find(
                    'stock_daily',
                    {'trade_date': {'$gte': self.start_date, '$lte': self.end_date}},
                    projection={'trade_date': 1, '_id': 0}
                )
                
                # 提取唯一日期
                if daily_data:
                    dates = set()
                    for item in daily_data:
                        if 'trade_date' in item:
                            date_str = item['trade_date']
                            # 处理可能的日期格式差异
                            if isinstance(date_str, datetime):
                                date_str = date_str.strftime('%Y%m%d')
                            dates.add(date_str)
                    
                    self.trading_dates = sorted(list(dates))
                else:
                    # 如果无法获取实际交易日，则使用工作日作为估计
                    logger.warning("无法获取实际交易日，使用工作日估计")
                    start_dt = datetime.strptime(self.start_date, '%Y%m%d')
                    end_dt = datetime.strptime(self.end_date, '%Y%m%d')
                    
                    dates = []
                    curr = start_dt
                    while curr <= end_dt:
                        # 0是周一，6是周日，工作日为0-4
                        if curr.weekday() < 5:  
                            dates.append(curr.strftime('%Y%m%d'))
                        curr += timedelta(days=1)
                    
                    self.trading_dates = dates
            else:
                # 从交易日历提取日期
                self.trading_dates = [item['cal_date'] for item in cal_data]
                
            self.trading_days_count = len(self.trading_dates)
            logger.info(f"获取到 {self.trading_days_count} 个交易日")
            return self.trading_dates
            
        except Exception as e:
            logger.error(f"获取交易日列表失败: {str(e)}")
            return []
    
    def get_active_stocks(self):
        """
        获取活跃股票列表（排除退市和未上市的股票）
        
        Returns:
            list: 股票代码列表
        """
        try:
            # 从数据库获取股票基本信息
            stocks = mongodb_handler.find(
                'stock_basic',
                {'list_status': 'L'}  # 上市状态：L上市 D退市 P暂停上市
            )
            
            if not stocks:
                logger.warning("未找到股票基本信息，尝试从股票日线数据中提取")
                # 如果没有股票基本信息，尝试从日线数据中提取
                daily_data = mongodb_handler.find_one(
                    'stock_daily',
                    {'trade_date': {'$gte': self.start_date, '$lte': self.end_date}},
                    projection={'ts_code': 1, '_id': 0}
                )
                
                # 获取所有活跃交易的股票代码
                active_stocks = mongodb_handler.distinct(
                    'stock_daily',
                    'ts_code',
                    {'trade_date': {'$gte': self.start_date, '$lte': self.end_date}}
                )
                
                if active_stocks:
                    logger.info(f"从日线数据中提取到 {len(active_stocks)} 个活跃股票")
                    return active_stocks
                else:
                    logger.error("无法获取股票列表")
                    return []
            
            # 从基本信息中提取股票代码
            active_stocks = [item['ts_code'] for item in stocks]
            logger.info(f"获取到 {len(active_stocks)} 个上市股票")
            return active_stocks
            
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            return []
    
    def check_stock_data_completeness(self, ts_code):
        """
        检查单只股票数据完整性
        
        Args:
            ts_code: 股票代码
            
        Returns:
            dict: 股票数据完整性信息
        """
        try:
            # 获取交易日列表
            if not self.trading_dates:
                self.get_recent_trading_dates()
                
            if not self.trading_dates:
                return {
                    'ts_code': ts_code,
                    'complete': False,
                    'available_days': 0,
                    'total_days': 0,
                    'completeness': 0,
                    'missing_dates': []
                }
            
            # 查询该股票在指定日期范围的数据
            stock_data = mongodb_handler.find(
                'stock_daily',
                {
                    'ts_code': ts_code,
                    'trade_date': {'$gte': self.start_date, '$lte': self.end_date}
                },
                projection={'trade_date': 1, '_id': 0}
            )
            
            # 获取该股票所有可用的交易日
            available_dates = set()
            for item in stock_data:
                if 'trade_date' in item:
                    date_str = item['trade_date']
                    # 处理可能的日期格式差异
                    if isinstance(date_str, datetime):
                        date_str = date_str.strftime('%Y%m%d')
                    available_dates.add(date_str)
            
            # 计算数据完整性
            available_days = len(available_dates)
            completeness = available_days / self.trading_days_count if self.trading_days_count > 0 else 0
            
            # 找出缺失的日期
            missing_dates = [date for date in self.trading_dates if date not in available_dates]
            
            return {
                'ts_code': ts_code,
                'complete': completeness >= self.threshold,
                'available_days': available_days,
                'total_days': self.trading_days_count,
                'completeness': completeness,
                'missing_dates': missing_dates
            }
            
        except Exception as e:
            logger.error(f"检查股票 {ts_code} 完整性失败: {str(e)}")
            return {
                'ts_code': ts_code,
                'complete': False,
                'error': str(e)
            }
    
    def check_all_stocks(self):
        """
        检查所有股票数据完整性
        
        Returns:
            tuple: (不完整的股票列表, 完整性报告)
        """
        # 获取活跃股票列表
        stocks = self.get_active_stocks()
        if not stocks:
            logger.error("无法获取活跃股票列表")
            return [], {}
            
        # 获取交易日列表
        trading_dates = self.get_recent_trading_dates()
        if not trading_dates:
            logger.error("无法获取交易日列表")
            return [], {}
            
        logger.info(f"开始检查 {len(stocks)} 只股票数据完整性")
        
        # 检查每只股票的完整性
        results = []
        incomplete_stocks = []
        
        for ts_code in stocks:
            report = self.check_stock_data_completeness(ts_code)
            results.append(report)
            
            if not report.get('complete', False):
                incomplete_stocks.append(ts_code)
                logger.debug(f"股票 {ts_code} 数据不完整: {report['available_days']}/{report['total_days']} "
                           f"({report['completeness']:.2%})")
                
        # 生成完整性报告
        completeness_report = {
            'total_stocks': len(stocks),
            'incomplete_stocks': len(incomplete_stocks),
            'completeness_rate': 1 - len(incomplete_stocks) / len(stocks) if stocks else 0,
            'check_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'period': f"{self.start_date} 至 {self.end_date}",
            'threshold': self.threshold
        }
        
        logger.info(f"数据完整性检查完成，共有 {len(incomplete_stocks)} 只股票数据不完整 "
                   f"(完整率: {1 - len(incomplete_stocks) / len(stocks):.2%})")
        
        return incomplete_stocks, completeness_report
    
    def fetch_data_for_stock(self, ts_code):
        """
        为指定股票抓取全量数据
        
        Args:
            ts_code: 股票代码
            
        Returns:
            bool: 是否成功
        """
        try:
            # 找到对应的数据获取器脚本
            data_fetcher_script = None
            for root, dirs, files in os.walk('data_fetcher'):
                for file in files:
                    if file.endswith('_fetcher.py') and not file.startswith('base'):
                        data_fetcher_script = os.path.join(root, file)
                        break
                if data_fetcher_script:
                    break
            
            if not data_fetcher_script:
                data_fetcher_script = 'data_fetcher/daily_fetcher.py'
                logger.warning(f"未找到数据获取器脚本，使用默认脚本: {data_fetcher_script}")
                
            if not os.path.exists(data_fetcher_script):
                logger.error(f"数据获取器脚本不存在: {data_fetcher_script}")
                return False
                
            # 构建命令
            cmd = [
                sys.executable, 
                data_fetcher_script,
                '--ts-code', ts_code,
                '--start-date', '20100101',  # 从2010年开始抓取历史数据
                '--end-date', self.end_date
            ]
            
            if self.silent:
                cmd.append('--silent')
                
            logger.debug(f"执行命令: {' '.join(cmd)}")
            
            # 执行数据获取
            process = subprocess.Popen(
                cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            stdout, stderr = process.communicate()
            
            if process.returncode != 0:
                logger.error(f"股票 {ts_code} 数据获取失败: {stderr}")
                return False
            
            logger.debug(f"股票 {ts_code} 数据获取成功")
            return True
            
        except Exception as e:
            logger.error(f"为股票 {ts_code} 抓取数据失败: {str(e)}")
            return False
    
    def fetch_data_for_incomplete_stocks(self, incomplete_stocks):
        """
        为不完整的股票抓取数据
        
        Args:
            incomplete_stocks: 不完整的股票列表
            
        Returns:
            dict: 抓取结果统计
        """
        if not incomplete_stocks:
            logger.info("没有需要抓取的股票")
            return {'success': 0, 'failed': 0, 'total': 0}
            
        logger.info(f"开始为 {len(incomplete_stocks)} 只数据不完整的股票抓取数据")
        
        success_count = 0
        failed_count = 0
        
        # 使用线程池并行抓取数据
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.concurrent) as executor:
            # 提交所有任务
            futures = {executor.submit(self.fetch_data_for_stock, ts_code): ts_code for ts_code in incomplete_stocks}
            
            # 处理完成的任务
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                ts_code = futures[future]
                try:
                    success = future.result()
                    if success:
                        success_count += 1
                    else:
                        failed_count += 1
                        
                    # 显示进度
                    progress = (i + 1) / len(incomplete_stocks) * 100
                    logger.info(f"进度: {progress:.1f}% ({i + 1}/{len(incomplete_stocks)}), "
                               f"成功: {success_count}, 失败: {failed_count}")
                    
                except Exception as e:
                    failed_count += 1
                    logger.error(f"股票 {ts_code} 数据获取异常: {str(e)}")
        
        results = {
            'success': success_count,
            'failed': failed_count,
            'total': len(incomplete_stocks)
        }
        
        logger.info(f"数据抓取完成，成功: {success_count}, 失败: {failed_count}, "
                   f"总计: {len(incomplete_stocks)}")
        
        return results
    
    def run(self, fetch=True):
        """
        运行数据完整性检查
        
        Args:
            fetch: 是否为不完整的股票抓取数据
            
        Returns:
            dict: 执行结果
        """
        # 检查所有股票数据完整性
        start_time = time.time()
        incomplete_stocks, completeness_report = self.check_all_stocks()
        check_time = time.time() - start_time
        
        # 为不完整的股票抓取数据
        fetch_results = {'executed': False}
        if fetch and incomplete_stocks:
            fetch_results = self.fetch_data_for_incomplete_stocks(incomplete_stocks)
            fetch_results['executed'] = True
            
        # 生成最终报告
        final_report = {
            'check': completeness_report,
            'incomplete_stocks': incomplete_stocks,
            'fetch': fetch_results,
            'execution_time': {
                'check': check_time,
                'total': time.time() - start_time
            }
        }
        
        return final_report


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='数据完整性检查工具')
    parser.add_argument('--days', type=int, default=30, help='检查最近多少天的数据')
    parser.add_argument('--threshold', type=float, default=0.8, help='完整性阈值 (0-1)')
    parser.add_argument('--no-fetch', action='store_true', help='不自动抓取数据')
    parser.add_argument('--concurrent', type=int, default=5, help='并发抓取任务数')
    parser.add_argument('--silent', action='store_true', help='静默模式')
    parser.add_argument('--output', help='输出报告文件路径')
    parser.add_argument('--list-only', action='store_true', help='只列出不完整的股票，不抓取')
    args = parser.parse_args()
    
    # 确保日志目录存在
    os.makedirs('logs', exist_ok=True)
    
    # 创建检查器
    checker = DataCompletenessChecker(
        days=args.days,
        threshold=args.threshold,
        concurrent=args.concurrent,
        silent=args.silent
    )
    
    # 运行检查
    report = checker.run(fetch=not args.no_fetch and not args.list_only)
    
    # 输出不完整的股票列表
    if args.list_only and report['incomplete_stocks']:
        print("\n数据不完整的股票列表:")
        for ts_code in report['incomplete_stocks']:
            print(ts_code)
            
    # 输出报告
    if args.output:
        try:
            os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
            with open(args.output, 'w', encoding='utf-8') as f:
                import json
                json.dump(report, f, ensure_ascii=False, indent=2)
                logger.info(f"报告已保存至: {args.output}")
        except Exception as e:
            logger.error(f"保存报告失败: {str(e)}")
    
    # 输出摘要
    incomplete_count = len(report['incomplete_stocks'])
    total_count = report['check'].get('total_stocks', 0)
    check_time = report['execution_time'].get('check', 0)
    
    print(f"\n数据完整性检查摘要:")
    print(f"- 检查范围: {report['check'].get('period', '')}")
    print(f"- 检查股票总数: {total_count}")
    print(f"- 数据不完整股票数: {incomplete_count} ({incomplete_count/total_count:.2%} 不完整)")
    print(f"- 检查耗时: {check_time:.2f} 秒")
    
    if report['fetch'].get('executed', False):
        fetch_success = report['fetch'].get('success', 0)
        fetch_failed = report['fetch'].get('failed', 0)
        total_time = report['execution_time'].get('total', 0)
        
        print(f"\n数据抓取摘要:")
        print(f"- 抓取成功: {fetch_success}")
        print(f"- 抓取失败: {fetch_failed}")
        print(f"- 总耗时: {total_time:.2f} 秒")


if __name__ == '__main__':
    main()