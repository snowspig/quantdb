"""
数据获取基础类模块，提供所有数据获取器的基础功能
"""
import os
import sys
import time
import json
import argparse
import logging
import pandas as pd
import numpy as np
import threading
import asyncio
import concurrent.futures
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union, Tuple, Set, Callable

# 导入核心模块
from config import config_manager
from storage.mongodb_handler import mongodb_handler
from network_manager.network_manager import network_manager


class BaseFetcher(ABC):
    """
    数据获取基础类，为所有数据获取器提供通用功能
    """
    
    def __init__(self, interface_name: str, config_path=None, silent=False):
        """
        初始化数据获取器
        
        Args:
            interface_name: 接口名称，用于加载接口配置
            config_path: 配置文件路径，不提供则使用默认路径
            silent: 静默模式，不输出日志
        """
        self.interface_name = interface_name
        self.config_path = config_path
        self.silent = silent
        
        # 日志设置
        self._setup_logging()
        
        # 接口配置
        self.interface_config = config_manager.get_interface_config(interface_name)
        if not self.interface_config:
            self.logger.error(f"接口配置未找到: {interface_name}")
            raise ValueError(f"接口配置未找到: {interface_name}")
            
        self.api_name = self.interface_config.get('api_name', interface_name)
        self.fields = self.interface_config.get('fields', [])
        self.field_names = [field['name'] for field in self.fields if field.get('name')]
        
        # 多WAN支持
        self.wan_enabled = config_manager.is_wan_enabled()
        
        # 数据获取设置
        self.fetcher_config = config_manager.get('data_fetcher', {})
        self.batch_size = self.fetcher_config.get('default_batch_size', 100)
        self.thread_count = self.fetcher_config.get('default_thread_count', 5)
        self.retry_count = self.fetcher_config.get('fetch_retry_count', 3)
        self.retry_delay = self.fetcher_config.get('fetch_retry_delay', 5)
        self.save_raw_data = self.fetcher_config.get('save_raw_data', False)
        
        # 初始化统计信息
        self.stats = {
            'fetch_count': 0,
            'process_count': 0,
            'error_count': 0,
            'skip_count': 0,
            'total_time': 0,
            'start_time': None,
            'end_time': None,
        }
        
        # 线程锁
        self.lock = threading.RLock()
        
        # 进度显示
        self.show_progress = not silent
        
        # 接口索引字段
        self.index_fields = self.interface_config.get('index_fields', [])
        self.collection_name = self.get_collection_name()
        
        # 创建MongoDB索引
        self._create_indexes()
        
        self.logger.info(f"初始化数据获取器: {interface_name}, API: {self.api_name}, 集合: {self.collection_name}")

    def _setup_logging(self):
        """
        设置日志记录
        """
        self.logger = logging.getLogger(f"fetcher.{self.__class__.__name__}")
        
        if not self.logger.handlers:
            # 避免重复添加处理程序
            if not self.silent:
                # 控制台处理程序
                console_handler = logging.StreamHandler()
                console_handler.setFormatter(logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                ))
                self.logger.addHandler(console_handler)
                
                # 文件处理程序
                log_config = config_manager.get_log_config()
                log_file = log_config.get('file', 'logs/fetcher.log')
                os.makedirs(os.path.dirname(log_file), exist_ok=True)
                file_handler = logging.FileHandler(log_file)
                file_handler.setFormatter(logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                ))
                self.logger.addHandler(file_handler)
            
            # 设置日志级别
            log_level = logging.INFO if not self.silent else logging.ERROR
            self.logger.setLevel(log_level)

    def get_collection_name(self) -> str:
        """
        获取数据存储的集合名称，子类可以重写此方法
        
        Returns:
            str: 集合名称
        """
        return self.interface_name
        
    def _create_indexes(self):
        """
        在MongoDB中创建必要的索引
        """
        if not self.index_fields:
            return
            
        try:
            # 创建索引
            if isinstance(self.index_fields, list) and self.index_fields:
                # 将字段列表转换为索引格式
                index_spec = [(field, pymongo.ASCENDING) for field in self.index_fields]
                
                # 创建索引
                mongodb_handler.ensure_index(
                    self.collection_name, 
                    index_spec,
                    unique=True  # 使用唯一索引避免重复数据
                )
                
                self.logger.debug(f"为集合 {self.collection_name} 创建索引: {self.index_fields}")
                
        except Exception as e:
            self.logger.error(f"创建索引失败: {str(e)}")
    
    def get_data_since_date(self, days: int) -> str:
        """
        获取从今天往前指定天数的日期字符串 (YYYYMMDD格式)
        
        Args:
            days: 天数
            
        Returns:
            str: 日期字符串
        """
        date = datetime.now() - timedelta(days=days)
        return date.strftime('%Y%m%d')
    
    def get_recent_date(self) -> str:
        """
        获取最近交易日期的数据
        
        Returns:
            str: 最近的交易日期
        """
        # 默认获取最近7天的数据，子类可以重写此方法
        return self.get_data_since_date(7)
    
    def get_date_range(self, start_date: str, end_date: str) -> List[str]:
        """
        获取日期范围内的所有日期
        
        Args:
            start_date: 开始日期 (YYYYMMDD格式)
            end_date: 结束日期 (YYYYMMDD格式)
            
        Returns:
            List[str]: 日期列表
        """
        try:
            start = datetime.strptime(start_date, '%Y%m%d')
            end = datetime.strptime(end_date, '%Y%m%d')
            
            if start > end:
                start, end = end, start
                
            dates = []
            current = start
            while current <= end:
                dates.append(current.strftime('%Y%m%d'))
                current += timedelta(days=1)
                
            return dates
            
        except ValueError:
            self.logger.error(f"日期格式错误: {start_date} 或 {end_date}, 应为YYYYMMDD格式")
            return []
    
    @abstractmethod
    def fetch_data(self, **kwargs) -> pd.DataFrame:
        """
        从数据源获取数据，需要子类实现
        
        Args:
            **kwargs: 参数列表
            
        Returns:
            pd.DataFrame: 获取的数据
        """
        pass
    
    def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        处理获取的数据，子类可以重写此方法
        
        Args:
            data: 获取的数据
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        # 默认不做处理，直接返回原数据
        return data
        
    def save_data(self, data: pd.DataFrame) -> int:
        """
        保存数据到MongoDB
        
        Args:
            data: 要保存的数据
            
        Returns:
            int: 保存的记录数
        """
        if data is None or data.empty:
            return 0
            
        try:
            # 转换为记录列表
            records = data.to_dict('records')
            if not records:
                return 0
                
            # 使用索引字段作为过滤条件，避免重复插入
            if self.index_fields:
                bulk_ops = []
                for record in records:
                    # 构建过滤条件
                    filter_dict = {field: record[field] for field in self.index_fields if field in record}
                    if filter_dict:
                        # 使用upsert操作，存在则更新，不存在则插入
                        bulk_ops.append(
                            pymongo.UpdateOne(
                                filter_dict,
                                {'$set': record},
                                upsert=True
                            )
                        )
                    else:
                        # 没有索引字段，直接插入
                        bulk_ops.append(pymongo.InsertOne(record))
                
                # 批量执行
                result = mongodb_handler.bulk_write(self.collection_name, bulk_ops)
                saved_count = result.get('inserted', 0) + result.get('updated', 0)
            else:
                # 没有索引字段，直接插入
                result = mongodb_handler.insert_many(self.collection_name, records)
                saved_count = len(result) if result else 0
            
            return saved_count
            
        except Exception as e:
            self.logger.error(f"保存数据失败: {str(e)}")
            return 0
    
    def check_data_exists(self, query: Dict) -> bool:
        """
        检查数据是否已存在
        
        Args:
            query: 查询条件
            
        Returns:
            bool: 是否存在
        """
        try:
            count = mongodb_handler.count_documents(self.collection_name, query)
            return count > 0
        except Exception as e:
            self.logger.error(f"检查数据存在性失败: {str(e)}")
            return False
    
    def fetch_and_save(self, **kwargs) -> int:
        """
        获取并保存数据
        
        Args:
            **kwargs: 参数列表
            
        Returns:
            int: 保存的记录数
        """
        try:
            # 记录开始时间
            start_time = time.time()
            
            # 获取数据
            self.logger.info(f"开始获取数据，参数: {kwargs}")
            data = self.fetch_data(**kwargs)
            
            if data is None or data.empty:
                self.logger.info("未获取到数据")
                return 0
                
            # 处理数据
            self.logger.debug(f"获取到 {len(data)} 条原始数据，开始处理")
            processed_data = self.process_data(data)
            
            # 保存数据
            self.logger.debug(f"处理后数据 {len(processed_data)} 条，开始保存")
            saved_count = self.save_data(processed_data)
            
            # 记录结束时间和统计信息
            end_time = time.time()
            with self.lock:
                self.stats['fetch_count'] += 1
                self.stats['process_count'] += len(data) if data is not None else 0
                self.stats['total_time'] += (end_time - start_time)
                
            self.logger.info(f"数据获取完成，保存了 {saved_count} 条记录，耗时 {end_time - start_time:.2f} 秒")
            
            return saved_count
            
        except Exception as e:
            self.logger.error(f"获取并保存数据失败: {str(e)}")
            with self.lock:
                self.stats['error_count'] += 1
            return 0
    
    def parallel_fetch(self, tasks: List[Dict], max_workers: int = None) -> int:
        """
        并行获取多个数据集
        
        Args:
            tasks: 任务列表，每个任务是一个参数字典
            max_workers: 最大工作线程数，不提供则使用默认值
            
        Returns:
            int: 总共保存的记录数
        """
        if not tasks:
            return 0
            
        # 使用默认线程数（如果未指定）
        if max_workers is None:
            max_workers = self.thread_count
            
        # 记录开始时间
        self.stats['start_time'] = time.time()
        total_saved = 0
        
        self.logger.info(f"开始并行获取 {len(tasks)} 个任务，最大线程数: {max_workers}")
        
        try:
            # 使用线程池并行获取
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                futures = [executor.submit(self.fetch_and_save, **task) for task in tasks]
                
                # 等待所有任务完成
                for i, future in enumerate(concurrent.futures.as_completed(futures)):
                    try:
                        saved_count = future.result()
                        total_saved += saved_count
                        
                        # 显示进度
                        if self.show_progress:
                            progress = (i + 1) / len(tasks) * 100
                            self.logger.info(f"进度: {progress:.1f}% ({i + 1}/{len(tasks)})")
                            
                    except Exception as e:
                        self.logger.error(f"任务执行失败: {str(e)}")
                        with self.lock:
                            self.stats['error_count'] += 1
            
        except Exception as e:
            self.logger.error(f"并行获取数据失败: {str(e)}")
            
        # 记录结束时间
        self.stats['end_time'] = time.time()
        self.stats['total_time'] = self.stats['end_time'] - self.stats['start_time']
        
        self.logger.info(f"并行获取完成，总共保存 {total_saved} 条记录，总耗时 {self.stats['total_time']:.2f} 秒")
        return total_saved
    
    def print_stats(self):
        """
        打印统计信息
        """
        if self.silent:
            return
            
        self.logger.info("=" * 60)
        self.logger.info(f"数据获取统计信息 - {self.interface_name}")
        self.logger.info("-" * 60)
        self.logger.info(f"获取次数: {self.stats['fetch_count']}")
        self.logger.info(f"处理记录数: {self.stats['process_count']}")
        self.logger.info(f"错误次数: {self.stats['error_count']}")
        self.logger.info(f"跳过次数: {self.stats['skip_count']}")
        self.logger.info(f"总耗时: {self.stats['total_time']:.2f} 秒")
        
        if self.stats['start_time'] and self.stats['end_time']:
            duration = self.stats['end_time'] - self.stats['start_time']
            self.logger.info(f"开始时间: {datetime.fromtimestamp(self.stats['start_time']).strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"结束时间: {datetime.fromtimestamp(self.stats['end_time']).strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info(f"总运行时间: {duration:.2f} 秒")
            
            # 计算处理速度
            if self.stats['process_count'] > 0:
                speed = self.stats['process_count'] / duration
                self.logger.info(f"处理速度: {speed:.2f} 记录/秒")
                
        self.logger.info("=" * 60)
    
    def _parse_args(self):
        """
        解析命令行参数
        
        Returns:
            argparse.Namespace: 解析后的参数
        """
        parser = argparse.ArgumentParser(description=f"{self.interface_name} 数据获取器")
        
        # 互斥参数组 - 运行模式
        mode_group = parser.add_mutually_exclusive_group(required=True)
        mode_group.add_argument('--recent', action='store_true', help='获取最近数据')
        mode_group.add_argument('--full', action='store_true', help='获取全量数据')
        mode_group.add_argument('--date', help='获取指定日期的数据 (YYYYMMDD格式)')
        mode_group.add_argument('--start-date', help='开始日期 (YYYYMMDD格式)')
        mode_group.add_argument('--ts-code', help='获取指定股票代码的数据 (如 000001.SZ)')
        
        # 日期范围的结束日期
        parser.add_argument('--end-date', help='结束日期 (YYYYMMDD格式)')
        
        # 其他参数
        parser.add_argument('--threads', type=int, default=self.thread_count, help=f'线程数 (默认: {self.thread_count})')
        parser.add_argument('--batch-size', type=int, default=self.batch_size, help=f'批处理大小 (默认: {self.batch_size})')
        parser.add_argument('--retry', type=int, default=self.retry_count, help=f'重试次数 (默认: {self.retry_count})')
        parser.add_argument('--silent', action='store_true', help='静默模式')
        parser.add_argument('--save-raw', action='store_true', help='保存原始数据')
        parser.add_argument('--debug', action='store_true', help='调试模式')
        
        # 解析参数
        args = parser.parse_args()
        
        # 校验参数
        if args.start_date and not args.end_date:
            parser.error("--start-date 需要同时指定 --end-date")
            
        # 更新配置
        self.thread_count = args.threads
        self.batch_size = args.batch_size
        self.retry_count = args.retry
        self.silent = args.silent
        self.save_raw_data = args.save_raw
        
        # 设置日志级别
        if args.debug:
            self.logger.setLevel(logging.DEBUG)
            
        return args
    
    def run(self) -> Dict[str, Any]:
        """
        运行数据获取器
        
        Returns:
            Dict[str, Any]: 执行结果
        """
        # 解析命令行参数
        args = self._parse_args()
        
        try:
            # 根据运行模式执行不同的操作
            if args.recent:
                # 获取最近数据
                self.logger.info("运行模式: 获取最近数据")
                result = self._run_recent_mode()
                
            elif args.full:
                # 获取全量数据
                self.logger.info("运行模式: 获取全量数据")
                result = self._run_full_mode()
                
            elif args.date:
                # 获取指定日期数据
                self.logger.info(f"运行模式: 获取指定日期数据 {args.date}")
                result = self._run_date_mode(args.date)
                
            elif args.start_date and args.end_date:
                # 获取日期范围数据
                self.logger.info(f"运行模式: 获取日期范围数据 {args.start_date} 至 {args.end_date}")
                result = self._run_date_range_mode(args.start_date, args.end_date)
                
            elif args.ts_code:
                # 获取指定股票数据
                end_date = args.end_date or datetime.now().strftime('%Y%m%d')
                start_date = args.start_date or self.get_data_since_date(90)  # 默认90天
                self.logger.info(f"运行模式: 获取指定股票数据 {args.ts_code} ({start_date} 至 {end_date})")
                result = self._run_stock_mode(args.ts_code, start_date, end_date)
                
            else:
                self.logger.error("未指定运行模式")
                return {'status': 'error', 'message': '未指定运行模式'}
                
            # 打印统计信息
            self.print_stats()
            
            return {
                'status': 'success', 
                'stats': self.stats,
                'result': result
            }
                
        except Exception as e:
            self.logger.exception(f"运行出错: {str(e)}")
            return {
                'status': 'error',
                'message': str(e),
                'stats': self.stats
            }
    
    def _run_recent_mode(self) -> Dict[str, Any]:
        """
        运行最近数据模式
        
        Returns:
            Dict[str, Any]: 执行结果
        """
        # 子类需要实现此方法
        self.logger.error("_run_recent_mode 方法需要子类实现")
        return {'status': 'not_implemented', 'message': '方法未实现'}
        
    def _run_full_mode(self) -> Dict[str, Any]:
        """
        运行全量数据模式
        
        Returns:
            Dict[str, Any]: 执行结果
        """
        # 子类需要实现此方法
        self.logger.error("_run_full_mode 方法需要子类实现")
        return {'status': 'not_implemented', 'message': '方法未实现'}
        
    def _run_date_mode(self, date: str) -> Dict[str, Any]:
        """
        运行指定日期模式
        
        Args:
            date: 日期 (YYYYMMDD格式)
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        # 子类需要实现此方法
        self.logger.error("_run_date_mode 方法需要子类实现")
        return {'status': 'not_implemented', 'message': '方法未实现'}
        
    def _run_date_range_mode(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        运行日期范围模式
        
        Args:
            start_date: 开始日期 (YYYYMMDD格式)
            end_date: 结束日期 (YYYYMMDD格式)
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        # 子类需要实现此方法
        self.logger.error("_run_date_range_mode 方法需要子类实现")
        return {'status': 'not_implemented', 'message': '方法未实现'}
        
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
        # 子类需要实现此方法
        self.logger.error("_run_stock_mode 方法需要子类实现")
        return {'status': 'not_implemented', 'message': '方法未实现'}