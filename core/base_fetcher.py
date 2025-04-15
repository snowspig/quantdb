#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
基础数据获取器模块
为所有数据获取器提供统一的基类和基础功能
"""
import os
import time
import logging
import threading
import traceback
import pandas as pd
import concurrent.futures
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional

# 导入核心组件
from .config_manager import config_manager
from .mongodb_handler import mongodb_handler

class BaseFetcher(ABC):
    """
    数据获取器基类
    提供通用的数据获取、处理和存储功能，包括：
    1. 多种运行模式支持（recent、date、range、full等）
    2. 并行数据获取
    3. 数据处理和存储
    4. 日志记录
    
    子类需要实现以下方法：
    - fetch_data: 从数据源获取数据
    - process_data: 处理获取的原始数据
    - get_collection_name: 获取数据存储的集合名称
    
    可选实现方法：
    - get_recent_date: 获取最近的交易日期（有默认实现）
    - get_date_range: 获取日期范围内的所有日期（有默认实现）
    """
    
    def __init__(self, 
                 config_path: str = "config/config.yaml",
                 interface_dir: str = "config/interfaces",
                 interface_name: str = None,
                 db_name: str = None,
                 collection_name: str = None,
                 verbose: bool = False,
                 shared_config: Dict[str, Any] = None,
                 skip_validation: bool = False,
                 mongo_handler_instance: Optional[Any] = None # 添加参数，接受外部传入的MongoDB处理器实例
                 ):
        """
        初始化基础抓取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口配置文件名称
            db_name: MongoDB数据库名称
            collection_name: MongoDB集合名称
            verbose: 是否输出详细日志
            shared_config: 共享配置
            skip_validation: 是否跳过配置验证
            mongo_handler_instance: 外部传入的MongoDB处理器实例
        """
        self.config_path = config_path
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.collection_name = collection_name
        self.verbose = verbose
        self.skip_validation = skip_validation
        self.db_name = db_name
        self.shared_config = shared_config
        self.mongo_handler_instance = mongo_handler_instance
        
        # 添加错误消息列表，用于记录运行过程中的错误
        self.error_messages = []
        
        # 初始化日志
        self._setup_logging()
        
        # 加载配置
        self.interface_config = config_manager.get_interface_config(self.interface_name)
        if not self.interface_config:
            self.logger.warning(f"接口配置不存在: {self.interface_name}.json，将使用默认配置")
            self.interface_config = {}
            
        # 获取抓取器配置
        fetcher_config = config_manager.get_fetch_config(self.interface_name)
        default_config = config_manager.get_default_fetcher_config()
        
        # 设置参数
        self.thread_count = fetcher_config.get('thread_count', default_config.get('thread_count', 5))
        self.batch_size = fetcher_config.get('batch_size', default_config.get('batch_size', 100))
        self.retry_count = fetcher_config.get('retry_count', default_config.get('retry_count', 3))
        self.retry_interval = fetcher_config.get('retry_interval', default_config.get('retry_interval', 1))
        self.timeout = fetcher_config.get('timeout', default_config.get('timeout', 30))
        
        # 任务执行计数器
        self._task_count = 0
        self._success_count = 0
        self._fail_count = 0
        self._task_lock = threading.Lock()
        
        # 初始化完成
        self.logger.info(f"{self.__class__.__name__} 初始化完成")
            
    def _setup_logging(self):
        """设置日志记录"""
        self.logger = logging.getLogger(f"core.BaseFetcher.{self.interface_name}")
        
        if not self.logger.handlers:
            # 控制台处理器
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            self.logger.addHandler(console_handler)
            
            # 文件处理器
            try:
                log_dir = 'logs'
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)
                    
                file_handler = logging.FileHandler(os.path.join(log_dir, f"{self.interface_name}.log"))
                file_handler.setFormatter(logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                ))
                self.logger.addHandler(file_handler)
            except Exception as e:
                pass  # 忽略文件处理器创建失败的异常
            
            # 设置日志级别
            if self.verbose:
                self.logger.setLevel(logging.INFO)
            else:
                self.logger.setLevel(logging.WARNING)
    
    @abstractmethod
    def fetch_data(self, **kwargs) -> pd.DataFrame:
        """
        从数据源获取数据
        
        Args:
            **kwargs: 查询参数
            
        Returns:
            pd.DataFrame: 获取的数据
        """
        pass
    
    @abstractmethod
    def process_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        处理获取的原始数据
        
        Args:
            data: 原始数据
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        pass
    
    @abstractmethod
    def get_collection_name(self) -> str:
        """
        获取数据存储的集合名称
        
        Returns:
            str: 集合名称
        """
        pass
    
    def get_recent_date(self) -> str:
        """
        获取最近的交易日期
        
        Returns:
            str: 日期字符串 (YYYYMMDD格式)
        """
        try:
            # 尝试从数据库获取最近交易日期
            collection = self.get_collection_name()
            recent_data = mongodb_handler.find_one(
                collection,
                sort=[('trade_date', -1)],
                projection={'trade_date': 1, '_id': 0}
            )
            
            if recent_data and 'trade_date' in recent_data:
                # 如果是datetime对象，转换为字符串
                trade_date = recent_data['trade_date']
                if isinstance(trade_date, datetime):
                    return trade_date.strftime('%Y%m%d')
                return str(trade_date)
            
            # 如果数据库中没有数据，使用当前日期
            return datetime.now().strftime('%Y%m%d')
            
        except Exception as e:
            self.logger.error(f"获取最近交易日期失败: {str(e)}")
            # 默认返回当前日期
            return datetime.now().strftime('%Y%m%d')
    
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
            # 转换为日期对象
            start = datetime.strptime(start_date, '%Y%m%d')
            end = datetime.strptime(end_date, '%Y%m%d')
            
            # 生成日期范围
            date_list = []
            current = start
            while current <= end:
                # 工作日过滤（可选）
                # if current.weekday() < 5:  # 0-4 表示周一至周五
                date_list.append(current.strftime('%Y%m%d'))
                current += timedelta(days=1)
                
            return date_list
            
        except Exception as e:
            self.logger.error(f"生成日期范围失败: {str(e)}")
            return []
    
    def save_data(self, data: pd.DataFrame) -> int:
        """
        保存数据到数据库
        
        Args:
            data: 要保存的数据
            
        Returns:
            int: 保存的记录数
        """
        if data.empty:
            return 0
            
        try:
            # 获取集合名称
            collection_name = self.get_collection_name()
            
            # 将DataFrame转换为字典列表
            records = data.to_dict('records')
            if not records:
                return 0
                
            # 插入数据库
            result_ids = mongodb_handler.insert_many(collection_name, records, ordered=False)
            saved_count = len(result_ids)
            
            if saved_count > 0:
                self.logger.debug(f"成功保存 {saved_count} 条记录到集合 {collection_name}")
                
                # 确保必要的索引存在
                self._ensure_indexes(collection_name, data.columns)
                
            return saved_count
            
        except Exception as e:
            self.logger.error(f"保存数据失败: {str(e)}")
            traceback.print_exc()
            return 0
    
    def _ensure_indexes(self, collection_name: str, columns: List[str]):
        """
        确保必要的索引存在
        
        Args:
            collection_name: 集合名称
            columns: 数据列名列表
        """
        try:
            # 索引映射
            common_indexes = {
                # 时间索引
                'trade_date': 'date',
                'ann_date': 'date',
                'start_date': 'date',
                'end_date': 'date',
                'report_date': 'date',
                
                # 代码索引
                'ts_code': 'code',
                'code': 'code',
                'symbol': 'code',
                
                # 数值索引
                'close': 'number',
                'open': 'number',
                'high': 'number',
                'low': 'number',
                'vol': 'number',
                'amount': 'number'
            }
            
            # 创建字段映射
            field_mappings = {}
            for col in columns:
                if col in common_indexes:
                    field_mappings[col] = common_indexes[col]
            
            # 创建索引
            if field_mappings:
                mongodb_handler.create_standard_indexes(collection_name, field_mappings)
                
        except Exception as e:
            self.logger.warning(f"创建索引失败: {str(e)}")
    
    def fetch_and_save(self, **kwargs) -> int:
        """
        获取并保存数据
        
        Args:
            **kwargs: 查询参数
            
        Returns:
            int: 保存的记录数
        """
        try:
            # 获取数据
            data = self.fetch_data(**kwargs)
            
            if data is None or data.empty:
                self.logger.debug(f"未获取到数据: {kwargs}")
                return 0
                
            # 处理数据
            processed_data = self.process_data(data)
            
            if processed_data is None or processed_data.empty:
                self.logger.debug(f"处理后的数据为空: {kwargs}")
                return 0
                
            # 保存数据
            saved_count = self.save_data(processed_data)
            
            return saved_count
            
        except Exception as e:
            self.logger.error(f"获取并保存数据失败: {str(e)}, 参数: {kwargs}")
            return 0
    
    def parallel_fetch(self, tasks: List[Dict], thread_count: int = None) -> int:
        """
        并行获取数据
        
        Args:
            tasks: 任务参数列表
            thread_count: 线程数
            
        Returns:
            int: 保存的总记录数
        """
        if not tasks:
            self.logger.info("没有任务需要执行")
            return 0
            
        # 设置线程数
        if thread_count is None:
            thread_count = self.thread_count
        
        thread_count = min(thread_count, len(tasks))
        
        # 重置计数器
        self._task_count = len(tasks)
        self._success_count = 0
        self._fail_count = 0
        
        # 保存的总记录数
        total_saved = 0
        
        self.logger.info(f"开始执行 {len(tasks)} 个任务，线程数: {thread_count}")
        
        # 定义任务处理函数
        def process_task(task):
            try:
                # 执行任务
                saved_count = self.fetch_and_save(**task)
                
                # 更新计数器
                with self._task_lock:
                    if saved_count > 0:
                        self._success_count += 1
                    else:
                        self._fail_count += 1
                
                return saved_count
                
            except Exception as e:
                # 更新失败计数
                with self._task_lock:
                    self._fail_count += 1
                    
                self.logger.error(f"任务执行失败: {str(e)}, 参数: {task}")
                return 0
        
        # 使用线程池并行执行任务
        with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
            # 提交所有任务
            future_to_task = {executor.submit(process_task, task): i for i, task in enumerate(tasks)}
            
            # 处理完成的任务
            for i, future in enumerate(concurrent.futures.as_completed(future_to_task)):
                task_index = future_to_task[future]
                try:
                    saved_count = future.result()
                    total_saved += saved_count
                    
                    # 打印进度
                    completed = i + 1
                    if completed % 10 == 0 or completed == len(tasks):
                        progress = completed / len(tasks) * 100
                        self.logger.info(f"进度: {progress:.1f}% ({completed}/{len(tasks)}), "
                                         f"成功: {self._success_count}, 失败: {self._fail_count}, "
                                         f"总保存记录数: {total_saved}")
                        
                except Exception as e:
                    self.logger.error(f"获取任务结果失败: {str(e)}")
        
        self.logger.info(f"任务执行完成，成功: {self._success_count}, 失败: {self._fail_count}, "
                          f"总保存记录数: {total_saved}")
        
        return total_saved
    
    def get_task_status(self) -> Dict:
        """
        获取任务执行状态
        
        Returns:
            Dict: 任务状态
        """
        return {
            'total': self._task_count,
            'success': self._success_count,
            'failed': self._fail_count,
            'completion_rate': self._success_count / self._task_count if self._task_count > 0 else 0
        }
    
    def run(self, mode: str = 'recent', **kwargs) -> Dict[str, Any]:
        """
        运行数据获取
        
        Args:
            mode: 运行模式，支持以下模式：
                - 'recent': 获取最近数据
                - 'date': 获取特定日期数据
                - 'range': 获取日期范围数据
                - 'full': 获取全量历史数据
                - 'stock': 获取特定股票的数据
            **kwargs: 其他参数，根据模式不同而不同：
                - date模式: date (日期字符串)
                - range模式: start_date, end_date (日期字符串)
                - stock模式: ts_code, start_date, end_date
                
        Returns:
            Dict[str, Any]: 运行结果
        """
        self.logger.info(f"开始运行 {self.__class__.__name__}，模式: {mode}")
        
        start_time = time.time()
        
        try:
            # 根据不同模式调用相应的处理方法
            if mode == 'recent':
                result = self._run_recent_mode()
            elif mode == 'date':
                date = kwargs.get('date')
                if not date:
                    return {'status': 'error', 'message': '日期参数缺失'}
                result = self._run_date_mode(date)
            elif mode == 'range':
                start_date = kwargs.get('start_date')
                end_date = kwargs.get('end_date')
                if not start_date or not end_date:
                    return {'status': 'error', 'message': '日期范围参数缺失'}
                result = self._run_date_range_mode(start_date, end_date)
            elif mode == 'full':
                result = self._run_full_mode()
            elif mode == 'stock':
                ts_code = kwargs.get('ts_code')
                start_date = kwargs.get('start_date')
                end_date = kwargs.get('end_date')
                if not ts_code or not start_date or not end_date:
                    return {'status': 'error', 'message': '股票代码或日期参数缺失'}
                result = self._run_stock_mode(ts_code, start_date, end_date)
            else:
                return {'status': 'error', 'message': f'不支持的运行模式: {mode}'}
                
            # 添加运行时间信息
            result['execution_time'] = time.time() - start_time
            
            # 记录结果
            if result.get('status') == 'success':
                self.logger.info(f"运行完成，耗时: {result['execution_time']:.2f}秒")
            else:
                self.logger.error(f"运行失败: {result.get('message', '未知错误')}")
                
            return result
            
        except Exception as e:
            self.logger.error(f"运行异常: {str(e)}")
            traceback.print_exc()
            return {
                'status': 'error',
                'message': str(e),
                'execution_time': time.time() - start_time
            }
    
    def _run_recent_mode(self) -> Dict[str, Any]:
        """
        运行最近数据模式：获取最近一个交易日的数据
        
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.warning("_run_recent_mode 方法未实现")
        return {'status': 'error', 'message': '未实现的方法'}
    
    def _run_date_mode(self, date: str) -> Dict[str, Any]:
        """
        运行指定日期模式：获取特定日期数据
        
        Args:
            date: 日期字符串 (YYYYMMDD格式)
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.warning("_run_date_mode 方法未实现")
        return {'status': 'error', 'message': '未实现的方法'}
    
    def _run_date_range_mode(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        运行日期范围模式：获取日期范围内的数据
        
        Args:
            start_date: 开始日期 (YYYYMMDD格式)
            end_date: 结束日期 (YYYYMMDD格式)
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.warning("_run_date_range_mode 方法未实现")
        return {'status': 'error', 'message': '未实现的方法'}
    
    def _run_full_mode(self) -> Dict[str, Any]:
        """
        运行全量数据模式：获取全部历史数据
        
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.warning("_run_full_mode 方法未实现")
        return {'status': 'error', 'message': '未实现的方法'}
    
    def _run_stock_mode(self, ts_code: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        运行指定股票模式：获取特定股票的数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期 (YYYYMMDD格式)
            end_date: 结束日期 (YYYYMMDD格式)
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        self.logger.warning("_run_stock_mode 方法未实现")
        return {'status': 'error', 'message': '未实现的方法'}