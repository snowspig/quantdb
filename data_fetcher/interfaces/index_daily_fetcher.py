#!/usr/bin/env python
"""
指数日线数据获取器 - 获取A股指数日线数据并保存到MongoDB

该脚本用于从湘财Tushare获取A股指数日线数据，并保存到MongoDB数据库中
该版本继承TushareFetcher基类，实现了与daily_fetcher相同的架构和功能

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=95

使用方法：
    python index_daily_fetcher.py                   # 使用湘财真实API数据，简洁日志模式，获取近期数据
    python index_daily_fetcher.py --verbose         # 使用湘财真实API数据，详细日志模式
    python index_daily_fetcher.py --start-date 20200101 --end-date 20231231  # 指定日期范围
    python index_daily_fetcher.py --serial          # 使用串行模式处理数据（默认为并行模式）
    python index_daily_fetcher.py --full            # 使用完整模式，按指数代码列表获取所有指数的基本数据
"""
import sys
import time
import json
import os
import pandas as pd
import threading
import queue
import concurrent.futures
import signal
import atexit
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from pathlib import Path
from loguru import logger
import random
import socket
import pymongo

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root))

# 导入平台核心模块
from core.tushare_fetcher import TushareFetcher
from core.mongodb_handler import MongoDBHandler, init_mongodb_handler

# 全局变量用于线程和进程控制
STOP_PROCESSING = False
executor_pool = None  # 全局线程池对象

# 信号处理函数
def signal_handler(sig, frame):
    global STOP_PROCESSING, executor_pool
    logger.warning("接收到中断信号(Ctrl+C)，正在强制退出...")
    STOP_PROCESSING = True
    
    # 强制关闭线程池
    if executor_pool:
        logger.info("正在关闭线程池...")
        executor_pool.shutdown(wait=False)
    
    # 强制退出程序
    logger.warning("程序被强制终止")
    os._exit(130)  # 使用os._exit强制退出

# 退出处理函数
def exit_handler():
    global STOP_PROCESSING
    if STOP_PROCESSING:
        logger.warning("程序正在通过退出处理器清理资源...")
    else:
        logger.info("程序正常退出")

# 共享配置加载函数
def load_shared_config(shared_config_path=None) -> Dict[str, Any]:
    """
    加载共享配置
    
    如果指定了共享配置路径，直接从文件加载
    否则尝试从环境变量获取路径
    
    Args:
        shared_config_path: 共享配置文件路径
        
    Returns:
        Dict[str, Any]: 共享配置字典
    """
    # 首先检查参数
    if shared_config_path:
        config_path = shared_config_path
    # 其次检查环境变量
    elif "QUANTDB_SHARED_CONFIG" in os.environ:
        config_path = os.environ.get("QUANTDB_SHARED_CONFIG")
    else:
        # 如果没有共享配置，返回空字典
        logger.debug("没有找到共享配置路径")
        return {}
    
    try:
        # 检查文件是否存在
        if not os.path.exists(config_path):
            logger.warning(f"共享配置文件不存在：{config_path}")
            return {}
        
        # 加载配置
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        logger.info(f"成功从共享配置中加载设置：{config_path}")
        return config
    except Exception as e:
        logger.error(f"加载共享配置失败：{str(e)}")
        return {}

def get_validation_status(shared_config: Dict[str, Any]) -> Dict[str, bool]:
    """
    从共享配置中获取验证状态
    
    Args:
        shared_config: 共享配置字典
        
    Returns:
        Dict[str, bool]: 验证状态字典
    """
    validation_summary = shared_config.get("validation_summary", {})
    return validation_summary

class IndexDailyFetcher(TushareFetcher):
    """
    指数日线数据获取器
    
    该类用于从Tushare获取指数日线数据并保存到MongoDB数据库
    使用TushareFetcher基类提供的通用功能
    支持串行和并行两种处理模式
    支持按日期和按指数代码两种抓取模式
    """
    
    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "index_daily.json",
        db_name: str = None,
        collection_name: str = "index_daily",
        start_date: str = None,
        end_date: str = None,
        verbose: bool = False,
        shared_config: Dict[str, Any] = None,
        skip_validation: bool = False,
        serial_mode: bool = False,  # 是否使用串行模式
        max_workers: int = 3,  # 并行模式下的最大工作线程数
        full_mode: bool = False,  # 是否使用完整模式（按指数代码抓取）
        mongo_handler_instance: Optional[MongoDBHandler] = None # 新增参数
    ):
        """
        初始化指数日线数据获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            db_name: MongoDB数据库名称，如果为None则从配置文件中读取
            collection_name: MongoDB集合名称
            start_date: 开始日期（格式：YYYYMMDD，默认为当前日期前一周）
            end_date: 结束日期（格式：YYYYMMDD，默认为当前日期）
            verbose: 是否输出详细日志
            shared_config: 共享配置字典
            skip_validation: 是否跳过验证
            serial_mode: 是否使用串行模式
            max_workers: 并行模式下的最大工作线程数
            full_mode: 是否使用完整模式（按指数代码抓取）
            mongo_handler_instance: 显式传入的 MongoDBHandler 实例
        """
        # 使用共享配置中的设置（如果有）
        if shared_config:
            # 可以从共享配置中获取配置文件路径
            config_path = shared_config.get("config_file", config_path)
            # 获取验证状态
            validation_status = get_validation_status(shared_config)
            skip_validation = skip_validation or validation_status.get("all_valid", False)
            
            logger.info(f"使用共享配置：配置文件={config_path}, 跳过验证={skip_validation}")
        
        # 保存skip_validation状态，但不传递给父类
        self.skip_validation = skip_validation
        
        # 检查TushareFetcher是否支持skip_validation参数
        import inspect
        parent_params = inspect.signature(TushareFetcher.__init__).parameters
        parent_args = {}
        
        # 基本参数
        parent_args['config_path'] = config_path
        parent_args['interface_dir'] = interface_dir
        parent_args['interface_name'] = interface_name
        parent_args['db_name'] = db_name
        parent_args['collection_name'] = collection_name
        parent_args['verbose'] = verbose
        
        # 如果父类支持skip_validation，则添加
        if 'skip_validation' in parent_params:
            parent_args['skip_validation'] = skip_validation
            if verbose:
                logger.debug("TushareFetcher支持skip_validation参数")
        else:
            logger.debug("TushareFetcher不支持skip_validation参数，将在子类中处理")
        
        # 添加传递的 MongoDB Handler 实例
        parent_args['mongo_handler_instance'] = mongo_handler_instance
        
        # 调用父类初始化方法
        super().__init__(**parent_args)
        
        # 将传入的实例也保存在子类中，供 get_trade_dates 使用
        self.mongodb_handler = mongo_handler_instance
        
        # 设置默认日期范围（如果未提供）
        if not start_date or not end_date:
            today = datetime.now()
            if not end_date:
                self.end_date = today.strftime("%Y%m%d")
            else:
                self.end_date = end_date
                
            if not start_date:
                # 默认获取最近一周的数据
                one_week_ago = today - timedelta(days=7)
                self.start_date = one_week_ago.strftime("%Y%m%d")
            else:
                self.start_date = start_date
        else:
            self.start_date = start_date
            self.end_date = end_date
        
        self.serial_mode = serial_mode
        self.full_mode = full_mode
        
        # 获取可用的WAN口数量
        try:
            # 获取端口分配器
            from core.wan_manager import get_port_allocator
            self.port_allocator = get_port_allocator()
            
            # 从port_allocator获取WAN口信息
            self.available_wan_indices = self.port_allocator.get_available_wan_indices()
            self.available_wan_count = len(self.available_wan_indices)
            
            logger.info(f"获取到WAN口索引: {self.available_wan_indices}")
        except ImportError as e:
            logger.error(f"导入端口分配器失败: {str(e)}")
            self.available_wan_indices = []
            self.available_wan_count = 0
        
        # 如果没有可用WAN口，检查wan配置
        if self.available_wan_count == 0:
            # 从配置中直接获取WAN列表
            wan_config = self.config.get("wan", {})
            wan_list = wan_config.get("wan_list", [])
            logger.info(f"配置中的WAN列表: {wan_list}")
            
            logger.warning("未找到可用的WAN口，将使用串行模式")
            self.serial_mode = True
            self.max_workers = 1
        else:
            # 确保并行线程数不超过可用WAN口数量
            if max_workers > self.available_wan_count:
                logger.warning(f"指定的线程数({max_workers})超过了可用WAN口数量({self.available_wan_count})，将自动调整")
                self.max_workers = self.available_wan_count
            else:
                self.max_workers = max_workers
        
        # 存储WAN口使用情况
        self.wan_locks = {}
        for wan_idx in self.available_wan_indices:
            self.wan_locks[wan_idx] = threading.Lock()
            
        # 如果没有WAN锁，至少创建一个默认锁以避免错误
        if not self.wan_locks:
            # 添加默认WAN锁用于串行模式
            self.wan_locks[0] = threading.Lock()
        
        # 用于存储并行处理结果的队列
        self.result_queue = queue.Queue()
        
        # 日志输出
        logger.info(f"日期范围: {self.start_date} - {self.end_date}")
        logger.info(f"处理模式: {'串行' if self.serial_mode else '并行'}, 可用WAN口数量: {self.available_wan_count}, 并行线程数: {self.max_workers}")
        if self.full_mode:
            logger.info("抓取模式: 完整模式(按指数代码)，将抓取所有历史数据而不限制日期范围")
        else:
            logger.info(f"抓取模式: 日期模式(按交易日)，日期范围: {self.start_date} - {self.end_date}")
        
        # 添加类型停止标志
        self.stop_processing = False
    
    def _get_wan_socket(self, wan_idx=None, retry_count=0):
        """
        获取一个WAN网络socket用于连接Tushare API
        
        Args:
            wan_idx: 指定WAN口索引，如果为None则随机选择
            retry_count: 重试次数
            
        Returns:
            元组 (socket对象, 端口号, WAN口索引)
        """
        # 如果超过最大重试次数，抛出异常
        max_retry = getattr(self, 'max_retry', 3)
        if retry_count > max_retry:
            logger.error(f"无法获取WAN端口，已重试{retry_count}次")
            return None
        
        # 端口分配器（如果未初始化，则尝试初始化）
        if not hasattr(self, 'port_allocator') or self.port_allocator is None:
            try:
                # 使用新的getter函数获取端口分配器
                from core.wan_manager import get_port_allocator
                self.port_allocator = get_port_allocator()
                
                # 获取可用WAN口
                self.available_wan_indices = self.port_allocator.get_available_wan_indices()
                self.available_wan_count = len(self.available_wan_indices)
                logger.debug(f"获取到的WAN口索引: {self.available_wan_indices}")
            except Exception as e:
                logger.error(f"获取端口分配器失败: {str(e)}")
                logger.warning("获取WAN端口失败")
                return None
        
        # 如果未指定WAN口，随机选择一个可用的WAN口
        if wan_idx is None:
            if not self.available_wan_indices:
                logger.warning("无可用WAN口，尝试使用默认WAN口")
                wan_idx = 0  # 使用默认WAN口
            else:
                # 引入随机延迟，避免多进程同时申请同一个WAN口
                if retry_count == 0:
                    time.sleep(random.uniform(0, 0.5))
                wan_idx = random.choice(self.available_wan_indices)
        
        try:
            # 申请端口
            port = self.port_allocator.allocate_port(wan_idx)
            if not port:
                # 分配失败，记录日志
                logger.warning(f"WAN端口分配失败: wan_idx={wan_idx}，将重试")
                
                # 引入随机延迟，减少端口竞争
                time.sleep(random.uniform(0.5, 1.0))
                
                # 递归重试，尝试其他WAN口
                return self._get_wan_socket(None, retry_count + 1)
                
            logger.debug(f"成功分配WAN端口: wan_idx={wan_idx}, port={port}")
            
            # 创建socket对象
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
            # 绑定到分配的端口
            sock.bind(('0.0.0.0', port))
            
            return sock, port, wan_idx
        except Exception as e:
            # 分配失败，记录日志
            logger.warning(f"WAN端口分配失败: wan_idx={wan_idx}, 错误={str(e)}, 将重试")
            
            # 确保释放已分配的端口
            try:
                if 'port' in locals() and port:
                    self.port_allocator.release_port(wan_idx, port)
            except:
                pass
            
            # 引入随机延迟，减少端口竞争
            time.sleep(random.uniform(0.5, 1.0))
            
            # 递归重试，尝试其他WAN口
            return self._get_wan_socket(None, retry_count + 1)
    
    def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        从Tushare获取指数日线数据
        
        Args:
            **kwargs: 查询参数，包括：
                ts_code: 指数代码
                trade_date: 交易日期
                start_date: 开始日期
                end_date: 结束日期
                limit: 返回数据的记录条数，默认10000，最大不超过10000
                offset: 返回数据的开始记录位置，默认0
                wan_idx: 指定WAN口索引，可选
                use_wan: 是否使用WAN口，默认True
        
        Returns:
            返回DataFrame或者None（如果出错）
        """
        ts_code = kwargs.get('ts_code')
        trade_date = kwargs.get('trade_date')
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        limit = kwargs.get('limit')
        offset = kwargs.get('offset')
        
        # 是否使用WAN口
        use_wan = kwargs.get('use_wan', True)
        
        # 提取WAN口索引（如果指定了）
        wan_idx = kwargs.get('wan_idx')
        
        # 设置API参数
        params = {}
        if ts_code:
            params['ts_code'] = ts_code
        if trade_date:
            params['trade_date'] = trade_date
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
        if limit is not None:
            params['limit'] = limit
        if offset is not None:
            params['offset'] = offset
        
        # 参数检查：至少需要一个查询条件，或者limit+offset分页模式
        if not (trade_date or (start_date and end_date) or ts_code or (limit is not None and offset is not None)):
            logger.error("必须提供查询条件：ts_code、trade_date、start_date+end_date或limit+offset")
            return None
        
        # 设置WAN接口参数
        wan_info = None
        sock = None
        
        try:
            # 如果需要使用WAN接口，获取一个WAN socket
            if use_wan:
                wan_info = self._get_wan_socket(wan_idx)
                if not wan_info:
                    logger.warning("无法获取WAN接口，将不使用WAN")
                    use_wan = False
            
            if use_wan:
                sock, port, wan_idx = wan_info
                logger.debug(f"使用WAN接口 {wan_idx} 和本地端口 {port} 请求数据")
            
            # 调用 self.client.get_data，并传递 wan_idx
            result = self.client.get_data(
                api_name='index_daily', 
                params=params,
                wan_idx=wan_idx # 传递 wan_idx
            )
            
            # 将 ts_code 重命名为 index_code
            if result is not None and not result.empty and 'ts_code' in result.columns:
                result.rename(columns={'ts_code': 'index_code'}, inplace=True)
                
            return result
        except Exception as e:
            # 添加更详细的日志，包括 wan_idx
            logger.error(f"调用 self.client.get_data 失败 (WAN: {wan_idx}): {str(e)}")
            return None
        finally:
            # 注意：get_data 内部的 finally 块会处理端口释放和状态重置
            pass
    
    def get_trade_dates(self, start_date: str = None, end_date: str = None) -> List[str]:
        """
        获取指定日期范围内的交易日列表
        
        先从trade_cal集合中查询实际交易日，如果失败则生成日期范围内的所有日期
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            
        Returns:
            交易日期列表
        """
        # 使用传入的参数或默认参数
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date
        
        # 首先尝试从MongoDB的trade_cal集合中获取交易日数据
        try:
            # 确保MongoDB连接 (使用 self.mongodb_handler)
            if not self.mongodb_handler:
                logger.error("MongoDB Handler 未初始化")
                raise Exception("MongoDB Handler 未初始化")
            elif not self.mongodb_handler.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if not self.mongodb_handler.connect():
                    logger.error("连接MongoDB失败")
                    raise Exception("连接MongoDB失败")
            
            # 构建查询条件
            query = {
                "trade_date": {"$gte": start_date, "$lte": end_date}
            }
            
            # 查询trade_cal集合
            result = self.mongodb_handler.find_documents("trade_cal", query)
            
            # 提取日期列表并去重、排序
            trade_dates_with_duplicates = [doc.get("trade_date") for doc in result if "trade_date" in doc]
            # 去重并排序
            trade_dates = sorted(list(set(trade_dates_with_duplicates)))
            
            if trade_dates:
                logger.info(f"从trade_cal集合获取到 {len(trade_dates)} 个不重复的交易日")
                return trade_dates
            else:
                logger.warning("trade_cal集合中未找到符合条件的交易日数据")
        except Exception as e:
            logger.error(f"查询trade_cal交易日数据失败: {str(e)}")
        
        # 如果从trade_cal获取失败，则生成日期范围内的所有日期作为备选
        logger.warning("无法从trade_cal获取交易日，将生成日期范围内的所有日期作为备选")
        logger.info(f"生成日期范围 {start_date} 到 {end_date} 内的所有日期作为交易日")
        
        start_date_obj = datetime.strptime(start_date, '%Y%m%d')
        end_date_obj = datetime.strptime(end_date, '%Y%m%d')
        
        all_dates = []
        current_date = start_date_obj
        while current_date <= end_date_obj:
            all_dates.append(current_date.strftime('%Y%m%d'))
            current_date += timedelta(days=1)
        
        logger.info(f"生成日期范围内的所有日期，共 {len(all_dates)} 个日期")
        return all_dates
    
    def fetch_daily_data(self, trade_date: str) -> Optional[pd.DataFrame]:
        """
        获取指定交易日的指数数据
        
        Args:
            trade_date: 交易日，格式为YYYYMMDD
            
        Returns:
            指定交易日的数据，如果失败则返回None
        """
        logger.info(f"正在获取交易日 {trade_date} 的指数数据...")
        return self.fetch_data(trade_date=trade_date)
    
    def fetch_index_data(self, ts_code: str) -> Optional[pd.DataFrame]:
        """获取指定指数代码的数据"""
        logger.info(f"正在获取指数 {ts_code} 的日线数据...")
        
        # 创建参数字典
        params = {'ts_code': ts_code}
        
        # 在full模式下不添加日期范围
        if not self.full_mode:
            params['start_date'] = self.start_date
            params['end_date'] = self.end_date
        
        return self.fetch_data(**params)
    
    def run(self) -> bool:
        """
        运行数据获取和保存流程
        
        Returns:
            是否成功
        """
        try:
            # 第一步：检查并确保集合和索引存在
            logger.info("第一步：检查并确保MongoDB集合和索引存在")
            if not self._ensure_collection_and_indexes():
                logger.error("无法确保MongoDB集合和索引，放弃数据获取")
                return False
            
            # 根据模式走不同的处理流程
            if self.full_mode:
                # 完整模式：按分页方式获取所有指数数据
                logger.info("使用完整模式，按分页方式获取所有指数历史数据")
                
                # 设置分页参数
                limit = 10000  # 每页最大记录数
                offset = 0     # 起始偏移量
                has_more_data = True
                all_success = True
                total_fetched = 0
                
                # 循环获取所有数据
                while has_more_data and not STOP_PROCESSING:
                    logger.info(f"获取分页数据：limit={limit}, offset={offset}")
                    
                    try:
                        # 构建分页查询参数
                        params = {
                            'limit': limit,
                            'offset': offset
                        }
                        
                        # 获取单页数据
                        df = self.fetch_data(**params)
                        
                        if df is None or df.empty:
                            logger.info("未获取到数据，分页查询完成")
                            has_more_data = False
                            continue
                        
                        # 记录当前页数据量
                        current_count = len(df)
                        total_fetched += current_count
                        logger.info(f"获取到 {current_count} 条记录，累计 {total_fetched} 条")
                        
                        # 保存数据到MongoDB
                        success = self.save_to_mongodb(df)
                        if success:
                            logger.success(f"成功保存 {current_count} 条数据")
                        else:
                            logger.error(f"保存数据失败")
                            all_success = False
                        
                        # 判断是否还有更多数据
                        if current_count < limit:
                            has_more_data = False
                            logger.info("已获取所有数据，分页查询完成")
                        else:
                            # 增加偏移量，准备获取下一页
                            offset += limit
                            
                            # 增加随机延时，避免API限制
                            delay = random.uniform(1, 3)
                            logger.info(f"添加 {delay:.2f} 秒延时，避免API限制")
                            time.sleep(delay)
                            
                    except Exception as e:
                        logger.error(f"获取分页数据时发生异常: {str(e)}")
                        all_success = False
                        # 尝试继续获取下一页
                        offset += limit
                
                logger.info(f"完整模式查询完成，共获取 {total_fetched} 条记录")
                return all_success
            else:
                # 日期模式：按交易日获取
                logger.info("使用日期模式，按交易日获取数据")
                
                # 第二步：获取日期范围内的所有交易日
                logger.info(f"第二步：获取日期范围 {self.start_date} - {self.end_date} 内的交易日...")
                trade_dates = self.get_trade_dates(self.start_date, self.end_date)
                
                if not trade_dates:
                    logger.warning("未找到交易日，没有数据需要处理")
                    return True  # 没有数据也视为成功
                
                # 第三步：获取数据
                # 串行模式
                logger.info(f"第三步：串行处理 {len(trade_dates)} 个交易日的数据")
                all_success = True
                
                for trade_date in trade_dates:
                    # 检查是否收到停止信号
                    if STOP_PROCESSING:
                        logger.warning("收到停止信号，中断处理")
                        return False
                        
                    logger.info(f"正在处理交易日: {trade_date}")
                    
                    try:
                        # 获取单日数据
                        df = self.fetch_daily_data(trade_date)
                        if df is None or df.empty:
                            logger.warning(f"交易日 {trade_date} 的数据为空或获取失败")
                            continue
                        
                        # 保存单日数据到MongoDB
                        success = self.save_to_mongodb(df)
                        if success:
                            logger.success(f"交易日 {trade_date} 的数据已保存到MongoDB")
                        else:
                            logger.error(f"保存交易日 {trade_date} 的数据到MongoDB失败")
                            all_success = False
                    except Exception as e:
                        logger.error(f"处理交易日 {trade_date} 的数据时发生异常: {str(e)}")
                        all_success = False
                
                return all_success
            
            logger.info("数据获取和保存流程完成")
            return True
            
        except Exception as e:
            logger.error(f"运行过程中发生异常: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

def main():
    """主函数"""
    import argparse
    
    # 设置信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 注册退出处理函数
    atexit.register(exit_handler)
    
    logger.info("初始化中断处理，可以使用Ctrl+C安全退出程序")

    # 显式初始化 MongoDB Handler
    logger.info("初始化 MongoDB Handler...")
    mongo_instance = None
    try:
        mongo_instance = init_mongodb_handler()
        if mongo_instance:
             logger.info("MongoDB Handler 初始化成功。")
        else:
             logger.warning("init_mongodb_handler() 未初始化处理程序。")
    except Exception as e:
        logger.error(f"初始化 MongoDB Handler 失败: {e}")
    
    parser = argparse.ArgumentParser(description='获取指数日线数据并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--start-date', help='开始日期，格式：YYYYMMDD')
    parser.add_argument('--end-date', help='结束日期，格式：YYYYMMDD')
    parser.add_argument('--db-name', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='index_daily', help='MongoDB集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    parser.add_argument('--shared-config', type=str, default=None, help='共享配置文件路径')
    parser.add_argument('--skip-validation', action='store_true', help='跳过配置验证')
    parser.add_argument('--serial', action='store_true', help='使用串行模式处理数据（默认）')
    parser.add_argument('--max-workers', type=int, default=3, help='并行模式下的最大工作线程数（默认为3）')
    parser.add_argument('--full', action='store_true', help='使用完整模式，按指数代码列表获取所有指数的历史数据')
    
    args = parser.parse_args()
    
    # 根据verbose参数设置日志级别
    if not args.verbose:
        # 非详细模式下，设置日志级别为INFO，不显示DEBUG消息
        logger.remove()  # 移除所有处理器
        logger.add(sys.stderr, level="INFO")  # 添加标准错误输出处理器，级别为INFO
    
    try:
        # 加载共享配置（如果有）
        shared_config = load_shared_config(args.shared_config)
        
        # 使用共享配置中的验证状态
        if shared_config:
            validation_status = get_validation_status(shared_config)
            logger.info(f"从共享配置获取验证状态：{validation_status}")
            
            # 如果共享配置中指定了配置文件路径，优先使用
            if "config_file" in shared_config and not args.config:
                args.config = shared_config.get("config_file")
                logger.info(f"从共享配置获取配置文件路径：{args.config}")
        
        # 创建获取器并运行 - 传入 mongo_instance
        fetcher = IndexDailyFetcher(
            config_path=args.config,
            interface_dir=args.interface_dir,
            start_date=args.start_date,
            end_date=args.end_date,
            db_name=args.db_name,
            collection_name=args.collection_name,
            verbose=args.verbose,
            shared_config=shared_config,
            skip_validation=args.skip_validation,
            serial_mode=args.serial,
            max_workers=args.max_workers,
            full_mode=args.full,
            mongo_handler_instance=mongo_instance # 显式传入实例
        )
        
        try:
            success = fetcher.run()
            
            if success:
                logger.success("指数日线数据获取和保存成功")
                return 0
            else:
                logger.error("指数日线数据获取或保存失败")
                return 1
        except KeyboardInterrupt:
            logger.warning("接收到Ctrl+C，正在强制退出...")
            return 130  # 标准的SIGINT退出码
        
    except Exception as e:
        logger.error(f"运行过程中发生异常: {str(e)}")
        import traceback
        logger.error(f"详细错误信息: {traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.warning("程序被用户中断")
        os._exit(130)  # 使用os._exit强制退出 