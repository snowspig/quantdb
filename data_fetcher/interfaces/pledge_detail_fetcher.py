#!/usr/bin/env python
"""
pledge_detail Basic Fetcher V2 - 获取股权质押明细并保存到MongoDB

该脚本用于从湘财Tushare获取股权质押明细，并保存到MongoDB数据库中
该版本继承TushareFetcher基类，实现了与stock_basic_fetcher相同的架构和功能

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=10110

使用方法：
    python pledge_detail_fetcher.py                   # 使用湘财真实API数据，简洁日志模式
    python pledge_detail_fetcher.py --verbose         # 使用湘财真实API数据，详细日志模式
    python pledge_detail_fetcher.py --exchange SZSE   # 获取深交所的股票数据
    python pledge_detail_fetcher.py --serial          # 使用串行模式处理数据（默认为并行模式）

注意：pledge_detail接口使用limit和offset参数分批获取所有数据，直到数据量小于limit停止
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

class pledge_detailFetcher(TushareFetcher):
    """
    股权质押明细获取器V2
    
    该类用于从Tushare获取股权质押明细并保存到MongoDB数据库
    使用TushareFetcher基类提供的通用功能
    支持串行和并行两种处理模式
    由于API限制，只能按股票代码逐个抓取数据
    """
    
    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "pledge_detail.json",
        db_name: str = None,
        collection_name: str = "pledge_detail",
        exchange: str = "SSE",  # 默认上交所
        verbose: bool = False,
        shared_config: Dict[str, Any] = None,
        skip_validation: bool = False,
        serial_mode: bool = False,  # 是否使用串行模式
        max_workers: int = 3,  # 并行模式下的最大工作线程数
        mongo_handler_instance: Optional[MongoDBHandler] = None # 显式传入的 MongoDBHandler 实例
    ):
        """
        初始化股权质押明细获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            db_name: MongoDB数据库名称，如果为None则从配置文件中读取
            collection_name: MongoDB集合名称
            exchange: 交易所代码（SSE：上交所，SZSE：深交所，默认SSE）
            verbose: 是否输出详细日志
            shared_config: 共享配置字典
            skip_validation: 是否跳过验证
            serial_mode: 是否使用串行模式
            max_workers: 并行模式下的最大工作线程数
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
        
        # 将传入的实例也保存在子类中，供后续使用
        self.mongodb_handler = mongo_handler_instance
        
        self.exchange = exchange
        
        # 保存模式设置
        self.serial_mode = serial_mode
        
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
        logger.info(f"交易所: {self.exchange}")
        logger.info(f"处理模式: {'串行' if self.serial_mode else '并行'}, 可用WAN口数量: {self.available_wan_count}, 并行线程数: {self.max_workers}")
        logger.info("抓取模式: 按股票代码逐个抓取(pledge_detail接口限制)")
        
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
                    time.sleep(random.uniform(0.5, 1.5))  # 增加初始随机延迟
                wan_idx = random.choice(self.available_wan_indices)
        
        # 如果是重试，添加指数退避延迟
        if retry_count > 0:
            backoff_delay = min(30, (2 ** retry_count)) * random.uniform(1.0, 2.0)
            logger.warning(f"WAN端口分配重试 #{retry_count}，等待 {backoff_delay:.2f} 秒")
            time.sleep(backoff_delay)
        
        try:
            # 申请端口前，确保端口分配器就绪
            if not hasattr(self.port_allocator, 'allocate_port'):
                logger.error("端口分配器不包含allocate_port方法")
                raise AttributeError("端口分配器不包含allocate_port方法")
                
            # 申请端口
            port = self.port_allocator.allocate_port(wan_idx)
            if not port:
                # 分配失败，记录日志
                logger.warning(f"WAN端口分配失败: wan_idx={wan_idx}，将重试")
                
                # 引入随机延迟，减少端口竞争
                time.sleep(random.uniform(2.0, 4.0))
                
                # 尝试获取当前已分配端口信息，帮助诊断
                if hasattr(self.port_allocator, 'get_allocated_ports'):
                    try:
                        allocated = self.port_allocator.get_allocated_ports(wan_idx)
                        logger.info(f"WAN {wan_idx} 已分配端口: {allocated}")
                    except:
                        pass
                
                # 递归重试，尝试其他WAN口
                return self._get_wan_socket(None, retry_count + 1)
                
            logger.debug(f"成功分配WAN端口: wan_idx={wan_idx}, port={port}")
            
            # 创建socket对象，启用地址重用选项
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            try:
                # 绑定到分配的端口，添加错误处理
                sock.bind(('0.0.0.0', port))
            except socket.error as bind_error:
                # 端口绑定失败，释放端口并记录日志
                logger.error(f"端口绑定失败: wan_idx={wan_idx}, port={port}, 错误={str(bind_error)}")
                try:
                    self.port_allocator.release_port(wan_idx, port)
                    logger.info(f"已释放端口: wan_idx={wan_idx}, port={port}")
                except Exception as release_err:
                    logger.error(f"释放端口失败: {str(release_err)}")
                
                # 添加额外延迟让端口有时间释放
                time.sleep(random.uniform(3.0, 6.0))
                
                # 递归重试
                return self._get_wan_socket(None, retry_count + 1)
            
            return sock, port, wan_idx
        except Exception as e:
            # 分配失败，记录日志
            logger.warning(f"WAN端口分配失败: wan_idx={wan_idx}, 错误={str(e)}, 将重试")
            
            # 确保释放已分配的端口
            try:
                if 'port' in locals() and port:
                    self.port_allocator.release_port(wan_idx, port)
            except Exception as release_err:
                logger.error(f"释放端口失败: {str(release_err)}")
            
            # 引入随机延迟，减少端口竞争
            time.sleep(random.uniform(3.0, 6.0))
            
            # 递归重试，尝试其他WAN口
            return self._get_wan_socket(None, retry_count + 1)
    
    def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        从Tushare获取股权质押明细
        
        Args:
            **kwargs: 查询参数，包括：
                ts_code: 股票代码(可选)
                limit: 单次获取最大条数，默认10000
                offset: 偏移量，默认0
                wan_idx: 指定WAN口索引，可选
                use_wan: 是否使用WAN口，默认True
        
        Returns:
            返回DataFrame或者None（如果出错）
        """
        ts_code = kwargs.get('ts_code')
        limit = kwargs.get('limit', 10000)  # 默认获取10000条数据
        offset = kwargs.get('offset', 0)    # 默认偏移量为0
        
        # 是否使用WAN口
        use_wan = kwargs.get('use_wan', True)
        
        # 提取WAN口索引（如果指定了）
        wan_idx = kwargs.get('wan_idx')
        
        # 打印详细信息
        param_msg = f"【抓取参数】limit={limit} offset={offset}"
        if ts_code:
            param_msg += f" 股票代码={ts_code}"
        if wan_idx is not None:
            param_msg += f" (WAN口={wan_idx})"
        logger.debug(param_msg)
        
        # 设置API参数
        params = {'limit': limit, 'offset': offset}
        if ts_code:
            params['ts_code'] = ts_code
        
        # 设置最大重试次数和退避策略
        max_retries = 3
        retry_count = 0
        result = None
        
        # 循环尝试，直到成功或达到最大重试次数
        while retry_count < max_retries:
            # 设置WAN接口参数
            wan_info = None
            sock = None
            
            try:
                # 如果需要使用WAN接口，获取一个WAN socket
                if use_wan:
                    # 添加随机延迟，避免多个线程同时申请端口
                    if retry_count > 0:
                        # 使用指数退避策略，每次重试增加等待时间
                        backoff_time = (2 ** retry_count) * random.uniform(2.0, 5.0)
                        logger.warning(f"第{retry_count+1}次重试，等待{backoff_time:.2f}秒后重试")
                        time.sleep(backoff_time)
                        
                    wan_info = self._get_wan_socket(wan_idx)
                    if not wan_info:
                        logger.warning("无法获取WAN接口，将不使用WAN")
                        use_wan = False
                
                if use_wan and wan_info:
                    sock, port, wan_idx = wan_info
                    logger.debug(f"使用WAN接口 {wan_idx} 和本地端口 {port} 请求数据")
                
                # 调用 self.client.get_data，并传递 wan_idx
                result = self.client.get_data(
                    api_name='pledge_detail', 
                    params=params,
                    wan_idx=wan_idx # 传递 wan_idx
                )
                
                # 如果成功获取数据，跳出循环
                if result is not None:
                    break
                    
            except ConnectionError as conn_err:
                # 特殊处理端口占用错误
                if "WinError 10048" in str(conn_err):
                    retry_count += 1
                    logger.warning(f"端口占用错误 (WinError 10048)，第 {retry_count}/{max_retries} 次重试")
                    # 连接错误后，添加更长的等待时间让端口释放
                    time.sleep(random.uniform(10.0, 20.0))
                    continue
                else:
                    # 其他连接错误
                    logger.error(f"连接错误: {str(conn_err)}")
                    return None
                    
            except Exception as e:
                # 添加更详细的日志，包括 wan_idx
                error_msg = f"调用 API 失败 (WAN: {wan_idx}): {str(e)}"
                if retry_count < max_retries - 1:
                    retry_count += 1
                    logger.warning(f"{error_msg}，第 {retry_count}/{max_retries} 次重试")
                    time.sleep(random.uniform(5.0, 10.0))
                    continue
                else:
                    logger.error(error_msg)
                    return None
            finally:
                # 确保资源被释放
                try:
                    if sock:
                        sock.close()
                except:
                    pass
        
        # 返回结果
        return result
    
    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        处理获取的股权质押明细
        只保留股票代码前两位为00、30、60、68的数据
        
        Args:
            df: 原始股权质押明细
            
        Returns:
            处理后的数据
        """
        if df is None or df.empty:
            return df
        
        # 检查是否存在ts_code字段
        if 'ts_code' not in df.columns:
            logger.warning("数据中不包含ts_code字段，无法按板块过滤")
            return df
        
        # 提取ts_code前两位数字
        try:
            # 假设ts_code格式为: 000001.SZ，我们需要提取000001的前两位
            df['code_prefix'] = df['ts_code'].apply(lambda x: x.split('.')[0][:2])
            
            # 过滤保留00、30、60、68开头的股票
            target_prefixes = ['00', '30', '60', '68']
            filtered_df = df[df['code_prefix'].isin(target_prefixes)].copy()
            
            # 删除临时列
            if 'code_prefix' in filtered_df.columns:
                filtered_df = filtered_df.drop('code_prefix', axis=1)
            
            original_count = len(df)
            filtered_count = len(filtered_df)
            
            logger.info(f"股票数据过滤: 原始 {original_count} 条，过滤后 {filtered_count} 条 (保留00、30、60、68板块)")
            
            # 记录过滤后的板块分布
            if filtered_count > 0 and self.verbose:
                prefix_counts = df[df['code_prefix'].isin(target_prefixes)]['code_prefix'].value_counts().to_dict()
                logger.debug(f"各板块数据量: {prefix_counts}")
            
            return filtered_df
        except Exception as e:
            logger.warning(f"过滤股票板块时发生异常: {str(e)}，返回原始数据")
            return df
    
    def get_trade_dates(self, start_date: str = None, end_date: str = None) -> List[str]:
        """
        获取指定日期范围内的公告日期列表
        
        先从trade_cal集合中查询实际交易日，如果失败则生成日期范围内的所有日期
        作为股权质押明细的公告日期范围
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            
        Returns:
            公告日期列表
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
            
            # 构建查询条件 - 移除 exchange 限制
            query = {
                "trade_date": {"$gte": start_date, "$lte": end_date}
                # "exchange": self.exchange # 移除交易所过滤
            }
            
            # 查询trade_cal集合
            result = self.mongodb_handler.find_documents("trade_cal", query)
            
            # 提取日期列表并去重、排序
            trade_dates_with_duplicates = [doc.get("trade_date") for doc in result if "trade_date" in doc]
            # 去重并排序
            trade_dates = sorted(list(set(trade_dates_with_duplicates)))
            
            if trade_dates:
                logger.info(f"从trade_cal集合获取到 {len(trade_dates)} 个不重复的交易日 (所有交易所)")
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
    
    def fetch_pledge_detail_data(self, ann_date: str) -> Optional[pd.DataFrame]:
        """
        获取指定公告日期的数据
        
        Args:
            ann_date: 公告日期，格式为YYYYMMDD
            
        Returns:
            指定公告日期的数据，如果失败则返回None
        """
        logger.info(f"正在获取公告日期 {ann_date} 的数据 (所有交易所)...")
        
        # 获取股票代码列表
        stock_codes = self.get_stock_codes()
        if not stock_codes:
            logger.error("未能获取到股票代码列表，无法获取股权质押明细数据")
            return None
            
        logger.info(f"获取到 {len(stock_codes)} 个股票代码，将逐个获取公告日期 {ann_date} 的数据")
        
        # 用于存储所有股票在该公告日期的数据
        all_data_frames = []
        
        # 每次处理一支股票，不使用批量处理
        processed_count = 0
        total_count = len(stock_codes)
        
        for i, ts_code in enumerate(stock_codes):
            try:
                # 每处理50个股票记录一次进度
                if i % 50 == 0:
                    logger.info(f"正在处理第 {i+1}/{total_count} 个股票: {ts_code}，公告日期 {ann_date}")
                
                # 对每个股票代码单独查询该公告日期的数据
                df = self.fetch_data(ts_code=ts_code, ann_date=ann_date)
                if df is not None and not df.empty:
                    all_data_frames.append(df)
                    processed_count += 1
                
                # 添加随机延时(3-8秒)，避免API限制
                delay = random.uniform(3.0, 8.0)
                logger.debug(f"API调用延时: {delay:.2f}秒")
                time.sleep(delay)
                
            except Exception as e:
                logger.warning(f"获取股票 {ts_code} 在公告日期 {ann_date} 的数据时出错: {str(e)}")
        
        logger.info(f"共处理了 {total_count} 个股票，成功获取 {processed_count} 个股票的数据")
        
        # 合并所有获取到的数据
        if all_data_frames:
            combined_df = pd.concat(all_data_frames, ignore_index=True)
            logger.info(f"公告日期 {ann_date} 合计获取到 {len(combined_df)} 条数据")
            return combined_df
        else:
            logger.warning(f"公告日期 {ann_date} 未获取到任何数据")
            return None
    
    def _process_date_with_wan(self, ann_date: str, wan_idx: int) -> bool:
        """
        使用指定的WAN口处理单个公告日期数据
        
        Args:
            ann_date: 公告日期
            wan_idx: 要使用的WAN口索引
            
        Returns:
            是否成功
        """
        # 检查WAN口索引是否有效
        if wan_idx not in self.wan_locks:
            logger.warning(f"WAN口索引 {wan_idx} 无效或不可用，尝试使用默认处理")
            # 尝试使用默认处理方式
            try:
                df = self.fetch_pledge_detail_data(ann_date)
                if df is None or df.empty:
                    return False
                
                processed_df = self.process_data(df)
                if processed_df is None or processed_df.empty:
                    return False
                
                success = self.save_to_mongodb(processed_df)
                if not success:
                    self.result_queue.put((ann_date, processed_df))
                
                return success
            except Exception as e:
                logger.error(f"默认处理公告日期 {ann_date} 时发生异常: {str(e)}")
                return False
        
        logger.info(f"线程使用WAN口 {wan_idx} 处理公告日期 {ann_date}")
        
        # 获取WAN口锁
        if not self.wan_locks[wan_idx].acquire(timeout=5):
            logger.warning(f"无法获取WAN口 {wan_idx} 的锁，跳过处理公告日期 {ann_date}")
            return False
        
        try:
            return self._process_date_with_wan_no_lock(ann_date, wan_idx)
        finally:
            # 释放WAN口锁
            self.wan_locks[wan_idx].release()
            logger.debug(f"释放WAN口 {wan_idx} 的锁")
    
    def _process_date_with_wan_no_lock(self, ann_date: str, wan_idx: int) -> bool:
        """
        使用指定的WAN口处理单个公告日期数据，不获取锁（由调用者控制锁）
        
        Args:
            ann_date: 公告日期
            wan_idx: 要使用的WAN口索引
            
        Returns:
            是否成功
        """
        logger.info(f"WAN口 {wan_idx} 处理公告日期 {ann_date}")
        
        success = False
        processed_count = 0
        
        try:
            # 获取股票代码列表
            stock_codes = self.get_stock_codes()
            if not stock_codes:
                logger.error("未能获取到股票代码列表，无法处理公告日期数据")
                return False
                
            logger.info(f"WAN口 {wan_idx} 获取到 {len(stock_codes)} 个股票代码，将逐个获取公告日期 {ann_date} 的数据")
            
            # 用于存储所有获取到的DataFrame
            all_data_frames = []
            
            # 每次处理一支股票，不使用批量处理
            total_count = len(stock_codes)
            
            for i, ts_code in enumerate(stock_codes):
                # 检查是否收到停止信号
                if STOP_PROCESSING:
                    logger.warning(f"WAN口 {wan_idx} 收到停止信号，中断处理")
                    break
                
                # 每处理50个股票记录一次进度
                if i % 50 == 0:
                    logger.info(f"WAN口 {wan_idx} 正在处理第 {i+1}/{total_count} 个股票: {ts_code}，公告日期 {ann_date}")
                
                try:
                    # 获取单个股票在指定公告日期的数据
                    df = self.fetch_data(ts_code=ts_code, ann_date=ann_date, wan_idx=wan_idx)
                    if df is not None and not df.empty:
                        all_data_frames.append(df)
                        processed_count += 1
                        
                    # 添加随机延时(3-8秒)，避免API限制
                    delay = random.uniform(3.0, 8.0)
                    logger.debug(f"WAN口 {wan_idx} API调用延时: {delay:.2f}秒")
                    time.sleep(delay)
                    
                except Exception as e:
                    logger.warning(f"WAN口 {wan_idx} 获取股票 {ts_code} 在公告日期 {ann_date} 的数据时出错: {str(e)}")
            
            # 如果没有数据，返回失败
            if not all_data_frames:
                logger.warning(f"WAN口 {wan_idx} 在公告日期 {ann_date} 未获取到任何数据")
                return False
            
            logger.info(f"WAN口 {wan_idx} 共处理了 {total_count} 个股票，成功获取 {processed_count} 个股票的数据")
            
            # 合并所有获取到的数据
            combined_df = pd.concat(all_data_frames, ignore_index=True)
            logger.info(f"WAN口 {wan_idx} 在公告日期 {ann_date} 合计获取到 {len(combined_df)} 条数据，来自 {processed_count} 个股票")
            
            # 处理数据
            processed_df = self.process_data(combined_df)
            if processed_df is None or processed_df.empty:
                logger.warning(f"WAN口 {wan_idx} 处理公告日期 {ann_date} 的数据后为空")
                return False
            
            # 保存单日数据到MongoDB
            if not self.mongodb_handler.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if not self.mongodb_handler.connect():
                    logger.error("连接MongoDB失败")
                    # 添加到结果队列，供后续处理
                    self.result_queue.put((ann_date, processed_df))
                    return False
                    
            # 保存到MongoDB
            success = self.save_to_mongodb(processed_df)
            if success:
                logger.success(f"WAN口 {wan_idx} 的公告日期 {ann_date} 的数据已保存到MongoDB")
            else:
                logger.error(f"WAN口 {wan_idx} 保存公告日期 {ann_date} 的数据到MongoDB失败")
                # 添加到结果队列，供后续处理
                self.result_queue.put((ann_date, processed_df))
        except Exception as e:
            logger.error(f"WAN口 {wan_idx} 处理公告日期 {ann_date} 时发生异常: {str(e)}")
            # 如果有数据但处理失败，添加到结果队列
            if 'combined_df' in locals() and combined_df is not None and not combined_df.empty:
                processed_df = self.process_data(combined_df)
                if processed_df is not None and not processed_df.empty:
                    self.result_queue.put((ann_date, processed_df))
        
        return success
    
    def _process_date_parallel(self, ann_dates: List[str]) -> bool:
        """
        并行处理多个公告日期数据
        
        Args:
            ann_dates: 公告日期列表
            
        Returns:
            是否全部成功
        """
        if self.available_wan_count == 0:
            logger.warning("未找到可用的WAN口，降级为串行处理模式")
            # 降级为串行处理
            all_success = True
            for ann_date in ann_dates:
                # 检查是否收到停止信号
                if STOP_PROCESSING:
                    logger.warning("收到停止信号，中断处理")
                    return False
                
                try:
                    df = self.fetch_pledge_detail_data(ann_date)
                    if df is not None and not df.empty:
                        processed_df = self.process_data(df)
                        if processed_df is not None and not processed_df.empty:
                            success = self.save_to_mongodb(processed_df)
                            if not success:
                                logger.error(f"保存公告日期 {ann_date} 的数据到MongoDB失败")
                                self.result_queue.put((ann_date, processed_df))
                                all_success = False
                except Exception as e:
                    logger.error(f"处理公告日期 {ann_date} 时发生异常: {str(e)}")
                    all_success = False
            return all_success
        
        threads_count = self.available_wan_count
        logger.info(f"并行处理 {len(ann_dates)} 个公告日期的数据，线程数: {threads_count}，可用WAN口: {self.available_wan_indices}")
        
        # 确保WAN索引列表有效
        if not self.available_wan_indices:
            logger.error("可用WAN口列表为空，无法进行并行处理")
            return False
        
        # 均匀分配公告日期到可用WAN口
        date_groups = {wan_idx: [] for wan_idx in self.available_wan_indices}
        for i, date in enumerate(ann_dates):
            wan_idx = self.available_wan_indices[i % len(self.available_wan_indices)]
            date_groups[wan_idx].append(date)
        
        # 记录分配情况
        for wan_idx, dates in date_groups.items():
            logger.info(f"WAN口 {wan_idx} 分配到 {len(dates)} 个公告日期")
        
        all_success = True
        processed_dates_count = 0  # 添加计数器跟踪处理的日期数
        
        # 创建每个WAN口对应的处理函数
        def process_wan_dates(wan_idx, dates):
            """处理单个WAN口对应的所有日期"""
            if not dates:  # 没有日期需要处理
                logger.info(f"WAN口 {wan_idx} 没有分配到公告日期，跳过")
                return True, 0
                
            # 获取WAN口锁，确保同一时间只有一个线程使用此WAN口
            if not self.wan_locks[wan_idx].acquire(timeout=10):  # 增加超时时间
                logger.warning(f"无法获取WAN口 {wan_idx} 的锁，跳过处理")
                return False, 0
            
            logger.info(f"线程成功获取WAN口 {wan_idx} 的锁")
            
            try:
                logger.info(f"线程开始处理WAN口 {wan_idx} 的 {len(dates)} 个公告日期")
                
                wan_success = True
                success_count = 0
                
                # 逐个处理该WAN口的所有公告日期
                for date in dates:
                    # 检查是否收到停止信号
                    if STOP_PROCESSING:
                        logger.warning(f"WAN口 {wan_idx} 收到停止信号，中断处理")
                        return wan_success, success_count
                        
                    try:
                        success = self._process_date_with_wan_no_lock(date, wan_idx)
                        if success:
                            success_count += 1
                        else:
                            logger.warning(f"WAN口 {wan_idx} 处理公告日期 {date} 失败")
                            wan_success = False
                    except Exception as e:
                        logger.error(f"WAN口 {wan_idx} 处理公告日期 {date} 时发生异常: {str(e)}")
                        wan_success = False
                
                # 如果至少成功处理了一个日期，视为部分成功
                if success_count > 0:
                    logger.info(f"WAN口 {wan_idx} 成功处理了 {success_count}/{len(dates)} 个公告日期")
                    # 即使有些失败，只要有成功的，我们就不认为整体失败
                    wan_success = True
                
                logger.info(f"线程完成WAN口 {wan_idx} 的所有公告日期处理")
                return wan_success, success_count
            finally:
                # 释放WAN口锁
                self.wan_locks[wan_idx].release()
                logger.info(f"释放WAN口 {wan_idx} 的锁")  # 改为INFO级别，更易于调试
        
        # 创建线程池，线程数等于可用WAN口数量，设置线程为守护线程
        global executor_pool
        executor_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=threads_count, 
            thread_name_prefix="wan_worker"
        )
        
        try:
            # 提交任务，每个WAN口一个任务
            future_to_wan = {}
            for wan_idx in self.available_wan_indices:
                dates = date_groups.get(wan_idx, [])
                # 即使没有日期，也提交任务以保持线程和WAN口一一对应
                logger.info(f"提交WAN口 {wan_idx} 处理 {len(dates)} 个公告日期的任务")
                future = executor_pool.submit(process_wan_dates, wan_idx, dates)
                future_to_wan[future] = wan_idx
            
            # 处理结果
            for future in concurrent.futures.as_completed(future_to_wan):
                # 检查是否收到停止信号
                if STOP_PROCESSING:
                    logger.warning("收到停止信号，不再等待其他任务完成")
                    break
                    
                wan_idx = future_to_wan[future]
                try:
                    result = future.result()
                    # 现在result是一个元组(success, count)
                    if isinstance(result, tuple) and len(result) == 2:
                        success, count = result
                        processed_dates_count += count
                        if not success:
                            logger.warning(f"WAN口 {wan_idx} 的任务处理失败")
                            # 不再直接设置all_success = False
                    else:
                        logger.warning(f"WAN口 {wan_idx} 返回了意外的结果格式: {result}")
                except Exception as e:
                    logger.error(f"WAN口 {wan_idx} 的任务处理时发生异常: {str(e)}")
                    # 不再直接设置all_success = False
        
        finally:
            # 确保线程池被关闭
            if executor_pool:
                executor_pool.shutdown(wait=False)
                executor_pool = None
        
        # 只有当没有成功处理任何日期时，才返回失败
        if processed_dates_count == 0 and len(ann_dates) > 0:
            logger.error(f"所有 {len(ann_dates)} 个公告日期都处理失败")
            return False
        
        # 如果至少处理了一些日期，就返回成功
        logger.success(f"成功处理了 {processed_dates_count}/{len(ann_dates)} 个公告日期")
        return True
    
    def fetch_stock_data(self, ts_code: str) -> Optional[pd.DataFrame]:
        """获取指定股票代码的数据"""
        logger.info(f"正在获取股票 {ts_code} 的基本数据...")
        
        # 创建参数字典
        params = {'ts_code': ts_code}
        
        # 在full模式下不添加日期范围和exchange参数
        if not self.full_mode:
            params['start_date'] = self.start_date
            params['end_date'] = self.end_date
            params['exchange'] = self.exchange
        
        return self.fetch_data(**params)
    
    def get_stock_codes(self) -> List[str]:
        """
        从MongoDB的stock_basic集合中获取股票代码列表
        筛选上市日期早于当前日期的股票
        
        Returns:
            股票代码列表
        """
        try:
            # 确保MongoDB连接
            if not self.mongodb_handler.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if not self.mongodb_handler.connect():
                    logger.error("连接MongoDB失败")
                    return []
            
            # 获取当前日期作为筛选条件
            current_date = datetime.now().strftime("%Y%m%d")
            
            # 构建查询条件 - 筛选上市日期早于当前日期的股票
            query = {
                "list_date": {"$lt": current_date}  # 上市日期早于当前日期
            }
            
            # 从stock_basic集合中查询
            result = self.mongodb_handler.find_documents("stock_basic", query)
            
            if not result:
                logger.warning("stock_basic集合中未找到数据")
                return []
                
            # 提取股票代码
            stock_codes = []
            for doc in result:
                if "ts_code" in doc:
                    stock_codes.append(doc["ts_code"])
            
            logger.info(f"从stock_basic集合获取到 {len(stock_codes)} 个上市日期早于 {current_date} 的股票代码")
            
            if not stock_codes:
                logger.warning("未找到符合条件的股票代码")
            
            return stock_codes
        except Exception as e:
            logger.error(f"获取股票代码列表失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误: {traceback.format_exc()}")
            return []
    
    def _process_stock_parallel(self, stock_codes: List[str]) -> bool:
        """
        并行处理多个股票代码数据
        
        Args:
            stock_codes: 股票代码列表
            
        Returns:
            是否全部成功
        """
        if self.available_wan_count == 0:
            logger.warning("未找到可用的WAN口，降级为串行处理模式")
            # 降级为串行处理
            all_success = True
            for ts_code in stock_codes:
                # 检查是否收到停止信号
                if STOP_PROCESSING:
                    logger.warning("收到停止信号，中断处理")
                    return False
                    
                try:
                    df = self.fetch_stock_data(ts_code)
                    if df is not None and not df.empty:
                        processed_df = self.process_data(df)
                        if processed_df is not None and not processed_df.empty:
                            success = self.save_to_mongodb(processed_df)
                            if not success:
                                logger.error(f"保存股票 {ts_code} 的数据到MongoDB失败")
                                all_success = False
                except Exception as e:
                    logger.error(f"处理股票 {ts_code} 时发生异常: {str(e)}")
                    all_success = False
            return all_success
        
        threads_count = self.available_wan_count
        logger.info(f"并行处理 {len(stock_codes)} 个股票的数据，线程数: {threads_count}，可用WAN口: {self.available_wan_indices}")
        
        # 确保WAN索引列表有效
        if not self.available_wan_indices:
            logger.error("可用WAN口列表为空，无法进行并行处理")
            return False
        
        # 均匀分配股票代码到可用WAN口
        stock_groups = {wan_idx: [] for wan_idx in self.available_wan_indices}
        for i, ts_code in enumerate(stock_codes):
            wan_idx = self.available_wan_indices[i % len(self.available_wan_indices)]
            stock_groups[wan_idx].append(ts_code)
        
        # 记录分配情况
        for wan_idx, stocks in stock_groups.items():
            logger.info(f"WAN口 {wan_idx} 分配到 {len(stocks)} 个股票")
        
        processed_stocks_count = 0
        
        # 创建每个WAN口对应的处理函数
        def process_wan_stocks(wan_idx, stocks):
            """处理单个WAN口对应的所有股票"""
            if not stocks:  # 没有股票需要处理
                logger.info(f"WAN口 {wan_idx} 没有分配到股票，跳过")
                return True, 0
                
            # 获取WAN口锁，确保同一时间只有一个线程使用此WAN口
            if not self.wan_locks[wan_idx].acquire(timeout=10):
                logger.warning(f"无法获取WAN口 {wan_idx} 的锁，跳过处理")
                return False, 0
            
            logger.info(f"线程成功获取WAN口 {wan_idx} 的锁")
            
            try:
                logger.info(f"线程开始处理WAN口 {wan_idx} 的 {len(stocks)} 个股票")
                
                wan_success = True
                success_count = 0
                
                # 直接在每次fetch_stock_data调用时传递wan_idx参数，不使用lambda替换
                # 逐个处理该WAN口的所有股票
                for ts_code in stocks:
                    # 检查是否收到停止信号
                    if STOP_PROCESSING:
                        logger.warning(f"WAN口 {wan_idx} 收到停止信号，中断处理")
                        return wan_success, success_count
                        
                    try:
                        # 打印正在处理的股票代码
                        logger.info(f"WAN口 {wan_idx} 正在处理股票: {ts_code}")  # 添加这一行，显示正在处理的股票代码
                        
                        # 获取单个股票数据，直接传递wan_idx参数
                        df = self.fetch_data(ts_code=ts_code, wan_idx=wan_idx)
                        if df is None or df.empty:
                            logger.warning(f"股票 {ts_code} 的数据为空或获取失败")
                            continue
                        
                        # 处理数据
                        processed_df = self.process_data(df)
                        if processed_df is None or processed_df.empty:
                            logger.warning(f"股票 {ts_code} 的处理后数据为空")
                            continue
                        
                        # 保存到MongoDB
                        success = self.save_to_mongodb(processed_df)
                        if success:
                            logger.success(f"股票 {ts_code} 的数据已保存到MongoDB")
                            success_count += 1
                        else:
                            logger.error(f"保存股票 {ts_code} 的数据到MongoDB失败")
                            wan_success = False
                    except Exception as e:
                        logger.error(f"WAN口 {wan_idx} 处理股票 {ts_code} 时发生异常: {str(e)}")
                        wan_success = False
                
                # 如果至少成功处理了一个股票，视为部分成功
                if success_count > 0:
                    logger.info(f"WAN口 {wan_idx} 成功处理了 {success_count}/{len(stocks)} 个股票")
                    wan_success = True
                
                logger.info(f"线程完成WAN口 {wan_idx} 的所有股票处理")
                return wan_success, success_count
            finally:
                # 释放WAN口锁
                self.wan_locks[wan_idx].release()
                logger.info(f"释放WAN口 {wan_idx} 的锁")
        
        all_success = True
        
        # 创建线程池，线程数等于可用WAN口数量，设置线程为守护线程
        global executor_pool
        executor_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=threads_count, 
            thread_name_prefix="wan_worker"
        )
        
        try:
            # 提交任务，每个WAN口一个任务
            future_to_wan = {}
            for wan_idx in self.available_wan_indices:
                stocks = stock_groups.get(wan_idx, [])
                # 即使没有股票，也提交任务以保持线程和WAN口一一对应
                logger.info(f"提交WAN口 {wan_idx} 处理 {len(stocks)} 个股票的任务")
                future = executor_pool.submit(process_wan_stocks, wan_idx, stocks)
                future_to_wan[future] = wan_idx
            
            # 处理结果
            for future in concurrent.futures.as_completed(future_to_wan):
                # 检查是否收到停止信号
                if STOP_PROCESSING:
                    logger.warning("收到停止信号，不再等待其他任务完成")
                    break
                    
                wan_idx = future_to_wan[future]
                try:
                    result = future.result()
                    # 现在result是一个元组(success, count)
                    if isinstance(result, tuple) and len(result) == 2:
                        success, count = result
                        processed_stocks_count += count
                        if not success and count == 0:
                            logger.warning(f"WAN口 {wan_idx} 的任务处理失败")
                            all_success = False
                    else:
                        logger.warning(f"WAN口 {wan_idx} 返回了意外的结果格式: {result}")
                except Exception as e:
                    logger.error(f"WAN口 {wan_idx} 的任务处理时发生异常: {str(e)}")
                    all_success = False
        
        finally:
            # 确保线程池被关闭
            if executor_pool:
                executor_pool.shutdown(wait=False)
                executor_pool = None
        
        # 只有当没有成功处理任何股票时，才返回失败
        if processed_stocks_count == 0 and len(stock_codes) > 0:
            logger.error(f"所有 {len(stock_codes)} 个股票都处理失败")
            return False
        
        # 如果至少处理了一些股票，就返回成功
        logger.success(f"成功处理了 {processed_stocks_count}/{len(stock_codes)} 个股票")
        return True
    
    def run(self) -> bool:
        """
        运行数据获取和保存流程，采用limit/offset方式批量获取数据
        
        根据API限制，使用limit和offset参数分批次获取所有数据
        
        Returns:
            是否成功
        """
        try:
            # 第一步：检查并确保集合和索引存在
            logger.info("第一步：检查并确保MongoDB集合和索引存在")
            if not self._ensure_collection_and_indexes():
                logger.error("无法确保MongoDB集合和索引，放弃数据获取")
                return False
            
            # 第二步：使用limit/offset批量抓取所有数据
            logger.info("第二步：使用limit/offset批量抓取所有质押明细数据")
            
            # 初始化偏移量和批次大小
            offset = 0
            limit = 10000  # 每批抓取10000条数据
            batch_num = 1
            total_records = 0
            total_filtered_records = 0
            continue_fetching = True
            
            # 如果没有可用的WAN口，使用串行处理
            if self.available_wan_count == 0 or self.serial_mode:
                logger.info("使用串行模式批量抓取数据")
                
                # 使用串行模式抓取
                while continue_fetching:
                    # 抓取当前批次的数据
                    logger.info(f"抓取第{batch_num}批数据 (offset={offset}, limit={limit})")
                    df = self.fetch_data(offset=offset, limit=limit)
                    
                    # 检查是否获取到数据
                    if df is None or df.empty:
                        logger.info(f"第{batch_num}批未获取到数据，抓取完成")
                        break
                    
                    # 记录获取到的数据量
                    batch_size = len(df)
                    total_records += batch_size
                    logger.info(f"第{batch_num}批获取到 {batch_size} 条数据，累计 {total_records} 条")
                    
                    # 处理数据（过滤掉不需要的股票代码）
                    processed_df = self.process_data(df)
                    if processed_df is not None and not processed_df.empty:
                        filtered_size = len(processed_df)
                        total_filtered_records += filtered_size
                        logger.info(f"第{batch_num}批过滤后保留 {filtered_size} 条数据，累计保留 {total_filtered_records} 条")
                        
                        # 保存到MongoDB
                        success = self.save_to_mongodb(processed_df)
                        if not success:
                            logger.error(f"保存第{batch_num}批数据到MongoDB失败")
                    
                    # 如果返回的数据量小于limit，说明已经获取完所有数据
                    if batch_size < limit:
                        logger.info(f"返回数据量({batch_size})小于limit({limit})，抓取完成")
                        continue_fetching = False
                    else:
                        # 更新offset，继续获取下一批数据
                        offset += limit
                        batch_num += 1
                        
                        # 添加随机延时，避免API限制
                        delay = random.uniform(5.0, 10.0)
                        logger.info(f"等待 {delay:.2f} 秒后抓取下一批数据")
                        time.sleep(delay)
            else:
                # 使用并行模式抓取
                logger.info(f"使用并行模式批量抓取数据，WAN口数量：{self.available_wan_count}，并行线程数：{self.max_workers}")
                
                # 创建队列存储待处理的offset
                offset_queue = queue.Queue()
                
                # 首先添加第一个offset到队列
                offset_queue.put(offset)
                
                # 创建结果锁和变量
                results_lock = threading.Lock()
                stop_event = threading.Event()
                processed_batches = 0
                success_batches = 0
                
                # 定义WAN工作线程
                def wan_worker(wan_idx):
                    """每个WAN口的工作线程，处理一个offset批次"""
                    nonlocal processed_batches, success_batches, total_records, total_filtered_records
                    
                    while not stop_event.is_set() and not offset_queue.empty():
                        try:
                            # 从队列获取一个offset
                            current_offset = offset_queue.get(block=False)
                        except queue.Empty:
                            # 队列为空，退出循环
                            break
                        
                        # 更新处理计数
                        with results_lock:
                            processed_batches += 1
                            current_batch = processed_batches
                        
                        try:
                            # 抓取当前批次的数据
                            logger.info(f"WAN口 {wan_idx} 抓取第{current_batch}批数据 (offset={current_offset}, limit={limit})")
                            df = self.fetch_data(offset=current_offset, limit=limit, wan_idx=wan_idx)
                            
                            # 检查是否获取到数据
                            if df is None or df.empty:
                                logger.info(f"WAN口 {wan_idx} 第{current_batch}批未获取到数据")
                                offset_queue.task_done()
                                continue
                            
                            # 记录获取到的数据量
                            batch_size = len(df)
                            
                            with results_lock:
                                total_records += batch_size
                                logger.info(f"WAN口 {wan_idx} 第{current_batch}批获取到 {batch_size} 条数据，累计 {total_records} 条")
                            
                            # 如果返回的数据量等于limit，添加下一个offset到队列
                            if batch_size == limit:
                                next_offset = current_offset + limit
                                offset_queue.put(next_offset)
                                logger.debug(f"WAN口 {wan_idx} 添加下一批次 offset={next_offset} 到队列")
                            
                            # 处理数据（过滤掉不需要的股票代码）
                            processed_df = self.process_data(df)
                            if processed_df is not None and not processed_df.empty:
                                filtered_size = len(processed_df)
                                
                                with results_lock:
                                    total_filtered_records += filtered_size
                                    logger.info(f"WAN口 {wan_idx} 第{current_batch}批过滤后保留 {filtered_size} 条数据，累计保留 {total_filtered_records} 条")
                                
                                # 保存到MongoDB
                                success = self.save_to_mongodb(processed_df)
                                if success:
                                    with results_lock:
                                        success_batches += 1
                                    logger.success(f"WAN口 {wan_idx} 成功保存第{current_batch}批数据到MongoDB")
                                else:
                                    logger.error(f"WAN口 {wan_idx} 保存第{current_batch}批数据到MongoDB失败")
                            
                            # 添加随机延时，避免API限制
                            delay = random.uniform(3.0, 8.0)
                            logger.debug(f"WAN口 {wan_idx} 延时 {delay:.2f} 秒")
                            time.sleep(delay)
                            
                        except Exception as e:
                            logger.error(f"WAN口 {wan_idx} 处理offset={current_offset}时发生异常: {str(e)}")
                        
                        # 标记任务完成
                        offset_queue.task_done()
                
                # 创建并启动WAN工作线程
                threads = []
                for wan_idx in self.available_wan_indices[:self.max_workers]:  # 限制线程数不超过max_workers
                    thread = threading.Thread(
                        target=wan_worker,
                        args=(wan_idx,),
                        name=f"wan_worker_{wan_idx}"
                    )
                    thread.daemon = True
                    thread.start()
                    threads.append(thread)
                    # 添加短暂延时，避免同时启动所有线程
                    time.sleep(0.5)
                
                try:
                    # 等待队列处理完毕
                    offset_queue.join()
                    
                    # 等待所有线程完成
                    for thread in threads:
                        thread.join(timeout=1.0)
                    
                except KeyboardInterrupt:
                    logger.warning("收到键盘中断，正在停止所有线程...")
                    stop_event.set()
                    return False
            
            # 输出总结信息
            logger.success(f"数据抓取完成，总共获取 {total_records} 条数据，过滤后保留 {total_filtered_records} 条")
            return True
            
        except Exception as e:
            logger.error(f"运行过程中发生异常: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False
    
    def _process_stocks_serial(self, stock_codes: List[str]) -> bool:
        """
        串行处理股票数据，一次只处理一个股票代码
        
        Args:
            stock_codes: 股票代码列表
            
        Returns:
            是否成功
        """
        logger.info(f"【串行模式】处理 {len(stock_codes)} 个股票")
        
        processed_count = 0
        success_count = 0
        total_stocks = len(stock_codes)
        
        for i, ts_code in enumerate(stock_codes):
            # 检查是否收到停止信号
            if STOP_PROCESSING:
                logger.warning("收到停止信号，中断处理")
                break
            
            # 记录处理进度
            if i % 10 == 0 or i == total_stocks - 1:
                logger.info(f"【进度】正在处理第 {i+1}/{total_stocks} 个股票: {ts_code}")
            
            try:
                # 清晰打印抓取的股票
                logger.info(f"【开始抓取】股票 {ts_code}")
                
                # 使用start_date和end_date参数获取单个股票的所有数据
                df = self.fetch_data(ts_code=ts_code)
                
                # 添加随机延时 (2-5秒)，避免触发API限制
                delay = random.uniform(2.0, 5.0)
                logger.debug(f"处理股票 {ts_code} 后延时 {delay:.2f} 秒")
                time.sleep(delay)
                
                if df is None or df.empty:
                    logger.info(f"【抓取结果】股票 {ts_code} 无数据")
                    continue
                
                # 数据抓取结果详细展示
                if 'end_date' in df.columns:
                    unique_dates = df['end_date'].nunique()
                    min_date = df['end_date'].min() if not pd.isna(df['end_date'].min()) else "未知"
                    max_date = df['end_date'].max() if not pd.isna(df['end_date'].max()) else "未知"
                    
                    logger.info(f"【抓取成功】股票 {ts_code} 获取到 {len(df)} 条数据，"
                               f"{unique_dates}个截至日期({min_date}至{max_date})")
                    
                else:
                    logger.info(f"【抓取成功】股票 {ts_code} 获取到 {len(df)} 条数据")
                
                # 处理数据
                processed_df = self.process_data(df)
                if processed_df is None or processed_df.empty:
                    logger.info(f"【处理结果】股票 {ts_code} 的处理后数据为空")
                    continue
                
                # 保存单日数据到MongoDB
                logger.info(f"【开始存储】股票 {ts_code} 的 {len(processed_df)} 条数据")
                success = self.save_to_mongodb(processed_df)
                if success:
                    success_count += 1
                    logger.info(f"【存储成功】股票 {ts_code} 的 {len(processed_df)} 条数据已保存到数据库")
                    if success_count % 10 == 0:
                        logger.success(f"已成功保存 {success_count}/{processed_count} 个股票的数据 (成功率: {success_count*100/processed_count:.1f}%)")
                else:
                    logger.error(f"【存储失败】股票 {ts_code} 的 {len(processed_df)} 条数据保存失败")
                
            except Exception as e:
                logger.error(f"处理股票 {ts_code} 的数据时发生异常: {str(e)}")
                
                # 出错后添加延时，避免连续失败
                time.sleep(random.uniform(2.0, 5.0))
            
            # 更新处理计数
            processed_count += 1
            
            # 每处理50个股票输出一次总进度
            if processed_count % 50 == 0:
                logger.success(f"总进度: 已处理 {processed_count}/{total_stocks} 个股票，成功: {success_count}")
        
        logger.success(f"串行处理完成，成功处理 {success_count}/{total_stocks} 个股票的数据")
        return success_count > 0  # 只要处理了一些数据就认为成功
    
    def save_to_mongodb(self, df: pd.DataFrame, max_retries=3, chunk_size=10000) -> bool:
        """
        保存数据到MongoDB，高效版本：增加差异检测，减少不必要的更新操作
        
        Args:
            df: 要保存的数据
            max_retries: 最大重试次数 
            chunk_size: 每批处理的记录数，默认10000
            
        Returns:
            是否成功
        """
        if df is None or df.empty:
            logger.warning("【存储警告】没有数据需要保存")
            return False
        
        # 获取数据库和集合名称
        db_name = self.db_name
        collection_name = self.collection_name
        
        logger.debug(f"【存储】保存数据到MongoDB数据库：{db_name}，集合：{collection_name}")
        
        # 检查MongoDB连接
        if not self.mongodb_handler.is_connected():
            logger.warning("MongoDB未连接，尝试连接...")
            if not self.mongodb_handler.connect():
                logger.error("【存储错误】连接MongoDB失败")
                return False
        
        # 转换DataFrame为字典列表，减少转换开销
        records = df.to_dict("records")
        total_records = len(records)
        
        # 跟踪统计
        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        
        try:
            # 获取集合
            collection = self.mongodb_handler.get_collection(collection_name)
            
            # 分批处理，减少每次处理的数据量
            for i in range(0, total_records, chunk_size):
                chunk = records[i:i+chunk_size]
                chunk_len = len(chunk)
                
                # 设置重试计数器
                retries = 0
                success = False
                
                while retries < max_retries and not success:
                    try:
                        # 1. 提取唯一标识符
                        identifiers = [
                            {field: doc[field] for field in self.index_fields if field in doc}
                            for doc in chunk
                        ]
                        
                        # 2. 检查已存在的记录
                        existing_docs_cursor = collection.find(
                            {"$or": identifiers},
                            {"_id": 1, **{field: 1 for field in self.index_fields}}
                        )
                        existing_ids = {
                            tuple(doc[field] for field in self.index_fields if field in doc): doc["_id"]
                            for doc in existing_docs_cursor
                        }
                        
                        # 3. 准备操作列表
                        inserts = []  # 新记录
                        updates = []  # 更新记录
                        
                        for doc in chunk:
                            # 创建复合键用于查找匹配记录
                            key = tuple(doc[field] for field in self.index_fields if field in doc)
                            
                            if key in existing_ids:
                                # 记录存在，需要更新
                                # 创建不包含_id字段的文档副本用于更新
                                update_doc = doc.copy()
                                if '_id' in update_doc:
                                    del update_doc['_id']  # 确保不包含_id字段
                                    
                                updates.append(
                                    pymongo.UpdateOne(
                                        {"_id": existing_ids[key]},
                                        {"$set": update_doc}
                                    )
                                )
                            else:
                                # 记录不存在，需要插入
                                inserts.append(doc)
                        
                        # 4. 执行插入操作
                        if inserts:
                            insert_result = collection.insert_many(inserts, ordered=False)
                            inserted_count += len(insert_result.inserted_ids)
                        
                        # 5. 执行更新操作
                        if updates:
                            update_result = collection.bulk_write(updates, ordered=False)
                            updated_count += update_result.modified_count
                            skipped_count += (len(updates) - update_result.modified_count)
                        
                        # 记录当前批次结果
                        if i % (chunk_size * 5) == 0 or i + chunk_size >= total_records:
                            logger.info(
                                f"进度 {(i+chunk_len)}/{total_records}, "
                                f"插入: {inserted_count}, 更新: {updated_count}, 跳过: {skipped_count}"
                            )
                        
                        success = True
                    
                    except pymongo.errors.BulkWriteError as bwe:
                        retries += 1
                        logger.warning(f"批次 {i//chunk_size + 1} 批量写入错误 (尝试 {retries}/{max_retries})")
                        if retries >= max_retries:
                            # 在最后一次尝试，记录错误详情
                            logger.error(f"批量写入失败: {bwe.details}")
                            return False
                        time.sleep(1)  # 重试前等待
                        
                    except Exception as e:
                        retries += 1
                        logger.warning(f"批次 {i//chunk_size + 1} 处理失败 (尝试 {retries}/{max_retries}): {str(e)}")
                        if retries >= max_retries:
                            logger.error(f"批次 {i//chunk_size + 1} 处理失败，已达到最大重试次数")
                            return False
                        time.sleep(1)  # 重试前等待
            
            # 记录最终结果
            logger.success(
                f"数据保存完成: 总计: {total_records}, 插入: {inserted_count}, "
                f"更新: {updated_count}, 跳过: {skipped_count}"
            )
            return True
            
        except Exception as e:
            logger.error(f"保存数据过程发生错误: {str(e)}")
            import traceback
            logger.debug(f"错误详情: {traceback.format_exc()}")
            return False

    def _ensure_collection_and_indexes(self) -> bool:
        """
        确保集合和索引存在
        
        检查MongoDB集合是否存在，如果不存在则创建
        检查索引是否存在，如果不存在则创建
        
        Returns:
            bool: 是否成功
        """
        try:
            # 确保MongoDB连接
            if not self.mongodb_handler:
                logger.error("MongoDB Handler 未初始化")
                return False
            elif not self.mongodb_handler.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if not self.mongodb_handler.connect():
                    logger.error("连接MongoDB失败")
                    return False
            
            # 检查集合是否存在
            logger.info(f"检查MongoDB集合 {self.collection_name} 是否存在")
            collection_exists = self.mongodb_handler.collection_exists(self.collection_name)
            if not collection_exists:
                logger.info(f"集合 {self.collection_name} 不存在，创建集合")
                self.mongodb_handler.create_collection(self.collection_name)
            else:
                logger.info(f"集合 {self.collection_name} 已存在")
            
            # 从接口配置文件中读取索引字段，而不是硬编码
            if hasattr(self, 'interface_config') and 'index_fields' in self.interface_config:
                self.index_fields = self.interface_config['index_fields']
                logger.info(f"从接口配置读取索引字段: {self.index_fields}")
            else:
                # 如果无法从配置读取，则使用默认索引字段
                self.index_fields = ['ts_code', 'ann_date', 'end_date', 'report_type']
                logger.warning(f"无法从接口配置读取索引字段，使用默认值: {self.index_fields}")
            
            logger.info(f"为集合 {self.collection_name} 设置索引字段: {self.index_fields}")
            
            # 获取集合
            collection = self.mongodb_handler.get_collection(self.collection_name)
            
            # 获取现有索引
            existing_indexes = collection.index_information()
            
            # 创建主索引 - 不再指定名称，让MongoDB自动生成
            primary_index_keys = [(field, pymongo.ASCENDING) for field in self.index_fields]
            
            # 检查类似的索引是否已存在
            primary_index_exists = False
            for index_name, index_info in existing_indexes.items():
                if index_name != "_id_" and set([(k, v) for k, v in index_info["key"]]) == set(primary_index_keys):
                    primary_index_exists = True
                    logger.info(f"复合主索引已存在: {index_name}")
                    break
            
            # 如果主索引不存在，创建它（不指定名称）
            if not primary_index_exists:
                logger.info(f"正在创建复合主索引，字段: {[key[0] for key in primary_index_keys]}")
                collection.create_index(
                    primary_index_keys,
                    unique=True,
                    background=True
                )
            
            # 为每个索引字段创建单字段索引
            for field in self.index_fields:
                # 所有字段都使用升序(1)索引
                sort_direction = pymongo.ASCENDING
                index_key = [(field, sort_direction)]
                
                # 检查索引是否存在
                index_exists = False
                for existing_index, index_info in existing_indexes.items():
                    if existing_index != "_id_" and set([(k, v) for k, v in index_info["key"]]) == set(index_key):
                        index_exists = True
                        logger.info(f"单字段索引已存在: {existing_index}")
                        break
                
                # 如果索引不存在，创建它（不指定名称）
                if not index_exists:
                    logger.info(f"正在创建字段 {field} 的升序索引")
                    collection.create_index(
                        index_key,
                        background=True
                    )
            
            return True
            
        except Exception as e:
            logger.error(f"确保集合和索引存在失败: {str(e)}")
            import traceback
            logger.debug(f"错误详情: {traceback.format_exc()}")
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

    # ---- 在解析参数前，先初始化核心服务 ----
    # （如果初始化依赖配置，则需要先解析参数）
    # 显式初始化 MongoDB Handler
    logger.info("Initializing MongoDB Handler...")
    mongo_instance = None # 初始化为 None
    try:
        mongo_instance = init_mongodb_handler() # 调用初始化函数并获取返回的实例
        if mongo_instance:
             logger.info("MongoDB Handler initialized successfully.")
        else:
             logger.warning("init_mongodb_handler() did not initialize the handler.")
    except Exception as e:
        logger.error(f"Failed to initialize MongoDB Handler: {e}")
        # 这里可以选择退出或继续（取决于是否必须要有DB）
        # sys.exit(1)
    # ---- 初始化结束 ----
    
    parser = argparse.ArgumentParser(description='获取股权质押明细并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--exchange', default='SSE', help='交易所代码：SSE-上交所, SZSE-深交所')
    parser.add_argument('--db-name', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='pledge_detail', help='MongoDB集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    parser.add_argument('--shared-config', type=str, default=None, help='共享配置文件路径')
    parser.add_argument('--skip-validation', action='store_true', help='跳过配置验证')
    parser.add_argument('--serial', action='store_true', help='使用串行模式处理数据（默认为并行模式）')
    parser.add_argument('--max-workers', type=int, default=3, help='并行模式下的最大工作线程数（默认为3）')
    parser.add_argument('--batch-size', type=int, default=10000, help='每批获取的数据条数，默认10000')
    
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
        fetcher = pledge_detailFetcher(
            config_path=args.config,
            interface_dir=args.interface_dir,
            exchange=args.exchange,
            db_name=args.db_name,
            collection_name=args.collection_name,
            verbose=args.verbose,
            shared_config=shared_config,
            skip_validation=args.skip_validation,
            serial_mode=args.serial,
            max_workers=args.max_workers,
            mongo_handler_instance=mongo_instance # 传入 MongoDB Handler 实例
        )
        
        try:
            success = fetcher.run()
            
            if success:
                logger.success("股权质押明细获取和保存成功")
                return 0
            else:
                logger.error("股权质押明细获取或保存失败")
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