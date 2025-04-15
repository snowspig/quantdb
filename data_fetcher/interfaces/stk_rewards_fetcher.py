#!/usr/bin/env python
"""
Stk_rewards Fetcher - 获取上市公司管理层薪酬和持股数据并保存到MongoDB

该脚本用于从湘财Tushare获取上市公司管理层薪酬和持股数据，并保存到MongoDB数据库中
该版本继承TushareFetcher基类，实现了特定接口的数据获取功能

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=194

特点：
- 必须使用ts_code参数获取数据
- 支持同时获取最多100支股票的数据
- 可以使用end_date参数指定报告期
- 支持批处理和并行处理模式

使用方法：
    python stk_rewards_fetcher.py                   # 使用湘财真实API数据，简洁日志模式，获取近期数据
    python stk_rewards_fetcher.py --verbose         # 使用湘财真实API数据，详细日志模式
    python stk_rewards_fetcher.py --end-date 20200630  # 指定报告期
    python stk_rewards_fetcher.py --serial          # 使用串行模式处理数据（默认为并行模式）
    python stk_rewards_fetcher.py --full            # 使用完整模式，按股票代码列表获取所有股票的基本数据
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
from typing import List, Optional, Dict, Any, Union
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

class stk_rewardsFetcher(TushareFetcher):
    """
    日线基本数据获取器V2
    
    该类用于从Tushare获取日线基本数据并保存到MongoDB数据库
    使用TushareFetcher基类提供的通用功能
    支持串行和并行两种处理模式
    支持按日期和按股票代码两种抓取模式
    """
    
    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "stk_rewards.json",
        db_name: str = None,
        collection_name: str = "stk_rewards",
        start_date: str = None,
        end_date: str = None,
        exchange: str = "SSE",  # 默认上交所
        verbose: bool = False,
        shared_config: Dict[str, Any] = None,
        skip_validation: bool = False,
        serial_mode: bool = False,  # 是否使用串行模式
        max_workers: int = 3,  # 并行模式下的最大工作线程数
        full_mode: bool = False,  # 是否使用完整模式（按股票代码抓取）
        mongo_handler_instance: Optional[MongoDBHandler] = None # 新增参数
    ):
        """
        初始化日线基本数据获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            db_name: MongoDB数据库名称，如果为None则从配置文件中读取
            collection_name: MongoDB集合名称
            start_date: 开始日期（格式：YYYYMMDD，默认为当前日期前一年）
            end_date: 结束日期（格式：YYYYMMDD，默认为当前日期）
            exchange: 交易所代码（SSE：上交所，SZSE：深交所，默认SSE）
            verbose: 是否输出详细日志
            shared_config: 共享配置字典
            skip_validation: 是否跳过验证
            serial_mode: 是否使用串行模式
            max_workers: 并行模式下的最大工作线程数
            full_mode: 是否使用完整模式（按股票代码抓取）
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
        
        self.exchange = exchange
        
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
        logger.info(f"交易所: {self.exchange}, 日期范围: {self.start_date} - {self.end_date}")
        logger.info(f"处理模式: {'串行' if self.serial_mode else '并行'}, 可用WAN口数量: {self.available_wan_count}, 并行线程数: {self.max_workers}")
        if self.full_mode:
            logger.info("抓取模式: 完整模式(按股票代码)，将抓取所有历史数据而不限制日期范围")
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
        从Tushare获取管理层薪酬及持股数据
        
        Args:
            **kwargs: 查询参数，包括：
                ts_code: 股票代码(必需)，可以是单个代码或多个代码的逗号分隔字符串
                end_date: 报告期，格式为YYYYMMDD
                wan_idx: 指定WAN口索引，可选
                use_wan: 是否使用WAN口，默认True
        
        Returns:
            返回DataFrame或者None（如果出错）
        """
        ts_code = kwargs.get('ts_code')
        end_date = kwargs.get('end_date')
        
        # 是否使用WAN口
        use_wan = kwargs.get('use_wan', True)
        
        # 提取WAN口索引（如果指定了）
        wan_idx = kwargs.get('wan_idx')
        
        # 参数检查：确保提供了ts_code
        if not ts_code:
            logger.error("必须提供ts_code参数")
            return None
        
        # 设置API参数
        params = {'ts_code': ts_code}
        
        # 如果提供了end_date，添加到参数中
        if end_date:
            params['end_date'] = end_date
        
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
                api_name='stk_rewards', 
                params=params,
                wan_idx=wan_idx # 传递 wan_idx
            )
            return result
        except Exception as e:
            # 添加更详细的日志，包括 wan_idx
            logger.error(f"调用 self.client.get_data 失败 (WAN: {wan_idx}): {str(e)}")
            return None
        finally:
            # 注意：get_data 内部的 finally 块会处理端口释放和状态重置
            pass
    
    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        处理获取的日线基本数据
        只保留股票代码前两位为00、30、60、68的数据
        但在full模式下直接返回原始数据，不进行过滤
        
        Args:
            df: 原始日线基本数据
            
        Returns:
            处理后的数据
        """
        if df is None or df.empty:
            return df
            
        # 在full模式下直接返回原始数据，不进行过滤
        if self.full_mode:
            logger.debug("完整模式(full)下不进行股票代码过滤，返回原始数据")
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
    
    def fetch_stk_rewards_data(self, end_date: str) -> Optional[pd.DataFrame]:
        """
        获取指定报告期的管理层薪酬及持股数据
        
        Args:
            end_date: 报告期，格式为YYYYMMDD
            
        Returns:
            指定报告期的管理层薪酬及持股数据，如果失败则返回None
        """
        # 首先获取所有股票代码
        stock_codes = self.get_stock_codes()
        if not stock_codes:
            logger.warning(f"没有找到股票代码，无法获取报告期 {end_date} 的数据")
            return None
            
        logger.info(f"正在获取报告期 {end_date} 的管理层薪酬及持股数据，共 {len(stock_codes)} 支股票...")
        
        # 按每批100支股票进行批处理
        batch_size = 100
        all_data = []
        
        for i in range(0, len(stock_codes), batch_size):
            batch_ts_codes = stock_codes[i:i+batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(stock_codes) + batch_size - 1) // batch_size
            
            logger.info(f"处理批次 {batch_num}/{total_batches}，包含 {len(batch_ts_codes)} 支股票")
            
            # 获取批次数据
            batch_df = self.fetch_stock_data(batch_ts_codes, end_date)
            if batch_df is not None and not batch_df.empty:
                all_data.append(batch_df)
        
        # 合并所有数据
        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"成功获取报告期 {end_date} 的管理层薪酬及持股数据，共 {len(result_df)} 条记录")
            return result_df
        else:
            logger.warning(f"报告期 {end_date} 没有管理层薪酬及持股数据")
            return None
    
    def _process_date_with_wan(self, end_date: str, wan_idx: int) -> bool:
        """
        使用指定的WAN口处理单个报告期数据
        
        Args:
            end_date: 报告期
            wan_idx: 要使用的WAN口索引
            
        Returns:
            是否成功
        """
        # 检查WAN口索引是否有效
        if wan_idx not in self.wan_locks:
            logger.warning(f"WAN口索引 {wan_idx} 无效或不可用，尝试使用默认处理")
            # 尝试使用默认处理方式
            try:
                df = self.fetch_stk_rewards_data(end_date)
                if df is None or df.empty:
                    return False
                
                processed_df = self.process_data(df)
                if processed_df is None or processed_df.empty:
                    return False
                
                success = self.save_to_mongodb(processed_df)
                if not success:
                    self.result_queue.put((end_date, processed_df))
                
                return success
            except Exception as e:
                logger.error(f"默认处理报告期 {end_date} 时发生异常: {str(e)}")
                return False
        
        logger.info(f"线程使用WAN口 {wan_idx} 处理报告期 {end_date}")
        
        # 获取WAN口锁
        if not self.wan_locks[wan_idx].acquire(timeout=5):
            logger.warning(f"无法获取WAN口 {wan_idx} 的锁，跳过处理报告期 {end_date}")
            return False
        
        try:
            return self._process_date_with_wan_no_lock(end_date, wan_idx)
        finally:
            # 释放WAN口锁
            self.wan_locks[wan_idx].release()
            logger.debug(f"释放WAN口 {wan_idx} 的锁")
    
    def _process_date_with_wan_no_lock(self, end_date: str, wan_idx: int) -> bool:
        """
        使用指定的WAN口处理单个报告期数据，不获取锁（由调用者控制锁）
        
        Args:
            end_date: 报告期
            wan_idx: 要使用的WAN口索引
            
        Returns:
            是否成功
        """
        logger.info(f"WAN口 {wan_idx} 处理报告期 {end_date}")
        
        success = False
        df = None
        
        try:
            # 获取股票代码列表
            stock_codes = self.get_stock_codes()
            if not stock_codes:
                logger.warning(f"没有找到股票代码，WAN {wan_idx} 无法处理报告期 {end_date}")
                return False
                
            # 按每批100支股票进行批处理
            batch_size = 100
            all_data = []
            batch_success = False
            
            for i in range(0, len(stock_codes), batch_size):
                batch_ts_codes = stock_codes[i:i+batch_size]
                batch_num = i // batch_size + 1
                total_batches = (len(stock_codes) + batch_size - 1) // batch_size
                
                logger.info(f"WAN {wan_idx} 处理报告期 {end_date} 的批次 {batch_num}/{total_batches}，包含 {len(batch_ts_codes)} 支股票")
                
                # 获取批次数据 - 直接传递 wan_idx 给 fetch_data
                batch_df = self.fetch_data(ts_code=",".join(batch_ts_codes), end_date=end_date, wan_idx=wan_idx)
                if batch_df is not None and not batch_df.empty:
                    all_data.append(batch_df)
                    batch_success = True
            
            # 如果获取了数据，则合并和处理
            if batch_success and all_data:
                df = pd.concat(all_data, ignore_index=True)
                logger.info(f"WAN {wan_idx} 成功获取报告期 {end_date} 的数据，共 {len(df)} 条记录")
            else:
                logger.warning(f"WAN {wan_idx} 未能获取报告期 {end_date} 的任何数据")
                return False
            
            # 处理数据
            processed_df = self.process_data(df)
            if processed_df is None or processed_df.empty:
                logger.warning(f"报告期 {end_date} 的处理后数据为空")
                return False
            
            # 保存单日数据到MongoDB
            if not self.mongodb_handler.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if not self.mongodb_handler.connect():
                    logger.error("连接MongoDB失败")
                    # 添加到结果队列，供后续处理
                    self.result_queue.put((end_date, processed_df))
                    return False
                    
            # 保存到MongoDB
            success = self.save_to_mongodb(processed_df)
            if success:
                logger.success(f"报告期 {end_date} 的数据已保存到MongoDB")
            else:
                logger.error(f"保存报告期 {end_date} 的数据到MongoDB失败")
                # 添加到结果队列，供后续处理
                self.result_queue.put((end_date, processed_df))
        except Exception as e:
            logger.error(f"处理报告期 {end_date} 时发生异常: {str(e)}")
            # 如果有数据但处理失败，添加到结果队列
            if df is not None and not df.empty:
                processed_df = self.process_data(df)
                if processed_df is not None and not processed_df.empty:
                    self.result_queue.put((end_date, processed_df))
        
        return success
    
    def _process_date_parallel(self, report_dates: List[str]) -> bool:
        """
        并行处理多个报告期数据
        
        Args:
            report_dates: 报告期列表
            
        Returns:
            是否全部成功
        """
        if self.available_wan_count == 0:
            logger.warning("未找到可用的WAN口，降级为串行处理模式")
            # 降级为串行处理
            all_success = True
            for end_date in report_dates:
                # 检查是否收到停止信号
                if STOP_PROCESSING:
                    logger.warning("收到停止信号，中断处理")
                    return False
                
                try:
                    df = self.fetch_stk_rewards_data(end_date)
                    if df is not None and not df.empty:
                        processed_df = self.process_data(df)
                        if processed_df is not None and not processed_df.empty:
                            success = self.save_to_mongodb(processed_df)
                            if not success:
                                logger.error(f"保存报告期 {end_date} 的数据到MongoDB失败")
                                self.result_queue.put((end_date, processed_df))
                                all_success = False
                except Exception as e:
                    logger.error(f"处理报告期 {end_date} 时发生异常: {str(e)}")
                    all_success = False
            return all_success
        
        threads_count = self.available_wan_count
        logger.info(f"并行处理 {len(report_dates)} 个报告期的数据，线程数: {threads_count}，可用WAN口: {self.available_wan_indices}")
        
        # 确保WAN索引列表有效
        if not self.available_wan_indices:
            logger.error("可用WAN口列表为空，无法进行并行处理")
            return False
        
        # 均匀分配报告期到可用WAN口
        date_groups = {wan_idx: [] for wan_idx in self.available_wan_indices}
        for i, date in enumerate(report_dates):
            wan_idx = self.available_wan_indices[i % len(self.available_wan_indices)]
            date_groups[wan_idx].append(date)
        
        # 记录分配情况
        for wan_idx, dates in date_groups.items():
            logger.info(f"WAN口 {wan_idx} 分配到 {len(dates)} 个报告期")
        
        all_success = True
        processed_dates_count = 0  # 添加计数器跟踪处理的报告期数
        
        # 创建每个WAN口对应的处理函数
        def process_wan_dates(wan_idx, dates):
            """处理单个WAN口对应的所有报告期"""
            if not dates:  # 没有报告期需要处理
                logger.info(f"WAN口 {wan_idx} 没有分配到报告期，跳过")
                return True, 0
                
            # 获取WAN口锁，确保同一时间只有一个线程使用此WAN口
            if not self.wan_locks[wan_idx].acquire(timeout=10):  # 增加超时时间
                logger.warning(f"无法获取WAN口 {wan_idx} 的锁，跳过处理")
                return False, 0
            
            logger.info(f"线程成功获取WAN口 {wan_idx} 的锁")
            
            try:
                logger.info(f"线程开始处理WAN口 {wan_idx} 的 {len(dates)} 个报告期")
                
                wan_success = True
                success_count = 0
                
                # 逐个处理该WAN口的所有报告期
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
                            logger.warning(f"WAN口 {wan_idx} 处理报告期 {date} 失败")
                            wan_success = False
                    except Exception as e:
                        logger.error(f"WAN口 {wan_idx} 处理报告期 {date} 时发生异常: {str(e)}")
                        wan_success = False
                
                # 如果至少成功处理了一个报告期，视为部分成功
                if success_count > 0:
                    logger.info(f"WAN口 {wan_idx} 成功处理了 {success_count}/{len(dates)} 个报告期")
                    # 即使有些失败，只要有成功的，我们就不认为整体失败
                    wan_success = True
                
                logger.info(f"线程完成WAN口 {wan_idx} 的所有报告期处理")
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
                # 即使没有报告期，也提交任务以保持线程和WAN口一一对应
                logger.info(f"提交WAN口 {wan_idx} 处理 {len(dates)} 个报告期的任务")
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
        
        # 只有当没有成功处理任何报告期时，才返回失败
        if processed_dates_count == 0 and len(report_dates) > 0:
            logger.error(f"所有 {len(report_dates)} 个报告期都处理失败")
            return False
        
        # 如果至少处理了一些报告期，就返回成功
        logger.success(f"成功处理了 {processed_dates_count}/{len(report_dates)} 个报告期")
        return True
    
    def fetch_stock_data(self, ts_codes: Union[str, List[str]], end_date: str = None) -> Optional[pd.DataFrame]:
        """
        获取指定股票代码的管理层薪酬及持股数据
        
        Args:
            ts_codes: 股票代码，可以是单个代码或代码列表
            end_date: 报告期，格式为YYYYMMDD
            
        Returns:
            管理层薪酬及持股数据DataFrame
        """
        # 确保ts_codes是字符串
        if isinstance(ts_codes, list):
            if not ts_codes:
                logger.warning("没有提供股票代码")
                return None
            # 将列表转换为逗号分隔的字符串，full模式下最多包含1个代码，其他模式最多100个代码
            max_batch_size = 1 if self.full_mode else 100
            ts_codes = ",".join(ts_codes[:max_batch_size])
            
        logger.info(f"正在获取股票代码 {ts_codes} 的管理层薪酬及持股数据...")
        
        # 创建参数字典
        params = {'ts_code': ts_codes}
        
        # 在非full模式下，如果提供了end_date，添加到参数中
        if end_date and not self.full_mode:
            params['end_date'] = end_date
            logger.debug(f"使用报告期: {end_date}")
        elif self.full_mode:
            logger.debug("Full模式下不指定end_date参数，获取所有报告期数据")
        
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
        并行处理多个股票代码数据，按批次进行处理
        full模式下每批次1支股票，其他模式每批次最多100支股票
        
        Args:
            stock_codes: 股票代码列表
            
        Returns:
            是否全部成功
        """
        if self.available_wan_count == 0:
            logger.warning("未找到可用的WAN口，降级为串行处理模式")
            # 降级为串行处理
            all_success = True
            
            # 按批次处理股票代码
            batch_size = 1 if self.full_mode else 100  # full模式下每批1支，其他模式每批最多100支
            for i in range(0, len(stock_codes), batch_size):
                batch_ts_codes = stock_codes[i:i+batch_size]
                
                # 检查是否收到停止信号
                if STOP_PROCESSING:
                    logger.warning("收到停止信号，中断处理")
                    return False
                    
                try:
                    # 批量获取数据
                    df = self.fetch_stock_data(batch_ts_codes, self.end_date if not self.full_mode else None)
                    if df is None or df.empty:
                        logger.warning(f"批次 {i//batch_size + 1} 的数据为空或获取失败")
                        continue
                    
                    processed_df = self.process_data(df)
                    if processed_df is None or processed_df.empty:
                        logger.warning(f"批次 {i//batch_size + 1} 的处理后数据为空")
                        continue
                    
                    success = self.save_to_mongodb(processed_df)
                    if not success:
                        logger.error(f"保存批次 {i//batch_size + 1} 的数据到MongoDB失败")
                        all_success = False
                        
                    # 在full模式下添加2-5秒的随机延时
                    if self.full_mode:
                        delay = random.uniform(2, 5)
                        logger.info(f"Full模式：等待 {delay:.2f} 秒后继续下一个请求...")
                        time.sleep(delay)
                except Exception as e:
                    logger.error(f"处理批次 {i//batch_size + 1} 时发生异常: {str(e)}")
                    all_success = False
            return all_success
        
        # 以下是并行处理模式
        threads_count = self.available_wan_count
        
        # 将股票代码按批次分组
        batch_size = 1 if self.full_mode else 100  # full模式下每批1支，其他模式每批最多100支
        batched_stock_codes = []
        for i in range(0, len(stock_codes), batch_size):
            batched_stock_codes.append(stock_codes[i:i+batch_size])
        
        logger.info(f"并行处理 {len(stock_codes)} 个股票的数据，分为 {len(batched_stock_codes)} 个批次，线程数: {threads_count}，可用WAN口: {self.available_wan_indices}")
        
        # 确保WAN索引列表有效
        if not self.available_wan_indices:
            logger.error("可用WAN口列表为空，无法进行并行处理")
            return False
        
        # 均匀分配股票批次到可用WAN口
        batch_groups = {wan_idx: [] for wan_idx in self.available_wan_indices}
        for i, batch in enumerate(batched_stock_codes):
            wan_idx = self.available_wan_indices[i % len(self.available_wan_indices)]
            batch_groups[wan_idx].append(batch)
        
        # 记录分配情况
        for wan_idx, batches in batch_groups.items():
            total_stocks = sum(len(batch) for batch in batches)
            logger.info(f"WAN口 {wan_idx} 分配到 {len(batches)} 个批次，共 {total_stocks} 支股票")
        
        processed_stocks_count = 0
        
        # 创建每个WAN口对应的处理函数
        def process_wan_batches(wan_idx, batches):
            """处理单个WAN口对应的所有批次"""
            if not batches:  # 没有批次需要处理
                logger.info(f"WAN口 {wan_idx} 没有分配到批次，跳过")
                return True, 0
                
            # 获取WAN口锁，确保同一时间只有一个线程使用此WAN口
            if not self.wan_locks[wan_idx].acquire(timeout=10):
                logger.warning(f"无法获取WAN口 {wan_idx} 的锁，跳过处理")
                return False, 0
            
            logger.info(f"线程成功获取WAN口 {wan_idx} 的锁")
            
            try:
                total_stocks = sum(len(batch) for batch in batches)
                logger.info(f"线程开始处理WAN口 {wan_idx} 的 {len(batches)} 个批次，共 {total_stocks} 支股票")
                
                wan_success = True
                success_count = 0
                
                for batch_idx, batch in enumerate(batches):
                    # 检查是否收到停止信号
                    if STOP_PROCESSING:
                        logger.warning(f"WAN口 {wan_idx} 收到停止信号，中断处理")
                        return wan_success, success_count
                        
                    try:
                        # 打印正在处理的批次
                        logger.info(f"WAN口 {wan_idx} 正在处理批次 {batch_idx+1}/{len(batches)}，包含 {len(batch)} 支股票")
                        
                        # 获取批次数据，在full模式下不传递end_date参数
                        end_date_param = None if self.full_mode else self.end_date
                        df = self.fetch_data(ts_code=",".join(batch), end_date=end_date_param, wan_idx=wan_idx)
                        if df is None or df.empty:
                            logger.warning(f"批次 {batch_idx+1} 的数据为空或获取失败")
                            continue
                        
                        # 处理数据
                        processed_df = self.process_data(df)
                        if processed_df is None or processed_df.empty:
                            logger.warning(f"批次 {batch_idx+1} 的处理后数据为空")
                            continue
                        
                        # 保存到MongoDB
                        success = self.save_to_mongodb(processed_df)
                        if success:
                            logger.success(f"批次 {batch_idx+1} 的数据已保存到MongoDB，包含 {len(processed_df)} 条记录")
                            success_count += len(batch)
                        else:
                            logger.error(f"保存批次 {batch_idx+1} 的数据到MongoDB失败")
                            wan_success = False
                        
                        # 在full模式下添加2-5秒的随机延时
                        if self.full_mode:
                            delay = random.uniform(2, 5)
                            logger.info(f"WAN {wan_idx} Full模式：等待 {delay:.2f} 秒后继续下一个请求...")
                            time.sleep(delay)
                    except Exception as e:
                        logger.error(f"WAN口 {wan_idx} 处理批次 {batch_idx+1} 时发生异常: {str(e)}")
                        wan_success = False
                
                # 如果至少成功处理了一个批次，视为部分成功
                if success_count > 0:
                    logger.info(f"WAN口 {wan_idx} 成功处理了 {success_count}/{total_stocks} 支股票")
                    wan_success = True
                
                logger.info(f"线程完成WAN口 {wan_idx} 的所有批次处理")
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
                batches = batch_groups.get(wan_idx, [])
                # 即使没有批次，也提交任务以保持线程和WAN口一一对应
                logger.info(f"提交WAN口 {wan_idx} 处理 {len(batches)} 个批次的任务")
                future = executor_pool.submit(process_wan_batches, wan_idx, batches)
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
                # 完整模式：按股票代码获取
                logger.info("使用完整模式，按股票代码获取管理层薪酬及持股历史数据（不指定end_date，每批次1支股票，2-5秒延时）")
                
                # 第二步：获取所有股票代码
                logger.info("第二步：从stock_basic集合获取股票代码列表")
                stock_codes = self.get_stock_codes()
                
                if not stock_codes:
                    logger.error("未能获取到任何股票代码，抓取失败")
                    return False  # 没有找到股票代码应该返回失败
                
                # 第三步：获取数据（串行或并行）
                logger.info(f"第三步：处理 {len(stock_codes)} 个股票的数据")
                
                if self.serial_mode:
                    # 串行模式 - 每次处理1支股票
                    batch_size = 1  # full模式下每批次只处理1支股票
                    logger.info(f"使用串行模式处理 {len(stock_codes)} 个股票的数据，每批次 {batch_size} 支股票，请求间隔2-5秒")
                    all_success = True
                    
                    for i in range(0, len(stock_codes), batch_size):
                        batch_ts_codes = stock_codes[i:i+batch_size]
                        batch_num = i // batch_size + 1
                        total_batches = (len(stock_codes) + batch_size - 1) // batch_size
                        
                        # 检查是否收到停止信号
                        if STOP_PROCESSING:
                            logger.warning("收到停止信号，中断处理")
                            return False
                            
                        logger.info(f"正在处理批次 {batch_num}/{total_batches}，包含 {len(batch_ts_codes)} 支股票")
                        
                        try:
                            # 批量获取股票数据 - 在full模式下不传递end_date参数
                            df = self.fetch_stock_data(batch_ts_codes)
                            if df is None or df.empty:
                                logger.warning(f"批次 {batch_num} 的数据为空或获取失败")
                                continue
                            
                            # 处理数据
                            processed_df = self.process_data(df)
                            if processed_df is None or processed_df.empty:
                                logger.warning(f"批次 {batch_num} 的处理后数据为空")
                                continue
                            
                            # 保存数据到MongoDB
                            success = self.save_to_mongodb(processed_df)
                            if success:
                                logger.success(f"批次 {batch_num} 的数据已保存到MongoDB，包含 {len(processed_df)} 条记录")
                            else:
                                logger.error(f"保存批次 {batch_num} 的数据到MongoDB失败")
                                all_success = False
                            
                            # 添加2-5秒的随机延时
                            if batch_num < total_batches:  # 如果不是最后一批
                                delay = random.uniform(2, 5)
                                logger.info(f"Full模式：等待 {delay:.2f} 秒后继续下一个请求...")
                                time.sleep(delay)
                        except Exception as e:
                            logger.error(f"处理批次 {batch_num} 的数据时发生异常: {str(e)}")
                            all_success = False
                    
                    return all_success
                else:
                    # 并行模式
                    logger.info(f"使用并行模式处理 {len(stock_codes)} 个股票的数据，每批次1支股票，请求间隔2-5秒")
                    return self._process_stock_parallel(stock_codes)
            else:
                # 日期模式：按报告期获取数据
                logger.info("使用日期模式，按报告期获取数据")
                
                # 第二步：获取季度末的报告期日期
                recent_mode = not (self.start_date and self.end_date)
                logger.info(f"第二步：获取{'最近一个已完成季度' if recent_mode else f'日期范围 {self.start_date} - {self.end_date} 内'}的季度末报告期...")
                report_dates = self.get_quarter_end_dates(self.start_date, self.end_date, recent=recent_mode)
                
                if not report_dates:
                    logger.warning("未找到可能的报告期，没有数据需要处理")
                    return True  # 没有数据也视为成功
                
                # 第三步：获取数据（串行或并行）
                if self.serial_mode:
                    # 串行模式
                    logger.info(f"第三步：串行处理 {len(report_dates)} 个报告期的数据")
                    all_success = True
                    
                    for end_date in report_dates:
                        # 检查是否收到停止信号
                        if STOP_PROCESSING:
                            logger.warning("收到停止信号，中断处理")
                            return False
                            
                        logger.info(f"正在处理报告期: {end_date}")
                        
                        try:
                            # 获取报告期数据
                            df = self.fetch_stk_rewards_data(end_date)
                            if df is None or df.empty:
                                logger.warning(f"报告期 {end_date} 的数据为空或获取失败")
                                continue
                            
                            # 处理数据
                            processed_df = self.process_data(df)
                            if processed_df is None or processed_df.empty:
                                logger.warning(f"报告期 {end_date} 的处理后数据为空")
                                continue
                            
                            # 保存单日数据到MongoDB
                            success = self.save_to_mongodb(processed_df)
                            if success:
                                logger.success(f"报告期 {end_date} 的数据已保存到MongoDB")
                            else:
                                logger.error(f"保存报告期 {end_date} 的数据到MongoDB失败")
                                all_success = False
                        except Exception as e:
                            logger.error(f"处理报告期 {end_date} 的数据时发生异常: {str(e)}")
                            all_success = False
                    
                    return all_success
                else:
                    # 并行模式 - 将日期处理的并行模式改为处理不同的报告期
                    logger.info(f"第三步：并行处理 {len(report_dates)} 个报告期的数据")
                    return self._process_date_parallel(report_dates)
            
            logger.info("数据获取和保存流程完成")
            return True
            
        except Exception as e:
            logger.error(f"运行过程中发生异常: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

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
            logger.warning("没有数据需要保存")
            return False
        
        # 获取数据库和集合名称
        db_name = self.db_name
        collection_name = self.collection_name
        
        logger.info(f"保存数据到MongoDB数据库：{db_name}，集合：{collection_name}")
        
        # 检查MongoDB连接
        if not self.mongodb_handler.is_connected():
            logger.warning("MongoDB未连接，尝试连接...")
            if not self.mongodb_handler.connect():
                logger.error("连接MongoDB失败")
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
                                # 移除文档中的_id字段，避免尝试修改不可变字段
                                update_doc = doc.copy()
                                if '_id' in update_doc:
                                    del update_doc['_id']
                                    
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
                        # full模式下增加重试等待时间
                        wait_time = random.uniform(2, 5) if self.full_mode else 1
                        logger.info(f"等待 {wait_time:.2f} 秒后重试...")
                        time.sleep(wait_time)  # 重试前等待
                        
                    except Exception as e:
                        retries += 1
                        logger.warning(f"批次 {i//chunk_size + 1} 处理失败 (尝试 {retries}/{max_retries}): {str(e)}")
                        if retries >= max_retries:
                            logger.error(f"批次 {i//chunk_size + 1} 处理失败，已达到最大重试次数")
                            return False
                        # full模式下增加重试等待时间
                        wait_time = random.uniform(2, 5) if self.full_mode else 1
                        logger.info(f"等待 {wait_time:.2f} 秒后重试...")
                        time.sleep(wait_time)  # 重试前等待
            
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

    # 添加一个获取季度末日期的函数
    def get_quarter_end_dates(self, start_date=None, end_date=None, recent=False) -> List[str]:
        """
        获取指定范围内的半年报和年报日期，或最近一个已完成的半年报/年报日期
        
        管理层薪酬及持股数据(stk_rewards)只在半年报和年报时发布，
        报告期为：6月30日（半年报）和12月31日（年报）
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            recent: 是否只返回最近一个已完成的半年报/年报日期
            
        Returns:
            半年报/年报日期列表，格式为YYYYMMDD
        """
        # 获取当前日期
        today = datetime.now()
        
        if recent:
            # 获取最近一个已完成的半年报/年报日期
            current_month = today.month
            current_year = today.year
            
            # 确定最近的报告期日期
            if current_month <= 6:
                # 去年年报
                report_month = 12
                report_year = current_year - 1
                report_day = 31
            else:
                # 今年半年报
                report_month = 6
                report_year = current_year
                report_day = 30
            
            report_date = datetime(report_year, report_month, report_day)
            return [report_date.strftime("%Y%m%d")]
        
        # 如果未提供日期范围，使用默认范围
        if not start_date or not end_date:
            if not end_date:
                end_date = today.strftime("%Y%m%d")
            if not start_date:
                # 默认为当年1月1日开始
                start_date = f"{today.year}0101"
        
        # 转换为datetime对象
        start_date_obj = datetime.strptime(start_date, "%Y%m%d")
        end_date_obj = datetime.strptime(end_date, "%Y%m%d")
        
        # 确保开始日期早于结束日期
        if start_date_obj > end_date_obj:
            logger.warning(f"开始日期 {start_date} 晚于结束日期 {end_date}，将交换两个日期")
            start_date_obj, end_date_obj = end_date_obj, start_date_obj
        
        # 获取开始日期和结束日期所在的年份
        start_year = start_date_obj.year
        end_year = end_date_obj.year
        report_dates = []
        
        # 遍历年份
        for year in range(start_year, end_year + 1):
            # 每年的半年报和年报日期
            report_ends = [
                datetime(year, 6, 30),  # 半年报
                datetime(year, 12, 31)  # 年报
            ]
            
            # 只添加在日期范围内的报告期日期
            for report_end in report_ends:
                if start_date_obj <= report_end <= end_date_obj:
                    report_dates.append(report_end)
        
        # 转换为YYYYMMDD格式
        report_end_dates = [r.strftime("%Y%m%d") for r in report_dates]
        logger.info(f"找到 {len(report_end_dates)} 个报告期日期: {', '.join(report_end_dates) if report_end_dates else '无'}")
        return report_end_dates

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
    
    parser = argparse.ArgumentParser(description='获取管理层薪酬及持股数据并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--exchange', default='SSE', help='交易所代码：SSE-上交所, SZSE-深交所')
    parser.add_argument('--start-date', help='开始日期，格式：YYYYMMDD')
    parser.add_argument('--end-date', help='结束日期/报告期，格式：YYYYMMDD')
    parser.add_argument('--db-name', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='stk_rewards', help='MongoDB集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    parser.add_argument('--shared-config', type=str, default=None, help='共享配置文件路径')
    parser.add_argument('--skip-validation', action='store_true', help='跳过配置验证')
    parser.add_argument('--serial', action='store_true', help='使用串行模式处理数据（默认为并行模式）')
    parser.add_argument('--recent', action='store_true', help='获取最近一个已完成季度的数据')
    parser.add_argument('--max-workers', type=int, default=3, help='并行模式下的最大工作线程数（默认为3）')
    parser.add_argument('--full', action='store_true', help='使用完整模式，按股票代码列表获取所有股票的基本数据')
    
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
        
        # 处理recent选项 - 如果指定了recent，忽略start_date和end_date
        if args.recent:
            args.start_date = None
            args.end_date = None
            logger.info("使用--recent选项，将获取最近一个已完成季度的数据")
        
        # 创建获取器并运行 - 传入 mongo_instance
        fetcher = stk_rewardsFetcher(
            config_path=args.config,
            interface_dir=args.interface_dir,
            exchange=args.exchange,
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
                logger.success("管理层薪酬及持股数据获取和保存成功")
                return 0
            else:
                logger.error("管理层薪酬及持股数据获取或保存失败")
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