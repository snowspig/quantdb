#!/usr/bin/env python
"""
top10_holders Basic Fetcher V2 - 获取前十大股东并保存到MongoDB

该脚本用于从湘财Tushare获取前十大股东，并保存到MongoDB数据库中
该版本继承TushareFetcher基类，实现了与stock_basic_fetcher相同的架构和功能

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=26

使用方法：
    python top10_holders_fetcher.py                   # 使用湘财真实API数据，简洁日志模式，获取近期数据
    python top10_holders_fetcher.py --verbose         # 使用湘财真实API数据，详细日志模式
    python top10_holders_fetcher.py --start-date 20200101 --end-date 20231231  # 指定日期范围
    python top10_holders_fetcher.py --exchange SZSE   # 获取深交所的日线数据
    python top10_holders_fetcher.py --serial          # 使用串行模式处理数据（默认为并行模式）
    python top10_holders_fetcher.py --full            # 使用完整模式，按股票代码列表获取所有股票的基本数据
    python top10_holders_fetcher.py --recent          # 使用recent模式，抓取所有股票最近一季度数据
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

class top10_holdersFetcher(TushareFetcher):
    """
    前十大股东获取器V2
    
    该类用于从Tushare获取前十大股东并保存到MongoDB数据库
    使用TushareFetcher基类提供的通用功能
    支持串行和并行两种处理模式
    支持按日期和按股票代码两种抓取模式
    """
    
    def get_api_delay_time(self, wan_idx=None):
        """
        获取API调用之间的延时时间
        
        根据当前模式返回合适的延时时间
        full模式: 8-15秒
        recent模式: 5-10秒
        标准模式: 2-5秒
        fake_full_mode: 2-5秒（覆盖full模式）
        
        Args:
            wan_idx: 可选的WAN口索引，用于日志输出
            
        Returns:
            生成的延时时间
        """
        # 根据模式选择不同的延时
        if self.full_mode:
            # full模式使用更长的延时
            delay = random.uniform(8.0, 15.0)
            wan_info = f"WAN口 {wan_idx} " if wan_idx is not None else ""
            logger.info(f"Full模式 - {wan_info}API调用延时: {delay:.2f}秒")
        elif self.recent_mode:
            # recent模式使用中等延时
            delay = random.uniform(5.0, 10.0)
            wan_info = f"WAN口 {wan_idx} " if wan_idx is not None else ""
            logger.info(f"Recent模式 - {wan_info}API调用延时: {delay:.2f}秒")
        else:
            # 标准模式使用较短延时
            delay = random.uniform(2.0, 5.0)
            wan_info = f"WAN口 {wan_idx} " if wan_idx is not None else ""
            logger.debug(f"{wan_info}API调用延时: {delay:.2f}秒")
            
        # 如果是fake_full_mode模式，则使用短延时覆盖
        if self.full_mode and self.fake_full_mode:
            delay = random.uniform(2.0, 5.0)
            wan_info = f"WAN口 {wan_idx} " if wan_idx is not None else ""
            logger.info(f"伪全量模式 - {wan_info}覆盖为短延时 {delay:.2f} 秒")
            
        return delay
    
    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "top10_holders.json",
        db_name: str = None,
        collection_name: str = "top10_holders",
        start_date: str = None,
        end_date: str = None,
        exchange: str = "SSE",  # 默认上交所
        verbose: bool = False,
        shared_config: Dict[str, Any] = None,
        skip_validation: bool = False,
        serial_mode: bool = False,  # 是否使用串行模式
        max_workers: int = 3,  # 并行模式下的最大工作线程数
        full_mode: bool = False,  # 是否使用完整模式（按股票代码抓取）
        recent_mode: bool = True,  # 默认使用recent模式（抓取最近一季度）
        fake_full_mode: bool = False, # 是否使用伪全量模式（看起来像全量但有优化）
        mongo_handler_instance: Optional[MongoDBHandler] = None # 新增参数
    ):
        """
        初始化前十大股东获取器
        
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
            recent_mode: 是否使用recent模式（抓取最近一季度）
            fake_full_mode: 是否使用伪全量模式（看起来像全量但有优化）
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
        
        # 保存模式设置，后面再处理优先级
        self.serial_mode = serial_mode
        self.full_mode = full_mode
        self.recent_mode = recent_mode
        self.fake_full_mode = fake_full_mode
        
        # 用户是否明确指定了日期范围
        user_specified_dates = start_date is not None and end_date is not None
        
        # 优先处理full_mode
        if self.full_mode:
            # 在full模式下，不受recent_mode影响
            self.recent_mode = False
            logger.info("使用完整模式(full)，将获取全部历史数据")
            
            # 如果设置了fake_full_mode，优化处理
            if self.fake_full_mode:
                logger.info("使用伪全量模式，将优化抓取效率")
            
            # 在full模式下，如果用户没有指定日期范围，设置为1990年至今
            if not user_specified_dates:
                today = datetime.now()
                self.end_date = today.strftime("%Y%m%d")
                self.start_date = "19900101"  # 从1990年1月1日开始
                logger.info(f"完整模式下设置日期范围: {self.start_date} - {self.end_date}")
            else:
                # 用户指定了日期范围，使用用户指定的
                self.start_date = start_date
                self.end_date = end_date
                logger.info(f"完整模式下使用用户指定的日期范围: {self.start_date} - {self.end_date}")
        elif not user_specified_dates:
            # 用户没有指定日期范围，且不是full模式
            today = datetime.now()
            self.end_date = today.strftime("%Y%m%d")
            
            if not self.recent_mode:
                # 不是recent模式也没有指定日期，使用默认的一周
                one_week_ago = today - timedelta(days=7)
                self.start_date = one_week_ago.strftime("%Y%m%d")
                logger.info(f"使用默认日期范围(一周): {self.start_date} - {self.end_date}")
            else:
                # recent模式，计算上一季度
                current_month = today.month
                current_year = today.year
                
                # 确定当前所在季度
                if 1 <= current_month <= 3:  # 第一季度
                    # 上一季度是上一年第四季度
                    quarter_start_month = 10
                    quarter_start_year = current_year - 1
                elif 4 <= current_month <= 6:  # 第二季度
                    # 上一季度是当年第一季度
                    quarter_start_month = 1
                    quarter_start_year = current_year
                elif 7 <= current_month <= 9:  # 第三季度
                    # 上一季度是当年第二季度
                    quarter_start_month = 4
                    quarter_start_year = current_year
                else:  # 第四季度
                    # 上一季度是当年第三季度
                    quarter_start_month = 7
                    quarter_start_year = current_year
                
                # 设置上一季度的第一天
                last_quarter_start = datetime(quarter_start_year, quarter_start_month, 1)
                self.start_date = last_quarter_start.strftime("%Y%m%d")
                
                logger.info(f"使用recent模式，抓取上一季度至今数据，时间范围: {self.start_date} - {self.end_date}")
                logger.info(f"上一季度开始日期: {last_quarter_start.strftime('%Y年%m月%d日')}")
        else:
            # 用户指定了日期范围，不是full模式
            self.start_date = start_date
            self.end_date = end_date
            
            # 如果用户指定了日期范围，但同时开启了recent_mode，发出警告
            if self.recent_mode:
                logger.warning(f"用户指定了日期范围 {self.start_date} - {self.end_date}，recent模式将被关闭")
                self.recent_mode = False
        
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
        elif self.recent_mode:
            logger.info(f"抓取模式: Recent模式，将抓取所有股票在 {self.start_date} - {self.end_date} 期间的数据")
        else:
            logger.info(f"抓取模式: 日期模式(按公告日期)，日期范围: {self.start_date} - {self.end_date}")
        
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
        从Tushare获取前十大股东数据
        
        Args:
            **kwargs: 查询参数，包括：
                ts_code: 股票代码
                ann_date: 公告日期
                start_date: 开始日期
                end_date: 结束日期
                wan_idx: 指定WAN口索引，可选
                use_wan: 是否使用WAN口，默认True
        
        Returns:
            返回DataFrame或者None（如果出错）
        """
        ts_code = kwargs.get('ts_code')
        ann_date = kwargs.get('ann_date')
        start_date = kwargs.get('start_date')
        end_date = kwargs.get('end_date')
        
        # 是否使用WAN口
        use_wan = kwargs.get('use_wan', True)
        
        # 提取WAN口索引（如果指定了）
        wan_idx = kwargs.get('wan_idx')
        
        # 打印详细信息
        info_msg = "【抓取参数】"
        if ts_code:
            info_msg += f"股票={ts_code} "
        if ann_date:
            info_msg += f"公告日期={ann_date} "
        if start_date:
            info_msg += f"开始日期={start_date} "
        if end_date:
            info_msg += f"结束日期={end_date} "
        if wan_idx is not None:
            info_msg += f"(WAN口={wan_idx})"
        logger.debug(info_msg)
        
        # 设置API参数
        params = {}
        if ts_code:
            params['ts_code'] = ts_code
        if ann_date:
            params['ann_date'] = ann_date
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
        
        # 参数检查：确保至少有ts_code参数，推荐同时使用start_date和end_date
        if not ts_code:
            logger.error("必须提供ts_code参数")
            return None
        
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
                    api_name='top10_holders', 
                    params=params,
                    wan_idx=wan_idx # 传递 wan_idx，不传递timeout参数
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
        
        # 注意：对API的每次调用后，应使用get_api_delay_time方法确定合适的延时时间
        # 这样可以根据不同模式优化抓取速度，防止API限制，提供了以下延时配置:
        # - full模式: 使用8-15秒延时，防止API限制
        # - recent模式: 使用5-10秒延时，平衡速度和稳定性
        # - 标准模式: 使用2-5秒延时，提高抓取速度
        # - fake_full_mode: 覆盖为2-5秒延时
    
    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        处理获取的前十大股东
        只保留股票代码前两位为00、30、60、68的数据
        但在full模式下直接返回原始数据，不进行过滤
        
        Args:
            df: 原始前十大股东
            
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
        获取指定日期范围内的公告日期列表
        
        先从trade_cal集合中查询实际交易日，如果失败则生成日期范围内的所有日期
        作为前十大股东的公告日期范围
        
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

    def fetch_top10_holders_data(self, ann_date: str) -> Optional[pd.DataFrame]:
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
            logger.error("未能获取到股票代码列表，无法获取前十大股东数据")
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
                
                # 使用延时方法获取延时时间
                delay = self.get_api_delay_time()
                time.sleep(delay)
                
                # 如果是fake_full_mode模式，不需要额外的操作
                # get_api_delay_time方法已经处理了fake_full_mode的特殊情况
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
                df = self.fetch_top10_holders_data(ann_date)
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
        combined_df = None
        
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
                        
                    # 使用延时方法获取延时时间
                    delay = self.get_api_delay_time(wan_idx)
                    time.sleep(delay)
                    
                    # 如果是fake_full_mode模式，不需要额外的操作
                    # get_api_delay_time方法已经处理了fake_full_mode的特殊情况
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
            if combined_df is not None and not combined_df.empty:
                processed_df = self.process_data(combined_df)
                if processed_df is not None and not processed_df.empty:
                    self.result_queue.put((ann_date, processed_df))
        
        return success
    
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
                df = self.fetch_data(ts_code=ts_code, start_date=self.start_date, end_date=self.end_date)
                
                # 使用延时方法获取延时时间
                delay = self.get_api_delay_time()
                time.sleep(delay)
                
                # 如果是fake_full_mode模式，不需要额外的操作
                # get_api_delay_time方法已经处理了fake_full_mode的特殊情况
                
                if df is None or df.empty:
                    logger.info(f"【抓取结果】股票 {ts_code} 无数据")
                    continue
                
                # 数据抓取结果详细展示
                if 'ann_date' in df.columns:
                    unique_dates = df['ann_date'].nunique()
                    min_date = df['ann_date'].min() if not pd.isna(df['ann_date'].min()) else "未知"
                    max_date = df['ann_date'].max() if not pd.isna(df['ann_date'].max()) else "未知"
                    unique_reports = df['report_type'].nunique() if 'report_type' in df.columns else 0
                    
                    logger.info(f"【抓取成功】股票 {ts_code} 获取到 {len(df)} 条数据，"
                               f"{unique_dates}个公告日期({min_date}至{max_date})，"
                               f"{unique_reports}种报告类型")
                    
                    # 如果详细日志模式，输出前几位股东名称
                    if self.verbose and 'holder_name' in df.columns:
                        top_holders = df['holder_name'].unique()[:5]  # 取前5个不同的股东名称
                        holder_names = "，".join(top_holders)
                        logger.debug(f"【股东示例】股票 {ts_code} 的部分股东: {holder_names}")
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

    def run(self) -> bool:
        """
        运行数据获取和保存流程，确保同一个WAN口一次只处理一个股票代码
        
        本方法使用get_api_delay_time()进行批次间延时优化：
        - full模式: 使用8-15秒延时，防止API限制
        - recent模式: 使用5-10秒延时，平衡速度和稳定性
        - 标准模式: 使用2-5秒延时，提高抓取速度
        - fake_full_mode: 覆盖为2-5秒延时
        
        Returns:
            是否成功
        """
        try:
            # 第一步：检查并确保集合和索引存在
            logger.info("第一步：检查并确保MongoDB集合和索引存在")
            if not self._ensure_collection_and_indexes():
                logger.error("无法确保MongoDB集合和索引，放弃数据获取")
                return False
            
            # 第二步：获取股票代码列表
            logger.info("第二步：从stock_basic集合获取股票代码列表")
            stock_codes = self.get_stock_codes()
            
            if not stock_codes:
                logger.error("未能获取到任何股票代码，抓取失败")
                return False
            
            logger.info(f"第三步：使用单股票队列模式处理 {len(stock_codes)} 个股票的数据")
            logger.info(f"日期范围: {self.start_date} - {self.end_date}")
            
            # 如果没有可用的WAN口，使用串行处理
            if self.available_wan_count == 0:
                return self._process_stocks_serial(stock_codes)
            
            # 创建股票代码队列
            stock_queue = queue.Queue()
            for ts_code in stock_codes:
                stock_queue.put(ts_code)
            
            total_stocks = len(stock_codes)
            processed_stocks_count = 0
            success_count = 0
            
            # 创建结果字典，用于记录处理结果
            results = {}
            results_lock = threading.Lock()
            
            # 停止事件
            stop_event = threading.Event()
            
            # 定义WAN工作线程
            def wan_worker(wan_idx):
                """每个WAN口的工作线程，每次只处理一个股票代码"""
                nonlocal processed_stocks_count, success_count
                
                while not stop_event.is_set() and not stock_queue.empty():
                    try:
                        # 从队列获取一个股票代码
                        ts_code = stock_queue.get(block=False)
                    except queue.Empty:
                        # 队列为空，退出循环
                        break
                    
                    # 更新处理计数
                    with results_lock:
                        processed_stocks_count += 1
                        current_count = processed_stocks_count
                    
                    # 记录处理进度
                    if current_count % 10 == 1 or current_count == total_stocks:
                        logger.info(f"正在处理第 {current_count}/{total_stocks} 个股票: {ts_code} (WAN口 {wan_idx})")
                    
                    try:
                        # 清晰打印抓取的股票，改为INFO级别
                        logger.info(f"【开始抓取】股票 {ts_code} (WAN口 {wan_idx})")
                        
                        # 使用start_date和end_date参数获取单个股票的所有数据
                        df = self.fetch_data(
                            ts_code=ts_code, 
                            start_date=self.start_date, 
                            end_date=self.end_date, 
                            wan_idx=wan_idx
                        )
                        
                        # 使用延时方法获取延时时间
                        delay = self.get_api_delay_time(wan_idx)
                        time.sleep(delay)
                        
                        # 如果是fake_full_mode模式，不需要额外的操作
                        # get_api_delay_time方法已经处理了fake_full_mode的特殊情况
                        
                        if df is None or df.empty:
                            logger.info(f"【抓取结果】股票 {ts_code} 无数据 (WAN口 {wan_idx})")
                            # 标记队列任务完成
                            stock_queue.task_done()
                            continue
                        
                        # 数据抓取结果详细展示
                        if 'ann_date' in df.columns:
                            unique_dates = df['ann_date'].nunique()
                            min_date = df['ann_date'].min() if not pd.isna(df['ann_date'].min()) else "未知"
                            max_date = df['ann_date'].max() if not pd.isna(df['ann_date'].max()) else "未知"
                            unique_reports = df['report_type'].nunique() if 'report_type' in df.columns else 0
                            
                            logger.info(f"【抓取成功】股票 {ts_code} 获取到 {len(df)} 条数据，"
                                       f"{unique_dates}个公告日期({min_date}至{max_date})，"
                                       f"{unique_reports}种报告类型")
                            
                            # 如果详细日志模式，输出前几位股东名称
                            if self.verbose and 'holder_name' in df.columns:
                                top_holders = df['holder_name'].unique()[:5]  # 取前5个不同的股东名称
                                holder_names = "，".join(top_holders)
                                logger.debug(f"【股东示例】股票 {ts_code} 的部分股东: {holder_names}")
                        else:
                            logger.info(f"【抓取成功】股票 {ts_code} 获取到 {len(df)} 条数据")
                        
                        # 处理数据
                        processed_df = self.process_data(df)
                        if processed_df is None or processed_df.empty:
                            logger.info(f"【处理结果】股票 {ts_code} 的处理后数据为空 (WAN口 {wan_idx})")
                            # 标记队列任务完成
                            stock_queue.task_done()
                            continue
                        
                        # 保存到MongoDB
                        logger.info(f"【开始存储】股票 {ts_code} 的 {len(processed_df)} 条数据 (WAN口 {wan_idx})")
                        success = self.save_to_mongodb(processed_df)
                        
                        if success:
                            with results_lock:
                                success_count += 1
                                curr_success = success_count
                            
                            logger.info(f"【存储成功】股票 {ts_code} 的 {len(processed_df)} 条数据已保存到数据库 (WAN口 {wan_idx})")
                            if curr_success % 10 == 0:
                                logger.success(f"已成功保存 {curr_success}/{processed_stocks_count} 个股票的数据 (成功率: {curr_success*100/processed_stocks_count:.1f}%)")
                        else:
                            logger.error(f"【存储失败】股票 {ts_code} 的 {len(processed_df)} 条数据保存失败 (WAN口 {wan_idx})")
                        
                        # 标记队列任务完成
                        stock_queue.task_done()
                        
                    except Exception as e:
                        logger.error(f"WAN口 {wan_idx} 处理股票 {ts_code} 时发生异常: {str(e)}")
                        # 标记队列任务完成
                        stock_queue.task_done()
                        
                        # 添加延时，避免连续失败
                        time.sleep(random.uniform(2.0, 5.0))
                    
                    # 每处理50个股票输出一次总进度
                    if processed_stocks_count % 50 == 0:
                        logger.success(f"总进度: 已处理 {processed_stocks_count}/{total_stocks} 个股票，成功: {success_count}")
            
            # 创建并启动WAN工作线程
            threads = []
            for wan_idx in self.available_wan_indices:
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
                # 等待所有股票处理完成或收到停止信号
                while not stock_queue.empty():
                    if STOP_PROCESSING:
                        logger.warning("收到停止信号，中断处理")
                        stop_event.set()
                        break
                    # 定期输出进度
                    logger.debug(f"队列中还有 {stock_queue.qsize()} 个股票等待处理")
                    time.sleep(5.0)
                
                # 等待所有线程完成
                for thread in threads:
                    thread.join(timeout=1.0)
                    
                logger.success(f"单股票队列处理完成，成功处理 {success_count}/{total_stocks} 个股票")
                return success_count > 0  # 只要处理了一些数据就认为成功
                
            except KeyboardInterrupt:
                logger.warning("收到键盘中断，正在停止所有线程...")
                stop_event.set()
                return False
            
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
    
    parser = argparse.ArgumentParser(description='获取前十大股东并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--exchange', default='SSE', help='交易所代码：SSE-上交所, SZSE-深交所')
    parser.add_argument('--start-date', help='开始日期，格式：YYYYMMDD')
    parser.add_argument('--end-date', help='结束日期，格式：YYYYMMDD')
    parser.add_argument('--db-name', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='top10_holders', help='MongoDB集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    parser.add_argument('--shared-config', type=str, default=None, help='共享配置文件路径')
    parser.add_argument('--skip-validation', action='store_true', help='跳过配置验证')
    parser.add_argument('--serial', action='store_true', help='使用串行模式处理数据（默认为并行模式）')
    parser.add_argument('--recent', action='store_true', help='使用recent模式抓取最近一季度所有股票数据(默认已启用)')
    parser.add_argument('--no-recent', action='store_true', help='关闭recent模式，使用标准日期或full模式')
    parser.add_argument('--max-workers', type=int, default=3, help='并行模式下的最大工作线程数（默认为3）')
    parser.add_argument('--full', action='store_true', help='使用完整模式，按股票代码列表获取从1990年至今的全部数据')
    parser.add_argument('--fake-full', action='store_true', help='使用伪全量模式，批次间延时仅2-5秒')
    
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
        
        # 处理模式优先级
        recent_mode = True  # 默认启用recent模式
        full_mode = args.full
        
        # 如果开启了full模式，自动关闭recent模式
        if full_mode:
            recent_mode = False
            logger.info("已开启full模式，将获取从1990年至今的全部数据，自动关闭recent模式")
        
        # 如果用户明确指定了start_date和end_date，自动关闭recent_mode
        if args.start_date and args.end_date:
            recent_mode = False
            logger.info(f"用户指定了日期范围 {args.start_date} - {args.end_date}，自动关闭recent模式")
        
        # 如果明确指定了--no-recent参数，关闭recent模式
        if args.no_recent:
            recent_mode = False
            logger.info("用户指定关闭recent模式，将使用标准日期模式")
        
        # 如果明确指定了--recent参数，强制启用recent模式
        if args.recent:
            # 只有当用户没有指定full模式时，才考虑启用recent模式
            if not full_mode:
                recent_mode = True
                if args.start_date and args.end_date:
                    logger.warning(f"虽然指定了日期范围 {args.start_date} - {args.end_date}，"
                                 f"但由于指定了--recent参数，将优先使用上一季度日期范围")
            else:
                logger.warning("同时指定了--full和--recent参数，full模式优先，recent模式将被忽略")
        
        # 创建获取器并运行 - 传入 mongo_instance
        fetcher = top10_holdersFetcher(
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
            full_mode=full_mode,
            recent_mode=recent_mode,
            fake_full_mode=args.fake_full,
            mongo_handler_instance=mongo_instance # 传入 MongoDB Handler 实例
        )
        
        try:
            success = fetcher.run()
            
            if success:
                logger.success("前十大股东获取和保存成功")
                return 0
            else:
                logger.error("前十大股东获取或保存失败")
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