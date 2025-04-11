#!/usr/bin/env python
"""
Daily Basic Fetcher V2 - 获取日线基本数据并保存到MongoDB

该脚本用于从湘财Tushare获取日线基本数据，并保存到MongoDB数据库中
该版本继承TushareFetcher基类，实现了与stock_basic_fetcher相同的架构和功能

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=26

使用方法：
    python daily_basic_fetcher.py                   # 使用湘财真实API数据，简洁日志模式，获取近期数据
    python daily_basic_fetcher.py --verbose         # 使用湘财真实API数据，详细日志模式
    python daily_basic_fetcher.py --start-date 20200101 --end-date 20231231  # 指定日期范围
    python daily_basic_fetcher.py --exchange SZSE   # 获取深交所的日线数据
    python daily_basic_fetcher.py --serial          # 使用串行模式处理数据（默认为并行模式）
    python daily_basic_fetcher.py --full            # 使用完整模式，按股票代码列表获取所有股票的基本数据
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
from typing import List, Optional, Dict, Any, Tuple
from pathlib import Path
from loguru import logger
import random
import socket

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root))

# 导入平台核心模块
from core.tushare_fetcher import TushareFetcher
from core import mongodb_handler

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

class DailyBasicFetcher(TushareFetcher):
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
        interface_name: str = "daily_basic_ts.json",
        db_name: str = None,
        collection_name: str = "daily_basic",
        start_date: str = None,
        end_date: str = None,
        exchange: str = "SSE",  # 默认上交所
        verbose: bool = False,
        shared_config: Dict[str, Any] = None,
        skip_validation: bool = False,
        serial_mode: bool = False,  # 是否使用串行模式
        max_workers: int = 3,  # 并行模式下的最大工作线程数
        full_mode: bool = False  # 是否使用完整模式（按股票代码抓取）
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
        
        # 调用父类初始化方法
        super().__init__(**parent_args)
        
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
            # 设置导入超时，避免卡住
            import signal
            import threading
            
            def timeout_handler():
                # 超时后抛出异常
                raise ImportError("导入port_allocator超时")
            
            # 创建超时定时器
            timer = threading.Timer(5.0, timeout_handler)
            timer.start()
            
            # 尝试导入
            try:
                from core.wan_manager import port_allocator
                self.port_allocator = port_allocator
                
                # 直接从port_allocator获取可用WAN口信息，而不是自己解析配置
                self.available_wan_indices = self.port_allocator.get_available_wan_indices()
                self.available_wan_count = len(self.available_wan_indices)
                
                # 添加更详细的日志输出进行调试
                logger.info(f"从port_allocator获取到的WAN口索引: {self.available_wan_indices}")
            except ImportError as e:
                logger.error(f"导入port_allocator失败: {str(e)}")
                # 创建一个简单的内部端口分配器
                self._create_fallback_port_allocator()
            finally:
                # 无论是否导入成功，取消定时器
                timer.cancel()
        except Exception as e:
            logger.error(f"获取WAN口信息时出错: {str(e)}")
            # 创建一个简单的内部端口分配器
            self._create_fallback_port_allocator()
            
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
    
    def _create_fallback_port_allocator(self):
        """创建一个简单的内部端口分配器，用于在导入失败时替代"""
        import threading
        import random
        import socket
        
        class SimplePortAllocator:
            def __init__(self):
                self.logger = logger
                self.wan_ranges = {
                    0: (50001, 51000),
                    1: (51001, 52000),
                    2: (52001, 53000),
                }
                self.lock = threading.Lock()
                self.allocated_ports = {idx: set() for idx in self.wan_ranges.keys()}
            
            def get_available_wan_indices(self):
                return list(self.wan_ranges.keys())
            
            def allocate_port(self, wan_idx):
                if wan_idx not in self.wan_ranges:
                    wan_idx = 0
                    
                with self.lock:
                    start_port, end_port = self.wan_ranges[wan_idx]
                    for _ in range(5):
                        port = random.randint(start_port, end_port)
                        if port in self.allocated_ports[wan_idx]:
                            continue
                            
                        try:
                            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            s.settimeout(0.05)
                            s.bind(('0.0.0.0', port))
                            s.close()
                            self.allocated_ports[wan_idx].add(port)
                            return port
                        except:
                            pass
                    
                    # 如果所有尝试都失败，返回一个固定端口
                    fallback_port = start_port + (os.getpid() % (end_port - start_port))
                    self.allocated_ports[wan_idx].add(fallback_port)
                    return fallback_port
            
            def release_port(self, wan_idx, port):
                if wan_idx in self.wan_ranges and port in self.allocated_ports.get(wan_idx, set()):
                    self.allocated_ports[wan_idx].remove(port)
                    return True
                return False
        
        # 创建简易端口分配器并设置
        self.port_allocator = SimplePortAllocator()
        self.available_wan_indices = self.port_allocator.get_available_wan_indices()
        self.available_wan_count = len(self.available_wan_indices)
        logger.info(f"使用简易端口分配器，WAN口索引: {self.available_wan_indices}")
        
    def _get_wan_socket(self):
        """
        获取WAN socket连接
        
        返回WAN索引和端口号，如果没有可用的WAN口则返回None
        
        Returns:
            Tuple[int, int] or None: (WAN索引, 端口号) 或 None
        """
        try:
            # 如果没有可用的WAN口，返回None
            if not self.available_wan_indices:
                return None
                
            # 随机选择一个WAN口
            wan_idx = random.choice(self.available_wan_indices)
            
            # 为该WAN口分配一个端口
            port = self.port_allocator.allocate_port(wan_idx)
            
            if port is None:
                logger.error(f"无法为WAN {wan_idx} 分配端口")
                return None
                
            logger.debug(f"成功为WAN {wan_idx} 分配端口 {port}")
            return (wan_idx, port)
        except Exception as e:
            logger.error(f"获取WAN socket连接失败: {str(e)}")
            return None
    
    def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        从Tushare获取日线基本数据
        
        Args:
            **kwargs: 可以传入覆盖默认参数的值，比如exchange, start_date, end_date, trade_date, ts_code
            
        Returns:
            日线基本数据DataFrame，如果失败则返回None
        """
        try:
            # 检查是否收到停止信号
            if STOP_PROCESSING:
                logger.warning("收到停止信号，中断数据获取")
                return None
            
            # 准备API参数
            api_name = self.interface_config.get("api_name", "daily_basic_ts")
            
            # 设置API参数
            params = {}
            
            # 在full模式下使用ts_code时，不需要传递exchange参数
            full_mode_ts_code = self.full_mode and 'ts_code' in kwargs and kwargs['ts_code']
            
            # 只有在非full模式或非ts_code模式下才设置exchange参数
            if not full_mode_ts_code:
                exchange = kwargs.get('exchange', self.exchange)
                params["exchange"] = exchange
            
            # 处理不同的API调用模式
            if 'ts_code' in kwargs and kwargs['ts_code']:
                # 按股票代码模式: ts_code + [start_date, end_date]
                ts_code = kwargs['ts_code']
                params["ts_code"] = ts_code
                if full_mode_ts_code:
                    logger.info(f"使用股票代码模式获取数据，代码：{ts_code}，完整模式不传递exchange参数")
                else:
                    logger.info(f"使用股票代码模式获取数据，代码：{ts_code}")
                
                # 可以选择性添加日期范围
                if 'start_date' in kwargs and kwargs['start_date']:
                    params["start_date"] = kwargs['start_date']
                if 'end_date' in kwargs and kwargs['end_date']:
                    params["end_date"] = kwargs['end_date']
            elif 'trade_date' in kwargs and kwargs['trade_date']:
                # 单日模式: trade_date
                trade_date = kwargs['trade_date']
                params["trade_date"] = trade_date
                logger.info(f"使用单日模式获取数据，交易日：{trade_date}")
            else:
                # 日期范围模式: start_date + end_date
                start_date = kwargs.get('start_date', self.start_date)
                end_date = kwargs.get('end_date', self.end_date)
                params["start_date"] = start_date
                params["end_date"] = end_date
                logger.info(f"使用日期范围模式获取数据，日期范围：{start_date} 至 {end_date}")
            
            # 使用接口配置中的available_fields作为请求字段
            fields = self.available_fields
            if not fields:
                logger.warning("接口配置中未定义available_fields，将获取所有字段")
            
            # 创建并使用WAN接口的socket，实现多WAN请求支持
            wan_info = self._get_wan_socket()
            use_wan = wan_info is not None
            
            # 调用Tushare API
            logger.info(f"正在从湘财Tushare获取日线基本数据...")
            logger.debug(f"API参数: {params}")
            
            if use_wan:
                wan_idx, port = wan_info
                logger.debug(f"使用WAN接口 {wan_idx} 和本地端口 {port} 请求数据")
            
            start_time = time.time()
            
            # 增加超时，设置为120秒
            self.client.set_timeout(120)
            logger.debug(f"增加API请求超时时间为120秒，提高网络可靠性")
            
            # 添加异常捕获，以便更好地调试
            try:
                # 如果使用WAN接口，设置本地地址绑定
                if use_wan:
                    wan_idx, port = wan_info
                    self.client.set_local_address('0.0.0.0', port)
                
                df = self.client.get_data(api_name=api_name, params=params, fields=fields)
                
                # 重置客户端设置
                if use_wan:
                    self.client.reset_local_address()
            except Exception as e:
                import traceback
                logger.error(f"获取API数据时发生异常: {str(e)}")
                logger.debug(f"异常详情: {traceback.format_exc()}")
                
                # 释放WAN端口（如果使用了）
                if use_wan:
                    wan_idx, port = wan_info
                    self.port_allocator.release_port(wan_idx, port)
                    
                return None
            
            # 数据验证和处理
            if df is not None and not df.empty:
                logger.success(f"成功获取数据，行数: {len(df)}, 列数: {df.shape[1]}")
                
                # 检查返回的数据包含的字段
                logger.debug(f"API返回的字段: {list(df.columns)}")
                
                # 确保返回字段与接口配置中的字段匹配
                # 但不做字段转换，保留API返回的原始字段名
                missing_fields = [field for field in self.index_fields if field not in df.columns]
                if missing_fields:
                    logger.warning(f"API返回的数据缺少索引字段: {missing_fields}，这可能会影响数据存储")
                    
                    # 如果缺少exchange字段但我们知道交易所，则添加
                    if "exchange" in missing_fields and "exchange" not in df.columns:
                        df["exchange"] = exchange
                        logger.info(f"已添加默认交易所字段: {exchange}")
                
                # 确保日期字段是字符串格式
                if "trade_date" in df.columns and df["trade_date"].dtype != "object":
                    df["trade_date"] = df["trade_date"].astype(str)
                    logger.debug("已将trade_date字段转换为字符串格式")
                
                if self.verbose:
                    logger.debug(f"数据示例：\n{df.head(3)}")
                
            elapsed = time.time() - start_time
            
            if df is None or df.empty:
                logger.error("数据为空")
                
                # 释放WAN端口（如果使用了）
                if use_wan:
                    wan_idx, port = wan_info
                    self.port_allocator.release_port(wan_idx, port)
                    
                return None
            
            # 释放WAN端口（如果使用了）
            if use_wan:
                wan_idx, port = wan_info
                self.port_allocator.release_port(wan_idx, port)
            
            logger.success(f"成功获取 {len(df)} 条日线基本数据，耗时 {elapsed:.2f}s")
            return df
            
        except Exception as e:
            logger.error(f"获取日线基本数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            
            # 如果有WAN信息，确保释放端口
            try:
                if 'wan_info' in locals() and wan_info:
                    wan_idx, port = wan_info
                    self.port_allocator.release_port(wan_idx, port)
                    logger.debug(f"已释放WAN {wan_idx} 的端口 {port}")
            except Exception as release_error:
                logger.warning(f"释放WAN端口时出错: {str(release_error)}")
                
            return None
    
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
            # 确保MongoDB连接
            if not mongodb_handler.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if not mongodb_handler.connect():
                    logger.error("连接MongoDB失败")
                    raise Exception("连接MongoDB失败")
            
            # 构建查询条件 - trade_cal集合中只有trade_date和exchange字段
            query = {
                "trade_date": {"$gte": start_date, "$lte": end_date},
                "exchange": self.exchange
            }
            
            # 查询trade_cal集合 - 根据错误信息修正参数
            # MongoDBHandler.find_documents() takes from 2 to 3 positional arguments but 4 were given
            result = mongodb_handler.find_documents("trade_cal", query)  # 只使用必要的两个参数
            
            # 提取日期列表并手动排序
            trade_dates = [doc.get("trade_date") for doc in result if "trade_date" in doc]
            trade_dates.sort()  # 手动对日期进行排序
            
            if trade_dates:
                logger.info(f"从trade_cal集合获取到 {len(trade_dates)} 个交易日")
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
        获取指定交易日的数据
        
        Args:
            trade_date: 交易日，格式为YYYYMMDD
            
        Returns:
            指定交易日的数据，如果失败则返回None
        """
        logger.info(f"正在获取交易日 {trade_date} 的数据...")
        return self.fetch_data(trade_date=trade_date)
    
    def _process_date_with_wan(self, trade_date: str, wan_idx: int) -> bool:
        """
        使用指定的WAN口处理单个交易日数据
        
        Args:
            trade_date: 交易日
            wan_idx: 要使用的WAN口索引
            
        Returns:
            是否成功
        """
        # 检查WAN口索引是否有效
        if wan_idx not in self.wan_locks:
            logger.warning(f"WAN口索引 {wan_idx} 无效或不可用，尝试使用默认处理")
            # 尝试使用默认处理方式
            try:
                df = self.fetch_daily_data(trade_date)
                if df is None or df.empty:
                    return False
                
                processed_df = self.process_data(df)
                if processed_df is None or processed_df.empty:
                    return False
                
                success = self.save_to_mongodb(processed_df)
                if not success:
                    self.result_queue.put((trade_date, processed_df))
                
                return success
            except Exception as e:
                logger.error(f"默认处理交易日 {trade_date} 时发生异常: {str(e)}")
                return False
        
        logger.info(f"线程使用WAN口 {wan_idx} 处理交易日 {trade_date}")
        
        # 获取WAN口锁
        if not self.wan_locks[wan_idx].acquire(timeout=5):
            logger.warning(f"无法获取WAN口 {wan_idx} 的锁，跳过处理交易日 {trade_date}")
            return False
        
        try:
            return self._process_date_with_wan_no_lock(trade_date, wan_idx)
        finally:
            # 释放WAN口锁
            self.wan_locks[wan_idx].release()
            logger.debug(f"释放WAN口 {wan_idx} 的锁")
    
    def _process_date_with_wan_no_lock(self, trade_date: str, wan_idx: int) -> bool:
        """
        使用指定的WAN口处理单个交易日数据，不获取锁（由调用者控制锁）
        
        Args:
            trade_date: 交易日
            wan_idx: 要使用的WAN口索引
            
        Returns:
            是否成功
        """
        logger.info(f"WAN口 {wan_idx} 处理交易日 {trade_date}")
        
        success = False
        df = None
        
        try:
            # 重写_get_wan_socket方法的行为，强制使用指定的WAN口
            def custom_get_wan_socket(self_func, forced_wan_idx=None):
                if forced_wan_idx is not None:
                    port = self.port_allocator.allocate_port(forced_wan_idx)
                    if port is not None:
                        logger.debug(f"成功为WAN口 {forced_wan_idx} 分配端口 {port}")
                        return (forced_wan_idx, port)
                    else:
                        logger.warning(f"无法为WAN口 {forced_wan_idx} 分配端口")
                return None
            
            # 临时替换_get_wan_socket方法
            original_get_wan_socket = self._get_wan_socket
            self._get_wan_socket = lambda: custom_get_wan_socket(self, wan_idx)
            
            try:
                # 获取单日数据
                df = self.fetch_daily_data(trade_date)
                if df is None or df.empty:
                    logger.warning(f"交易日 {trade_date} 的数据为空或获取失败")
                    return False
                
                # 处理数据
                processed_df = self.process_data(df)
                if processed_df is None or processed_df.empty:
                    logger.warning(f"交易日 {trade_date} 的处理后数据为空")
                    return False
                
                # 保存单日数据到MongoDB
                if not mongodb_handler.is_connected():
                    logger.warning("MongoDB未连接，尝试连接...")
                    if not mongodb_handler.connect():
                        logger.error("连接MongoDB失败")
                        # 添加到结果队列，供后续处理
                        self.result_queue.put((trade_date, processed_df))
                        return False
                        
                # 保存到MongoDB
                success = self.save_to_mongodb(processed_df)
                if success:
                    logger.success(f"交易日 {trade_date} 的数据已保存到MongoDB")
                else:
                    logger.error(f"保存交易日 {trade_date} 的数据到MongoDB失败")
                    # 添加到结果队列，供后续处理
                    self.result_queue.put((trade_date, processed_df))
            finally:
                # 恢复原始方法
                self._get_wan_socket = original_get_wan_socket
        
        except Exception as e:
            logger.error(f"处理交易日 {trade_date} 时发生异常: {str(e)}")
            # 如果有数据但处理失败，添加到结果队列
            if df is not None and not df.empty:
                processed_df = self.process_data(df)
                if processed_df is not None and not processed_df.empty:
                    self.result_queue.put((trade_date, processed_df))
        
        return success
    
    def _process_date_parallel(self, trade_dates: List[str]) -> bool:
        """
        并行处理多个交易日数据
        
        Args:
            trade_dates: 交易日列表
            
        Returns:
            是否全部成功
        """
        if self.available_wan_count == 0:
            logger.warning("未找到可用的WAN口，降级为串行处理模式")
            # 降级为串行处理
            all_success = True
            for trade_date in trade_dates:
                # 检查是否收到停止信号
                if STOP_PROCESSING:
                    logger.warning("收到停止信号，中断处理")
                    return False
                
                try:
                    df = self.fetch_daily_data(trade_date)
                    if df is not None and not df.empty:
                        processed_df = self.process_data(df)
                        if processed_df is not None and not processed_df.empty:
                            success = self.save_to_mongodb(processed_df)
                            if not success:
                                logger.error(f"保存交易日 {trade_date} 的数据到MongoDB失败")
                                self.result_queue.put((trade_date, processed_df))
                                all_success = False
                except Exception as e:
                    logger.error(f"处理交易日 {trade_date} 时发生异常: {str(e)}")
                    all_success = False
            return all_success
        
        threads_count = self.available_wan_count
        logger.info(f"并行处理 {len(trade_dates)} 个交易日的数据，线程数: {threads_count}，可用WAN口: {self.available_wan_indices}")
        
        # 确保WAN索引列表有效
        if not self.available_wan_indices:
            logger.error("可用WAN口列表为空，无法进行并行处理")
            return False
        
        # 均匀分配交易日到可用WAN口
        date_groups = {wan_idx: [] for wan_idx in self.available_wan_indices}
        for i, date in enumerate(trade_dates):
            wan_idx = self.available_wan_indices[i % len(self.available_wan_indices)]
            date_groups[wan_idx].append(date)
        
        # 记录分配情况
        for wan_idx, dates in date_groups.items():
            logger.info(f"WAN口 {wan_idx} 分配到 {len(dates)} 个交易日: {dates}")
        
        all_success = True
        processed_dates_count = 0  # 添加计数器跟踪处理的日期数
        
        # 创建每个WAN口对应的处理函数
        def process_wan_dates(wan_idx, dates):
            """处理单个WAN口对应的所有日期"""
            if not dates:  # 没有日期需要处理
                logger.info(f"WAN口 {wan_idx} 没有分配到交易日，跳过")
                return True, 0
                
            # 获取WAN口锁，确保同一时间只有一个线程使用此WAN口
            if not self.wan_locks[wan_idx].acquire(timeout=10):  # 增加超时时间
                logger.warning(f"无法获取WAN口 {wan_idx} 的锁，跳过处理")
                return False, 0
            
            logger.info(f"线程成功获取WAN口 {wan_idx} 的锁")
            
            try:
                logger.info(f"线程开始处理WAN口 {wan_idx} 的 {len(dates)} 个交易日")
                
                wan_success = True
                success_count = 0
                
                # 逐个处理该WAN口的所有交易日
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
                            logger.warning(f"WAN口 {wan_idx} 处理交易日 {date} 失败")
                            wan_success = False
                    except Exception as e:
                        logger.error(f"WAN口 {wan_idx} 处理交易日 {date} 时发生异常: {str(e)}")
                        wan_success = False
                
                # 如果至少成功处理了一个日期，视为部分成功
                if success_count > 0:
                    logger.info(f"WAN口 {wan_idx} 成功处理了 {success_count}/{len(dates)} 个交易日")
                    # 即使有些失败，只要有成功的，我们就不认为整体失败
                    wan_success = True
                
                logger.info(f"线程完成WAN口 {wan_idx} 的所有交易日处理")
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
                logger.info(f"提交WAN口 {wan_idx} 处理 {len(dates)} 个交易日的任务")
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
        if processed_dates_count == 0 and len(trade_dates) > 0:
            logger.error(f"所有 {len(trade_dates)} 个交易日都处理失败")
            return False
        
        # 如果至少处理了一些日期，就返回成功
        logger.success(f"成功处理了 {processed_dates_count}/{len(trade_dates)} 个交易日")
        return True
    
    def fetch_stock_data(self, ts_code: str) -> Optional[pd.DataFrame]:
        """
        获取指定股票代码的数据
        
        Args:
            ts_code: 股票代码，格式如 000001.SZ
            
        Returns:
            指定股票的数据，如果失败则返回None
        """
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
            if not mongodb_handler.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if not mongodb_handler.connect():
                    logger.error("连接MongoDB失败")
                    return []
            
            # 获取当前日期作为筛选条件
            current_date = datetime.now().strftime("%Y%m%d")
            
            # 构建查询条件 - 筛选上市日期早于当前日期的股票
            query = {
                "list_date": {"$lt": current_date}  # 上市日期早于当前日期
            }
            
            # 从stock_basic集合中查询
            result = mongodb_handler.find_documents("stock_basic", query)
            
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
            elif self.verbose:
                # 打印前10个股票代码示例
                sample_codes = stock_codes[:10]
                logger.debug(f"股票代码示例: {sample_codes}")
            
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
            if self.verbose:
                logger.debug(f"WAN口 {wan_idx} 的股票: {stocks}")
        
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
                
                # 为每个股票创建自定义的_get_wan_socket方法，确保使用正确的WAN口
                def custom_get_wan_socket(self_func, forced_wan_idx=None):
                    if forced_wan_idx is not None:
                        port = self.port_allocator.allocate_port(forced_wan_idx)
                        if port is not None:
                            logger.debug(f"成功为WAN口 {forced_wan_idx} 分配端口 {port}")
                            return (forced_wan_idx, port)
                        else:
                            logger.warning(f"无法为WAN口 {forced_wan_idx} 分配端口")
                    return None
                
                # 临时替换_get_wan_socket方法
                original_get_wan_socket = self._get_wan_socket
                self._get_wan_socket = lambda: custom_get_wan_socket(self, wan_idx)
                
                try:
                    # 逐个处理该WAN口的所有股票
                    for ts_code in stocks:
                        # 检查是否收到停止信号
                        if STOP_PROCESSING:
                            logger.warning(f"WAN口 {wan_idx} 收到停止信号，中断处理")
                            return wan_success, success_count
                            
                        try:
                            # 获取单个股票数据
                            df = self.fetch_stock_data(ts_code)
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
                finally:
                    # 恢复原始方法
                    self._get_wan_socket = original_get_wan_socket
                
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
                logger.info("使用完整模式，按股票代码获取所有历史数据")
                
                # 第二步：获取所有股票代码
                logger.info("第二步：从stock_basic集合获取股票代码列表")
                stock_codes = self.get_stock_codes()
                
                if not stock_codes:
                    logger.error("未能获取到任何股票代码，抓取失败")
                    return False  # 没有找到股票代码应该返回失败
                
                # 第三步：获取数据（串行或并行）
                logger.info(f"第三步：处理 {len(stock_codes)} 个股票的数据")
                
                if self.serial_mode:
                    # 串行模式
                    logger.info(f"使用串行模式处理 {len(stock_codes)} 个股票的数据")
                    all_success = True
                    
                    for ts_code in stock_codes:
                        # 检查是否收到停止信号
                        if STOP_PROCESSING:
                            logger.warning("收到停止信号，中断处理")
                            return False
                            
                        logger.info(f"正在处理股票: {ts_code}")
                        
                        try:
                            # 获取单个股票数据
                            df = self.fetch_stock_data(ts_code)
                            if df is None or df.empty:
                                logger.warning(f"股票 {ts_code} 的数据为空或获取失败")
                                continue
                            
                            # 处理数据
                            processed_df = self.process_data(df)
                            if processed_df is None or processed_df.empty:
                                logger.warning(f"股票 {ts_code} 的处理后数据为空")
                                continue
                            
                            # 保存数据到MongoDB
                            success = self.save_to_mongodb(processed_df)
                            if success:
                                logger.success(f"股票 {ts_code} 的数据已保存到MongoDB")
                            else:
                                logger.error(f"保存股票 {ts_code} 的数据到MongoDB失败")
                                all_success = False
                        except Exception as e:
                            logger.error(f"处理股票 {ts_code} 的数据时发生异常: {str(e)}")
                            all_success = False
                    
                    return all_success
                else:
                    # 并行模式
                    logger.info(f"使用并行模式处理 {len(stock_codes)} 个股票的数据")
                    return self._process_stock_parallel(stock_codes)
            else:
                # 日期模式：按交易日获取
                logger.info("使用日期模式，按交易日获取数据")
                
                # 第二步：获取日期范围内的所有交易日
                logger.info(f"第二步：获取日期范围 {self.start_date} - {self.end_date} 内的交易日...")
                trade_dates = self.get_trade_dates(self.start_date, self.end_date)
                
                if not trade_dates:
                    logger.warning("未找到交易日，没有数据需要处理")
                    return True  # 没有数据也视为成功
                
                # 第三步：获取数据（串行或并行）
                if self.serial_mode:
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
                            
                            # 处理数据
                            processed_df = self.process_data(df)
                            if processed_df is None or processed_df.empty:
                                logger.warning(f"交易日 {trade_date} 的处理后数据为空")
                                continue
                            
                            # 保存单日数据到MongoDB
                            success = self.save_to_mongodb(processed_df)
                            if success:
                                logger.success(f"交易日 {trade_date} 的数据已保存到MongoDB")
                            else:
                                logger.error(f"保存交易日 {trade_date} 的数据到MongoDB失败")
                                all_success = False
                        except Exception as e:
                            logger.error(f"处理交易日 {trade_date} 的数据时发生异常: {str(e)}")
                            all_success = False
                    
                    return all_success
                else:
                    # 并行模式
                    logger.info(f"第三步：并行处理 {len(trade_dates)} 个交易日的数据")
                    return self._process_date_parallel(trade_dates)
            
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
    
    parser = argparse.ArgumentParser(description='获取日线基本数据并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--exchange', default='SSE', help='交易所代码：SSE-上交所, SZSE-深交所')
    parser.add_argument('--start-date', help='开始日期，格式：YYYYMMDD')
    parser.add_argument('--end-date', help='结束日期，格式：YYYYMMDD')
    parser.add_argument('--db-name', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='daily_basic', help='MongoDB集合名称')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    parser.add_argument('--shared-config', type=str, default=None, help='共享配置文件路径')
    parser.add_argument('--skip-validation', action='store_true', help='跳过配置验证')
    parser.add_argument('--serial', action='store_true', help='使用串行模式处理数据（默认为并行模式）')
    parser.add_argument('--recent', action='store_true', help='使用并行模式处理数据（默认）')
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
        
        # 创建获取器并运行
        fetcher = DailyBasicFetcher(
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
            serial_mode=args.serial,  # 使用串行模式
            max_workers=args.max_workers,  # 并行模式下的最大工作线程数
            full_mode=args.full  # 是否使用完整模式（按股票代码抓取）
        )
        
        try:
            success = fetcher.run()
            
            if success:
                logger.success("日线基本数据获取和保存成功")
                return 0
            else:
                logger.error("日线基本数据获取或保存失败")
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
    import sys
    
    # 处理简单的help参数，避免复杂初始化
    if len(sys.argv) > 1 and sys.argv[1] in ['--help', '-h']:
        print(__doc__)
        sys.exit(0)
    
    # 添加测试命令，用于诊断
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        print("开始测试WAN端口分配")
        try:
            from core.wan_manager import port_allocator
            print("成功导入port_allocator")
            
            indices = port_allocator.get_available_wan_indices()
            print(f"可用WAN索引: {indices}")
            
            if indices:
                port = port_allocator.allocate_port(indices[0])
                print(f"分配端口: {port}")
                
                if port:
                    released = port_allocator.release_port(indices[0], port)
                    print(f"端口释放: {released}")
            
            print("测试完成")
        except Exception as e:
            print(f"测试失败: {str(e)}")
            import traceback
            print(traceback.format_exc())
        
        sys.exit(0)
    
    # 正常执行
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.warning("程序被用户中断")
        os._exit(130)  # 使用os._exit强制退出 