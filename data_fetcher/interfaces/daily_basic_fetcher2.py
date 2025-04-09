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
"""
import sys
import time
import json
import os
import pandas as pd
import threading
import queue
import concurrent.futures
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple
from pathlib import Path
from loguru import logger

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root))

# 导入平台核心模块
from core.tushare_fetcher import TushareFetcher
from core import mongodb_handler

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
        max_workers: int = 3  # 并行模式下的最大工作线程数
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
        
        # 获取可用的WAN口数量
        from core.wan_manager import port_allocator
        self.port_allocator = port_allocator
        self.available_wan_count = 0
        
        # 从配置中获取可用WAN口
        wan_config = self.config.get("wan", {})
        wan_list = wan_config.get("wan_list", [])
        self.available_wan_indices = []
        
        for wan_info in wan_list:
            wan_idx = wan_info.get("wan_idx")
            if wan_idx and wan_info.get("enabled", True):
                self.available_wan_indices.append(wan_idx)
        
        self.available_wan_count = len(self.available_wan_indices)
        
        if self.available_wan_count == 0:
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
        
        # 用于存储并行处理结果的队列
        self.result_queue = queue.Queue()
        
        # 日志输出
        logger.info(f"交易所: {self.exchange}, 日期范围: {self.start_date} - {self.end_date}")
        logger.info(f"处理模式: {'串行' if self.serial_mode else '并行'}, 可用WAN口数量: {self.available_wan_count}, 并行线程数: {self.max_workers}")
    
    def fetch_data(self, **kwargs) -> Optional[pd.DataFrame]:
        """
        从Tushare获取日线基本数据
        
        Args:
            **kwargs: 可以传入覆盖默认参数的值，比如exchange, start_date, end_date, trade_date
            
        Returns:
            日线基本数据DataFrame，如果失败则返回None
        """
        try:
            # 准备API参数
            api_name = self.interface_config.get("api_name", "daily_basic_ts")
            
            # 设置API参数：交易所和日期范围，优先使用传入的参数
            exchange = kwargs.get('exchange', self.exchange)
            
            # 如果传入了单个交易日，使用单日模式
            if 'trade_date' in kwargs and kwargs['trade_date']:
                trade_date = kwargs['trade_date']
                start_date = trade_date
                end_date = trade_date
                logger.info(f"使用单日模式获取数据，交易日：{trade_date}")
            else:
                # 否则使用日期范围模式
                start_date = kwargs.get('start_date', self.start_date)
                end_date = kwargs.get('end_date', self.end_date)
            
            params = {
                "exchange": exchange,
                "start_date": start_date,
                "end_date": end_date
            }
            
            # 使用接口配置中的available_fields作为请求字段
            fields = self.available_fields
            if not fields:
                logger.warning("接口配置中未定义available_fields，将获取所有字段")
            
            # 创建并使用WAN接口的socket，实现多WAN请求支持
            wan_info = self._get_wan_socket()
            use_wan = wan_info is not None
            
            # 调用Tushare API
            logger.info(f"正在从湘财Tushare获取日线基本数据...")
            logger.info(f"数据范围：{start_date} 至 {end_date}，交易所：{exchange}")
            
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
        可以在这里实现对数据的任何处理，如果不需要特殊处理，可以直接返回原始数据
        
        Args:
            df: 原始日线基本数据
            
        Returns:
            处理后的数据
        """
        # 本例中不需要对日线基本数据做特殊处理，返回原始数据即可
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
        logger.info(f"线程使用WAN口 {wan_idx} 处理交易日 {trade_date}")
        
        # 获取WAN口锁
        if not self.wan_locks[wan_idx].acquire(timeout=5):
            logger.warning(f"无法获取WAN口 {wan_idx} 的锁，跳过处理交易日 {trade_date}")
            return False
        
        success = False
        df = None
        
        try:
            # 重写_get_wan_socket方法的行为，强制使用指定的WAN口
            def custom_get_wan_socket(self_func, forced_wan_idx=None):
                if forced_wan_idx is not None:
                    port = self.port_allocator.allocate_port(forced_wan_idx)
                    if port is not None:
                        return (forced_wan_idx, port)
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
        finally:
            # 释放WAN口锁
            self.wan_locks[wan_idx].release()
            logger.debug(f"释放WAN口 {wan_idx} 的锁")
        
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
            logger.warning("未找到可用的WAN口，无法并行处理数据")
            return False
            
        logger.info(f"并行处理 {len(trade_dates)} 个交易日的数据，最大线程数: {self.max_workers}，可用WAN口: {self.available_wan_indices}")
        
        # 将交易日分配到不同的WAN口
        date_groups = {}
        for i, date in enumerate(trade_dates):
            # 使用取模运算均匀分配交易日到可用WAN口
            index_position = i % len(self.available_wan_indices)
            wan_idx = self.available_wan_indices[index_position]
            
            if wan_idx not in date_groups:
                date_groups[wan_idx] = []
            date_groups[wan_idx].append(date)
        
        all_success = True
        
        # 创建线程池，线程数不超过可用WAN口数量
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交任务
            future_to_wan = {}
            for wan_idx, dates in date_groups.items():
                logger.info(f"提交WAN口 {wan_idx} 处理 {len(dates)} 个交易日的任务")
                for date in dates:
                    future = executor.submit(self._process_date_with_wan, date, wan_idx)
                    future_to_wan[future] = (wan_idx, date)
            
            # 处理结果
            for future in concurrent.futures.as_completed(future_to_wan):
                wan_idx, date = future_to_wan[future]
                try:
                    success = future.result()
                    if not success:
                        logger.warning(f"WAN口 {wan_idx} 处理交易日 {date} 失败")
                        all_success = False
                except Exception as e:
                    logger.error(f"WAN口 {wan_idx} 处理交易日 {date} 时发生异常: {str(e)}")
                    all_success = False
        
        return all_success
    
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
            
            # 第二步：获取日期范围内的所有交易日
            logger.info(f"第二步：获取日期范围 {self.start_date} - {self.end_date} 内的交易日...")
            trade_dates = self.get_trade_dates(self.start_date, self.end_date)
            
            # 第三步：获取数据（串行或并行）
            if self.serial_mode:
                # 串行模式
                logger.info(f"第三步：串行处理 {len(trade_dates)} 个交易日的数据")
                all_success = True
                all_dfs = []
                
                for trade_date in trade_dates:
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
                        try:
                            # 确保MongoDB连接
                            if not mongodb_handler.is_connected():
                                logger.warning("MongoDB未连接，尝试连接...")
                                if not mongodb_handler.connect():
                                    logger.error("连接MongoDB失败")
                                    all_dfs.append(processed_df)
                                    continue
                                    
                            # 尝试使用父类的save_to_mongodb方法
                            success = self.save_to_mongodb(processed_df)
                            if success:
                                logger.success(f"交易日 {trade_date} 的数据已保存到MongoDB")
                            else:
                                logger.error(f"保存交易日 {trade_date} 的数据到MongoDB失败")
                                all_dfs.append(processed_df)
                        except Exception as e:
                            logger.error(f"保存交易日 {trade_date} 的数据时发生异常: {str(e)}")
                            all_dfs.append(processed_df)
                            
                    except Exception as e:
                        logger.error(f"处理交易日 {trade_date} 的数据时发生异常: {str(e)}")
                        all_success = False
            else:
                # 并行模式
                logger.info(f"第三步：并行处理 {len(trade_dates)} 个交易日的数据")
                all_success = self._process_date_parallel(trade_dates)
                
                # 从结果队列中获取未能保存的数据
                all_dfs = []
                while not self.result_queue.empty():
                    try:
                        trade_date, df = self.result_queue.get(block=False)
                        logger.debug(f"从结果队列获取交易日 {trade_date} 的数据")
                        all_dfs.append(df)
                    except queue.Empty:
                        break
            
            # 第四步：如果有未保存的数据，尝试合并后一次性保存
            if all_dfs:
                logger.info("第四步：处理未能成功保存的数据")
                try:
                    combined_df = pd.concat(all_dfs, ignore_index=True)
                    logger.info(f"合并了 {len(all_dfs)} 个交易日的数据，共 {len(combined_df)} 条记录")
                    
                    # 尝试保存合并的数据
                    # 确保MongoDB连接
                    if not mongodb_handler.is_connected():
                        logger.warning("MongoDB未连接，尝试连接...")
                        mongodb_handler.connect()
                        
                    success = self.save_to_mongodb(combined_df)
                    if success:
                        logger.success("合并的数据已保存到MongoDB")
                    else:
                        logger.error("保存合并的数据到MongoDB失败")
                        all_success = False
                except Exception as e:
                    logger.error(f"合并或保存数据时发生异常: {str(e)}")
                    all_success = False
            
            logger.info("数据获取和保存流程完成")
            return all_success
            
        except Exception as e:
            logger.error(f"运行过程中发生异常: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

def main():
    """主函数"""
    import argparse
    
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
            max_workers=args.max_workers  # 并行模式下的最大工作线程数
        )
        
        success = fetcher.run()
        
        if success:
            logger.success("日线基本数据获取和保存成功")
            return 0
        else:
            logger.error("日线基本数据获取或保存失败")
            return 1
        
    except Exception as e:
        logger.error(f"运行过程中发生异常: {str(e)}")
        import traceback
        logger.error(f"详细错误信息: {traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 