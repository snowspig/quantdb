#!/usr/bin/env python
"""
日线基本指标数据获取器 - 获取每日指标数据并保存到MongoDB

该脚本用于从湘财Tushare获取每日指标数据，并保存到MongoDB数据库中
该版本通过分时间段获取和多WAN接口并行处理，解决大量数据获取问题

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=10268

使用方法：
    python daily_basic_fetcher.py              # 默认使用recent模式获取最近一周的数据更新
    python daily_basic_fetcher.py --full        # 获取完整历史数据而非默认的最近一周数据
    python daily_basic_fetcher.py --verbose     # 使用湘财真实API数据，详细日志模式
    python daily_basic_fetcher.py --mock        # 使用模拟数据模式（API不可用时）
    python daily_basic_fetcher.py --start-date 20100101 --end-date 20201231  # 指定日期范围获取数据
    python daily_basic_fetcher.py --recent      # 显式指定recent模式（最近一周数据更新，默认模式）
"""
import os
import sys
import json
import yaml
import time
import asyncio
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Any, Tuple, Union
from pathlib import Path
from loguru import logger
from concurrent.futures import ThreadPoolExecutor
import pymongo
import requests.adapters
import socket
import requests
import random
import queue
import threading

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

# 导入项目模块
from data_fetcher.tushare_client import TushareClient
from storage.mongodb_client import MongoDBClient
from wan_manager.port_allocator import PortAllocator

class DailyBasicFetcher:
    """
    每日指标数据获取器
    
    该类用于从Tushare获取每日指标数据并保存到MongoDB数据库
    优化点：
    1. 支持按时间段分批获取数据，避免一次获取超过10000条数据限制
    2. 多WAN接口并行获取，提高数据获取效率
    3. 增加数据获取重试机制，提高稳定性 (出现失败时进行5次延时重试)
    4. 支持recent模式、full模式以及指定日期范围模式
    """

    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "daily_basic_ts.json",
        target_market_codes: Set[str] = {"00", "30", "60", "68"},  # 默认只保存00 30 60 68四个板块的股票数据
        db_name: str = None,
        collection_name: str = "daily_basic",
        verbose: bool = False,
        max_workers: int = 3,  # 并行工作线程数
        retry_count: int = 5,  # 数据获取重试次数 (5次)
        retry_delay: int = 5,  # 重试延迟时间(秒)
        batch_size: int = 10000  # 每批次获取数据的最大数量，防止超过API限制
    ):
        """
        初始化每日指标数据获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口配置文件名
            target_market_codes: 目标市场代码集合，只保存这些市场的股票数据
            db_name: MongoDB数据库名称，为None则使用config中的设置
            collection_name: MongoDB集合名称，为None则使用接口名称
            verbose: 是否输出详细日志
            max_workers: 并行工作线程数
            retry_count: 数据获取重试次数
            retry_delay: 重试延迟时间(秒)
            batch_size: 每批次获取数据的最大数量，防止超过API限制
        """
        # 配置loguru日志
        log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <level>{message}</level>"
        logger.remove()  # 移除默认的处理器
        
        if verbose:
            # 详细模式 - 输出所有级别的日志到控制台
            logger.add(sys.stderr, format=log_format, level="DEBUG")
            # 输出INFO以上级别的日志到文件
            logger.add("logs/daily_basic_fetcher_{time}.log", format=log_format, level="INFO", rotation="500 MB")
        else:
            # 标准模式 - 仅输出INFO以上级别的日志到控制台
            logger.add(sys.stderr, format=log_format, level="INFO")
            # 仍然保存DEBUG以上级别的日志到文件
            logger.add("logs/daily_basic_fetcher_{time}.log", format=log_format, level="DEBUG", rotation="500 MB")
            
        logger.info("初始化日线基本指标数据获取器")
        
        # 保存配置参数
        self.config_path = config_path
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.target_market_codes = target_market_codes
        self.max_workers = max_workers
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.batch_size = batch_size
        
        # 加载全局配置
        self.config = self._load_config(config_path)
        if not self.config:
            logger.error(f"加载配置文件失败: {config_path}")
            raise ValueError(f"无法加载配置文件: {config_path}")
            
        # 提取必要的配置项
        mongo_config = self.config.get('mongodb', {})
        tushare_config = self.config.get('tushare', {})
        wan_config = self.config.get('wan', {})
        
        # 设置数据库名称 - 优先使用传入参数，否则使用配置文件中的设置
        self.db_name = db_name or mongo_config.get('db_name', 'tushare_data')
        
        # 设置集合名称 - 优先使用传入参数，否则使用接口配置中的设置或接口名称
        self.collection_name = collection_name
        
        # 初始化MongoDB客户端
        self.mongo_client = MongoDBClient(
            host=mongo_config.get('host'),
            port=mongo_config.get('port'),
            username=mongo_config.get('username'),
            password=mongo_config.get('password'),
            db_name=self.db_name,
            auth_source=mongo_config.get('auth_source'),
            auth_mechanism=mongo_config.get('auth_mechanism'),
            read_preference=mongo_config.get('read_preference', 'primaryPreferred')
        )
        
        # 初始化Tushare客户端
        self.ts_client = TushareClient(
            token=tushare_config.get('token'),
            api_url=tushare_config.get('api_url')
        )
        
        # 加载接口配置文件
        interface_path = os.path.join(self.interface_dir, self.interface_name)
        self.interface_config = self._load_interface_config(interface_path)
        if not self.interface_config:
            logger.error(f"加载接口配置文件失败: {interface_path}")
            raise ValueError(f"无法加载接口配置文件: {interface_path}")
            
        # 确保获取API名称
        self.api_name = self.interface_config.get('api_name', 'daily_basic_ts')
        logger.info(f"使用API: {self.api_name}")
        
        # 初始化端口分配器(如果启用多WAN接口)
        self.port_allocator = None
        if wan_config.get('enabled', False):
            try:
                from wan_manager.port_allocator import PortAllocator
                self.port_allocator = PortAllocator(wan_config)
                logger.info(f"已启用多WAN接口模式，可用WAN数量: {self.port_allocator.get_available_wan_count()}")
            except ImportError:
                logger.warning("无法导入PortAllocator模块，将使用单一网络接口")
            except Exception as e:
                logger.warning(f"初始化PortAllocator失败: {str(e)}，将使用单一网络接口")
        
        # 统计信息
        self.last_operation_stats = {}
        
        # 确保MongoDB连接
        if not self.mongo_client.connect():
            logger.error("连接MongoDB失败")
            raise ConnectionError("无法连接MongoDB数据库")
            
        # 确保索引
        self._ensure_index()
        
        logger.success("初始化日线基本指标数据获取器完成")

    def _ensure_index(self):
        """
        确保MongoDB中的集合有适当的索引
        """
        try:
            # 确保MongoDB连接
            if not self.mongo_client.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if not self.mongo_client.connect():
                    logger.error("连接MongoDB失败，无法创建索引")
                    return False
            
            # 获取数据库对象
            db = self.mongo_client.get_db(self.db_name)
            
            # 检查集合是否存在
            collection_exists = self.collection_name in db.list_collection_names()
            
            # 获取集合对象
            collection = db[self.collection_name]
            
            # 获取当前集合的所有索引
            existing_indexes = collection.index_information()
            logger.debug(f"集合 {self.collection_name} 现有索引: {existing_indexes}")
            
            # 从接口配置中获取索引字段，如果没有指定，则使用默认的 ["ts_code", "trade_date"]
            index_fields = self.interface_config.get("index_fields", ["ts_code", "trade_date"])
            
            # 检查是否需要创建复合索引
            if len(index_fields) > 0:
                # 构建索引名称
                index_name = "_".join(index_fields) + "_idx"
                
                # 检查索引是否已存在
                if index_name not in existing_indexes and not any(index_fields == [(key, 1) for key, _ in index_info["key"]] for index_info in existing_indexes.values()):
                    # 创建索引
                    logger.info(f"正在为集合 {self.collection_name} 创建索引: {index_fields}")
                    # 构建索引参数 - 每个字段都是升序
                    index_spec = [(field, 1) for field in index_fields]
                    collection.create_index(index_spec, name=index_name, background=True)
                    logger.success(f"成功为集合 {self.collection_name} 创建索引: {index_name}")
                else:
                    logger.info(f"集合 {self.collection_name} 已存在索引 {index_fields}，无需创建")
            
            return True
            
        except Exception as e:
            logger.error(f"确保索引时出错: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False
        
    def _load_config(self, config_path: str) -> Dict:
        """
        加载YAML格式的配置文件
        
        Args:
            config_path: 配置文件路径
        
        Returns:
            配置字典
        """
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                logger.debug(f"成功加载配置文件: {config_path}")
                return config
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            return {}
    
    def _load_interface_config(self, interface_path: str) -> Dict:
        """
        加载JSON格式的接口配置文件
        
        Args:
            interface_path: 接口配置文件路径
        
        Returns:
            接口配置字典
        """
        try:
            with open(interface_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                logger.debug(f"成功加载接口配置: {interface_path}")
                return config
        except Exception as e:
            logger.error(f"加载接口配置失败: {str(e)}")
            return {}

    def _generate_date_ranges(self, start_date: str, end_date: str, interval_days: int = 365) -> List[Tuple[str, str]]:
        """
        生成日期范围列表，将长时间段按interval_days天分割成多个短时间段
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            interval_days: 每个时间段的天数
            
        Returns:
            日期范围列表，每个元素为(开始日期, 结束日期)的元组
        """
        try:
            start_date_obj = datetime.strptime(start_date, '%Y%m%d')
            end_date_obj = datetime.strptime(end_date, '%Y%m%d')
            
            if start_date_obj > end_date_obj:
                logger.error(f"开始日期 {start_date} 晚于结束日期 {end_date}，将交换这两个日期")
                start_date_obj, end_date_obj = end_date_obj, start_date_obj
                
            date_ranges = []
            current_start = start_date_obj
            
            while current_start <= end_date_obj:
                current_end = current_start + timedelta(days=interval_days)
                if current_end > end_date_obj:
                    current_end = end_date_obj
                    
                date_ranges.append(
                    (current_start.strftime('%Y%m%d'), current_end.strftime('%Y%m%d'))
                )
                
                current_start = current_end + timedelta(days=1)
                if current_start > end_date_obj:
                    break
                    
            return date_ranges
        except Exception as e:
            logger.error(f"生成日期范围时出错: {str(e)}")
            return []
            
    def _get_all_stock_codes(self) -> List[str]:
        """
        获取所有股票代码
        
        Returns:
            股票代码列表
        """
        try:
            # 使用stock_basic接口获取所有股票列表
            df = self.ts_client.get_data(
                api_name="stock_basic",
                params={},
                fields=["ts_code"]
            )
            
            if df.empty:
                logger.error("获取股票列表失败，API返回数据为空")
                return []
                
            return df['ts_code'].tolist()
        except Exception as e:
            logger.error(f"获取股票列表失败: {str(e)}")
            return []
            
    def _split_stock_codes(self, stock_codes: List[str], batch_size: int = 100) -> List[List[str]]:
        """
        将股票代码列表分批处理
        
        Args:
            stock_codes: 股票代码列表
            batch_size: 每批处理的股票代码数量
            
        Returns:
            股票代码批次列表
        """
        return [stock_codes[i:i+batch_size] for i in range(0, len(stock_codes), batch_size)]

    def fetch_daily_basic_by_date(self, trade_date: str, ts_code: str = None) -> pd.DataFrame:
        """
        按日期获取每日指标数据
        
        Args:
            trade_date: 交易日期，格式为YYYYMMDD
            ts_code: 可选，股票代码
            
        Returns:
            DataFrame形式的日线数据
        """
        try:
            params = {"trade_date": trade_date}
            if ts_code:
                params["ts_code"] = ts_code
                
            logger.debug(f"获取日期 {trade_date} 的每日指标数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
            
            # 添加重试逻辑，出现失败时进行5次延时重试后再跳过该批次的抓取
            for retry in range(self.retry_count):
                df = self.ts_client.get_data(
                    api_name=self.api_name,
                    params=params,
                    fields=self.interface_config.get("available_fields", [])
                )
                
                if df is not None:  # 只有当返回None时才重试，空DataFrame是正常的情况
                    break
                    
                # 重试延迟，增加随机延迟时间
                retry_sleep = self.retry_delay + random.uniform(0, 2)
                logger.warning(f"获取数据失败，重试 ({retry+1}/{self.retry_count})，等待 {retry_sleep:.1f} 秒")
                time.sleep(retry_sleep)
            else:  # 所有重试都失败
                logger.error(f"所有重试都失败，跳过获取日期 {trade_date} 的数据")
                return pd.DataFrame()
            
            if df.empty:
                logger.warning(f"日期 {trade_date} 未获取到数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
                return pd.DataFrame()
            
            logger.info(f"成功获取日期 {trade_date} 的每日指标数据"+(f" 股票代码: {ts_code}" if ts_code else f"，共 {len(df)} 条记录"))
            return df
        except Exception as e:
            logger.error(f"获取日期 {trade_date} 的每日指标数据失败: {str(e)}")
            return pd.DataFrame()

    def fetch_daily_basic_by_code(self, ts_code: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        按股票代码获取每日指标数据
        
        Args:
            ts_code: 股票代码
            start_date: 可选，开始日期，格式为YYYYMMDD
            end_date: 可选，结束日期，格式为YYYYMMDD
            
        Returns:
            DataFrame形式的每日指标数据
        """
        try:
            params = {"ts_code": ts_code}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
                
            logger.debug(f"获取股票 {ts_code} 的每日指标数据"+(f" 日期范围: {start_date} 至 {end_date}" if start_date and end_date else ""))
            
            # 添加重试逻辑，出现失败时进行5次延时重试后再跳过该批次的抓取
            for retry in range(self.retry_count):
                df = self.ts_client.get_data(
                    api_name=self.api_name,
                    params=params,
                    fields=self.interface_config.get("available_fields", [])
                )
                
                if df is not None:  # 只有当返回None时才重试，空DataFrame是正常的情况
                    break
                    
                # 重试延迟，增加随机延迟时间
                retry_sleep = self.retry_delay + random.uniform(0, 2)
                logger.warning(f"获取数据失败，重试 ({retry+1}/{self.retry_count})，等待 {retry_sleep:.1f} 秒")
                time.sleep(retry_sleep)
            else:  # 所有重试都失败
                logger.error(f"所有重试都失败，跳过获取股票 {ts_code} 的数据")
                return pd.DataFrame()
            
            if df.empty:
                logger.warning(f"股票 {ts_code} 未获取到数据"+(f" 日期范围: {start_date} 至 {end_date}" if start_date and end_date else ""))
                return pd.DataFrame()
                
            record_count = len(df)
            
            # 检查返回的数据量是否接近限制，如果是则可能数据不完整
            if record_count >= self.batch_size * 0.9:  # 如果返回的数据量超过批次大小的90%
                logger.warning(f"股票 {ts_code} 返回数据量 {record_count} 接近API限制，数据可能不完整，建议缩小时间范围")
            
            logger.info(f"成功获取股票 {ts_code} 的每日指标数据，共 {record_count} 条记录")
            return df
        except Exception as e:
            logger.error(f"获取股票 {ts_code} 的每日指标数据失败: {str(e)}")
            return pd.DataFrame()

    def get_target_ts_codes_from_stock_basic(self) -> Set[str]:
        """
        从stock_basic集合中获取目标板块的股票代码
        
        Returns:
            目标板块股票代码集合
        """
        try:
            # 确保MongoDB连接
            if not self.mongo_client.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if not self.mongo_client.connect():
                    logger.error("连接MongoDB失败，无法获取股票代码")
                    return set()
                    
            # 获取数据库对象
            db = self.mongo_client.get_db(self.db_name)
            
            # 检查stock_basic集合是否存在
            if "stock_basic" not in db.list_collection_names():
                logger.error("stock_basic集合不存在，无法获取股票代码")
                return set()
                
            # 获取stock_basic集合对象
            collection = db["stock_basic"]
            
            # 查询目标市场代码的股票
            target_codes = set()
            for market_code in self.target_market_codes:
                # 查询每个市场的股票代码
                cursor = collection.find({"ts_code": {"$regex": f"\.{market_code}$"}})
                market_codes = {doc["ts_code"] for doc in cursor}
                target_codes.update(market_codes)
            
            if not target_codes:
                logger.warning(f"未找到目标市场 {self.target_market_codes} 的股票代码")
                return set()
                
            logger.info(f"成功从stock_basic集合获取到 {len(target_codes)} 个目标市场股票代码")
            return target_codes
        except Exception as e:
            logger.error(f"获取目标市场股票代码失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return set()
            
    def store_data(self, data: pd.DataFrame) -> Dict:
        """
        存储数据到MongoDB
        
        Args:
            data: 需要存储的数据
            
        Returns:
            包含插入、更新和跳过记录数的字典
        """
        if data.empty:
            logger.warning("传入的数据为空，无需存储")
            return {"inserted": 0, "updated": 0, "skipped": 0}
            
        try:
            # 确保MongoDB连接
            if not self.mongo_client.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if not self.mongo_client.connect():
                    logger.error("连接MongoDB失败，无法存储数据")
                    return {"inserted": 0, "updated": 0, "skipped": 0}
                    
            # 获取数据库对象
            db = self.mongo_client.get_db(self.db_name)
            
            # 获取集合对象
            collection = db[self.collection_name]
            
            # 初始化统计计数器
            stats = {"inserted": 0, "updated": 0, "skipped": 0}
            
            # 从接口配置中获取主键字段，如果没有指定，则使用默认的 ["ts_code", "trade_date"]
            primary_keys = self.interface_config.get("primary_keys", ["ts_code", "trade_date"])
            
            # 将DataFrame转换为字典列表
            records = data.to_dict("records")
            
            # 批量处理记录
            for record in records:
                # 构建查询条件 - 使用所有主键字段
                query = {key: record[key] for key in primary_keys if key in record}
                
                # 检查是否已存在记录
                existing_doc = collection.find_one(query)
                
                if existing_doc:
                    # 记录已存在，检查是否需要更新
                    # 创建不包含_id的记录副本，用于比较
                    existing_doc_compare = {k: v for k, v in existing_doc.items() if k != "_id"}
                    record_compare = {k: v for k, v in record.items() if k in existing_doc_compare}
                    
                    # 比较数据是否有更新
                    if existing_doc_compare != record_compare:
                        # 数据有更新，执行更新操作
                        collection.update_one({"_id": existing_doc["_id"]}, {"$set": record})
                        stats["updated"] += 1
                    else:
                        # 数据没有变化，跳过
                        stats["skipped"] += 1
                else:
                    # 记录不存在，执行插入操作
                    collection.insert_one(record)
                    stats["inserted"] += 1
                    
            # 更新统计信息
            self.last_operation_stats = stats
            
            logger.info(f"数据存储完成: 插入 {stats['inserted']} 条，更新 {stats['updated']} 条，跳过 {stats['skipped']} 条")
            return stats
            
        except Exception as e:
            logger.error(f"存储数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return {"inserted": 0, "updated": 0, "skipped": 0}

    def fetch_daily_basic_data_by_date_range(self, start_date: str, end_date: str, batch_size: int = 100):
        """
        按日期范围获取每日指标数据
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            batch_size: 每批获取的股票数量
        """
        try:
            # 获取目标板块股票代码
            stock_codes = list(self.get_target_ts_codes_from_stock_basic())
            if not stock_codes:
                logger.error("未获取到目标板块股票代码，无法继续获取数据")
                return
                
            # 记录总股票数量
            total_stocks = len(stock_codes)
            logger.info(f"准备获取 {total_stocks} 个股票的每日指标数据，日期范围: {start_date} - {end_date}")
            
            # 分批处理股票
            batches = self._split_stock_codes(stock_codes, batch_size)
            total_batches = len(batches)
            
            # 处理每一批股票
            for i, batch in enumerate(batches, 1):
                logger.info(f"处理第 {i}/{total_batches} 批股票，包含 {len(batch)} 个股票")
                
                # 使用ThreadPoolExecutor并行处理股票
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # 为每个股票创建任务
                    future_to_code = {executor.submit(self.fetch_daily_basic_by_code, code, start_date, end_date): code for code in batch}
                    
                    # 处理完成的任务
                    for future in future_to_code:
                        code = future_to_code[future]
                        try:
                            df = future.result()
                            if not df.empty:
                                # 存储数据
                                self.store_data(df)
                        except Exception as e:
                            logger.error(f"处理股票 {code} 时出错: {str(e)}")
                
                # 每批次后稍微延迟，避免API连续请求过快
                time.sleep(1)
                
            logger.success(f"按日期范围获取每日指标数据完成，日期范围: {start_date} - {end_date}")
            
        except Exception as e:
            logger.error(f"按日期范围获取每日指标数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
    
    def fetch_recent_data(self, days: int = 7):
        """
        获取最近几天的每日指标数据
        
        Args:
            days: 最近几天，默认7天
        """
        try:
            # 计算日期范围
            end_date = datetime.now().strftime('%Y%m%d')
            start_date_obj = datetime.now() - timedelta(days=days)
            start_date = start_date_obj.strftime('%Y%m%d')
            
            logger.info(f"获取最近 {days} 天的每日指标数据，日期范围: {start_date} - {end_date}")
            
            # 调用按日期范围获取数据的方法
            self.fetch_daily_basic_data_by_date_range(start_date, end_date)
            
            logger.success(f"获取最近 {days} 天的每日指标数据完成")
            
        except Exception as e:
            logger.error(f"获取最近数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
    
    def fetch_full_history(self, start_date: str = "19900101"):
        """
        获取从start_date到今天的完整历史每日指标数据
        
        Args:
            start_date: 起始日期，格式为YYYYMMDD，默认为19900101
        """
        try:
            # 设置结束日期为今天
            end_date = datetime.now().strftime('%Y%m%d')
            
            logger.info(f"获取完整历史每日指标数据，日期范围: {start_date} - {end_date}")
            
            # 调用按日期范围获取数据的方法
            self.fetch_daily_basic_data_by_date_range(start_date, end_date)
            
            logger.success(f"获取完整历史每日指标数据完成")
            
        except Exception as e:
            logger.error(f"获取完整历史数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")


if __name__ == "__main__":
    import argparse
    
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description="每日指标数据获取器 - 从湘财Tushare获取每日指标数据并存储到MongoDB")
    
    # 添加命令行参数
    parser.add_argument("--full", action="store_true", help="获取完整历史数据(从19900101至今)")
    parser.add_argument("--recent", action="store_true", help="获取最近7天数据(默认模式)")
    parser.add_argument("--start-date", type=str, help="指定开始日期(YYYYMMDD格式)")
    parser.add_argument("--end-date", type=str, help="指定结束日期(YYYYMMDD格式)")
    parser.add_argument("--verbose", action="store_true", help="显示详细日志")
    parser.add_argument("--mock", action="store_true", help="使用模拟数据模式")
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 初始化获取器
    fetcher = DailyBasicFetcher(verbose=args.verbose)
    
    # 根据命令行参数执行不同的操作
    if args.full:
        # 获取完整历史数据
        fetcher.fetch_full_history()
    elif args.start_date and args.end_date:
        # 获取指定日期范围的数据
        fetcher.fetch_daily_basic_data_by_date_range(args.start_date, args.end_date)
    else:
        # 默认获取最近7天的数据
        fetcher.fetch_recent_data()
    
    logger.success("每日指标数据获取器执行完毕")
