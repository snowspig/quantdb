#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import random
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Set, Optional, Any, Union, Callable
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import threading
from pymongo import MongoClient
from contextlib import contextmanager
import requests
import requests.adapters

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.append(project_root)

# 导入日志模块
from loguru import logger

# 导入数据获取相关模块
from data_fetcher.tushare_client import TushareClient

# 导入WAN口管理器
try:
    from wan_manager.port_allocator import port_allocator
except ImportError:
    logger.error("未找到wan_manager.port_allocator模块，请确保WAN管理器已正确安装")
    port_allocator = None

# 设置日志格式
logger.remove()  # 移除默认处理器
log_format = "{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
logger.add(sys.stderr, format=log_format, level="INFO")

# 加载YAML配置的函数
def load_yaml_config(config_path: str) -> Dict:
    """
    加载YAML配置文件
    
    Args:
        config_path: 配置文件路径
    
    Returns:
        Dict: 配置字典
    """
    try:
        import yaml
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logger.error(f"加载配置文件失败: {str(e)}")
        return {}

class SourceAddressAdapter(requests.adapters.HTTPAdapter):
    """HTTP适配器，支持指定源IP地址和端口"""
    
    def __init__(self, source_address, **kwargs):
        """
        初始化适配器
        
        Args:
            source_address: 源地址元组 (host, port)
        """
        self.source_address = source_address
        super(SourceAddressAdapter, self).__init__(**kwargs)
        
    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        """
        初始化连接池管理器，设置源地址
        
        Args:
            connections: 连接数
            maxsize: 最大连接数
            block: 是否阻塞
            pool_kwargs: 连接池参数
        """
        # 设置源地址
        logger.debug(f"初始化连接池: source_address={self.source_address}")
        pool_kwargs['source_address'] = self.source_address
        super(SourceAddressAdapter, self).init_poolmanager(
            connections, maxsize, block, **pool_kwargs
        )

class TushareClientWAN:
    """
    专用于WAN绑定的Tushare客户端
    """
    
    def __init__(self, token: str, timeout: int = 60, api_url: str = None):
        """初始化WAN绑定的Tushare客户端"""
        self.token = token
        self.timeout = timeout
        
        # 使用传入的API URL或默认为湘财Tushare API地址
        self.url = api_url or "http://api.waditu.com"
        
        self.headers = {
            "Content-Type": "application/json",
        }
        self.proxies = None
        self.local_addr = None
        
        # 验证token
        mask_token = token[:4] + '*' * (len(token) - 8) + token[-4:] if len(token) > 8 else '***'
        logger.debug(f"TushareClientWAN初始化: {mask_token} (长度: {len(token)}), API URL: {self.url}")
    
    def set_local_address(self, host: str, port: int):
        """设置本地地址绑定"""
        self.local_addr = (host, port)
        logger.debug(f"已设置本地地址绑定: {host}:{port}")
    
    def reset_local_address(self):
        """重置本地地址绑定"""
        self.local_addr = None
        logger.debug("已重置本地地址绑定")
    
    def set_timeout(self, timeout: int):
        """设置请求超时"""
        self.timeout = timeout
        logger.debug(f"已设置请求超时: {timeout}秒")
    
    def get_data(self, api_name: str, params: dict, fields: list = None):
        """
        获取API数据
        
        Args:
            api_name: API名称
            params: 请求参数
            fields: 返回字段列表
            
        Returns:
            DataFrame格式的数据
        """
        try:
            # 创建请求数据 - 与原始TushareClient请求格式保持一致
            req_params = {
                "api_name": api_name,
                "token": self.token,
                "params": params or {},
                "fields": fields or ""
            }
            
            logger.debug(f"请求URL: {self.url}, API: {api_name}, Token长度: {len(self.token)}")
            
            # 使用requests发送请求
            start_time = time.time()
            
            # 支持本地地址绑定的请求
            s = requests.Session()
            if self.local_addr:
                # 设置source_address
                s.mount('http://', SourceAddressAdapter(self.local_addr))
                s.mount('https://', SourceAddressAdapter(self.local_addr))
            
            response = s.post(
                self.url,
                json=req_params,
                headers=self.headers,
                timeout=self.timeout,
                proxies=self.proxies
            )
            
            elapsed = time.time() - start_time
            logger.debug(f"API请求耗时: {elapsed:.2f}s")
            
            # 检查响应状态
            if response.status_code != 200:
                logger.error(f"API请求错误: {response.status_code} - {response.text}")
                return None
                
            # 解析响应
            result = response.json()
            if result.get('code') != 0:
                logger.error(f"API返回错误: {result.get('code')} - {result.get('msg')}")
                return None
                
            # 转换为DataFrame
            data = result.get('data')
            if not data or not data.get('items'):
                logger.debug("API返回空数据")
                return pd.DataFrame()
                
            items = data.get('items')
            columns = data.get('fields')
            
            # 创建DataFrame
            df = pd.DataFrame(items, columns=columns)
            return df
            
        except Exception as e:
            logger.error(f"API请求失败: {str(e)}")
            return None

class MoneyflowFetcher:
    """
    个股资金流向数据获取器
    
    该类用于从Tushare获取个股资金流向数据并保存到MongoDB数据库
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
        interface_name: str = "moneyflow.json",
        target_market_codes: Set[str] = {"00", "30", "60", "68"},  # 默认只保存00 30 60 68四个板块的股票数据
        db_name: str = None,
        collection_name: str = "moneyflow",
        verbose: bool = False,
        max_workers: int = 3,  # 并行工作线程数
        retry_count: int = 5,  # 数据获取重试次数 (5次)
        retry_delay: int = 5,  # 重试延迟时间(秒)
        batch_size: int = 10000  # 每批次获取数据的最大数量，防止超过API限制
    ):
        """
        初始化个股资金流向数据获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口配置文件名
            target_market_codes: 目标市场代码集合，只保存这些市场的股票数据
            db_name: 数据库名称，如果为None则使用配置中的默认值
            collection_name: 集合名称
            verbose: 详细模式，输出更多日志信息
            max_workers: 并行工作线程数
            retry_count: 数据获取重试次数
            retry_delay: 重试延迟时间(秒)
            batch_size: 每批次获取数据的最大数量
        """
        # 加载配置
        self.config = load_yaml_config(config_path)
        
        # 设置详细模式
        self.verbose = verbose
        
        # 设置目标市场代码集合
        self.target_market_codes = target_market_codes
        
        # 设置重试参数
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        
        # 设置接口信息
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.interface_path = os.path.join(interface_dir, interface_name)
        
        # 加载接口配置
        self._load_interface_config()
        
        # 获取API名称
        self.api_name = self.interface_config.get("api_name", "moneyflow")
        
        # 初始化Tushare客户端
        self.ts_client = self._init_client()
        
        # 直接保存token和api_url便于WAN客户端使用
        self.token = self.ts_client.token
        self.api_url = self.ts_client.api_url
        
        # 初始化数据库连接
        # db_config用于MongoDB连接配置
        db_config = self.config.get("mongodb", {})
        self.mongodb_client = self._init_mongodb_client(db_config)
        
        # 如果db_name为None，则使用配置中的默认值
        if db_name is None:
            db_name = db_config.get("database", "tushare_data")


        # 获取数据库和集合
        self.db = self.mongodb_client[db_name]
        self.collection = self.db[collection_name]
        
        # 提高处理效率的并行设置
        self.max_workers = max_workers
        self.batch_size = batch_size
        
        # 设置WAN接口配置
        self.port_allocator = port_allocator  # 使用全局端口分配器
        
        # 添加索引验证标志，避免重复验证
        self.indexes_verified = False
        
        # 初始化时创建一次索引
        self._ensure_indexes(self.collection)
        
        logger.success("初始化个股资金流向数据获取器完成")

    def _ensure_indexes(self, collection) -> bool:
        """确保必要的索引存在，避免重复调用"""
        # 如果已经验证过索引，直接返回
        if hasattr(self, 'indexes_verified') and self.indexes_verified:
            return True
        
        try:
            # 获取所有已存在的索引
            existing_indexes = collection.index_information()
            logger.debug(f"集合 {collection.name} 现有索引信息: {existing_indexes}")
            
            # 从接口配置获取索引字段
            index_fields = self.interface_config.get("index_fields", ["ts_code", "trade_date"])
            if "index_fields" not in self.interface_config and "primary_keys" in self.interface_config:
                index_fields = self.interface_config.get("primary_keys", ["ts_code", "trade_date"])
            
            # 检查是否已存在复合索引
            has_compound_index = False
            
            # 查找所有索引，检查是否存在所需的复合索引
            for idx_name, idx_info in existing_indexes.items():
                # 跳过_id索引
                if idx_name == '_id_':
                    continue
                    
                # 检查是否有匹配的复合索引
                if len(idx_info.get('key', [])) == len(index_fields):
                    keys = [k[0] for k in idx_info.get('key', [])]
                    if all(field in keys for field in index_fields):
                        has_compound_index = True
                        logger.info(f"已存在复合索引 {index_fields}，索引名: {idx_name}，跳过创建")
                        break
            
            # 如果没有复合索引，创建一个
            if not has_compound_index:
                logger.info(f"正在为集合 {collection.name} 创建复合唯一索引 {index_fields}...")
                collection.create_index(
                    [(field, 1) for field in index_fields],
                    unique=True,
                    background=True
                )
                logger.success(f"已成功创建复合唯一索引 {index_fields}")
            
            # 设置标志，表示索引已验证
            self.indexes_verified = True
            return True
        except Exception as e:
            logger.error(f"创建索引时出错: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False
    
    def _load_interface_config(self):
        """加载接口配置文件"""
        try:
            if not os.path.exists(self.interface_path):
                logger.error(f"接口配置文件不存在: {self.interface_path}")
                # 创建默认配置
                self.interface_config = {
                    "api_name": "moneyflow",
                    "available_fields": [],
                    "index_fields": ["ts_code", "trade_date"], 
                    "primary_keys": ["ts_code", "trade_date"]   # 保留兼容性
                }
                logger.warning("使用默认接口配置")
                return
                
            with open(self.interface_path, 'r', encoding='utf-8') as f:
                self.interface_config = json.load(f)
                
            logger.debug(f"成功加载接口配置: {self.interface_name}")
                
            # 验证接口配置，保证必要字段存在
            if "api_name" not in self.interface_config:
                logger.warning(f"接口配置缺少api_name字段，将使用默认值: moneyflow")
                self.interface_config["api_name"] = "moneyflow"
                
            if "available_fields" not in self.interface_config:
                logger.warning(f"接口配置缺少available_fields字段，将使用空列表")
                self.interface_config["available_fields"] = []
                
            # 从index_fields字段获取主键，如果不存在则使用primary_keys或默认值
            if "index_fields" in self.interface_config:
                primary_keys = self.interface_config["index_fields"]
                logger.debug(f"从接口配置的index_fields字段获取主键: {primary_keys}")
                # 同时设置primary_keys以保持兼容性
                self.interface_config["primary_keys"] = primary_keys
            elif "primary_keys" in self.interface_config:
                primary_keys = self.interface_config["primary_keys"]
                logger.debug(f"从接口配置的primary_keys字段获取主键: {primary_keys}")
                # 同时设置index_fields
                self.interface_config["index_fields"] = primary_keys
            else:
                logger.warning(f"接口配置缺少index_fields和primary_keys字段，将使用默认值: ['ts_code', 'trade_date']")
                self.interface_config["primary_keys"] = ["ts_code", "trade_date"]
                self.interface_config["index_fields"] = ["ts_code", "trade_date"]
                
        except Exception as e:
            logger.error(f"加载接口配置文件失败: {str(e)}")
            # 使用默认配置
            self.interface_config = {
                "api_name": "moneyflow",
                "available_fields": [],
                "index_fields": ["ts_code", "trade_date"],
                "primary_keys": ["ts_code", "trade_date"]
            }
            logger.warning("使用默认接口配置")
    
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
            
    def _split_stock_codes(self, ts_codes: List[str], batch_size: int) -> List[List[str]]:
        """
        将股票代码列表分成指定大小的批次
        
        Args:
            ts_codes: 股票代码列表
            batch_size: 每批的大小
            
        Returns:
            分批后的股票代码列表的列表
        """
        if not ts_codes:
            return []
            
        # 计算需要多少批次
        total_batches = (len(ts_codes) + batch_size - 1) // batch_size
        
        # 分批
        batches = []
        for i in range(total_batches):
            start_idx = i * batch_size
            end_idx = min(start_idx + batch_size, len(ts_codes))
            batch = ts_codes[start_idx:end_idx]
            batches.append(batch)
            
        return batches

    def fetch_moneyflow_by_date(self, trade_date: str, ts_code: str = None) -> pd.DataFrame:
        """
        按日期获取个股资金流向数据
        
        Args:
            trade_date: 交易日期，格式为YYYYMMDD
            ts_code: 可选，股票代码
            
        Returns:
            DataFrame形式的资金流向数据
        """
        try:
            params = {"trade_date": trade_date}
            if ts_code:
                params["ts_code"] = ts_code
                
            logger.debug(f"获取日期 {trade_date} 的个股资金流向数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
            
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
            
            logger.info(f"成功获取日期 {trade_date} 的个股资金流向数据"+(f" 股票代码: {ts_code}" if ts_code else f"，共 {len(df)} 条记录"))
            return df
        except Exception as e:
            logger.error(f"获取日期 {trade_date} 的个股资金流向数据失败: {str(e)}")
            return pd.DataFrame()

    def fetch_moneyflow_by_code(self, ts_code: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        按股票代码获取个股资金流向数据
        
        Args:
            ts_code: 股票代码
            start_date: 可选，开始日期，格式为YYYYMMDD
            end_date: 可选，结束日期，格式为YYYYMMDD
            
        Returns:
            DataFrame形式的个股资金流向数据
        """
        try:
            params = {"ts_code": ts_code}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
                
            logger.debug(f"获取股票 {ts_code} 的个股资金流向数据"+(f" 日期范围: {start_date} 至 {end_date}" if start_date and end_date else ""))
            
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
            
            logger.info(f"成功获取股票 {ts_code} 的个股资金流向数据，共 {record_count} 条记录")
            return df
        except Exception as e:
            logger.error(f"获取股票 {ts_code} 的个股资金流向数据失败: {str(e)}")
            return pd.DataFrame()

    def get_target_ts_codes_from_stock_basic(self) -> Set[str]:
        """
        从stock_basic集合中获取目标板块的股票代码
        如果当前数据库中没有stock_basic集合，会尝试从tushare_data数据库获取
        如果仍未找到，则直接从Tushare API获取
        
        Returns:
            目标板块股票代码集合
        """
        try:
            # 尝试从当前数据库获取stock_basic集合
            ts_codes = self._get_ts_codes_from_db(self.db)
            if ts_codes:
                return ts_codes
                
            # 如果当前数据库没有stock_basic集合，尝试从config中指定的tushare_data数据库获取
            mongo_config = self.config.get("mongodb", {})
            tushare_db_name = mongo_config.get("db_name", "tushare_data")
            
            if tushare_db_name != self.db.name:
                logger.info(f"在当前数据库 {self.db.name} 中未找到stock_basic集合，尝试从 {tushare_db_name} 数据库获取")
                tushare_db = self.mongodb_client[tushare_db_name]
                ts_codes = self._get_ts_codes_from_db(tushare_db)
                if ts_codes:
                    return ts_codes
            
            # 如果数据库中没有找到，直接从Tushare API获取股票列表
            logger.info("尝试直接从Tushare API获取股票列表")
            ts_codes = self._get_ts_codes_from_api()
            if ts_codes:
                return ts_codes
                
            logger.warning(f"未找到目标市场 {self.target_market_codes} 的股票代码")
            return set()
                    
        except Exception as e:
            logger.error(f"获取目标市场股票代码失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return set()
            
    def _get_ts_codes_from_db(self, db) -> Set[str]:
        """从指定数据库获取股票代码"""
        try:
            # 检查stock_basic集合是否存在
            if "stock_basic" not in db.list_collection_names():
                logger.warning(f"数据库 {db.name} 中不存在stock_basic集合")
                return set()
                
            # 获取stock_basic集合对象
            collection = db.stock_basic
            
            # 查询目标市场代码的股票
            logger.info(f"从数据库 {db.name} 的stock_basic集合查询目标板块 {self.target_market_codes} 的股票代码")
            
            # 构建查询条件：symbol前两位在target_market_codes中
            query_conditions = []
            for market_code in self.target_market_codes:
                # 使用正则表达式匹配symbol前两位
                query_conditions.append({"symbol": {"$regex": f"^{market_code}"}})
                
            # 使用$or操作符组合多个条件
            query = {"$or": query_conditions} if query_conditions else {}
            
            # 只查询ts_code字段
            result = collection.find(query, {"ts_code": 1, "_id": 0})
            
            # 提取ts_code集合
            ts_codes = set()
            for doc in result:
                if "ts_code" in doc:
                    ts_codes.add(doc["ts_code"])
            
            if ts_codes:
                logger.success(f"从数据库 {db.name} 的stock_basic集合获取到 {len(ts_codes)} 个目标市场股票代码")
                
                # 输出详细日志
                if hasattr(self, 'verbose') and self.verbose:
                    sample_codes = list(ts_codes)[:5] if ts_codes else []
                    logger.debug(f"样例股票代码: {sample_codes}")
                    
            return ts_codes
        except Exception as e:
            logger.warning(f"从数据库 {db.name} 获取股票代码失败: {str(e)}")
            return set()
            
    def _get_ts_codes_from_api(self) -> Set[str]:
        """直接从Tushare API获取股票列表"""
        try:
            # 使用stock_basic接口获取所有股票列表
            df = self.ts_client.get_data(
                api_name="stock_basic",
                params={},
                fields=["ts_code", "symbol", "name", "area", "industry", "list_date"]
            )
            
            if df.empty:
                logger.error("从Tushare API获取股票列表失败，API返回数据为空")
                return set()
            
            # 过滤出目标市场代码的股票
            if 'symbol' in df.columns:
                filtered_df = df[df['symbol'].str[:2].isin(self.target_market_codes)]
                ts_codes = set(filtered_df['ts_code'].tolist())
                
                logger.success(f"从Tushare API获取到 {len(ts_codes)} 个目标市场股票代码")
                
                # 尝试保存到当前数据库的stock_basic集合中
                try:
                    if len(df) > 0:
                        logger.info(f"保存获取到的股票基本信息到 {self.db.name}.stock_basic 集合")
                        records = df.to_dict("records")
                        self.db.stock_basic.insert_many(records, ordered=False)
                        logger.success(f"成功保存 {len(records)} 条股票基本信息")
                except Exception as save_error:
                    logger.warning(f"保存股票基本信息失败: {str(save_error)}")
                
                return ts_codes
            else:
                logger.error("从Tushare API获取的数据没有symbol列")
                return set()
                
        except Exception as e:
            logger.error(f"从Tushare API获取股票列表失败: {str(e)}")
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
            start_time = time.time()
            
            # 获取数据库对象和集合对象
            collection = self.collection
            
            # 将DataFrame转换为字典列表
            records = data.to_dict("records")
            
            # 从接口配置中获取主键字段，如果没有指定，则使用默认的 ["ts_code", "trade_date"]
            primary_keys = self.interface_config.get("index_fields", ["ts_code", "trade_date"])
            
            # 批量处理，避免一次性处理太多记录
            batch_size = 10000  # 减小batch_size以减轻MongoDB负担
            total_batches = (len(records) + batch_size - 1) // batch_size
            
            total_inserted = 0
            total_updated = 0
            total_skipped = 0
            
            # 准备批量操作
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                
                # 增加超时时间，避免大批量操作超时
                try:
                    batch_result = self._batch_upsert(collection, batch, primary_keys)
                    
                    total_inserted += batch_result["inserted"]
                    total_updated += batch_result["updated"]
                    total_skipped += batch_result["skipped"]
                    
                    # 进度显示
                    if hasattr(self, 'verbose') and self.verbose and total_batches > 1:
                        progress = (i + len(batch)) / len(records) * 100
                        progress = min(progress, 100)
                        logger.debug(f"MongoDB保存进度: {i+len(batch)}/{len(records)} ({progress:.1f}%)")
                except Exception as e:
                    logger.error(f"批量处理数据时出错: {str(e)}")
                    import traceback
                    logger.debug(f"详细错误信息: {traceback.format_exc()}")
            
            elapsed = time.time() - start_time
            total_processed = total_inserted + total_updated + total_skipped
            
            # 保存操作统计信息，供调用方使用
            self.last_operation_stats = {
                "inserted": total_inserted,
                "updated": total_updated,
                "skipped": total_skipped,
                "total": total_processed
            }
            
            # 输出详细的统计信息
            logger.success(f"数据处理完成: 新插入 {total_inserted} 条记录，更新 {total_updated} 条记录，跳过 {total_skipped} 条重复记录，共处理 {total_processed}/{len(records)} 条记录，耗时 {elapsed:.2f}s")
            
            # 确认是否成功处理了数据
            if total_processed > 0:
                return self.last_operation_stats
            else:  # 使用else替换错误的缩进
                if len(records) > 0:
                    logger.warning(f"提交了 {len(records)} 条记录，但MongoDB未报告任何插入、更新或跳过的记录")
                return {"inserted": 0, "updated": 0, "skipped": 0}
                
        except Exception as e:
            logger.error(f"存储数据到MongoDB失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return {"inserted": 0, "updated": 0, "skipped": 0}
            
    def is_connected(self) -> bool:
        """
        检查是否已连接到MongoDB
        """
        try:
            # 简单的检查方法，尝试获取服务器信息
            self.mongodb_client.server_info()
            return True
        except Exception:
            return False
            
    def _batch_upsert(self, collection, records: List[Dict], unique_keys: List[str]) -> Dict[str, int]:
        """
        批量更新或插入记录，使用唯一键检测记录是否存在
        
        Args:
            collection: MongoDB集合对象
            records: 要保存的记录列表
            unique_keys: 唯一键列表
            
        Returns:
            包含插入、更新和跳过记录数的字典
        """
        if not records:
            return {"inserted": 0, "updated": 0, "skipped": 0}
            
        # 为了提高效率，我们不再逐个检查记录是否存在，而是批量处理
        # 而是用两步策略：先查询哪些记录已存在，然后将记录分为插入和更新两组
        inserted = 0
        updated = 0
        skipped = 0
        
        # 构建所有记录的唯一键查询
        existing_records = set()
        queries = []
        valid_records = []
        
        # 第一步：提取所有有效记录并构建查询条件
        for record in records:
            # 构建查询条件
            query = {}
            key_str = ""
            is_valid = True
            
            for key in unique_keys:
                if key in record and record[key] is not None:
                    query[key] = record[key]
                    key_str += str(record[key]) + "_"
                else:
                    # 缺少唯一键字段，标记为无效
                    is_valid = False
                    break
                    
            if not is_valid or len(query) != len(unique_keys):
                # 跳过无效记录
                skipped += 1
                continue
                
            # 添加到有效记录列表
            valid_records.append((record, query))
            # 添加查询条件
            queries.append(query)
            
        # 第二步：批量查询数据库中已存在的记录
        if not valid_records:
            return {"inserted": 0, "updated": 0, "skipped": 0}
            
        # 使用$or查询批量检查记录是否存在
        existing_keys = set()
        try:
            if queries:
                existing_docs = list(collection.find({"$or": queries}, {key: 1 for key in unique_keys}))
                
                # 记录已存在的记录的唯一键
                for doc in existing_docs:
                    key_values = tuple(doc.get(key) for key in unique_keys)
                    existing_keys.add(key_values)
        except Exception as e:
            logger.error(f"批量查询记录存在性失败: {str(e)}")
            # 如果查询失败，假设所有记录都需要更新
            existing_keys = set()  # 清空集合，后续会执行upsert
            
        # 第三步：根据查询结果准备插入和更新操作
        # 导入pymongo模块
        from pymongo import UpdateOne, InsertOne
        from pymongo import WriteConcern
        
        update_ops = []
        insert_ops = []
        
        
        for record, query in valid_records:
            # 构建唯一键元组
            key_values = tuple(record.get(key) for key in unique_keys)
            
            if key_values in existing_keys:
                # 记录已存在，执行更新
                update_ops.append(
                    UpdateOne(
                        query,
                        {"$set": record}
                    )
                )
                updated += 1
            else:
                # 记录不存在，执行插入
                insert_ops.append(InsertOne(record))
                inserted += 1
                
        # 分别执行插入和更新操作
        try:
            # 设置合理的WriteConcern参数
            temp_collection = collection.with_options(
                write_concern=WriteConcern(w=1, j=False)
            )
            
            # 执行插入操作
            if insert_ops:
                try:
                    insert_result = temp_collection.bulk_write(insert_ops, ordered=False)
                    real_inserted = insert_result.inserted_count
                    if real_inserted != len(insert_ops):
                        logger.warning(f"实际插入数量与预期不一致: 预期={len(insert_ops)}, 实际={real_inserted}")
                        inserted = real_inserted
                except Exception as bwe:
                    # 处理部分失败的插入
                    if hasattr(bwe, 'details'):
                        details = bwe.details
                        if 'nInserted' in details:
                            inserted = details['nInserted']
                        skipped += len(insert_ops) - inserted
                        
                        # 检查是否有重复键错误
                        if 'writeErrors' in details:
                            for error in details['writeErrors']:
                                if error.get('code') == 11000:  # 重复键错误
                                    if hasattr(self, 'verbose') and self.verbose:
                                        logger.debug(f"插入操作重复键错误: {error.get('errmsg', '')}")
                                        
                    logger.warning(f"插入操作部分失败: {len(bwe.details.get('writeErrors', []))} 错误")
            
            # 执行更新操作
            if update_ops:
                try:
                    update_result = temp_collection.bulk_write(update_ops, ordered=False)
                    real_updated = update_result.modified_count
                    if real_updated != len(update_ops):
                        logger.debug(f"部分记录未被修改，可能数据未变化: 预期={len(update_ops)}, 实际修改={real_updated}")
                except Exception as bwe:
                    # 处理部分失败的更新
                    if hasattr(bwe, 'details'):
                        details = bwe.details
                        if 'nModified' in details:
                            updated = details['nModified']
                        skipped += len(update_ops) - updated
                    logger.warning(f"更新操作部分失败: {len(bwe.details.get('writeErrors', []))} 错误")
                    
        except Exception as e:
            logger.error(f"执行批量操作失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")

        return {"inserted": inserted, "updated": updated, "skipped": skipped}

    def fetch_moneyflow_data_by_date_range(self, start_date: str, end_date: str, batch_size: int = 1, use_parallel: bool = True):
        """
        按日期范围获取个股资金流向数据
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            batch_size: 每批获取的股票数量
            use_parallel: 是否使用多WAN口并行处理
        """
        try:
            # 获取目标板块股票代码
            stock_codes = list(self.get_target_ts_codes_from_stock_basic())
            if not stock_codes:
                logger.error("未获取到目标板块股票代码，无法继续获取数据")
                return
                
            # 记录总股票数量
            total_stocks = len(stock_codes)
            logger.info(f"准备获取 {total_stocks} 个股票的个股资金流向数据，日期范围: {start_date} - {end_date}")
            
            # 检查是否可以并行处理
            if use_parallel and self.port_allocator:
                available_wans = self.port_allocator.get_available_wan_indices()
                if available_wans:
                    logger.info(f"使用并行模式处理 {len(stock_codes)} 个股票代码，每批 {batch_size} 个股票")
                    return self._fetch_data_parallel(stock_codes, start_date, end_date, batch_size)
            
            # 分批处理股票
            batches = self._split_stock_codes(stock_codes, batch_size)
            total_batches = len(batches)
            
            # 处理每一批股票
            for i, batch in enumerate(batches, 1):
                logger.info(f"处理第 {i}/{total_batches} 批股票，包含 {len(batch)} 个股票")
                
                # 使用ThreadPoolExecutor并行处理股票
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # 为每个股票创建任务
                    future_to_code = {executor.submit(self.fetch_moneyflow_by_code, code, start_date, end_date): code for code in batch}
                    
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
                
            logger.success(f"按日期范围获取个股资金流向数据完成，日期范围: {start_date} - {end_date}")
            
        except Exception as e:
            logger.error(f"按日期范围获取个股资金流向数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
    
    def fetch_recent_data(self, days: int = 7, verbose: bool = True, use_parallel: bool = True, batch_size: int = 1):
        """
        获取最近几天的个股资金流向数据
        
        Args:
            days: 最近几天，默认7天
            verbose: 详细模式，用于控制日志和测试数据生成
            use_parallel: 是否使用多WAN口并行处理
            batch_size: 每批处理的股票数量
        """
        try:
            # 临时设置详细模式，以便在没有交易日历时生成测试数据
            old_verbose = getattr(self, 'verbose', False)  # 安全获取属性，默认False
            self.verbose = verbose
            
            # 计算日期范围
            end_date = datetime.now().strftime('%Y%m%d')
            start_date_obj = datetime.now() - timedelta(days=days)
            start_date = start_date_obj.strftime('%Y%m%d')
            
            logger.info(f"获取最近 {days} 天的个股资金流向数据，日期范围: {start_date} - {end_date}")
            
            # 生成日期列表 - 为近期数据使用假的工作日（周一至周五）
            trade_dates = []
            
            # 使用工作日作为交易日的估算值
            logger.info(f"为日期范围 {start_date} 至 {end_date} 生成工作日作为交易日期")
            
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            
            # 获取日期范围内的所有工作日
            current_dt = start_dt
            while current_dt <= end_dt:
                # 跳过周六(5)和周日(6)
                if current_dt.weekday() < 5:  # 0-4表示周一至周五
                    trade_dates.append(current_dt.strftime('%Y%m%d'))
                current_dt += timedelta(days=1)
                
            logger.info(f"生成了 {len(trade_dates)} 个工作日日期")
            
            # 获取目标板块的股票代码
            target_ts_codes = self.get_target_ts_codes_from_stock_basic()
            
            if not target_ts_codes:
                logger.warning("未能获取到目标板块的股票代码，将使用直接按日期获取全市场数据模式")
                
            # 使用多WAN口并行处理
            if use_parallel and self.port_allocator and len(self.port_allocator.get_available_wan_indices()) > 0:
                available_wans = self.port_allocator.get_available_wan_indices()
                wan_count = len(available_wans)
                logger.info(f"使用并行模式处理近期数据，目标日期: {len(trade_dates)}个，使用 {wan_count} 个WAN接口")
                
                # 创建线程池处理每个日期
                with ThreadPoolExecutor(max_workers=min(wan_count, len(trade_dates))) as executor:
                    # 为每个日期分配一个WAN
                    futures = {}
                    for i, trade_date in enumerate(trade_dates):
                        # 循环分配WAN
                        wan_idx = available_wans[i % wan_count]
                        future = executor.submit(self._fetch_data_for_date_parallel, trade_date, wan_idx, target_ts_codes)
                        futures[future] = trade_date
                    
                    # 处理结果
                    for future in as_completed(futures):
                        trade_date = futures[future]
                        try:
                            result_stats = future.result()
                            total_inserted = result_stats.get("inserted", 0)
                            total_updated = result_stats.get("updated", 0)
                            total_skipped = result_stats.get("skipped", 0)
                            logger.debug(f"日期 {trade_date} 处理完成: 新增 {total_inserted} 条记录，更新 {total_updated} 条记录，跳过 {total_skipped} 条记录")
                        except Exception as e:
                            logger.error(f"处理日期 {trade_date} 时发生错误: {str(e)}")
            else:
                # 串行处理
                logger.info(f"使用串行模式处理近期数据，目标日期: {len(trade_dates)}个")
                for trade_date in trade_dates:
                    try:
                        logger.info(f"处理日期 {trade_date} 的个股资金流向数据")
                        # 使用直接获取全市场数据的方式
                        df = self.fetch_moneyflow_by_date(trade_date=trade_date)
                        
                        if df.empty:
                            logger.warning(f"日期 {trade_date} 未获取到数据或过滤后无目标板块数据")
                            continue
                            
                        # 筛选目标板块的股票
                        if target_ts_codes and not df.empty:
                            # 筛选目标板块的股票
                            df = df[df['ts_code'].isin(target_ts_codes)]
                        
                        if df.empty:
                            logger.warning(f"日期 {trade_date} 过滤后无目标板块数据")
                            continue
                            
                        # 存储数据
                        result_stats = self.store_data(df)
                        logger.info(f"日期 {trade_date} 处理完成: 新增 {result_stats.get('inserted', 0)} 条记录，更新 {result_stats.get('updated', 0)} 条记录，跳过 {result_stats.get('skipped', 0)} 条记录")
                    except Exception as e:
                        logger.error(f"处理日期 {trade_date} 时发生错误: {str(e)}")
                        import traceback
                        logger.debug(f"详细错误信息: {traceback.format_exc()}")
            
            # 恢复详细模式设置
            self.verbose = old_verbose
            
        except Exception as e:
            # 恢复详细模式设置
            self.verbose = old_verbose
            
            logger.error(f"获取最近 {days} 天的个股资金流向数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            
        logger.success(f"获取最近 {days} 天的个股资金流向数据完成")

    def _fetch_data_for_date_parallel(self, trade_date: str, wan_idx: int, target_ts_codes: Set[str] = None) -> Dict:
        """
        并行处理特定日期的数据
        
        Args:
            trade_date: 交易日期
            wan_idx: WAN接口索引
            target_ts_codes: 目标股票代码集合（可选）
            
        Returns:
            处理结果的统计信息字典
        """
        try:
            logger.info(f"处理日期 {trade_date} 的个股资金流向数据")
            
            # 使用多WAN接口直接按日期获取全市场数据
            logger.info(f"使用多WAN接口直接按日期获取 {trade_date} 的全市场数据")
            
            wan_info = self._get_wan_socket(wan_idx)
            if not wan_info:
                logger.warning(f"未能获取WAN {wan_idx} 的端口，将使用普通方式获取数据")
                df = self.fetch_moneyflow_by_date(trade_date)
            else:
                try:
                    df = self.fetch_moneyflow_by_date_with_wan(trade_date=trade_date, wan_info=wan_info)
                finally:
                    # 释放WAN端口
                    if wan_info:
                        wan_idx, port = wan_info
                        time.sleep(1)  # 短暂延迟，确保端口完全释放
                        self.port_allocator.release_port(wan_idx, port)
                        
            if df.empty:
                logger.warning(f"日期 {trade_date} 未获取到数据或过滤后无目标板块数据")
                return {"inserted": 0, "updated": 0, "skipped": 0}
                
            # 如果有目标股票代码，过滤数据
            if target_ts_codes and not df.empty:
                df = df[df['ts_code'].isin(target_ts_codes)]
                
            if df.empty:
                logger.warning(f"日期 {trade_date} 过滤后无目标板块数据")
                return {"inserted": 0, "updated": 0, "skipped": 0}
                
            # 存储数据并返回统计信息
            result_stats = self.store_data(df)
            logger.info(f"日期 {trade_date} 处理完成: 新增 {result_stats.get('inserted', 0)} 条记录，更新 {result_stats.get('updated', 0)} 条记录，跳过 {result_stats.get('skipped', 0)} 条记录")
            
            return result_stats
                
        except Exception as e:
            logger.error(f"处理日期 {trade_date} 时发生错误: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return {"inserted": 0, "updated": 0, "skipped": 0}

    def fetch_full_history(self, start_date: str = "19900101", batch_size: int = 1, use_parallel: bool = True):
        """
        获取从start_date到今天的完整历史每日指标数据
        使用多WAN口并行抓取模式处理每个股票的数据
        
        Args:
            start_date: 起始日期，格式为YYYYMMDD，默认为19900101
            batch_size: 每批处理的股票数量，默认为1
            use_parallel: 是否使用多WAN口并行处理，默认为True
        """
        try:
            # 设置结束日期为今天
            end_date = datetime.now().strftime('%Y%m%d')
            
            logger.info(f"获取完整历史每日指标数据，日期范围: {start_date} - {end_date}")
            
            # 获取目标股票代码
            target_ts_codes = list(self.get_target_ts_codes_from_stock_basic())
            if not target_ts_codes:
                logger.error("未能获取到任何目标板块的股票代码")
                return False
                
            logger.info(f"已获取 {len(target_ts_codes)} 个目标股票代码，将按ts_code分批获取日期范围 {start_date} 至 {end_date} 的数据")
            
            # 检查是否可以并行处理
            if use_parallel and self.port_allocator:
                available_wans = self.port_allocator.get_available_wan_indices()
                if available_wans:
                    logger.info(f"使用并行模式处理 {len(target_ts_codes)} 个股票代码，每批 {batch_size} 个股票，可用的WAN接口数量: {len(available_wans)}")
                    return self._fetch_data_parallel(target_ts_codes, start_date, end_date, batch_size)
            
            # 如果不能并行处理，使用串行模式
            logger.info(f"使用串行模式处理 {len(target_ts_codes)} 个股票代码，每批 {batch_size} 个股票")
            return self._fetch_data_batch(target_ts_codes, start_date, end_date, batch_size)
            
        except Exception as e:
            logger.error(f"获取完整历史数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

    def _fetch_data_parallel(self, ts_codes: List[str], start_date: str, end_date: str, batch_size: int = 1) -> bool:
        """
        使用多WAN口并行获取多个股票的每日指标数据
        
        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
            batch_size: 每批处理的股票数量
            
        Returns:
            是否成功处理
        """
        import threading
        import queue
        
        if not ts_codes:
            logger.warning("没有股票代码可以查询")
            return False
            
        # 计算批次数
        total_batches = (len(ts_codes) + batch_size - 1) // batch_size
        logger.info(f"开始并行批量获取 {len(ts_codes)} 个股票的每日指标数据，分为 {total_batches} 个批次处理")
        
        # 获取可用的WAN接口
        available_wans = []
        if self.port_allocator:
            available_wans = self.port_allocator.get_available_wan_indices()
            logger.info(f"可用的WAN接口数量: {len(available_wans)}")
            
        if not available_wans:
            logger.warning("没有可用的WAN接口，将使用系统默认网络接口")
            # 如果没有可用WAN，回退到普通批处理
            return self._fetch_data_batch(ts_codes, start_date, end_date, batch_size)
        
        # 创建结果队列和线程列表
        result_queue = queue.Queue()
        threads = []
        
        # 创建一个线程锁用于日志和进度更新
        log_lock = threading.Lock()
        
        # 创建失败任务队列，用于存储失败后需要重试的任务
        retry_queue = queue.Queue()
        
        # 定义最大重试次数
        MAX_RETRY = 10
        
        # 处理批次的线程函数
        def process_batch(batch_index, batch_ts_codes, wan_idx, retry_count=0):
            try:
                with log_lock:
                    logger.debug(f"WAN {wan_idx} 开始处理批次 {batch_index+1}/{total_batches}" + 
                                (f" (重试 {retry_count}/{MAX_RETRY})" if retry_count > 0 else ""))
                
                # 获取WAN接口和端口
                wan_info = self._get_wan_socket(wan_idx)
                if not wan_info:
                    with log_lock:
                        logger.warning(f"无法为WAN {wan_idx} 获取端口，尝试其他WAN接口")
                    
                    # 如果还有重试次数，将任务放入重试队列
                    if retry_count < MAX_RETRY:
                        # 选择一个不同的WAN接口
                        new_wan_idx = available_wans[(available_wans.index(wan_idx) + 1) % len(available_wans)]
                        retry_queue.put((batch_index, batch_ts_codes, new_wan_idx, retry_count + 1))
                    else:
                        # 超过最大重试次数，直接报告失败
                        result_queue.put((batch_index, False, 0, 0, 0, 0, wan_idx))
                        return
                
                wan_idx, port = wan_info
                batch_success = False
                total_records = 0
                total_inserted = 0
                total_updated = 0
                total_skipped = 0
                
                try:
                    for code in batch_ts_codes:
                        try:
                            # 使用WAN接口获取数据
                            df = self.fetch_moneyflow_by_code_with_wan(code, start_date, end_date, wan_info)
                            if not df.empty:
                                # 立即保存数据
                                result = self.store_data(df)
                                
                                total_records += len(df)
                                total_inserted += result.get("inserted", 0)
                                total_updated += result.get("updated", 0)
                                total_skipped += result.get("skipped", 0)
                                
                                batch_success = True
                                with log_lock:
                                    logger.info(f"成功获取股票 {code} 的每日指标数据，共 {len(df)} 条记录")
                            else:
                                with log_lock:
                                    logger.warning(f"WAN {wan_idx} 获取股票 {code} 数据为空")
                                
                            # 短暂休眠，避免API调用过于频繁
                            time.sleep(1)
                        except Exception as e:
                            with log_lock:
                                logger.error(f"WAN {wan_idx} 获取股票 {code} 数据失败: {str(e)}")
                            # 单个股票失败，继续处理下一个股票
                            
                    # 报告结果
                    result_queue.put((batch_index, batch_success, total_records, total_inserted, total_updated, total_skipped, wan_idx))
                    
                except Exception as e:
                    with log_lock:
                        logger.error(f"WAN {wan_idx} 批次处理异常: {str(e)}")
                    # 如果批次获取失败且还有重试次数，将任务放入重试队列
                    if retry_count < MAX_RETRY:
                        with log_lock:
                            logger.warning(f"WAN {wan_idx} 批次 {batch_index+1} 处理异常，将在 {retry_count+1} 秒后重试 ({retry_count+1}/{MAX_RETRY})")
                        
                        # 等待一段时间后再重试，时间随重试次数增加
                        time.sleep(retry_count + 1)
                        
                        # 选择一个不同的WAN接口
                        new_wan_idx = available_wans[(available_wans.index(wan_idx) + 1) % len(available_wans)]
                        retry_queue.put((batch_index, batch_ts_codes, new_wan_idx, retry_count + 1))
                    else:
                        # 超过最大重试次数，直接报告失败
                        result_queue.put((batch_index, False, 0, 0, 0, 0, wan_idx))
                finally:
                    # 确保在处理完成或出错时都释放端口
                    if wan_info:
                        # 等待一小段时间确保端口完全释放
                        time.sleep(1)
                        self.port_allocator.release_port(wan_idx, port)
                
                # 如果批次获取失败且还有重试次数，将任务放入重试队列
                if not batch_success and retry_count < MAX_RETRY:
                    with log_lock:
                        logger.warning(f"WAN {wan_idx} 批次 {batch_index+1} 获取失败，将在 {retry_count+1} 秒后重试 ({retry_count+1}/{MAX_RETRY})")
                    
                    # 等待一段时间后再重试，时间随重试次数增加
                    time.sleep(retry_count + 1)
                    
                    # 选择一个不同的WAN接口
                    new_wan_idx = available_wans[(available_wans.index(wan_idx) + 1) % len(available_wans)]
                    retry_queue.put((batch_index, batch_ts_codes, new_wan_idx, retry_count + 1))
                    return
            
            except Exception as e:
                with log_lock:
                    logger.error(f"WAN {wan_idx} 处理批次 {batch_index+1} 失败: {str(e)}")
                import traceback
                logger.debug(f"详细错误信息: {traceback.format_exc()}")
                
                # 如果还有重试次数，将任务放入重试队列
                if retry_count < MAX_RETRY:
                    with log_lock:
                        logger.warning(f"WAN {wan_idx} 批次 {batch_index+1} 处理出错，将在 {retry_count+1} 秒后重试 ({retry_count+1}/{MAX_RETRY})")
                    
                    # 等待一段时间后再重试，时间随重试次数增加
                    time.sleep(retry_count + 1)
                    
                    # 选择一个不同的WAN接口
                    new_wan_idx = available_wans[(available_wans.index(wan_idx) + 1) % len(available_wans)]
                    retry_queue.put((batch_index, batch_ts_codes, new_wan_idx, retry_count + 1))
                else:
                    # 超过最大重试次数，直接报告失败
                    result_queue.put((batch_index, False, 0, 0, 0, 0, wan_idx))
                
                # 确保释放WAN端口
                if 'wan_info' in locals() and wan_info:
                    wan_idx, port = wan_info
                    # 添加短暂延迟，确保端口完全释放
                    time.sleep(1)
                    self.port_allocator.release_port(wan_idx, port)
        
        # 启动处理线程
        start_time_total = time.time()
        processed_batches = 0
        success_batches = 0
        total_records = 0
        total_inserted = 0
        total_updated = 0
        total_skipped = 0
        
        # 控制最大并发线程数
        max_concurrent = min(len(available_wans), 3)  # 降低并发数：每个WAN最多1个线程，总共不超过3个
        
        # 分批处理
        batch_queue = []
        for i in range(total_batches):
            start_idx = i * batch_size
            end_idx = min(start_idx + batch_size, len(ts_codes))
            batch_ts_codes = ts_codes[start_idx:end_idx]
            
            # 选择WAN接口 - 轮询方式
            wan_idx = available_wans[i % len(available_wans)]
            
            batch_queue.append((i, batch_ts_codes, wan_idx, 0))  # 添加重试计数为0
        
        active_threads = 0
        next_batch_index = 0
        
        # 管理线程池和处理结果
        while processed_batches < total_batches:
            # 优先处理重试队列中的任务
            while not retry_queue.empty() and active_threads < max_concurrent:
                batch_idx, batch_codes, wan_idx, retry_count = retry_queue.get()
                
                # 创建线程处理重试批次
                thread = threading.Thread(
                    target=process_batch, 
                    args=(batch_idx, batch_codes, wan_idx, retry_count)
                )
                thread.daemon = True
                thread.start()
                threads.append(thread)
                active_threads += 1
                
                # 短暂等待，避免同时启动过多线程
                time.sleep(1)
            
            # 启动新线程直到达到最大并发数或所有批次已启动
            while active_threads < max_concurrent and next_batch_index < total_batches:
                batch_idx, batch_codes, wan_idx, retry_count = batch_queue[next_batch_index]
                
                # 创建线程处理批次
                thread = threading.Thread(
                    target=process_batch, 
                    args=(batch_idx, batch_codes, wan_idx, retry_count)
                )
                thread.daemon = True
                thread.start()
                threads.append(thread)
                active_threads += 1
                next_batch_index += 1
                
                # 短暂等待，避免同时启动过多线程
                time.sleep(1)
            
            # 等待并处理完成的批次
            try:
                # 设置超时，避免无限等待
                batch_idx, batch_success, records, inserted, updated, skipped, wan_idx = result_queue.get(timeout=60)
                active_threads -= 1
                processed_batches += 1
                
                if batch_success:
                    success_batches += 1
                    total_records += records
                    total_inserted += inserted
                    total_updated += updated
                    total_skipped += skipped
                
                # 更新进度
                elapsed = time.time() - start_time_total
                avg_time_per_batch = elapsed / processed_batches if processed_batches > 0 else 0
                remaining = (total_batches - processed_batches) * avg_time_per_batch
                progress = processed_batches / total_batches * 100
                logger.info(f"批次进度: {processed_batches}/{total_batches} ({progress:.1f}%)，已处理时间: {elapsed:.1f}s，预估剩余: {remaining:.1f}s")
            
            except queue.Empty:
                # 检查是否有死锁情况（所有线程都卡住）
                logger.warning("等待批次完成超时，检查线程状态...")
                active_count = sum(1 for t in threads if t.is_alive())
                logger.warning(f"当前活动线程数: {active_count}/{len(threads)}")
                
                # 如果没有活动线程但队列为空，可能是所有线程都失败了
                if active_count == 0 and active_threads > 0:
                    logger.error("检测到线程异常退出，重置活动线程计数")
                    active_threads = 0
                
                # 避免CPU占用过高
                time.sleep(1)
        
        # 处理完成
        elapsed_total = time.time() - start_time_total
        logger.success(f"并行处理成功获取 {success_batches}/{total_batches} 个批次的每日指标数据，共 {total_records} 条记录，耗时 {elapsed_total:.2f}s")
        logger.info(f"数据统计: 总记录数 {total_records}，新增 {total_inserted}，更新 {total_updated}，跳过 {total_skipped}")
        
        return success_batches > 0
        
    def _fetch_data_batch(self, ts_codes: List[str], start_date: str, end_date: str, batch_size: int = 1) -> bool:
        """
        批量获取多个股票的每日指标数据
        
        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
            batch_size: 每批处理的股票数量
        
        Returns:
            是否成功处理
        """
        if not ts_codes:
            logger.warning("没有股票代码可以查询")
            return False
            
        # 配置参数
        total_batches = (len(ts_codes) + batch_size - 1) // batch_size
        logger.info(f"开始批量获取 {len(ts_codes)} 个股票的每日指标数据，分为 {total_batches} 个批次处理")
        
        # 定义最大重试次数
        MAX_RETRY = 3
        
        # 进度统计变量
        processed_batches = 0
        success_batches = 0
        total_records = 0
        total_inserted = 0
        total_updated = 0
        total_skipped = 0
        start_time_total = time.time()
        
        # 批量处理股票代码
        for i in range(0, len(ts_codes), batch_size):
            # 获取当前批次的股票代码
            batch_ts_codes = ts_codes[i:i+batch_size]
            
            # 处理当前批次
            retry_count = 0
            batch_success = False
            batch_records = 0
            batch_inserted = 0
            batch_updated = 0
            batch_skipped = 0
            
            # 添加重试逻辑
            while retry_count < MAX_RETRY and not batch_success:
                if retry_count > 0:
                    # 重试间隔时间随着重试次数增加
                    retry_wait = retry_count * 2
                    logger.warning(f"批次 {processed_batches+1}/{total_batches} 获取失败，第 {retry_count}/{MAX_RETRY} 次重试，等待 {retry_wait} 秒")
                    time.sleep(retry_wait)
                
                # 批量处理每个股票
                for code in batch_ts_codes:
                    try:
                        # 获取数据
                        df = self.fetch_moneyflow_by_code_with_wan(code, start_date, end_date, wan_info)
                        if not df.empty:
                            # 保存数据
                            result = self.store_data(df)
                            
                            batch_records += len(df)
                            batch_inserted += result.get("inserted", 0)
                            batch_updated += result.get("updated", 0)
                            batch_skipped += result.get("skipped", 0)
                            
                            batch_success = True
                            logger.info(f"成功获取股票 {code} 的每日指标数据，共 {len(df)} 条记录")
                        else:
                            logger.warning(f"获取股票 {code} 数据为空")
                            
                        # 短暂休眠，避免API调用过于频繁
                        time.sleep(1)
                    except Exception as e:
                        logger.error(f"获取股票 {code} 数据失败: {str(e)}")
                        # 单个股票失败，继续处理下一个股票
                
                # 如果至少有一个股票成功，则认为批次成功
                if batch_success:
                    break
                
                retry_count += 1
            
            processed_batches += 1
            
            # 如果成功获取了数据，更新统计信息
            if batch_success:
                success_batches += 1
                total_records += batch_records
                total_inserted += batch_inserted
                total_updated += batch_updated
                total_skipped += batch_skipped
            else:
                logger.warning(f"批次 {processed_batches}/{total_batches} 处理失败，即使经过 {retry_count} 次重试")
            
            # 更新进度
            elapsed = time.time() - start_time_total
            avg_time_per_batch = elapsed / processed_batches if processed_batches > 0 else 0
            remaining = (total_batches - processed_batches) * avg_time_per_batch
            progress = processed_batches / total_batches * 100
            logger.info(f"批次进度: {processed_batches}/{total_batches} ({progress:.1f}%)，已处理时间: {elapsed:.1f}s，预估剩余: {remaining:.1f}s")
            
            # 增加短暂休眠，避免API调用过于频繁
            time.sleep(1)
        
        # 处理完成
        elapsed_total = time.time() - start_time_total
        logger.success(f"批量处理成功获取 {success_batches}/{total_batches} 个批次的每日指标数据，共 {total_records} 条记录，耗时 {elapsed_total:.2f}s")
        logger.info(f"数据统计: 总记录数 {total_records}，新增 {total_inserted}，更新 {total_updated}，跳过 {total_skipped}")
        
        return success_batches > 0
        
    def fetch_moneyflow_by_code_with_wan(self, ts_code: str, start_date: str, end_date: str, wan_info: Tuple[int, int]) -> pd.DataFrame:
        """
        使用WAN接口按股票代码获取日期范围内的个股资金流向数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            wan_info: WAN接口信息(wan_idx, port)
            
        Returns:
            DataFrame形式的个股资金流向数据
        """
        try:
            # 准备请求参数
            params = {
                "ts_code": ts_code,
                "start_date": start_date,
                "end_date": end_date
            }
            
            if hasattr(self, 'verbose') and self.verbose:
                logger.debug(f"使用WAN接口获取股票 {ts_code} 的个股资金流向数据，日期范围: {start_date} - {end_date}")
            
            # 准备API参数
            api_name = self.api_name
            fields = self.interface_config.get("available_fields", [])
            
            if not wan_info:
                logger.warning("未提供WAN接口信息，使用普通客户端获取数据")
                return self.fetch_moneyflow_by_code(ts_code, start_date, end_date)
            
            # 解包WAN信息
            wan_idx, port = wan_info
            
            # 创建WAN专用客户端
            # 使用self.token和self.api_url而非来自ts_client的属性
            client = TushareClientWAN(token=self.token, timeout=120, api_url=self.api_url)
            client.set_local_address('0.0.0.0', port)
            
            # 使用偏移量处理数据超限
            all_data = []
            offset = 0
            max_count = 10000  # 每次请求的最大记录数
            
            while True:
                # 复制参数并添加分页参数
                current_params = params.copy()
                current_params["offset"] = offset
                current_params["limit"] = max_count
                
                # 获取数据 - 添加重试逻辑
                retry_attempts = 0
                max_retries = 5
                success = False
                df = None
                
                while not success and retry_attempts < max_retries:
                    try:
                        # 使用WAN绑定的客户端
                        df = client.get_data(
                            api_name=api_name,
                            params=current_params,
                            fields=fields
                        )
                        
                        if df is not None:
                            success = True
                        else:
                            retry_attempts += 1
                            logger.warning(f"WAN {wan_idx} 获取股票 {ts_code} 数据失败，重试 ({retry_attempts}/{max_retries})，等待 {retry_attempts*30}秒")
                            time.sleep(retry_attempts*30)  # 递增等待时间
                    except Exception as e:
                        retry_attempts += 1
                        logger.warning(f"WAN {wan_idx} 获取股票 {ts_code} 数据异常: {str(e)}，重试 ({retry_attempts}/{max_retries})，等待 {retry_attempts*30}秒")
                        time.sleep(retry_attempts*30)
                
                if not success:
                    if offset == 0:
                        logger.warning(f"WAN {wan_idx} 获取股票 {ts_code} 数据失败")
                        return pd.DataFrame()
                    else:
                        # 部分数据已获取，继续处理
                        break
                
                if df.empty:
                    if offset == 0:
                        logger.warning(f"WAN {wan_idx} 获取股票 {ts_code} 未获取到数据")
                        return pd.DataFrame()
                    else:
                        # 已经获取了一部分数据，当前批次为空表示数据已获取完毕
                        break
                
                # 添加到结果列表
                all_data.append(df)
                records_count = len(df)
                
                if hasattr(self, 'verbose') and self.verbose:
                    logger.debug(f"WAN {wan_idx} 获取到股票 {ts_code} 的 {records_count} 条记录，偏移量: {offset}")
                
                # 如果返回的数据量等于最大请求数量，可能还有更多数据
                if records_count == max_count:
                    logger.debug(f"WAN {wan_idx} 返回数据量达到单次请求上限，继续获取下一批数据")
                # 如果返回的数据量小于请求的数量，说明已经没有更多数据
                if records_count < max_count:
                    break
                    
                # 设置下一批次的偏移量
                offset += max_count
                
                # 短暂休眠，避免过于频繁的请求
                time.sleep(1)
            
            # 合并所有数据
            if not all_data:
                return pd.DataFrame()
            
            result_df = pd.concat(all_data, ignore_index=True)
            total_records = len(result_df)
            
            logger.debug(f"WAN {wan_idx} 成功获取股票 {ts_code} 的每日指标数据，共 {total_records} 条记录")
            return result_df
            
        except Exception as e:
            logger.error(f"WAN接口获取股票 {ts_code} 的每日指标数据失败: {str(e)}")
            if hasattr(self, 'verbose') and self.verbose:
                import traceback
                logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return pd.DataFrame()
            
    def _socket_info_from_wan(self, wan_info: Tuple[int, int]) -> Tuple[str, int]:
        """
        从WAN接口信息创建套接字信息
        
        Args:
            wan_info: WAN接口信息(wan_idx, port)
            
        Returns:
            (bind_ip, port) 元组
        """
        wan_idx, port = wan_info
        # 直接使用0.0.0.0作为绑定地址，表示所有接口
        return ("0.0.0.0", port)

    def _get_trade_calendar(self, start_date: str, end_date: str) -> List[str]:
        """
        生成指定日期范围内的工作日（周一至周五）作为交易日期
        由于MongoDB连接问题，使用这种替代方案
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            
        Returns:
            交易日期列表，格式为YYYYMMDD
        """
        logger.info(f"为日期范围 {start_date} 至 {end_date} 生成工作日作为交易日期")
        
        try:
            # 将字符串日期转换为datetime对象
            start_date_obj = datetime.strptime(start_date, '%Y%m%d')
            end_date_obj = datetime.strptime(end_date, '%Y%m%d')
            
            # 生成日期列表，仅包含工作日（周一至周五）
            trade_dates = []
            current_date = start_date_obj
            while current_date <= end_date_obj:
                # weekday()返回0-6，其中0=周一，4=周五，5=周六，6=周日
                if current_date.weekday() < 5:  # 过滤周末
                    trade_dates.append(current_date.strftime('%Y%m%d'))
                current_date += timedelta(days=1)
            
            logger.info(f"生成了 {len(trade_dates)} 个工作日日期")
            return trade_dates
            
        except Exception as e:
            logger.error(f"生成交易日期时出错: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return []

    def _get_wan_socket(self, wan_idx: int = None) -> Optional[Tuple[int, int]]:
        """
        获取WAN接口和端口
        
        Args:
            wan_idx: WAN接口索引，如果为None则自动选择
            
        Returns:
            (wan_idx, port)元组，如果无法获取则返回None
        """
        if not self.port_allocator:
            logger.warning("未配置端口分配器，无法获取WAN端口")
            return None
            
        try:
            # 获取可用的WAN接口索引
            available_indices = self.port_allocator.get_available_wan_indices()
            if not available_indices:
                logger.warning("没有可用的WAN接口")
                return None
                
            # 如果指定了WAN索引，检查是否可用
            if wan_idx is not None:
                if wan_idx not in available_indices:
                    logger.warning(f"指定的WAN {wan_idx} 不可用，尝试自动选择")
                    wan_idx = None
                    
            # 如果未指定或指定的不可用，自动选择一个WAN接口
            if wan_idx is None:
                # 轮询选择一个WAN接口
                wan_idx = available_indices[0]  # 简单起见，选择第一个
                
            # 分配端口
            retry_count = 10
            port = None
            
            while retry_count > 0 and port is None:
                port = self.port_allocator.allocate_port(wan_idx)
                if port:
                    logger.debug(f"成功分配WAN接口 {wan_idx}，本地端口 {port}")
                    return (wan_idx, port)
                else:
                    logger.warning(f"WAN {wan_idx} 没有可用端口，重试 {retry_count}")
                    retry_count -= 1
                    time.sleep(1)  # 等待0.5秒再重试
            
            if port is None:
                logger.warning(f"WAN {wan_idx} 经过多次尝试仍没有可用端口")
                return None
                
            return (wan_idx, port)
            
        except Exception as e:
            logger.error(f"获取WAN {wan_idx} 端口时出错: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return None

    def fetch_moneyflow_by_date_with_wan(self, trade_date: str, wan_info: Tuple[int, int]) -> pd.DataFrame:
        """
        使用WAN接口按交易日期获取个股资金流向数据
        
        Args:
            trade_date: 交易日期，格式为YYYYMMDD
            wan_info: WAN接口信息(wan_idx, port)
            
        Returns:
            DataFrame形式的个股资金流向数据
        """
        try:
            # 准备请求参数
            params = {
                "trade_date": trade_date
            }
            
            if hasattr(self, 'verbose') and self.verbose:
                logger.debug(f"使用WAN接口获取交易日期 {trade_date} 的个股资金流向数据")
            
            # 准备API参数
            api_name = self.api_name
            fields = self.interface_config.get("available_fields", [])
            
            if not wan_info:
                logger.warning("未提供WAN接口信息，使用普通客户端获取数据")
                return self.fetch_moneyflow_by_date(trade_date)
            
            # 解包WAN信息
            wan_idx, port = wan_info
            
            # 创建WAN专用客户端
            # 使用self.token和self.api_url而非来自ts_client的属性
            client = TushareClientWAN(token=self.token, timeout=120, api_url=self.api_url)
            client.set_local_address('0.0.0.0', port)
            
            # 使用偏移量处理数据超限
            all_data = []
            offset = 0
            max_count = 10000  # 每次请求的最大记录数
            
            while True:
                # 复制参数并添加分页参数
                current_params = params.copy()
                current_params["offset"] = offset
                current_params["limit"] = max_count
                
                # 获取数据 - 添加重试逻辑
                retry_attempts = 0
                max_retries = 5
                success = False
                df = None
                
                while not success and retry_attempts < max_retries:
                    try:
                        # 使用WAN绑定的客户端
                        df = client.get_data(
                            api_name=api_name,
                            params=current_params,
                            fields=fields
                        )
                        
                        if df is not None:
                            success = True
                        else:
                            retry_attempts += 1
                            logger.warning(f"WAN {wan_idx} 获取交易日期 {trade_date} 数据失败，重试 ({retry_attempts}/{max_retries})，等待 {retry_attempts*30}秒")
                            time.sleep(retry_attempts*30)  # 递增等待时间
                    except Exception as e:
                        retry_attempts += 1
                        logger.warning(f"WAN {wan_idx} 获取交易日期 {trade_date} 数据异常: {str(e)}，重试 ({retry_attempts}/{max_retries})，等待 {retry_attempts*30}秒")
                        time.sleep(retry_attempts*30)
                
                if not success:
                    if offset == 0:
                        logger.warning(f"WAN {wan_idx} 获取交易日期 {trade_date} 数据失败")
                        return pd.DataFrame()
                    else:
                        # 部分数据已获取，继续处理
                        break
                
                if df.empty:
                    if offset == 0:
                        logger.warning(f"WAN {wan_idx} 获取交易日期 {trade_date} 未获取到数据")
                        return pd.DataFrame()
                    else:
                        # 已经获取了一部分数据，当前批次为空表示数据已获取完毕
                        break
                
                # 添加到结果列表
                all_data.append(df)
                records_count = len(df)
                
                if hasattr(self, 'verbose') and self.verbose:
                    logger.debug(f"WAN {wan_idx} 获取到交易日期 {trade_date} 的 {records_count} 条记录，偏移量: {offset}")
                
                # 如果返回的数据量等于最大请求数量，可能还有更多数据
                if records_count == max_count:
                    logger.debug(f"WAN {wan_idx} 返回数据量达到单次请求上限，继续获取下一批数据")
                # 如果返回的数据量小于请求的数量，说明已经没有更多数据
                if records_count < max_count:
                    break
                    
                # 设置下一批次的偏移量
                offset += max_count
                
                # 短暂休眠，避免过于频繁的请求
                time.sleep(1)
            
            # 合并所有数据
            if not all_data:
                return pd.DataFrame()
            
            result_df = pd.concat(all_data, ignore_index=True)
            total_records = len(result_df)
            
            # 筛选目标板块的股票
            if hasattr(self, 'target_market_codes') and self.target_market_codes:
                # 提取股票代码的前两位来匹配板块
                result_df['market_code'] = result_df['ts_code'].str[:2]
                filtered_df = result_df[result_df['market_code'].isin(self.target_market_codes)]
                
                # 删除临时添加的market_code列
                if 'market_code' in filtered_df.columns:
                    filtered_df = filtered_df.drop('market_code', axis=1)
                    
                filtered_records = len(filtered_df)
                if filtered_records < total_records:
                    logger.info(f"按目标板块 {self.target_market_codes} 过滤后，记录数从 {total_records} 减少到 {filtered_records}")
                result_df = filtered_df
            
            logger.debug(f"WAN {wan_idx} 成功获取交易日期 {trade_date} 的每日指标数据，共 {len(result_df)} 条记录")
            return result_df
            
        except Exception as e:
            logger.error(f"WAN接口获取交易日期 {trade_date} 的每日指标数据失败: {str(e)}")
            if hasattr(self, 'verbose') and self.verbose:
                import traceback
                logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return pd.DataFrame()

    def _init_client(self) -> TushareClient:
        """初始化Tushare客户端"""
        try:
            # 从配置中获取token和api_url
            tushare_config = self.config.get("tushare", {})
            token = tushare_config.get("token", "")
            api_url = tushare_config.get("api_url", "")
            
            if not token:
                logger.error("未配置Tushare API Token")
                sys.exit(1)
                
            # 记录token和api_url信息
            mask_token = token[:4] + '*' * (len(token) - 8) + token[-4:] if len(token) > 8 else '***'
            logger.debug(f"使用token初始化客户端: {mask_token} (长度: {len(token)}), API URL: {api_url}")
                
            # 创建并返回客户端实例，传递api_url
            return TushareClient(token=token, api_url=api_url)
        except Exception as e:
            logger.error(f"初始化Tushare客户端失败: {str(e)}")
            sys.exit(1)
            
    def _init_mongodb_client(self, db_config: Dict) -> MongoClient:
        """初始化MongoDB客户端"""
        try:
            # 检查配置项
            host = db_config.get("host", "localhost")
            port = db_config.get("port", 27017)
            username = db_config.get("username", "")
            password = db_config.get("password", "")
            auth_db = db_config.get("auth_source", "admin")
            
            # 构建连接URI
            if username and password:
                uri = f"mongodb://{username}:{password}@{host}:{port}/{auth_db}"
            else:
                uri = f"mongodb://{host}:{port}/"
                
            logger.debug(f"连接MongoDB: {host}:{port}")
            
            # 创建客户端实例
            client = MongoClient(
                uri,
                serverSelectionTimeoutMS=5000,  # 5秒超时
                connectTimeoutMS=5000,
                socketTimeoutMS=30000,  # 操作超时时间延长到30秒
            )
            
            # 测试连接
            client.admin.command('ping')
            logger.debug("MongoDB连接成功")
            
            return client
        except Exception as e:
            logger.error(f"初始化MongoDB客户端失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            sys.exit(1)


if __name__ == "__main__":
    import argparse
    
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description="个股资金流向数据获取器 - 从湘财Tushare获取个股资金流向数据并存储到MongoDB")
    
    # 添加命令行参数
    parser.add_argument("--full", action="store_true", help="获取完整历史数据(从19900101至今)")
    parser.add_argument("--recent", action="store_true", help="获取最近7天数据(默认模式)")
    parser.add_argument("--start-date", type=str, help="指定开始日期(YYYYMMDD格式)")
    parser.add_argument("--end-date", type=str, help="指定结束日期(YYYYMMDD格式)")
    parser.add_argument("--verbose", action="store_true", help="显示详细日志")
    parser.add_argument("--mock", action="store_true", help="使用模拟数据模式")
    parser.add_argument("--batch-size", type=int, default=1, help="每批处理的股票数量，默认为1")
    parser.add_argument("--no-parallel", dest="use_parallel", action="store_false", help="不使用并行处理")
    parser.set_defaults(use_parallel=True)
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 初始化获取器
    fetcher = MoneyflowFetcher(verbose=args.verbose)
    
    # 根据命令行参数执行不同的操作
    if args.full:
        # 获取完整历史数据
        logger.info("尝试获取完整历史数据...")
        
        # 首先测试直接从tushare_data数据库获取
        try:
            mongo_config = fetcher.config.get("mongodb", {})
            client = MongoClient(
                host=mongo_config.get("host", "localhost"),
                port=int(mongo_config.get("port", 27017)),
                username=mongo_config.get("username", ""),
                password=mongo_config.get("password", ""),
                authSource=mongo_config.get("auth_source", "admin")
            )
            
            # 直接检查tushare_data.stock_basic集合
            tushare_db = client["tushare_data"]
            if "stock_basic" in tushare_db.list_collection_names():
                logger.info("找到tushare_data.stock_basic集合，尝试获取股票代码")
                stock_basic = tushare_db.stock_basic
                
                # 构建查询条件：symbol前两位在target_market_codes中
                query_conditions = []
                for market_code in fetcher.target_market_codes:
                    # 使用正则表达式匹配symbol前两位
                    query_conditions.append({"symbol": {"$regex": f"^{market_code}"}})
                    
                # 使用$or操作符组合多个条件
                query = {"$or": query_conditions} if query_conditions else {}
                
                # 只查询ts_code字段
                result = stock_basic.find(query, {"ts_code": 1, "_id": 0})
                
                # 提取ts_code集合
                ts_codes = set()
                for doc in result:
                    if "ts_code" in doc:
                        ts_codes.add(doc["ts_code"])
                
                if ts_codes:
                    logger.info(f"直接从tushare_data.stock_basic获取到 {len(ts_codes)} 个目标市场股票代码")
                    # 使用获取到的股票代码运行
                    fetcher.fetch_full_history(batch_size=args.batch_size, use_parallel=args.use_parallel)
                else:
                    logger.error("tushare_data.stock_basic中未找到目标市场股票代码")
            else:
                logger.warning("未找到tushare_data.stock_basic集合，回退到原始方法")
                # 回退到原始方法
                target_ts_codes = fetcher.get_target_ts_codes_from_stock_basic()
                logger.info(f"获取到 {len(target_ts_codes)} 个目标股票代码")
                
                if target_ts_codes:
                    fetcher.fetch_full_history(batch_size=args.batch_size, use_parallel=args.use_parallel)
                else:
                    logger.error("未能获取目标股票代码，无法进行完整历史数据获取")
        except Exception as e:
            logger.error(f"直接检查tushare_data.stock_basic时出错: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            
            # 回退到原始方法
            target_ts_codes = fetcher.get_target_ts_codes_from_stock_basic()
            logger.info(f"获取到 {len(target_ts_codes)} 个目标股票代码")
            
            if target_ts_codes:
                fetcher.fetch_full_history(batch_size=args.batch_size, use_parallel=args.use_parallel)
            else:
                logger.error("未能获取目标股票代码，无法进行完整历史数据获取")
    elif args.start_date and args.end_date:
        # 获取指定日期范围的数据
        fetcher.fetch_moneyflow_data_by_date_range(args.start_date, args.end_date, batch_size=args.batch_size)
    else:
        # 默认获取最近7天的数据
        fetcher.fetch_recent_data(use_parallel=args.use_parallel, batch_size=args.batch_size)
    
    logger.success("个股资金流向数据获取器执行完毕")
