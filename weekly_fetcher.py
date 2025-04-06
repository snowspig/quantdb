#!/usr/bin/env python
"""
周线行情数据获取器 - 获取周线行情数据并保存到MongoDB

该脚本用于从湘财Tushare获取周线行情数据，并保存到MongoDB数据库中
该版本通过分时间段获取和多WAN接口并行处理，解决大量数据获取问题

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=10144

使用方法：
    python weekly_fetcher.py              # 默认使用recent模式获取最近一个月的数据更新，每周五的数据
    python weekly_fetcher.py --full        # 获取完整历史数据而非默认的最近一个月数据
    python weekly_fetcher.py --verbose     # 使用湘财真实API数据，详细日志模式
    python weekly_fetcher.py --mock        # 使用模拟数据模式（API不可用时）
    python weekly_fetcher.py --start-date 20100101 --end-date 20201231  # 指定日期范围获取数据
    python weekly_fetcher.py --recent      # 显式指定recent模式（最近一个月数据更新，每周五的数据，默认模式）
"""
import os
import sys
import json
import yaml
import time
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
            logger.error(f"获取API数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return None


class SourceAddressAdapter(requests.adapters.HTTPAdapter):
    """用于设置源地址的HTTP适配器"""
    
    def __init__(self, source_address, **kwargs):
        self.source_address = source_address
        super(SourceAddressAdapter, self).__init__(**kwargs)
    
    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs['source_address'] = self.source_address
        super(SourceAddressAdapter, self).init_poolmanager(
            connections, maxsize, block, **pool_kwargs)

class WeeklyFetcher:
    """
    周线行情数据获取器
    
    该类用于从Tushare获取周线行情数据并保存到MongoDB数据库
    优化点：
    1. 支持按时间段分批获取数据，避免一次获取超过10000条数据限制
    2. 多WAN接口并行获取，提高数据获取效率
    3. 增加数据获取重试机制，提高稳定性
    4. 支持recent模式、full模式以及指定日期范围模式
    5. 只保存00、30、60、68四个板块的股票数据
    """

    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "weekly.json",
        target_market_codes: Set[str] = {"00", "30", "60", "68"},  # 默认只保存00 30 60 68四个板块的股票数据
        db_name: str = None,
        collection_name: str = "weekly",
        verbose: bool = False,
        max_workers: int = 3,  # 并行工作线程数
        retry_count: int = 10,  # 数据获取重试次数
        retry_delay: int = 5,  # 重试延迟时间(秒)
        batch_size: int = 10000  # 每批次获取数据的最大数量，防止超过API限制
    ):
        """
        初始化周线行情数据获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            target_market_codes: 目标市场代码集合，只保存这些板块的股票数据
            db_name: MongoDB数据库名称，如果为None则从配置文件中读取
            collection_name: MongoDB集合名称
            verbose: 是否输出详细日志
            max_workers: 并行工作线程数
            retry_count: 数据获取重试次数
            retry_delay: 重试延迟时间(秒)
            batch_size: 每批获取的最大记录数
        """
        self.config_path = config_path
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.target_market_codes = target_market_codes
        self.collection_name = collection_name
        self.verbose = verbose
        self.max_workers = max_workers
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.batch_size = batch_size

        # 设置日志级别
        log_level = "DEBUG" if verbose else "INFO"
        logger.remove()
        logger.add(sys.stderr, level=log_level, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")

        # 加载配置
        self.config = self._load_config()
        self.interface_config = self._load_interface_config()
        
        # 获取token和api_url - 从配置文件读取
        tushare_config = self.config.get("tushare", {})
        self.token = tushare_config.get("token")
        self.api_url = tushare_config.get("api_url")
        
        if not self.token or not self.api_url:
            logger.error("未配置Tushare API token或API URL，请检查配置文件")
            raise ValueError("未配置Tushare API token或API URL")
        
        # 从配置获取MongoDB数据库名称
        if db_name is None:
            mongodb_config = self.config.get("mongodb", {})
            self.db_name = mongodb_config.get("db_name")
            if not self.db_name:
                logger.warning("未在配置文件中找到MongoDB数据库名称，使用默认名称'tushare_data'")
                self.db_name = "tushare_data"
        else:
            self.db_name = db_name
        
        logger.info(f"MongoDB配置: 数据库名={self.db_name}, 集合名={self.collection_name}")
        logger.info(f"目标市场代码: {', '.join(self.target_market_codes)}")
        
        # 初始化Tushare客户端
        self.ts_client = TushareClient(token=self.token, api_url=self.api_url)
        
        # 初始化MongoDB客户端
        self.mongo_client = MongoDBClient(
            host=self.config.get("mongodb", {}).get("host"),
            port=self.config.get("mongodb", {}).get("port"),
            username=self.config.get("mongodb", {}).get("username"),
            password=self.config.get("mongodb", {}).get("password"),
            auth_source=self.config.get("mongodb", {}).get("auth_source"),
            auth_mechanism=self.config.get("mongodb", {}).get("auth_mechanism")
        )
        
        # 初始化端口分配器
        wan_config = self.config.get("wan", {})
        if wan_config.get("enabled", False):
            # 使用现有的全局端口分配器
            from wan_manager.port_allocator import port_allocator
            self.port_allocator = port_allocator
            wan_count = len(self.port_allocator.get_available_wan_indices())
            logger.info(f"已启用多WAN接口支持，WAN接口数量: {wan_count}")
        else:
            self.port_allocator = None
            logger.debug("未启用多WAN接口支持")

    def _load_config(self) -> Dict:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                logger.debug(f"成功加载配置文件: {self.config_path}")
                return config
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            return {}
            
    def _load_interface_config(self) -> Dict:
        """加载接口配置文件"""
        try:
            interface_path = os.path.join(self.interface_dir, self.interface_name)
            if not os.path.exists(interface_path):
                logger.error(f"接口配置文件不存在: {interface_path}")
                return {}
                
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

    def get_target_ts_codes_from_stock_basic(self) -> Set[str]:
        """
        从stock_basic集合中获取目标板块的股票代码
        
        Returns:
            目标板块股票代码集合
        """
        try:
            # 确保MongoDB连接
            if not hasattr(self.mongo_client, 'connect') or not self.mongo_client.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if hasattr(self.mongo_client, 'connect') and not self.mongo_client.connect():
                    logger.error("连接MongoDB失败")
                    return set()
                
            # 查询stock_basic集合中符合条件的股票代码
            logger.info(f"从stock_basic集合查询目标板块 {self.target_market_codes} 的股票代码")
            
            # 构建查询条件：symbol前两位在target_market_codes中
            query_conditions = []
            for market_code in self.target_market_codes:
                # 使用正则表达式匹配symbol前两位
                query_conditions.append({"symbol": {"$regex": f"^{market_code}"}})
                
            # 使用$or操作符组合多个条件
            query = {"$or": query_conditions} if query_conditions else {}
            
            # 只查询ts_code字段
            db = self.mongo_client.get_db(self.db_name)
            collection = db["stock_basic"]
            result = collection.find(query, {"ts_code": 1, "_id": 0})
            
            # 提取ts_code集合
            ts_codes = set()
            for doc in result:
                if "ts_code" in doc:
                    ts_codes.add(doc["ts_code"])
            
            logger.success(f"从stock_basic集合获取到 {len(ts_codes)} 个目标股票代码")
            
            # 输出详细日志
            if self.verbose:
                sample_codes = list(ts_codes)[:5] if ts_codes else []
                logger.debug(f"样例股票代码: {sample_codes}")
                
            return ts_codes
            
        except Exception as e:
            logger.error(f"查询stock_basic集合失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return set()
            
    def filter_weekly_data(self, df: pd.DataFrame, target_ts_codes: Set[str]) -> pd.DataFrame:
        """
        根据目标股票代码集合过滤周线数据
        
        Args:
            df: 周线数据
            target_ts_codes: 目标股票代码集合
        
        Returns:
            过滤后的数据
        """
        if df is None or df.empty:
            logger.warning("没有周线数据可过滤")
            return pd.DataFrame()
        
        logger.info(f"过滤前周线数据数量: {len(df)}")
        
        # 确保ts_code列存在
        if 'ts_code' not in df.columns:
            logger.error("数据中没有ts_code列，无法按股票代码过滤")
            return df
        
        # 过滤数据
        df_filtered = df[df['ts_code'].isin(target_ts_codes)].copy()
        
        # 输出过滤统计信息
        logger.info(f"过滤后周线数据数量: {len(df_filtered)}")
        
        # 详细统计信息
        if self.verbose:
            # 统计各市场的股票数量
            if not df_filtered.empty and 'ts_code' in df_filtered.columns:
                # 从ts_code提取市场代码
                df_filtered['market_code'] = df_filtered['ts_code'].str[:6].str[:2]
                market_stats = df_filtered['market_code'].value_counts().to_dict()
                
                logger.debug("过滤后各市场代码分布:")
                for code, count in sorted(market_stats.items()):
                    logger.debug(f"  {code}: {count} 记录")
        
        return df_filtered

    async def _get_stock_list(self) -> List[str]:
        """
        获取股票代码列表
        """
        df = await self.ts_client.query('stock_basic', exchange='', list_status='L', fields=['ts_code'])
        if df.empty:
            logger.error("获取股票列表失败")
            return []
        return df['ts_code'].tolist()

    def _get_trade_cal(self, start_date: str, end_date: str) -> List[str]:
        """
        从MongoDB中获取指定日期范围内的交易日历
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            
        Returns:
            交易日期列表，格式为YYYYMMDD
        """
        try:
            # 确保MongoDB连接
            if not hasattr(self.mongo_client, 'is_connected') or not self.mongo_client.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if hasattr(self.mongo_client, 'connect') and not self.mongo_client.connect():
                    logger.error("连接MongoDB失败")
                    return []
                    
            # 连接MongoDB
            db = self.mongo_client.get_db(self.db_name)
            
            # 一般交易日历保存在trade_cal集合中
            collection = db["trade_cal"]
            
            # 查询指定日期范围内的交易日
            query = {
                "trade_date": {"$gte": start_date, "$lte": end_date}
                # 注意：trade_cal集合中可能没有is_open字段
            }
            
            # 只获取日期字段
            projection = {"trade_date": 1, "_id": 0}
            
            # 检查集合中是否有数据
            count = collection.count_documents({})
            logger.info(f"交易日历集合中共有 {count} 条记录")
            
            # 记录查询条件
            logger.info(f"查询交易日历: 条件={query}, 投影={projection}")
            
            # 执行查询
            cursor = collection.find(query, projection).sort("trade_date", 1)
            
            # 将结果转换为列表
            trade_dates = [doc["trade_date"] for doc in cursor]
            
            if not trade_dates:
                logger.warning(f"未找到时间范围内的交易日: {start_date}至{end_date}")
            else:
                logger.info(f"查询到 {len(trade_dates)} 个交易日: 从 {trade_dates[0]} 到 {trade_dates[-1]}")
            
            return trade_dates
            
        except Exception as e:
            logger.exception(f"获取交易日历时出错: {e}")
            return []

    def _get_recent_weeks(self, weeks: int = 4) -> Tuple[str, str]:
        """
        获取最近几周的日期范围
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=weeks * 7)
        return (
            start_date.strftime("%Y%m%d"),
            end_date.strftime("%Y%m%d")
        )
    
    def _split_list(self, lst: List, n: int) -> List[List]:
        """
        将列表分割为n个子列表
        """
        return [lst[i:i + n] for i in range(0, len(lst), n)]

    async def _fetch_weekly_by_date(self, date: str) -> pd.DataFrame:
        """
        根据交易日期获取周线数据
        """
        # 实际场景中可能需要使用实际绑定IP地址
        tushare_client = TushareClientWAN(token=self.token)
        df = await tushare_client.get_data(
            'weekly',
            {'trade_date': date},
            self.interface_config.get('available_fields', [])
        )
        return df

    async def _fetch_weekly_by_ts_code(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        根据股票代码获取周线数据
        """
        # 实际场景中可能需要使用实际绑定IP地址
        tushare_client = TushareClientWAN(token=self.token)
        df = await tushare_client.get_data(
            'weekly',
            {'ts_code': ts_code, 'start_date': start_date, 'end_date': end_date},
            self.interface_config.get('available_fields', [])
        )
        return df

    async def _fetch_weekly_by_date_range_for_stock(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取指定股票指定日期范围的周线数据
        """
        return await self._fetch_weekly_by_ts_code(ts_code, start_date, end_date)

    async def _process_stock_batch(self, stock_batch: List[str], start_date: str, end_date: str, 
                               wan_ip: str = None) -> List[Dict]:
        """
        处理一批股票的数据获取
        """
        all_data = []
        for ts_code in stock_batch:
            try:
                # 创建绑定指定WAN的客户端
                tushare_client = TushareClientWAN(token=self.token, local_addr=wan_ip)
                
                # 获取数据
                df = await tushare_client.get_data(
                    'weekly',
                    {'ts_code': ts_code, 'start_date': start_date, 'end_date': end_date},
                    self.interface_config.get('available_fields', [])
                )
                
                if not df.empty:
                    records = df.to_dict('records')
                    all_data.extend(records)
                    logger.info(f"获取股票 {ts_code} 的周线数据: {len(records)} 条记录")
                else:
                    logger.warning(f"股票 {ts_code} 的周线数据为空")
                    
                # API限流控制，每次请求后稍作延迟
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.exception(f"处理股票 {ts_code} 时出错: {e}")
                
        return all_data


    def _mock_weekly_data(self) -> List[Dict]:
        """
        生成模拟周线数据（仅用于测试）
        """
        mock_data = []
        ts_codes = ['000001.SZ', '000002.SZ', '600000.SH', '600036.SH']
        trade_dates = ['20230101', '20230108', '20230115', '20230122']
        
        for ts_code in ts_codes:
            for date in trade_dates:
                # 模拟周线数据结构
                record = {
                    'ts_code': ts_code,
                    'trade_date': date,
                    'open': round(random.uniform(10, 50), 2),
                    'high': round(random.uniform(10, 50), 2),
                    'low': round(random.uniform(10, 50), 2),
                    'close': round(random.uniform(10, 50), 2),
                    'pre_close': round(random.uniform(10, 50), 2),
                    'change': round(random.uniform(-5, 5), 2),
                    'pct_chg': round(random.uniform(-10, 10), 2),
                    'vol': round(random.uniform(1000, 10000), 2),
                    'amount': round(random.uniform(10000, 100000), 2)
                }
                mock_data.append(record)
                
        return mock_data

    def _save_to_mongodb(self, data: List[Dict]) -> bool:
        """
        保存数据到MongoDB
        """
        if not data:
            logger.warning("没有数据需要保存")
            return False
            
        try:
            # 按照composite_index创建一个复合索引，用于防止重复数据
            collection = self.mongo_client.get_collection(self.collection_name)
            collection.create_index(
                [(index_field, pymongo.ASCENDING) for index_field in self.interface_config.get('index_fields', [])],
                unique=True,
                background=True
            )
            
            # 使用bulk_write进行批量插入或更新，提高效率
            operations = []
            for record in data:
                filter_dict = {index_field: record[index_field] 
                                for index_field in self.interface_config.get('index_fields', []) 
                                if index_field in record}
                if filter_dict:
                    operations.append(
                        pymongo.UpdateOne(
                            filter_dict,
                            {'$set': record},
                            upsert=True
                        )
                    )
                    
            if operations:
                result = collection.bulk_write(operations, ordered=False)
                logger.info(f"数据保存成功: 插入 {result.upserted_count} 条, 更新 {result.modified_count} 条")
                return True
            else:
                logger.warning("未生成有效的MongoDB操作")
                return False
        except Exception as e:
            logger.exception(f"保存数据到MongoDB失败: {e}")
            return False


    async def fetch_recent_data(self):
        """
        获取最近几周的数据更新
        """
        start_date, end_date = self._get_recent_weeks(weeks=4)
        logger.info(f"获取最近几周数据: 从 {start_date} 到 {end_date}")
        
        # 获取交易日历
        trade_dates = self._get_trade_cal(start_date, end_date)
        if not trade_dates:
            logger.error("获取交易日历失败，无法继续")
            return False
            
        # 按周进行采集，避免单次请求数据过多
        for date_batch in self._split_list(trade_dates, 5):
            # 每周最后一个交易日当作周数据点
            weekly_date = date_batch[-1]
            logger.info(f"处理周数据日期: {weekly_date}")
            
            if self.mock:
                data = self._mock_weekly_data()
                logger.info(f"模拟周线数据: {len(data)} 条记录")
            else:
                df = await self._fetch_weekly_by_date(weekly_date)
                if df.empty:
                    logger.warning(f"日期 {weekly_date} 的周线数据为空")
                    continue
                    
                data = df.to_dict('records')
                logger.info(f"获取到周线数据: {len(data)} 条记录")
                
            if data:
                self._save_to_mongodb(data)
                
        return True

    async def fetch_full_history(self, batch_size: int = 1, market_codes: List[str] = None):
        """
        获取完整历史周线数据
        """
        # 默认获取从1990年至今的所有周线数据
        start_date = "19900101"
        end_date = datetime.now().strftime("%Y%m%d")
        logger.info(f"获取完整历史周线数据: 从 {start_date} 到 {end_date}")
        
        if self.mock:
            data = self._mock_weekly_data()
            self._save_to_mongodb(data)
            logger.info(f"模拟数据: {len(data)} 条记录")
            return True

            
        # 获取股票列表
        stock_list = await self._get_stock_list()
        if not stock_list:
            logger.error("获取股票列表失败，无法继续")
            return False
            
        logger.info(f"获取到股票列表: {len(stock_list)} 只股票")
        
        # 根据市场代码过滤股票
        if market_codes:
            filtered_stock_list = []
            for ts_code in stock_list:
                # 股票代码格式为: 600000.SH，取前2位为市场代码
                code_prefix = ts_code[:2]
                if code_prefix in market_codes:
                    filtered_stock_list.append(ts_code)
            stock_list = filtered_stock_list
            logger.info(f"过滤后的股票列表: {len(stock_list)} 只股票")
            
        # 按批次处理股票
        stock_batches = self._split_list(stock_list, batch_size)
        logger.info(f"分批处理股票: {len(stock_batches)} 批次，每批 {batch_size} 只股票")
        
        total_records = 0
        success_batches = 0
        
        # 获取可用的WAN接口IP
        wan_ips = self.port_allocator.get_available_ips()
        if not wan_ips and self.use_parallel:
            logger.warning("没有可用的WAN接口IP，使用默认网络连接")
        
        # 并行处理数据获取
        if self.use_parallel and wan_ips:
            logger.info(f"使用并行处理模式，{len(wan_ips)} 个WAN接口")
            
            # 创建工作队列
            task_queue = queue.Queue()
            for i, batch in enumerate(stock_batches):
                task_queue.put((i, batch))
                
            # 创建结果容器
            results = []
                
            # 定义工作线程函数
            async def worker(worker_id, worker_ip):
                worker_records = 0
                while not task_queue.empty():
                    try:
                        batch_id, stock_batch = task_queue.get_nowait()
                    except queue.Empty:
                        break
                        
                    try:
                        logger.info(f"工作线程 {worker_id} (IP: {worker_ip}) 处理批次 {batch_id}: {stock_batch}")
                        batch_data = await self._process_stock_batch(
                            stock_batch=stock_batch,
                            start_date=start_date,
                            end_date=end_date,
                            wan_ip=worker_ip
                        )
                        
                        if batch_data:
                            with self.lock:
                                results.extend(batch_data)
                                worker_records += len(batch_data)
                            
                        task_queue.task_done()
                        
                    except Exception as e:
                        logger.exception(f"工作线程 {worker_id} 处理批次 {batch_id} 时出错: {e}")
                        task_queue.task_done()
                
                return worker_records

                
            # 启动工作线程
            workers = []
            for i, ip in enumerate(wan_ips):
                workers.append(worker(i, ip))
                
            # 等待所有任务完成
            worker_results = await asyncio.gather(*workers)
            
            # 汇总结果
            for worker_records in worker_results:
                total_records += worker_records
                
            # 保存到数据库
            if results:
                success = self._save_to_mongodb(results)
                if success:
                    success_batches += 1
                    
        else:
            logger.info("使用串行处理模式")
            # 串行处理
            for i, batch in enumerate(stock_batches):
                try:
                    logger.info(f"处理批次 {i+1}/{len(stock_batches)}: {batch}")
                    batch_data = await self._process_stock_batch(
                        stock_batch=batch,
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    if batch_data:
                        # 批量保存数据
                        success = self._save_to_mongodb(batch_data)
                        total_records += len(batch_data)
                        
                        if success:
                            success_batches += 1
                            
                except Exception as e:
                    logger.exception(f"处理批次 {i+1} 时出错: {e}")
                    
                # 添加延迟，避免API限流
                await asyncio.sleep(1)
        
        logger.info(f"周线数据获取完成: 总共获取 {total_records} 条记录，成功批次: {success_batches}/{len(stock_batches)}")
        return True

    async def fetch_by_date_range(self, start_date: str, end_date: str, 
                              batch_size: int = 1, market_codes: List[str] = None):
        """
        按日期范围获取周线数据
        """
        logger.info(f"按日期范围获取周线数据: 从 {start_date} 到 {end_date}")
        
        if self.mock:
            data = self._mock_weekly_data()
            self._save_to_mongodb(data)
            logger.info(f"模拟数据: {len(data)} 条记录")
            return True
        
        # 获取股票列表
        stock_list = await self._get_stock_list()
        if not stock_list:
            logger.error("获取股票列表失败，无法继续")
            return False

            
        logger.info(f"获取到股票列表: {len(stock_list)} 只股票")
        
        # 根据市场代码过滤股票
        if market_codes:
            filtered_stock_list = []
            for ts_code in stock_list:
                code_prefix = ts_code[:2]
                if code_prefix in market_codes:
                    filtered_stock_list.append(ts_code)
            stock_list = filtered_stock_list
            logger.info(f"过滤后的股票列表: {len(stock_list)} 只股票")
            
        # 按批次处理股票
        stock_batches = self._split_list(stock_list, batch_size)
        logger.info(f"分批处理股票: {len(stock_batches)} 批次，每批 {batch_size} 只股票")
        
        total_records = 0
        success_batches = 0
        
        # 获取可用的WAN接口IP
        wan_ips = self.port_allocator.get_available_ips()
        if not wan_ips and self.use_parallel:
            logger.warning("没有可用的WAN接口IP，使用默认网络连接")
        
        # 并行处理数据获取
        if self.use_parallel and wan_ips:
            logger.info(f"使用并行处理模式，{len(wan_ips)} 个WAN接口")
            
            # 创建工作队列
            task_queue = queue.Queue()
            for i, batch in enumerate(stock_batches):
                task_queue.put((i, batch))
                
            # 创建结果容器
            results = []
                
            # 定义工作线程函数
            async def worker(worker_id, worker_ip):
                worker_records = 0
                while not task_queue.empty():
                    try:
                        batch_id, stock_batch = task_queue.get_nowait()
                    except queue.Empty:
                        break
                        
                    try:
                        logger.info(f"工作线程 {worker_id} (IP: {worker_ip}) 处理批次 {batch_id}: {stock_batch}")
                        batch_data = await self._process_stock_batch(
                            stock_batch=stock_batch,
                            start_date=start_date,
                            end_date=end_date,
                            wan_ip=worker_ip
                        )
                        
                        if batch_data:
                            with self.lock:
                                results.extend(batch_data)
                                worker_records += len(batch_data)
                            
                        task_queue.task_done()
                        
                    except Exception as e:
                        logger.exception(f"工作线程 {worker_id} 处理批次 {batch_id} 时出错: {e}")
                        task_queue.task_done()
                
                return worker_records
                
            # 启动工作线程
            workers = []
            for i, ip in enumerate(wan_ips):
                workers.append(worker(i, ip))
                
            # 等待所有任务完成
            worker_results = await asyncio.gather(*workers)
            
            # 汇总结果
            for worker_records in worker_results:
                total_records += worker_records
                
            # 保存到数据库
            if results:
                success = self._save_to_mongodb(results)
                if success:
                    success_batches += 1
                    
        else:
            logger.info("使用串行处理模式")
            # 串行处理
            for i, batch in enumerate(stock_batches):
                try:
                    logger.info(f"处理批次 {i+1}/{len(stock_batches)}: {batch}")
                    batch_data = await self._process_stock_batch(
                        stock_batch=batch,
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    if batch_data:
                        # 批量保存数据
                        success = self._save_to_mongodb(batch_data)
                        total_records += len(batch_data)
                        
                        if success:
                            success_batches += 1
                            
                except Exception as e:
                    logger.exception(f"处理批次 {i+1} 时出错: {e}")
                    
                # 添加延迟，避免API限流
                await asyncio.sleep(1)
        
        logger.info(f"周线数据获取完成: 总共获取 {total_records} 条记录，成功批次: {success_batches}/{len(stock_batches)}")
        return True

    def fetch_weekly_by_date(self, trade_date: str, ts_code: str = None) -> pd.DataFrame:
        """
        按日期获取周线行情数据
        
        Args:
            trade_date: 交易日期，格式为YYYYMMDD
            ts_code: 可选，股票代码
            
        Returns:
            DataFrame形式的周线数据
        """
        try:
            params = {"trade_date": trade_date}
            if ts_code:
                params["ts_code"] = ts_code
                
            logger.debug(f"获取日期 {trade_date} 的周线数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
            
            df = self.ts_client.get_data(
                api_name="weekly",
                params=params,
                fields=self.interface_config.get("available_fields", [])
            )
            
            if df.empty:
                logger.warning(f"日期 {trade_date} 未获取到数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
                return pd.DataFrame()
            
            logger.info(f"成功获取日期 {trade_date} 的周线数据"+(f" 股票代码: {ts_code}" if ts_code else f"，共 {len(df)} 条记录"))
            return df
        except Exception as e:
            logger.error(f"获取日期 {trade_date} 的周线数据失败: {str(e)}")
            return pd.DataFrame()
            
    def fetch_weekly_by_code(self, ts_code: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        按股票代码获取周线行情数据
        
        Args:
            ts_code: 股票代码
            start_date: 可选，开始日期，格式为YYYYMMDD
            end_date: 可选，结束日期，格式为YYYYMMDD
            
        Returns:
            DataFrame形式的周线数据
        """
        try:
            params = {"ts_code": ts_code}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
                
            logger.debug(f"获取股票 {ts_code} 的周线数据"+(f" 日期范围: {start_date} 至 {end_date}" if start_date and end_date else ""))
            
            df = self.ts_client.get_data(
                api_name="weekly",
                params=params,
                fields=self.interface_config.get("available_fields", [])
            )
            
            if df.empty:
                logger.warning(f"股票 {ts_code} 未获取到数据"+(f" 日期范围: {start_date} 至 {end_date}" if start_date and end_date else ""))
                return pd.DataFrame()
                
            record_count = len(df)
            
            # 检查返回的数据量是否接近限制，如果是则可能数据不完整
            if record_count >= self.batch_size * 0.9:  # 如果返回的数据量超过批次大小的90%
                logger.warning(f"股票 {ts_code} 返回数据量 {record_count} 接近API限制，数据可能不完整，建议缩小时间范围")
            
            logger.info(f"成功获取股票 {ts_code} 的周线数据，共 {record_count} 条记录")
            return df
        except Exception as e:
            logger.error(f"获取股票 {ts_code} 的周线数据失败: {str(e)}")
            return pd.DataFrame()
    
    def _get_wan_socket(self, wan_idx: int = None) -> Optional[Tuple[int, int]]:
        """
        获取WAN接口和端口
        
        Args:
            wan_idx: 指定WAN接口索引，如果为None则自动选择
            
        Returns:
            (wan_idx, port) 元组，或者None表示失败
        """
        if not self.port_allocator:
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
                    logger.debug(f"使用WAN接口 {wan_idx}，本地端口 {port}")
                    # 添加短暂延迟，确保端口完全释放
                    time.sleep(0.5)  # 增加到500毫秒的延迟，有助于避免端口重用问题
                    return (wan_idx, port)
                else:
                    logger.warning(f"WAN {wan_idx} 没有可用端口，重试 {retry_count}")
                    retry_count -= 1
                    time.sleep(0.5)  # 等待0.5秒再重试
            
            if port is None:
                logger.warning(f"WAN {wan_idx} 经过多次尝试仍没有可用端口")
                return None
            
            return (wan_idx, port)
            
        except Exception as e:
            logger.error(f"获取WAN接口失败: {str(e)}")
            return None

    def _ensure_indexes(self, collection) -> bool:
        """
        确保必要的索引存在
        
        Args:
            collection: MongoDB集合对象
            
        Returns:
            是否需要创建索引
        """
        try:
            # 获取现有索引
            existing_indexes = collection.index_information()
            logger.debug(f"现有索引信息: {existing_indexes}")
            
            # 检查复合唯一索引 (ts_code, trade_date)
            index_name = "ts_code_1_trade_date_1"
            index_created = False
            
            # 检查索引是否存在并且结构正确
            if index_name in existing_indexes:
                # 验证索引的键和属性
                existing_index = existing_indexes[index_name]
                expected_keys = [("ts_code", 1), ("trade_date", 1)]
                
                # 确保是有序的正确键和唯一约束
                keys_match = all(key in expected_keys for key in existing_index['key']) and len(existing_index['key']) == len(expected_keys)
                is_unique = existing_index.get('unique', False)
                
                if keys_match and is_unique:
                    logger.debug(f"复合唯一索引 (ts_code, trade_date) 已存在且结构正确，跳过创建")
                else:
                    # 索引存在但结构不正确，删除并重建
                    logger.info(f"复合唯一索引 (ts_code, trade_date) 存在但结构不正确，删除并重建索引")
                    try:
                        collection.drop_index(index_name)
                        logger.debug(f"成功删除现有索引: {index_name}")
                    except Exception as e:
                        logger.error(f"删除索引时出错: {str(e)}")
                    
                    # 创建正确的索引
                    collection.create_index(
                        [("ts_code", 1), ("trade_date", 1)], 
                        unique=True, 
                        background=True
                    )
                    logger.success(f"已重建复合唯一索引 (ts_code, trade_date)")
                    index_created = True
            else:
                # 索引不存在，创建它
                logger.info(f"正在为集合 {collection.name} 创建复合唯一索引 (ts_code, trade_date)...")
                collection.create_index(
                    [("ts_code", 1), ("trade_date", 1)], 
                    unique=True, 
                    background=True
                )
                logger.success(f"已成功创建复合唯一索引 (ts_code, trade_date)")
                index_created = True
            
            # 检查单字段索引
            for field in ["ts_code", "trade_date"]:
                index_field_name = f"{field}_1"
                if index_field_name not in existing_indexes:
                    logger.info(f"正在为字段 {field} 创建索引...")
                    collection.create_index(field)
                    logger.success(f"已为字段 {field} 创建索引")
                    index_created = True
                else:
                    logger.debug(f"字段 {field} 的索引已存在，跳过创建")
            
            # 确保在创建索引后等待一小段时间，让MongoDB完成索引构建
            if index_created:
                logger.info("索引已创建或修改，等待MongoDB完成索引构建...")
                time.sleep(1.0)  # 等待1秒，让MongoDB完成索引构建
            
            return True
                    
        except Exception as e:
            logger.error(f"创建索引时出错: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

    def save_to_mongodb(self, df: pd.DataFrame) -> bool:
        """
        将周线数据保存到MongoDB
        
        Args:
            df: DataFrame形式的周线数据
            
        Returns:
            是否成功保存数据
        """
        if df is None or df.empty:
            logger.warning("没有数据可保存到MongoDB")
            return False
            
        try:
            start_time = time.time()
            
            # 获取MongoDB集合
            db = self.mongo_client.get_db(self.db_name)
            collection = db[self.collection_name]
            
            # 首先创建索引 - 提前创建索引以提高插入和查询效率
            if not self._ensure_indexes(collection):
                logger.warning("索引创建失败，将尝试继续保存数据")
            
            # 将DataFrame转换为字典列表
            records = df.to_dict('records')
            
            # 批量处理，避免一次性处理太多记录
            batch_size = 5000  # 减小batch_size以减轻MongoDB负担
            total_batches = (len(records) + batch_size - 1) // batch_size
            
            total_inserted = 0
            total_updated = 0
            total_skipped = 0
            
            # 准备批量操作
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                
                # 增加超时时间，避免大批量操作超时
                try:
                    batch_result = self._batch_upsert(collection, batch, ["ts_code", "trade_date"])
                    
                    total_inserted += batch_result["inserted"]
                    total_updated += batch_result["updated"]
                    total_skipped += batch_result["skipped"]
                    
                    # 进度显示
                    if self.verbose and total_batches > 1:
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
                return True
            else:
                if len(records) > 0:
                    logger.warning(f"提交了 {len(records)} 条记录，但MongoDB未报告任何插入、更新或跳过的记录")
                return False
                
        except Exception as e:
            logger.error(f"保存数据到MongoDB失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
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
                
            # 检查是否已处理过相同的记录
            if key_str in existing_records:
                skipped += 1
                continue
                
            # 记录唯一键，准备查询
            existing_records.add(key_str)
            queries.append(query)
            valid_records.append((record, query))
            
        # 第二步：查询哪些记录已经存在
        if not valid_records:
            logger.warning("没有有效记录可以处理")
            return {"inserted": 0, "updated": 0, "skipped": skipped}
            
        # 使用$or查询批量检查记录是否存在
        existing_keys = set()
        try:
            if queries:
                query = {"$or": queries}
                existing_docs = collection.find(query, {"_id": 0, **{k: 1 for k in unique_keys}})
                
                # 记录已存在的记录的唯一键
                for doc in existing_docs:
                    key_values = tuple(doc.get(key) for key in unique_keys)
                    existing_keys.add(key_values)
        except Exception as e:
            logger.error(f"批量查询记录存在性失败: {str(e)}")
            # 如果查询失败，假设所有记录都需要更新
            existing_keys = set()  # 清空集合，后续会执行upsert
            
        # 根据查询结果准备插入和更新操作
        insert_ops = []
        update_ops = []
        
        for record, query in valid_records:
            # 构建唯一键元组
            key_values = tuple(record.get(key) for key in unique_keys)
            
            if key_values in existing_keys:
                # 记录已存在，执行更新
                update_ops.append(
                    pymongo.UpdateOne(
                        query,
                        {"$set": record}
                    )
                )
                updated += 1
            else:
                # 记录不存在，执行插入
                insert_ops.append(pymongo.InsertOne(record))
                inserted += 1
                
        # 分别执行插入和更新操作
        try:
            # 设置合理的WriteConcern参数
            from pymongo import WriteConcern
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
                except pymongo.errors.BulkWriteError as bwe:
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
                                    if self.verbose:
                                        logger.debug(f"插入操作重复键错误: {error.get('errmsg', '')}")
                                        
                    logger.warning(f"插入操作部分失败: {len(bwe.details.get('writeErrors', []))} 错误")
            
            # 执行更新操作
            if update_ops:
                try:
                    update_result = temp_collection.bulk_write(update_ops, ordered=False)
                    real_updated = update_result.modified_count
                    if real_updated != len(update_ops):
                        logger.debug(f"部分记录未被修改，可能数据未变化: 预期={len(update_ops)}, 实际修改={real_updated}")
                except pymongo.errors.BulkWriteError as bwe:
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

    def fetch_data_parallel(self, ts_codes: Set[str], start_date: str, end_date: str, batch_size: int = 10) -> pd.DataFrame:
        """
        使用多WAN口并行获取多个股票的周线行情数据
        
        Args:
            ts_codes: 股票代码集合
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
            batch_size: 每批处理的股票数量，默认为10
            
        Returns:
            所有股票的周线行情数据合并后的DataFrame
        """
        import threading
        import queue
        
        if not ts_codes:
            logger.warning("没有股票代码可以查询")
            return pd.DataFrame()
            
        # 将集合转换为列表，便于分批处理
        ts_codes_list = list(ts_codes)
        
        # 计算批次数
        total_batches = (len(ts_codes_list) + batch_size - 1) // batch_size
        logger.info(f"开始并行批量获取 {len(ts_codes_list)} 个股票的周线行情数据，分为 {total_batches} 个批次处理")
        
        # 获取可用的WAN接口
        available_wans = []
        if self.port_allocator:
            available_wans = self.port_allocator.get_available_wan_indices()
            logger.info(f"可用的WAN接口数量: {len(available_wans)}")
            
        if not available_wans:
            logger.warning("没有可用的WAN接口，将使用系统默认网络接口")
            # 如果没有可用WAN，回退到普通批处理
            return self.fetch_data_batch(ts_codes, start_date, end_date, batch_size)
        
        # 创建结果队列和线程列表
        result_queue = queue.Queue()
        threads = []
        all_data = []
        
        # 速率控制器 - 每个WAN接口一个
        rate_controllers = {wan_idx: {
            "minute_call_count": 0,
            "hour_call_count": 0,
            "minute_start_time": time.time(),
            "hour_start_time": time.time(),
            "minute_rate_limit": 500,  # 每个WAN接口每分钟最大500次调用
            "hour_rate_limit": 4000    # 每个WAN接口每小时最大4000次调用
        } for wan_idx in available_wans}
        
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
                        return
                    else:
                        # 超过最大重试次数，直接报告失败
                        result_queue.put((batch_index, None, wan_idx))
                        return
                
                wan_idx, port = wan_info
                batch_data = []
                fetch_success = False
                
                try:
                    for code in batch_ts_codes:
                        try:
                            # 使用WAN接口获取数据
                            df = self.fetch_weekly_by_code_with_wan(code, start_date, end_date, wan_info)
                            if not df.empty:
                                # 立即保存数据
                                self.save_to_mongodb(df)
                                batch_data.append(df)
                                fetch_success = True
                            else:
                                logger.warning(f"WAN {wan_idx} 获取股票 {code} 数据为空")
                                
                            # 短暂休眠，避免API调用过于频繁
                            time.sleep(0.5)
                        except Exception as e:
                            logger.error(f"WAN {wan_idx} 获取股票 {code} 数据失败: {str(e)}")
                            # 单个股票失败，继续处理下一个股票
                finally:
                    # 确保在处理完成或出错时都释放端口
                    if wan_info:
                        # 等待一小段时间确保端口完全释放
                        time.sleep(0.5)  # 增加到500毫秒的延迟
                        self.port_allocator.release_port(wan_idx, port)
                
                # 合并批次数据
                batch_df = pd.concat(batch_data, ignore_index=True) if batch_data else pd.DataFrame()
                
                # 如果批次获取失败且还有重试次数，将任务放入重试队列
                if not fetch_success and retry_count < MAX_RETRY:
                    with log_lock:
                        logger.warning(f"WAN {wan_idx} 批次 {batch_index+1} 获取失败，将在 {retry_count+1} 秒后重试 ({retry_count+1}/{MAX_RETRY})")
                    
                    # 等待一段时间后再重试，时间随重试次数增加
                    time.sleep(retry_count + 1)
                    
                    # 选择一个不同的WAN接口
                    new_wan_idx = available_wans[(available_wans.index(wan_idx) + 1) % len(available_wans)]
                    retry_queue.put((batch_index, batch_ts_codes, new_wan_idx, retry_count + 1))
                    return
                
                # 记录结果
                with log_lock:
                    if not batch_df.empty:
                        logger.debug(f"WAN {wan_idx} 批次 {batch_index+1} 成功获取 {len(batch_df)} 条记录")
                    else:
                        # 如果重试已达到最大次数，记录最终失败
                        if retry_count >= MAX_RETRY:
                            logger.warning(f"WAN {wan_idx} 批次 {batch_index+1} 在 {MAX_RETRY} 次重试后仍无数据，跳过此批次")
                        else:
                            logger.debug(f"WAN {wan_idx} 批次 {batch_index+1} 无数据")
                        
                # 放入结果队列
                result_queue.put((batch_index, batch_df, wan_idx))
                
                # 增加短暂休眠，避免API调用过于频繁
                time.sleep(0.5)
                
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
                    result_queue.put((batch_index, None, wan_idx))
                
                # 确保释放WAN端口
                if 'wan_info' in locals() and wan_info:
                    wan_idx, port = wan_info
                    # 添加短暂延迟，确保端口完全释放
                    time.sleep(0.5)
                    self.port_allocator.release_port(wan_idx, port)
        
        # 启动处理线程
        start_time_total = time.time()
        processed_batches = 0
        success_batches = 0
        total_records = 0
        all_data_sample = None  # 保存样本数据
        
        # 控制最大并发线程数
        max_concurrent = min(len(available_wans), 3)  # 降低并发数：每个WAN最多1个线程，总共不超过3个
        
        # 分批处理
        batch_queue = []
        for i in range(total_batches):
            start_idx = i * batch_size
            end_idx = min(start_idx + batch_size, len(ts_codes_list))
            batch_ts_codes = ts_codes_list[start_idx:end_idx]
            
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
                time.sleep(0.5)
            
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
                time.sleep(0.5)
            
            # 等待并处理完成的批次
            try:
                # 设置超时，避免无限等待
                batch_idx, batch_df, wan_idx = result_queue.get(timeout=60)
                active_threads -= 1
                processed_batches += 1
                
                if batch_df is not None and not batch_df.empty:
                    success_batches += 1
                    total_records += len(batch_df)
                    
                    # 保存样本数据
                    if all_data_sample is None or len(all_data_sample) < 100:
                        sample_size = min(100 - (0 if all_data_sample is None else len(all_data_sample)), len(batch_df))
                        if all_data_sample is None:
                            all_data_sample = batch_df.head(sample_size)
                        else:
                            all_data_sample = pd.concat([all_data_sample, batch_df.head(sample_size)], ignore_index=True)
                            all_data_sample = all_data_sample.head(100)  # 确保不超过100行
                
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
        logger.success(f"并行处理成功获取 {success_batches}/{total_batches} 个批次的周线行情数据，共 {total_records} 条记录，耗时 {elapsed_total:.2f}s")
        
        return all_data_sample if all_data_sample is not None else pd.DataFrame()
    
    def fetch_data_batch(self, ts_codes: Set[str], start_date: str, end_date: str, batch_size: int = 100, minute_rate_limit: int = 500, hour_rate_limit: int = 4000) -> pd.DataFrame:
        """
        批量获取多个股票的周线行情数据
        
        Args:
            ts_codes: 股票代码集合
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
            batch_size: 每批处理的股票数量
            minute_rate_limit: 每分钟API调用限制
            hour_rate_limit: 每小时API调用限制
        
        Returns:
            所有股票的周线行情数据合并后的DataFrame的样本
        """
        if not ts_codes:
            logger.warning("没有股票代码可以查询")
            return pd.DataFrame()
            
        # 将集合转换为列表，便于分批处理
        ts_codes_list = list(ts_codes)
        
        # 配置参数
        total_batches = (len(ts_codes_list) + batch_size - 1) // batch_size
        logger.info(f"开始批量获取 {len(ts_codes_list)} 个股票的周线行情数据，分为 {total_batches} 个批次处理")
        
        # 定义最大重试次数
        MAX_RETRY = 3
        
        # 进度统计变量
        processed_batches = 0
        success_batches = 0
        total_records = 0
        start_time_total = time.time()
        all_data_sample = None  # 保存样本数据
        
        # 初始化速率控制器
        rate_controllers = {
            "minute_call_count": 0,
            "hour_call_count": 0,
            "minute_start_time": time.time(),
            "hour_start_time": time.time(),
            "minute_rate_limit": minute_rate_limit,
            "hour_rate_limit": hour_rate_limit
        }
        
        # 批量处理股票代码
        for i in range(0, len(ts_codes_list), batch_size):
            # 获取当前批次的股票代码
            batch_ts_codes = ts_codes_list[i:i+batch_size]
            
            # 获取批次数据，添加重试逻辑
            retry_count = 0
            batch_data = None
            is_success = False
            
            while retry_count < MAX_RETRY and not is_success:
                if retry_count > 0:
                    # 重试间隔时间随着重试次数增加
                    retry_wait = retry_count * 2
                    logger.warning(f"批次 {processed_batches+1}/{total_batches} 获取失败，第 {retry_count}/{MAX_RETRY} 次重试，等待 {retry_wait} 秒")
                    time.sleep(retry_wait)
                
                batch_data, rate_controllers, is_success = self._fetch_batch(
                    batch_ts_codes, 
                    start_date, 
                    end_date, 
                    rate_controllers
                )
                
                retry_count += 1
                
                # 如果成功或者已达到最大重试次数，退出循环
                if is_success or retry_count >= MAX_RETRY:
                    break
            
            processed_batches += 1
            
            # 如果成功获取了数据，保存到MongoDB
            if is_success and not batch_data.empty:
                success_batches += 1
                total_records += len(batch_data)
                self.save_to_mongodb(batch_data)
                
                # 保存样本数据
                if all_data_sample is None or len(all_data_sample) < 100:
                    sample_size = min(100 - (0 if all_data_sample is None else len(all_data_sample)), len(batch_data))
                    if all_data_sample is None:
                        all_data_sample = batch_data.head(sample_size)
                    else:
                        all_data_sample = pd.concat([all_data_sample, batch_data.head(sample_size)], ignore_index=True)
                        all_data_sample = all_data_sample.head(100)  # 确保不超过100行
            else:
                logger.warning(f"批次 {processed_batches}/{total_batches} 处理失败，即使经过 {retry_count} 次重试")
            
            # 更新进度
            elapsed = time.time() - start_time_total
            avg_time_per_batch = elapsed / processed_batches if processed_batches > 0 else 0
            remaining = (total_batches - processed_batches) * avg_time_per_batch
            progress = processed_batches / total_batches * 100
            logger.info(f"批次进度: {processed_batches}/{total_batches} ({progress:.1f}%)，已处理时间: {elapsed:.1f}s，预估剩余: {remaining:.1f}s")
            
            # 增加短暂休眠，避免API调用过于频繁，并减轻端口冲突
            time.sleep(0.5)
        
        # 处理完成
        elapsed_total = time.time() - start_time_total
        logger.success(f"批量处理成功获取 {success_batches}/{total_batches} 个批次的周线行情数据，共 {total_records} 条记录，耗时 {elapsed_total:.2f}s")
        
        return all_data_sample if all_data_sample is not None else pd.DataFrame()
    
    def _fetch_batch(
        self, 
        batch_ts_codes: List[str], 
        start_date: str = None, 
        end_date: str = None,
        rate_controllers: Dict[str, Any] = None
    ) -> Tuple[pd.DataFrame, Dict[str, Any], bool]:
        """
        获取单个批次的数据，同时处理速率限制
        
        Args:
            batch_ts_codes: 批次股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            rate_controllers: 速率控制器字典，包含计数器和时间戳
            
        Returns:
            (批次数据DataFrame, 更新后的速率控制器, 是否成功)
        """
        if rate_controllers is None:
            # 初始化速率控制器
            rate_controllers = {
                "minute_call_count": 0,
                "hour_call_count": 0,
                "minute_start_time": time.time(),
                "hour_start_time": time.time(),
                "minute_rate_limit": 500,
                "hour_rate_limit": 4000
            }
            
        # 提取速率控制变量
        minute_call_count = rate_controllers["minute_call_count"]
        hour_call_count = rate_controllers["hour_call_count"]
        minute_start_time = rate_controllers["minute_start_time"]
        hour_start_time = rate_controllers["hour_start_time"]
        minute_rate_limit = rate_controllers["minute_rate_limit"]
        hour_rate_limit = rate_controllers["hour_rate_limit"]
        
        # 双层速率限制控制 - 小时级
        hour_call_count += 1
        hour_elapsed = time.time() - hour_start_time
        
        # 小时级限制控制
        if hour_call_count >= hour_rate_limit:
            # 如果接近小时限制，计算需要等待的时间
            if hour_elapsed < 3600:  # 3600秒 = 1小时
                wait_time = 3600 - hour_elapsed + 5  # 额外5秒作为缓冲
                logger.warning(f"接近API小时调用限制 ({hour_rate_limit}/小时)，等待 {wait_time:.1f} 秒")
                time.sleep(wait_time)
            # 重置计数器
            hour_call_count = 1
            hour_start_time = time.time()
        elif hour_call_count > hour_rate_limit * 0.9:  # 接近90%的限制
            # 计算当前调用频率
            calls_per_hour = hour_call_count / (hour_elapsed / 3600) if hour_elapsed > 0 else 0
            if calls_per_hour > hour_rate_limit:
                # 如果预计会超过限制，主动降低频率
                wait_time = 10  # 降低频率的等待时间
                logger.info(f"API调用频率较高 ({calls_per_hour:.1f}/小时)，主动等待 {wait_time} 秒")
                time.sleep(wait_time)
        
        # 双层速率限制控制 - 分钟级
        minute_call_count += 1
        minute_elapsed = time.time() - minute_start_time
        
        # 分钟级限制控制
        if minute_call_count >= minute_rate_limit:
            # 如果接近分钟限制，计算需要等待的时间
            if minute_elapsed < 60:  # 60秒 = 1分钟
                wait_time = 60 - minute_elapsed + 2  # 额外2秒作为缓冲
                logger.info(f"接近API分钟调用限制 ({minute_rate_limit}/分钟)，等待 {wait_time:.1f} 秒")
                time.sleep(wait_time)
            # 重置计数器
            minute_call_count = 1
            minute_start_time = time.time()
        
        # 获取批次数据
        all_data = []
        for ts_code in batch_ts_codes:
            df = self.fetch_weekly_by_code_with_offset(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if not df.empty:
                all_data.append(df)
        
        # 合并数据
        batch_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
        is_success = not batch_df.empty
        
        # 更新速率控制器
        rate_controllers.update({
            "minute_call_count": minute_call_count,
            "hour_call_count": hour_call_count,
            "minute_start_time": minute_start_time,
            "hour_start_time": hour_start_time
        })
        
        # 短暂休眠以避免API调用过于频繁
        time.sleep(0.5)  # 增加到0.5秒的间隔，防止端口冲突
        
        return batch_df, rate_controllers, is_success

    def get_trade_calendar(self, start_date: str, end_date: str) -> List[str]:
        """
        从mongodb中获取指定日期范围内的交易日历
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            
        Returns:
            交易日期列表，格式为YYYYMMDD
        """
        try:
            # 确保MongoDB连接
            if not hasattr(self.mongo_client, 'is_connected') or not self.mongo_client.is_connected():
                logger.warning("MongoDB未连接，尝试连接...")
                if hasattr(self.mongo_client, 'connect') and not self.mongo_client.connect():
                    logger.error("连接MongoDB失败")
                    return []
                    
            # 连接MongoDB
            db = self.mongo_client.get_db(self.db_name)
            # 一般交易日历保存在trade_cal集合中
            collection = db["trade_cal"]
            
            # 查询交易日期
            query = {
                "cal_date": {"$gte": start_date, "$lte": end_date},
                "is_open": 1  # 1表示交易日，0表示非交易日
            }
            
            # 只查询日期字段
            result = collection.find(query, {"cal_date": 1, "_id": 0}).sort("cal_date", 1)
            
            # 提取日期列表
            trade_dates = [doc["cal_date"] for doc in result]
            
            logger.info(f"从交易日历获取到日期范围 {start_date} 至 {end_date} 内的 {len(trade_dates)} 个交易日")
            
            if not trade_dates:
                logger.warning(f"未从交易日历获取到日期范围 {start_date} 至 {end_date} 内的交易日，将使用日期范围内的所有日期")
                # 生成日期范围内的所有日期
                start_date_obj = datetime.strptime(start_date, '%Y%m%d')
                end_date_obj = datetime.strptime(end_date, '%Y%m%d')
                
                trade_dates = []
                current_date = start_date_obj
                while current_date <= end_date_obj:
                    trade_dates.append(current_date.strftime('%Y%m%d'))
                    current_date += timedelta(days=1)
                
                logger.info(f"生成日期范围内的 {len(trade_dates)} 个日期")
            
            return trade_dates
            
        except Exception as e:
            logger.error(f"获取交易日历失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            
            # 出错时生成日期范围内的所有日期作为备选
            start_date_obj = datetime.strptime(start_date, '%Y%m%d')
            end_date_obj = datetime.strptime(end_date, '%Y%m%d')
            
            trade_dates = []
            current_date = start_date_obj
            while current_date <= end_date_obj:
                trade_dates.append(current_date.strftime('%Y%m%d'))
                current_date += timedelta(days=1)
            
            logger.info(f"生成日期范围内的 {len(trade_dates)} 个日期作为备选")
            return trade_dates

    def _filter_fridays(self, date_list: List[str]) -> List[str]:
        """
        过滤出周五的日期
        
        Args:
            date_list: 日期列表
            
        Returns:
            只包含周五的日期列表
        """
        fridays = []
        for date_str in date_list:
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            # 0代表周一，4代表周五
            if date_obj.weekday() == 4:
                fridays.append(date_str)
                
        logger.info(f"从 {len(date_list)} 个日期中筛选出 {len(fridays)} 个周五")
        return fridays

    def _fetch_sequential_by_dates(self, date_list: List[str], target_ts_codes: Set[str]) -> bool:
        """
        按顺序获取多个日期的数据
        
        Args:
            date_list: 日期列表
            target_ts_codes: 目标股票代码集合
            
        Returns:
            是否成功
        """
        total_days = len(date_list)
        total_records = 0
        processed_days = 0
        success_days = 0
        
        # 用于统计的变量
        total_inserted = 0
        total_updated = 0
        total_skipped = 0
        
        start_time = time.time()
        
        # 逐日获取数据
        for date_str in date_list:
            logger.info(f"正在获取日期 {date_str} 的周线数据...")
            
            # 获取当日所有股票数据，使用支持offset的方法，不传递ts_code参数
            df = self.fetch_weekly_by_date_with_offset(trade_date=date_str)
            
            if not df.empty:
                # 过滤目标板块股票
                df_filtered = self.filter_weekly_data(df, target_ts_codes)
                
                # 保存到MongoDB
                if not df_filtered.empty:
                    success = self.save_to_mongodb(df_filtered)
                    
                    # 获取详细统计数据
                    inserted = 0
                    updated = 0
                    skipped = 0
                    if hasattr(self, 'last_operation_stats'):
                        inserted = self.last_operation_stats.get('inserted', 0)
                        updated = self.last_operation_stats.get('updated', 0)
                        skipped = self.last_operation_stats.get('skipped', 0)
                    
                    if success:
                        success_days += 1
                        total_records += len(df_filtered)
                        total_inserted += inserted
                        total_updated += updated
                        total_skipped += skipped
                        logger.success(f"成功保存 {date_str} 的周线数据，{len(df_filtered)} 条记录，新增 {inserted}，更新 {updated}，跳过 {skipped}")
                    else:
                        logger.warning(f"保存 {date_str} 的周线数据失败")
                else:
                    logger.warning(f"日期 {date_str} 过滤后无目标板块股票数据")
            else:
                logger.warning(f"日期 {date_str} 未获取到数据")
            
            # 更新进度
            processed_days += 1
            progress = processed_days / total_days * 100
            elapsed = time.time() - start_time
            avg_time_per_day = elapsed / processed_days if processed_days > 0 else 0
            remaining = (total_days - processed_days) * avg_time_per_day
            logger.info(f"日期进度: {processed_days}/{total_days} ({progress:.1f}%)，已处理时间: {elapsed:.1f}s，预估剩余: {remaining:.1f}s")
            
            # 进入下一天前短暂休眠
            time.sleep(1)
        
        # 处理完成
        elapsed_total = time.time() - start_time
        
        # 输出详细统计信息
        logger.success(f"按日期顺序获取周线数据完成，成功处理 {success_days}/{total_days} 天")
        logger.info(f"数据统计: 总记录数 {total_records}，新增 {total_inserted}，更新 {total_updated}，跳过 {total_skipped}，耗时 {elapsed_total:.1f}s")
        
        # 即使只有一天成功，也认为处理成功
        return success_days > 0

    def _fetch_parallel_by_dates(self, date_list: List[str], target_ts_codes: Set[str], available_wans: List[int]) -> bool:
        """
        使用多WAN口并行获取多个日期的数据
        
        Args:
            date_list: 日期列表
            target_ts_codes: 目标股票代码集合
            available_wans: 可用WAN接口列表
            
        Returns:
            是否成功
        """
        import threading
        import queue
        
        total_days = len(date_list)
        # 设置最大并发数
        max_workers = min(len(available_wans), total_days, 4)  # 最多4个并发，避免过多资源消耗
        result_queue = queue.Queue()
        active_threads = []
        
        # 线程锁用于日志和进度更新
        log_lock = threading.Lock()
        
        total_records = 0
        processed_days = 0
        success_days = 0
        
        # 线程函数 - 处理单个日期的数据
        def process_date(date_str, wan_idx):
            try:
                with log_lock:
                    logger.debug(f"WAN {wan_idx} 开始处理日期 {date_str}")
                
                # 获取WAN端口
                wan_info = self._get_wan_socket(wan_idx)
                if not wan_info:
                    with log_lock:
                        logger.warning(f"无法为WAN {wan_idx} 获取端口，使用默认网络接口")
                    # 注意：这里不使用ts_code参数，按日期获取全市场数据
                    df = self.fetch_weekly_by_date_with_offset(trade_date=date_str)
                else:
                    # 使用WAN口获取数据
                    wan_idx, port = wan_info
                    try:
                        # 使用WAN接口获取当日数据，不使用ts_code参数
                        df = self.fetch_weekly_by_date_with_wan(trade_date=date_str, wan_info=wan_info)
                    finally:
                        # 确保释放WAN端口
                        if wan_info:
                            # 添加短暂延迟，确保端口完全释放
                            time.sleep(0.5)
                            self.port_allocator.release_port(wan_idx, port)
                
                success = False
                records_count = 0
                inserted_count = 0
                updated_count = 0
                skipped_count = 0
                
                if not df.empty:
                    # 过滤目标板块股票
                    df_filtered = self.filter_weekly_data(df, target_ts_codes)
                    
                    # 保存到MongoDB
                    if not df_filtered.empty:
                        save_success = self.save_to_mongodb(df_filtered)
                        records_count = len(df_filtered)
                        
                        # 获取详细统计数据 - 最近一次操作的结果存储在类的属性中
                        if hasattr(self, 'last_operation_stats'):
                            inserted_count = self.last_operation_stats.get('inserted', 0)
                            updated_count = self.last_operation_stats.get('updated', 0)
                            skipped_count = self.last_operation_stats.get('skipped', 0)
                        
                        success = save_success
                
                # 放入结果队列 - 增加更多统计信息
                result_queue.put((date_str, success, records_count, inserted_count, updated_count, skipped_count))
                
                with log_lock:
                    if success:
                        logger.success(f"WAN {wan_idx} 成功处理日期 {date_str} 的数据，共 {records_count} 条记录，新增 {inserted_count}，更新 {updated_count}，跳过 {skipped_count}")
                    else:
                        logger.warning(f"WAN {wan_idx} 处理日期 {date_str} 失败或无数据")
            
            except Exception as e:
                with log_lock:
                    logger.error(f"WAN {wan_idx} 处理日期 {date_str} 出错: {str(e)}")
                    import traceback
                    logger.debug(f"详细错误信息: {traceback.format_exc()}")
                result_queue.put((date_str, False, 0, 0, 0, 0))
                
                # 确保释放WAN端口
                if 'wan_info' in locals() and wan_info:
                    wan_idx, port = wan_info
                    # 添加短暂延迟，确保端口完全释放
                    time.sleep(0.5)
                    self.port_allocator.release_port(wan_idx, port)
        
        # 循环处理所有日期，控制最大线程数
        start_time = time.time()
        
        # 用于统计的变量
        total_inserted = 0
        total_updated = 0
        total_skipped = 0
        
        for i, date_str in enumerate(date_list):
            # 选择WAN接口 - 轮询方式
            wan_idx = available_wans[i % len(available_wans)]
            
            # 创建并启动线程
            thread = threading.Thread(
                target=process_date,
                args=(date_str, wan_idx)
            )
            thread.start()
            active_threads.append(thread)
            
            # 控制最大并发数
            if len(active_threads) >= max_workers:
                # 等待一个线程完成
                while result_queue.empty():
                    time.sleep(0.5)
                
                # 处理结果
                date_str, success, records_count, inserted, updated, skipped = result_queue.get()
                processed_days += 1
                if success:
                    success_days += 1
                    total_records += records_count
                    total_inserted += inserted
                    total_updated += updated
                    total_skipped += skipped
                
                # 更新进度
                elapsed = time.time() - start_time
                avg_time_per_day = elapsed / processed_days if processed_days > 0 else 0
                remaining = (total_days - processed_days) * avg_time_per_day
                progress = processed_days / total_days * 100
                logger.info(f"日期进度: {processed_days}/{total_days} ({progress:.1f}%)，已处理时间: {elapsed:.1f}s，预估剩余: {remaining:.1f}s")
                
                # 移除已完成的线程
                active_threads = [t for t in active_threads if t.is_alive()]
                
                # 短暂休眠，避免过于频繁的请求
                time.sleep(0.5)
        
        # 等待所有剩余线程完成
        for thread in active_threads:
            thread.join()
        
        # 处理剩余结果
        while not result_queue.empty():
            date_str, success, records_count, inserted, updated, skipped = result_queue.get()
            processed_days += 1
            if success:
                success_days += 1
                total_records += records_count
                total_inserted += inserted
                total_updated += updated
                total_skipped += skipped
        
        # 处理完成
        elapsed_total = time.time() - start_time
        
        # 输出详细统计信息
        logger.success(f"按日期并行获取周线数据完成，成功处理 {success_days}/{total_days} 天")
        logger.info(f"数据统计: 总记录数 {total_records}，新增 {total_inserted}，更新 {total_updated}，跳过 {total_skipped}，耗时 {elapsed_total:.1f}s")
        
        # 即使只有一天成功，也认为处理成功
        return success_days > 0

    def _run_by_trade_dates(self, start_date: str, end_date: str, use_parallel: bool = True) -> bool:
        """
        按交易日期获取数据，获取每个交易日的数据
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            use_parallel: 是否使用并行处理
            
        Returns:
            是否成功
        """
        # 从交易日历获取日期范围内的交易日
        trade_dates = self.get_trade_calendar(start_date, end_date)
        if not trade_dates:
            logger.warning(f"日期范围 {start_date} 至 {end_date} 内无交易日")
            return False
        
        # 获取目标股票代码
        target_ts_codes = self.get_target_ts_codes_from_stock_basic()
        if not target_ts_codes:
            logger.error("未能获取到任何目标板块的股票代码")
            return False
        
        logger.info(f"通过交易日历获取到 {len(trade_dates)} 个交易日，将获取全市场数据")
        
        # 检查是否可以并行处理
        if use_parallel and self.port_allocator:
            available_wans = self.port_allocator.get_available_wan_indices()
            if available_wans:
                logger.info(f"使用并行模式处理 {len(trade_dates)} 个交易日，每个交易日获取全市场数据")
                return self._fetch_parallel_by_dates(trade_dates, target_ts_codes, available_wans)
                
        # 串行处理
        logger.info(f"使用串行模式处理 {len(trade_dates)} 个交易日，每个交易日获取全市场数据")
        return self._fetch_sequential_by_dates(trade_dates, target_ts_codes)

    def _run_by_stock_codes(self, start_date: str, end_date: str, batch_size: int = 10, use_parallel: bool = True) -> bool:
        """
        按股票代码列表获取数据，适用于full模式和start-date/end-date模式
        对于每个股票代码，获取指定日期范围内的所有周线数据
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            batch_size: 每批处理的股票数量
            use_parallel: 是否使用并行处理
            
        Returns:
            是否成功
        """
        # 获取目标股票代码
        target_ts_codes = self.get_target_ts_codes_from_stock_basic()
        if not target_ts_codes:
            logger.error("未能获取到任何目标板块的股票代码")
            return False
            
        logger.info(f"已获取 {len(target_ts_codes)} 个目标股票代码，将按ts_code分批获取日期范围 {start_date} 至 {end_date} 的周线数据")
            
        # 检查是否可以并行处理
        if use_parallel and self.port_allocator:
            available_wans = self.port_allocator.get_available_wan_indices()
            if available_wans:
                logger.info(f"使用并行模式处理 {len(target_ts_codes)} 个股票代码，每批 {batch_size} 个股票")
                return self.fetch_data_parallel(target_ts_codes, start_date, end_date, batch_size) is not None
                
        # 串行处理
        logger.info(f"使用串行模式处理 {len(target_ts_codes)} 个股票代码，每批 {batch_size} 个股票")
        return self.fetch_data_batch(target_ts_codes, start_date, end_date, batch_size) is not None

    def run(self, config: Optional[Dict[str, Any]] = None) -> bool:
        """
        运行数据获取和保存流程，支持自定义配置
        
        Args:
            config: 配置字典，包含start_date, end_date, full, recent等信息
            
        Returns:
            是否成功
        """
        # 使用默认配置
        default_config = {
            "start_date": None,
            "end_date": None,
            "full": False,
            "recent": False,
            "batch_size": 10,  # 每批次处理股票数量
            "minute_rate_limit": 500,
            "hour_rate_limit": 4000,
            "retry_count": 10,
            "use_parallel": True  # 是否使用并行处理
        }
        
        # 合并配置
        if config is None:
            config = {}
        
        effective_config = {**default_config, **config}
        start_date = effective_config["start_date"]
        end_date = effective_config["end_date"]
        full = effective_config["full"]
        recent = effective_config["recent"]
        batch_size = effective_config["batch_size"]
        minute_rate_limit = effective_config["minute_rate_limit"]
        hour_rate_limit = effective_config["hour_rate_limit"]
        use_parallel = effective_config["use_parallel"]
        
        # 处理不同模式
        if full:
            # --full 模式: 从1990年至今的所有数据
            start_date = "19900101"  # 从1990年1月1日开始
            end_date = datetime.now().strftime('%Y%m%d')
            logger.info(f"使用full模式：获取 {start_date} 至 {end_date} 期间的全部周线数据")
            return self._run_by_stock_codes(start_date, end_date, batch_size, use_parallel)
            
        elif recent:
            # --recent 模式: 获取最近一个月的数据，筛选出周五的数据
            today = datetime.now()
            end_date = today.strftime('%Y%m%d')
            start_date = (today - timedelta(days=30)).strftime('%Y%m%d')  # 最近30天
            logger.info(f"使用recent模式：获取 {start_date} 至 {end_date} 期间的周五交易日数据")
            return self._run_by_trade_dates(start_date, end_date, use_parallel)
            
        elif start_date and end_date:
            # --start-date --end-date 模式: 指定日期范围，按股票代码列表分批获取
            logger.info(f"使用日期范围模式：获取 {start_date} 至 {end_date} 期间的周线数据")
            return self._run_by_stock_codes(start_date, end_date, batch_size, use_parallel)
            
        else:
            # 默认模式: 同recent模式，但使用最近一个月
            today = datetime.now()
            end_date = today.strftime('%Y%m%d')
            start_date = (today - timedelta(days=30)).strftime('%Y%m%d')  # 最近30天
            logger.info(f"使用默认模式（等同于recent）：获取 {start_date} 至 {end_date} 期间的周五交易日数据")
            return self._run_by_trade_dates(start_date, end_date, use_parallel)

    def fetch_weekly_by_date_with_wan(self, trade_date: str, ts_code: str = None, wan_info: Tuple[int, int] = None, max_count: int = 9000) -> pd.DataFrame:
        """
        使用WAN接口按日期获取周线行情数据，支持offset处理超过API限制的数据
        
        Args:
            trade_date: 交易日期，格式为YYYYMMDD
            ts_code: 可选，股票代码
            wan_info: WAN接口和端口信息(wan_idx, port)
            max_count: 每次请求的最大记录数
            
        Returns:
            DataFrame形式的周线数据
        """
        try:
            # 准备请求参数
            params = {"trade_date": trade_date}
            if ts_code:
                params["ts_code"] = ts_code
                
            logger.debug(f"使用WAN接口获取日期 {trade_date} 的周线数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
            
            # 准备API参数
            api_name = "weekly"
            fields = self.interface_config.get("available_fields", [])
            
            # 创建WAN专用客户端
            if not wan_info:
                logger.warning("未提供WAN接口信息，使用普通客户端获取数据")
                return self.fetch_weekly_by_date_with_offset(trade_date=trade_date, ts_code=ts_code, max_count=max_count)
            
            wan_idx, port = wan_info
            client = TushareClientWAN(token=self.token, timeout=120, api_url=self.api_url)
            try:
                client.set_local_address('0.0.0.0', port)
                
                # 使用偏移量处理数据超限
                all_data = []
                offset = 0
                
                while True:
                    # 复制参数并添加分页参数
                    current_params = params.copy()
                    current_params["offset"] = offset
                    current_params["limit"] = max_count
                    
                    # 获取数据
                    start_time = time.time()
                    df = client.get_data(api_name=api_name, params=current_params, fields=fields)
                    elapsed = time.time() - start_time
                    
                    if df is None or df.empty:
                        if offset == 0:
                            logger.warning(f"WAN {wan_idx} 获取日期 {trade_date} 未获取到数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
                            return pd.DataFrame()
                        else:
                            # 已经获取了一部分数据，当前批次为空表示数据已获取完毕
                            break
                    
                    # 添加到结果列表
                    all_data.append(df)
                    records_count = len(df)
                    logger.debug(f"WAN {wan_idx} 获取到 {records_count} 条记录，偏移量: {offset}, 耗时: {elapsed:.2f}s")
                    
                    # 如果返回的数据量等于最大请求数量，可能还有更多数据
                    if records_count == max_count:
                        logger.info(f"WAN {wan_idx} 返回数据量达到单次请求上限 {max_count}，将继续获取下一批数据")
                    # 如果返回的数据量小于请求的数量，说明已经没有更多数据
                    if records_count < max_count:
                        break
                        
                    # 设置下一批次的偏移量
                    offset += max_count
                    
                    # 短暂休眠，避免过于频繁的请求
                    time.sleep(0.5)
                
                # 合并所有数据
                if not all_data:
                    return pd.DataFrame()
                    
                result_df = pd.concat(all_data, ignore_index=True)
                total_records = len(result_df)
                
                # 只有当总记录数超过9500才提示可能数据不完整
                if total_records > 9500 and len(all_data) == 1:
                    logger.warning(f"WAN {wan_idx} 股票数据总量 {total_records} 接近API限制(10000)，可能数据不完整，建议按时间段分割获取")
                
                logger.info(f"WAN {wan_idx} 成功获取日期 {trade_date} 的周线数据"+(f" 股票代码: {ts_code}" if ts_code else f"，共 {total_records} 条记录"))
                return result_df
            finally:
                # 重置客户端设置，确保资源释放
                if hasattr(client, 'reset_local_address'):
                    client.reset_local_address()
                
        except Exception as e:
            logger.error(f"WAN接口获取日期 {trade_date} 的周线数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return pd.DataFrame()
            
    def fetch_weekly_by_code_with_wan(self, ts_code: str, start_date: str = None, end_date: str = None, wan_info: Tuple[int, int] = None, max_count: int = 9000) -> pd.DataFrame:
        """
        使用WAN接口按股票代码获取周线行情数据，支持offset处理超过API限制的数据
        
        Args:
            ts_code: 股票代码
            start_date: 可选，开始日期，格式为YYYYMMDD
            end_date: 可选，结束日期，格式为YYYYMMDD
            wan_info: WAN接口和端口信息(wan_idx, port)
            max_count: 每次请求的最大记录数
            
        Returns:
            DataFrame形式的周线数据
        """
        try:
            # 准备请求参数
            params = {"ts_code": ts_code}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
                
            logger.debug(f"使用WAN接口获取股票 {ts_code} 的周线数据"+(f" 日期范围: {start_date} 至 {end_date}" if start_date and end_date else ""))
            
            # 准备API参数
            api_name = "weekly"
            fields = self.interface_config.get("available_fields", [])
            
            # 创建WAN专用客户端
            if not wan_info:
                logger.warning("未提供WAN接口信息，使用普通客户端获取数据")
                return self.fetch_weekly_by_code_with_offset(ts_code=ts_code, start_date=start_date, end_date=end_date, max_count=max_count)
            
            wan_idx, port = wan_info
            client = TushareClientWAN(token=self.token, timeout=120, api_url=self.api_url)
            try:
                client.set_local_address('0.0.0.0', port)
                
                # 使用偏移量处理数据超限
                all_data = []
                offset = 0
                
                while True:
                    # 复制参数并添加分页参数
                    current_params = params.copy()
                    current_params["offset"] = offset
                    current_params["limit"] = max_count
                    
                    # 获取数据
                    start_time = time.time()
                    df = client.get_data(api_name=api_name, params=current_params, fields=fields)
                    elapsed = time.time() - start_time
                    
                    if df is None or df.empty:
                        if offset == 0:
                            logger.warning(f"WAN {wan_idx} 获取股票 {ts_code} 未获取到数据"+(f" 日期范围: {start_date} 至 {end_date}" if start_date and end_date else ""))
                            return pd.DataFrame()
                        else:
                            # 已经获取了一部分数据，当前批次为空表示数据已获取完毕
                            break
                    
                    # 添加到结果列表
                    all_data.append(df)
                    records_count = len(df)
                    logger.debug(f"WAN {wan_idx} 获取到 {records_count} 条记录，偏移量: {offset}, 耗时: {elapsed:.2f}s")
                    
                    # 如果返回的数据量等于最大请求数量，可能还有更多数据
                    if records_count == max_count:
                        logger.info(f"WAN {wan_idx} 股票 {ts_code} 返回数据量达到单次请求上限 {max_count}，将继续获取下一批数据")
                    # 如果返回的数据量小于请求的数量，说明已经没有更多数据
                    if records_count < max_count:
                        break
                        
                    # 设置下一批次的偏移量
                    offset += max_count
                    
                    # 短暂休眠，避免过于频繁的请求
                    time.sleep(0.5)
                
                # 合并所有数据
                if not all_data:
                    return pd.DataFrame()
                    
                result_df = pd.concat(all_data, ignore_index=True)
                total_records = len(result_df)
                
                # 只有当总记录数超过9500才提示可能数据不完整
                if total_records > 9500 and len(all_data) == 1:
                    logger.warning(f"WAN {wan_idx} 股票 {ts_code} 数据总量 {total_records} 接近API限制(10000)，可能数据不完整，建议按时间段分割获取")
                
                logger.info(f"WAN {wan_idx} 成功获取股票 {ts_code} 的周线数据，共 {total_records} 条记录")
                return result_df
            finally:
                # 重置客户端设置，确保资源释放
                if hasattr(client, 'reset_local_address'):
                    client.reset_local_address()
                
        except Exception as e:
            logger.error(f"WAN接口获取股票 {ts_code} 的周线数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return pd.DataFrame()

    def fetch_weekly_by_date_with_offset(self, trade_date: str, ts_code: str = None, max_count: int = 9000) -> pd.DataFrame:
        """
        按日期获取周线行情数据，支持offset处理超过API限制的数据
        
        Args:
            trade_date: 交易日期，格式为YYYYMMDD
            ts_code: 可选，股票代码
            max_count: 每次请求的最大记录数
            
        Returns:
            DataFrame形式的周线数据
        """
        try:
            params = {"trade_date": trade_date}
            if ts_code:
                params["ts_code"] = ts_code
                
            logger.debug(f"获取日期 {trade_date} 的周线数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
            
            df = self.fetch_data_with_offset(
                api_name="weekly",
                params=params,
                fields=self.interface_config.get("available_fields", []),
                max_count=max_count,
                offset=0
            )
            
            if df.empty:
                logger.warning(f"日期 {trade_date} 未获取到数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
                return pd.DataFrame()
            
            # 只有当总记录数超过9500才提示可能数据不完整
            if len(df) > 9500 and len(df) < 10000:
                logger.warning(f"日期 {trade_date} 返回数据量 {len(df)} 接近API限制(10000)，可能数据不完整")
            
            logger.info(f"成功获取日期 {trade_date} 的周线数据"+(f" 股票代码: {ts_code}" if ts_code else f"，共 {len(df)} 条记录"))
            return df
        except Exception as e:
            logger.error(f"获取日期 {trade_date} 的周线数据失败: {str(e)}")
            return pd.DataFrame()
            
    def fetch_weekly_by_code_with_offset(self, ts_code: str, start_date: str = None, end_date: str = None, max_count: int = 9000) -> pd.DataFrame:
        """
        按股票代码获取周线行情数据，支持offset处理超过API限制的数据
        
        Args:
            ts_code: 股票代码
            start_date: 可选，开始日期，格式为YYYYMMDD
            end_date: 可选，结束日期，格式为YYYYMMDD
            max_count: 每次请求的最大记录数
            
        Returns:
            DataFrame形式的周线数据
        """
        try:
            params = {"ts_code": ts_code}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
                
            logger.debug(f"获取股票 {ts_code} 的周线数据"+(f" 日期范围: {start_date} 至 {end_date}" if start_date and end_date else ""))
            
            df = self.fetch_data_with_offset(
                api_name="weekly",
                params=params,
                fields=self.interface_config.get("available_fields", []),
                max_count=max_count,
                offset=0
            )
            
            if df.empty:
                logger.warning(f"股票 {ts_code} 未获取到数据"+(f" 日期范围: {start_date} 至 {end_date}" if start_date and end_date else ""))
                return pd.DataFrame()
                
            # 只有当总记录数超过9500才提示可能数据不完整
            if len(df) > 9500 and len(df) < 10000:
                logger.warning(f"股票 {ts_code} 返回数据量 {len(df)} 接近API限制(10000)，可能数据不完整")
            
            logger.info(f"成功获取股票 {ts_code} 的周线数据，共 {len(df)} 条记录")
            return df
        except Exception as e:
            logger.error(f"获取股票 {ts_code} 的周线数据失败: {str(e)}")
            return pd.DataFrame()

    def fetch_data_with_offset(self, api_name: str, params: dict, fields: list = None, max_count: int = 10000, offset: int = 0) -> pd.DataFrame:
        """
        获取API数据，支持offset参数处理超过API限制的数据
        
        Args:
            api_name: API名称
            params: 请求参数
            fields: 返回字段列表
            max_count: 每次请求的最大记录数
            offset: 起始偏移量
            
        Returns:
            合并后的DataFrame
        """
        try:
            # 拷贝参数，避免修改原始参数
            params_copy = params.copy() if params else {}
            
            # 添加offset和limit参数
            params_copy["offset"] = offset
            params_copy["limit"] = max_count
            
            # 获取第一批数据
            logger.debug(f"获取 {api_name} 数据，偏移量: {offset}, 限制: {max_count}")
            df = self.ts_client.get_data(api_name=api_name, params=params_copy, fields=fields)
            
            if df is None or df.empty:
                logger.warning(f"API {api_name} 未返回数据")
                return pd.DataFrame()
                
            # 记录获取到的数据数量
            records_count = len(df)
            logger.debug(f"获取到 {records_count} 条记录")
            
            # 如果返回的数据数量等于限制，可能还有更多数据
            if records_count == max_count:
                logger.info(f"返回数据数量达到请求上限 {max_count}，尝试获取更多数据")
                
                # 递归获取下一批数据
                next_offset = offset + max_count
                next_df = self.fetch_data_with_offset(
                    api_name=api_name,
                    params=params,
                    fields=fields,
                    max_count=max_count,
                    offset=next_offset
                )
                
                # 合并数据
                if next_df is not None and not next_df.empty:
                    df = pd.concat([df, next_df], ignore_index=True)
                    logger.info(f"合并后共 {len(df)} 条记录")
            
            # 只有当总记录数超过9500才提示可能数据不完整
            if records_count > 9500 and records_count < max_count:
                logger.warning(f"API {api_name} 返回数据量 {records_count} 接近限制(10000)，可能数据不完整")
            
            return df
            
        except Exception as e:
            logger.error(f"获取数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return pd.DataFrame()

    def _get_last_trading_days_of_weeks(self, date_list: List[str]) -> List[str]:
        """
        获取每周的最后一个交易日
        
        Args:
            date_list: 按日期排序的交易日列表
            
        Returns:
            每周最后一个交易日的列表
        """
        if not date_list:
            return []
            
        # 确保日期列表是按日期升序排序的
        sorted_dates = sorted(date_list)
        
        # 用于存储每周最后一个交易日
        weekly_last_trading_days = []
        
        # 当前处理的周数
        current_week = None
        last_date = None
        
        for date_str in sorted_dates:
            # 解析日期字符串
            date_obj = datetime.strptime(date_str, '%Y%m%d')
            
            # 获取该日期的年份和周数
            year_week = date_obj.isocalendar()[:2]  # (year, week_number)
            
            if current_week is None:
                # 第一次迭代
                current_week = year_week
                last_date = date_str
            elif year_week != current_week:
                # 周数变化，之前的last_date是上一周的最后一个交易日
                weekly_last_trading_days.append(last_date)
                current_week = year_week
                last_date = date_str
            else:
                # 同一周内，更新last_date
                last_date = date_str
        
        # 添加最后一周的最后一个交易日
        if last_date is not None:
            weekly_last_trading_days.append(last_date)
        
        logger.info(f"从 {len(date_list)} 个交易日中提取出 {len(weekly_last_trading_days)} 个周末最后交易日")
        return weekly_last_trading_days


def main():
    """主函数"""
    import argparse
    from datetime import datetime, timedelta
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="周线行情数据获取工具")
    parser.add_argument("--verbose", action="store_true", help="输出详细日志")
    parser.add_argument("--mock", action="store_true", help="使用模拟数据模式（API不可用时）")
    parser.add_argument("--start-date", type=str, help="开始日期，格式为YYYYMMDD，如20100101")
    parser.add_argument("--end-date", type=str, help="结束日期，格式为YYYYMMDD，如20201231")
    parser.add_argument("--recent", action="store_true", help="获取最近一个月的周五交易日数据（默认模式）")
    parser.add_argument("--full", action="store_true", help="获取从1990年1月1日至今的完整历史数据，按ts_code列表分批获取")
    parser.add_argument("--ts-code", type=str, help="指定股票代码，例如600000.SH")
    parser.add_argument("--batch-size", type=int, default=1, help="每批次处理的股票数量，默认1")
    parser.add_argument("--market-codes", type=str, default="00,30,60,68", help="目标市场代码，用逗号分隔，默认为00,30,60,68")
    parser.add_argument("--no-parallel", dest="use_parallel", action="store_false", help="不使用并行处理")
    parser.add_argument("--minute-rate-limit", type=int, default=500, help="每分钟API调用限制")
    parser.add_argument("--hour-rate-limit", type=int, default=4000, help="每小时API调用限制")
    parser.add_argument("--config", type=str, default="config/config.yaml", help="配置文件路径")
    parser.set_defaults(use_parallel=True)
    args = parser.parse_args()
    
    # 解析市场代码
    target_market_codes = set(args.market_codes.split(','))
    
    # 创建获取器
    fetcher = WeeklyFetcher(
        config_path=args.config,
        verbose=args.verbose, 
        batch_size=args.batch_size,
        target_market_codes=target_market_codes
    )
    
    # 获取周线行情数据
    if args.mock:
        logger.warning("使用模拟数据模式，生成随机的周线行情数据")
        # 使用mock数据的逻辑可以在这里实现
    else:
        if args.ts_code:
            # 如果指定了单个股票代码，使用股票代码方式获取
            start_date = args.start_date
            end_date = args.end_date
            
            # 设置默认日期范围
            if not start_date or not end_date:
                today = datetime.now()
                end_date = today.strftime('%Y%m%d')  # 今天
                start_date = (today - timedelta(days=30)).strftime('%Y%m%d')  # 一个月前
                logger.info(f"为指定股票设置默认日期范围: {start_date} 至 {end_date}")
            
            logger.info(f"获取单个股票 {args.ts_code} 的周线数据，日期范围: {start_date} 至 {end_date}")
            
            # 使用支持offset的方法获取数据
            result_df = fetcher.fetch_weekly_by_code_with_offset(
                ts_code=args.ts_code,
                start_date=start_date,
                end_date=end_date,
                max_count=9000
            )
            
            if not result_df.empty:
                # 检查是否是目标板块的股票
                code_prefix = args.ts_code[:6][:2] if len(args.ts_code) >= 6 else ""
                if code_prefix in target_market_codes:
                    # 保存数据
                    fetcher.save_to_mongodb(result_df)
                    logger.success(f"数据获取和保存成功，共 {len(result_df)} 条记录")
                else:
                    logger.warning(f"股票 {args.ts_code} 不在目标市场代码 {target_market_codes} 中，不保存数据")
            else:
                logger.warning("未获取到任何周线行情数据")
        else:
            # 构建运行配置字典
            run_config = {
                "start_date": args.start_date,
                "end_date": args.end_date,
                "full": args.full,
                "recent": args.recent,
                "batch_size": args.batch_size,
                "minute_rate_limit": args.minute_rate_limit,
                "hour_rate_limit": args.hour_rate_limit,
                "use_parallel": args.use_parallel
            }
            
            # 输出不同模式的获取策略
            if args.full:
                logger.info("Full模式: 获取1990年1月1日至今的全部周线数据，按ts_code列表分批获取")
            elif args.recent:
                logger.info("Recent模式: 获取最近一个月的周五交易日数据，筛选出目标板块")
            elif args.start_date and args.end_date:
                logger.info(f"日期范围模式: 获取 {args.start_date} 至 {args.end_date} 期间的周线数据，按ts_code列表分批获取")
            else:
                logger.info("默认模式: 等同于Recent模式，获取最近一个月的周五交易日数据")
            
            # 使用配置字典运行
            logger.info(f"使用批量模式获取周线数据，目标市场代码: {', '.join(target_market_codes)}")
            result = fetcher.run(config=run_config)
            
            if result:
                logger.success("周线数据获取和保存成功")
            else:
                logger.error("周线数据获取或保存失败")

if __name__ == "__main__":
    main()

