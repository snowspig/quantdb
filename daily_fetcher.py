#!/usr/bin/env python
"""
日线行情数据获取器 - 获取日线行情数据并保存到MongoDB

该脚本用于从湘财Tushare获取日线行情数据，并保存到MongoDB数据库中
该版本通过分时间段获取和多WAN接口并行处理，解决大量数据获取问题

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=27

使用方法：
    python daily_fetcher.py              # 默认使用recent模式获取最近一周的数据更新
    python daily_fetcher.py --full        # 获取完整历史数据而非默认的最近一周数据
    python daily_fetcher.py --verbose     # 使用湘财真实API数据，详细日志模式
    python daily_fetcher.py --mock        # 使用模拟数据模式（API不可用时）
    python daily_fetcher.py --start-date 20100101 --end-date 20201231  # 指定日期范围获取数据
    python daily_fetcher.py --recent      # 显式指定recent模式（最近一周数据更新，默认模式）
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

class DailyFetcher:
    """
    日线行情数据获取器
    
    该类用于从Tushare获取日线行情数据并保存到MongoDB数据库
    优化点：
    1. 支持按时间段分批获取数据，避免一次获取超过10000条数据限制
    2. 多WAN接口并行获取，提高数据获取效率
    3. 增加数据获取重试机制，提高稳定性
    4. 支持recent模式、full模式以及指定日期范围模式
    """

    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "daily.json",
        target_market_codes: Set[str] = {"00", "30", "60", "68"},  # 默认只保存00 30 60 68四个板块的股票数据
        db_name: str = None,
        collection_name: str = "daily",
        verbose: bool = False,
        max_workers: int = 4,  # 并行工作线程数
        retry_count: int = 3,  # 数据获取重试次数
        retry_delay: int = 5,  # 重试延迟时间(秒)
        batch_size: int = 10000  # 每批次获取数据的最大数量，防止超过API限制
    ):
        """
        初始化日线行情数据获取器
        
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

    def fetch_daily_by_date(self, trade_date: str, ts_code: str = None) -> pd.DataFrame:
        """
        按日期获取日线行情数据
        
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
                
            logger.debug(f"获取日期 {trade_date} 的日线数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
            
            df = self.ts_client.get_data(
                api_name="daily",
                params=params,
                fields=self.interface_config.get("available_fields", [])
            )
            
            if df.empty:
                logger.warning(f"日期 {trade_date} 未获取到数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
                return pd.DataFrame()
            
            logger.info(f"成功获取日期 {trade_date} 的日线数据"+(f" 股票代码: {ts_code}" if ts_code else f"，共 {len(df)} 条记录"))
            return df
        except Exception as e:
            logger.error(f"获取日期 {trade_date} 的日线数据失败: {str(e)}")
            return pd.DataFrame()
            
    def fetch_daily_by_code(self, ts_code: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        按股票代码获取日线行情数据
        
        Args:
            ts_code: 股票代码
            start_date: 可选，开始日期，格式为YYYYMMDD
            end_date: 可选，结束日期，格式为YYYYMMDD
            
        Returns:
            DataFrame形式的日线数据
        """
        try:
            params = {"ts_code": ts_code}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
                
            logger.debug(f"获取股票 {ts_code} 的日线数据"+(f" 日期范围: {start_date} 至 {end_date}" if start_date and end_date else ""))
            
            df = self.ts_client.get_data(
                api_name="daily",
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
            
            logger.info(f"成功获取股票 {ts_code} 的日线数据，共 {record_count} 条记录")
            return df
        except Exception as e:
            logger.error(f"获取股票 {ts_code} 的日线数据失败: {str(e)}")
            return pd.DataFrame()
    
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
            
    def filter_daily_data(self, df: pd.DataFrame, target_ts_codes: Set[str]) -> pd.DataFrame:
        """
        根据目标股票代码集合过滤日线数据
        
        Args:
            df: 日线数据
            target_ts_codes: 目标股票代码集合
        
        Returns:
            过滤后的数据
        """
        if df is None or df.empty:
            logger.warning("没有日线数据可过滤")
            return pd.DataFrame()
        
        logger.info(f"过滤前日线数据数量: {len(df)}")
        
        # 确保ts_code列存在
        if 'ts_code' not in df.columns:
            logger.error("数据中没有ts_code列，无法按股票代码过滤")
            return df
        
        # 过滤数据
        df_filtered = df[df['ts_code'].isin(target_ts_codes)].copy()
        
        # 输出过滤统计信息
        logger.info(f"过滤后日线数据数量: {len(df_filtered)}")
        
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
    
    def fetch_data_for_date_range(self, start_date: str, end_date: str, ts_code: str = None, use_wan: bool = False, wan_index: int = None) -> pd.DataFrame:
        """
        获取指定日期范围内的日线数据
        如果日期范围内的交易日超过10000天，则按股票代码分批获取
        每获取一批数据后立即保存到MongoDB，提高处理效率和内存使用
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            ts_code: 可选，股票代码
            use_wan: 是否使用WAN接口
            wan_index: WAN接口索引
            
        Returns:
            处理结果统计的DataFrame
        """
        try:
            # 计算日期范围内的交易日数，估算数据量
            days_diff = (datetime.strptime(end_date, '%Y%m%d') - datetime.strptime(start_date, '%Y%m%d')).days + 1
            
            # 初始化统计数据
            total_inserted = 0
            total_updated = 0
            total_skipped = 0
            all_data_count = 0
            all_data_sample = None  # 保存一小部分数据作为样本返回
            
            # 获取目标股票代码列表（除非已指定单个股票代码）
            target_ts_codes = set()
            if not ts_code:
                target_ts_codes = self.get_target_ts_codes_from_stock_basic()
                if not target_ts_codes:
                    logger.warning("未能获取到任何目标板块的股票代码，将获取所有股票数据")
            
            # 如果已指定股票代码，则直接获取该股票的数据
            if ts_code:
                # 检查是否是目标板块的股票
                if target_ts_codes and ts_code not in target_ts_codes:
                    code_prefix = ts_code[:6][:2] if len(ts_code) >= 6 else ""
                    if code_prefix not in self.target_market_codes:
                        logger.warning(f"股票 {ts_code} 不在目标市场代码 {self.target_market_codes} 中，将被跳过")
                        return pd.DataFrame()
                
                df = self.fetch_daily_by_code(ts_code=ts_code, start_date=start_date, end_date=end_date)
                if not df.empty:
                    # 立即保存数据
                    self.save_to_mongodb(df)
                    all_data_count += len(df)
                    all_data_sample = df.head(min(100, len(df)))  # 保存样本数据
                
                logger.info(f"已处理股票 {ts_code} 的数据，获取到 {len(df)} 条记录")
                return all_data_sample if all_data_sample is not None else pd.DataFrame()
            
            # 如果交易日可能超过10000天，按股票代码分批获取
            if days_diff > 50:  # 假设平均每年250个交易日，10000/250=40年，保守估计为50天
                logger.info(f"日期范围 {start_date} 至 {end_date} 可能包含大量交易日，将按股票代码分批获取")
                
                # 获取所有股票代码或使用目标股票代码
                stock_codes = list(target_ts_codes) if target_ts_codes else self._get_all_stock_codes()
                if not stock_codes:
                    logger.error("获取股票列表失败，无法继续获取数据")
                    return pd.DataFrame()
                    
                # 分批处理股票代码
                code_batches = self._split_stock_codes(stock_codes, batch_size=50)  # 每批处理50只股票
                
                # 逐批获取数据
                for i, batch in enumerate(code_batches):
                    logger.info(f"正在获取第 {i+1}/{len(code_batches)} 批股票数据，共 {len(batch)} 只股票")
                    
                    for code in batch:
                        df = self.fetch_daily_by_code(ts_code=code, start_date=start_date, end_date=end_date)
                        if not df.empty:
                            # 立即保存数据
                            save_result = self.save_to_mongodb(df)
                            all_data_count += len(df)
                            
                            # 保存样本数据
                            if all_data_sample is None or len(all_data_sample) < 100:
                                sample_size = min(100 - (0 if all_data_sample is None else len(all_data_sample)), len(df))
                                if all_data_sample is None:
                                    all_data_sample = df.head(sample_size)
                                else:
                                    all_data_sample = pd.concat([all_data_sample, df.head(sample_size)], ignore_index=True)
                                    all_data_sample = all_data_sample.head(100)  # 确保不超过100行
                
                logger.success(f"已处理所有批次，共获取和保存了 {all_data_count} 条日线数据")
                return all_data_sample if all_data_sample is not None else pd.DataFrame()
            else:
                # 逐日获取数据
                date_ranges = self._generate_date_ranges(start_date, end_date, interval_days=30)  # 每次获取30天数据
                
                for start, end in date_ranges:
                    logger.info(f"获取日期范围 {start} 至 {end} 的日线数据")
                    
                    current_date = datetime.strptime(start, '%Y%m%d')
                    end_date_obj = datetime.strptime(end, '%Y%m%d')
                    
                    while current_date <= end_date_obj:
                        date_str = current_date.strftime('%Y%m%d')
                        df = self.fetch_daily_by_date(trade_date=date_str, ts_code=ts_code)
                        if not df.empty:
                            # 如果有目标股票代码集合，先过滤数据
                            if target_ts_codes and not ts_code:
                                df = self.filter_daily_data(df, target_ts_codes)
                            
                            if not df.empty:
                                # 立即保存数据
                                save_result = self.save_to_mongodb(df)
                                all_data_count += len(df)
                                
                                # 保存样本数据
                                if all_data_sample is None or len(all_data_sample) < 100:
                                    sample_size = min(100 - (0 if all_data_sample is None else len(all_data_sample)), len(df))
                                    if all_data_sample is None:
                                        all_data_sample = df.head(sample_size)
                                    else:
                                        all_data_sample = pd.concat([all_data_sample, df.head(sample_size)], ignore_index=True)
                                        all_data_sample = all_data_sample.head(100)  # 确保不超过100行
                            
                        current_date += timedelta(days=1)
                
                logger.success(f"已处理所有日期，共获取和保存了 {all_data_count} 条日线数据")
                return all_data_sample if all_data_sample is not None else pd.DataFrame()
        except Exception as e:
            logger.error(f"获取日期范围 {start_date} 至 {end_date} 的日线数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
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
                    logger.warning(f"指定的WAN {wan_idx} 不可用")
                    return None
            else:
                # 轮询选择一个WAN接口
                wan_idx = available_indices[0]  # 简单起见，选择第一个
            
            # 分配端口
            port = self.port_allocator.allocate_port(wan_idx)
            if not port:
                logger.warning(f"WAN {wan_idx} 没有可用端口")
                return None
                
            logger.debug(f"使用WAN接口 {wan_idx}，本地端口 {port}")
            return (wan_idx, port)
            
        except Exception as e:
            logger.error(f"获取WAN接口失败: {str(e)}")
            return None

    def save_to_mongodb(self, df: pd.DataFrame) -> bool:
        """
        将日线数据保存到MongoDB
        
        Args:
            df: DataFrame形式的日线数据
            
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
            
            # 确保索引存在 - 提前创建索引以提高插入和查询效率
            try:
                # 创建复合唯一索引 (ts_code, trade_date)
                index_name = "ts_code_1_trade_date_1"
                existing_indexes = collection.index_information()
                if index_name not in existing_indexes:
                    collection.create_index(
                        [("ts_code", 1), ("trade_date", 1)], 
                        unique=True, 
                        background=True
                    )
                    logger.debug(f"已为字段组合 (ts_code, trade_date) 创建唯一复合索引")
                
                # 创建其他常用查询字段的索引
                for field in ["ts_code", "trade_date"]:
                    if f"{field}_1" not in existing_indexes:
                        collection.create_index(field)
                        logger.debug(f"已为字段 {field} 创建索引")
            except Exception as e:
                logger.warning(f"创建索引时出错: {str(e)}")
            
            # 将DataFrame转换为字典列表
            records = df.to_dict('records')
            
            # 批量处理，避免一次性处理太多记录
            batch_size = 10000
            total_batches = (len(records) + batch_size - 1) // batch_size
            
            total_inserted = 0
            total_updated = 0
            total_skipped = 0
            
            # 准备批量操作
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                batch_result = self._batch_upsert(collection, batch, ["ts_code", "trade_date"])
                
                total_inserted += batch_result["inserted"]
                total_updated += batch_result["updated"]
                total_skipped += batch_result["skipped"]
                
                # 进度显示
                if self.verbose and total_batches > 1:
                    progress = (i + len(batch)) / len(records) * 100
                    progress = min(progress, 100)
                    logger.debug(f"MongoDB保存进度: {i+len(batch)}/{len(records)} ({progress:.1f}%)")
            
            elapsed = time.time() - start_time
            total_processed = total_inserted + total_updated + total_skipped
            
            logger.success(f"数据处理完成: 新插入 {total_inserted} 条记录，更新 {total_updated} 条记录，跳过 {total_skipped} 条重复记录，共处理 {total_processed} 条记录，耗时 {elapsed:.2f}s")
            return total_processed > 0
                
        except Exception as e:
            logger.error(f"保存数据到MongoDB失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False
    
    def _batch_upsert(self, collection, records: List[Dict], unique_keys: List[str]) -> Dict[str, int]:
        """
        批量更新或插入记录
        
        Args:
            collection: MongoDB集合对象
            records: 要保存的记录列表
            unique_keys: 唯一键列表
            
        Returns:
            包含插入、更新和跳过记录数的字典
        """
        inserted = 0
        updated = 0
        skipped = 0
        
        # 使用bulk操作提高效率
        bulk_operations = []
        
        # 存储已处理记录的唯一键，用于检测重复
        processed_keys = set()
        
        for record in records:
            # 构建查询条件和唯一键
            query = {}
            key_str = ""
            
            for key in unique_keys:
                if key in record:
                    query[key] = record[key]
                    key_str += str(record[key]) + "_"
                else:
                    # 缺少唯一键字段，无法确定唯一性
                    key_str = None
                    break
            
            # 如果无法创建唯一键或者该记录已经处理过，跳过
            if key_str is None or key_str in processed_keys:
                skipped += 1
                continue
                    
            # 记录这个唯一键，避免重复处理
            processed_keys.add(key_str)
            
            # 如果缺少唯一键字段，则直接插入
            if len(query) != len(unique_keys):
                bulk_operations.append(pymongo.InsertOne(record))
                inserted += 1
                continue
            
            # 先检查记录是否已存在
            try:
                existing_record = collection.find_one(query, {"_id": 1})
                if existing_record:
                    # 记录已存在，执行更新
                    bulk_operations.append(
                        pymongo.UpdateOne(
                            query,
                            {"$set": record}
                        )
                    )
                    updated += 1
                else:
                    # 记录不存在，执行插入
                    bulk_operations.append(pymongo.InsertOne(record))
                    inserted += 1
            except Exception as e:
                logger.warning(f"检查记录存在性时出错: {str(e)}，将尝试插入")
                bulk_operations.append(
                    pymongo.UpdateOne(
                        query,
                        {"$set": record},
                        upsert=True
                    )
                )
                updated += 1
        
        # 如果有操作，则执行批量操作
        if bulk_operations:
            try:
                result = collection.bulk_write(bulk_operations, ordered=False)
                
                # 实际插入和更新的数量可能与我们的计数不一致，使用MongoDB返回的结果
                real_inserted = result.inserted_count
                real_updated = result.modified_count
                real_upserted = result.upserted_count if hasattr(result, 'upserted_count') else 0
                
                # 如果MongoDB返回的数量与我们的计数不一致，记录警告
                if real_inserted != inserted or real_updated != updated:
                    logger.warning(f"MongoDB返回的插入/更新数量与预期不一致: 预期插入={inserted}, 实际插入={real_inserted}; 预期更新={updated}, 实际更新={real_updated}, 实际upsert={real_upserted}")
                
                # 使用MongoDB返回的数量更新我们的统计
                inserted = real_inserted
                updated = real_updated + real_upserted
                
            except pymongo.errors.BulkWriteError as bwe:
                # 处理部分失败的情况
                if hasattr(bwe, 'details'):
                    if 'nInserted' in bwe.details:
                        inserted = bwe.details['nInserted']
                    if 'nModified' in bwe.details:
                        updated = bwe.details['nModified']
                    if 'nUpserted' in bwe.details:
                        updated += bwe.details['nUpserted']
                    
                    # 检查写入错误
                    if 'writeErrors' in bwe.details:
                        # 更新跳过计数 - 重复键错误通常是跳过的记录
                        duplicate_count = 0
                        for error in bwe.details['writeErrors']:
                            if error.get('code') == 11000:  # 重复键错误
                                duplicate_count += 1
                        
                        if duplicate_count > 0:
                            logger.debug(f"检测到 {duplicate_count} 条重复键错误，这些记录将被跳过")
                            skipped += duplicate_count
                
                logger.warning(f"批量写入部分失败: {len(bwe.details.get('writeErrors', []))} 错误")
                
        return {"inserted": inserted, "updated": updated, "skipped": skipped}

    def fetch_daily_by_date_with_wan(self, trade_date: str, ts_code: str = None, wan_info: Tuple[int, int] = None) -> pd.DataFrame:
        """
        使用WAN接口按日期获取日线行情数据
        
        Args:
            trade_date: 交易日期，格式为YYYYMMDD
            ts_code: 可选，股票代码
            wan_info: WAN接口和端口信息(wan_idx, port)
            
        Returns:
            DataFrame形式的日线数据
        """
        try:
            params = {"trade_date": trade_date}
            if ts_code:
                params["ts_code"] = ts_code
                
            logger.debug(f"使用WAN接口获取日期 {trade_date} 的日线数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
            
            # 准备API参数
            api_name = "daily"
            fields = self.interface_config.get("available_fields", [])
            
            # 创建WAN专用客户端
            if not wan_info:
                logger.warning("未提供WAN接口信息，使用普通客户端获取数据")
                return self.fetch_daily_by_date(trade_date=trade_date, ts_code=ts_code)
            
            wan_idx, port = wan_info
            client = TushareClientWAN(token=self.token, timeout=120, api_url=self.api_url)
            client.set_local_address('0.0.0.0', port)
            
            # 获取数据
            start_time = time.time()
            df = client.get_data(api_name=api_name, params=params, fields=fields)
            elapsed = time.time() - start_time
            
            if df is None or df.empty:
                logger.warning(f"WAN {wan_idx} 获取日期 {trade_date} 未获取到数据"+(f" 股票代码: {ts_code}" if ts_code else ""))
                return pd.DataFrame()
            
            logger.info(f"WAN {wan_idx} 成功获取日期 {trade_date} 的日线数据"+(f" 股票代码: {ts_code}" if ts_code else f"，共 {len(df)} 条记录"))
            logger.debug(f"数据获取耗时: {elapsed:.2f}s")
            return df
        except Exception as e:
            logger.error(f"WAN接口获取日期 {trade_date} 的日线数据失败: {str(e)}")
            return pd.DataFrame()
    
    def fetch_daily_by_code_with_wan(self, ts_code: str, start_date: str = None, end_date: str = None, wan_info: Tuple[int, int] = None) -> pd.DataFrame:
        """
        使用WAN接口按股票代码获取日线行情数据
        
        Args:
            ts_code: 股票代码
            start_date: 可选，开始日期，格式为YYYYMMDD
            end_date: 可选，结束日期，格式为YYYYMMDD
            wan_info: WAN接口和端口信息(wan_idx, port)
            
        Returns:
            DataFrame形式的日线数据
        """
        try:
            params = {"ts_code": ts_code}
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
                
            logger.debug(f"使用WAN接口获取股票 {ts_code} 的日线数据"+(f" 日期范围: {start_date} 至 {end_date}" if start_date and end_date else ""))
            
            # 准备API参数
            api_name = "daily"
            fields = self.interface_config.get("available_fields", [])
            
            # 创建WAN专用客户端
            if not wan_info:
                logger.warning("未提供WAN接口信息，使用普通客户端获取数据")
                return self.fetch_daily_by_code(ts_code=ts_code, start_date=start_date, end_date=end_date)
            
            wan_idx, port = wan_info
            client = TushareClientWAN(token=self.token, timeout=120, api_url=self.api_url)
            client.set_local_address('0.0.0.0', port)
            
            # 获取数据
            start_time = time.time()
            df = client.get_data(api_name=api_name, params=params, fields=fields)
            elapsed = time.time() - start_time
            
            if df is None or df.empty:
                logger.warning(f"WAN {wan_idx} 获取股票 {ts_code} 未获取到数据"+(f" 日期范围: {start_date} 至 {end_date}" if start_date and end_date else ""))
                return pd.DataFrame()
            
            record_count = len(df)
            
            # 检查返回的数据量是否接近限制，如果是则可能数据不完整
            if record_count >= self.batch_size * 0.9:  # 如果返回的数据量超过批次大小的90%
                logger.warning(f"WAN {wan_idx} 股票 {ts_code} 返回数据量 {record_count} 接近API限制，数据可能不完整，建议缩小时间范围")
            
            logger.info(f"WAN {wan_idx} 成功获取股票 {ts_code} 的日线数据，共 {record_count} 条记录")
            logger.debug(f"数据获取耗时: {elapsed:.2f}s")
            return df
                
        except Exception as e:
            logger.error(f"WAN接口获取股票 {ts_code} 的日线数据失败: {str(e)}")
            return pd.DataFrame()
            
    def fetch_data_parallel(self, ts_codes: Set[str], start_date: str, end_date: str, batch_size: int = 10) -> pd.DataFrame:
        """
        使用多WAN口并行获取多个股票的日线行情数据
        
        Args:
            ts_codes: 股票代码集合
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
            batch_size: 每批处理的股票数量，默认为10
            
        Returns:
            所有股票的日线行情数据合并后的DataFrame
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
        logger.info(f"开始并行批量获取 {len(ts_codes_list)} 个股票的日线行情数据，分为 {total_batches} 个批次处理")
        
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
        
        # 处理批次的线程函数
        def process_batch(batch_index, batch_ts_codes, wan_idx):
            try:
                with log_lock:
                    logger.debug(f"WAN {wan_idx} 开始处理批次 {batch_index+1}/{total_batches}")
                
                # 获取WAN接口和端口
                wan_info = self._get_wan_socket(wan_idx)
                if not wan_info:
                    with log_lock:
                        logger.warning(f"无法为WAN {wan_idx} 获取端口，跳过批次 {batch_index+1}")
                    result_queue.put((batch_index, None, wan_idx))
                    return
                
                batch_data = []
                for code in batch_ts_codes:
                    # 使用WAN接口获取数据
                    df = self.fetch_daily_by_code_with_wan(code, start_date, end_date, wan_info)
                    if not df.empty:
                        # 立即保存数据
                        self.save_to_mongodb(df)
                        batch_data.append(df)
                
                # 合并批次数据
                batch_df = pd.concat(batch_data, ignore_index=True) if batch_data else pd.DataFrame()
                
                # 记录结果
                with log_lock:
                    if not batch_df.empty:
                        logger.debug(f"WAN {wan_idx} 批次 {batch_index+1} 成功获取 {len(batch_df)} 条记录")
                    else:
                        logger.debug(f"WAN {wan_idx} 批次 {batch_index+1} 无数据")
                        
                # 放入结果队列
                result_queue.put((batch_index, batch_df, wan_idx))
                
                # 释放WAN端口
                wan_port = wan_info[1]
                self.port_allocator.release_port(wan_idx, wan_port)
                
                # 增加短暂休眠，避免API调用过于频繁
                time.sleep(1)
                
            except Exception as e:
                with log_lock:
                    logger.error(f"WAN {wan_idx} 处理批次 {batch_index+1} 失败: {str(e)}")
                import traceback
                logger.debug(f"详细错误信息: {traceback.format_exc()}")
                result_queue.put((batch_index, None, wan_idx))
                
                # 确保释放WAN端口
                if 'wan_info' in locals() and wan_info:
                    wan_port = wan_info[1]
                    self.port_allocator.release_port(wan_idx, wan_port)
        
        # 启动处理线程
        start_time_total = time.time()
        processed_batches = 0
        success_batches = 0
        total_records = 0
        all_data_sample = None  # 保存样本数据
        
        # 分配批次到不同WAN接口 - 轮询方式
        for i in range(0, len(ts_codes_list), batch_size):
            batch_index = i // batch_size
            batch_ts_codes = ts_codes_list[i:i+batch_size]
            
            # 选择WAN接口 - 简单轮询
            wan_idx = available_wans[batch_index % len(available_wans)]
            
            # 创建线程处理批次
            thread = threading.Thread(
                target=process_batch, 
                args=(batch_index, batch_ts_codes, wan_idx)
            )
            threads.append(thread)
            thread.start()
            
            # 控制并发线程数，避免创建过多线程
            max_concurrent = min(len(available_wans) * 2, 8)  # 降低并发数：每个WAN最多2个线程，总共不超过8个
            if len(threads) >= max_concurrent:
                # 等待一个线程完成
                while result_queue.empty():
                    time.sleep(0.1)
                
                # 处理一个结果
                batch_idx, batch_df, wan_idx = result_queue.get()
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
                
                # 添加额外等待时间，减少端口冲突
                time.sleep(0.5)
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
            
        # 处理剩余结果
        while not result_queue.empty():
            batch_idx, batch_df, wan_idx = result_queue.get()
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
        
        # 完成处理
        elapsed_total = time.time() - start_time_total
        logger.success(f"并行处理成功获取 {success_batches}/{total_batches} 个批次的日线行情数据，共 {total_records} 条记录，耗时 {elapsed_total:.2f}s")
        
        return all_data_sample if all_data_sample is not None else pd.DataFrame()
        
    def fetch_data_batch(self, ts_codes: Set[str], start_date: str, end_date: str, batch_size: int = 100, minute_rate_limit: int = 500, hour_rate_limit: int = 4000) -> pd.DataFrame:
        """
        批量获取多个股票的日线行情数据
        
        Args:
            ts_codes: 股票代码集合
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
            batch_size: 每批处理的股票数量
            minute_rate_limit: 每分钟API调用限制
            hour_rate_limit: 每小时API调用限制
        
        Returns:
            所有股票的日线行情数据合并后的DataFrame的样本
        """
        if not ts_codes:
            logger.warning("没有股票代码可以查询")
            return pd.DataFrame()
            
        # 将集合转换为列表，便于分批处理
        ts_codes_list = list(ts_codes)
        
        # 配置参数
        total_batches = (len(ts_codes_list) + batch_size - 1) // batch_size
        logger.info(f"开始批量获取 {len(ts_codes_list)} 个股票的日线行情数据，分为 {total_batches} 个批次处理")
        
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
            
            # 获取批次数据
            batch_data, rate_controllers, is_success = self._fetch_batch(
                batch_ts_codes, 
                start_date, 
                end_date, 
                rate_controllers
            )
            
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
        logger.success(f"批量处理成功获取 {success_batches}/{total_batches} 个批次的日线行情数据，共 {total_records} 条记录，耗时 {elapsed_total:.2f}s")
        
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
            df = self.fetch_daily_by_code(ts_code=ts_code, start_date=start_date, end_date=end_date)
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

    def run(self, config: Optional[Dict[str, Any]] = None) -> bool:
        """
        运行数据获取和保存流程，支持自定义配置
        
        Args:
            config: 配置字典，包含start_date, end_date, full等信息
            
        Returns:
            是否成功
        """
        # 使用默认配置
        default_config = {
            "start_date": None,
            "end_date": None,
            "full": False,
            "batch_size": 10,  # 每批次处理股票数量
            "minute_rate_limit": 500,
            "hour_rate_limit": 4000,
            "retry_count": 3,
            "use_parallel": True  # 是否使用并行处理
        }
        
        # 合并配置
        if config is None:
            config = {}
        
        effective_config = {**default_config, **config}
        start_date = effective_config["start_date"]
        end_date = effective_config["end_date"]
        full = effective_config["full"]
        batch_size = effective_config["batch_size"]
        minute_rate_limit = effective_config["minute_rate_limit"]
        hour_rate_limit = effective_config["hour_rate_limit"]
        use_parallel = effective_config["use_parallel"]
        
        # 如果未提供日期并且不是全量模式，则设置默认为最近一周
        if not start_date and not end_date and not full:
            today = datetime.now()
            end_date = today.strftime('%Y%m%d')  # 今天
            start_date = (today - timedelta(days=7)).strftime('%Y%m%d')  # 一周前
            logger.info(f"设置默认日期范围: {start_date} - {end_date}")
        
        # 如果指定了full模式，获取全量数据
        elif full:
            start_date = "19901219"  # A股市场最早交易日（上证）
            end_date = datetime.now().strftime('%Y%m%d')  # 今天
            logger.info(f"使用full模式：获取 {start_date} 至 {end_date} 期间的全部数据")
        
        # 从stock_basic集合获取目标股票代码
        target_ts_codes = self.get_target_ts_codes_from_stock_basic()
        if not target_ts_codes:
            logger.error("未能从stock_basic集合获取目标股票代码")
            return False
        
        logger.info(f"成功获取 {len(target_ts_codes)} 个目标股票代码")
        
        # 批量获取日线行情数据
        if use_parallel and self.port_allocator:
            # 使用多WAN口并行抓取
            logger.info("使用多WAN口并行获取数据")
            result_df = self.fetch_data_parallel(
                ts_codes=target_ts_codes, 
                start_date=start_date, 
                end_date=end_date,
                batch_size=batch_size
            )
        else:
            # 使用普通批量获取
            logger.info("使用普通批量方式获取数据")
            result_df = self.fetch_data_batch(
                ts_codes=target_ts_codes, 
                start_date=start_date, 
                end_date=end_date,
                batch_size=batch_size,
                minute_rate_limit=minute_rate_limit,
                hour_rate_limit=hour_rate_limit
            )
        
        # 查看结果
        if result_df.empty:
            logger.warning("未获取到任何日线行情数据")
            return False
            
        logger.success(f"日线行情数据获取和保存成功，获取了样本数据 {len(result_df)} 条记录")
        return True

    def fetch_by_date_range_daily(self, start_date: str, end_date: str, target_ts_codes: Set[str]) -> bool:
        """
        按日期范围逐日获取所有股票数据，然后过滤目标板块的股票
        用于recent模式，适合获取最近几天的数据
        
        Args:
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD
            target_ts_codes: 目标股票代码集合
        
        Returns:
            是否成功
        """
        try:
            # 计算日期范围
            start_date_obj = datetime.strptime(start_date, '%Y%m%d')
            end_date_obj = datetime.strptime(end_date, '%Y%m%d')
            
            # 确保日期顺序正确
            if start_date_obj > end_date_obj:
                start_date_obj, end_date_obj = end_date_obj, start_date_obj
                start_date = start_date_obj.strftime('%Y%m%d')
                end_date = end_date_obj.strftime('%Y%m%d')
            
            # 生成日期列表
            date_list = []
            current_date = start_date_obj
            while current_date <= end_date_obj:
                date_list.append(current_date.strftime('%Y%m%d'))
                current_date += timedelta(days=1)
            
            # 计算总天数
            total_days = len(date_list)
            logger.info(f"按日期获取数据，日期范围: {start_date} 至 {end_date}，共 {total_days} 天")
            
            # 检查是否可以使用多WAN口并行处理
            use_parallel = False
            available_wans = []
            if self.port_allocator:
                available_wans = self.port_allocator.get_available_wan_indices()
                use_parallel = len(available_wans) > 0
            
            # 多WAN口并行处理各个日期
            if use_parallel and total_days > 1:
                return self._fetch_parallel_by_dates(date_list, target_ts_codes, available_wans)
            else:
                return self._fetch_sequential_by_dates(date_list, target_ts_codes)
                
        except Exception as e:
            logger.error(f"按日期范围获取数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            return False

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
                    df = self.fetch_daily_by_date(trade_date=date_str)
                else:
                    # 使用WAN口获取数据
                    wan_idx, port = wan_info
                    # 使用WAN接口获取当日数据
                    df = self.fetch_daily_by_date_with_wan(trade_date=date_str, wan_info=wan_info)
                    # 释放WAN端口
                    self.port_allocator.release_port(wan_idx, port)
                
                success = False
                records_count = 0
                
                if not df.empty:
                    # 过滤目标板块股票
                    df_filtered = self.filter_daily_data(df, target_ts_codes)
                    
                    # 保存到MongoDB
                    if not df_filtered.empty:
                        save_success = self.save_to_mongodb(df_filtered)
                        records_count = len(df_filtered)
                        success = save_success
                
                # 放入结果队列
                result_queue.put((date_str, success, records_count))
                
                with log_lock:
                    if success:
                        logger.success(f"WAN {wan_idx} 成功处理日期 {date_str} 的数据，共 {records_count} 条记录")
                    else:
                        logger.warning(f"WAN {wan_idx} 处理日期 {date_str} 失败或无数据")
            
            except Exception as e:
                with log_lock:
                    logger.error(f"WAN {wan_idx} 处理日期 {date_str} 出错: {str(e)}")
                    import traceback
                    logger.debug(f"详细错误信息: {traceback.format_exc()}")
                result_queue.put((date_str, False, 0))
                
                # 确保释放WAN端口
                if 'wan_info' in locals() and wan_info:
                    wan_idx, port = wan_info
                    self.port_allocator.release_port(wan_idx, port)
        
        # 循环处理所有日期，控制最大线程数
        start_time = time.time()
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
                    time.sleep(0.1)
                
                # 处理结果
                date_str, success, records_count = result_queue.get()
                processed_days += 1
                if success:
                    success_days += 1
                    total_records += records_count
                
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
            date_str, success, records_count = result_queue.get()
            processed_days += 1
            if success:
                success_days += 1
                total_records += records_count
        
        # 处理完成
        elapsed_total = time.time() - start_time
        logger.success(f"按日期并行获取数据完成，成功处理 {success_days}/{total_days} 天，共保存 {total_records} 条记录，耗时 {elapsed_total:.1f}s")
        return total_records > 0
    
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
        
        start_time = time.time()
        
        # 逐日获取数据
        for date_str in date_list:
            logger.info(f"正在获取日期 {date_str} 的日线数据...")
            
            # 获取当日所有股票数据
            df = self.fetch_daily_by_date(trade_date=date_str)
            
            if not df.empty:
                # 过滤目标板块股票
                df_filtered = self.filter_daily_data(df, target_ts_codes)
                
                # 保存到MongoDB
                if not df_filtered.empty:
                    success = self.save_to_mongodb(df_filtered)
                    if success:
                        success_days += 1
                        total_records += len(df_filtered)
                        logger.success(f"成功保存 {date_str} 的日线数据，{len(df_filtered)} 条记录")
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
        logger.success(f"按日期顺序获取数据完成，成功处理 {success_days}/{total_days} 天，共保存 {total_records} 条记录，耗时 {elapsed_total:.1f}s")
        return total_records > 0

def main():
    """主函数"""
    import argparse
    from datetime import datetime, timedelta
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="日线行情数据获取工具")
    parser.add_argument("--verbose", action="store_true", help="输出详细日志")
    parser.add_argument("--mock", action="store_true", help="使用模拟数据模式（API不可用时）")
    parser.add_argument("--start-date", type=str, help="开始日期，格式为YYYYMMDD，如20100101")
    parser.add_argument("--end-date", type=str, help="结束日期，格式为YYYYMMDD，如20201231")
    parser.add_argument("--recent", action="store_true", help="仅获取最近一周的数据更新（默认模式）")
    parser.add_argument("--full", action="store_true", help="获取完整历史数据而非默认的最近一周数据")
    parser.add_argument("--ts-code", type=str, help="指定股票代码，例如600000.SH")
    parser.add_argument("--batch-size", type=int, default=10, help="每批次处理的股票数量，默认10")
    parser.add_argument("--market-codes", type=str, default="00,30,60,68", help="目标市场代码，用逗号分隔，默认为00,30,60,68")
    parser.add_argument("--no-parallel", dest="use_parallel", action="store_false", help="不使用并行处理")
    parser.add_argument("--minute-rate-limit", type=int, default=500, help="每分钟API调用限制")
    parser.add_argument("--hour-rate-limit", type=int, default=4000, help="每小时API调用限制")
    parser.set_defaults(use_parallel=True)
    args = parser.parse_args()
    
    # 解析市场代码
    target_market_codes = set(args.market_codes.split(','))
    
    # 创建获取器
    fetcher = DailyFetcher(
        verbose=args.verbose, 
        batch_size=args.batch_size,
        target_market_codes=target_market_codes
    )
    
    # 获取日线行情数据
    if args.mock:
        logger.warning("使用模拟数据模式，生成随机的日线行情数据")
        # 使用mock数据的逻辑（暂未实现）
    else:
        if args.ts_code:
            # 如果指定了单个股票代码，使用原来的方式获取
            start_date = args.start_date
            end_date = args.end_date
            
            # 设置默认日期范围
            if not start_date or not end_date:
                today = datetime.now()
                end_date = today.strftime('%Y%m%d')  # 今天
                start_date = (today - timedelta(days=7)).strftime('%Y%m%d')  # 一周前
                logger.info(f"为指定股票设置默认日期范围: {start_date} 至 {end_date}")
            
            logger.info(f"获取单个股票 {args.ts_code} 的日线数据，日期范围: {start_date} 至 {end_date}")
            result_df = fetcher.fetch_data_for_date_range(
                start_date=start_date,
                end_date=end_date,
                ts_code=args.ts_code
            )
            
            # 显示样例数据
            if not result_df.empty:
                logger.info(f"数据获取和保存成功，以下是数据样例（最多100条）:")
                if args.verbose:
                    for _, row in result_df.iterrows():
                        logger.debug(f"  {row['ts_code']} - {row['trade_date']} - 开盘: {row['open']} - 收盘: {row['close']}")
            else:
                logger.warning("未获取到任何日线行情数据")
        else:
            # 使用批量获取方式
            # 构建运行配置字典
            run_config = {
                "start_date": args.start_date,
                "end_date": args.end_date,
                "full": args.full,
                "batch_size": args.batch_size,
                "minute_rate_limit": args.minute_rate_limit,
                "hour_rate_limit": args.hour_rate_limit,
                "use_parallel": args.use_parallel
            }
            
            # 使用配置字典运行
            logger.info(f"使用批量模式获取日线数据，目标市场代码: {', '.join(target_market_codes)}")
            success = fetcher.run(config=run_config)
            
            if success:
                logger.success("日线数据获取和保存成功")
            else:
                logger.error("日线数据获取或保存失败")

if __name__ == "__main__":
    main()
