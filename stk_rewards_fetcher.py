#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
股票管理层薪酬及持股数据获取器 - 获取管理层薪酬及持股数据并保存到MongoDB

该脚本用于从湘财Tushare获取股票管理层薪酬及持股数据，并保存到MongoDB数据库中，仅保留00、30、60、68板块的股票

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=194

使用方法：
    python stk_rewards_fetcher.py              # 使用湘财真实API数据，简洁日志模式
    python stk_rewards_fetcher.py --verbose     # 使用湘财真实API数据，详细日志模式
    python stk_rewards_fetcher.py --mock        # 使用模拟数据模式（API不可用时）
    python stk_rewards_fetcher.py --full        # 获取所有历史数据（默认只获取最近一周数据）
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
import random
import pymongo
import requests.adapters
import socket
import requests

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


class StkRewardsFetcher:
    """
    股票管理层薪酬及持股数据获取器
    
    该类用于从Tushare获取股票管理层薪酬及持股数据并保存到MongoDB数据库，支持按市场代码过滤
    """

    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "stk_rewards.json",
        target_market_codes: Set[str] = {"00", "30", "60", "68"},
        db_name: str = None,
        collection_name: str = "stk_rewards",
        verbose: bool = False
    ):
        """
        初始化股票管理层薪酬及持股数据获取器
        
        Args:
            config_path: 配置文件路径
            interface_dir: 接口配置文件目录
            interface_name: 接口名称
            target_market_codes: 目标市场代码集合
            db_name: MongoDB数据库名称，如果为None则从配置文件中读取
            collection_name: MongoDB集合名称
            verbose: 是否输出详细日志
        """
        self.config_path = config_path
        self.interface_dir = interface_dir
        self.interface_name = interface_name
        self.target_market_codes = target_market_codes
        self.collection_name = collection_name
        self.verbose = verbose

        # 设置日志级别
        log_level = "DEBUG" if verbose else "INFO"
        logger.remove()
        logger.add(sys.stderr, level=log_level, format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
        
        # 加载配置
        self.config = self._load_config()
        self.interface_config = self._load_interface_config()
        
        # 获取token和api_url - 从配置文件读取
        tushare_config = self.config.get("tushare", {})
        self.token = tushare_config.get("token", "")
        self.api_url = tushare_config.get("api_url", "")
        
        # 从配置中读取db_name
        mongodb_config = self.config.get("mongodb", {})
        config_db_name = mongodb_config.get("db_name", "tushare_data")
        # 如果未传入db_name或传入为None，则使用配置文件中的值
        self.db_name = db_name if db_name is not None else config_db_name
        logger.debug(f"使用数据库名称: {self.db_name}")
        
        if not self.token:
            logger.error("未配置Tushare API Key")
            sys.exit(1)
        
        # 以安全方式记录token
        mask_token = self.token[:4] + '*' * (len(self.token) - 8) + self.token[-4:] if len(self.token) > 8 else '***'
        logger.debug(f"获取到的API token长度: {len(self.token)}")
        logger.debug(f"获取到的API URL: {self.api_url}")
        
        # 初始化原始Tushare客户端（用于非WAN场景）
        self.client = self._init_client()
        
        # 初始化MongoDB客户端
        self.mongo_client = self._init_mongo_client()
        
        # 初始化多WAN口管理器
        self.port_allocator = self._init_port_allocator()


    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                return config
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            sys.exit(1)

    def _load_interface_config(self) -> Dict[str, Any]:
        """加载接口配置文件"""
        config_path = os.path.join(self.interface_dir, self.interface_name)
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"加载接口配置失败 {self.interface_name}: {str(e)}")
        
        logger.warning(f"接口配置文件不存在: {config_path}，将使用默认配置")
        return {
            "description": "高管薪酬及持股",
            "api_name": "stk_rewards",
            "fields": [],
            "params": {},
            "available_fields": [
                "ts_code", "ann_date", "end_date", "name", "title", 
                "reward", "hold_vol", "hold_change"
            ],
            "index_fields": ["ts_code", "name", "end_date"]
        }

    def _init_client(self) -> TushareClient:
        """初始化Tushare客户端"""
        try:
            # 从配置中获取token和api_url
            tushare_config = self.config.get("tushare", {})
            token = tushare_config.get("token", "")
            api_url = tushare_config.get("api_url", "")
            
            # 验证token
            if not token:
                logger.error("未配置Tushare API Key")
                sys.exit(1)
            
            # 这里我们记录一下token的前几位和长度，便于调试
            # 注意：实际生产环境中应谨慎记录敏感信息
            mask_token = token[:4] + '*' * (len(token) - 8) + token[-4:] if len(token) > 8 else '***'
            logger.debug(f"使用token初始化客户端: {mask_token} (长度: {len(token)}), API URL: {api_url}")
                
            # 创建并返回客户端实例，传入api_url
            return TushareClient(token=token, api_url=api_url)
        except Exception as e:
            logger.error(f"初始化Tushare客户端失败: {str(e)}")
            sys.exit(1)

    def _init_mongo_client(self) -> MongoDBClient:
        """初始化MongoDB客户端"""
        try:
            mongodb_config = self.config.get("mongodb", {})
            
            # 获取MongoDB连接信息
            uri = mongodb_config.get("uri", "")
            host = mongodb_config.get("host", "localhost")
            port = mongodb_config.get("port", 27017)
            username = mongodb_config.get("username", "")
            password = mongodb_config.get("password", "")
            auth_source = mongodb_config.get("auth_source", "admin")
            auth_mechanism = mongodb_config.get("auth_mechanism", "SCRAM-SHA-1")
            
            # 获取MongoDB连接选项
            options = mongodb_config.get("options", {})
            connection_pool_size = options.get("max_pool_size", 100)
            timeout_ms = options.get("connect_timeout_ms", 30000)
            
            # 记录MongoDB连接信息
            logger.debug(f"MongoDB连接信息: {host}:{port}, 认证源: {auth_source}, 认证机制: {auth_mechanism}")
            logger.debug(f"MongoDB连接选项: 连接池大小: {connection_pool_size}, 超时: {timeout_ms}ms")
            logger.debug(f"MongoDB数据库名称: {self.db_name}")
            
            # 创建MongoDB客户端 - 使用从配置或参数中获取的数据库名称
            mongo_client = MongoDBClient(
                uri=uri,
                host=host,
                port=port,
                username=username,
                password=password,
                db_name=self.db_name,  # 使用从配置或初始化参数中获取的db_name
                auth_source=auth_source,
                auth_mechanism=auth_mechanism,
                connection_pool_size=connection_pool_size,
                timeout_ms=timeout_ms
            )
            
            # 连接到数据库
            if not mongo_client.connect():
                logger.error("连接MongoDB失败")
                sys.exit(1)
                
            return mongo_client
        except Exception as e:
            logger.error(f"初始化MongoDB客户端失败: {str(e)}")
            sys.exit(1)

    def _init_port_allocator(self) -> Optional[PortAllocator]:
        """初始化多WAN口管理器"""
        try:
            # 检查是否启用WAN接口
            wan_config = self.config.get("wan", {})
            wan_enabled = wan_config.get("enabled", False)
            
            if not wan_enabled:
                logger.warning("多WAN口功能未启用，将使用系统默认网络接口")
                return None
                
            # 获取WAN接口配置
            if not wan_config.get("port_ranges"):
                logger.warning("未配置WAN接口端口范围，将使用系统默认网络接口")
                return None
            
            # 使用全局端口分配器
            from wan_manager.port_allocator import port_allocator
            
            # 检查是否有可用WAN接口
            available_indices = port_allocator.get_available_wan_indices()
            if not available_indices:
                logger.warning("没有可用的WAN接口，将使用系统默认网络接口")
                return None
                
            logger.debug(f"已初始化多WAN口管理器，可用接口索引: {available_indices}")
            return port_allocator
        except Exception as e:
            logger.error(f"初始化多WAN口管理器失败: {str(e)}")
            return None

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

    def get_target_ts_codes_from_stock_basic(self) -> Set[str]:
        """
        从stock_basic集合中获取目标板块的股票代码
        
        Returns:
            目标板块股票代码集合
        """
        try:
            # 确保MongoDB连接
            if not self.mongo_client.is_connected():
                logger.warning("MongoDB未连接，尝试重新连接...")
                if not self.mongo_client.connect():
                    logger.error("重新连接MongoDB失败")
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
            result = self.mongo_client.find("stock_basic", query, projection={"ts_code": 1, "_id": 0})
            
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

    def fetch_stk_rewards_for_ts_codes(self, ts_codes: List[str], start_date: str = None, end_date: str = None, wan_info: Tuple[int, int] = None) -> pd.DataFrame:
        """
        批量获取多个股票的管理层薪酬及持股数据
        
        Args:
            ts_codes: 股票代码列表
            start_date: 开始日期，格式YYYYMMDD（不使用）
            end_date: 结束日期，格式YYYYMMDD（不使用）
            wan_info: WAN接口和端口信息
            
        Returns:
            管理层薪酬及持股数据DataFrame
        """
        if not ts_codes:
            logger.warning("没有提供股票代码，无法获取数据")
            return pd.DataFrame()
            
        # 将股票代码列表转换为逗号分隔的字符串
        ts_codes_str = ",".join(ts_codes)
        
        # 准备参数
        api_name = self.interface_config.get("api_name", "stk_rewards")
        params = self.interface_config.get("params", {}).copy()
        fields = self.interface_config.get("fields", [])
        
        # 添加查询参数 - 只添加ts_code，不添加日期参数
        params["ts_code"] = ts_codes_str
        
        # 确保使用正确的字段（根据接口定义）
        if not fields:
            fields = self.interface_config.get("available_fields", [])
        
        # 使用传入的WAN接口
        use_wan = wan_info is not None
        
        # 设置最大重试次数
        max_retries = 3
        retry_count = 0
        retry_delay = 2  # 初始延迟时间（秒）
        
        # 验证token是否有效
        if not self.token:
            logger.error("无效的token，请检查配置")
            return pd.DataFrame()
        
        while retry_count <= max_retries:
            try:
                # 调用Tushare API
                if self.verbose:
                    logger.debug(f"批量获取 {len(ts_codes)} 个股票的管理层薪酬及持股数据...")
                    if self.verbose > 1:
                        logger.debug(f"API参数: {params}")
                
                # 选择客户端：如果是WAN模式，使用WAN专用客户端
                client = None
                start_time = time.time()
                
                if use_wan:
                    wan_idx, port = wan_info
                    logger.debug(f"使用WAN接口 {wan_idx} 和本地端口 {port} 请求数据")
                    
                    # 创建WAN专用客户端，确保传递正确的token和api_url
                    logger.debug(f"使用token (长度: {len(self.token)}) 创建WAN客户端")
                    client = TushareClientWAN(token=self.token, timeout=120, api_url=self.api_url)
                    client.set_local_address('0.0.0.0', port)
                else:
                    # 使用普通客户端
                    client = self.client
                    client.set_timeout(120)
                
                # 获取数据
                df = client.get_data(api_name=api_name, params=params, fields=fields)
                
                elapsed = time.time() - start_time
                
                if df is None or df.empty:
                    if self.verbose:
                        logger.debug(f"批量查询的 {len(ts_codes)} 个股票无管理层薪酬及持股数据")
                    
                    # 释放WAN端口（如果使用了）
                    if use_wan:
                        self.port_allocator.release_port(wan_idx, port)
                        
                    return pd.DataFrame()
                
                if self.verbose:
                    logger.debug(f"成功获取 {len(df)} 条记录，涉及 {len(ts_codes)} 个股票")
                    logger.debug(f"批量获取 {len(ts_codes)} 个股票数据耗时 {elapsed:.2f}s，平均每个股票 {elapsed/len(ts_codes):.3f}s")
                
                # 释放WAN端口（如果使用了）
                if use_wan:
                    self.port_allocator.release_port(wan_idx, port)
                
                return df
                
            except Exception as e:
                retry_count += 1
                
                # 释放WAN端口（如果使用了）
                if use_wan:
                    wan_idx, port = wan_info
                    self.port_allocator.release_port(wan_idx, port)
                
                # 如果还有重试机会
                if retry_count <= max_retries:
                    # 指数退避算法计算等待时间
                    wait_time = retry_delay * (2 ** (retry_count - 1))
                    
                    # 加入随机抖动，避免多个请求同时重试
                    jitter = random.uniform(0, 0.1 * wait_time)
                    wait_time += jitter
                    
                    logger.warning(f"获取数据失败 (尝试 {retry_count}/{max_retries}): {str(e)}，将在 {wait_time:.2f} 秒后重试")
                    time.sleep(wait_time)
                else:
                    # 超过最大重试次数
                    logger.error(f"获取数据失败，已达最大重试次数: {str(e)}")
                    import traceback
                    logger.debug(f"详细错误信息: {traceback.format_exc()}")
                    return pd.DataFrame()
        
        # 默认返回空DataFrame (虽然不会执行到这里)
        return pd.DataFrame()

    def fetch_stk_rewards_parallel(self, ts_codes: Set[str], start_date: str = None, end_date: str = None, batch_size: int = 10) -> pd.DataFrame:
        """
        使用多WAN口并行获取多个股票的管理层薪酬及持股数据
        
        Args:
            ts_codes: 股票代码集合
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
            batch_size: 每批处理的股票数量，默认为10
            
        Returns:
            所有股票的管理层薪酬及持股数据合并后的DataFrame
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
        logger.info(f"开始并行批量获取 {len(ts_codes_list)} 个股票的管理层薪酬及持股数据，分为 {total_batches} 个批次处理")
        
        # 获取可用的WAN接口
        available_wans = []
        if self.port_allocator:
            available_wans = self.port_allocator.get_available_wan_indices()
            logger.info(f"可用的WAN接口数量: {len(available_wans)}")
            
        if not available_wans:
            logger.warning("没有可用的WAN接口，将使用系统默认网络接口")
            # 如果没有可用WAN，回退到普通批处理
            return self.fetch_stk_rewards_batch(ts_codes, start_date, end_date, batch_size)
        
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
                
                # 获取批次数据
                batch_df = self.fetch_stk_rewards_for_ts_codes(batch_ts_codes, start_date, end_date, wan_info)
                
                # 记录结果
                with log_lock:
                    if batch_df is not None and not batch_df.empty:
                        logger.debug(f"WAN {wan_idx} 批次 {batch_index+1} 成功获取 {len(batch_df)} 条记录")
                    else:
                        logger.debug(f"WAN {wan_idx} 批次 {batch_index+1} 无数据")
                        
                # 放入结果队列
                result_queue.put((batch_index, batch_df, wan_idx))
                
                # 增加短暂休眠，避免API调用过于频繁，并减轻端口冲突
                time.sleep(1)
                
            except Exception as e:
                with log_lock:
                    logger.error(f"WAN {wan_idx} 处理批次 {batch_index+1} 失败: {str(e)}")
                result_queue.put((batch_index, None, wan_idx))
        
        # 启动处理线程
        start_time_total = time.time()
        processed_batches = 0
        success_batches = 0
        total_records = 0
        
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
                    all_data.append(batch_df)
                
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
                all_data.append(batch_df)
        
        # 合并所有数据
        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            logger.success(f"并行处理成功获取 {success_batches}/{total_batches} 个批次的管理层薪酬及持股数据，共 {len(result_df)} 条记录")
            return result_df
        else:
            logger.warning("没有获取到任何管理层薪酬及持股数据")
            return pd.DataFrame()

    def fetch_stk_rewards_batch(self, ts_codes: Set[str], start_date: str = None, end_date: str = None, batch_size: int = 100, minute_rate_limit: int = 500, hour_rate_limit: int = 4000) -> pd.DataFrame:
        """
        批量获取多个股票的管理层薪酬及持股数据
        
        Args:
            ts_codes: 股票代码集合
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
            batch_size: 每批处理的股票数量
            minute_rate_limit: 每分钟API调用限制
            hour_rate_limit: 每小时API调用限制
        
        Returns:
            所有股票的管理层薪酬及持股数据合并后的DataFrame
        """
        if not ts_codes:
            logger.warning("没有股票代码可以查询")
            return pd.DataFrame()
            
        # 将集合转换为列表，便于分批处理
        ts_codes_list = list(ts_codes)
        
        # 配置参数
        total_batches = (len(ts_codes_list) + batch_size - 1) // batch_size
        logger.info(f"开始批量获取 {len(ts_codes_list)} 个股票的管理层薪酬及持股数据，分为 {total_batches} 个批次处理")
        
        # 进度统计变量
        processed_batches = 0
        success_batches = 0
        total_records = 0
        start_time_total = time.time()
        all_data = []
        
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
            batch_df, rate_controllers, is_success = self._fetch_batch(
                batch_ts_codes, 
                start_date, 
                end_date, 
                rate_controllers
            )
            
            processed_batches += 1
            
            # 更新进度
            elapsed = time.time() - start_time_total
            avg_time_per_batch = elapsed / processed_batches if processed_batches > 0 else 0
            remaining = (total_batches - processed_batches) * avg_time_per_batch
            progress = processed_batches / total_batches * 100
            logger.info(f"批次进度: {processed_batches}/{total_batches} ({progress:.1f}%)，已处理时间: {elapsed:.1f}s，预估剩余: {remaining:.1f}s")
            
            if is_success:
                success_batches += 1
                total_records += len(batch_df)
                all_data.append(batch_df)
            
            # 增加短暂休眠，避免API调用过于频繁，并减轻端口冲突
            time.sleep(0.5)
            
        # 合并所有数据
        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            logger.success(f"成功获取 {success_batches}/{total_batches} 个批次的管理层薪酬及持股数据，共 {len(result_df)} 条记录")
            return result_df
        else:
            logger.warning("没有获取到任何管理层薪酬及持股数据")
            return pd.DataFrame()

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
        
        # 获取当前批次的数据
        wan_info = self._get_wan_socket()  # 获取一个默认WAN接口
        batch_df = self.fetch_stk_rewards_for_ts_codes(batch_ts_codes, start_date, end_date, wan_info)
        is_success = batch_df is not None and not batch_df.empty
        
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

    def save_to_mongodb(self, df: pd.DataFrame) -> bool:
        """
        将数据保存到MongoDB
        
        Args:
            df: 待保存的DataFrame
            
        Returns:
            是否成功保存
        """
        # 强制确保使用tushare_data作为数据库名称
        logger.info(f"保存数据到MongoDB数据库：{self.db_name}，集合：{self.collection_name}")
        
        if df is None or df.empty:
            logger.warning("没有数据可保存到MongoDB")
            return False
            
        try:
            # 将DataFrame转换为记录列表
            records = df.to_dict('records')
            
            # 保存到MongoDB
            start_time = time.time()
            
            # 确保MongoDB连接
            if not self.mongo_client.is_connected():
                logger.warning("MongoDB未连接，尝试重新连接...")
                if not self.mongo_client.connect():
                    logger.error("重新连接MongoDB失败")
                    return False
            
            # 获取MongoDB数据库和集合
            db = self.mongo_client.get_database(self.db_name)
            collection = db[self.collection_name]
            
            # 批量更新记录（使用标准MongoDB操作代替upsert_many）
            inserted_count = 0
            updated_count = 0
            unique_keys = ["ts_code", "name", "end_date"]
            
            # 批量处理，避免一次性处理太多记录
            batch_size = 1000
            total_batches = (len(records) + batch_size - 1) // batch_size
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                batch_result = self._batch_upsert(collection, batch, unique_keys)
                inserted_count += batch_result["inserted"]
                updated_count += batch_result["updated"]
                
                # 进度显示
                if self.verbose and total_batches > 1:
                    progress = (i + batch_size) / len(records) * 100
                    progress = min(progress, 100)
                    logger.debug(f"MongoDB保存进度: {i+len(batch)}/{len(records)} ({progress:.1f}%)")
            
            elapsed = time.time() - start_time
            
            # 创建索引
            try:
                # 根据接口配置中的index_fields创建索引
                index_fields = self.interface_config.get("index_fields", [])
                if index_fields:
                    # 为ts_code, name, end_date创建复合索引
                    if all(field in index_fields for field in ["ts_code", "name", "end_date"]):
                        collection.create_index(
                            [("ts_code", 1), ("name", 1), ("end_date", 1)],
                            unique=True,
                            background=True
                        )
                        logger.debug("已为字段组合 (ts_code, name, end_date) 创建唯一复合索引")
                    
                    # 创建其他单字段索引
                    for field in ["ts_code", "end_date"]:
                        if field in index_fields:
                            collection.create_index(field)
                            logger.debug(f"已为字段 {field} 创建索引")
                else:
                    # 默认创建索引
                    collection.create_index([("ts_code", 1), ("name", 1), ("end_date", 1)], unique=True)
                    collection.create_index("ts_code")
                    collection.create_index("end_date")
                    logger.debug("已创建默认索引")
            except Exception as e:
                logger.warning(f"创建索引时出错: {str(e)}")
            
            total_modified = inserted_count + updated_count
            logger.success(f"成功保存 {total_modified} 条记录到 MongoDB (新增: {inserted_count}, 更新: {updated_count})，耗时 {elapsed:.2f}s")
            return True
            
        except Exception as e:
            logger.error(f"保存到MongoDB失败: {str(e)}")
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
            包含插入和更新记录数的字典
        """
        inserted = 0
        updated = 0
        
        # 使用bulk操作提高效率
        bulk_operations = []
        
        for record in records:
            # 构建查询条件
            query = {key: record[key] for key in unique_keys if key in record}
            
            # 如果缺少唯一键字段，则直接插入
            if len(query) != len(unique_keys):
                bulk_operations.append(pymongo.InsertOne(record))
                inserted += 1
                continue
                
            # 否则执行更新
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
                # 重新统计真实的插入和更新数量
                inserted = result.inserted_count
                # upserted_count是新插入的，modified_count是更新的
                updated = result.upserted_count + result.modified_count
            except pymongo.errors.BulkWriteError as bwe:
                # 处理部分失败的情况
                if hasattr(bwe, 'details'):
                    if 'nInserted' in bwe.details:
                        inserted = bwe.details['nInserted']
                    if 'nUpserted' in bwe.details:
                        updated = bwe.details['nUpserted']
                    if 'nModified' in bwe.details:
                        updated += bwe.details['nModified']
                logger.warning(f"批量写入部分失败: {len(bwe.details.get('writeErrors', []))} 错误")
                
        return {"inserted": inserted, "updated": updated}

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
            "batch_size": 10,           # 降低每批次处理股票数量为10
            "minute_rate_limit": 500,
            "hour_rate_limit": 4000,
            "retry_count": 3,
            "use_parallel": True        # 是否使用并行处理
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
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
            logger.info(f"设置默认日期范围: {start_date} - {end_date}")
        
        # 从stock_basic集合获取目标股票代码
        target_ts_codes = self.get_target_ts_codes_from_stock_basic()
        if not target_ts_codes:
            logger.error("未能从stock_basic集合获取目标股票代码")
            return False
            
        # 批量获取管理层薪酬及持股数据
        if use_parallel and self.port_allocator:
            # 使用多WAN口并行抓取
            logger.info("使用多WAN口并行获取数据")
            df = self.fetch_stk_rewards_parallel(
                ts_codes=target_ts_codes, 
                start_date=start_date, 
                end_date=end_date,
                batch_size=batch_size
            )
        else:
            # 使用普通批量获取
            logger.info("使用普通批量方式获取数据")
            df = self.fetch_stk_rewards_batch(
                ts_codes=target_ts_codes, 
                start_date=start_date, 
                end_date=end_date,
                batch_size=batch_size,
                minute_rate_limit=minute_rate_limit,
                hour_rate_limit=hour_rate_limit
            )
        
        if df.empty:
            logger.warning("没有获取到任何管理层薪酬及持股数据")
            return False
            
        # 保存数据到MongoDB
        success = self.save_to_mongodb(df)
        
        # 关闭MongoDB连接
        self.mongo_client.close()
        
        return success


def create_mock_data() -> pd.DataFrame:
    """创建模拟数据用于测试"""
    logger.info("创建模拟管理层薪酬及持股数据用于测试")
    
    # 创建模拟数据
    data = [
        {'ts_code': '000001.SZ', 'ann_date': '20220430', 'end_date': '20211231', 'name': '张三', 'title': '董事长', 'reward': 500.00, 'hold_vol': 10000, 'hold_change': 0},
        {'ts_code': '000001.SZ', 'ann_date': '20220430', 'end_date': '20211231', 'name': '李四', 'title': '总经理', 'reward': 450.00, 'hold_vol': 8000, 'hold_change': 1000},
        {'ts_code': '000002.SZ', 'ann_date': '20220428', 'end_date': '20211231', 'name': '王五', 'title': '董事长', 'reward': 600.00, 'hold_vol': 15000, 'hold_change': 2000},
        {'ts_code': '000002.SZ', 'ann_date': '20220428', 'end_date': '20211231', 'name': '赵六', 'title': '财务总监', 'reward': 350.00, 'hold_vol': 5000, 'hold_change': -1000},
        {'ts_code': '300059.SZ', 'ann_date': '20220425', 'end_date': '20211231', 'name': '钱七', 'title': '董事长', 'reward': 800.00, 'hold_vol': 20000, 'hold_change': 5000},
        {'ts_code': '600000.SH', 'ann_date': '20220426', 'end_date': '20211231', 'name': '孙八', 'title': '董事长', 'reward': 700.00, 'hold_vol': 18000, 'hold_change': 3000},
        {'ts_code': '600519.SH', 'ann_date': '20220422', 'end_date': '20211231', 'name': '周九', 'title': '董事长', 'reward': 1200.00, 'hold_vol': 30000, 'hold_change': 8000},
        {'ts_code': '688981.SH', 'ann_date': '20220420', 'end_date': '20211231', 'name': '吴十', 'title': '董事长', 'reward': 900.00, 'hold_vol': 25000, 'hold_change': 4000}
    ]
    
    # 转换为DataFrame
    df = pd.DataFrame(data)
    
    logger.success(f"已创建 {len(df)} 条模拟管理层薪酬及持股数据")
    return df

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='获取股票管理层薪酬及持股数据并保存到MongoDB')
    parser.add_argument('--config', default='config/config.yaml', help='配置文件路径')
    parser.add_argument('--interface-dir', default='config/interfaces', help='接口配置文件目录')
    parser.add_argument('--market-codes', default='00,30,60,68', help='目标市场代码，用逗号分隔')
    parser.add_argument('--db-name', default='tushare_data', help='MongoDB数据库名称')
    parser.add_argument('--collection-name', default='stk_rewards', help='MongoDB集合名称')
    parser.add_argument('--start-date', help='开始日期，格式YYYYMMDD')
    parser.add_argument('--end-date', help='结束日期，格式YYYYMMDD')
    parser.add_argument('--full', action='store_true', help='获取所有历史数据（默认只获取最近一周）')
    parser.add_argument('--verbose', action='store_true', help='输出详细日志')
    parser.add_argument('--batch-size', type=int, default=1, help='每批请求的股票数量')
    parser.add_argument('--minute-rate-limit', type=int, default=100, help='每分钟API调用限制')
    parser.add_argument('--hour-rate-limit', type=int, default=2000, help='每小时API调用限制')
    parser.add_argument('--retry-count', type=int, default=3, help='API调用失败重试次数')
    parser.add_argument('--no-parallel', action='store_false', dest='use_parallel', help='不使用并行处理')
    parser.add_argument('--mock', action='store_false', dest='use_real_api', help='使用模拟数据（当API不可用时）')
    parser.add_argument('--use-real-api', action='store_true', default=True, help='使用湘财真实API数据（默认）')
    parser.add_argument('--dry-run', action='store_true', help='仅运行流程，不保存数据')
    args = parser.parse_args()
    
    # 解析市场代码
    target_market_codes = set(args.market_codes.split(','))
    
    # 创建获取器并运行
    fetcher = StkRewardsFetcher(
        config_path=args.config,
        interface_dir=args.interface_dir,
        target_market_codes=target_market_codes,
        db_name=args.db_name,  # 这个值会被内部强制设为"tushare_data"
        collection_name=args.collection_name,
        verbose=args.verbose
    )
    
    # 使用真实API或模拟数据模式
    if args.use_real_api:
        logger.info("使用湘财Tushare真实API获取数据")
        
        # 构建运行配置字典
        run_config = {
            "start_date": args.start_date,
            "end_date": args.end_date,
            "full": args.full,
            "batch_size": args.batch_size,
            "minute_rate_limit": args.minute_rate_limit,
            "hour_rate_limit": args.hour_rate_limit,
            "retry_count": args.retry_count,
            "use_parallel": args.use_parallel
        }
        
        # 使用配置字典运行
        success = fetcher.run(config=run_config)
    else:
        logger.info("使用模拟数据模式")
        # 创建模拟数据
        df = create_mock_data()
        
        # 获取目标股票代码以过滤模拟数据
        target_ts_codes = fetcher.get_target_ts_codes_from_stock_basic()
        if not target_ts_codes:
            # 模拟模式下，如果无法获取真实股票代码，使用模拟数据中的所有代码
            target_ts_codes = set(df['ts_code'].unique().tolist())
            logger.warning("无法从数据库获取股票代码，使用模拟数据中的所有代码")
        
        # 过滤数据，只保留目标股票代码
        df_filtered = df[df['ts_code'].isin(target_ts_codes)]
        
        if df_filtered.empty:
            logger.warning("过滤后没有符合条件的管理层薪酬及持股数据")
            sys.exit(1)
        
        # 是否实际保存
        if args.dry_run:
            logger.info("干运行模式，不保存数据")
            success = True
        else:
            # 保存数据到MongoDB
            success = fetcher.save_to_mongodb(df_filtered)
            # 关闭MongoDB连接
            fetcher.mongo_client.close()
    
    if success:
        logger.success("数据获取和保存成功")
        sys.exit(0)
    else:
        logger.error("数据获取或保存失败")
        sys.exit(1)

if __name__ == "__main__":
    main()
