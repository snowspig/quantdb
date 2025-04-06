#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
停牌数据获取器 - 获取A股停复牌信息并保存到MongoDB

该脚本用于从湘财Tushare获取A股停复牌信息，并保存到MongoDB数据库中
支持多WAN接口并行抓取、多种模式获取数据，并具备失败重试机制

参考接口文档：http://tushare.xcsc.com:7173/document/2?doc_id=31

使用方法：
    python suspend_fetcher.py                 # 使用recent模式获取最近一周的停牌数据
    python suspend_fetcher.py --full          # 获取从1990年至今的所有停牌数据
    python suspend_fetcher.py --verbose       # 输出详细日志
    python suspend_fetcher.py --mock          # 使用模拟数据（API不可用时）
    python suspend_fetcher.py --ts-code 600000.SH  # 获取指定股票的停牌数据
    python suspend_fetcher.py --batch-size 1      # 设置每批处理20只股票
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
import threading
import queue

# 添加项目根目录到Python路径
current_dir = Path(__file__).resolve().parent
sys.path.append(str(current_dir))

# 导入项目模块
from data_fetcher.tushare_client import TushareClient
from storage.mongodb_client import MongoDBClient
from wan_manager.port_allocator import PortAllocator

def tcp_port_is_free(port):
    """检查TCP端口是否可用"""
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(('0.0.0.0', port))
        s.listen(1)
        s.close()
        return True
    except OSError:
        if s:
            s.close()
        return False

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
        self.wan_idx = None
        self.local_port = None
        
        # 验证token
        mask_token = token[:4] + '*' * (len(token) - 8) + token[-4:] if len(token) > 8 else '***'
        logger.debug(f"TushareClientWAN初始化: {mask_token} (长度: {len(token)}), API URL: {self.url}")
    
    def set_local_address(self, host: str, port: int, wan_idx: int = None):
        """设置本地地址绑定"""
        # 检查端口是否可用
        if not tcp_port_is_free(port):
            logger.warning(f"端口 {port} 不可用，可能仍在TIME_WAIT状态")
            # 尝试选择临近端口
            for offset in range(1, 11):
                alt_port = port + offset
                if tcp_port_is_free(alt_port):
                    logger.info(f"选择替代端口 {alt_port} 替代不可用的端口 {port}")
                    port = alt_port
                    break
            else:
                logger.error(f"无法找到可用的替代端口")
                raise OSError(f"所有尝试的端口都不可用")
        
        self.local_addr = (host, port)
        self.wan_idx = wan_idx
        self.local_port = port
        logger.debug(f"已设置本地地址绑定: {host}:{port}, WAN索引: {wan_idx}")
    
    def reset_local_address(self):
        """重置本地地址绑定"""
        self.local_addr = None
        self.wan_idx = None
        self.local_port = None
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
        # 创建请求数据 - 与原始TushareClient请求格式保持一致
        req_params = {
            "api_name": api_name,
            "token": self.token,
            "params": params or {},
            "fields": fields or ""
        }
        
        logger.debug(f"请求URL: {self.url}, API: {api_name}, Token长度: {len(self.token)}")
        
        # 使用requests发送请求，增强错误处理
        start_time = time.time()
        
        try:
            # 使用类似wan_test_client的方式直接创建socket并绑定
            if self.local_addr and self.wan_idx is not None:
                logger.debug(f"使用WAN {self.wan_idx} 端口 {self.local_port} 发送请求")
                
                # 从URL解析主机和端口
                import urllib.parse
                parsed_url = urllib.parse.urlparse(self.url)
                host = parsed_url.hostname
                port = parsed_url.port or 80
                is_https = parsed_url.scheme == 'https'
                
                if is_https:
                    # 对于HTTPS，我们需要使用SSL
                    import ssl
                    context = ssl.create_default_context()
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.bind(self.local_addr)
                    s.settimeout(self.timeout)
                    wrapped_socket = context.wrap_socket(s, server_hostname=host)
                    wrapped_socket.connect((host, port))
                    
                    # 构建HTTP请求
                    request = f"POST {parsed_url.path} HTTP/1.1\r\n"
                    request += f"Host: {host}\r\n"
                    request += "Content-Type: application/json\r\n"
                    
                    # 添加其他请求头
                    for header, value in self.headers.items():
                        if header.lower() != "content-type" and header.lower() != "host":
                            request += f"{header}: {value}\r\n"
                    
                    # 添加内容长度
                    json_data = json.dumps(req_params)
                    request += f"Content-Length: {len(json_data)}\r\n"
                    request += "Connection: close\r\n\r\n"
                    request += json_data
                    
                    # 发送请求
                    wrapped_socket.sendall(request.encode())
                    
                    # 接收响应
                    response = b""
                    while True:
                        try:
                            data = wrapped_socket.recv(4096)
                            if not data:
                                break
                            response += data
                        except socket.timeout:
                            break
                    
                    wrapped_socket.close()
                    
                    # 解析HTTP响应
                    response_text = response.decode('utf-8', errors='ignore')
                    
                    # 提取JSON主体
                    body_start = response_text.find('\r\n\r\n')
                    if body_start != -1:
                        body = response_text[body_start + 4:]
                        
                        # 解析JSON
                        try:
                            result = json.loads(body)
                            # 检查响应状态
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
                        except json.JSONDecodeError:
                            logger.error("解析JSON响应失败")
                            return None
                    else:
                        logger.error("无法找到HTTP响应主体")
                        return None
                
                else:
                    # 对于HTTP请求，必须使用SourceAddressAdapter来绑定源地址
                    s = requests.Session()
                    # 使用自定义适配器绑定源地址
                    s.mount('http://', SourceAddressAdapter(self.local_addr))
                    s.mount('https://', SourceAddressAdapter(self.local_addr))
                    
                    response = s.post(
                        self.url,
                        json=req_params,
                        headers=self.headers,
                        timeout=self.timeout,
                        proxies=self.proxies
                    )
                    
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
            
            else:
                # 如果没有设置本地地址绑定，使用普通请求
                s = requests.Session()
                
                # 使用SourceAddressAdapter
                if self.local_addr:
                    s.mount('http://', SourceAddressAdapter(self.local_addr))
                    s.mount('https://', SourceAddressAdapter(self.local_addr))
                
                response = s.post(
                    self.url,
                    json=req_params,
                    headers=self.headers,
                    timeout=self.timeout,
                    proxies=self.proxies
                )
                
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
            elapsed = time.time() - start_time
            logger.error(f"获取API数据失败: {str(e)}")
            import traceback
            logger.debug(f"详细错误信息: {traceback.format_exc()}")
            logger.debug(f"请求耗时: {elapsed:.2f}s 后失败")
            raise  # 重新抛出异常，让调用者处理


class SourceAddressAdapter(requests.adapters.HTTPAdapter):
    """用于设置源地址的HTTP适配器"""
    
    def __init__(self, source_address, **kwargs):
        self.source_address = source_address
        super(SourceAddressAdapter, self).__init__(**kwargs)
    
    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs['source_address'] = self.source_address
        super(SourceAddressAdapter, self).init_poolmanager(
            connections, maxsize, block, **pool_kwargs)


class SuspendFetcher:
    """
    停牌数据获取器
    
    该类用于从Tushare获取A股停复牌信息并保存到MongoDB数据库，支持多种获取模式：
    1. recent模式：获取最近一周的停牌数据，其中 suspend_date = trade_date
    2. full模式：获取从1990年1月1日至今的所有停牌数据，按股票代码列表抓取
    """

    def __init__(
        self,
        config_path: str = "config/config.yaml",
        interface_dir: str = "config/interfaces",
        interface_name: str = "suspend.json",
        target_market_codes: Set[str] = {"00", "30", "60", "68"},
        db_name: str = None,
        collection_name: str = "suspend",
        verbose: bool = False,
        max_workers: int = 3,  # 并行工作线程数
        retry_count: int = 5,   # 数据获取重试次数
        retry_delay: int = 5,   # 重试延迟时间(秒)
        batch_size: int = 1,    # 每批次处理的股票数量
        port_release_timeout: int = 120  # 端口释放超时时间(秒)
    ):
        """
        初始化停牌数据获取器
        
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
            batch_size: 每批处理的股票数量
            port_release_timeout: 端口释放超时时间(秒)
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
        self.port_release_timeout = port_release_timeout
        
        # 端口使用记录，用于跟踪端口的最近使用时间
        self.used_ports = {}
        self.port_lock = threading.Lock()
        
        # 错误计数和动态重试调整
        self.error_count = 0
        self.error_threshold = 5  # 连续错误阈值
        self.backoff_factor = 1.0  # 初始退避系数
        self.backoff_lock = threading.Lock()
        self.last_error_time = time.time()

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
                logger.debug(f"成功加载配置文件: {self.config_path}")
                return config
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            return {}

    def _load_interface_config(self) -> Dict[str, Any]:
        """加载接口配置文件"""
        try:
            interface_path = os.path.join(self.interface_dir, self.interface_name)
            if not os.path.exists(interface_path):
                logger.warning(f"接口配置文件不存在: {interface_path}，将使用默认配置")
                # 使用默认配置
                return {
                    "description": "A股停复牌信息",
                    "api_name": "suspend",
                    "fields": [],
                    "params": {},
                    "available_fields": [
                        "ts_code", "suspend_date", "suspend_type", "resume_date", 
                        "change_reason", "suspend_time", "change_reason_type"
                    ],
                    "index_fields": ["ts_code", "suspend_date"]
                }
                
            with open(interface_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                logger.debug(f"成功加载接口配置: {interface_path}")
                return config
        except Exception as e:
            logger.error(f"加载接口配置失败: {str(e)}")
            # 使用默认配置
            return {
                "description": "A股停复牌信息",
                "api_name": "suspend",
                "fields": [],
                "params": {},
                "available_fields": [
                    "ts_code", "suspend_date", "suspend_type", "resume_date", 
                    "change_reason", "suspend_time", "change_reason_type"
                ],
                "index_fields": ["ts_code", "suspend_date"]
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
                    logger.warning(f"指定的WAN {wan_idx} 不可用，尝试其他WAN")
                    # 选择另一个可用的WAN
                    wan_idx = random.choice(available_indices)
            else:
                # 随机选择一个WAN接口
                wan_idx = random.choice(available_indices)
            
            # 在分配端口前短暂休眠，减少端口冲突
            time.sleep(0.1 + random.random() * 0.3)  # 0.1-0.4秒随机延迟
            
            # 分配端口（包含随机性）
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    # 获取一个端口
                    port = self.port_allocator.allocate_port(wan_idx)
                    if port:
                        # 检查端口是否最近使用过，如果是，则检查时间间隔
                        now = time.time()
                        with self.port_lock:
                            port_key = f"{wan_idx}:{port}"
                            if port_key in self.used_ports:
                                last_used = self.used_ports[port_key]
                                elapsed = now - last_used
                                if elapsed < self.port_release_timeout:
                                    logger.warning(f"端口 {port_key} 最近已使用，间隔 {elapsed:.1f}秒 小于超时 {self.port_release_timeout}秒，释放并重新选择")
                                    self.port_allocator.release_port(wan_idx, port)
                                    # 尝试选择另一个WAN或等待
                                    time.sleep(0.5 + random.random())
                                    continue
                                    
                            # 记录端口使用时间
                            self.used_ports[port_key] = now
                            
                        # 检查端口是否真的可用
                        if not tcp_port_is_free(port):
                            logger.warning(f"端口 {port} 系统指示不可用，释放并重试")
                            self.port_allocator.release_port(wan_idx, port)
                            # 尝试选择另一个WAN或等待
                            time.sleep(0.5 + random.random())
                            continue
                            
                        logger.debug(f"使用WAN接口 {wan_idx}，本地端口 {port}")
                        return (wan_idx, port)
                    else:
                        # 如果此WAN没有可用端口，尝试其他WAN接口
                        logger.warning(f"WAN {wan_idx} 没有可用端口，尝试其他WAN")
                        other_indices = [idx for idx in available_indices if idx != wan_idx]
                        if other_indices:
                            # 使用其他WAN索引
                            wan_idx = random.choice(other_indices)
                        else:
                            # 所有WAN接口都没有可用端口
                            logger.warning("所有WAN接口都没有可用端口")
                            # 短暂等待后重试当前WAN
                            time.sleep(0.5 + random.random())
                except Exception as e:
                    logger.warning(f"分配端口时出错: {str(e)}，尝试第 {attempt+1}/{max_attempts} 次")
                    time.sleep(0.5 + random.random())
            
            logger.warning(f"无法为WAN {wan_idx} 分配端口，所有尝试均失败")
            return None
            
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
            
    def _ensure_indexes(self, collection) -> bool:
        """
        确保必要的索引存在
        
        Args:
            collection: MongoDB集合对象
            
        Returns:
            是否成功创建或确认索引
        """
        try:
            # 获取现有索引
            existing_indexes = collection.index_information()
            logger.debug(f"现有索引信息: {existing_indexes}")
            
            # 获取索引字段配置
            index_fields = self.interface_config.get("index_fields", ["ts_code", "suspend_date"])
            
            # 检查复合唯一索引 (ts_code, suspend_date)
            index_name = "_".join([f"{field}_1" for field in index_fields])
            index_created = False
            
            # 检查索引是否存在并且结构正确
            if index_name in existing_indexes:
                # 验证索引的键和属性
                existing_index = existing_indexes[index_name]
                expected_keys = [(field, 1) for field in index_fields]
                
                # 确保是有序的正确键和唯一约束
                keys_match = all(key in expected_keys for key in existing_index['key']) and len(existing_index['key']) == len(expected_keys)
                is_unique = existing_index.get('unique', False)
                
                if keys_match and is_unique:
                    logger.debug(f"复合唯一索引 ({', '.join(index_fields)}) 已存在且结构正确，跳过创建")
                else:
                    # 索引存在但结构不正确，删除并重建
                    logger.info(f"复合唯一索引 ({', '.join(index_fields)}) 存在但结构不正确，删除并重建索引")
                    try:
                        collection.drop_index(index_name)
                        logger.debug(f"成功删除现有索引: {index_name}")
                    except Exception as e:
                        logger.error(f"删除索引时出错: {str(e)}")
                    
                    # 创建正确的索引
                    collection.create_index(
                        [(field, 1) for field in index_fields], 
                        unique=True, 
                        background=True
                    )
                    logger.success(f"已重建复合唯一索引 ({', '.join(index_fields)})")
                    index_created = True
            else:
                # 索引不存在，创建它
                logger.info(f"正在为集合 {collection.name} 创建复合唯一索引 ({', '.join(index_fields)})...")
                collection.create_index(
                    [(field, 1) for field in index_fields], 
                    unique=True, 
                    background=True
                )
                logger.success(f"已成功创建复合唯一索引 ({', '.join(index_fields)})")
                index_created = True
            
            # 检查单字段索引
            for field in index_fields:
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

    def fetch_suspend_by_date(self, trade_date: str) -> pd.DataFrame:
        """
        按日期获取停牌信息
        
        Args:
            trade_date: 交易日期，格式为YYYYMMDD
            
        Returns:
            DataFrame形式的停牌数据
        """
        try:
            params = {"suspend_date": trade_date}
                
            logger.debug(f"获取日期 {trade_date} 的停牌数据")
            
            df = self.client.get_data(
                api_name="suspend",
                params=params,
                fields=self.interface_config.get("available_fields", [])
            )
            
            if df.empty:
                logger.warning(f"日期 {trade_date} 未获取到停牌数据")
                return pd.DataFrame()
            
            logger.info(f"成功获取日期 {trade_date} 的停牌数据，共 {len(df)} 条记录")
            return df
        except Exception as e:
            logger.error(f"获取日期 {trade_date} 的停牌数据失败: {str(e)}")
            return pd.DataFrame()
    
    def fetch_suspend_by_code(self, ts_code: str) -> pd.DataFrame:
        """
        按股票代码获取停牌信息
        
        Args:
            ts_code: 股票代码
            
        Returns:
            DataFrame形式的停牌数据
        """
        try:
            params = {"ts_code": ts_code}
                
            logger.debug(f"获取股票 {ts_code} 的停牌数据")
            
            df = self.client.get_data(
                api_name="suspend",
                params=params,
                fields=self.interface_config.get("available_fields", [])
            )
            
            if df.empty:
                logger.warning(f"股票 {ts_code} 未获取到停牌数据")
                return pd.DataFrame()
                
            logger.info(f"成功获取股票 {ts_code} 的停牌数据，共 {len(df)} 条记录")
            return df
        except Exception as e:
            logger.error(f"获取股票 {ts_code} 的停牌数据失败: {str(e)}")
            return pd.DataFrame()
    
    def filter_by_target_ts_codes(self, df: pd.DataFrame, target_ts_codes: Set[str]) -> pd.DataFrame:
        """
        根据目标股票代码过滤数据
        
        Args:
            df: 待过滤的DataFrame
            target_ts_codes: 目标股票代码集合
        
        Returns:
            过滤后的DataFrame
        """
        if df.empty:
            return df
            
        # 过滤不在目标股票代码中的记录
        logger.debug(f"过滤前记录数: {len(df)}")
        filtered_df = df[df['ts_code'].isin(target_ts_codes)]
        logger.debug(f"过滤后记录数: {len(filtered_df)}")
        
        return filtered_df
    
    def save_to_mongodb(self, df: pd.DataFrame) -> bool:
        """
        将数据保存到MongoDB
        
        Args:
            df: 待保存的DataFrame
            
        Returns:
            是否成功保存
        """
        if df is None or df.empty:
            logger.warning("没有数据可保存到MongoDB")
            return False
            
        try:
            start_time = time.time()
            
            # 确保MongoDB连接
            if not self.mongo_client.is_connected():
                logger.warning("MongoDB未连接，尝试重新连接...")
                if not self.mongo_client.connect():
                    logger.error("重新连接MongoDB失败")
                    return False
            
            # 获取MongoDB数据库和集合
            db = self.mongo_client.client[self.db_name]  # 使用client属性直接访问数据库
            collection = db[self.collection_name]
            
            # 首先创建索引 - 提前创建索引以提高插入和查询效率
            if not self._ensure_indexes(collection):
                logger.warning("索引创建或确认失败，将尝试继续保存数据")
            
            # 将DataFrame转换为字典列表
            records = df.to_dict('records')
            
            # 批量处理，避免一次性处理太多记录
            batch_size = 1000  # 设置合适的批量大小
            total_batches = (len(records) + batch_size - 1) // batch_size
            
            total_inserted = 0
            total_updated = 0
            total_skipped = 0
            
            # 准备批量操作
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                
                # 增加超时时间，避免大批量操作超时
                try:
                    # 使用更适合的唯一键 - 根据业务逻辑
                    unique_keys = self.interface_config.get("index_fields", ["ts_code", "suspend_date"])
                    batch_result = self._batch_upsert(collection, batch, unique_keys)
                    
                    # 更新统计信息
                    total_inserted += batch_result.get("inserted", 0)
                    total_updated += batch_result.get("updated", 0)
                    if "skipped" in batch_result:
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
            logger.success(f"数据处理完成: 新插入 {total_inserted} 条记录，更新 {total_updated} 条记录，跳过 {total_skipped} 条记录，共处理 {total_processed}/{len(records)} 条记录，耗时 {elapsed:.2f}s")
            
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
        批量更新或插入记录
        
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

    def fetch_suspend_with_wan(self, ts_code: str = None, suspend_date: str = None, resume_date: str = None, wan_info: Tuple[int, int] = None) -> pd.DataFrame:
        """
        使用WAN接口获取停牌数据
        
        Args:
            ts_code: 可选，股票代码
            suspend_date: 可选，停牌日期
            resume_date: 可选，复牌日期
            wan_info: WAN接口和端口信息，格式为(wan_idx, port)
            
        Returns:
            DataFrame形式的停牌数据
        """
        params = {}
        if ts_code:
            params["ts_code"] = ts_code
        if suspend_date:
            params["suspend_date"] = suspend_date
        if resume_date:
            params["resume_date"] = resume_date
            
        # 准备日志消息
        log_msg = "获取停牌数据"
        if ts_code:
            log_msg += f" 股票代码: {ts_code}"
        if suspend_date:
            log_msg += f" 停牌日期: {suspend_date}"
        if resume_date:
            log_msg += f" 复牌日期: {resume_date}"
            
        logger.debug(log_msg)
        
        # 准备API参数
        api_name = "suspend"
        fields = self.interface_config.get("available_fields", [])
        
        # 使用传入的WAN接口
        use_wan = wan_info is not None
        max_retries = 5
        retry_count = 0
        
        while retry_count <= max_retries:
            try:
                # 调用Tushare API
                if use_wan:
                    wan_idx, port = wan_info
                    logger.debug(f"使用WAN接口 {wan_idx} 和本地端口 {port} 请求数据")
                    
                    # 创建WAN专用客户端
                    client = TushareClientWAN(token=self.token, timeout=120, api_url=self.api_url)
                    client.set_local_address('0.0.0.0', port, wan_idx)
                else:
                    # 使用普通客户端
                    client = self.client
                    client.set_timeout(120)
                
                # 获取数据
                start_time = time.time()
                df = client.get_data(api_name=api_name, params=params, fields=fields)
                elapsed = time.time() - start_time
                
                # 释放WAN端口（如果使用了）
                if use_wan:
                    # 增加随机延迟再释放端口
                    release_delay = 1 + random.random() * 2  # 1-3秒的随机延迟
                    time.sleep(release_delay)
                    logger.debug(f"延迟 {release_delay:.2f}秒后释放WAN {wan_idx} 的端口 {port}")
                    self.port_allocator.release_port(wan_idx, port)
                
                if df is None:
                    # 记录错误并调整退避策略
                    self._record_error()
                    
                    logger.warning(f"{log_msg} 失败，返回None")
                    retry_count += 1
                    
                    # 获取动态调整的等待时间
                    wait_time = self._get_retry_wait_time(retry_count)
                    logger.info(f"第 {retry_count}/{max_retries} 次重试，等待 {wait_time:.1f} 秒")
                    time.sleep(wait_time)
                    continue
                
                if df.empty:
                    logger.info(f"{log_msg} 未获取到数据")
                    return pd.DataFrame()
                
                # 记录成功
                self._record_success()
                logger.info(f"成功获取 {len(df)} 条停牌记录，耗时 {elapsed:.2f}s")
                return df
                
            except Exception as e:
                # 记录错误
                self._record_error()
                
                retry_count += 1
                error_msg = str(e)
                
                # 释放WAN端口（如果使用了）
                if use_wan:
                    wan_idx, port = wan_info
                    # 增加随机延迟再释放端口
                    release_delay = 1 + random.random() * 2  # 1-3秒的随机延迟
                    time.sleep(release_delay)
                    logger.debug(f"延迟 {release_delay:.2f}秒后释放WAN {wan_idx} 的端口 {port}")
                    self.port_allocator.release_port(wan_idx, port)
                
                # 判断是否为速率限制错误
                error_type = "rate_limit" if "40203" in error_msg else None
                wait_time = self._get_retry_wait_time(retry_count, error_type)
                
                if "40203" in error_msg:
                    logger.warning(f"触发API速率限制，将等待 {wait_time:.1f} 秒后重试 ({retry_count}/{max_retries})")
                else:
                    logger.warning(f"获取数据失败: {error_msg}，将在 {wait_time:.1f} 秒后重试 ({retry_count}/{max_retries})")
                
                if retry_count > max_retries:
                    logger.error(f"达到最大重试次数，放弃获取数据")
                    return pd.DataFrame()
                    
                time.sleep(wait_time)
        
        logger.error("获取数据失败，已达最大重试次数")
        return pd.DataFrame()

    def fetch_suspend_batch(self, ts_codes: List[str], suspend_date: str = None, resume_date: str = None) -> pd.DataFrame:
        """
        批量获取多个股票的停牌数据（串行方式）
        
        Args:
            ts_codes: 股票代码列表
            suspend_date: 可选，停牌日期
            resume_date: 可选，复牌日期
            
        Returns:
            所有股票的停牌数据合并后的DataFrame
        """
        if not ts_codes:
            logger.warning("没有股票代码可以查询")
            return pd.DataFrame()
        
        # 进度统计变量
        total_stocks = len(ts_codes)
        processed_stocks = 0
        success_stocks = 0
        total_records = 0
        all_data = []
        
        start_time = time.time()
        
        # 逐个股票代码获取数据
        for ts_code in ts_codes:
            # 获取停牌数据
            df = self.fetch_suspend_with_wan(
                ts_code=ts_code,
                suspend_date=suspend_date,
                resume_date=resume_date,
                wan_info=self._get_wan_socket()
            )
            
            # 更新统计信息
            processed_stocks += 1
            if not df.empty:
                success_stocks += 1
                total_records += len(df)
                all_data.append(df)
                logger.info(f"股票 {ts_code} 获取到 {len(df)} 条停牌记录")
                
                # 每批次数据立即保存
                self.save_to_mongodb(df)
            else:
                logger.info(f"股票 {ts_code} 未获取到停牌数据")
            
            # 更新进度
            progress = processed_stocks / total_stocks * 100
            elapsed = time.time() - start_time
            avg_time = elapsed / processed_stocks
            remaining = avg_time * (total_stocks - processed_stocks)
            
            logger.info(f"进度: {processed_stocks}/{total_stocks} ({progress:.1f}%), "
                       f"耗时: {elapsed:.1f}s, 剩余: {remaining:.1f}s")
            
            # 短暂休眠，避免API调用过于频繁
            time.sleep(1)
        
        # 合并所有数据
        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            logger.success(f"共获取 {success_stocks}/{total_stocks} 个股票的停牌数据，总计 {total_records} 条记录")
            return result_df
        else:
            logger.warning(f"未获取到任何停牌数据")
            return pd.DataFrame()

    def fetch_suspend_parallel(self, ts_codes: List[str], suspend_date: str = None, resume_date: str = None, batch_size: int = 10) -> pd.DataFrame:
        """
        使用多WAN口并行获取多个股票的停牌数据
        
        Args:
            ts_codes: 股票代码列表
            suspend_date: 可选，停牌日期
            resume_date: 可选，复牌日期
            batch_size: 每批处理的股票数量
            
        Returns:
            所有股票的停牌数据合并后的DataFrame（空DataFrame，因为数据已保存到MongoDB）
        """
        if not ts_codes:
            logger.warning("没有股票代码可以查询")
            return pd.DataFrame()
            
        # 获取可用的WAN接口
        available_wans = []
        if self.port_allocator:
            available_wans = self.port_allocator.get_available_wan_indices()
            logger.info(f"可用的WAN接口数量: {len(available_wans)}, WAN索引: {available_wans}")
            
        if not available_wans:
            logger.warning("没有可用的WAN接口，将使用系统默认网络接口")
            # 如果没有可用WAN，回退到普通批处理
            return self.fetch_suspend_batch(ts_codes, suspend_date, resume_date)
        
        import threading
        import queue
        
        # 将股票代码按批次分组
        total_stocks = len(ts_codes)
        batches = []
        for i in range(0, total_stocks, batch_size):
            batch = ts_codes[i:i+batch_size]
            batches.append(batch)
        
        total_batches = len(batches)
        logger.info(f"将 {total_stocks} 个股票分为 {total_batches} 批处理，每批 {batch_size} 个")
        
        # 创建工作队列和结果队列
        task_queue = queue.Queue()
        result_queue = queue.Queue()
        
        # 创建失败任务重试队列
        retry_queue = queue.Queue()
        
        # 加入所有任务
        for i, batch in enumerate(batches):
            task_queue.put((i, batch))
        
        # 线程锁
        lock = threading.Lock()
        
        # 追踪已处理的批次和WAN口使用情况
        processed_batches = set()
        wan_usage_stats = {wan_idx: 0 for wan_idx in available_wans}
        batch_wan_mapping = {}  # 记录每个批次使用的WAN
        
        # 最大重试次数
        MAX_RETRY = 5
        
        # 工作线程函数
        def worker():
            while True:
                # 优先处理重试队列中的任务
                retry_task = None
                try:
                    if not retry_queue.empty():
                        retry_task = retry_queue.get(block=False)
                        batch_id, stock_batch, retry_count = retry_task
                        
                        with lock:
                            logger.info(f"开始处理重试任务: 批次 {batch_id+1}/{total_batches} (重试 {retry_count}/{MAX_RETRY})")
                        
                        # 为重试任务选择一个不同的WAN
                        if batch_id in batch_wan_mapping:
                            used_wan = batch_wan_mapping[batch_id]
                            # 尝试选择一个不同的WAN
                            other_wans = [w for w in available_wans if w != used_wan]
                            if other_wans:
                                wan_idx = random.choice(other_wans)
                            else:
                                wan_idx = random.choice(available_wans)
                        else:
                            wan_idx = random.choice(available_wans)
                    elif not task_queue.empty():
                        batch_id, stock_batch = task_queue.get(block=False)
                        retry_count = 0
                        
                        # 选择WAN接口 - 负载最小的优先
                        wan_usage_counts = [(idx, count) for idx, count in wan_usage_stats.items()]
                        wan_usage_counts.sort(key=lambda x: x[1])  # 按使用次数排序
                        wan_idx = wan_usage_counts[0][0]  # 使用次数最少的WAN
                    else:
                        # 所有任务都已处理
                        return
                except queue.Empty:
                    # 所有任务都已处理
                    return
                
                # 更新WAN使用统计
                with lock:
                    wan_usage_stats[wan_idx] = wan_usage_stats.get(wan_idx, 0) + 1
                    batch_wan_mapping[batch_id] = wan_idx
                
                # 获取WAN接口
                wan_info = self._get_wan_socket(wan_idx)
                if not wan_info:
                    with lock:
                        logger.warning(f"无法为WAN {wan_idx} 获取端口，将尝试其他WAN接口")
                    
                    # 如果还有重试次数，放入重试队列
                    if retry_count < MAX_RETRY:
                        retry_queue.put((batch_id, stock_batch, retry_count + 1))
                    else:
                        with lock:
                            logger.error(f"批次 {batch_id+1} 重试达到最大次数 {MAX_RETRY}，放弃处理")
                        result_queue.put((batch_id, len(stock_batch), 0, wan_idx))
                    
                    continue
                
                wan_idx, port = wan_info
                
                # 处理批次
                try:
                    with lock:
                        logger.info(f"开始处理批次 {batch_id+1}/{total_batches} 使用WAN {wan_idx} 端口 {port}" + 
                                   (f" (重试 {retry_count}/{MAX_RETRY})" if retry_count > 0 else ""))
                    
                    batch_records = 0
                    batch_data = []
                    
                    # 逐个股票处理
                    for ts_code in stock_batch:
                        # 获取停牌数据 - 根据是否提供日期参数决定调用方式
                        df = self.fetch_suspend_with_wan(
                            ts_code=ts_code,
                            suspend_date=suspend_date,
                            resume_date=resume_date,
                            wan_info=wan_info
                        )
                        
                        if not df.empty:
                            batch_records += len(df)
                            batch_data.append(df)
                            with lock:
                                logger.info(f"WAN {wan_idx} 批次 {batch_id+1}: 股票 {ts_code} 获取到 {len(df)} 条停牌记录")
                            
                            # 单个股票数据立即保存
                            self.save_to_mongodb(df)
                        else:
                            with lock:
                                logger.debug(f"WAN {wan_idx} 批次 {batch_id+1}: 股票 {ts_code} 未获取到停牌数据")
                        
                        # 短暂休眠避免API调用过于频繁
                        time.sleep(1)
                    
                    # 合并批次数据并报告结果
                    batch_df = pd.concat(batch_data, ignore_index=True) if batch_data else pd.DataFrame()
                    with lock:
                        logger.info(f"WAN {wan_idx} 批次 {batch_id+1}/{total_batches} 处理完成，获取到 {batch_records} 条记录")
                    
                    # 放入结果队列
                    result_queue.put((batch_id, len(stock_batch), batch_records, wan_idx))
                    
                    # 添加到已处理批次集合
                    with lock:
                        processed_batches.add(batch_id)
                    
                except Exception as e:
                    with lock:
                        logger.error(f"WAN {wan_idx} 处理批次 {batch_id+1} 时出错: {str(e)}")
                        import traceback
                        logger.debug(f"详细错误信息: {traceback.format_exc()}")
                    
                    # 如果还有重试次数，放入重试队列
                    if retry_count < MAX_RETRY:
                        with lock:
                            logger.warning(f"批次 {batch_id+1} 处理失败，将在重试队列中等待 (重试 {retry_count+1}/{MAX_RETRY})")
                        retry_queue.put((batch_id, stock_batch, retry_count + 1))
                    else:
                        with lock:
                            logger.error(f"批次 {batch_id+1} 重试达到最大次数 {MAX_RETRY}，放弃处理")
                        result_queue.put((batch_id, len(stock_batch), 0, wan_idx))
                    
                finally:
                    # 释放WAN端口
                    if wan_info:
                        # 增加延迟确保端口完全释放
                        # Windows系统TIME_WAIT状态通常为2分钟，提供足够的端口释放时间
                        release_delay = 1 + random.random() * 2  # 1-3秒的随机延迟
                        time.sleep(release_delay)
                        logger.debug(f"延迟 {release_delay:.2f}秒后释放WAN {wan_idx} 的端口 {port}")
                        self.port_allocator.release_port(wan_idx, port)
        
        # 启动工作线程
        threads = []
        max_concurrent = min(len(available_wans), self.max_workers)
        logger.info(f"启动 {max_concurrent} 个工作线程进行并行处理，共 {total_batches} 个批次")
        
        for _ in range(max_concurrent):
            thread = threading.Thread(target=worker)
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        # 主线程等待所有任务完成
        start_time = time.time()
        completed_batches = 0
        total_success_stocks = 0
        total_records = 0
        
        # 处理结果
        while completed_batches < total_batches:
            try:
                batch_id, batch_size, batch_records, wan_idx = result_queue.get(timeout=60)
                
                completed_batches += 1
                if batch_records > 0:
                    total_success_stocks += 1
                    total_records += batch_records
                
                # 更新进度
                progress = completed_batches / total_batches * 100
                elapsed = time.time() - start_time
                avg_time = elapsed / completed_batches if completed_batches > 0 else 0
                remaining = avg_time * (total_batches - completed_batches)
                
                # 活跃的WAN
                active_wans = set()
                for bid, wid in batch_wan_mapping.items():
                    if bid not in processed_batches:
                        active_wans.add(wid)
                active_wans_str = ", ".join([f"WAN-{w}" for w in active_wans]) if active_wans else "无"
                
                logger.info(f"总进度: {completed_batches}/{total_batches} ({progress:.1f}%) [WAN-{wan_idx}完成, 活跃:{active_wans_str}], "
                           f"耗时: {elapsed:.1f}s, 剩余: {remaining:.1f}s")
                
                # 每完成25%的批次或所有批次完成时，显示WAN使用统计
                if completed_batches % max(1, total_batches // 4) == 0 or completed_batches == total_batches:
                    stats_msg = ", ".join([f"WAN-{idx}: {count}次" for idx, count in wan_usage_stats.items()])
                    logger.info(f"WAN使用统计: {stats_msg}")
                
            except queue.Empty:
                # 检查是否有活动线程
                active_threads = sum(1 for t in threads if t.is_alive())
                if active_threads == 0:
                    logger.warning("等待超时且所有线程已结束，检查是否有未完成的任务")
                    
                    remaining_tasks = task_queue.qsize() + retry_queue.qsize()
                    if remaining_tasks == 0:
                        logger.warning("没有剩余任务，可能有部分结果未被正确处理")
                        break
                    else:
                        logger.warning(f"还有 {remaining_tasks} 个任务未完成，重新启动工作线程")
                        # 重新启动一些工作线程
                        for _ in range(min(remaining_tasks, max_concurrent)):
                            thread = threading.Thread(target=worker)
                            thread.daemon = True
                            thread.start()
                            threads.append(thread)
                        
                logger.info(f"等待结果中，活动线程: {active_threads}, 已完成: {completed_batches}/{total_batches}")
                # 避免CPU占用过高
                time.sleep(1)
        
        # 等待所有线程结束
        for thread in threads:
            thread.join(timeout=5)
            
        # 输出最终统计信息
        elapsed_total = time.time() - start_time
        stats_msg = ", ".join([f"WAN-{idx}: {count}次" for idx, count in wan_usage_stats.items()])
        
        logger.success(f"并行处理完成，共获取 {total_success_stocks}/{total_batches} 批次的停牌数据，"
                      f"总计 {total_records} 条记录，耗时 {elapsed_total:.1f}s")
        logger.info(f"最终WAN使用统计: {stats_msg}")
        
        # 返回空DataFrame，因为数据已保存到MongoDB
        return pd.DataFrame()

    def get_trade_calendar(self, start_date: str, end_date: str) -> List[str]:
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
            if not self.mongo_client.is_connected():
                logger.warning("MongoDB未连接，尝试重新连接...")
                if not self.mongo_client.connect():
                    logger.error("重新连接MongoDB失败")
                    return []
                    
            # 连接MongoDB
            db = self.mongo_client.client[self.db_name]
            
            # 一般交易日历保存在trade_cal集合中
            collection = db["trade_cal"]
            
            # 查询指定日期范围内的交易日
            query = {
                "cal_date": {"$gte": start_date, "$lte": end_date},
                "is_open": 1  # 1表示交易日，0表示非交易日
            }
            
            # 只获取日期字段
            result = collection.find(query, {"cal_date": 1, "_id": 0}).sort("cal_date", 1)
            
            # 提取日期列表
            trade_dates = [doc["cal_date"] for doc in result]
            
            logger.info(f"从交易日历获取到日期范围 {start_date} 至 {end_date} 内的 {len(trade_dates)} 个交易日")
            
            if not trade_dates:
                logger.warning("未从交易日历获取到交易日，将使用日期范围内的所有日期")
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

    def _run_recent_mode(self) -> bool:
        """
        最近一周模式 - 获取最近一周的停牌数据
        
        Returns:
            是否成功
        """
        # 计算最近一周的日期范围
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
        
        logger.info(f"使用recent模式获取最近一周 {start_date} 至 {end_date} 的停牌数据")
        
        # 获取该时间范围内的交易日
        trade_dates = self.get_trade_calendar(start_date, end_date)
        if not trade_dates:
            logger.error("未获取到交易日历，无法继续")
            return False
        
        # 获取目标股票代码
        target_ts_codes = self.get_target_ts_codes_from_stock_basic()
        if not target_ts_codes:
            logger.error("未能获取到任何目标板块的股票代码")
            return False
            
        # 检查WAN接口可用性
        available_wans = []
        if self.port_allocator:
            available_wans = self.port_allocator.get_available_wan_indices()
            if available_wans:
                logger.info(f"获取到 {len(trade_dates)} 个交易日和 {len(target_ts_codes)} 个目标股票代码，将使用 {len(available_wans)} 个WAN接口并行处理")
                # 使用并行模式处理多个交易日
                return self._run_recent_mode_parallel(trade_dates, target_ts_codes)
            else:
                logger.warning("未检测到可用的WAN接口，将使用普通模式获取数据")
        else:
            logger.warning("未启用多WAN口功能，将使用普通模式获取数据")
            
        logger.info(f"获取到 {len(trade_dates)} 个交易日和 {len(target_ts_codes)} 个目标股票代码")
        
        # 按交易日获取停牌数据
        total_records = 0
        for trade_date in trade_dates:
            logger.info(f"获取交易日 {trade_date} 的停牌数据")
            
            # 使用停牌日期等于交易日期的条件获取数据
            df = self.fetch_suspend_with_wan(
                suspend_date=trade_date,
                wan_info=self._get_wan_socket()
            )
            
            if not df.empty:
                # 过滤目标板块股票
                filtered_df = self.filter_by_target_ts_codes(df, target_ts_codes)
                
                if not filtered_df.empty:
                    # 保存到MongoDB
                    success = self.save_to_mongodb(filtered_df)
                    if success:
                        total_records += len(filtered_df)
                        logger.success(f"成功保存交易日 {trade_date} 的 {len(filtered_df)} 条停牌记录")
                    else:
                        logger.warning(f"保存交易日 {trade_date} 的停牌数据失败")
                else:
                    logger.info(f"交易日 {trade_date} 没有目标板块的停牌记录")
            else:
                logger.info(f"交易日 {trade_date} 未获取到停牌数据")
            
            # 避免请求过于频繁
            time.sleep(1)
        
        logger.success(f"recent模式执行完成，共获取并保存 {total_records} 条停牌记录")
        return total_records > 0
        
    def _run_recent_mode_parallel(self, trade_dates: List[str], target_ts_codes: Set[str]) -> bool:
        """
        并行处理recent模式 - 使用多WAN口并行获取最近一周的停牌数据
        
        Args:
            trade_dates: 要处理的交易日列表
            target_ts_codes: 目标股票代码集合
            
        Returns:
            是否成功
        """
        if not trade_dates:
            logger.error("没有交易日可以处理")
            return False
            
        # 获取可用的WAN接口
        available_wans = []
        if self.port_allocator:
            available_wans = self.port_allocator.get_available_wan_indices()
            logger.info(f"可用的WAN接口数量: {len(available_wans)}, WAN索引: {available_wans}")
            
        if not available_wans:
            logger.warning("没有可用的WAN接口，将使用系统默认网络接口")
            # 如果没有可用WAN，回退到普通处理
            return self._run_recent_mode()
        
        import threading
        import queue
        
        # 创建工作队列和结果队列
        task_queue = queue.Queue()
        result_queue = queue.Queue()
        
        # 创建失败任务重试队列
        retry_queue = queue.Queue()
        
        # 加入所有任务
        for i, trade_date in enumerate(trade_dates):
            task_queue.put((i, trade_date))
        
        total_dates = len(trade_dates)
        
        # 线程锁
        lock = threading.Lock()
        
        # 追踪已处理的任务和WAN口使用情况
        processed_tasks = set()
        wan_usage_stats = {wan_idx: 0 for wan_idx in available_wans}
        task_wan_mapping = {}  # 记录每个任务使用的WAN
        
        # 最大重试次数
        MAX_RETRY = 3
        
        # 工作线程函数
        def worker():
            while True:
                # 优先处理重试队列中的任务
                retry_task = None
                try:
                    if not retry_queue.empty():
                        retry_task = retry_queue.get(block=False)
                        task_id, trade_date, retry_count = retry_task
                        
                        with lock:
                            logger.info(f"开始处理重试任务: 交易日 {trade_date} (重试 {retry_count}/{MAX_RETRY})")
                        
                        # 为重试任务选择一个不同的WAN
                        if task_id in task_wan_mapping:
                            used_wan = task_wan_mapping[task_id]
                            # 尝试选择一个不同的WAN
                            other_wans = [w for w in available_wans if w != used_wan]
                            if other_wans:
                                wan_idx = random.choice(other_wans)
                            else:
                                wan_idx = random.choice(available_wans)
                        else:
                            wan_idx = random.choice(available_wans)
                    elif not task_queue.empty():
                        task_id, trade_date = task_queue.get(block=False)
                        retry_count = 0
                        
                        # 选择WAN接口 - 负载最小的优先
                        wan_usage_counts = [(idx, count) for idx, count in wan_usage_stats.items()]
                        wan_usage_counts.sort(key=lambda x: x[1])  # 按使用次数排序
                        wan_idx = wan_usage_counts[0][0]  # 使用次数最少的WAN
                    else:
                        # 所有任务都已处理
                        return
                except queue.Empty:
                    # 所有任务都已处理
                    return
                
                # 更新WAN使用统计
                with lock:
                    wan_usage_stats[wan_idx] = wan_usage_stats.get(wan_idx, 0) + 1
                    task_wan_mapping[task_id] = wan_idx
                
                # 获取WAN接口
                wan_info = self._get_wan_socket(wan_idx)
                if not wan_info:
                    with lock:
                        logger.warning(f"无法为WAN {wan_idx} 获取端口，将尝试其他WAN接口")
                    
                    # 如果还有重试次数，放入重试队列
                    if retry_count < MAX_RETRY:
                        retry_queue.put((task_id, trade_date, retry_count + 1))
                    else:
                        with lock:
                            logger.error(f"交易日 {trade_date} 重试达到最大次数 {MAX_RETRY}，放弃处理")
                        result_queue.put((task_id, trade_date, 0, wan_idx))
                    
                    continue
                
                wan_idx, port = wan_info
                
                # 处理任务
                try:
                    with lock:
                        logger.info(f"开始处理交易日 {trade_date}，使用WAN {wan_idx} 端口 {port}" + 
                                   (f" (重试 {retry_count}/{MAX_RETRY})" if retry_count > 0 else ""))
                    
                    # 使用停牌日期等于交易日期的条件获取数据
                    df = self.fetch_suspend_with_wan(
                        suspend_date=trade_date,
                        wan_info=wan_info
                    )
                    
                    if not df.empty:
                        # 过滤目标板块股票
                        filtered_df = self.filter_by_target_ts_codes(df, target_ts_codes)
                        
                        records_count = 0
                        if not filtered_df.empty:
                            # 保存到MongoDB
                            success = self.save_to_mongodb(filtered_df)
                            if success:
                                records_count = len(filtered_df)
                                with lock:
                                    logger.success(f"WAN {wan_idx}: 成功保存交易日 {trade_date} 的 {records_count} 条停牌记录")
                            else:
                                with lock:
                                    logger.warning(f"WAN {wan_idx}: 保存交易日 {trade_date} 的停牌数据失败")
                        else:
                            with lock:
                                logger.info(f"WAN {wan_idx}: 交易日 {trade_date} 没有目标板块的停牌记录")
                    else:
                        with lock:
                            logger.info(f"WAN {wan_idx}: 交易日 {trade_date} 未获取到停牌数据")
                    
                    # 添加到已处理集合
                    with lock:
                        processed_tasks.add(task_id)
                    
                    # 放入结果队列
                    if not df.empty and 'filtered_df' in locals():
                        result_queue.put((task_id, trade_date, len(filtered_df), wan_idx))
                    else:
                        result_queue.put((task_id, trade_date, 0, wan_idx))
                    
                except Exception as e:
                    with lock:
                        logger.error(f"WAN {wan_idx} 处理交易日 {trade_date} 时出错: {str(e)}")
                        import traceback
                        logger.debug(f"详细错误信息: {traceback.format_exc()}")
                    
                    # 如果还有重试次数，放入重试队列
                    if retry_count < MAX_RETRY:
                        with lock:
                            logger.warning(f"交易日 {trade_date} 处理失败，将在重试队列中等待 (重试 {retry_count+1}/{MAX_RETRY})")
                        retry_queue.put((task_id, trade_date, retry_count + 1))
                    else:
                        with lock:
                            logger.error(f"交易日 {trade_date} 重试达到最大次数 {MAX_RETRY}，放弃处理")
                        result_queue.put((task_id, trade_date, 0, wan_idx))
                    
                finally:
                    # 释放WAN端口
                    if wan_info:
                        # 增加延迟确保端口完全释放
                        time.sleep(1)
                        self.port_allocator.release_port(wan_idx, port)
        
        # 启动工作线程
        threads = []
        max_concurrent = min(len(available_wans), self.max_workers)
        logger.info(f"启动 {max_concurrent} 个工作线程进行并行处理，共 {total_dates} 个交易日")
        
        for _ in range(max_concurrent):
            thread = threading.Thread(target=worker)
            thread.daemon = True
            thread.start()
            threads.append(thread)
        
        # 主线程等待所有任务完成
        start_time = time.time()
        completed_tasks = 0
        total_records = 0
        
        # 处理结果
        while completed_tasks < total_dates:
            try:
                task_id, trade_date, records_count, wan_idx = result_queue.get(timeout=60)
                
                completed_tasks += 1
                total_records += records_count
                
                # 更新进度
                progress = completed_tasks / total_dates * 100
                elapsed = time.time() - start_time
                avg_time = elapsed / completed_tasks if completed_tasks > 0 else 0
                remaining = avg_time * (total_dates - completed_tasks)
                
                # 活跃的WAN
                active_wans = set()
                for tid, wid in task_wan_mapping.items():
                    if tid not in processed_tasks:
                        active_wans.add(wid)
                active_wans_str = ", ".join([f"WAN-{w}" for w in active_wans]) if active_wans else "无"
                
                logger.info(f"总进度: {completed_tasks}/{total_dates} ({progress:.1f}%) [WAN-{wan_idx}完成, 活跃:{active_wans_str}], "
                           f"耗时: {elapsed:.1f}s, 剩余: {remaining:.1f}s")
                
            except queue.Empty:
                # 检查是否有活动线程
                active_threads = sum(1 for t in threads if t.is_alive())
                if active_threads == 0:
                    logger.warning("等待超时且所有线程已结束，检查是否有未完成的任务")
                    
                    remaining_tasks = task_queue.qsize() + retry_queue.qsize()
                    if remaining_tasks == 0:
                        logger.warning("没有剩余任务，可能有部分结果未被正确处理")
                        break
                    else:
                        logger.warning(f"还有 {remaining_tasks} 个任务未完成，重新启动工作线程")
                        # 重新启动一些工作线程
                        for _ in range(min(remaining_tasks, max_concurrent)):
                            thread = threading.Thread(target=worker)
                            thread.daemon = True
                            thread.start()
                            threads.append(thread)
                        
                logger.info(f"等待结果中，活动线程: {active_threads}, 已完成: {completed_tasks}/{total_dates}")
                # 避免CPU占用过高
                time.sleep(1)
        
        # 等待所有线程结束
        for thread in threads:
            thread.join(timeout=5)
            
        # 输出最终统计信息
        elapsed_total = time.time() - start_time
        stats_msg = ", ".join([f"WAN-{idx}: {count}次" for idx, count in wan_usage_stats.items()])
        
        logger.success(f"recent模式（并行）执行完成，共获取并保存 {total_records} 条停牌记录，耗时 {elapsed_total:.1f}s")
        logger.info(f"最终WAN使用统计: {stats_msg}")
        
        return total_records > 0

    def _run_full_mode(self) -> bool:
        """
        全量模式 - 获取从1990年至今的所有停牌数据
        
        Returns:
            是否成功
        """
        # 设置日期范围 - 从1990年1月1日到今天
        start_date = "19900101"
        end_date = datetime.now().strftime("%Y%m%d")
        
        logger.info(f"使用full模式获取从 {start_date} 至 {end_date} 的所有停牌数据")
        
        # 获取目标股票代码
        target_ts_codes = list(self.get_target_ts_codes_from_stock_basic())
        if not target_ts_codes:
            logger.error("未能获取到任何目标板块的股票代码")
            return False
        
        # 检查WAN接口可用性
        available_wans = []
        if self.port_allocator:
            available_wans = self.port_allocator.get_available_wan_indices()
            if available_wans:
                logger.info(f"获取到 {len(target_ts_codes)} 个目标股票代码，将使用 {len(available_wans)} 个WAN接口并行获取停牌数据")
            else:
                logger.warning("未检测到可用的WAN接口，将使用普通模式获取数据")
        else:
            logger.warning("未启用多WAN口功能，将使用普通模式获取数据")
            
        # 使用并行方式获取数据，无需指定开始日期和结束日期
        # full模式下，会自动获取所有历史数据
        result = self.fetch_suspend_parallel(
            target_ts_codes, 
            batch_size=self.batch_size
        )
        
        # 判断是否成功
        if hasattr(self, 'last_operation_stats'):
            total_records = sum(
                self.last_operation_stats.get(key, 0) 
                for key in ['inserted', 'updated']
            )
            logger.success(f"full模式执行完成，共处理 {total_records} 条停牌记录")
            return total_records > 0
        
        logger.warning("full模式执行完成，但无法获取处理记录数")
        return True  # 假设成功

    def run(self, config: Optional[Dict[str, Any]] = None) -> bool:
        """
        运行停牌数据获取器，支持多种模式
        
        Args:
            config: 配置字典，包含如下字段：
                - recent: 获取最近一周的停牌数据
                - full: 获取从1990年至今的所有停牌数据
                - batch_size: 每批处理的股票数量
                
        Returns:
            是否成功
        """
        # 默认配置
        default_config = {
            "recent": False,
            "full": False,
            "batch_size": self.batch_size,
        }
        
        # 合并配置
        if config is None:
            config = {}
        
        effective_config = {**default_config, **config}
        recent = effective_config["recent"]
        full = effective_config["full"]
        self.batch_size = effective_config["batch_size"]
        
        # 设置默认模式 - 如果没有指定模式，使用recent模式
        if not (recent or full):
            logger.info("未指定获取模式，默认使用recent模式")
            recent = True
        
        # 根据模式执行相应的操作
        if recent:
            return self._run_recent_mode()
            
        elif full:
            return self._run_full_mode()
            
        else:
            logger.error("无效的运行模式")
            return False

    def create_mock_data(self) -> pd.DataFrame:
        """
        创建模拟停牌数据（用于测试）
        
        Returns:
            模拟的停牌数据DataFrame
        """
        # 模拟数据
        sample_data = [
            {
                "ts_code": "000001.SZ", 
                "suspend_date": "20230101", 
                "suspend_type": 444016000, 
                "resume_date": "20230110", 
                "change_reason": "重大事项", 
                "suspend_time": "09:30", 
                "change_reason_type": 1
            },
            {
                "ts_code": "000002.SZ", 
                "suspend_date": "20230105", 
                "suspend_type": 444003000, 
                "resume_date": "20230106", 
                "change_reason": "临时停牌", 
                "suspend_time": "09:30", 
                "change_reason_type": 2
            },
            {
                "ts_code": "600000.SH", 
                "suspend_date": "20230110", 
                "suspend_type": 444016000, 
                "resume_date": "20230111", 
                "change_reason": "重大资产重组", 
                "suspend_time": "09:30", 
                "change_reason_type": 3
            },
            {
                "ts_code": "600036.SH", 
                "suspend_date": "20230115", 
                "suspend_type": 444016000, 
                "resume_date": None, 
                "change_reason": "重大事项", 
                "suspend_time": "09:30", 
                "change_reason_type": 1
            },
        ]
        
        # 创建DataFrame
        df = pd.DataFrame(sample_data)
        logger.info(f"生成 {len(df)} 条模拟停牌数据")
        return df

    def _record_error(self):
        """记录错误并调整退避策略"""
        now = time.time()
        with self.backoff_lock:
            self.error_count += 1
            # 计算错误频率
            error_interval = now - self.last_error_time
            self.last_error_time = now
            
            # 检查是否需要调整退避因子
            if self.error_count > self.error_threshold:
                # 如果错误间隔很短，增加退避因子
                if error_interval < 5.0:
                    old_factor = self.backoff_factor
                    self.backoff_factor = min(self.backoff_factor * 1.5, 10.0)
                    logger.warning(f"连续错误频率高，增加退避因子: {old_factor:.2f} -> {self.backoff_factor:.2f}")
                    
                    # 当错误累积较多时，强制等待一段时间
                    if self.error_count > self.error_threshold * 2:
                        wait_time = 30 + random.randint(0, 30)
                        logger.warning(f"检测到高频错误，强制等待 {wait_time} 秒后继续")
                        time.sleep(wait_time)
                        # 减少错误计数
                        self.error_count = max(self.error_count - self.error_threshold, 0)
    
    def _record_success(self):
        """记录成功并调整退避策略"""
        with self.backoff_lock:
            # 连续成功，降低错误计数和退避因子
            self.error_count = max(self.error_count - 1, 0)
            if self.error_count == 0 and self.backoff_factor > 1.0:
                self.backoff_factor = max(self.backoff_factor * 0.8, 1.0)
                logger.info(f"操作成功，降低退避因子至 {self.backoff_factor:.2f}")
                
    def _get_retry_wait_time(self, retry_count, error_type=None):
        """
        计算重试等待时间，结合指数退避、随机抖动和动态调整
        
        Args:
            retry_count: 当前重试次数
            error_type: 错误类型，可用于针对特定错误调整等待时间
            
        Returns:
            等待时间(秒)
        """
        # 基础公式: base_delay * backoff_factor * (2 ^ retry_count) * (0.5 + random.random())
        max_delay = 3600  # 最大等待时间（秒）
        jitter_factor = 0.5 + random.random()  # 0.5-1.5之间的随机因子
        
        # 根据错误类型调整
        if error_type == "rate_limit":
            # API速率限制
            wait_time = min(max_delay, self.retry_delay * self.backoff_factor * (3 ** retry_count) * jitter_factor)
        else:
            # 一般错误
            wait_time = min(max_delay, self.retry_delay * self.backoff_factor * (2 ** retry_count) * jitter_factor)
            
        return wait_time


def main():
    """主函数"""
    import argparse
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="A股停复牌信息获取工具")
    parser.add_argument("--verbose", action="store_true", help="输出详细日志")
    parser.add_argument("--mock", action="store_true", help="使用模拟数据模式（API不可用时）")
    parser.add_argument("--recent", action="store_true", help="获取最近一周的停牌数据")
    parser.add_argument("--full", action="store_true", help="获取从1990年至今的所有停牌数据")
    parser.add_argument("--ts-code", type=str, help="指定股票代码，例如600000.SH")
    parser.add_argument("--batch-size", type=int, default=1, help="每批次处理的股票数量，默认10")
    parser.add_argument("--max-workers", type=int, default=3, help="最大并行工作线程数，默认3")
    parser.add_argument("--market-codes", type=str, default="00,30,60,68", help="目标市场代码，用逗号分隔，默认为00,30,60,68")
    parser.add_argument("--config", type=str, default="config/config.yaml", help="配置文件路径")
    args = parser.parse_args()
    
    # 解析市场代码
    target_market_codes = set(args.market_codes.split(','))
    
    # 创建获取器
    fetcher = SuspendFetcher(
        config_path=args.config,
        target_market_codes=target_market_codes,
        verbose=args.verbose,
        max_workers=args.max_workers,
        batch_size=args.batch_size
    )
    
    if args.mock:
        logger.warning("使用模拟数据模式")
        # 生成模拟数据
        df = fetcher.create_mock_data()
        # 保存到MongoDB
        fetcher.save_to_mongodb(df)
        logger.success("模拟数据获取和保存成功")
        sys.exit(0)
    
    if args.ts_code:
        # 单个股票处理模式
        logger.info(f"获取单个股票 {args.ts_code} 的停牌数据")
        df = fetcher.fetch_suspend_by_code(
            ts_code=args.ts_code
        )
        
        if not df.empty:
            # 过滤目标板块
            code_prefix = args.ts_code[:6][:2] if len(args.ts_code) >= 6 else ""
            if code_prefix in target_market_codes:
                # 保存到MongoDB
                fetcher.save_to_mongodb(df)
                logger.success(f"股票 {args.ts_code} 的停牌数据获取和保存成功，共 {len(df)} 条记录")
            else:
                logger.warning(f"股票 {args.ts_code} 不在目标市场 {target_market_codes} 中，不保存数据")
        else:
            logger.warning(f"未获取到股票 {args.ts_code} 的停牌数据")
        
        sys.exit(0)
    
    # 构建运行配置
    run_config = {
        "recent": args.recent,
        "full": args.full,
        "batch_size": args.batch_size
    }
    
    # 运行获取器
    logger.info("开始获取停牌数据...")
    success = fetcher.run(config=run_config)
    
    if success:
        logger.success("停牌数据获取和保存成功")
        sys.exit(0)
    else:
        logger.error("停牌数据获取或保存失败")
        sys.exit(1)


if __name__ == "__main__":
    main() 