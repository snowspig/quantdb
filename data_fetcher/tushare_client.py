"""
Tushare API客户端模块,用于与Tushare API交互获取金融数据
"""
import time
import json
import os
from typing import Dict, Any, List, Optional, Union
import pandas as pd
import requests
import socket
from loguru import logger
from retry import retry
from requests.adapters import HTTPAdapter
from urllib3.connection import HTTPConnection
from urllib3.connectionpool import HTTPConnectionPool
from config import config_manager


class SocketHTTPConnection(HTTPConnection):
    """使用自定义套接字的HTTP连接"""
    
    def __init__(self, *args, **kwargs):
        self._socket = kwargs.pop('socket', None)
        super(SocketHTTPConnection, self).__init__(*args, **kwargs)
    
    def connect(self):
        """使用提供的套接字连接到服务器"""
        if self._socket:
            self.sock = self._socket
            logger.debug(f"使用预绑定的套接字连接到 {self.host}:{self.port}")
        else:
            # 无套接字则使用默认连接方式
            super().connect()


class SocketHTTPConnectionPool(HTTPConnectionPool):
    """使用自定义套接字的HTTP连接池"""
    
    def __init__(self, *args, **kwargs):
        self._socket = kwargs.pop('socket', None)
        super(SocketHTTPConnectionPool, self).__init__(*args, **kwargs)
    
    def _new_conn(self):
        """创建新的连接"""
        return SocketHTTPConnection(
            host=self.host,
            port=self.port,
            timeout=self.timeout.connect_timeout,
            strict=self.strict,
            socket=self._socket
        )


class SocketHTTPAdapter(HTTPAdapter):
    """使用自定义套接字的HTTP适配器"""
    
    def __init__(self, *args, **kwargs):
        self._socket = kwargs.pop('socket', None)
        super(SocketHTTPAdapter, self).__init__(*args, **kwargs)
    
    def get_connection(self, url, proxies=None):
        """获取连接"""
        conn = super(SocketHTTPAdapter, self).get_connection(url, proxies)
        conn._socket = self._socket
        return conn
    
    def init_poolmanager(self, *args, **kwargs):
        """初始化连接池管理器"""
        kwargs['socket'] = self._socket
        super(SocketHTTPAdapter, self).init_poolmanager(*args, **kwargs)


class TushareClient:
    """
    Tushare API客户端类,处理与Tushare API的通信
    """
    
    def __init__(self, token: str = None, api_url: str = None):
        """
        初始化Tushare客户端
        
        Args:
            token: Tushare API密钥,如果为None则从配置中获取
            api_url: Tushare API URL,如果为None则从配置中获取
        """
        self.token = 'b5bb9d57e35cf485f0366eb6581017fb69cefff888da312b0128f3a0'
        self.api_url = 'http://116.128.206.39:7172'
        self.timeout = config_manager.get('tushare', 'timeout', 60)
        self.max_retries = config_manager.get('tushare', 'max_retries', 3)
        self.retry_delay = config_manager.get('tushare', 'retry_delay', 5)
        
        # 用于绑定特定网络接口的套接字
        self._socket = None
        # 自定义session，用于支持套接字绑定
        self._session = None
        
        self._interface_config_cache = {}
        self._load_interface_configs()
        
        logger.info(f"Tushare客户端初始化成功,API URL: {self.api_url}")
    
    def set_socket(self, sock: socket.socket):
        """
        设置用于网络请求的套接字
        
        Args:
            sock: 预先绑定到特定网络接口的套接字
        """
        if not isinstance(sock, socket.socket):
            raise TypeError("提供的对象不是有效的套接字")
        
        self._socket = sock
        
        # 创建自定义session，使用套接字适配器
        self._session = requests.Session()
        adapter = SocketHTTPAdapter(socket=sock)
        self._session.mount('http://', adapter)
        self._session.mount('https://', adapter)
        
        logger.info(f"设置客户端使用自定义套接字: {sock}")
    
    def _load_interface_configs(self):
        """加载接口配置文件"""
        config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'interfaces')
        
        if not os.path.exists(config_dir):
            os.makedirs(config_dir)
            logger.warning(f"接口配置目录不存在,已创建: {config_dir}")
            return
            
        for filename in os.listdir(config_dir):
            if filename.endswith('.json'):
                interface_name = filename.replace('.json', '')
                try:
                    with open(os.path.join(config_dir, filename), 'r', encoding='utf-8') as f:
                        self._interface_config_cache[interface_name] = json.load(f)
                        logger.debug(f"已加载接口配置: {interface_name}")
                except Exception as e:
                    logger.error(f"加载接口配置失败 {filename}: {str(e)}")
    
    def get_interface_config(self, interface_name: str) -> Dict[str, Any]:
        """
        获取接口配置
        
        Args:
            interface_name: 接口名称
            
        Returns:
            接口配置字典
        """
        if interface_name in self._interface_config_cache:
            return self._interface_config_cache[interface_name]
            
        # 尝试加载配置文件
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'config', 'interfaces', f'{interface_name}.json'
        )
        
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    self._interface_config_cache[interface_name] = config
                    return config
            except Exception as e:
                logger.error(f"加载接口配置失败 {interface_name}: {str(e)}")
        
        # 返回默认配置
        return {"fields": [], "params": {}}
    
    @retry(tries=3, delay=2, backoff=2)
    def request(self, api_name: str, params: Dict[str, Any] = None, fields: List[str] = None) -> Dict[str, Any]:
        """
        向Tushare API发送请求
        
        Args:
            api_name: API名称
            params: 请求参数
            fields: 返回字段列表
            
        Returns:
            API响应数据
        """
        if params is None:
            params = {}
            
        # 确保fields是列表类型或None
        if fields is not None and not isinstance(fields, list):
            logger.warning(f"API {api_name} 的fields参数不是列表类型，将转换为列表类型: {fields}")
            
            # 如果是字符串，尝试按照','分割
            if isinstance(fields, str):
                fields = fields.split(',')
            else:
                try:
                    fields = list(fields)  # 尝试转换为列表
                except:
                    logger.error(f"无法将fields参数转换为列表，将使用空列表: {fields}")
                    fields = []
        
        # 对daily API特殊处理，不传递fields，让API返回所有字段
        if api_name == 'daily':
            logger.debug(f"daily API请求不传递fields参数，将使用API默认返回的所有字段")
            fields = None
            
        # 准备请求数据
        request_data = {
            "api_name": api_name,
            "token": self.token,
            "params": params
        }
        
        # 只有在fields不为None时才添加到请求数据中
        if fields is not None:
            request_data["fields"] = fields
        
        # 日志记录添加网络接口信息
        socket_info = ""
        if self._socket:
            socket_info = " (使用绑定套接字)"
        logger.debug(f"发送Tushare API请求{socket_info}: {api_name}, 参数: {params}")
        
        # 对daily API添加额外日志
        if api_name == 'daily':
            logger.debug(f"发送daily API请求{socket_info}, 参数: {params}, 请求字段: {'全部字段' if fields is None else fields}")
        
        try:
            start_time = time.time()
            
            # 打印请求超时设置
            logger.debug(f"API请求超时设置: {self.timeout}秒")
            
            # 根据是否设置了套接字选择不同的发送方式
            if self._session and self._socket:
                logger.debug(f"使用自定义套接字发送请求: {api_name}")
                response = self._session.post(
                    self.api_url,
                    json=request_data,
                    timeout=self.timeout
                )
            else:
                logger.debug(f"使用默认会话发送请求: {api_name}")
                response = requests.post(
                    self.api_url,
                    json=request_data,
                    timeout=self.timeout
                )
                
            elapsed = time.time() - start_time
            
            # 检查响应状态
            if response.status_code != 200:
                logger.error(f"Tushare API请求失败{socket_info}: {api_name}, 状态码: {response.status_code}, 响应: {response.text}")
                raise Exception(f"API请求失败: HTTP {response.status_code}")
            
            # 解析响应
            result = response.json()
            if result.get('code') != 0:
                logger.error(f"Tushare API返回错误{socket_info}: {api_name}, 代码: {result.get('code')}, 消息: {result.get('msg')}")
                raise Exception(f"API错误: {result.get('msg')}")
            
            # 添加调试输出，显示完整返回
            logger.debug(f"API {api_name} 完整返回: {result}")
            
            # 检查是否有数据返回，显示详细信息
            if "data" in result and "items" in result["data"]:
                items = result["data"]["items"]
                logger.debug(f"API {api_name} 返回数据项数量: {len(items)}")
                if len(items) > 0:
                    logger.debug(f"第一条数据: {items[0]}")
                else:
                    logger.debug(f"API返回空数据集")
            
            # 记录API返回的完整字段信息
            if "data" in result and "fields" in result["data"]:
                available_fields = result["data"]["fields"]
                logger.debug(f"API {api_name} 返回的所有字段(来自响应): {available_fields}")
                
                # 如果有请求的具体字段，检查是否所有字段都可用
                if fields and set(fields) != set(available_fields):
                    missing_fields = set(fields) - set(available_fields)
                    if missing_fields:
                        logger.warning(f"API {api_name} 不支持以下请求的字段: {missing_fields}")
            
            # 对daily API添加数据量统计日志
            if api_name == 'daily' and "data" in result and "items" in result["data"]:
                items_count = len(result["data"]["items"])
                logger.debug(f"API {api_name} 请求成功，获取到 {items_count} 条数据，耗时: {elapsed:.2f}秒")
            else:
                logger.debug(f"Tushare API请求成功{socket_info}: {api_name}, 耗时: {elapsed:.2f}秒")
            
            return result
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Tushare API请求网络错误{socket_info}: {api_name}, 错误: {str(e)}")
            raise
    
    def get_data(
        self, 
        api_name: str, 
        params: Dict[str, Any] = None, 
        fields: List[str] = None,
        use_config_fields: bool = True
    ) -> pd.DataFrame:
        """
        获取数据并转换为DataFrame
        
        Args:
            api_name: API名称
            params: 请求参数
            fields: 返回字段列表
            use_config_fields: 是否使用配置中的字段（当fields为None时）
            
        Returns:
            数据DataFrame
        """
        # 使用接口配置
        interface_config = self.get_interface_config(api_name)
        
        # 合并参数
        if params is None:
            params = {}
        default_params = interface_config.get("params", {})
        for k, v in default_params.items():
            if k not in params:
                params[k] = v
        
        # 使用配置的字段，如果未提供且允许使用配置字段
        if fields is None and "fields" in interface_config and use_config_fields:
            fields = interface_config.get("fields", [])
            
            # 确保fields是列表类型
            if not isinstance(fields, list):
                logger.warning(f"配置中的fields不是列表类型，将转换为列表: {fields}")
                try:
                    if isinstance(fields, str):
                        fields = fields.split(',')
                    else:
                        fields = list(fields)
                except:
                    logger.error(f"无法将配置的fields转换为列表，将使用空列表")
                    fields = []
            
            logger.debug(f"使用配置的字段列表: {fields}")
        
        # 发送请求
        result = self.request(api_name, params, fields)
        
        # 转换为DataFrame
        if "data" in result and "items" in result["data"]:
            data = result["data"]["items"]
            columns = result["data"].get("fields", fields)
            
            if data and columns:
                df = pd.DataFrame(data, columns=columns)
                
                # 记录获取到的所有字段
                available_fields = list(df.columns)
                logger.debug(f"API {api_name} 获取到的所有字段: {available_fields}")
                
                # 如果配置的字段与获取到的字段不一致，记录差异
                if fields and set(fields) != set(available_fields):
                    missing_fields = set(fields) - set(available_fields)
                    extra_fields = set(available_fields) - set(fields)
                    if missing_fields:
                        logger.warning(f"API {api_name} 缺少配置的字段: {missing_fields}")
                    if extra_fields:
                        logger.debug(f"API {api_name} 包含未配置的额外字段: {extra_fields}")
                
                return df
            else:
                logger.warning(f"API {api_name} 返回空数据")
                return pd.DataFrame(columns=columns if columns else [])
        else:
            logger.error(f"API响应格式不正确: {result}")
            return pd.DataFrame()

    def set_timeout(self, timeout: int):
        """
        设置API请求超时时间
        
        Args:
            timeout: 超时时间（秒）
        """
        if timeout > 0:
            self.timeout = timeout
            logger.info(f"设置API请求超时时间为 {timeout} 秒")
        else:
            logger.warning(f"无效的超时时间值: {timeout}，将保持当前值: {self.timeout} 秒")


# 创建全局Tushare客户端实例
tushare_client = TushareClient()

def set_timeout(timeout: int):
    """
    设置Tushare API请求的全局超时时间
    
    Args:
        timeout: 超时时间（秒）
    """
    return tushare_client.set_timeout(timeout) 