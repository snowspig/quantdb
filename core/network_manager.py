#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
网络管理模块
负责管理多WAN口连接和网络请求分发
"""
import os
import sys
import time
import logging
import random
import socket
import requests
import threading
from typing import Dict, List, Any, Optional, Union, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor
from requests.exceptions import RequestException, ConnectionError, Timeout

# 导入配置管理器
from .config_manager import config_manager

class NetworkManager:
    """
    网络管理器类
    提供多WAN口管理和网络请求功能，包括：
    1. 多WAN口负载均衡
    2. 请求重试和容错
    3. 并发请求处理
    4. 会话管理
    """
    
    _instance = None
    
    def __new__(cls):
        """单例模式实现"""
        if cls._instance is None:
            cls._instance = super(NetworkManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初始化网络管理器"""
        # 避免重复初始化
        if hasattr(self, '_initialized') and self._initialized:
            return
            
        # 设置日志
        self.logger = logging.getLogger("core.NetworkManager")
        if not self.logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            self.logger.addHandler(console_handler)
            
            # 设置日志级别
            log_level = os.environ.get("QUANTDB_LOG_LEVEL", "INFO").upper()
            self.logger.setLevel(getattr(logging, log_level, logging.INFO))
            
        # 读取网络配置
        self.wan_config = config_manager.get_wan_config()
        self.wan_enabled = config_manager.is_wan_enabled()
        
        # 初始化WAN接口列表
        self.interfaces = config_manager.get_wan_interfaces()
        self.active_interfaces = []
        
        # 会话管理
        self.sessions = {}
        self.session_lock = threading.Lock()
        self.session_counter = 0
        
        # 负载均衡相关
        self.current_interface_index = 0
        self.interface_lock = threading.Lock()
        
        # 默认请求配置
        self.default_timeout = self.wan_config.get('timeout', 30)
        self.default_retries = self.wan_config.get('retries', 3)
        self.backoff_factor = self.wan_config.get('backoff_factor', 0.3)
        
        # 初始化WAN接口
        self._init_wan_interfaces()
        
        # 标记为已初始化
        self._initialized = True
        
        self.logger.info(f"网络管理器初始化完成，多WAN口启用状态: {self.wan_enabled}")
        
    def _init_wan_interfaces(self):
        """初始化WAN接口"""
        if not self.wan_enabled:
            self.logger.debug("多WAN口功能未启用")
            return
            
        # 检查配置的接口
        if not self.interfaces:
            self.logger.warning("未配置WAN接口，多WAN口功能将不可用")
            self.wan_enabled = False
            return
            
        # 测试接口连通性
        self.active_interfaces = []
        for interface in self.interfaces:
            if self._check_interface(interface):
                self.active_interfaces.append(interface)
                
        if self.active_interfaces:
            self.logger.info(f"已激活 {len(self.active_interfaces)}/{len(self.interfaces)} 个WAN接口")
        else:
            self.logger.warning("所有WAN接口均不可用，将使用默认网络")
            self.wan_enabled = False
            
    def _check_interface(self, interface: Dict) -> bool:
        """
        检查接口是否可用
        
        Args:
            interface: 接口配置
            
        Returns:
            bool: 是否可用
        """
        try:
            interface_name = interface.get('name', 'unknown')
            interface_ip = interface.get('source_ip')
            
            if not interface_ip:
                self.logger.warning(f"接口 {interface_name} 未配置源IP，将被跳过")
                return False
                
            # 简单连通性测试
            test_host = "www.baidu.com"
            test_port = 443
            
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3)
            
            # 绑定到指定接口IP
            s.bind((interface_ip, 0))
            
            # 连接测试
            s.connect((test_host, test_port))
            s.close()
            
            self.logger.debug(f"接口 {interface_name} ({interface_ip}) 连通性测试通过")
            return True
            
        except Exception as e:
            self.logger.warning(f"接口 {interface.get('name', 'unknown')} ({interface.get('source_ip', 'unknown')}) 不可用: {str(e)}")
            return False
            
    def _get_next_interface(self) -> Dict:
        """
        获取下一个可用接口（轮询算法）
        
        Returns:
            Dict: 接口配置
        """
        if not self.active_interfaces:
            return {}
            
        with self.interface_lock:
            interface = self.active_interfaces[self.current_interface_index]
            self.current_interface_index = (self.current_interface_index + 1) % len(self.active_interfaces)
            
        return interface
        
    def _get_random_interface(self) -> Dict:
        """
        随机获取一个可用接口
        
        Returns:
            Dict: 接口配置
        """
        if not self.active_interfaces:
            return {}
            
        return random.choice(self.active_interfaces)
        
    def create_session(self, name: str = None, interface: Dict = None, 
                      retries: int = None, timeout: int = None) -> str:
        """
        创建一个新的HTTP会话
        
        Args:
            name: 会话名称（用于标识，可选）
            interface: 指定接口配置（可选）
            retries: 重试次数（可选）
            timeout: 超时时间（可选）
            
        Returns:
            str: 会话ID
        """
        # 使用默认值
        if retries is None:
            retries = self.default_retries
            
        if timeout is None:
            timeout = self.default_timeout
            
        # 创建会话
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # 设置超时
        session.timeout = timeout
        
        # 若启用多WAN且未指定接口，则选择一个接口
        selected_interface = None
        if self.wan_enabled and not interface:
            selected_interface = self._get_next_interface()
        elif interface:
            selected_interface = interface
            
        # 生成会话ID
        with self.session_lock:
            session_id = str(self.session_counter)
            self.session_counter += 1
            
        # 保存会话信息
        self.sessions[session_id] = {
            'session': session,
            'interface': selected_interface,
            'name': name or f"session_{session_id}",
            'created_at': time.time()
        }
        
        self.logger.debug(f"创建会话 {session_id} - {name or f'session_{session_id}'}")
        return session_id
        
    def close_session(self, session_id: str) -> bool:
        """
        关闭并移除会话
        
        Args:
            session_id: 会话ID
            
        Returns:
            bool: 是否成功关闭
        """
        if session_id not in self.sessions:
            self.logger.warning(f"会话不存在: {session_id}")
            return False
            
        try:
            session_info = self.sessions[session_id]
            session_info['session'].close()
            del self.sessions[session_id]
            self.logger.debug(f"关闭会话: {session_id} - {session_info['name']}")
            return True
            
        except Exception as e:
            self.logger.error(f"关闭会话失败: {session_id}, 错误: {str(e)}")
            return False
            
    def get_session(self, session_id: str) -> Optional[requests.Session]:
        """
        获取会话对象
        
        Args:
            session_id: 会话ID
            
        Returns:
            Optional[requests.Session]: 会话对象
        """
        if session_id not in self.sessions:
            self.logger.warning(f"会话不存在: {session_id}")
            return None
            
        return self.sessions[session_id]['session']
        
    def request(self, method: str, url: str, session_id: str = None, 
                interface: Dict = None, **kwargs) -> Optional[requests.Response]:
        """
        发送HTTP请求
        
        Args:
            method: 请求方法（GET、POST等）
            url: 请求URL
            session_id: 会话ID（可选）
            interface: 指定接口（可选）
            **kwargs: 其他请求参数
            
        Returns:
            Optional[requests.Response]: 请求响应
        """
        # 处理会话
        session = None
        session_info = None
        
        if session_id:
            if session_id in self.sessions:
                session_info = self.sessions[session_id]
                session = session_info['session']
            else:
                self.logger.warning(f"会话不存在: {session_id}，将创建临时会话")
                
        if not session:
            # 创建临时会话
            session = requests.Session()
            
            # 配置重试策略
            retry_strategy = Retry(
                total=kwargs.pop('retries', self.default_retries),
                backoff_factor=self.backoff_factor,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            
        # 设置超时
        timeout = kwargs.pop('timeout', self.default_timeout)
        
        # 确定请求接口
        selected_interface = None
        if interface:
            selected_interface = interface
        elif session_info and session_info['interface']:
            selected_interface = session_info['interface']
        elif self.wan_enabled:
            selected_interface = self._get_next_interface()
            
        # 执行请求
        try:
            # 设置源IP
            if selected_interface and 'source_ip' in selected_interface:
                source_ip = selected_interface['source_ip']
                proxies = {
                    'http': f'http://{source_ip}',
                    'https': f'http://{source_ip}'
                }
                kwargs['proxies'] = proxies
                
            # 发送请求
            self.logger.debug(f"发送 {method.upper()} 请求: {url}")
            response = session.request(method, url, timeout=timeout, **kwargs)
            return response
            
        except RequestException as e:
            self.logger.error(f"请求失败: {url}, 方法: {method}, 错误: {str(e)}")
            return None
            
    def get(self, url: str, **kwargs) -> Optional[requests.Response]:
        """
        发送GET请求
        
        Args:
            url: 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            Optional[requests.Response]: 请求响应
        """
        return self.request('get', url, **kwargs)
        
    def post(self, url: str, **kwargs) -> Optional[requests.Response]:
        """
        发送POST请求
        
        Args:
            url: 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            Optional[requests.Response]: 请求响应
        """
        return self.request('post', url, **kwargs)
        
    def parallel_requests(self, requests_list: List[Dict], max_workers: int = None) -> List[Dict]:
        """
        并行发送多个请求
        
        Args:
            requests_list: 请求配置列表，格式为 [
                {
                    'method': 'get',
                    'url': 'http://example.com',
                    'kwargs': {...}  # 其他请求参数
                },
                ...
            ]
            max_workers: 最大并行工作线程数
            
        Returns:
            List[Dict]: 响应结果列表，格式为 [
                {
                    'status': 'success' | 'error',
                    'response': Response 对象 或 None,
                    'error': 错误信息,
                    'request': 原始请求配置
                },
                ...
            ]
        """
        if not requests_list:
            return []
            
        if max_workers is None:
            # 并行数不超过CPU核心数的2倍
            max_workers = min(len(requests_list), os.cpu_count() * 2 or 4)
            
        results = []
        
        def _process_request(req_config):
            """处理单个请求"""
            method = req_config.get('method', 'get').lower()
            url = req_config.get('url')
            kwargs = req_config.get('kwargs', {})
            
            if not url:
                return {
                    'status': 'error',
                    'response': None,
                    'error': '请求URL不能为空',
                    'request': req_config
                }
                
            try:
                response = self.request(method, url, **kwargs)
                
                if response:
                    return {
                        'status': 'success',
                        'response': response,
                        'error': None,
                        'request': req_config
                    }
                else:
                    return {
                        'status': 'error',
                        'response': None,
                        'error': '请求失败，无响应',
                        'request': req_config
                    }
                    
            except Exception as e:
                return {
                    'status': 'error',
                    'response': None,
                    'error': str(e),
                    'request': req_config
                }
                
        # 使用线程池并行处理请求
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(_process_request, requests_list))
            
        return results
        
    def check_connectivity(self, url: str = "https://www.baidu.com") -> bool:
        """
        检查网络连通性
        
        Args:
            url: 测试URL
            
        Returns:
            bool: 是否连通
        """
        try:
            response = self.get(url, timeout=5)
            return response is not None and response.status_code == 200
        except Exception as e:
            self.logger.error(f"网络连通性检查失败: {str(e)}")
            return False
            
    def refresh_interfaces(self) -> int:
        """
        刷新接口状态
        
        Returns:
            int: 可用接口数量
        """
        if not self.wan_enabled:
            return 0
            
        # 测试接口连通性
        self.active_interfaces = []
        for interface in self.interfaces:
            if self._check_interface(interface):
                self.active_interfaces.append(interface)
                
        self.logger.info(f"刷新接口状态：{len(self.active_interfaces)}/{len(self.interfaces)} 个接口可用")
        
        # 如果没有可用接口，关闭多WAN功能
        if not self.active_interfaces:
            self.logger.warning("所有WAN接口均不可用，将使用默认网络")
            self.wan_enabled = False
            
        return len(self.active_interfaces)
        
    def get_interfaces_status(self) -> Dict:
        """
        获取所有接口的状态信息
        
        Returns:
            Dict: 接口状态信息
        """
        result = {
            'enabled': self.wan_enabled,
            'total_interfaces': len(self.interfaces),
            'active_interfaces': len(self.active_interfaces),
            'interfaces': []
        }
        
        # 检查每个接口的状态
        for interface in self.interfaces:
            active = interface in self.active_interfaces
            
            interface_info = {
                'name': interface.get('name', 'unknown'),
                'source_ip': interface.get('source_ip', 'unknown'),
                'active': active
            }
            
            result['interfaces'].append(interface_info)
            
        return result


# 创建全局网络管理器实例
network_manager = NetworkManager()