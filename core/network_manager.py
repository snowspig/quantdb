#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
网络管理模块
负责管理多WAN口连接和网络请求分发
"""
import os
import time
import logging
import random
import socket
import requests
import threading
import json
import urllib.parse
from typing import Dict, List, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from requests.exceptions import RequestException

# 导入配置管理器 - 统一使用绝对导入
# from .config_manager import ConfigManager # 不再使用相对导入
from core.config_manager import ConfigManager # 使用绝对导入

# 使用函数导入WAN管理模块组件，以避免模块级变量在导入时自动初始化
from .wan_manager import get_port_allocator, get_load_balancer


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
    _init_lock = threading.Lock()  # 添加初始化锁，确保线程安全
    
    def __new__(cls):
        """单例模式实现"""
        if cls._instance is None:
            with cls._init_lock:  # 使用锁确保线程安全
                if cls._instance is None:  # 双重检查锁定
                    cls._instance = super(NetworkManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """初始化网络管理器"""
        self.logger = logging.getLogger('core.NetworkManager')
        
        # 确保单例模式
        if hasattr(NetworkManager, '_initialized') and NetworkManager._initialized:
            return
            
        # 标记为已初始化
        NetworkManager._initialized = True
        
        # 加载配置 - 获取实例并使用
        try:
            _config_manager = ConfigManager() # 获取单例实例
            self.config = _config_manager.get_all_config()
        except Exception as e:
            self.logger.error(f"无法加载配置: {str(e)}")
            self.config = {}
            
        # 初始化网络组件
        try:
            # 初始化会话存储
            self._sessions = {}
            
            # 请求追踪
            self._processing_requests = set()
            self._request_counter = 0
            self._request_lock = threading.Lock()
            
            # 请求ID与WAN口映射
            self._request_wan_mapping = {}
            
            # 初始化WAN接口
            self._init_wan_interfaces()
            self.logger.info("网络管理器初始化完成")
        except Exception as e:
            self.logger.error(f"网络管理器初始化失败: {str(e)}")
            # 确保初始化失败时也有默认值
            self.interfaces = []
            self.wan_enabled = False
        
        # 启动WAN监控
        try:
            # 导入WAN监控器，如果未初始化则通过访问get_wan_monitor触发延迟初始化
            from core.wan_manager import get_wan_monitor
            self.wan_monitor = get_wan_monitor()
            
            # 确保WAN监控器已启动
            if self.wan_monitor:
                try:
                    self.wan_monitor.start_monitoring()
                    self.logger.info("WAN监控器已启动")
                except Exception as e:
                    self.logger.warning(f"启动WAN监控器失败: {str(e)}")
            else:
                self.logger.warning("WAN监控器不可用")
        except Exception as e:
            self.logger.warning(f"获取WAN监控器失败: {str(e)}")
            self.wan_monitor = None
    
    def __del__(self):
        """析构函数，在对象销毁时执行清理"""
        try:
            # 停止WAN监控
            if hasattr(self, 'wan_monitor') and self.wan_monitor:
                self.wan_monitor.stop_monitoring()
        except:
            pass
    
    def _init_wan_interfaces(self):
        """初始化WAN接口"""
        # 确保网络配置存在
        network_config = self.config.get('network', {})
        wan_config = network_config.get('wan', {})
        
        # 初始化WAN启用状态
        self.wan_enabled = wan_config.get('enabled', False)
        
        # 获取配置的接口列表
        self.interfaces = wan_config.get('interfaces', [])
        
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
        self.interface_id_map = {}
        
        # 把interfaces列表映射为id索引
        for idx, interface in enumerate(self.interfaces):
            self.interface_id_map[idx] = interface
            interface['wan_idx'] = idx  # 添加WAN索引
            
            if self._check_interface(interface):
                self.active_interfaces.append(interface)
                
        # 更新负载均衡器的可用WAN列表
        try:
            available_wan_idxs = [interface.get('wan_idx') for interface in self.active_interfaces]
            lb = get_load_balancer()
            if lb:
                lb.update_available_wans(available_wan_idxs)
        except Exception as e:
            self.logger.warning(f"更新负载均衡器失败: {str(e)}")
                
        if self.active_interfaces:
            self.logger.info(f"已激活 {len(self.active_interfaces)}/{len(self.interfaces)} 个WAN接口")
        else:
            self.logger.warning("所有WAN接口均不可用，将使用默认网络")
            self.wan_enabled = False
            
    def _check_interface(self, interface: Dict) -> bool:
        """
        检查接口是否可用并验证外部IP
        
        Args:
            interface: 接口配置
            
        Returns:
            bool: 是否可用
        """
        interface_name = interface.get('name', 'unknown')
        interface_ip = interface.get('source_ip')
        wan_idx = interface.get('wan_idx', -1)
        
        if not interface_ip:
            self.logger.warning(f"接口 {interface_name} 未配置源IP，将被跳过")
            return False
        
        # 避免重复检查，如果接口已经有external_ip且验证时间不超过5分钟，直接返回
        if 'external_ip' in interface and 'last_verified' in interface:
            if time.time() - interface['last_verified'] < 300:  # 5分钟内不重复检查
                return True
            
        try:    
            # 测试URL
            test_url = "http://106.14.185.239:29990/test"
            
            # 使用socket方式请求测试URL
            try:
                # 解析URL
                parsed_url = urllib.parse.urlparse(test_url)
                host = parsed_url.hostname
                port = parsed_url.port or 80
                
                # 尝试获取端口分配器
                try:
                    port_allocator = get_port_allocator()
                    if not port_allocator:
                        self.logger.warning(f"无法获取端口分配器，接口 {interface_name} 将被跳过")
                        return False
                        
                    # 分配端口
                    local_port = port_allocator.allocate_port(wan_idx)
                    if not local_port:
                        self.logger.warning(f"无法为接口 {interface_name} (WAN {wan_idx}) 分配本地端口")
                        return False
                except Exception as e:
                    self.logger.warning(f"端口分配失败: {str(e)}")
                    return False
                
                # 创建socket
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    # 绑定到特定端口
                    sock.bind(('0.0.0.0', local_port))
                    
                    # 连接到服务器
                    sock.settimeout(5)
                    sock.connect((host, port))
                    
                    # 构建HTTP请求
                    request = f"GET {parsed_url.path}?wan_idx={wan_idx} HTTP/1.1\r\n"
                    request += f"Host: {host}:{port}\r\n"
                    request += "Connection: close\r\n\r\n"
                    
                    # 发送请求
                    sock.sendall(request.encode())
                    
                    # 接收响应
                    response = b""
                    while True:
                        chunk = sock.recv(4096)
                        if not chunk:
                            break
                        response += chunk
                        
                    # 解析响应
                    response_str = response.decode('utf-8', errors='ignore')
                    
                    # 提取JSON部分
                    json_start = response_str.find('{')
                    if json_start != -1:
                        try:
                            data = json.loads(response_str[json_start:])
                            external_ip = data.get('your_ip')
                            
                            if not external_ip:
                                self.logger.warning(f"接口 {interface_name} (WAN {wan_idx}) 无法获取外部IP")
                                return False
                                
                            # 将外部IP保存到接口配置中
                            interface['external_ip'] = external_ip
                            interface['last_verified'] = time.time()
                            
                            self.logger.info(f"接口 {interface_name} (WAN {wan_idx}) 测试通过，外部IP: {external_ip}")
                            return True
                            
                        except json.JSONDecodeError:
                            self.logger.warning(f"接口 {interface_name} (WAN {wan_idx}) 返回的数据不是有效的JSON")
                            return False
                    else:
                        self.logger.warning(f"接口 {interface_name} (WAN {wan_idx}) 返回的数据不包含JSON")
                        return False
                        
                finally:
                    sock.close()
                    # 释放端口
                    try:
                        if port_allocator and local_port:
                            port_allocator.release_port(wan_idx, local_port)
                    except Exception as e:
                        self.logger.warning(f"释放端口失败: {str(e)}")
                    
            except Exception as e:
                self.logger.warning(f"通过Socket测试接口 {interface_name} (WAN {wan_idx}) 失败: {str(e)}")
                
                # 回退到HTTP请求测试（暂时禁用，避免循环依赖）
                return False
                
        except Exception as e:
            self.logger.warning(f"接口 {interface.get('name', 'unknown')} ({interface.get('source_ip', 'unknown')}) 不可用: {str(e)}")
            return False
            
    def validate_multi_wan(self) -> Dict:
        """
        验证多WAN接口是否有不同的外部IP
        
        Returns:
            Dict: 验证结果
        """
        if not self.wan_enabled:
            return {
                'status': 'disabled',
                'message': '多WAN口功能未启用',
                'validation': False
            }
            
        if len(self.active_interfaces) < 2:
            return {
                'status': 'insufficient',
                'message': f'可用WAN接口数量不足，当前只有 {len(self.active_interfaces)} 个接口',
                'validation': False,
                'interfaces': self.active_interfaces
            }
            
        # 验证每个接口的外部IP是否不同
        external_ips = set()
        ip_info = []
        
        for interface in self.active_interfaces:
            # 如果接口没有保存external_ip，重新测试
            if 'external_ip' not in interface:
                if not self._check_interface(interface):
                    continue
                    
            external_ip = interface.get('external_ip')
            external_ips.add(external_ip)
            
            ip_info.append({
                'name': interface.get('name'),
                'source_ip': interface.get('source_ip'),
                'external_ip': external_ip,
                'wan_idx': interface.get('wan_idx', -1)
            })
            
        # 判断是否所有接口都有不同的外部IP
        is_valid = len(external_ips) == len(ip_info)
        
        result = {
            'status': 'valid' if is_valid else 'invalid',
            'message': f'多WAN验证{("通过" if is_valid else "失败")}，有 {len(external_ips)} 个不同的外部IP',
            'validation': is_valid,
            'interfaces': ip_info
        }
            
        # 只有在debug级别时输出日志，避免重复
        self.logger.debug(f"多WAN验证结果: {result['message']}")
        
        return result
            
    def _get_interface_by_wan_idx(self, wan_idx: int) -> Dict:
        """
        根据WAN索引获取接口配置
        
        Args:
            wan_idx: WAN接口索引
            
        Returns:
            Dict: 接口配置，如果不存在返回空字典
        """
        for interface in self.active_interfaces:
            if interface.get('wan_idx') == wan_idx:
                return interface
        return {}
        
    def _get_balanced_interface(self) -> Dict:
        """
        使用负载均衡器获取一个接口
        
        Returns:
            Dict: 接口配置
        """
        if not self.active_interfaces or not self.wan_enabled:
            return {}
            
        # 使用负载均衡器选择WAN接口
        try:
            lb = get_load_balancer()
            if not lb:
                # 如果负载均衡器不可用，随机选择接口
                self.logger.warning("负载均衡器不可用，随机选择WAN接口")
                if self.active_interfaces:
                    return random.choice(self.active_interfaces)
                return {}
                
            wan_idx = lb.select_wan()
            
            if wan_idx == -1:
                return {}
                
            # 根据WAN索引获取接口配置
            return self._get_interface_by_wan_idx(wan_idx)
        except Exception as e:
            self.logger.warning(f"获取平衡接口失败: {str(e)}")
            # 如果有可用接口，随机选择一个
            if self.active_interfaces:
                return random.choice(self.active_interfaces)
            return {}
        
    def create_session(self, name: str = None, interface: Dict = None, wan_idx: int = None,
                      retries: int = None, timeout: int = None) -> str:
        """
        创建一个新的HTTP会话
        
        Args:
            name: 会话名称（用于标识，可选）
            interface: 指定接口配置（可选）
            wan_idx: 指定WAN接口索引（可选，优先级高于interface）
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
        
        # 选择接口
        selected_interface = None
        
        # 如果指定了WAN索引，使用该索引对应的接口
        if wan_idx is not None and self.wan_enabled:
            selected_interface = self._get_interface_by_wan_idx(wan_idx)
            
        # 如果指定了接口配置，使用该接口
        elif interface:
            selected_interface = interface
            
        # 若启用多WAN且未指定接口，则使用负载均衡选择接口
        elif self.wan_enabled:
            selected_interface = self._get_balanced_interface()
            
        # 生成会话ID
        with self.session_lock:
            session_id = str(self.session_counter)
            self.session_counter += 1
            
        # 保存会话信息
        self.sessions[session_id] = {
            'session': session,
            'interface': selected_interface,
            'name': name or f"session_{session_id}",
            'created_at': time.time(),
            'wan_idx': selected_interface.get('wan_idx') if selected_interface else None
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
                interface: Dict = None, wan_idx: int = None, **kwargs) -> Optional[requests.Response]:
        """
        发送HTTP请求
        
        Args:
            method: 请求方法（GET、POST等）
            url: 请求URL
            session_id: 会话ID（可选）
            interface: 指定接口（可选）
            wan_idx: 指定WAN接口索引（可选，优先级高于interface）
            **kwargs: 其他请求参数
            
        Returns:
            Optional[requests.Response]: 请求响应
        """
        # 处理会话
        session = None
        session_info = None
        local_port = None
        original_create_connection = None  # 保存原始socket连接方法
        
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
        selected_wan_idx = None
        
        # 如果指定了WAN索引，使用该索引对应的接口
        if wan_idx is not None and self.wan_enabled:
            selected_interface = self._get_interface_by_wan_idx(wan_idx)
            selected_wan_idx = wan_idx
            
        # 如果指定了接口配置，使用该接口
        elif interface:
            selected_interface = interface
            selected_wan_idx = interface.get('wan_idx')
            
        # 如果会话有绑定的接口，使用会话接口
        elif session_info and session_info['interface']:
            selected_interface = session_info['interface']
            selected_wan_idx = selected_interface.get('wan_idx')
            
        # 若启用多WAN且未指定接口，则使用负载均衡选择接口
        elif self.wan_enabled:
            selected_interface = self._get_balanced_interface()
            selected_wan_idx = selected_interface.get('wan_idx')
            
        # 执行请求
        try:
            start_time = time.time()
            
            # 从kwargs中移除socket专用参数，避免传递给requests库
            source_address = kwargs.pop('source_address', None)
            
            # 如果使用端口分配方式
            if self.wan_enabled and selected_wan_idx is not None and self.wan_config.get('use_port_binding', True):
                # 尝试使用socket方式发送请求
                try:
                    # 尝试从环境变量获取端口(如果已经设置)
                    if 'LOCAL_BIND_PORT' in os.environ:
                        local_port = int(os.environ['LOCAL_BIND_PORT'])
                        self.logger.debug(f"使用已设置的端口: {local_port}")
                    else:
                        # 分配端口
                        local_port = get_port_allocator().allocate_port(selected_wan_idx)
                        if not local_port:
                            self.logger.warning(f"无法为WAN {selected_wan_idx} 分配端口，回退到代理方式")
                        else:
                            # 设置环境变量供底层socket使用
                            os.environ['LOCAL_BIND_PORT'] = str(local_port)
                            self.logger.debug(f"设置端口绑定，WAN {selected_wan_idx} 端口 {local_port}")
                            
                            # 保存原始连接方法
                            original_create_connection = socket.create_connection
                            
                            # 定义socket创建钩子，用于绑定特定端口
                            def create_connection_with_binding(address, *args, **kwargs):
                                # 使用环境变量中的端口
                                bind_port = int(os.environ.get('LOCAL_BIND_PORT', 0))
                                self.logger.debug(f"尝试绑定端口: {bind_port} 连接到 {address}")
                                if bind_port > 0:
                                    try:
                                        # 创建socket
                                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                        # 设置更短的超时时间
                                        sock.settimeout(min(timeout, 10))
                                        # 设置socket选项
                                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                                        # 绑定到特定端口
                                        sock.bind(('0.0.0.0', bind_port))
                                        # 连接到服务器
                                        sock.connect(address)
                                        self.logger.debug(f"成功使用端口 {bind_port} 连接到 {address}")
                                        return sock
                                    except Exception as e:
                                        self.logger.warning(f"使用端口 {bind_port} 连接失败: {str(e)}，回退到默认连接")
                                        return original_create_connection(address, *args, **kwargs)
                                else:
                                    # 回退到默认连接方法
                                    return original_create_connection(address, *args, **kwargs)
                            
                            # 替换socket的连接方法
                            socket.create_connection = create_connection_with_binding
                            
                except Exception as e:
                    self.logger.warning(f"设置端口绑定失败: {str(e)}")
                    local_port = None
                    
            # 使用源IP代理方式（旧方式，用作备选）
            elif selected_interface and 'source_ip' in selected_interface:
                source_ip = selected_interface['source_ip']
                proxies = {
                    'http': f'http://{source_ip}',
                    'https': f'http://{source_ip}'
                }
                kwargs['proxies'] = proxies
                
            # 发送请求
            self.logger.info(f"发送 {method.upper()} 请求: {url}, WAN索引: {selected_wan_idx}, 超时: {timeout}秒")
            
            # 设置更严格的超时控制
            req_timeout = min(timeout, 20)  # 最长20秒超时
            kwargs['timeout'] = req_timeout
            
            # 创建计时器线程，如果请求超时则记录警告
            def log_timeout():
                time.sleep(req_timeout + 1)
                self.logger.warning(f"请求可能已超时: {url}, WAN索引: {selected_wan_idx}, 已等待 {req_timeout+1} 秒")
                
            timeout_timer = threading.Timer(req_timeout + 1, log_timeout)
            timeout_timer.daemon = True
            timeout_timer.start()
            
            try:
                response = session.request(method, url, **kwargs)
                # 取消计时器
                timeout_timer.cancel()
                
            except Exception as e:
                # 取消计时器
                timeout_timer.cancel()
                # 记录错误并重新抛出
                self.logger.error(f"请求异常: {url}, WAN索引: {selected_wan_idx}, 错误: {str(e)}")
                raise
            finally:
                # 恢复socket的原始连接方法
                if original_create_connection is not None:
                    socket.create_connection = original_create_connection
            
            # 请求完成时间
            request_time = time.time() - start_time
            
            # 记录响应状态
            self.logger.info(f"请求完成: {url}, WAN索引: {selected_wan_idx}, 状态码: {response.status_code}, 耗时: {request_time:.3f}秒")
            
            # 如果请求成功且使用了负载均衡，记录成功
            if selected_wan_idx is not None and response and response.status_code == 200:
                get_load_balancer().record_success(selected_wan_idx, request_time)
                
            # 清理环境变量
            if 'LOCAL_BIND_PORT' in os.environ:
                del os.environ['LOCAL_BIND_PORT']
                
            return response
            
        except RequestException as e:
            # 记录失败
            if selected_wan_idx is not None:
                get_load_balancer().record_error(selected_wan_idx)
                
            self.logger.error(f"请求失败: {url}, 方法: {method}, 错误: {str(e)}")
            return None
        finally:
            # 释放分配的端口，如果使用了端口绑定且是我们分配的
            if local_port and selected_wan_idx is not None:
                get_port_allocator().release_port(selected_wan_idx, local_port)
                
            # 清除环境变量
            if 'LOCAL_BIND_PORT' in os.environ:
                del os.environ['LOCAL_BIND_PORT']
            
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
    
    def check_connectivity(self, url: str = "http://106.14.185.239:29990/test") -> bool:
        """
        检查网络连通性
        
        Args:
            url: 测试URL
            
        Returns:
            bool: 是否连通
        """
        try:
            response = self.get(url, timeout=5)
            if response and response.status_code == 200:
                try:
                    # 尝试解析JSON并记录IP
                    data = response.json()
                    if 'your_ip' in data:
                        self.logger.info(f"网络连通性测试通过，检测到的外部IP: {data['your_ip']}")
                    return True
                except ValueError:
                    # 即使JSON解析失败，只要状态码是200也认为连通
                    return True
            return False
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
        
        previously_active = len(self.active_interfaces)
            
        # 测试接口连通性
        self.active_interfaces = []
        self.interface_id_map = {}
        
        # 把interfaces列表映射为id索引
        for idx, interface in enumerate(self.interfaces):
            self.interface_id_map[idx] = interface
            interface['wan_idx'] = idx  # 添加WAN索引
            
            if self._check_interface(interface):
                self.active_interfaces.append(interface)
                
        # 更新负载均衡器的可用WAN列表
        available_wan_idxs = [interface.get('wan_idx') for interface in self.active_interfaces]
        get_load_balancer().update_available_wans(available_wan_idxs)
        
        # 验证多WAN配置，但不输出日志
        multi_wan_valid = False
        if len(self.active_interfaces) >= 2:
            # 只检查结果，不输出日志
            validation_result = self.validate_multi_wan()
            multi_wan_valid = validation_result.get('validation', False)
            
        # 如果接口数量有变化时才输出日志
        if previously_active != len(self.active_interfaces):
            self.logger.info(f"刷新接口状态：{len(self.active_interfaces)}/{len(self.interfaces)} 个接口可用")
            if len(self.active_interfaces) >= 2:
                self.logger.info(f"多WAN验证: {'通过' if multi_wan_valid else '失败'}")
        
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
            
            # 添加外部IP信息（如果有）
            if active and 'external_ip' in interface:
                interface_info['external_ip'] = interface.get('external_ip')
            
            # 添加WAN索引
            if 'wan_idx' in interface:
                interface_info['wan_idx'] = interface.get('wan_idx')
            
            result['interfaces'].append(interface_info)
            
        # 添加多WAN验证状态（如果有多个活跃接口），不输出日志
        if len(self.active_interfaces) >= 2:
            validation = self.validate_multi_wan()
            result['multi_wan_validation'] = {
                'status': validation['status'],
                'message': validation['message'],
                'is_valid': validation['validation']
            }
            
        # 添加负载均衡器状态
        if self.wan_enabled:
            result['load_balancer'] = get_load_balancer().get_stats()
            
        return result
        
    def parallel_requests(self, requests_list: List[Dict], max_workers: int = None) -> List[Dict]:
        """
        并行发送多个请求
        
        Args:
            requests_list: 请求配置列表，格式为 [
                {
                    'method': 'get',
                    'url': 'http://example.com',
                    'kwargs': {...},  # 其他请求参数
                    'wan_idx': 1,     # 可选，指定WAN接口索引
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
            
        self.logger.info(f"开始并行执行 {len(requests_list)} 个请求，最大工作线程数: {max_workers}")
        
        results = []
        
        def _process_request(req_config):
            """处理单个请求"""
            method = req_config.get('method', 'get').lower()
            url = req_config.get('url')
            kwargs = req_config.get('kwargs', {})
            wan_idx = req_config.get('wan_idx')
            
            # 确保每个请求都有超时设置
            if 'timeout' not in kwargs:
                kwargs['timeout'] = min(self.default_timeout, 20)  # 最大20秒超时
            
            # 跟踪请求开始时间
            start_time = time.time()
            
            if not url:
                return {
                    'status': 'error',
                    'response': None,
                    'error': '请求URL不能为空',
                    'request': req_config,
                    'elapsed': 0
                }
                
            try:
                # 添加WAN索引到请求参数
                if wan_idx is not None:
                    kwargs['wan_idx'] = wan_idx
                    
                self.logger.info(f"发送并行请求: {method.upper()} {url}, WAN: {wan_idx}, 超时: {kwargs.get('timeout')}秒")
                
                # 发送请求
                response = self.request(method, url, **kwargs)
                
                # 计算请求耗时
                elapsed = time.time() - start_time
                
                if response:
                    self.logger.info(f"并行请求成功: {url}, WAN: {wan_idx}, 状态码: {response.status_code}, 耗时: {elapsed:.3f}秒")
                    return {
                        'status': 'success',
                        'response': response,
                        'error': None,
                        'request': req_config,
                        'elapsed': elapsed
                    }
                else:
                    self.logger.error(f"并行请求失败: {url}, WAN: {wan_idx}, 无响应, 耗时: {elapsed:.3f}秒")
                    return {
                        'status': 'error',
                        'response': None,
                        'error': '请求失败，无响应',
                        'request': req_config,
                        'elapsed': elapsed
                    }
                    
            except Exception as e:
                elapsed = time.time() - start_time
                self.logger.error(f"并行请求异常: {url}, WAN: {wan_idx}, 错误: {str(e)}, 耗时: {elapsed:.3f}秒")
                return {
                    'status': 'error',
                    'response': None,
                    'error': str(e),
                    'request': req_config,
                    'elapsed': elapsed
                }
                
        # 使用线程池并行处理请求
        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 保存future对象
                futures = []
                for req in requests_list:
                    future = executor.submit(_process_request, req)
                    futures.append(future)
                
                # 设置较短的等待时间
                for future in futures:
                    try:
                        # 最多等待30秒
                        result = future.result(timeout=30)
                        results.append(result)
                    except Exception as e:
                        self.logger.error(f"获取请求结果超时或异常: {str(e)}")
                        results.append({
                            'status': 'error',
                            'response': None,
                            'error': f"结果处理异常: {str(e)}",
                            'request': {'error': 'unknown'},
                            'elapsed': 0
                        })
        except Exception as e:
            self.logger.error(f"并行请求执行异常: {str(e)}")
        
        # 确保结果数量与请求数量一致
        if len(results) != len(requests_list):
            self.logger.warning(f"结果数量({len(results)})与请求数量({len(requests_list)})不一致，可能有请求丢失")
        
        # 统计成功率
        success_count = sum(1 for r in results if r.get('status') == 'success')
        self.logger.info(f"并行请求完成: {success_count}/{len(results)} 成功")
            
        return results

    def request_via_wan(self, url: str, wan_idx: int, **kwargs) -> Dict:
        """
        通过特定WAN口发送请求（直接使用socket方式）
        
        Args:
            url: 请求URL
            wan_idx: WAN口索引
            **kwargs: 其他请求参数
            
        Returns:
            Dict: 请求结果，包含success、response等信息
        """
        self.logger.info(f"通过WAN {wan_idx} 发送请求到 {url}")
        
        try:
            # 从URL解析主机和端口
            parsed_url = urllib.parse.urlparse(url)
            host = parsed_url.hostname
            port = parsed_url.port or 80
            
            # 分配端口
            local_port = get_port_allocator().allocate_port(wan_idx)
            if not local_port:
                raise Exception(f"无法为WAN {wan_idx} 分配端口")
            
            # 创建套接字
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                # 设置连接和读取超时
                sock_timeout = kwargs.get('timeout', self.default_timeout)
                if sock_timeout > 10:  # 限制最大超时时间为10秒
                    sock_timeout = 10
                sock.settimeout(sock_timeout)
                
                # 绑定到指定端口
                sock.bind(('0.0.0.0', local_port))
                self.logger.debug(f"套接字成功绑定到WAN {wan_idx}, 端口 {local_port}")
                
                # 连接到服务器
                start_time = time.time()
                
                try:
                    # 添加连接超时处理
                    sock.settimeout(5)  # 连接超时5秒
                    sock.connect((host, port))
                    
                    # 设置读取超时
                    sock.settimeout(sock_timeout)
                    
                    # 构建HTTP请求
                    request = f"GET {parsed_url.path or '/'}{('?' + parsed_url.query) if parsed_url.query else ''} HTTP/1.1\r\n"
                    request += f"Host: {host}:{port}\r\n"
                    
                    # 添加自定义头部
                    headers = kwargs.get('headers', {})
                    for header, value in headers.items():
                        request += f"{header}: {value}\r\n"
                    
                    # 添加Connection: close头，确保服务器关闭连接
                    request += "Connection: close\r\n\r\n"
                    
                    # 发送请求
                    sock.sendall(request.encode())
                    
                    # 接收响应，设置接收超时
                    response_chunks = []
                    max_response_size = 1024 * 1024  # 最大响应大小限制为1MB
                    total_received = 0
                    
                    # 添加接收超时处理
                    receive_start = time.time()
                    receive_timeout = sock_timeout  # 最长接收时间
                    
                    while time.time() - receive_start < receive_timeout:
                        try:
                            chunk = sock.recv(4096)
                            if not chunk:
                                break  # 连接关闭
                            
                            response_chunks.append(chunk)
                            total_received += len(chunk)
                            
                            if total_received > max_response_size:
                                self.logger.warning(f"WAN {wan_idx} 响应超过大小限制（{max_response_size}字节），截断响应")
                                break
                                
                        except socket.timeout:
                            self.logger.warning(f"WAN {wan_idx} 接收响应超时")
                            break
                    
                    response = b''.join(response_chunks)
                except socket.timeout:
                    raise Exception(f"连接到 {host}:{port} 超时")
                
                # 计算时间和解析响应
                elapsed = time.time() - start_time
                
                # 检查是否收到任何响应
                if not response:
                    return {
                        'success': False,
                        'error': '未收到响应数据',
                        'wan_idx': wan_idx
                    }
                    
                response_str = response.decode('utf-8', errors='ignore')
                
                # 解析HTTP响应
                header_end = response_str.find('\r\n\r\n')
                if header_end == -1:
                    return {
                        'success': False,
                        'error': '无效的HTTP响应',
                        'wan_idx': wan_idx
                    }
                    
                headers_str = response_str[:header_end]
                body = response_str[header_end + 4:]
                
                # 解析状态行
                status_line = headers_str.split('\r\n')[0]
                parts = status_line.split(' ', 2)
                if len(parts) < 3:
                    return {
                        'success': False,
                        'error': '无效的HTTP状态行',
                        'wan_idx': wan_idx
                    }
                    
                status_code = int(parts[1])
                
                # 尝试解析JSON响应
                try:
                    # 查找JSON开始位置
                    json_start = body.find('{')
                    if json_start != -1:
                        data = json.loads(body[json_start:])
                        
                        # 记录成功
                        if wan_idx is not None:
                            get_load_balancer().record_success(wan_idx, elapsed)
                            
                        # 获取外部IP
                        external_ip = data.get('your_ip')
                        if external_ip:
                            self.logger.info(f"WAN {wan_idx} 请求成功，服务器看到的IP: {external_ip}")
                            
                        return {
                            'success': True,
                            'status_code': status_code,
                            'elapsed': elapsed,
                            'data': data,
                            'wan_idx': wan_idx,
                            'external_ip': external_ip
                        }
                    else:
                        return {
                            'success': True,
                            'status_code': status_code,
                            'elapsed': elapsed,
                            'body': body,
                            'wan_idx': wan_idx
                        }
                except json.JSONDecodeError:
                    return {
                        'success': True,
                        'status_code': status_code,
                        'elapsed': elapsed,
                        'body': body,
                        'wan_idx': wan_idx
                    }
            finally:
                # 确保关闭套接字
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except:
                    pass  # 忽略关闭错误
                sock.close()
                # 释放端口
                get_port_allocator().release_port(wan_idx, local_port)
                
        except Exception as e:
            # 记录失败
            if wan_idx is not None:
                get_load_balancer().record_error(wan_idx)
                
            self.logger.error(f"WAN {wan_idx} 请求异常: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'wan_idx': wan_idx
            }
    
    def test_all_wans(self, test_url: str = None, parallel: bool = True) -> List[Dict]:
        """
        测试所有可用的WAN接口，使用Tushare API
        
        Args:
            test_url: 测试URL，如果为None则使用Tushare API
            parallel: 是否并行测试，默认为并行
            
        Returns:
            List[Dict]: 测试结果列表
        """
        self.logger.info("开始测试所有WAN接口...")
        
        # 获取可用WAN列表
        available_wans = []
        for interface in self.active_interfaces:
            wan_idx = interface.get('wan_idx')
            if wan_idx is not None:
                available_wans.append(wan_idx)
        
        if not available_wans:
            self.logger.warning("没有可用的WAN接口")
            return []
        
        # 获取Tushare配置
        all_config = config_manager.get_all_config()
        tushare_config = all_config.get('tushare', {})
        api_url = tushare_config.get('api_url', 'http://116.128.206.39:7172')
        token = tushare_config.get('token', 'b5bb9d57e35cf485f0366eb6581017fb69cefff888da312b0128f3a0')
        
        # 设置测试URL
        if test_url:
            self.logger.info(f"使用指定的测试URL: {test_url}")
            url_to_test = test_url
            # 使用测试URL则直接使用socket方式发送请求
            test_method = self.request_via_wan
        else:
            # 使用Tushare API进行测试
            self.logger.info(f"使用Tushare API进行测试: {api_url}")
            url_to_test = api_url
            
            # 准备Tushare API数据
            tushare_data = {
                'api_name': 'trade_cal',
                'token': token,
                'params': {
                    'exchange': '',
                    'start_date': '20180901',
                    'end_date': '20181001'
                },
                'fields': ''
            }
            
            # 测试方法定义
            def test_tushare_api(wan_idx):
                """测试单个WAN口的Tushare API访问"""
                self.logger.info(f"测试WAN {wan_idx}...")
                
                try:
                    # 获取本地端口
                    local_port = get_port_allocator().allocate_port(wan_idx)
                    if not local_port:
                        self.logger.error(f"无法为WAN {wan_idx} 分配端口")
                        return {
                            'success': False,
                            'error': f"无法分配端口",
                            'wan_idx': wan_idx
                        }
                    
                    try:
                        start_test_time = time.time()
                        
                        # 设置环境变量，让底层socket使用它
                        os.environ['LOCAL_BIND_PORT'] = str(local_port)
                        
                        # 使用request方法发送HTTP请求
                        response = self.request(
                            'post',
                            api_url,
                            wan_idx=wan_idx,
                            json=tushare_data,
                            headers={'Content-Type': 'application/json'},
                            timeout=10
                        )
                        
                        # 清理环境变量
                        if 'LOCAL_BIND_PORT' in os.environ:
                            del os.environ['LOCAL_BIND_PORT']
                        
                        test_elapsed = time.time() - start_test_time
                        
                        if response and response.status_code == 200:
                            try:
                                resp_data = response.json()
                                
                                # 检查返回结果
                                if 'data' in resp_data and resp_data.get('code') == 0:
                                    # 获取数据行数
                                    data_rows = len(resp_data['data'].get('items', []))
                                    
                                    # 获取外部IP (如果有)
                                    external_ip = None
                                    for intf in self.active_interfaces:
                                        if intf.get('wan_idx') == wan_idx and 'external_ip' in intf:
                                            external_ip = intf.get('external_ip')
                                            break
                                    
                                    self.logger.info(f"WAN {wan_idx} 请求成功，获取到 {data_rows} 行数据，耗时: {test_elapsed:.3f}秒")
                                    
                                    # 保存结果
                                    return {
                                        'success': True,
                                        'wan_idx': wan_idx,
                                        'data_rows': data_rows,
                                        'elapsed': test_elapsed,
                                        'external_ip': external_ip
                                    }
                                    
                                else:
                                    error_msg = resp_data.get('msg', '未知错误')
                                    self.logger.error(f"WAN {wan_idx} 请求返回错误: {error_msg}")
                                    
                                    # 保存错误结果
                                    return {
                                        'success': False,
                                        'wan_idx': wan_idx,
                                        'error': f"API错误: {error_msg}",
                                        'elapsed': test_elapsed
                                    }
                                    
                            except Exception as e:
                                self.logger.error(f"WAN {wan_idx} 解析响应失败: {str(e)}")
                                return {
                                    'success': False,
                                    'wan_idx': wan_idx,
                                    'error': f"解析响应失败: {str(e)}"
                                }
                        else:
                            status_code = response.status_code if response else 'N/A'
                            self.logger.error(f"WAN {wan_idx} 请求失败，状态码: {status_code}")
                            
                            return {
                                'success': False,
                                'wan_idx': wan_idx,
                                'error': f"HTTP错误: {status_code}"
                            }
                            
                    except Exception as e:
                        self.logger.exception(f"WAN {wan_idx} Tushare API调用异常: {str(e)}")
                        return {
                            'success': False,
                            'wan_idx': wan_idx,
                            'error': str(e)
                        }
                    finally:
                        # 释放端口
                        get_port_allocator().release_port(wan_idx, local_port)
                        
                        # 确保环境变量已清理
                        if 'LOCAL_BIND_PORT' in os.environ:
                            del os.environ['LOCAL_BIND_PORT']
                        
                except Exception as e:
                    self.logger.exception(f"WAN {wan_idx} 测试过程发生异常: {str(e)}")
                    return {
                        'success': False,
                        'wan_idx': wan_idx,
                        'error': str(e)
                    }
            
            # 设置测试方法
            test_method = test_tushare_api
        
        # 开始测试并记录开始时间
        start_time = time.time()
        
        # 根据parallel参数决定使用并行或串行测试
        if parallel:
            self.logger.info(f"开始并行测试 {len(available_wans)} 个WAN口...")
            
            results = []
            futures = []
            
            # 使用线程池并行执行所有WAN口测试
            with ThreadPoolExecutor(max_workers=len(available_wans)) as executor:
                # 提交所有测试任务
                for wan_idx in available_wans:
                    # 如果是URL测试，传入timeout参数
                    if test_url:
                        future = executor.submit(test_method, url_to_test, wan_idx, timeout=5)
                    else:
                        future = executor.submit(test_method, wan_idx)
                    futures.append((future, wan_idx))
                
                # 收集结果
                for future, wan_idx in futures:
                    try:
                        # 增加超时时间到30秒，避免超时
                        result = future.result(timeout=30)  # 30秒超时
                        results.append(result)
                        if result.get('success'):
                            self.logger.info(f"WAN {wan_idx} 并行测试完成: 成功")
                        else:
                            self.logger.error(f"WAN {wan_idx} 并行测试完成: 失败 - {result.get('error', '未知错误')}")
                    except TimeoutError:
                        self.logger.error(f"WAN {wan_idx} 测试超时，未能在规定时间内完成")
                        results.append({
                            'success': False,
                            'error': "测试超时",
                            'wan_idx': wan_idx
                        })
                    except Exception as e:
                        self.logger.exception(f"WAN {wan_idx} 测试过程中发生异常: {e}")
                        results.append({
                            'success': False,
                            'error': str(e),
                            'wan_idx': wan_idx
                        })
        else:
            # 串行测试
            self.logger.info(f"开始串行测试 {len(available_wans)} 个WAN口...")
            results = []
            
            # 添加超时控制，避免整体测试时间过长
            total_timeout = min(30, 10 * len(available_wans))  # 最多等待30秒
            
            for wan_idx in available_wans:
                # 检查是否已超时
                if time.time() - start_time > total_timeout:
                    self.logger.warning(f"WAN测试已运行 {total_timeout} 秒，超过最大超时时间，停止后续测试")
                    break
                
                # 执行测试
                if test_url:
                    # 测试URL，使用request_via_wan方法
                    result = test_method(url_to_test, wan_idx, timeout=5)
                else:
                    # 测试Tushare API
                    result = test_method(wan_idx)
                    
                results.append(result)
                time.sleep(1)  # 防止请求太频繁
        
        # 计算测试总耗时
        test_elapsed = time.time() - start_time
        
        # 统计成功率
        success_count = sum(1 for r in results if r.get('success', False))
        self.logger.info(f"WAN测试完成: {success_count}/{len(results)} 成功，总耗时: {test_elapsed:.3f}秒")
        
        # 计算性能统计
        if success_count > 0:
            # 计算平均请求耗时
            avg_time = sum(r.get('elapsed', 0) for r in results if r.get('success', False) and 'elapsed' in r) / success_count
            self.logger.info(f"成功请求的平均耗时: {avg_time:.3f}秒")
            
            # 如果是并行测试，找出最快和最慢的WAN口
            if parallel:
                success_results = [r for r in results if r.get('success', False) and 'elapsed' in r]
                if success_results:
                    fastest = min(success_results, key=lambda x: x.get('elapsed', float('inf')))
                    slowest = max(success_results, key=lambda x: x.get('elapsed', 0))
                    
                    self.logger.info(f"最快的WAN口: WAN {fastest.get('wan_idx')}, 耗时: {fastest.get('elapsed', 0):.3f}秒")
                    self.logger.info(f"最慢的WAN口: WAN {slowest.get('wan_idx')}, 耗时: {slowest.get('elapsed', 0):.3f}秒")
        
        # 检查IP是否不同
        unique_ips = set()
        for result in results:
            if result.get('success') and 'external_ip' in result and result['external_ip']:
                unique_ips.add(result['external_ip'])
        
        multi_wan_valid = len(unique_ips) == success_count and success_count > 1
        self.logger.info(f"多WAN验证: {'通过' if multi_wan_valid else '失败'}, 不同IP数: {len(unique_ips)}")
        
        if unique_ips:
            self.logger.info(f"检测到的外部IP: {', '.join(unique_ips)}")
            
        return results
    
    def parallel_tushare_requests(self, api_params: List[Dict], token: str = None, 
                                  api_url: str = None, max_workers: int = None) -> List[Dict]:
        """
        并行发送多个Tushare API请求，通过不同WAN接口
        
        Args:
            api_params: Tushare API参数列表
            token: Tushare API Token，如果为None则从配置中读取
            api_url: Tushare API URL，如果为None则从配置中读取
            max_workers: 最大并行工作线程数，默认为可用WAN口数量
            
        Returns:
            List[Dict]: 响应结果列表
        """
        # 使用请求ID避免重复处理
        request_id = f"parallel_req_{time.time()}"
        
        # 如果已经在处理中，直接返回空列表
        if hasattr(self, '_processing_requests') and request_id in getattr(self, '_processing_requests', set()):
            self.logger.warning(f"请求 {request_id} 已在处理中，跳过重复执行")
            return []
        
        # 初始化处理中请求集合（如果不存在）
        if not hasattr(self, '_processing_requests'):
            self._processing_requests = set()
        
        # 添加到处理中请求集合
        self._processing_requests.add(request_id)
        
        try:
            if not api_params:
                return []
                
            # 获取Tushare配置
            if not token or not api_url:
                all_config = config_manager.get_all_config()
                tushare_config = all_config.get('tushare', {})
                
                if not token:
                    token = tushare_config.get('token', 'b5bb9d57e35cf485f0366eb6581017fb69cefff888da312b0128f3a0')
                    
                if not api_url:
                    api_url = tushare_config.get('api_url', 'http://116.128.206.39:7172')
                    
            self.logger.info(f"并行请求Tushare API: {api_url}, API数量: {len(api_params)}")
            
            # 获取可用WAN接口
            active_wans = []
            for interface in self.active_interfaces:
                wan_idx = interface.get('wan_idx')
                if wan_idx is not None:
                    active_wans.append(wan_idx)
                    
            if not active_wans:
                self.logger.warning("没有可用的WAN接口，无法进行并行请求")
                return []
                
            # 最大并行数不能超过可用WAN口数量
            if max_workers is None or max_workers > len(active_wans):
                max_workers = len(active_wans)
                
            self.logger.info(f"可用WAN口数量: {len(active_wans)}, 设置并行线程数: {max_workers}")
            
            # 初始化请求锁（如果不存在）
            if not hasattr(self, 'request_locks') or not self.request_locks:
                self.request_locks = {wan_idx: threading.Lock() for wan_idx in active_wans}
            else:
                # 确保所有WAN都有对应的锁
                for wan_idx in active_wans:
                    if wan_idx not in self.request_locks:
                        self.request_locks[wan_idx] = threading.Lock()
            
            # 将请求分批处理，每批次的请求数量等于可用WAN口数量
            all_results = []
            
            # 跟踪总请求时间
            total_start_time = time.time()
            
            # WAN口使用状态追踪，防止同一批次重复使用同一个WAN口
            wan_usage = {wan_idx: False for wan_idx in active_wans}
            
            # 分批处理所有请求
            for batch_start in range(0, len(api_params), max_workers):
                batch_end = min(batch_start + max_workers, len(api_params))
                batch_params = api_params[batch_start:batch_end]
                
                self.logger.info(f"处理批次 {batch_start//max_workers+1}, 请求数: {len(batch_params)}")
                
                # 构建请求配置并分配WAN口
                requests_list = []
                used_wans = set()  # 记录本批次已使用的WAN口
                
                for i, api_config in enumerate(batch_params):
                    # 如果API配置中已指定WAN口，则使用指定的
                    if 'wan_idx' in api_config and api_config['wan_idx'] in active_wans:
                        wan_idx = api_config['wan_idx']
                    else:
                        # 选择未使用的WAN口
                        available_wans = [w for w in active_wans if w not in used_wans]
                        if not available_wans:  # 如果所有WAN都已使用，则重置
                            self.logger.warning("所有WAN口已分配，部分WAN口将处理多个请求")
                            used_wans = set()
                            available_wans = active_wans
                        
                        # 选择一个可用WAN口
                        wan_idx = available_wans[i % len(available_wans)]
                    
                    # 标记该WAN口已使用
                    used_wans.add(wan_idx)
                    
                    # 构建请求数据
                    api_name = api_config.get('api_name', '')
                    params = api_config.get('params', {})
                    fields = api_config.get('fields', '')
                    
                    post_data = {
                        'api_name': api_name,
                        'token': token,
                        'params': params,
                        'fields': fields
                    }
                    
                    requests_list.append({
                        'method': 'post',
                        'url': api_url,
                        'kwargs': {
                            'json': post_data,
                            'headers': {'Content-Type': 'application/json'},
                            'timeout': 15  # 设置较短的超时时间，最长15秒
                        },
                        'wan_idx': wan_idx,
                        # 添加额外信息供结果处理使用
                        'meta': {
                            'api_name': api_name,
                            'params': params,
                            'batch_idx': batch_start//max_workers+1,
                            'request_idx': batch_start+i
                        }
                    })
                    
                self.logger.info(f"开始执行批次 {batch_start//max_workers+1}, 共 {len(requests_list)} 个并行请求")
                
                # 自定义执行函数，确保使用线程锁
                def execute_with_lock(req_idx):
                    req_config = requests_list[req_idx]
                    wan_idx = req_config.get('wan_idx')
                    method = req_config.get('method')
                    url = req_config.get('url')
                    kwargs = req_config.get('kwargs', {}).copy()  # 复制一份避免修改原始数据
                    meta = req_config.get('meta', {})
                    api_name = meta.get('api_name', '')
                    
                    # 生成请求唯一标识
                    request_tag = f"{wan_idx}_{api_name}_{meta.get('request_idx')}"
                    
                    # 使用线程锁确保同一个WAN口不会同时处理多个请求
                    if wan_idx in self.request_locks:
                        lock = self.request_locks[wan_idx]
                    else:
                        # 如果锁不存在，创建一个新锁
                        lock = threading.Lock()
                        self.request_locks[wan_idx] = lock
                    
                    with lock:
                        start_req = time.time()
                        self.logger.debug(f"[批次{meta.get('batch_idx')}] 开始执行请求: WAN{wan_idx}, API: {api_name}, ID: {request_tag}")
                        
                        try:
                            # 分配本地端口
                            local_port = get_port_allocator().allocate_port(wan_idx)
                            if not local_port:
                                self.logger.error(f"无法为WAN {wan_idx} 分配端口，请求失败")
                                return {
                                    'status': 'error',
                                    'response': None,
                                    'error': f"无法分配端口",
                                    'request': req_config,
                                    'elapsed': time.time() - start_req,
                                    'request_tag': request_tag
                                }
                            
                            try:
                                # 设置环境变量
                                os.environ['LOCAL_BIND_PORT'] = str(local_port)
                                
                                # 发送请求，确保传递wan_idx
                                kwargs['wan_idx'] = wan_idx
                                response = self.request(method, url, **kwargs)
                                elapsed = time.time() - start_req
                                
                                if response and response.status_code == 200:
                                    return {
                                        'status': 'success',
                                        'response': response,
                                        'error': None,
                                        'request': req_config,
                                        'elapsed': elapsed,
                                        'request_tag': request_tag
                                    }
                                else:
                                    status_code = response.status_code if response else 'N/A'
                                    return {
                                        'status': 'error',
                                        'response': response,
                                        'error': f"HTTP错误: {status_code}",
                                        'request': req_config,
                                        'elapsed': elapsed,
                                        'request_tag': request_tag
                                    }
                            finally:
                                # 清除环境变量
                                if 'LOCAL_BIND_PORT' in os.environ:
                                    del os.environ['LOCAL_BIND_PORT']
                                    
                                # 释放端口
                                get_port_allocator().release_port(wan_idx, local_port)
                        except Exception as e:
                            elapsed = time.time() - start_req
                            self.logger.error(f"WAN {wan_idx} 请求 {api_name} 异常: {str(e)}")
                            return {
                                'status': 'error',
                                'response': None,
                                'error': str(e),
                                'request': req_config,
                                'elapsed': elapsed,
                                'request_tag': request_tag
                            }
                
                # 设置一个等待超时，确保批次请求不会无限等待
                batch_timeout = 30  # 最多等待30秒
                batch_start_time = time.time()
                
                # 执行批次请求
                batch_results = []
                try:
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        # 并行执行所有请求
                        futures = [executor.submit(execute_with_lock, i) for i in range(len(requests_list))]
                        
                        # 等待所有请求完成，设置超时
                        for future in futures:
                            try:
                                # 最多等待15秒，避免请求卡住
                                batch_results.append(future.result(timeout=15))
                            except Exception as e:
                                self.logger.error(f"请求执行异常或超时: {str(e)}")
                                batch_results.append({
                                    'status': 'error',
                                    'response': None,
                                    'error': f"执行异常: {str(e)}",
                                    'request': {'error': 'unknown'},
                                    'request_tag': 'unknown'
                                })
                except Exception as e:
                    self.logger.error(f"批次执行异常: {str(e)}")
                
                batch_elapsed = time.time() - batch_start_time
                
                # 处理批次结果
                processed_results = []
                seen_request_tags = set()  # 用于检测和过滤重复请求
                
                for result in batch_results:
                    request_tag = result.get('request_tag', 'unknown')
                    
                    # 跳过重复请求
                    if request_tag in seen_request_tags:
                        self.logger.warning(f"跳过重复请求: {request_tag}")
                        continue
                    
                    seen_request_tags.add(request_tag)
                    
                    request_config = result['request']
                    meta = request_config.get('meta', {})
                    api_name = meta.get('api_name', '')
                    wan_idx = request_config.get('wan_idx')
                    
                    if result['status'] == 'success' and result['response'] and result['response'].status_code == 200:
                        try:
                            resp_data = result['response'].json()
                            elapsed = result.get('elapsed', 0)
                            
                            if 'data' in resp_data and resp_data.get('code') == 0:
                                # 获取数据行数
                                data_items = resp_data['data'].get('items', [])
                                data_rows = len(data_items)
                                
                                processed_result = {
                                    'success': True,
                                    'wan_idx': wan_idx,
                                    'api_name': api_name,
                                    'data_rows': data_rows,
                                    'elapsed': elapsed,
                                    'data': resp_data['data'],
                                    'meta': meta
                                }
                                
                                self.logger.info(f"WAN {wan_idx} 请求 {api_name} 成功，获取到 {data_rows} 行数据")
                            else:
                                error_msg = resp_data.get('msg', '未知错误')
                                
                                processed_result = {
                                    'success': False,
                                    'wan_idx': wan_idx,
                                    'api_name': api_name,
                                    'error': f"API错误: {error_msg}",
                                    'elapsed': elapsed,
                                    'meta': meta
                                }
                                
                                self.logger.error(f"WAN {wan_idx} 请求 {api_name} 返回错误: {error_msg}")
                        except Exception as e:
                            processed_result = {
                                'success': False,
                                'wan_idx': wan_idx,
                                'api_name': api_name,
                                'error': f"解析响应失败: {str(e)}",
                                'meta': meta
                            }
                            
                            self.logger.error(f"WAN {wan_idx} 请求 {api_name} 解析响应失败: {str(e)}")
                    else:
                        error = result.get('error', '未知错误')
                        status_code = result['response'].status_code if result.get('response') else 'N/A'
                        
                        processed_result = {
                            'success': False,
                            'wan_idx': wan_idx,
                            'api_name': api_name,
                            'error': f"请求失败: {error}",
                            'meta': meta
                        }
                        
                        self.logger.error(f"WAN {wan_idx} 请求 {api_name} 失败: {error}")
                    
                    # 保存处理结果
                    processed_results.append(processed_result)
                    
                # 统计批次成功率
                batch_success_count = sum(1 for r in processed_results if r.get('success', False))
                self.logger.info(f"批次 {batch_start//max_workers+1} 完成: {batch_success_count}/{len(processed_results)} 成功，耗时: {batch_elapsed:.3f}秒")
                
                # 将处理结果添加到总结果中
                all_results.extend(processed_results)
                
                # 批次间等待一小段时间，避免过快发送请求
                if batch_end < len(api_params):
                    time.sleep(1)
                    
            # 统计总成功率
            total_elapsed = time.time() - total_start_time
            total_success_count = sum(1 for r in all_results if r.get('success', False))
            self.logger.info(f"全部 {len(api_params)} 个请求处理完成: {total_success_count}/{len(all_results)} 成功, 总耗时: {total_elapsed:.3f}秒")
            
            return all_results
        finally:
            # 从处理中请求集合移除
            if hasattr(self, '_processing_requests'):
                self._processing_requests.discard(request_id)


# 创建全局网络管理器实例函数
def get_network_manager():
    """获取网络管理器单例实例，如果不存在则创建"""
    # 第一次导入模块时懒加载网络管理器
    global _network_manager_instance
    if _network_manager_instance is None:
        try:
            _network_manager_instance = NetworkManager()
        except Exception as e:
            logging.error(f"创建网络管理器失败: {str(e)}")
            # 返回一个空壳实例，避免空引用错误
            _network_manager_instance = EmptyNetworkManager()
    return _network_manager_instance

# 空壳网络管理器类，用于在创建实际网络管理器失败时返回
class EmptyNetworkManager:
    """网络管理器空壳，用于在创建实际网络管理器失败时返回"""
    def __getattr__(self, name):
        # 返回一个空函数，避免调用非存在方法时出错
        def empty_func(*args, **kwargs):
            return None
        return empty_func

# 全局实例变量
_network_manager_instance = None

# 向后兼容，在其他模块中可以通过network_manager直接访问实例
# 但在导入时不会创建实例，只有在首次访问时才创建
network_manager = get_network_manager()