"""
网络管理器模块，集成WAN多网络管理、监控和负载均衡功能
"""
import os
import sys
import time
import socket
import logging
import threading
import requests
from typing import Dict, List, Any, Optional, Tuple, Set, Union

# 导入配置管理器
from config import config_manager

# 导入WAN管理组件
from wan_manager.wan_port_allocator import wan_port_allocator
from wan_manager.wan_monitor import wan_monitor
from wan_manager.load_balancer import load_balancer


class NetworkManager:
    """
    网络管理器类，提供统一的网络管理接口，集成WAN多网络管理、监控和负载均衡功能
    """
    
    def __init__(self, config_path=None, silent=False):
        """
        初始化网络管理器
        
        Args:
            config_path: 配置文件路径，不提供则使用默认配置
            silent: 静默模式，不输出日志
        """
        self.config_path = config_path
        self.silent = silent
        self.lock = threading.RLock()
        
        # 从配置中加载设置
        self.network_config = config_manager.get('network', {})
        self.wan_enabled = config_manager.is_wan_enabled()
        
        # 网络配置
        self.timeout = self.network_config.get('timeout', 60)
        self.retry_count = self.network_config.get('retry_count', 3)
        self.max_connections = self.network_config.get('max_connections', 10)
        
        # 代理配置
        self.proxy_config = self.network_config.get('proxy', {})
        self.proxy_enabled = self.proxy_config.get('enabled', False)
        
        # 会话池
        self.sessions = {}
        
        # 初始化
        if not self.silent:
            if self.wan_enabled:
                logging.info(f"网络管理器初始化完成，多WAN模式已启用")
            else:
                logging.info(f"网络管理器初始化完成，标准网络模式")
    
    def create_session(self, session_id: str = None) -> str:
        """
        创建一个新的会话
        
        Args:
            session_id: 会话ID，不提供则自动生成
            
        Returns:
            str: 会话ID
        """
        with self.lock:
            # 生成会话ID
            if session_id is None:
                session_id = f"session_{int(time.time())}_{len(self.sessions)}"
            
            # 创建会话
            session = requests.Session()
            
            # 设置超时
            session.timeout = self.timeout
            
            # 设置代理（如果启用）
            if self.proxy_enabled:
                proxy_host = self.proxy_config.get('host')
                proxy_port = self.proxy_config.get('port')
                proxy_username = self.proxy_config.get('username')
                proxy_password = self.proxy_config.get('password')
                
                if proxy_host and proxy_port:
                    proxy_url = f"http://"
                    if proxy_username and proxy_password:
                        proxy_url += f"{proxy_username}:{proxy_password}@"
                    proxy_url += f"{proxy_host}:{proxy_port}"
                    
                    session.proxies = {
                        'http': proxy_url,
                        'https': proxy_url
                    }
            
            # 存储会话
            self.sessions[session_id] = {
                'session': session,
                'created_at': time.time(),
                'last_used': time.time(),
                'request_count': 0,
                'wan_idx': None
            }
            
            if not self.silent:
                logging.debug(f"创建会话: {session_id}")
                
            return session_id
    
    def get_session(self, session_id: str) -> Optional[requests.Session]:
        """
        获取指定的会话
        
        Args:
            session_id: 会话ID
            
        Returns:
            requests.Session: 会话对象，不存在则返回None
        """
        with self.lock:
            if session_id in self.sessions:
                session_info = self.sessions[session_id]
                session_info['last_used'] = time.time()
                return session_info['session']
            return None
    
    def close_session(self, session_id: str) -> bool:
        """
        关闭指定的会话
        
        Args:
            session_id: 会话ID
            
        Returns:
            bool: 是否成功关闭
        """
        with self.lock:
            if session_id in self.sessions:
                session_info = self.sessions[session_id]
                session_info['session'].close()
                del self.sessions[session_id]
                
                if not self.silent:
                    logging.debug(f"关闭会话: {session_id}")
                    
                return True
            return False
    
    def cleanup_sessions(self, max_age: int = 3600):
        """
        清理过期会话
        
        Args:
            max_age: 最大会话年龄（秒）
        """
        current_time = time.time()
        
        with self.lock:
            session_ids = list(self.sessions.keys())
            for session_id in session_ids:
                session_info = self.sessions[session_id]
                session_age = current_time - session_info['last_used']
                
                if session_age > max_age:
                    self.close_session(session_id)
    
    def allocate_wan(self) -> Optional[int]:
        """
        分配一个WAN接口
        
        Returns:
            int: WAN接口索引，分配失败则返回None
        """
        if not self.wan_enabled:
            return None
            
        return load_balancer.select_wan()
    
    def allocate_wan_port(self, wan_idx: Optional[int] = None) -> Tuple[Optional[int], Optional[int]]:
        """
        分配WAN端口
        
        Args:
            wan_idx: WAN接口索引，不提供则自动选择
            
        Returns:
            Tuple[Optional[int], Optional[int]]: (WAN接口索引, 端口号)，分配失败则返回(None, None)
        """
        if not self.wan_enabled:
            return (None, None)
            
        return wan_port_allocator.allocate_port(wan_idx)
    
    def release_wan_port(self, port: int) -> bool:
        """
        释放WAN端口
        
        Args:
            port: 端口号
            
        Returns:
            bool: 是否成功释放
        """
        if not self.wan_enabled:
            return True
            
        return wan_port_allocator.release_port(port)
    
    def bind_socket(self, socket_obj: socket.socket, wan_idx: Optional[int] = None) -> Tuple[bool, Optional[int], Optional[int]]:
        """
        将套接字绑定到指定或自动分配的WAN接口
        
        Args:
            socket_obj: 套接字对象
            wan_idx: WAN接口索引，不提供则自动选择
            
        Returns:
            Tuple[bool, Optional[int], Optional[int]]: (是否成功, WAN接口索引, 端口号)
        """
        if not self.wan_enabled:
            return (False, None, None)
        
        # 分配WAN端口
        wan_idx, port = self.allocate_wan_port(wan_idx)
        if wan_idx is None or port is None:
            return (False, None, None)
        
        # 绑定套接字
        success = wan_port_allocator.bind_socket_to_wan(socket_obj, wan_idx, port)
        
        if not success:
            # 绑定失败，释放端口
            self.release_wan_port(port)
            return (False, None, None)
            
        return (True, wan_idx, port)
    
    def report_wan_usage(self, wan_idx: int, success: bool, response_time: float,
                      bytes_sent: int = 0, bytes_received: int = 0):
        """
        报告WAN使用情况
        
        Args:
            wan_idx: WAN接口索引
            success: 是否成功
            response_time: 响应时间（秒）
            bytes_sent: 发送的字节数
            bytes_received: 接收的字节数
        """
        if not self.wan_enabled or wan_idx is None:
            return
            
        load_balancer.report_wan_usage(wan_idx, success, response_time, bytes_sent, bytes_received)
    
    def get_wan_status(self, wan_idx: Optional[int] = None) -> Dict[str, Any]:
        """
        获取WAN状态
        
        Args:
            wan_idx: WAN接口索引，不提供则返回所有接口状态
            
        Returns:
            Dict: WAN状态信息
        """
        if not self.wan_enabled:
            return {'enabled': False}
            
        return load_balancer.get_wan_status(wan_idx)
    
    def test_all_wans(self) -> Dict[int, Dict[str, Any]]:
        """
        测试所有WAN接口连通性
        
        Returns:
            Dict[int, Dict[str, Any]]: 测试结果 {wan_idx: wan_info, ...}
        """
        if not self.wan_enabled:
            return {}
            
        # 强制执行一次状态检查
        wan_monitor.force_check()
        
        # 等待检查完成
        time.sleep(1)
        
        # 获取所有WAN接口信息
        return wan_monitor.get_all_wan_info()
    
    def make_request(self, url: str, method: str = 'GET', headers: Dict = None, data: Any = None,
                  json: Dict = None, timeout: int = None, session_id: str = None,
                  wan_idx: Optional[int] = None, retry: int = None, **kwargs) -> requests.Response:
        """
        发送HTTP请求，支持多WAN负载均衡
        
        Args:
            url: 请求URL
            method: 请求方法
            headers: 请求头
            data: 请求数据
            json: JSON数据
            timeout: 超时时间（秒）
            session_id: 会话ID，不提供则创建新会话
            wan_idx: WAN接口索引，不提供则自动选择
            retry: 重试次数，不提供则使用默认值
            **kwargs: 其他请求参数
            
        Returns:
            requests.Response: 响应对象
            
        Raises:
            requests.RequestException: 请求失败
        """
        # 设置超时
        if timeout is None:
            timeout = self.timeout
            
        # 设置重试次数
        if retry is None:
            retry = self.retry_count
            
        # 获取或创建会话
        if session_id is None:
            session_id = self.create_session()
        session = self.get_session(session_id)
        if session is None:
            session_id = self.create_session()
            session = self.get_session(session_id)
        
        # 记录会话WAN索引
        session_info = self.sessions.get(session_id, {})
        
        # 如果未指定WAN接口，则使用会话已分配的接口或分配新接口
        if self.wan_enabled:
            if wan_idx is None:
                wan_idx = session_info.get('wan_idx')
                if wan_idx is None:
                    wan_idx = self.allocate_wan()
            
            # 更新会话WAN索引
            if session_id in self.sessions and wan_idx is not None:
                self.sessions[session_id]['wan_idx'] = wan_idx
        
        # 准备请求
        request_kwargs = {
            'url': url,
            'headers': headers,
            'data': data,
            'json': json,
            'timeout': timeout,
            **kwargs
        }
        
        # 记录请求开始时间
        start_time = time.time()
        response = None
        success = False
        bytes_sent = len(str(data)) if data else 0
        bytes_received = 0
        
        try:
            # 尝试发送请求，最多重试指定次数
            for attempt in range(retry + 1):
                try:
                    if method.upper() == 'GET':
                        response = session.get(**request_kwargs)
                    elif method.upper() == 'POST':
                        response = session.post(**request_kwargs)
                    elif method.upper() == 'PUT':
                        response = session.put(**request_kwargs)
                    elif method.upper() == 'DELETE':
                        response = session.delete(**request_kwargs)
                    elif method.upper() == 'HEAD':
                        response = session.head(**request_kwargs)
                    elif method.upper() == 'OPTIONS':
                        response = session.options(**request_kwargs)
                    elif method.upper() == 'PATCH':
                        response = session.patch(**request_kwargs)
                    else:
                        raise ValueError(f"不支持的HTTP方法: {method}")
                    
                    # 请求成功
                    success = response.status_code < 400
                    bytes_received = len(response.content)
                    
                    # 增加会话请求计数
                    if session_id in self.sessions:
                        self.sessions[session_id]['request_count'] += 1
                    
                    # 如果请求成功，跳出重试循环
                    if success:
                        break
                        
                    # 请求失败但状态码非5xx，不再重试
                    if response.status_code < 500:
                        break
                        
                    # 重试前等待
                    if attempt < retry:
                        time.sleep(2 ** attempt)  # 指数退避
                        
                except (requests.RequestException, socket.error) as e:
                    # 网络错误，尝试重试
                    if attempt < retry:
                        time.sleep(2 ** attempt)  # 指数退避
                    else:
                        raise e
        
        finally:
            # 计算响应时间
            response_time = time.time() - start_time
            
            # 报告WAN使用情况
            if self.wan_enabled and wan_idx is not None:
                self.report_wan_usage(wan_idx, success, response_time, bytes_sent, bytes_received)
            
            if not self.silent:
                log_level = logging.INFO if success else logging.WARNING
                logging.log(log_level, f"{method} {url} - {response.status_code if response else 'Failed'} "
                          f"({response_time:.2f}s) {f'via WAN {wan_idx}' if self.wan_enabled and wan_idx is not None else ''}")
        
        return response
    
    def get(self, url: str, **kwargs) -> requests.Response:
        """
        发送GET请求
        
        Args:
            url: 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            requests.Response: 响应对象
        """
        return self.make_request(url, method='GET', **kwargs)
    
    def post(self, url: str, **kwargs) -> requests.Response:
        """
        发送POST请求
        
        Args:
            url: 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            requests.Response: 响应对象
        """
        return self.make_request(url, method='POST', **kwargs)
    
    def put(self, url: str, **kwargs) -> requests.Response:
        """
        发送PUT请求
        
        Args:
            url: 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            requests.Response: 响应对象
        """
        return self.make_request(url, method='PUT', **kwargs)
    
    def delete(self, url: str, **kwargs) -> requests.Response:
        """
        发送DELETE请求
        
        Args:
            url: 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            requests.Response: 响应对象
        """
        return self.make_request(url, method='DELETE', **kwargs)
    
    def download(self, url: str, save_path: str, wan_idx: Optional[int] = None, chunk_size: int = 8192, **kwargs) -> bool:
        """
        下载文件
        
        Args:
            url: 下载URL
            save_path: 保存路径
            wan_idx: WAN接口索引，不提供则自动选择
            chunk_size: 分块大小
            **kwargs: 其他请求参数
            
        Returns:
            bool: 是否成功下载
        """
        try:
            # 创建目录
            os.makedirs(os.path.dirname(os.path.abspath(save_path)), exist_ok=True)
            
            # 发送请求
            response = self.make_request(url, method='GET', wan_idx=wan_idx, stream=True, **kwargs)
            response.raise_for_status()
            
            # 下载文件
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
            
            return True
        except Exception as e:
            if not self.silent:
                logging.error(f"下载文件失败: {url} -> {save_path}, 错误: {str(e)}")
            return False
    
    def upload(self, url: str, file_path: str, field_name: str = 'file', wan_idx: Optional[int] = None, **kwargs) -> Optional[requests.Response]:
        """
        上传文件
        
        Args:
            url: 上传URL
            file_path: 文件路径
            field_name: 文件字段名
            wan_idx: WAN接口索引，不提供则自动选择
            **kwargs: 其他请求参数
            
        Returns:
            requests.Response: 响应对象，上传失败则返回None
        """
        try:
            with open(file_path, 'rb') as f:
                files = {field_name: (os.path.basename(file_path), f, 'application/octet-stream')}
                response = self.make_request(url, method='POST', wan_idx=wan_idx, files=files, **kwargs)
                response.raise_for_status()
                return response
        except Exception as e:
            if not self.silent:
                logging.error(f"上传文件失败: {file_path} -> {url}, 错误: {str(e)}")
            return None
    
    def cleanup(self):
        """
        清理资源
        """
        # 关闭所有会话
        with self.lock:
            for session_id in list(self.sessions.keys()):
                self.close_session(session_id)
        
        if not self.silent:
            logging.info("网络管理器资源已清理")


# 创建全局网络管理器实例
network_manager = NetworkManager()