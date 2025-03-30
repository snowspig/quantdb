"""
底层API请求处理模块,提供多WAN支持和负载均衡功能
"""
import random
import socket
import time
from typing import Dict, Any, Optional, Tuple
import requests
from loguru import logger
from config import config_manager
from wan_manager.port_allocator import PortAllocator


class APIRequest:
    """
    API请求处理类,支持多WAN负载均衡
    """
    
    def __init__(self):
        """
        初始化API请求处理器
        """
        self.timeout = config_manager.get('tushare', 'timeout', 60)
        self.wan_enabled = config_manager.is_wan_enabled()
        
        if self.wan_enabled:
            try:
                self.port_allocator = PortAllocator()
                logger.info("多WAN支持已启用")
            except Exception as e:
                logger.error(f"初始化端口分配器失败: {str(e)}")
                self.wan_enabled = False
        else:
            logger.info("多WAN支持未启用")
    
    def _bind_socket(self, sock: socket.socket, wan_index: int) -> bool:
        """
        将套接字绑定到特定WAN端口
        
        Args:
            sock: 要绑定的套接字
            wan_index: WAN接口索引
            
        Returns:
            是否绑定成功
        """
        if not self.wan_enabled:
            return False
            
        try:
            port = self.port_allocator.allocate_port(wan_index)
            if port:
                sock.bind(('0.0.0.0', port))
                logger.debug(f"套接字绑定到WAN {wan_index}, 端口 {port}")
                return True
            else:
                logger.warning(f"无法为WAN {wan_index} 分配端口")
                return False
        except Exception as e:
            logger.error(f"绑定套接字到WAN {wan_index} 失败: {str(e)}")
            return False
    
    def _create_socket_session(self, wan_index: Optional[int] = None) -> Tuple[requests.Session, Optional[int]]:
        """
        创建带有自定义套接字的会话
        
        Args:
            wan_index: WAN接口索引,如果为None则随机选择
            
        Returns:
            (requests会话, 使用的WAN索引)
        """
        session = requests.Session()
        
        if not self.wan_enabled:
            return session, None
        
        # 如果未指定WAN索引,随机选择
        if wan_index is None:
            wan_ranges = config_manager.get('wan', 'port_ranges', {})
            if wan_ranges:
                wan_index = random.choice(list(wan_ranges.keys()))
            else:
                return session, None
        
        # 创建自定义适配器
        class BoundHTTPAdapter(requests.adapters.HTTPAdapter):
            def __init__(self, wan_index, api_request, **kwargs):
                self.wan_index = wan_index
                self.api_request = api_request
                super().__init__(**kwargs)
                
            def get_connection(self, url, proxies=None):
                conn = super().get_connection(url, proxies)
                if hasattr(conn, 'sock') and conn.sock:
                    self.api_request._bind_socket(conn.sock, self.wan_index)
                return conn
        
        # 设置适配器
        adapter = BoundHTTPAdapter(wan_index, self, pool_connections=10, pool_maxsize=100)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        return session, wan_index
    
    def post(
        self, 
        url: str, 
        data: Dict[str, Any] = None,
        json: Dict[str, Any] = None,
        wan_index: Optional[int] = None,
        **kwargs
    ) -> requests.Response:
        """
        发送POST请求,支持多WAN
        
        Args:
            url: 请求URL
            data: 表单数据
            json: JSON数据
            wan_index: 指定WAN索引,如果为None则随机选择
            **kwargs: 其他请求参数
            
        Returns:
            请求响应
        """
        session, used_wan = self._create_socket_session(wan_index)
        
        try:
            start_time = time.time()
            response = session.post(
                url, 
                data=data, 
                json=json, 
                timeout=kwargs.get('timeout', self.timeout),
                **{k: v for k, v in kwargs.items() if k != 'timeout'}
            )
            elapsed = time.time() - start_time
            
            if used_wan is not None:
                logger.debug(f"通过WAN {used_wan} 请求 {url} 完成,耗时: {elapsed:.2f}秒,状态: {response.status_code}")
            else:
                logger.debug(f"请求 {url} 完成,耗时: {elapsed:.2f}秒,状态: {response.status_code}")
                
            return response
            
        except Exception as e:
            if used_wan is not None:
                logger.error(f"通过WAN {used_wan} 请求 {url} 失败: {str(e)}")
            else:
                logger.error(f"请求 {url} 失败: {str(e)}")
            raise
        finally:
            session.close()
    
    def get(
        self, 
        url: str, 
        params: Dict[str, Any] = None,
        wan_index: Optional[int] = None,
        **kwargs
    ) -> requests.Response:
        """
        发送GET请求,支持多WAN
        
        Args:
            url: 请求URL
            params: 查询参数
            wan_index: 指定WAN索引,如果为None则随机选择
            **kwargs: 其他请求参数
            
        Returns:
            请求响应
        """
        session, used_wan = self._create_socket_session(wan_index)
        
        try:
            start_time = time.time()
            response = session.get(
                url, 
                params=params, 
                timeout=kwargs.get('timeout', self.timeout),
                **{k: v for k, v in kwargs.items() if k != 'timeout'}
            )
            elapsed = time.time() - start_time
            
            if used_wan is not None:
                logger.debug(f"通过WAN {used_wan} 请求 {url} 完成,耗时: {elapsed:.2f}秒,状态: {response.status_code}")
            else:
                logger.debug(f"请求 {url} 完成,耗时: {elapsed:.2f}秒,状态: {response.status_code}")
                
            return response
            
        except Exception as e:
            if used_wan is not None:
                logger.error(f"通过WAN {used_wan} 请求 {url} 失败: {str(e)}")
            else:
                logger.error(f"请求 {url} 失败: {str(e)}")
            raise
        finally:
            session.close()


# 创建全局API请求实例
api_request = APIRequest() 