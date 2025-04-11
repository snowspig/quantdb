#!/usr/bin/env python
"""
端口分配器适配器 - 提供与旧API兼容的接口

该模块提供了一个适配器类，使旧的PortAllocator API能够使用新的WanPortPool实现，
确保现有代码可以无缝迁移到新的端口池实现。
"""
import logging
from typing import Dict, List, Tuple, Optional, Set
from .wan_port_pool import WanPortPool
import random
import socket
import threading
import time
import os

class PortAllocatorAdapter:
    """
    端口分配器适配器类，提供与旧PortAllocator兼容的API
    该适配器将调用转发到WanPortPool单例实现
    """
    
    def __init__(self, silent=False):
        """
        初始化端口分配器适配器
        
        Args:
            silent: 静默模式，不输出日志
        """
        self.logger = logging.getLogger("core.wan_manager.PortAllocatorAdapter")
        self.silent = silent
        
        # 设置默认值，以防初始化失败
        self.port_pool = None
        self.wan_ranges = {0: (50001, 51000)}
        
        try:
            # 获取WanPortPool单例实例
            self.logger.info("正在初始化端口分配器适配器...")
            from .wan_port_pool import WanPortPool
            self.port_pool = WanPortPool.get_instance()
            
            if not self.silent:
                self.logger.info("端口分配器适配器初始化完成，使用WanPortPool实现")
        except Exception as e:
            self.logger.error(f"初始化WanPortPool失败: {str(e)}")
            import traceback
            self.logger.error(f"错误详情: {traceback.format_exc()}")
            self.logger.warning("将使用内部简易实现替代")
            # 创建一个简单的内部实现，不依赖WanPortPool
            self._create_internal_implementation()
    
    def _create_internal_implementation(self):
        """创建内部简易实现，当WanPortPool不可用时使用"""
        import random
        import socket
        import threading
        
        self.logger.info("创建内部简易端口分配实现")
        # 使用简单的端口范围
        self.wan_ranges = {
            0: (50001, 51000),
            1: (51001, 52000),
            2: (52001, 53000),
        }
        self._lock = threading.Lock()
        self._allocated = {idx: set() for idx in self.wan_ranges.keys()}
    
    def get_available_wan_indices(self) -> List[int]:
        """
        获取可用的WAN接口索引
        
        Returns:
            可用WAN接口索引列表
        """
        try:
            if self.port_pool:
                return self.port_pool.get_available_wan_indices()
            else:
                return list(self.wan_ranges.keys())
        except Exception as e:
            self.logger.error(f"获取WAN索引时出错: {str(e)}")
            # 返回默认值
            return [0, 1, 2]
    
    def allocate_port(self, wan_idx: int) -> Optional[int]:
        """
        为指定WAN接口分配一个可用端口
        
        Args:
            wan_idx: WAN接口索引
            
        Returns:
            分配的端口号，如果无法分配则返回None
        """
        try:
            # 尝试使用WanPortPool
            if self.port_pool:
                self.logger.debug(f"使用WanPortPool分配WAN {wan_idx}的端口")
                port = self.port_pool.allocate_port(wan_idx)
                if port:
                    self.logger.debug(f"成功分配端口: {port}")
                    return port
                else:
                    self.logger.warning(f"WanPortPool无法分配端口，尝试内部实现")
            
            # 如果WanPortPool不可用或分配失败，使用内部实现
            if not hasattr(self, '_lock'):
                self._create_internal_implementation()
            
            return self._internal_allocate_port(wan_idx)
        except Exception as e:
            self.logger.error(f"分配端口时出错: {str(e)}")
            # 在出错情况下返回固定端口，确保程序能继续运行
            fallback_port = 50000 + (wan_idx * 1000) + (os.getpid() % 1000)
            self.logger.warning(f"使用应急端口: {fallback_port}")
            return fallback_port
    
    def _internal_allocate_port(self, wan_idx: int) -> Optional[int]:
        """内部端口分配实现"""
        if wan_idx not in self.wan_ranges:
            wan_idx = 0  # 默认使用第一个WAN
            
        with self._lock:
            start_port, end_port = self.wan_ranges[wan_idx]
            allocated = self._allocated[wan_idx]
            
            # 添加超时机制
            start_time = time.time()
            max_try_time = 1.0  # 最多尝试1秒
            
            # 尝试查找可用端口
            attempts = 0
            max_attempts = 10  # 减少最大尝试次数
            
            while attempts < max_attempts and (time.time() - start_time) < max_try_time:
                # 随机选择一个端口
                port = random.randint(start_port, end_port)
                
                # 检查端口是否已分配
                if port in allocated:
                    attempts += 1
                    time.sleep(0.01)
                    continue
                
                # 尝试绑定端口测试可用性
                try:
                    test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    test_socket.settimeout(0.05)
                    test_socket.bind(('0.0.0.0', port))
                    test_socket.close()
                    
                    # 端口可用，标记为已分配
                    allocated.add(port)
                    return port
                except:
                    attempts += 1
            
            # 如果所有尝试都失败，返回一个固定端口
            fallback_port = start_port + (os.getpid() % (end_port - start_port))
            allocated.add(fallback_port)
            return fallback_port
    
    def release_port(self, wan_idx: int, port: int) -> bool:
        """
        释放已分配的端口
        
        Args:
            wan_idx: WAN接口索引
            port: 要释放的端口号
            
        Returns:
            是否成功释放
        """
        try:
            # 尝试使用WanPortPool
            if self.port_pool:
                return self.port_pool.release_port(wan_idx, port)
            
            # 如果WanPortPool不可用，使用内部实现
            if hasattr(self, '_allocated') and wan_idx in self._allocated:
                with self._lock:
                    if port in self._allocated[wan_idx]:
                        self._allocated[wan_idx].remove(port)
                        return True
            return False
        except Exception as e:
            self.logger.error(f"释放端口时出错: {str(e)}")
            return False
    
    def get_port_range(self, wan_idx: int) -> Tuple[int, int]:
        """
        获取指定WAN接口的端口范围
        
        Args:
            wan_idx: WAN接口索引
            
        Returns:
            Tuple[int, int]: 端口范围(开始端口,结束端口)
        """
        try:
            # 尝试使用WanPortPool
            if self.port_pool:
                # 从端口池的端口范围获取信息
                port_info = self.port_pool.get_port_usage_info()
                wan_idx_str = str(wan_idx)
                
                # 从端口池的端口范围获取信息，这是对旧API的兼容
                if wan_idx_str in port_info["wan_port_usage"]:
                    port_usage = port_info["wan_port_usage"][wan_idx_str]
                    # 尝试获取端口范围，如果不可用，返回默认值
                    return self.port_pool.wan_port_ranges.get(wan_idx, (0, 0))
            
            # 如果WanPortPool不可用或获取失败，使用内部实现
            if hasattr(self, 'wan_ranges') and wan_idx in self.wan_ranges:
                return self.wan_ranges[wan_idx]
                
            # 如果没有可用的端口范围信息，返回默认值
            self.logger.warning(f"未找到WAN {wan_idx} 的端口范围配置，使用默认值")
            default_ranges = {
                0: (50001, 51000),
                1: (51001, 52000),
                2: (52001, 53000)
            }
            return default_ranges.get(wan_idx, (0, 0))
        except Exception as e:
            self.logger.error(f"获取端口范围时出错: {str(e)}")
            # 返回默认值
            return (50000 + wan_idx * 1000, 51000 + wan_idx * 1000)

# 创建全局适配器实例，提供与旧API兼容的接口
# 将来导入现有代码中的port_allocator可以使用这个适配器替换
port_allocator_adapter = PortAllocatorAdapter(silent=False) 