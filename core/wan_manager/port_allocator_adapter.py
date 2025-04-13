#!/usr/bin/env python
"""
端口分配器适配器 - 提供与旧API兼容的接口

该模块提供了一个适配器类，使旧的PortAllocator API能够使用新的WanPortPool实现，
确保现有代码可以无缝迁移到新的端口池实现。
"""
import logging
from typing import List, Tuple, Optional, Set
import socket


# 导入新的端口池实现 - 改为导入 getter
# from .wan_port_pool import WanPortPool, wan_port_pool
from . import get_wan_port_pool # 导入 getter
# 可能还需要 WanPortPool 类用于类型提示或备用方案，如果需要可以单独导入
# from .wan_port_pool import WanPortPool 

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
        self._ensure_port_pool()
        
        if not self.silent:
            self.logger.info("端口分配器适配器初始化完成，使用WanPortPool实现")
    
    def _ensure_port_pool(self):
        """确保端口池已正确初始化"""
        if self.port_pool is not None:
            return
            
        try:
            # 使用 getter 函数获取 WanPortPool 单例
            self.port_pool = get_wan_port_pool()
            if self.port_pool is None:
                # 如果 getter 返回 None (初始化失败)，则触发备用方案
                raise RuntimeError("get_wan_port_pool() returned None")
                
            if not self.silent:
                self.logger.debug("已连接到WanPortPool")
        except Exception as e:
            self.logger.error(f"无法获取或使用 WanPortPool 实例: {str(e)}")
            import traceback
            self.logger.error(f"错误详情: {traceback.format_exc()}")
            
            # 创建一个简单的内部端口池作为备用
            self.logger.warning("创建备用端口池")
            class DummyPortPool:
                def __init__(self):
                    self.wan_port_ranges = {0: (50000, 60000)}
                    self.allocated_ports = {0: set()}
                
                def get_available_wan_indices(self):
                    return [0]
                
                def allocate_port(self, wan_idx):
                    # 简单的随机端口分配
                    try:
                        import random
                        for _ in range(5):
                            port = random.randint(50000, 60000)
                            if port not in self.allocated_ports.get(0, set()):
                                try:
                                    # 尝试绑定端口检查可用性
                                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                    s.settimeout(0.1)
                                    s.bind(('0.0.0.0', port))
                                    s.close()
                                    # 标记为已分配
                                    if 0 not in self.allocated_ports:
                                        self.allocated_ports[0] = set()
                                    self.allocated_ports[0].add(port)
                                    return port
                                except:
                                    pass
                    except:
                        pass
                    return None
                
                def release_port(self, wan_idx, port):
                    try:
                        if wan_idx in self.allocated_ports and port in self.allocated_ports[wan_idx]:
                            self.allocated_ports[wan_idx].remove(port)
                            return True
                    except:
                        pass
                    return False
            
            # 使用备用端口池
            self.port_pool = DummyPortPool()
            self.logger.info("已初始化备用端口池")
    
    def get_available_wan_indices(self) -> List[int]:
        """
        获取可用的WAN接口索引
        
        Returns:
            可用WAN接口索引列表
        """
        self._ensure_port_pool()
        return self.port_pool.get_available_wan_indices()
    
    def allocate_port(self, wan_idx: int) -> Optional[int]:
        """
        为指定WAN接口分配一个可用端口
        
        Args:
            wan_idx: WAN接口索引
            
        Returns:
            分配的端口号，如果无法分配则返回None
        """
        self._ensure_port_pool()
        return self.port_pool.allocate_port(wan_idx)
    
    def release_port(self, wan_idx: int, port: int) -> bool:
        """
        释放已分配的端口
        
        Args:
            wan_idx: WAN接口索引
            port: 要释放的端口号
            
        Returns:
            是否成功释放
        """
        self._ensure_port_pool()
        return self.port_pool.release_port(wan_idx, port)
    
    def get_port_range(self, wan_idx: int) -> Tuple[int, int]:
        """
        获取指定WAN接口的端口范围
        
        Args:
            wan_idx: WAN接口索引
            
        Returns:
            Tuple[int, int]: 端口范围(开始端口,结束端口)
        """
        self._ensure_port_pool()
        # 从端口池的端口范围获取信息
        return self.port_pool.wan_port_ranges.get(wan_idx, (0, 0))

# 创建全局适配器实例，提供与旧API兼容的接口 - 移除模块级实例化
# port_allocator_adapter = PortAllocatorAdapter(silent=False)
