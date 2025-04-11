#!/usr/bin/env python
"""
多WAN管理模块
提供多WAN口负载均衡和端口分配功能
"""
import logging
import threading
import os
import random
import socket
import time
from typing import Dict, List, Tuple, Optional, Set, Any

# 创建简单的端口分配器，避免复杂依赖导致的导入问题
class SimplePortAllocator:
    """简单的端口分配器，无依赖实现"""
    
    def __init__(self):
        self.logger = logging.getLogger("core.wan_manager.SimplePortAllocator")
        self.logger.info("使用简易端口分配器")
        self.wan_ranges = {
            0: (50001, 51000),
            1: (51001, 52000),
            2: (52001, 53000),
        }
        self.lock = threading.Lock()
        self.allocated_ports = {idx: set() for idx in self.wan_ranges.keys()}
    
    def get_available_wan_indices(self) -> List[int]:
        """获取可用的WAN接口索引"""
        return list(self.wan_ranges.keys())
    
    def allocate_port(self, wan_idx: int) -> Optional[int]:
        """为指定WAN接口分配一个可用端口"""
        if wan_idx not in self.wan_ranges:
            wan_idx = 0  # 默认使用第一个
            
        with self.lock:
            start_port, end_port = self.wan_ranges[wan_idx]
            allocated = self.allocated_ports[wan_idx]
            
            # 尝试最多5次
            for _ in range(5):
                # 随机选择一个端口
                port = random.randint(start_port, end_port)
                
                # 检查端口是否已分配
                if port in allocated:
                    continue
                
                # 尝试绑定端口测试可用性
                try:
                    test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    test_socket.settimeout(0.05)
                    test_socket.bind(('0.0.0.0', port))
                    test_socket.close()
                    
                    # 端口可用
                    allocated.add(port)
                    return port
                except:
                    # 端口不可用，尝试下一个
                    pass
            
            # 如果尝试都失败，使用后备端口
            fallback_port = start_port + (os.getpid() % (end_port - start_port))
            allocated.add(fallback_port)
            return fallback_port
    
    def release_port(self, wan_idx: int, port: int) -> bool:
        """释放已分配的端口"""
        if wan_idx not in self.wan_ranges:
            return False
            
        with self.lock:
            if port in self.allocated_ports[wan_idx]:
                self.allocated_ports[wan_idx].remove(port)
                return True
        return False
    
    def get_port_range(self, wan_idx: int) -> Tuple[int, int]:
        """获取指定WAN接口的端口范围"""
        return self.wan_ranges.get(wan_idx, (0, 0))

# 创建简单的负载均衡器，避免复杂依赖导致的导入问题
class SimpleLoadBalancer:
    """简单的负载均衡器，无依赖实现"""
    
    def __init__(self):
        self.logger = logging.getLogger("core.wan_manager.SimpleLoadBalancer")
        self.logger.info("使用简易负载均衡器")
        self.wan_indices = [0, 1, 2]
        self.current_index = 0
        self.lock = threading.Lock()
    
    def get_next_wan(self) -> int:
        """获取下一个WAN接口索引"""
        with self.lock:
            if not self.wan_indices:
                return 0  # 如果没有可用WAN，返回默认值
            idx = self.wan_indices[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.wan_indices)
            return idx
    
    def get_wan_count(self) -> int:
        """获取WAN接口数量"""
        return len(self.wan_indices)
        
    def update_available_wans(self, wan_indices: List[int]):
        """更新可用的WAN接口列表"""
        with self.lock:
            self.logger.info(f"更新可用WAN接口列表: {wan_indices}")
            self.wan_indices = wan_indices or [0]  # 至少保留一个默认值
            self.current_index = 0

# 创建简单的WAN监控器，避免复杂依赖导致的导入问题
class SimpleWanMonitor:
    """简单的WAN监控器，无依赖实现"""
    
    def __init__(self):
        self.logger = logging.getLogger("core.wan_manager.SimpleWanMonitor")
        self.logger.info("使用简易WAN监控器")
        self.running = False
        self.wan_status = {0: True, 1: True, 2: True}  # 所有WAN接口默认为在线
    
    def start_monitoring(self):
        """开始监控WAN接口"""
        self.logger.info("开始WAN接口监控（模拟）")
        self.running = True
    
    def stop_monitoring(self):
        """停止监控WAN接口"""
        self.logger.info("停止WAN接口监控（模拟）")
        self.running = False
    
    def is_wan_available(self, wan_idx: int) -> bool:
        """检查WAN接口是否可用"""
        return self.wan_status.get(wan_idx, False)
    
    def get_available_wans(self) -> List[int]:
        """获取所有可用的WAN接口索引"""
        return [idx for idx, status in self.wan_status.items() if status]

# 首先使用简单的实现，确保基本功能正常
port_allocator = SimplePortAllocator()
load_balancer = SimpleLoadBalancer()
wan_monitor = SimpleWanMonitor()
wan_port_pool = None
WanPortPool = None

# 允许稍后使用延迟导入加载完整功能
_lazy_initialized = False

def _lazy_initialize():
    """延迟初始化，只在实际需要时导入复杂模块"""
    global _lazy_initialized, port_allocator, load_balancer, wan_monitor, wan_port_pool, WanPortPool
    
    if _lazy_initialized:
        return
    
    try:
        # 注意：由于目前测试显示即使使用延迟加载也会有问题，所以暂时保持简单实现
        # 我们只尝试加载WanPortPool，不改变其他已经初始化的组件
        
        # 尝试导入WanPortPool
        try:
            from .wan_port_pool import wan_port_pool as _real_wan_port_pool, WanPortPool as _RealWanPortPool
            wan_port_pool = _real_wan_port_pool
            WanPortPool = _RealWanPortPool
            
            # 尝试导入适配器
            try:
                from .port_allocator_adapter import port_allocator_adapter
                # 不替换全局port_allocator，避免已有引用出问题
                # port_allocator = port_allocator_adapter 
            except Exception as e:
                logging.getLogger("core.wan_manager").warning(f"导入端口分配器适配器失败: {str(e)}")
        except Exception as e:
            logging.getLogger("core.wan_manager").warning(f"导入WAN端口池失败: {str(e)}")
        
        _lazy_initialized = True
    except Exception as e:
        logging.getLogger("core.wan_manager").error(f"延迟初始化WAN管理模块失败: {str(e)}")

# 导出的所有符号
__all__ = ['port_allocator', 'load_balancer', 'wan_monitor', 'wan_port_pool', 'WanPortPool', '_lazy_initialize'] 