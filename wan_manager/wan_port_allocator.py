"""
WAN端口分配器模块，负责管理多WAN连接的端口分配
"""
import os
import sys
import time
import random
import socket
import logging
import threading
import ipaddress
from typing import Dict, List, Set, Tuple, Optional, Any

# 导入配置管理器
from config import config_manager


class WanPortAllocator:
    """
    WAN端口分配器类，负责分配和管理多WAN连接的端口
    """
    
    def __init__(self, config_path=None, silent=False):
        """
        初始化WAN端口分配器
        
        Args:
            config_path: 配置文件路径，不提供则使用默认配置
            silent: 静默模式，不输出日志
        """
        self.config_path = config_path
        self.silent = silent
        self.lock = threading.RLock()
        
        # 从配置中加载设置
        self.enabled = config_manager.is_wan_enabled()
        self.wan_config = config_manager.get_wan_config()
        
        # 端口范围设置
        self.global_port_range = self.wan_config.get('port_range', [10000, 20000])
        self.allocation_strategy = self.wan_config.get('allocation_strategy', 'round_robin')
        
        # 接口配置
        self.interfaces = self.wan_config.get('interfaces', [])
        
        # 端口分配状态
        self.allocated_ports = {}  # {wan_idx: set(ports)}
        self.port_mappings = {}    # {port: (wan_idx, wan_port)}
        self.last_allocation_idx = -1
        
        # 初始化
        self._init_port_allocation()
        
        if not self.silent:
            if self.enabled:
                logging.info(f"WAN端口分配器初始化完成，策略: {self.allocation_strategy}")
                logging.info(f"可用WAN接口数量: {len(self.interfaces)}")
            else:
                logging.info("WAN端口分配器初始化完成，多WAN功能已禁用")
    
    def _init_port_allocation(self):
        """
        初始化端口分配
        """
        if not self.enabled:
            return
        
        with self.lock:
            # 初始化每个接口的端口分配集合
            for i, interface in enumerate(self.interfaces):
                if interface.get('enabled', True):
                    # 获取接口的端口范围
                    port_range = interface.get('port_range', self.global_port_range)
                    
                    # 创建端口集合
                    self.allocated_ports[i] = set()
                    
                    if not self.silent:
                        logging.debug(f"初始化WAN接口 {i}: {interface.get('name', f'wan{i}')}，"
                                    f"IP: {interface.get('ip', 'unknown')}，"
                                    f"端口范围: {port_range}")
    
    def get_available_wan_indices(self) -> List[int]:
        """
        获取所有可用的WAN接口索引
        
        Returns:
            List[int]: WAN接口索引列表
        """
        if not self.enabled:
            return []
        
        with self.lock:
            available = []
            for i, interface in enumerate(self.interfaces):
                if interface.get('enabled', True):
                    available.append(i)
            return available
    
    def allocate_port(self, wan_idx: Optional[int] = None) -> Tuple[Optional[int], Optional[int]]:
        """
        分配一个端口
        
        Args:
            wan_idx: 指定的WAN接口索引，None表示自动选择
            
        Returns:
            Tuple[Optional[int], Optional[int]]: (WAN接口索引, 分配的端口)，分配失败则返回(None, None)
        """
        if not self.enabled:
            return (None, None)
        
        with self.lock:
            # 获取可用的WAN接口
            available_indices = self.get_available_wan_indices()
            if not available_indices:
                if not self.silent:
                    logging.error("没有可用的WAN接口")
                return (None, None)
            
            # 如果没有指定接口或指定的接口不可用，则根据策略选择一个接口
            if wan_idx is None or wan_idx not in available_indices:
                wan_idx = self._select_wan_by_strategy()
            
            # 获取接口配置
            if wan_idx >= len(self.interfaces):
                if not self.silent:
                    logging.error(f"WAN接口索引 {wan_idx} 超出范围")
                return (None, None)
                
            interface = self.interfaces[wan_idx]
            port_range = interface.get('port_range', self.global_port_range)
            
            # 尝试分配端口
            start_port, end_port = port_range
            for port in range(start_port, end_port + 1):
                if port not in self.allocated_ports.get(wan_idx, set()):
                    # 分配端口
                    if wan_idx not in self.allocated_ports:
                        self.allocated_ports[wan_idx] = set()
                    self.allocated_ports[wan_idx].add(port)
                    self.port_mappings[port] = (wan_idx, port)
                    
                    if not self.silent:
                        logging.debug(f"为WAN {wan_idx} 分配端口: {port}")
                        
                    return (wan_idx, port)
            
            # 没有可用端口
            if not self.silent:
                logging.error(f"WAN接口 {wan_idx} 没有可用端口")
            return (None, None)
    
    def release_port(self, port: int) -> bool:
        """
        释放端口
        
        Args:
            port: 要释放的端口
            
        Returns:
            bool: 是否成功释放
        """
        if not self.enabled:
            return True
        
        with self.lock:
            if port in self.port_mappings:
                wan_idx, _ = self.port_mappings[port]
                if wan_idx in self.allocated_ports and port in self.allocated_ports[wan_idx]:
                    self.allocated_ports[wan_idx].remove(port)
                    del self.port_mappings[port]
                    
                    if not self.silent:
                        logging.debug(f"释放WAN {wan_idx} 的端口: {port}")
                    
                    return True
            
            # 端口不存在或不是由此分配器分配的
            return False
    
    def get_port_info(self, port: int) -> Optional[Tuple[int, int]]:
        """
        获取端口信息
        
        Args:
            port: 端口号
            
        Returns:
            Tuple[int, int]: (WAN接口索引, WAN端口)，如果端口未分配则返回None
        """
        with self.lock:
            return self.port_mappings.get(port)
    
    def get_wan_info(self, wan_idx: int) -> Optional[Dict[str, Any]]:
        """
        获取WAN接口信息
        
        Args:
            wan_idx: WAN接口索引
            
        Returns:
            Dict: WAN接口信息，不存在则返回None
        """
        if not self.enabled or wan_idx >= len(self.interfaces):
            return None
        
        with self.lock:
            interface = self.interfaces[wan_idx]
            if not interface.get('enabled', True):
                return None
                
            port_range = interface.get('port_range', self.global_port_range)
            allocated = len(self.allocated_ports.get(wan_idx, set()))
            total = port_range[1] - port_range[0] + 1
                
            return {
                'index': wan_idx,
                'name': interface.get('name', f'wan{wan_idx}'),
                'ip': interface.get('ip', 'unknown'),
                'enabled': interface.get('enabled', True),
                'port_range': port_range,
                'allocated_ports': allocated,
                'total_ports': total,
                'usage_percent': round(allocated / total * 100, 2) if total > 0 else 0
            }
    
    def _select_wan_by_strategy(self) -> int:
        """
        根据分配策略选择一个WAN接口
        
        Returns:
            int: 选中的WAN接口索引
        """
        available_indices = self.get_available_wan_indices()
        if not available_indices:
            return -1
        
        # 只有一个接口时直接返回
        if len(available_indices) == 1:
            return available_indices[0]
        
        # 根据策略选择
        if self.allocation_strategy == 'round_robin':
            # 轮询策略
            self.last_allocation_idx = (self.last_allocation_idx + 1) % len(available_indices)
            return available_indices[self.last_allocation_idx]
            
        elif self.allocation_strategy == 'random':
            # 随机策略
            return random.choice(available_indices)
            
        elif self.allocation_strategy == 'load_based':
            # 基于负载的策略，选择已分配端口数最少的接口
            min_load = float('inf')
            min_idx = -1
            
            for idx in available_indices:
                wan_info = self.get_wan_info(idx)
                if wan_info:
                    load = wan_info['usage_percent']
                    if load < min_load:
                        min_load = load
                        min_idx = idx
            
            return min_idx if min_idx >= 0 else available_indices[0]
        
        # 默认策略
        return available_indices[0]
    
    def get_allocation_status(self) -> Dict[str, Any]:
        """
        获取端口分配状态信息
        
        Returns:
            Dict: 分配状态信息
        """
        with self.lock:
            status = {
                'enabled': self.enabled,
                'strategy': self.allocation_strategy,
                'total_interfaces': len(self.interfaces),
                'available_interfaces': len(self.get_available_wan_indices()),
                'allocated_ports': sum(len(ports) for ports in self.allocated_ports.values()),
                'interfaces': {}
            }
            
            for i in self.get_available_wan_indices():
                wan_info = self.get_wan_info(i)
                if wan_info:
                    status['interfaces'][i] = wan_info
            
            return status
    
    def reset(self):
        """
        重置端口分配状态
        """
        with self.lock:
            self.allocated_ports = {}
            self.port_mappings = {}
            self.last_allocation_idx = -1
            self._init_port_allocation()
            
            if not self.silent:
                logging.info("WAN端口分配器已重置")
    
    def allocate_port_range(self, count: int, wan_idx: Optional[int] = None) -> List[Tuple[int, int]]:
        """
        分配一组连续的端口
        
        Args:
            count: 需要的端口数量
            wan_idx: 指定的WAN接口索引，None表示自动选择
            
        Returns:
            List[Tuple[int, int]]: [(WAN接口索引, 分配的端口), ...] 列表，分配失败则返回空列表
        """
        if not self.enabled or count <= 0:
            return []
        
        with self.lock:
            # 获取可用的WAN接口
            available_indices = self.get_available_wan_indices()
            if not available_indices:
                return []
            
            # 如果没有指定接口或指定的接口不可用，则根据策略选择一个接口
            if wan_idx is None or wan_idx not in available_indices:
                wan_idx = self._select_wan_by_strategy()
            
            # 获取接口配置
            if wan_idx >= len(self.interfaces):
                return []
                
            interface = self.interfaces[wan_idx]
            port_range = interface.get('port_range', self.global_port_range)
            
            # 寻找连续可用的端口
            start_port, end_port = port_range
            allocated = []
            
            for start in range(start_port, end_port - count + 2):
                continuous = True
                for offset in range(count):
                    port = start + offset
                    if port in self.allocated_ports.get(wan_idx, set()):
                        continuous = False
                        break
                
                if continuous:
                    # 找到连续的端口，进行分配
                    for offset in range(count):
                        port = start + offset
                        if wan_idx not in self.allocated_ports:
                            self.allocated_ports[wan_idx] = set()
                        self.allocated_ports[wan_idx].add(port)
                        self.port_mappings[port] = (wan_idx, port)
                        allocated.append((wan_idx, port))
                    
                    if not self.silent:
                        logging.debug(f"为WAN {wan_idx} 分配 {count} 个连续端口，起始端口: {start}")
                    
                    return allocated
            
            # 没有找到足够的连续端口，回退到非连续分配
            if not self.silent:
                logging.warning(f"WAN {wan_idx} 没有足够的连续端口，尝试非连续分配")
            
            allocated = []
            for _ in range(count):
                result = self.allocate_port(wan_idx)
                if result[0] is not None and result[1] is not None:
                    allocated.append(result)
                else:
                    # 分配失败，释放已分配的端口
                    for _, port in allocated:
                        self.release_port(port)
                    return []
            
            return allocated
    
    def bind_socket_to_wan(self, socket_obj: socket.socket, wan_idx: int, wan_port: int) -> bool:
        """
        将套接字绑定到指定的WAN接口和端口
        
        Args:
            socket_obj: 套接字对象
            wan_idx: WAN接口索引
            wan_port: 端口号
            
        Returns:
            bool: 是否成功绑定
        """
        try:
            # 检查接口是否可用
            if not self.enabled or wan_idx >= len(self.interfaces):
                return False
            
            interface = self.interfaces[wan_idx]
            if not interface.get('enabled', True):
                return False
            
            # 获取接口IP
            wan_ip = interface.get('ip')
            if not wan_ip:
                return False
            
            # 绑定套接字
            socket_obj.bind((wan_ip, wan_port))
            
            return True
        except Exception as e:
            if not self.silent:
                logging.error(f"绑定套接字到WAN {wan_idx} (IP:{wan_ip}, 端口:{wan_port}) 失败: {str(e)}")
            return False
    
    def is_port_available(self, port: int, wan_idx: Optional[int] = None) -> bool:
        """
        检查端口是否可用
        
        Args:
            port: 端口号
            wan_idx: WAN接口索引，None表示检查所有接口
            
        Returns:
            bool: 端口是否可用
        """
        if not self.enabled:
            return False
        
        with self.lock:
            # 检查端口是否已被分配
            if port in self.port_mappings:
                return False
            
            # 如果指定了接口，检查该接口的端口范围
            if wan_idx is not None:
                if wan_idx >= len(self.interfaces):
                    return False
                
                interface = self.interfaces[wan_idx]
                if not interface.get('enabled', True):
                    return False
                
                port_range = interface.get('port_range', self.global_port_range)
                start_port, end_port = port_range
                
                return start_port <= port <= end_port
            
            # 未指定接口，检查所有接口
            for i, interface in enumerate(self.interfaces):
                if not interface.get('enabled', True):
                    continue
                
                port_range = interface.get('port_range', self.global_port_range)
                start_port, end_port = port_range
                
                if start_port <= port <= end_port:
                    return True
            
            return False
    
    def get_wan_ip(self, wan_idx: int) -> Optional[str]:
        """
        获取指定WAN接口的IP地址
        
        Args:
            wan_idx: WAN接口索引
            
        Returns:
            str: IP地址，不存在或不可用则返回None
        """
        if not self.enabled or wan_idx >= len(self.interfaces):
            return None
        
        interface = self.interfaces[wan_idx]
        if not interface.get('enabled', True):
            return None
        
        return interface.get('ip')


# 创建全局WAN端口分配器实例
wan_port_allocator = WanPortAllocator()