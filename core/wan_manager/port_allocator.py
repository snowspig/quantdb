"""
多WAN端口分配器模块,负责为不同WAN接口分配端口
"""
import threading
import random
import socket
import logging
from typing import Dict, List, Tuple, Optional, Set
from ..config_manager import config_manager


class PortAllocator:
    """
    端口分配器类,为多WAN连接分配本地端口
    """
    
    def __init__(self, silent=False):
        """
        初始化端口分配器
        
        Args:
            silent: 静默模式，不输出日志
        """
        self.logger = logging.getLogger("core.wan_manager.PortAllocator")
        self.silent = silent
        self.wan_port_ranges = self._parse_port_ranges()
        self.allocated_ports: Dict[int, Set[int]] = {idx: set() for idx in self.wan_port_ranges.keys()}
        self.lock = threading.Lock()
        
        # 检查配置有效性
        if not self.wan_port_ranges:
            self.logger.warning("未找到有效的WAN端口范围配置")
        elif not self.silent:
            self.logger.info(f"已加载 {len(self.wan_port_ranges)} 个WAN接口的端口范围")
            for wan_idx, (start, end) in self.wan_port_ranges.items():
                self.logger.debug(f"WAN {wan_idx} 端口范围: {start}-{end}")
    
    def _parse_port_ranges(self) -> Dict[int, Tuple[int, int]]:
        """
        解析配置中的端口范围
        
        Returns:
            字典,键为WAN索引,值为(起始端口,结束端口)元组
        """
        port_ranges = {}
        
        # 从配置中获取端口范围
        ranges_config = config_manager.get_wan_config().get('port_ranges', {})
        
        # 如果配置为空,使用默认配置
        if not ranges_config:
            port_ranges = {
                0: (50001, 51000),  # WAN 0
                1: (51001, 52000),  # WAN 1
                2: (52001, 53000),  # WAN 2
            }
            if not self.silent:
                # 使用debug级别，避免过多日志
                self.logger.debug("使用默认端口范围配置")
            return port_ranges
            
        # 解析配置
        for wan_idx_str, port_range in ranges_config.items():
            try:
                wan_idx = int(wan_idx_str)
                if isinstance(port_range, list) and len(port_range) == 2:
                    start_port, end_port = port_range
                    port_ranges[wan_idx] = (int(start_port), int(end_port))
                elif isinstance(port_range, dict) and 'start' in port_range and 'end' in port_range:
                    start_port, end_port = port_range['start'], port_range['end']
                    port_ranges[wan_idx] = (int(start_port), int(end_port))
            except (ValueError, TypeError) as e:
                self.logger.error(f"解析端口范围失败: {str(e)}")
        
        return port_ranges
    
    def get_available_wan_indices(self) -> List[int]:
        """
        获取可用的WAN接口索引
        
        Returns:
            可用WAN接口索引列表
        """
        return list(self.wan_port_ranges.keys())
    
    def allocate_port(self, wan_idx: int) -> Optional[int]:
        """
        为指定WAN接口分配一个可用端口
        
        Args:
            wan_idx: WAN接口索引
            
        Returns:
            分配的端口号,如果无法分配则返回None
        """
        if wan_idx not in self.wan_port_ranges:
            self.logger.warning(f"WAN索引 {wan_idx} 不存在")
            return None
            
        with self.lock:
            start_port, end_port = self.wan_port_ranges[wan_idx]
            allocated = self.allocated_ports[wan_idx]
            
            if not self.silent:
                self.logger.debug(f"WAN {wan_idx} 端口范围: {start_port}-{end_port}, 已分配端口数: {len(allocated)}")
            
            # 尝试查找可用端口
            attempts = 0
            max_attempts = min(100, end_port - start_port + 1)
            
            while attempts < max_attempts:
                # 随机选择一个端口
                port = random.randint(start_port, end_port)
                
                # 检查端口是否已分配
                if port in allocated:
                    attempts += 1
                    continue
                
                # 尝试绑定端口测试可用性
                try:
                    test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    test_socket.settimeout(0.1)
                    test_socket.bind(('0.0.0.0', port))
                    test_socket.close()
                    
                    # 端口可用，标记为已分配
                    allocated.add(port)
                    if not self.silent:
                        self.logger.debug(f"为WAN {wan_idx} 分配端口 {port} (范围: {start_port}-{end_port})")
                    return port
                except OSError:
                    # 端口已被占用，尝试下一个
                    attempts += 1
            
            self.logger.warning(f"无法为WAN {wan_idx} 分配可用端口，所有尝试都失败")
            return None
    
    def release_port(self, wan_idx: int, port: int) -> bool:
        """
        释放已分配的端口
        
        Args:
            wan_idx: WAN接口索引
            port: 要释放的端口号
            
        Returns:
            是否成功释放
        """
        if wan_idx not in self.allocated_ports:
            return False
            
        with self.lock:
            if port in self.allocated_ports[wan_idx]:
                self.allocated_ports[wan_idx].remove(port)
                if not self.silent:
                    self.logger.debug(f"已释放WAN {wan_idx} 的端口 {port}")
                return True
            return False
            
    def get_port_range(self, wan_idx: int) -> Tuple[int, int]:
        """
        获取指定WAN接口的端口范围
        
        Args:
            wan_idx: WAN接口索引
            
        Returns:
            Tuple[int, int]: 端口范围(开始端口,结束端口)
        """
        if wan_idx not in self.wan_port_ranges:
            self.logger.warning(f"未找到WAN {wan_idx} 的端口范围配置")
            return (0, 0)
            
        return self.wan_port_ranges[wan_idx]


# 创建全局端口分配器实例
port_allocator = PortAllocator(silent=False) 